"""Read-only JSON API + static frontend for the PhoenixCoin Quantum explorer.

Usage:
    python3 server.py [dsn] [--port 8080]

`dsn` is a libpq connection string (see the libpq docs); it defaults to
$EXPLORER_DSN, then to "dbname=explorer".

Endpoints:
    GET /api/summary              tip height/hash, counts
    GET /api/block/<height|hash>  block + tx list
    GET /api/tx/<txid>            transaction detail
    GET /api/address/<addr>       address balance + related txs
    GET /api/script/<hash>        script-level balance (multisig etc.)
    GET /api/mempool              current mempool txids
    GET /                         static web UI (web/index.html)

Balances are reported twice, never as one ambiguous number:

    "confirmed"  only confirmed txs on both sides. A confirmed output that a
                 mempool tx also spends is still confirmed-unspent here.
    "live"       every tx the index knows, mempool included: the balance that
                 could be spent right now.

The distinction matters most for multisig, where the script is the only view
of the money (a multi-address vout credits no single address).
"""

import argparse
import contextlib
import ipaddress
import json
import os
import queue
import signal
import socket
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import psycopg

from db import DB, COIN, DEFAULT_DSN, TOTAL_COINBASE_SQL

WEB_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")

BLOCK_COLS = ["height", "hash", "version", "merkleroot", "time", "nonce",
              "bits", "difficulty", "size", "prev_hash", "next_hash"]

# TOTAL_COINBASE_SQL lives in db.py now: it is the definition behind the
# maintained counter that db.total_coinbase() reads, and re-deriving it per
# request is what made this endpoint take 31 seconds. It is re-exported here
# so the tests that assert the read path never runs it keep resolving it.


def poke(v):
    """Integer pokes -> decimal string with 8 decimals (NOT hex).

    Split rather than scaled by a float: "%.8f" % (v / COIN) rounds through a
    double, so pokes past 2**53 display the wrong amount (9007199254740993
    renders as ...94). Dividing the digits keeps it exact at any magnitude.
    """
    v = int(v)
    neg = v < 0
    whole, frac = divmod(abs(v), COIN)
    return "%s%d.%08d" % ("-" if neg else "", whole, frac)


def _is_height(ref):
    """True if a URL path segment is a block height rather than a hash.

    ASCII digits only: str.isdigit() also accepts the Unicode digit forms
    ('²' and friends), and int() would then take a different path than the one
    this is deciding about.
    """
    return ref.isascii() and ref.isdigit()


def with_hex(balances):
    """Add the *_hex display form to a balance dict, in place."""
    for key in ("value_received", "value_spent", "balance"):
        balances[key + "_hex"] = poke(balances[key])
    return balances


def url_for(host, port):
    """A URL for `host`:`port` that can actually be opened.

    An IPv6 literal contains colons, which a URL has to bracket, so the
    unbracketed `http://::1:8080/` that used to be printed at startup was not an
    address anyone could paste into a browser.
    """
    return "http://%s:%d/" % ("[%s]" % host if ":" in host else host, port)


def reachable_off_machine(host):
    """Whether binding `host` lets anything but this machine in.

    Anything not positively a loopback address counts as reachable, because the
    cost of guessing wrong here is an unnoticed public port and the cost of
    over-warning is one line in a log. A hostname resolves to something, but
    which something is not ours to assume, so it is treated as reachable.
    """
    if host == "localhost":
        return False
    try:
        return not ipaddress.ip_address(host).is_loopback
    except ValueError:
        return True


# How long a request waits for a pool connection before answering 503. The
# pool is small and queries are short, so this only trips when several slow
# queries are in flight at once -- and an indefinite wait there is strictly
# worse than a bounded one.
POOL_TIMEOUT = 5.0

# An address's tx history is two halves of one set: the txs that paid it, and
# the txs that spent what it was paid. Written once here because the list and
# the count below have to agree on it exactly -- they are the same question
# asked twice, once bounded and once not.
#
# Orphaned txs are excluded: the txs list is current history, and no other
# endpoint counts an orphan anywhere. Reorgs sever an orphan's vin rows
# (db._clear_from), so the join could not see one on a real database -- but the
# filter states the contract rather than leaning on that data-layer detail
# alone, and an orphan test that leaves the rows in place is exactly the case
# where leaning on it would be wrong.
#
# The txs join is the expensive half and stays. Dropping it was measured as
# worth ~4s on the 510k-output address, which is not a good trade for weakening
# the guarantee.
ADDR_TXS_UNION = (
    "SELECT txid FROM addr_out WHERE address=?"
    " UNION"
    " SELECT i.txid FROM vin i"
    " JOIN txs t ON t.txid = i.txid"
    " JOIN addr_out a ON a.txid = i.prev_txid AND a.n = i.prev_vout"
    " WHERE a.address=? AND t.status != 'orphaned'")


class DBPool:
    """A fixed set of read connections, handed out to request threads.

    ThreadingHTTPServer starts a thread per request, so a per-thread connection
    would be no better than one per request; a bounded pool is what actually
    amortises the connect cost. Sized for concurrent readers, not for CPU: the
    queries are index-driven and short, and the indexer owns the writes.

    Connections are opened here, once, so a connection can be used by whichever
    thread borrows it. Checkout is what keeps that safe.

    A borrow must not nest: a handler that needed two connections at once would
    wait for itself once the pool is empty. Nothing here does, and the
    background coinbase thread keeps its own connection for the same reason.

    borrow(timeout=...) passes to the FreeLifoQueue's get: the API hands out
    503 once the pool stays exhausted, instead of a thread waiting forever.
    """

    def __init__(self, dsn, size=4):
        self._free = queue.LifoQueue()
        self._all = [DB.connect(dsn) for _ in range(size)]
        for db in self._all:
            self._free.put(db)

    @contextlib.contextmanager
    def borrow(self, timeout=None):
        # timeout=None keeps the historical indefinite wait for callers that
        # want it; queue.Empty propagates to the caller with timeout set.
        db = self._free.get(timeout=timeout)
        try:
            yield db
        finally:
            self._free.put(db)

    def close(self):
        for db in self._all:
            try:
                db.conn.close()
            except psycopg.Error:
                pass


class Explorer:
    def __init__(self, db):
        self.db = db

    def total_coinbase(self):
        # A single indexed meta lookup. This used to be a scan of every vout
        # row, cached on the tip hash and recomputed whenever the indexer moved
        # the tip -- which, on a chain being indexed continuously, was every
        # page load. 31s per request on 4.6M blocks, and the background thread
        # recomputing it held a read snapshot that stopped SQLite from ever
        # resetting the WAL, so the file grew to 33GB and every read got
        # slower, which made the scan slower still.
        #
        # The counter is maintained by the same transaction that writes the
        # rows it summarises, so this needs no cache and cannot be stale
        # against the data it is read alongside.
        return self.db.total_coinbase()

    def summary(self):
        rows = self.db.query(
            "SELECT height, hash, version, merkleroot, time, nonce, bits, "
            "difficulty, size, prev_hash, next_hash FROM blocks "
            "ORDER BY height DESC LIMIT 1")
        tip = rows[0] if rows else None
        if not tip:
            return {"tip": None}
        tipdict = dict(zip(BLOCK_COLS, tip))
        # The two counts and the supply are read from the maintained counters
        # rather than counted: COUNT(*) over 4.6M blocks and 5.3M txs is a full
        # index scan each, 0.28s and 0.63s, on the path every page load takes.
        # See DB.recompute_stats for why the counters are exact.
        nblocks = self.db.n_blocks()
        ntx = self.db.n_txs()
        tip_ntx = self.db.query(
            "SELECT COUNT(*) FROM txs WHERE height=? AND status='confirmed'",
            (tipdict["height"],))[0][0]
        nmempool = self.db.query(
            "SELECT COUNT(*) FROM txs WHERE status='mempool'")[0][0]
        out = self.total_coinbase()
        return {
            "tip": tipdict,
            "n_blocks": nblocks,
            "n_txs": ntx,
            "n_txs_tip": tip_ntx,
            "n_mempool": nmempool,
            "total_coinbase": out,
            "total_coinbase_pxc": poke(out),
        }

    def block(self, ref):
        # `ref` is a URL path segment, so it arrives as a str that is usually a
        # hash and sometimes a height. One statement cannot ask for both here:
        # blocks.height is an integer column, and Postgres rejects
        # height='<64 hex chars>' outright, where sqlite3's dynamic typing
        # silently compared them and matched nothing. So the reference is
        # classified first, and the two lookups ask disjoint questions -- a
        # 64-character hex hash cannot also be a height.
        col, val = ("height", int(ref)) if _is_height(ref) else ("hash", ref)
        rows = self.db.query(
            "SELECT height, hash, version, merkleroot, time, nonce, bits, "
            "difficulty, size, prev_hash, next_hash FROM blocks "
            "WHERE " + col + "=?", (val,))
        if not rows:
            return None
        block = dict(zip(BLOCK_COLS, rows[0]))
        txs = self.db.query(
            "SELECT txid, tx_index, is_coinbase FROM txs "
            "WHERE height=? AND status='confirmed' ORDER BY tx_index",
            (block["height"],))
        block["txs"] = [
            {"txid": t[0], "index": t[1], "coinbase": bool(t[2])} for t in txs
        ]
        out = self.total_coinbase()
        block["total_coinbase"] = out
        block["total_coinbase_pxc"] = poke(out)
        return block

    def tx(self, txid):
        row = self.db.query(
            "SELECT txid, height, tx_index, version, locktime, size, "
            "is_coinbase, status FROM txs WHERE txid=?", (txid,))
        if not row:
            return None
        t = row[0]
        vin = self.db.query(
            "SELECT prev_txid, prev_vout, coinbase, script_asm, script_hex, "
            "sequence FROM vin WHERE txid=? ORDER BY n", (txid,))
        vout = self.db.query(
            "SELECT value, type, addresses, req_sigs, script_asm, script_hex, "
            "script_hash FROM vout WHERE txid=? ORDER BY n", (txid,))
        return {
            "txid": t[0],
            "height": t[1],
            "tx_index": t[2],
            "version": t[3],
            "locktime": t[4],
            "size": t[5],
            "is_coinbase": bool(t[6]),
            "status": t[7],
            "vin": [
                {"prev_txid": v[0], "prev_vout": v[1], "coinbase": v[2],
                 "script_asm": v[3], "script_hex": v[4], "sequence": v[5]}
                for v in vin
            ],
            "vout": [
                {"value": v[0], "value_hex": poke(v[0]), "type": v[1],
                 "addresses": json.loads(v[2]) if v[2] else [],
                 "req_sigs": v[3], "script_asm": v[4], "script_hex": v[5],
                 "script_hash": v[6]}
                for v in vout
            ],
        }

    def script(self, h):
        row = self.db.query(
            "SELECT script_hash, type, req_sigs, addresses, created_height, "
            "last_height FROM scripts WHERE script_hash=?", (h,))
        if not row:
            return None
        r = row[0]
        bal = self.db.script_balances(h)
        return {
            "script_hash": r[0],
            "type": r[1],
            "req_sigs": r[2],
            "addresses": json.loads(r[3]) if r[3] else [],
            "created_height": r[4],
            "last_height": r[5],
            "confirmed": with_hex(bal["confirmed"]),
            "live": with_hex(bal["live"]),
        }

    ADDR_OUT_LIMIT = 2000

    def address(self, addr):
        n_out = self.db.query(
            "SELECT COUNT(*) FROM addr_out WHERE address=?", (addr,))[0][0]
        if n_out == 0:
            return None
        bal = self.db.address_balances(addr)
        outs = self.db.query(
            "SELECT txid, n, value, type, spent_by FROM addr_out "
            "WHERE address=? ORDER BY txid, n LIMIT ?",
            (addr, self.ADDR_OUT_LIMIT))
        # Transactions touching the address from either side: the ones that paid
        # it and the ones that spent what it was paid, which is what
        # ADDR_TXS_UNION holds. The list used to be built from addr_out alone, so
        # an address whose every received coin was later spent ended its tx
        # history at the last payout -- the transactions that moved those coins
        # back out were invisible, and n_txs understated the participation.
        #
        # Both halves used to be assembled here in Python, which made this the
        # one endpoint whose cost the caller chose: the spender enumeration had
        # no row cap, n_txs was len() of the union, and the union was then
        # sorted. So a high-activity address put its entire tx history into the
        # request thread's heap -- one statement's worth of rows, plus a sort
        # over them -- before being sliced back down to ADDR_OUT_LIMIT entries
        # for the response. The outputs half above was capped; this one was not,
        # and the 510k-output address in the tests is the case that proves it.
        #
        # Now the cap is a LIMIT in the statement, so the database stops when it
        # has enough and this process never sees the rest. n_txs stays exact --
        # txs_truncated is judged against it, and reporting a bounded number as
        # the real one would be a different lie -- but it is counted where the
        # rows already are, so the set behind it is the database's memory
        # concern rather than ours.
        txs = [r[0] for r in self.db.query(
            ADDR_TXS_UNION + " ORDER BY txid LIMIT ?",
            (addr, addr, self.ADDR_OUT_LIMIT))]
        n_txs = self.db.query(
            "SELECT COUNT(*) FROM (%s) u" % ADDR_TXS_UNION,
            (addr, addr))[0][0]
        return {
            "address": addr,
            "confirmed": with_hex(bal["confirmed"]),
            "live": with_hex(bal["live"]),
            "txs": txs,
            "n_txs": n_txs,
            "txs_truncated": n_txs > self.ADDR_OUT_LIMIT,
            "n_outputs": n_out,
            "outputs_truncated": n_out > self.ADDR_OUT_LIMIT,
            "outputs": [
                {"txid": r[0], "n": r[1], "value": r[2],
                 "value_hex": poke(r[2]), "type": r[3],
                 # spent_by is a mask, so the live reading is "any bit" and the
                 # confirmed one is the low bit -- the same split the balances
                 # use, read here per output rather than aggregated.
                 "spent": bool(r[4] & 3),
                 "spent_confirmed": bool(r[4] & 1)}
                for r in outs
            ],
        }

    def mempool(self):
        return [r[0] for r in self.db.query(
            "SELECT txid FROM txs WHERE status='mempool' ORDER BY txid")]

    def recent_blocks(self, limit=20):
        # One statement, not 1 + limit round-trips. Written as a correlated
        # scalar subquery rather than a LEFT JOIN + GROUP BY on purpose: the
        # subquery is a seek on idx_txs_status_height per block, while grouping
        # has to scan every tx row in the table to aggregate them. Measured on
        # 2000 blocks / 160k txs: 0.24 ms here, 0.51 ms for the GROUP BY form.
        rows = self.db.query(
            "SELECT height, hash, version, merkleroot, time, nonce, bits, "
            "difficulty, size, prev_hash, next_hash, "
            "(SELECT COUNT(*) FROM txs t WHERE t.height=b.height "
            "  AND t.status='confirmed') "
            "FROM blocks b ORDER BY height DESC LIMIT ?", (limit,))
        return [dict(zip(BLOCK_COLS + ["n_txs"], r)) for r in rows]


class DualStackHTTPServer(ThreadingHTTPServer):
    """IPv4/IPv6 dual-stack HTTP server."""
    address_family = socket.AF_INET6

    def server_bind(self):
        if self.address_family == socket.AF_INET6:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


class Handler(BaseHTTPRequestHandler):
    pool = None            # set once in main(); a DBPool

    # HTTP/1.1 so a page load's three requests (document, summary, recent
    # blocks) reuse one connection. The default is HTTP/1.0, which closes the
    # socket after every response, so the browser paid three TCP handshakes to
    # render one page. Safe here because every response below sets
    # Content-Length, which is what tells the client where a body ends.
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        ip = self.client_address[0] if self.client_address else "?"
        print("%s %s" % (ip, fmt % args), flush=True)

    def _send(self, status, obj):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_404(self):
        self._send(404, {"error": "not found"})

    def do_GET(self):
        url = urlparse(self.path)
        path = url.path

        if path == "/" or path == "/index.html":
            self._serve_static("index.html")
            return
        if path.startswith("/api/"):
            self._api(path)
            return
        self._serve_static(path.lstrip("/"))

    def _serve_static(self, name):
        root = os.path.realpath(WEB_ROOT)
        target = os.path.realpath(os.path.join(WEB_ROOT, name))
        # commonpath() rejects true traversal (".." escaping the tree) unlike
        # a naive startswith(WEB_ROOT) prefix check, which would also accept
        # sibling dirs like "/web-secret" and follow symlinks.
        if os.path.commonpath((root, target)) != root or \
                not os.path.isfile(target):
            self._send_404()
            return
        stat = os.stat(target)
        etag = '"%x-%x-%x"' % (stat.st_ino, stat.st_mtime_ns, stat.st_size)
        # The document is the same bytes on every page load, and it is the one
        # response that does not need the database at all -- so it is the one
        # that can be answered without reading anything. A returning visitor
        # revalidates and gets a 304 with no body; the first load reads the
        # file once and lets the browser keep it.
        if self.headers.get("If-None-Match") == etag:
            self.send_response(304)
            self.send_header("ETag", etag)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        with open(target, "rb") as f:
            body = f.read()
        ctype = "text/html" if target.endswith(".html") else \
            "application/javascript" if target.endswith(".js") else \
            "text/css" if target.endswith(".css") else \
            "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", etag)
        # The UI is one self-contained file with no build step, so a stale
        # copy in a browser cache is the only way a fix can fail to reach
        # anyone. must-revalidate keeps the 304 cheap without pinning them to
        # a copy they cannot refresh.
        self.send_header("Cache-Control", "no-cache, must-revalidate")
        self.end_headers()
        self.wfile.write(body)

    def _api(self, path):
        parts = [p for p in path.split("/") if p]
        if not parts or parts[0] != "api":
            self._send_404()
            return
        try:
            # The borrow only spans the querying. The response body is written
            # after the connection is returned, so a slow client doggedly
            # draining the socket cannot hold a connection that another
            # request is waiting on -- only concurrent *queries* share the
            # pool, never *writes*.
            with self.pool.borrow(timeout=POOL_TIMEOUT) as db:
                exp = Explorer(db)
                if len(parts) == 1 or parts[1] == "summary":
                    res = 200, exp.summary()
                elif parts[1] == "mempool":
                    res = 200, exp.mempool()
                elif parts[1] == "recent_blocks":
                    res = 200, exp.recent_blocks()
                elif parts[1] == "block" and len(parts) == 3:
                    b = exp.block(parts[2])
                    res = (200, b) if b else (404, {"error": "not found"})
                elif parts[1] == "tx" and len(parts) == 3:
                    t = exp.tx(parts[2])
                    res = (200, t) if t else (404, {"error": "not found"})
                elif parts[1] == "address" and len(parts) == 3:
                    a = exp.address(parts[2])
                    res = (200, a) if a else (404, {"error": "not found"})
                elif parts[1] == "script" and len(parts) == 3:
                    s = exp.script(parts[2])
                    res = (200, s) if s else (404, {"error": "not found"})
                else:
                    res = 404, {"error": "not found"}
        except queue.Empty:
            # Every connection is on loan and none came free in time -- four
            # simultaneous long queries. Say so instead of waiting forever.
            self._send(503, {"error": "busy"})
            return
        except psycopg.Error as e:
            self._send(500, {"error": str(e)})
            return
        self._send(*res)


def argument_parser():
    """The command line, in one place so the defaults can be asserted on.

    `main()` used to build this inline, which left the bind address -- the one
    setting that decides whether the explorer is reachable from the network --
    with no way to read it back without starting a server.
    """
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("dsn", nargs="?", default=DEFAULT_DSN)
    p.add_argument("--host", default="127.0.0.1",
                   help="address to bind; loopback by default, so exposing "
                        "this is something you ask for rather than something "
                        "that happens")
    p.add_argument("--port", type=int, default=8080)
    return p


def main():
    args = argument_parser().parse_args()

    server_cls = DualStackHTTPServer
    if ":" not in args.host:  # literal IPv4 address -> plain IPv4 bind
        server_cls = ThreadingHTTPServer
    # Bind BEFORE the schema work, and treat that work as optional. The schema
    # belongs to the indexer: on a database being built from scratch it is
    # creating tables and locking them for as long as that takes, and this
    # process used to stand still waiting for that lock before opening a
    # socket -- so a cold start answered the browser with connection-refused
    # until someone ran it a second time. Reading needs no lock and no DDL
    # (DB.connect opens schema=False), so there is nothing here that has to
    # finish before the port answers.
    httpd = server_cls((args.host, args.port), Handler)
    print("explorer running on %s" % url_for(args.host, args.port))
    if reachable_off_machine(args.host):
        print("warning: bound to %s, so this port answers from outside this "
              "machine and the explorer is served over plain HTTP with no "
              "authentication -- bind loopback and let a TLS-terminating proxy "
              "be the public face" % args.host)

    try:
        DB.initialize(args.dsn)   # create the schema if nobody else has
    except Exception as e:       # already; a failure here must not stop the
        print("warning: schema init failed (%s); serving anyway" % e)
    Handler.pool = DBPool(args.dsn)   # and warm the read connections

    def _stop(signum, frame):
        raise KeyboardInterrupt

    # Route SIGTERM through the same graceful path as Ctrl-C.
    signal.signal(signal.SIGTERM, _stop)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()
    httpd.server_close()
    Handler.pool.close()


if __name__ == "__main__":
    sys.exit(main())