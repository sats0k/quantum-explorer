"""Read-only JSON API + static frontend for the PhoenixCoin Quantum explorer.

Usage:
    python3 server.py [db-path] [--port 8080]

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
import json
import os
import queue
import signal
import socket
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from db import DB, COIN

WEB_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")

BLOCK_COLS = ["height", "hash", "version", "merkleroot", "time", "nonce",
              "bits", "difficulty", "size", "prev_hash", "next_hash"]

# Cumulative minted supply: every coinbase output ever indexed. This is NOT
# "outstanding/unspent" — it is never decremented by spends. (A true unspent
# figure would need a UTXO model, e.g. subtracting spent prevouts.)
#
# The status='confirmed' is redundant today: clear_from() deletes the vout rows
# of the txs it orphans, so an orphaned coinbase contributes nothing either way.
# It stays because that redundancy is invisible from this query alone, and the
# invariant is worth stating here rather than leaving it to a reader who does
# not know what clear_from() does. A surviving orphan vout would otherwise
# silently inflate minted supply for the life of the process.
TOTAL_COINBASE_SQL = (
    "SELECT COALESCE(SUM(v.value),0) FROM vout v JOIN txs t "
    "ON v.txid = t.txid WHERE t.is_coinbase = 1 AND t.status = 'confirmed'")

# The cache is keyed on the tip HASH, not the tip height. A reorg can replace
# the block at the current tip without changing its height, so a height key
# would keep serving the pre-reorg total. Hash changes on every reorg, and the
# sum only moves when a coinbase is indexed or orphaned, which always comes
# with a tip change.
TIP_HASH_SQL = "SELECT hash FROM blocks ORDER BY height DESC LIMIT 1"
_TOTAL_LOCK = threading.Lock()
_TOTAL_CACHE = None  # (tip_hash, pokes)


def _total_coinbase_query(db):
    return db.query(TOTAL_COINBASE_SQL)[0][0]


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


def with_hex(balances):
    """Add the *_hex display form to a balance dict, in place."""
    for key in ("value_received", "value_spent", "balance"):
        balances[key + "_hex"] = poke(balances[key])
    return balances


# How long a request waits for a pool connection before answering 503. The
# pool is small and queries are short, so this only trips when several slow
# queries are in flight at once -- and an indefinite wait there is strictly
# worse than a bounded one.
POOL_TIMEOUT = 5.0


class DBPool:
    """A fixed set of read connections, handed out to request threads.

    ThreadingHTTPServer starts a thread per request, so a per-thread connection
    would be no better than one per request; a bounded pool is what actually
    amortises the connect cost. Sized for concurrent readers, not for CPU: the
    queries are index-driven and short, and SQLite serialises writes anyway
    (the indexer owns those).

    Connections are opened here, once, with check_same_thread=False, so a
    connection can be used by whichever thread borrows it. Checkout is what
    keeps that safe, not the flag.

    A borrow must not nest: a handler that needed two connections at once would
    wait for itself once the pool is empty. Nothing here does, and the
    background coinbase thread keeps its own connection for the same reason.

    borrow(timeout=...) passes to the FreeLifoQueue's get: the API hands out
    503 once the pool stays exhausted, instead of a thread waiting forever.
    """

    def __init__(self, path, size=4):
        self._free = queue.LifoQueue()
        self._all = [DB.connect(path) for _ in range(size)]
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
            except sqlite3.Error:
                pass


class Explorer:
    def __init__(self, db):
        self.db = db

    def total_coinbase(self):
        # Keyed on the tip hash so a new block (or a reorg at the same height)
        # invalidates immediately, instead of going stale for up to 60s. The
        # validity check is a 0.015 ms indexed lookup; the value it guards is an
        # 84 ms scan of every vout row on a 2000-block chain, and that gap is
        # what the cache is for.
        global _TOTAL_CACHE
        rows = self.db.query(TIP_HASH_SQL)
        tip = rows[0][0] if rows else None
        with _TOTAL_LOCK:
            c = _TOTAL_CACHE
            if c and c[0] == tip:
                return c[1]
        val = _total_coinbase_query(self.db)
        with _TOTAL_LOCK:
            _TOTAL_CACHE = (tip, val)
        return val

    def summary(self):
        rows = self.db.query(
            "SELECT height, hash, version, merkleroot, time, nonce, bits, "
            "difficulty, size, prev_hash, next_hash FROM blocks "
            "ORDER BY height DESC LIMIT 1")
        tip = rows[0] if rows else None
        nblocks = self.db.query("SELECT COUNT(*) FROM blocks")[0][0]
        ntx = self.db.query(
            "SELECT COUNT(*) FROM txs WHERE status != 'orphaned'")[0][0]
        if not tip:
            return {"tip": None}
        tipdict = dict(zip(BLOCK_COLS, tip))
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
        rows = self.db.query(
            "SELECT height, hash, version, merkleroot, time, nonce, bits, "
            "difficulty, size, prev_hash, next_hash FROM blocks "
            "WHERE hash=? OR height=?", (ref, ref))
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
            "SELECT txid, n, value, type, is_spent FROM addr_out "
            "WHERE address=? ORDER BY txid, n LIMIT ?",
            (addr, self.ADDR_OUT_LIMIT))
        # Every transaction spending one of this address's outputs, in a single
        # enumeration. The per-output spent_confirmed set and the spending half
        # of the txs list both come from it, so a busy address is not walked
        # twice for the two purposes; is_spent is live (a mempool spend sets it),
        # so per-output status is reported as both readings rather than one
        # "spent" flag.
        #
        # Orphaned txs are excluded: the txs list is current history, and no
        # other endpoint counts an orphan anywhere. Reorgs already sever an
        # orphan's vin rows (db._clear_from), so the join could not see one
        # anyway -- the status filter states the contract here rather than
        # leaning on that data-layer detail alone.
        spends = self.db.query(
            "SELECT i.txid, i.prev_txid, i.prev_vout, t.status FROM vin i"
            " JOIN txs t ON t.txid = i.txid"
            " JOIN addr_out a ON a.txid = i.prev_txid AND a.n = i.prev_vout"
            " WHERE a.address=? AND t.status != 'orphaned'", (addr,))
        spend_txids = set()
        spent_confirmed = set()
        for txid, prev_txid, prev_vout, status in spends:
            spend_txids.add(txid)
            if status == "confirmed":
                spent_confirmed.add((prev_txid, prev_vout))
        # Transactions touching the address from either side: the receiving ones
        # straight from addr_out (the primary key streams them in txid order),
        # unioned with the spenders above, deduped and sorted in Python. The
        # list used to be built from addr_out alone, so an address whose every
        # received coin was later spent ended its tx history at the last payout
        # -- the transactions that moved those coins back out were invisible,
        # and n_txs understated the participation.
        involved = sorted(set(r[0] for r in self.db.query(
            "SELECT DISTINCT txid FROM addr_out WHERE address=?", (addr,)))
            | spend_txids)
        n_txs = len(involved)
        txs = involved[:self.ADDR_OUT_LIMIT]
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
                 "spent": bool(r[4]),
                 "spent_confirmed": (r[0], r[1]) in spent_confirmed}
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
        with open(target, "rb") as f:
            body = f.read()
        ctype = "text/html" if target.endswith(".html") else \
            "application/javascript" if target.endswith(".js") else \
            "text/css" if target.endswith(".css") else \
            "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
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
        except sqlite3.Error as e:
            self._send(500, {"error": str(e)})
            return
        self._send(*res)


def _total_coinbase_loop(db_path, interval=5):
    """Keep the coinbase cache warm so requests never pay the vout scan.

    This used to recompute the sum on a 60s timer. It now watches the tip hash
    instead and recomputes only when it moves: the poll is an indexed lookup
    (0.015 ms) where the sum is a full scan of vout (84 ms on a 2000-block
    chain), so an idle explorer now does ~0.02 ms of work every 5s rather than
    an 84 ms scan every 60s, and the value is never stale by more than one
    poll interval. total_coinbase() recomputes on demand too, so correctness
    never depends on this thread running.

    connect(), not DB(): this thread is a reader, and it must not race the
    indexer into a schema migration. main() has already initialized.
    """
    db = DB.connect(db_path)
    global _TOTAL_CACHE
    while True:
        try:
            rows = db.query(TIP_HASH_SQL)
            tip = rows[0][0] if rows else None
            with _TOTAL_LOCK:
                c = _TOTAL_CACHE
                fresh = c is not None and c[0] == tip
            if not fresh:
                val = _total_coinbase_query(db)
                with _TOTAL_LOCK:
                    _TOTAL_CACHE = (tip, val)
        except sqlite3.Error:
            pass
        time.sleep(interval)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("db", nargs="?", default="explorer.db")
    p.add_argument("--host", default="::")
    p.add_argument("--port", type=int, default=8080)
    args = p.parse_args()

    DB.initialize(args.db)   # create/migrate the schema exactly once
    Handler.pool = DBPool(args.db)   # and warm the read connections
    threading.Thread(target=_total_coinbase_loop, args=(args.db,),
                     daemon=True).start()
    server_cls = DualStackHTTPServer
    if ":" not in args.host:  # literal IPv4 address -> plain IPv4 bind
        server_cls = ThreadingHTTPServer
    httpd = server_cls((args.host, args.port), Handler)
    print("explorer running on http://%s:%d/" % (args.host, args.port))

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