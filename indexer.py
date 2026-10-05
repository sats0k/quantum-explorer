"""Chain + mempool indexer.

Walks the daemon's JSON-RPC from height 0 to the tip, storing the
transactions it decodes into PostgreSQL. Chain transactions arrive inside
the block reply (`getblock <hash> 2`), so a window costs one RPC call per
block rather than one per block plus one per tx; mempool transactions,
which have no block, are still fetched individually. The (already decoded
by the daemon) script types/addresses are stored as given. Re-runs pick up
where it stopped; the mempool is refreshed each cycle.

Usage:
    python3 indexer.py [dsn] --rpcuser U --rpcpassword P [--host H] [--port N]

`dsn` is a libpq connection string (see the libpq docs); it defaults to
$EXPLORER_DSN, then to "dbname=explorer".
"""

import argparse
import collections
import signal
import sys
import threading
import time

from decimal import Decimal

import psycopg

from rpc import RPC, RPCError
from db import DB, COIN, DEFAULT_DSN

TX_TYPES_WITH_ADDRESSES = {
    "pubkey", "pubkeyhash", "scripthash", "hybrid_pubkey",
    "hybrid_pubkeyhash", "hybrid_multisig",
}


class Out:
    def __init__(self, value, type_, addresses, req_sigs, script_asm, script_hex):
        self.value = value
        self.type = type_
        self.addresses = addresses
        self.req_sigs = req_sigs
        self.script_asm = script_asm
        self.script_hex = script_hex


class In:
    def __init__(self, prev_txid, prev_vout, coinbase, script_asm, script_hex, sequence):
        self.prev_txid = prev_txid
        self.prev_vout = prev_vout
        self.coinbase = coinbase
        self.script_asm = script_asm
        self.script_hex = script_hex
        self.sequence = sequence


class Tx:
    def __init__(self, txid, height, tx_index, version, locktime, size,
                 is_coinbase, vin, vout):
        self.txid = txid
        self.height = height
        self.tx_index = tx_index
        self.version = version
        self.locktime = locktime
        self.size = size
        self.is_coinbase = is_coinbase
        self.vin = vin
        self.vout = vout


class Block:
    def __init__(self, j):
        self.height = int(j["height"])
        self.hash = j["hash"]
        self.version = j.get("version")
        self.merkleroot = j.get("merkleroot")
        self.time = j.get("time")
        self.nonce = j.get("nonce")
        self.bits = j.get("bits")
        # RPC decoding uses parse_float=Decimal so amounts stay exact (see
        # amount_to_pokes), which means this genuinely-fractional field arrives
        # as a Decimal too. The driver cannot bind one, so it is converted here --
        # the only non-integer numeric stored anywhere, into a REAL column.
        d = j.get("difficulty")
        self.difficulty = float(d) if d is not None else None
        self.size = j.get("size")
        self.prev_hash = j.get("previousblockhash")
        self.next_hash = j.get("nextblockhash")
        # verbosity=2 puts a decoded transaction object in each slot; verbosity=1
        # (or no argument) puts a bare txid string. Both are accepted so the
        # indexer still works against a daemon without the verbosity argument.
        self.txs = j.get("tx", [])


def tx_from_verbose(j, height, tx_index):
    vin = []
    is_coinbase = False
    for raw in j.get("vin", []):
        if "coinbase" in raw:
            is_coinbase = True
            vin.append(In(None, None, raw["coinbase"], None, None,
                          raw.get("sequence")))
        else:
            ss = raw.get("scriptSig") or {}
            vin.append(In(raw.get("txid"), raw.get("vout"), None,
                          ss.get("asm"), ss.get("hex"), raw.get("sequence")))
    vout = []
    for raw in j.get("vout", []):
        sp = raw.get("scriptPubKey") or {}
        vout.append(Out(
            amount_to_pokes(raw["value"]),
            sp.get("type"),
            sp.get("addresses") or [],
            sp.get("reqSigs"),
            sp.get("asm"),
            sp.get("hex"),
        ))
    size = j.get("size")
    if size is None:
        h = j.get("hex", "")
        size = len(h) // 2 if h else None
    return Tx(j.get("txid"), height, tx_index, j.get("version"),
              j.get("locktime"), size, is_coinbase, vin, vout)


def amount_to_pokes(raw):
    """Coins (Decimal, str, or int) -> integer pokes, exactly.

    Never float. A double carries 53 bits of mantissa, so an amount past
    2**53 pokes -- around 90 million PXC -- is silently misrounded, and
    int(round(...)) hides it by producing a confident wrong integer. Decimal
    keeps the literal the daemon sent.

    A value that is not a whole number of pokes is an error rather than a
    truncation: PhoenixCoin has 8 decimals, so a 9th decimal means the amount
    came from somewhere untrustworthy, and quietly dropping it would be how a
    supply figure goes wrong without anyone noticing.
    """
    d = raw if isinstance(raw, Decimal) else Decimal(str(raw))
    scaled = d * COIN
    if scaled != scaled.to_integral_value():
        raise ValueError("amount %r is not a whole number of pokes" % (raw,))
    pokes = int(scaled)
    if not -(2 ** 63) <= pokes < 2 ** 63:
        raise ValueError("amount %r does not fit in an int64" % (raw,))
    return pokes


def tx_stub(txid, height, tx_index, is_coinbase):
    """Placeholder for a tx the daemon will not hand out.

    Only reachable on the fallback path, against a daemon with no getblock
    verbosity argument. This fork is built without -txindex (no such arg in the
    binary, and `getrawtransaction` falls back to the utxo view), so a confirmed
    tx whose outputs are ALL spent is unretrievable there -- the genesis
    coinbase is the one guaranteed case. Store the txid so the block view and
    tx counts stay complete, with no vin/vout rather than a crash.
    """
    return Tx(txid, height, tx_index, None, None, None, is_coinbase, [], [])


# How far a getrawtransaction batch goes before giving up on a slot that
# failed without looking permanent. One hiccup in a 500-call window used to
# stub a tx forever; re-batching only the failed slots turns a transient
# refusal into a properly indexed tx, and a bounded cap keeps a broken daemon
# from stalling the window indefinitely.
_TX_RETRIES = 3
_TX_BACKOFF = 0.5


def _stub_worthy(e):
    # Only a refusal that will never clear -- the daemon has no information for
    # the tx (this fork has no -txindex, so a confirmed tx with all outputs
    # spent is permanently unretrievable) -- is worth stubbing on the first
    # pass. Anything else is a transient refusal and gets the retry loop.
    # Matched on the message: a single-call RPCError carries only its text,
    # whereas a batch slot also carries a numeric code (-5).
    return "no information available about transaction" in str(e).lower()


def _slot_txid(t):
    """The txid of a getblock `tx` slot, decoded object or bare string."""
    return t.get("txid") if isinstance(t, dict) else t


# A daemon that does not know the argument answers with its usage string for
# getblock, which starts with the call signature ("getblock <hash> ...") or says
# plainly that the parameter is wrong.
_USAGE_MARKERS = (
    "getblock <hash>",
    "wrong number of parameters",
    "unknown parameter",
    "unrecognized parameter",
    "invalid parameter",
    "verbosity must be",
)

# Checked first, and decisive: these mean the request never reached a daemon
# that had an opinion about its arguments. A proxy's own 400 page can quote the
# phrase "invalid parameter" verbatim, so a transport failure must not be read
# as a usage complaint however the wording looks.
_TRANSPORT_MARKERS = (
    "connection failed",
    "http 4",
    "http 5",
    "invalid json-rpc response",
    "timed out",
    "timeout",
)


def _verbosity_unsupported(e):
    """Is this RPCError the daemon refusing the verbosity argument?

    Deliberately narrow. Treating *any* batch failure as a missing argument
    would let one hiccup -- a dropped connection, a 500, a malformed reply,
    an individual block that could not be read -- silently retire the fast
    path for the rest of the process's life, with nothing in the log but a
    fallback notice. Only a complaint about the argument itself counts; every
    other failure is left to propagate so the retry loop can see it.
    """
    text = str(e).lower()
    if any(m in text for m in _TRANSPORT_MARKERS):
        return False
    return any(m in text for m in _USAGE_MARKERS)


class Indexer:
    def __init__(self, db, rpc):
        self.db = db
        self.rpc = rpc
        self.counters = collections.Counter()
        # Set by SIGTERM/SIGINT handler; run() checks it between units of
        # work (never inside a bulk transaction) and exits cleanly.
        self._stop = threading.Event()
        # Ask for decoded transactions inside the block reply. Dropped to 1 the
        # first time a daemon turns out not to honour the argument, and
        # remembered, so the fallback is paid for once rather than once per
        # window.
        self._verbosity = 2

    def get_blocks(self, hashes):
        """Blocks by hash, with their transactions already decoded.

        Two ways a daemon can not oblige, both permanent:

        - it rejects the second argument, saying so in a usage error, or
        - it accepts the call and answers with bare txids, having ignored the
          argument. Only the reply can tell us this, and it must be acted on:
          asking again next window would get the same bare txids, and every
          window would pay for a per-tx fallback it had already learned is
          unnecessary. So the shapes are checked on every window, not just the
          first, and `_verbosity` is latched down as soon as txids come back.

        A window holding no transactions at all proves nothing either way, so
        the verbosity is left alone rather than given up on that evidence.

        A refusal of the argument is the one failure that is worth acting on.
        Any other RPCError is re-raised untouched: a daemon that was briefly
        unreachable has not told us anything about its arguments, and treating
        the hiccup as a missing feature would cost the fast path permanently
        and quietly.
        """
        if self._verbosity == 1:
            return self.rpc.batch([("getblock", (bh,)) for bh in hashes])
        try:
            blocks = self.rpc.batch(
                [("getblock", (bh, self._verbosity)) for bh in hashes])
        except RPCError as e:
            if not _verbosity_unsupported(e):
                raise
            self._verbosity = 1
            print("daemon rejects the getblock verbosity argument, falling "
                  "back to one lookup per tx")
            return self.rpc.batch([("getblock", (bh,)) for bh in hashes])
        slots = [t for j in blocks for t in j.get("tx", [])]
        if slots and not any(isinstance(t, dict) for t in slots):
            self._verbosity = 1
            print("daemon ignores the getblock verbosity argument (block "
                  "replies carry txids), falling back to one lookup per tx")
        return blocks

    def store_stub(self, txid, height, tx_index, exhausted=False):
        """Write the placeholder for a tx we will never detail, and count it."""
        self.counters["tx_stub"] += 1
        if self.counters["tx_stub"] <= 5:
            if exhausted:
                print("getrawtransaction still failing after %d attempts, "
                      "storing %s at height %d without inputs/outputs"
                      % (_TX_RETRIES, txid, height))
            else:
                print("no txindex: %s at height %d is not retrievable, "
                      "storing it without inputs/outputs" % (txid, height))
        self.db.add_tx(tx_stub(txid, height, tx_index, tx_index == 0))

    def add_tx_by_txid(self, height, tx_index, txid):
        """Fallback for a daemon with no getblock verbosity support.

        The transaction is fetched one RPC call at a time, since there is no
        batched block reply to take it from. A refusal that will never clear
        (this fork has no -txindex, so a confirmed tx whose outputs are all
        spent cannot be retrieved) is stubbed straight away; anything else gets
        the bounded retry loop before being stubbed, so one transient hiccup
        does not permanently cost a transaction its inputs and outputs.
        """
        for attempt in range(_TX_RETRIES):
            if attempt:
                time.sleep(_TX_BACKOFF * attempt)
            try:
                j = self.rpc.getrawtransaction(txid, verbose=True)
            except RPCError as e:
                if _stub_worthy(e):
                    self.store_stub(txid, height, tx_index)
                    return True
                continue
            if j is None:
                self.store_stub(txid, height, tx_index)
                return True
            self.db.add_tx(tx_from_verbose(j, height, tx_index))
            return True
        # Retries exhausted for a refusal that never said "not found": store
        # the row, lose the detail, rather than hold the window open forever.
        self.store_stub(txid, height, tx_index, exhausted=True)
        return False

    def sync_blocks(self, batch_size=100):
        # Stubs found during THIS call, not the running total: the counters
        # live for the process, so a cumulative count would reprint the summary
        # on every cycle even when nothing new was indexed.
        stubs = 0
        while True:
            if self._stop.is_set():
                return
            tip = int(self.rpc.getblockcount())
            synced = self.db.tip_height()
            if synced > tip:
                # We hold blocks the daemon does not, which no reorg of a longer
                # chain explains: it rolled back, or was restored from an older
                # backup. Hand them back before anything else, because the tip
                # check below asks for getblockhash(synced) -- a height the
                # daemon does not have -- and that error is retried forever
                # without ever reaching the code that truncates.
                if tip < 0:
                    # No chain to compare against yet. Truncating here would
                    # throw away a good index over a reading about to change.
                    return
                print("daemon tip %d is behind our %d, truncating" % (tip, synced))
                self.db.clear_from(tip + 1)
                synced = tip
            if synced >= tip:
                # We're at (or beyond) the tip — verify the tip hash matches,
                # otherwise the chain reorganized underneath us.
                want = self.rpc.getblockhash(synced)
                have = self.db.query("SELECT hash FROM blocks WHERE height=?", (synced,))
                if have and have[0][0] == want:
                    break
                if not have:
                    synced -= 1
                    self.db.clear_from(synced + 1)
                else:
                    print("tip hash changed at %d, truncating" % synced)
                    self.db.clear_from(synced)
                    synced -= 1
                continue
            # One-time seed for the catch-up: hash at the last height we hold.
            # After that we carry prev_hash across the window loop, so no
            # per-block SELECT hash FROM blocks is needed.
            seed = None
            if synced >= 0:
                seed = self.rpc.getblockhash(synced)
            prev_hash = seed
            for start in range(synced + 1, tip + 1, batch_size):
                heights = list(range(start, min(start + batch_size, tip + 1)))
                hashes = self.rpc.batch([("getblockhash", (h,)) for h in heights])
                # verbosity=2 returns the block's transactions already decoded
                # inside the block reply, so a window costs one RPC call per
                # block instead of one per block plus one per transaction. On
                # real data that is ~880 fewer calls per 100 blocks.
                blocks = self.get_blocks(hashes)
                txlist = []
                for h, j in zip(heights, blocks):
                    for i, t in enumerate(Block(j).txs):
                        txlist.append((h, i, t))
                with self.db.bulk():
                    for h, bh, j in zip(heights, hashes, blocks):
                        blk = Block(j)
                        if blk.prev_hash and prev_hash and prev_hash != blk.prev_hash:
                            print("reorg at %d, truncating" % h)
                            self.db.clear_from(h)
                            return
                        self.db.add_block(blk)
                        prev_hash = bh
                    # The window's transactions are already in hand: a
                    # verbosity=2 slot is the decoded transaction object.
                    # Ask once, for every txid in the window, what each of them
                    # already held. Inside the block, add_tx makes two round
                    # trips per tx to find out -- and on a chain being indexed
                    # for the first time the answer is always "nothing", which
                    # is exactly the case two round trips per tx cannot notice.
                    # Done before the window's first add_tx, so the reads see
                    # the same rows they would have seen individually.
                    self.db.prefetch_txs(
                        _slot_txid(t) for _, _, t in txlist)
                    for h, idx, t in txlist:
                        if isinstance(t, dict):
                            self.db.add_tx(tx_from_verbose(t, h, idx))
                        else:
                            # Daemon without verbosity support: fall back to a
                            # per-tx lookup, stubbing whatever it won't hand out.
                            if not self.add_tx_by_txid(h, idx, t):
                                stubs += 1
                        self.counters["tx"] += 1
                self.counters["block"] += len(heights)
                print("height %d" % min(heights))
                if self._stop.is_set():
                    return
            self.db.set_meta("last_sync", int(time.time()))
        if stubs:
            print("note: %d tx(s) stored without inputs/outputs "
                  "(the daemon's getblock had no verbosity argument and "
                  "getrawtransaction could not resolve them)" % stubs)

    def sync_mempool(self):
        try:
            txids = self.rpc.getrawmempool()
        except RPCError as e:
            print("mempool failed: %s" % e)
            # None, not []: the caller must be able to tell "we could not ask"
            # from "the daemon says it is empty". run_once() skips the stale
            # sweep on None, because treating an unanswered getrawmempool as an
            # empty one deleted every pending row and reverted their spends.
            return None
        pending = []
        if txids:
            # Which of these we already track, asked by txid -- the primary key --
            # rather than by listing everything we track and intersecting. The
            # old form pulled every confirmed txid in the index into a Python set
            # once a second to compare against a mempool of a few dozen: 688 ms
            # and 5M strings at half a million txs, growing forever.
            #
            # The status test is applied in Python, not in SQL, so that txid is
            # the query's only constraint and the planner has no choice but the
            # primary key. Put "AND status IN (...)" in the SQL and it prefers
            # idx_txs_status_height, scanning every confirmed row anyway -- the
            # same scan wearing a different hat. With the filter here: 0.6 ms
            # against 500k txs, flat in the size of the index.
            #
            # Known = tracked as confirmed or pending. Orphaned tombstones are
            # NOT included, so a tx re-appearing in the mempool after a reorg is
            # refetched and flips orphaned -> mempool again.
            known = set()
            for i in range(0, len(txids), 500):
                chunk = txids[i:i + 500]
                known.update(r[0] for r in self.db.query(
                    "SELECT txid, status FROM txs WHERE txid IN (%s)"
                    % ",".join("?" * len(chunk)), chunk)
                    if r[1] != "orphaned")
            pending = [txid for txid in txids if txid not in known]
        added = 0
        self.db.prefetch_txs(pending)
        fetch = [("getrawtransaction", (txid, 1)) for txid in pending]
        # Batch in chunks like the chain indexer (one HTTP round-trip per N
        # txs instead of one per tx). If a batch fails (e.g. a tx confirmed
        # and dropped from the mempool mid-fetch), fall back to per-tx calls
        # and skip the stragglers rather than losing the whole batch.
        batches = [fetch[i:i + 500] for i in range(0, len(fetch), 500)]
        with self.db.bulk():  # one DB transaction for all mempool adds
            for chunk in batches:
                try:
                    dicts = self.rpc.batch(chunk)
                except RPCError:
                    dicts = []
                    for _, params in chunk:
                        try:
                            dicts.append(
                                self.rpc.getrawtransaction(params[0], True))
                        except RPCError:
                            dicts.append(None)
                for j in dicts:
                    if not isinstance(j, dict):
                        continue
                    self.db.add_tx(tx_from_verbose(j, None, None))
                    added += 1
        return txids

    def close_stale_mempool(self, txids):
        live = set(txids)
        stale = self.db.query(
            "SELECT txid FROM txs WHERE status='mempool'")
        # One call, one transaction: the spent flags are restored after every
        # delete, so the result does not depend on the order stale txs are
        # removed in (a chain A -> B -> C evicting B and C together).
        self.db.remove_txs(txid for (txid,) in stale if txid not in live)

    def run_once(self):
        self.sync_blocks()
        txids = self.sync_mempool()
        if txids is None:
            # The daemon did not answer. On an RPCError sync_mempool() used to
            # return [] and this sweep read it as "mempool just emptied", being
            # unable to tell a failure from a genuinely empty reply: every
            # pending row was deleted and its spends reverted. Keep the set we
            # already track until we have a real answer.
            return
        self.close_stale_mempool(txids)

    def run(self, interval):
        print("starting indexer at height %d" % self.db.tip_height())
        backoff = interval
        max_backoff = max(interval, 60.0)
        while not self._stop.is_set():
            try:
                self.run_once()
                backoff = interval
            except (RPCError, psycopg.Error) as e:
                print("transient error: %s (retrying in %.1fs)" % (e, backoff))
                if self._stop.wait(backoff):
                    break
                backoff = min(backoff * 2, max_backoff)
                continue
            if self._stop.wait(interval):
                break
        # Graceful exit only happens between transactions (never mid-bulk).
        # Close the connection so WAL checkpoints / locks are released now
        # rather than at process teardown.
        self.db.conn.close()
        print("shutdown: indexer stopped cleanly")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("dsn", nargs="?", default=DEFAULT_DSN)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9554)
    p.add_argument("--rpcuser", default="")
    p.add_argument("--rpcpassword", default="")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--once", action="store_true",
                   help="index once and exit (single-pass catch-up)")
    args = p.parse_args()

    rpc = RPC(args.host, args.port, args.rpcuser, args.rpcpassword)
    # rebuild=True is what makes this process the only one that can throw the
    # database away: if the schema on disk is not the one this code wants, the
    # chain is discarded and re-synced from genesis rather than migrated in
    # place. The web server opens without it and therefore can never trigger
    # that on a live site.
    db = DB.initialize(args.dsn, rebuild=True)
    idx = Indexer(db, rpc)

    # Surface SIGTERM/SIGINT as a clean stop at the next transaction
    # boundary instead of killing the process mid-cycle.
    def _stop_handler(signum, frame):
        print("signal %d received, shutting down..." % signum, flush=True)
        idx._stop.set()

    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)

    if args.once:
        idx.run_once()
        print("done: %d blocks, %d txs (%d without detail)" % (
            idx.counters["block"], idx.counters["tx"], idx.counters["tx_stub"]))
        return 0

    idx.run(args.interval)


if __name__ == "__main__":
    sys.exit(main())