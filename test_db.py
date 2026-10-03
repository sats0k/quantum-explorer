"""Regression tests. Run: python3 -m unittest test_db -v

Needs a PostgreSQL server; point $EXPLORER_TEST_DSN at it (default
"dbname=explorer_test"). Each test gets its own schema inside that
database, so the suite runs against one server and a test's leftovers
cannot collide with the next one's.

The mempool-eviction cases matter because removing a chain of txs (A -> B -> C)
leaves addr_out.spent_by describing whichever mempool the index happened to be
holding when each delete ran. The property under test is that the final state is
the same whatever order the removals happen in, so the ordering test sweeps
every permutation rather than one hand-picked sequence.
"""

import contextlib
import itertools
import json
import os
import queue
import re
import random
import shutil
import socket
import tempfile
import threading
import unittest
from unittest import mock

import psycopg

import db as db_module
import indexer as indexer_module
import server as server_module
from decimal import Decimal
from db import DB, script_hash_of
from indexer import (Block, Indexer, In, Out, Tx, amount_to_pokes,
                     tx_from_verbose)
from rpc import CallError, RPCError
from server import DBPool, Explorer, poke

POKE = 100_000_000

# The server the suite runs against. One database, one schema per test.
TEST_DSN = os.environ.get("EXPLORER_TEST_DSN", "dbname=explorer_test")

# Schema names have to be unique across the whole run, including tests that
# create their own schema by hand, so the counter is module-level.
_schema_seq = itertools.count()


def _with_search_path(dsn, schema):
    """Return `dsn` pinned to `schema`.

    Passed as a libpq `options` parameter rather than a SET the connection has
    to remember to run, so every connection built from the same string -- the
    pool's four, a test's second DB -- lands in the same schema without any of
    them having to do anything.
    """
    if "://" in dsn:                       # a URI DSN
        sep = "&" if "?" in dsn else "?"
        return "%s%soptions=-c%%20search_path%%3D%s" % (dsn, sep, schema)
    return "%s options='-c search_path=%s'" % (dsn, schema)


def _create_schema(name):
    with contextlib.closing(psycopg.connect(TEST_DSN, autocommit=True)) as c:
        c.execute("CREATE SCHEMA " + name)


def _drop_schema(name):
    with contextlib.closing(psycopg.connect(TEST_DSN, autocommit=True)) as c:
        c.execute("DROP SCHEMA IF EXISTS %s CASCADE" % name)


@contextlib.contextmanager
def force_index_use(db):
    """EXPLAIN with sequential scans switched off, for plan assertions.

    The plan tests below ask which index serves a lookup, but their fixtures are
    a row or two, and on a table that small a sequential scan really is cheaper
    than any index -- the planner is right, and asserting an index node would be
    asserting something the server was never asked to choose. Turning seqscan
    off for the duration makes the plan the index-only one the test is about,
    which is what is being checked: that the covering index CAN answer the
    lookup without touching the table, not that it beats a scan on one row.
    """
    db.conn.execute("SET enable_seqscan = off")
    try:
        yield
    finally:
        db.conn.execute("SET enable_seqscan = on")


def indexes_on(db, table):
    """Index names on `table`, sorted. pg_indexes is the catalog listing."""
    return sorted(r[0] for r in db.query(
        "SELECT indexname FROM pg_indexes "
        "WHERE schemaname = current_schema() AND tablename = ?", (table,)))


def tables_in(db):
    """Table names in the connection's current schema."""
    return {r[0] for r in db.query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = current_schema()")}


def columns_of(db, table):
    return [r[0] for r in db.query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = ?", (table,))]


def _schema_dsn(name=None):
    """A fresh schema, dropped when the test ends, and a DSN pointing at it."""
    name = name or "t%d_%d" % (os.getpid(), next(_schema_seq))
    _create_schema(name)
    return name, _with_search_path(TEST_DSN, name)


class _PGTestCase(unittest.TestCase):
    """Base for tests that need a database but not the DB fixture."""

    def add_schema_dsn(self):
        """A DSN for a schema of its own, dropped when the test ends."""
        name, dsn = _schema_dsn()
        self.addCleanup(_drop_schema, name)
        return dsn

    def fresh_schema(self):
        """(name, dsn) for a second schema, for tests needing two."""
        name, dsn = _schema_dsn()
        self.addCleanup(_drop_schema, name)
        return name, dsn


class FakeDaemon:
    """The RPC surface the indexer uses, over a chain of `tip` blocks.

    A block hashes as "b<height>", so `hashes` can be used to make the daemon's
    chain differ from the indexed one at a height both agree on. getblockhash
    past the tip raises, the way a real daemon answers a height it does not have.

    `mempool` is the live set getrawmempool reports; `fetched` records which
    txids sync_mempool actually asked for, so a test can tell "already tracked"
    from "refetched". A txid the daemon does not know raises, as it would.
    """

    def __init__(self, tip, hashes=None, mempool=()):
        self.tip = tip
        self.hashes = hashes or {}
        self.mempool = list(mempool)
        self.fetched = []

    def hash_at(self, height):
        return self.hashes.get(height, "b%04d" % height)

    def getblockcount(self):
        return self.tip

    def getblockhash(self, height):
        height = int(height)
        if not 0 <= height <= self.tip:
            raise RPCError("Block height out of range")
        return self.hash_at(height)

    def getblock(self, blockhash):
        height = int(blockhash[1:])
        return {"height": height, "hash": self.hash_at(height), "tx": [],
                "previousblockhash":
                    self.hash_at(height - 1) if height else None}

    def getrawmempool(self):
        return list(self.mempool)

    def getrawtransaction(self, txid, verbose=True):
        self.fetched.append(txid)
        if txid not in self.mempool:
            raise RPCError("No such mempool transaction: %s" % txid)
        return {"txid": txid, "version": 1, "locktime": 0, "size": 100,
                "vin": [{"txid": "prev", "vout": 0, "sequence": 0xFFFFFFFF}],
                "vout": [{"value": Decimal("1.5"), "n": 0,
                          "scriptPubKey": {
                              "type": "pubkeyhash", "addresses": ["addr1"],
                              "reqSigs": 1, "asm": "OP_DUP", "hex": "76a914"}}]}

    def batch(self, calls, strict=True):
        out = []
        for i, (method, params) in enumerate(calls):
            try:
                out.append(getattr(self, method)(*params))
            except RPCError as e:
                if strict:
                    raise
                out.append(CallError(method, i, message=str(e)))
        return out


def block_at(daemon, height, txids=()):
    return {"height": height, "hash": daemon.hash_at(height),
            "tx": list(txids),
            "previousblockhash":
                daemon.hash_at(height - 1) if height else None}


class BlockDaemon(FakeDaemon):
    """A FakeDaemon whose blocks name their transactions, so sync_blocks has
    something to fetch detail for (FakeDaemon's blocks are empty)."""

    def __init__(self, tip, txids_at, **kw):
        super().__init__(tip, **kw)
        self.txids_at = txids_at

    def getblock(self, blockhash):
        height = int(blockhash[1:])
        return block_at(self, height, self.txids_at.get(height, ()))


def script_hex(tag):
    """A distinct valid P2PKH script per tag."""
    return "76a914" + ("%02x" % tag) * 20 + "88ac"


def tx(txid, height, pays, spends=(), coinbase=False, owners=None, req_sigs=1):
    """Build a Tx paying `pays` = [(address, pokes, tag)] and spending
    `spends` = [(prev_txid, prev_vout)].

    `owners` overrides the single paying address, for multisig outputs, which
    are credited to no one address.
    """
    if coinbase:
        vin = [In(None, None, "00deadbeef", None, None, 0)]
    else:
        vin = [In(t, n, None, None, None, 0xFFFFFFFF) for (t, n) in spends]
    vout = [Out(pokes, "multisig" if owners else "pubkeyhash",
                owners or [address], req_sigs, None, script_hex(tag))
            for (address, pokes, tag) in pays]
    return Tx(txid, height, 0, 1, 0, 100, coinbase, vin, vout)


def full_spent_recompute(db):
    """Ground truth for the spent_by mask, recomputed from vin for the whole
    table. Any write path, whatever order it ran in, must agree with this.

    Recomputed for addr_out and vout both, and per spender status rather than
    as a single boolean, because the mask is what the balance views read and a
    ground truth that only pinned the live reading would let a mempool-only
    spender through unnoticed.
    """
    for table in ("addr_out", "vout"):
        db.conn.execute("UPDATE %s SET spent_by=0" % table)
        db.conn.execute(
            "UPDATE %s SET spent_by = (CASE WHEN EXISTS ("
            "  SELECT 1 FROM vin JOIN txs ON txs.txid = vin.txid"
            "  WHERE vin.prev_txid = %s.txid AND vin.prev_vout = %s.n"
            "    AND txs.status = 'confirmed') THEN 1 ELSE 0 END)"
            " | (CASE WHEN EXISTS ("
            "  SELECT 1 FROM vin JOIN txs ON txs.txid = vin.txid"
            "  WHERE vin.prev_txid = %s.txid AND vin.prev_vout = %s.n"
            "    AND txs.status = 'mempool') THEN 2 ELSE 0 END)"
            % (table, table, table, table, table))


class DBTestCase(_PGTestCase):
    def setUp(self):
        self.schema, self.db_path = self.fresh_schema()
        self.db = self.fresh_db(self.db_path)
        self.indexer = Indexer(self.db, rpc=None)
        self.explorer = Explorer(self.db)

    def fresh_db(self, dsn=None):
        """A DB of its own, for tests that need more than one."""
        if dsn is None:
            dsn = self.add_schema_dsn()
        db = DB(dsn)
        self.addCleanup(db.conn.close)
        return db

    def reopened(self, rebuild):
        """A second connection to this test's schema, as a cold start opens it.

        `rebuild` is passed straight through, because whether a reopen may
        discard the chain is the thing the rebuild tests vary.
        """
        db = DB.initialize(self.db_path, rebuild=rebuild)
        self.addCleanup(db.conn.close)
        return db

    def derived(self):
        """The total as the defining query computes it, ignoring the counter."""
        return self.db.conn.execute(
            db_module.TOTAL_COINBASE_SQL).fetchone()[0]

    def assertMatchesDerived(self, msg=""):
        """Every maintained counter must equal the query that defines it."""
        for key, sql in db_module.STATS:
            self.assertEqual(self.db._stat(key),
                             self.db.conn.execute(sql).fetchone()[0],
                             "%s: %s" % (key, msg))


    def spent_by(self, txid, n=0):
        """The spent_by mask of an output, or None when the output is gone.

        The raw mask, not a boolean: the tests below assert on the bit that
        distinguishes a mempool spender from a confirmed one, which is the
        distinction the two balance views turn on.
        """
        rows = self.db.query(
            "SELECT spent_by FROM addr_out WHERE txid=? AND n=?", (txid, n))
        return None if not rows else rows[0][0]

    def is_spent(self, txid, n=0):
        """Whether any known tx spends an output, or None when it is gone.

        The live reading, which is what this name always meant here: bit 0 of
        the mask, set by either a confirmed or a mempool spender.
        """
        mask = self.spent_by(txid, n)
        return None if mask is None else bool(mask & 3)

    def vout_spent_by(self, txid, n=0):
        """The same mask on vout, which the script balances read instead."""
        rows = self.db.query(
            "SELECT spent_by FROM vout WHERE txid=? AND n=?", (txid, n))
        return None if not rows else rows[0][0]

    def spent_flags(self):
        return dict(self.db.query("SELECT txid, spent_by FROM addr_out"))

    def rows_for(self, table, txid):
        return self.db.query(
            "SELECT COUNT(*) FROM %s WHERE txid=?" % table, (txid,))[0][0]

    def build_chain(self, length):
        """A -> B -> C -> ... all in the mempool, each paying one address."""
        names = "ABCDEFGH"[:length]
        self.db.add_tx(tx(names[0], None, [("addr0", POKE, 1)], coinbase=True))
        for i, name in enumerate(names[1:]):
            self.db.add_tx(tx(name, None, [("addr%d" % (i + 1), POKE, i + 2)],
                              spends=[(names[i], 0)]))
        return names


class MempoolEvictionTest(DBTestCase):
    """close_stale_mempool() drops every mempool tx absent from the live set."""

    def test_parent_only_removed(self):
        self.build_chain(3)
        self.indexer.close_stale_mempool(["A", "C"])  # B falls out
        self.assertEqual(self.is_spent("A"), False)
        self.assertIsNone(self.is_spent("B"), "B's output went with B")
        self.assertEqual(self.is_spent("C"), False)

    def test_child_only_removed(self):
        self.build_chain(3)
        self.indexer.close_stale_mempool(["A", "B"])  # C falls out
        self.assertEqual(self.is_spent("A"), True, "B still spends A:0")
        self.assertEqual(self.is_spent("B"), False,
                         "C was the only spender of B:0")
        self.assertIsNone(self.is_spent("C"))

    def test_parent_and_child_removed(self):
        names = self.build_chain(3)
        self.indexer.close_stale_mempool(["A"])  # B and C fall out
        self.assertEqual(self.is_spent("A"), False)
        for gone in names[1:]:
            self.assertIsNone(self.is_spent(gone))
            for table in ("txs", "vin", "vout", "addr_out"):
                self.assertEqual(self.rows_for(table, gone), 0,
                                 "%s still has %s rows" % (gone, table))

    def test_three_level_chain_removed(self):
        self.build_chain(4)
        self.indexer.close_stale_mempool(["A"])  # B, C and D fall out
        self.assertEqual(self.is_spent("A"), False)
        for gone in ("B", "C", "D"):
            self.assertIsNone(self.is_spent(gone))

    def test_removal_order_does_not_matter(self):
        for order in itertools.permutations(["B", "C", "D"]):
            with self.subTest(order=order):
                db = self.fresh_db()
                tx_a = tx("A", None, [("addr0", POKE, 1)], coinbase=True)
                db.add_tx(tx_a)
                for i, name in enumerate(["B", "C", "D"]):
                    db.add_tx(tx(name, None,
                                 [("addr%d" % (i + 1), POKE, i + 2)],
                                 spends=[("ABC"[i], 0)]))
                db.remove_txs(order)
                self.assertEqual(
                    dict(db.query("SELECT txid, spent_by FROM addr_out")),
                    {"A": 0})

    def test_shared_output_with_two_spenders(self):
        """Two mempool txs spending one output: an RBF conflict."""
        self.db.add_tx(tx("A", None, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("B", None, [("addr1", POKE, 2)], spends=[("A", 0)]))
        self.db.add_tx(tx("C", None, [("addr2", POKE, 3)], spends=[("A", 0)]))
        self.assertEqual(self.is_spent("A"), True)

        self.db.remove_tx("B")
        self.assertEqual(self.is_spent("A"), True, "C still spends A:0")

        self.db.remove_tx("C")
        self.assertEqual(self.is_spent("A"), False, "no spender is left")

    def test_shared_output_removed_together(self):
        self.db.add_tx(tx("A", None, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("B", None, [("addr1", POKE, 2)], spends=[("A", 0)]))
        self.db.add_tx(tx("C", None, [("addr2", POKE, 3)], spends=[("A", 0)]))
        self.db.remove_txs(["B", "C"])
        self.assertEqual(self.is_spent("A"), False)

    def test_removing_unknown_or_empty_is_a_no_op(self):
        self.build_chain(2)
        self.db.remove_txs(["nope", "B"])
        self.assertEqual(self.spent_flags(), {"A": 0})
        self.db.remove_txs([])
        self.assertEqual(self.spent_flags(), {"A": 0})

    def test_mempool_spend_of_a_confirmed_output(self):
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("B", None, [("addr1", POKE, 2)], spends=[("A", 0)]))
        self.assertEqual(self.is_spent("A"), True)
        self.db.remove_tx("B")
        self.assertEqual(self.is_spent("A"), False)


class VariableLimitTest(DBTestCase):
    """Id lists longer than one statement's bind cap must not fail.

    Postgres allows 65535 parameters per statement and a statement over it
    raises rather than degrading, which would roll back the caller's whole
    transaction: a permanently stuck indexer, a mempool too big to evict or a
    reorg too deep to apply, retried forever. Reaching 65535 to test that
    would take minutes, so the cap is simulated the way it bites -- by making
    the chunk size small, which is the same code path a real overrun takes.
    """

    def cap_variables(self, n):
        # addCleanup takes stop, not the patcher. Handing it the patcher calls
        # __enter__ again, which re-patches rather than undoing, and the cap
        # would outlive the test and shrink the chunk for every test after it.
        # SQLite's setlimit was per-connection so it could not leak like this;
        # the chunk size is module-level here, so it has to be undone.
        patcher = mock.patch.object(db_module, "SQL_VAR_CHUNK", n)
        patcher.start()
        self.addCleanup(patcher.stop)

    def fill_mempool(self, n):
        """n mempool txs, all one address, none spending anything."""
        with self.db.bulk():
            for i in range(n):
                self.db.add_tx(tx("M%05d" % i, None,
                                  [("addr%d" % (i % 97), POKE, i + 1)],
                                  coinbase=True))
        return ["M%05d" % i for i in range(n)]

    def counts(self, db=None):
        db = db or self.db
        return [db.query("SELECT COUNT(*) FROM " + t)[0][0]
                for t in ("txs", "vin", "vout", "addr_out", "scripts")]

    def test_an_eviction_past_the_bind_cap_works(self):
        # Two chunks' worth of stale txs, a cap of one chunk: the IN lists in
        # _remove_txs and the rebuild scope behind them both need the walk.
        stale = self.fill_mempool(db_module.SQL_VAR_CHUNK * 2)
        self.cap_variables(db_module.SQL_VAR_CHUNK)
        self.indexer.close_stale_mempool([])      # every tx is stale
        self.assertEqual(self.counts(), [0, 0, 0, 0, 0])

    def test_the_eviction_state_is_the_same_chunked_or_not(self):
        # The point of chunking is that it changes nothing but the statement
        # sizes, so check the capped result against the uncapped one.
        stale = self.fill_mempool(db_module.SQL_VAR_CHUNK * 2)
        self.cap_variables(db_module.SQL_VAR_CHUNK)
        self.indexer.close_stale_mempool([])
        chunked = self.counts()
        self.assertEqual(chunked, [0, 0, 0, 0, 0],
                         "the capped eviction should have left nothing")

        db = self.fresh_db()
        with db.bulk():
            for i in range(len(stale)):
                db.add_tx(tx(stale[i], None,
                             [("addr%d" % (i % 97), POKE, i + 1)], coinbase=True))
        db.remove_txs(stale)                      # host cap: one statement
        self.assertEqual(self.counts(db), chunked)

    def test_a_chunked_eviction_is_still_one_transaction(self):
        # Chunking inside the transaction must not make a failure partial: the
        # spent flags and the scripts are only consistent because the whole
        # eviction either lands or does not.
        names = self.build_chain(3)
        self.cap_variables(2)                     # far below one chunk
        with mock.patch.object(DB, "_refresh_spent_flags",
                               side_effect=psycopg.OperationalError("boom")):
            with self.assertRaises(psycopg.OperationalError):
                self.db.remove_txs(list(names))
        self.assertEqual(self.counts(), [3, 3, 3, 3, 3], "half-evicted")
        # Bit 2, not bit 1: the chain is all mempool, so A and B are spent by an
        # unconfirmed tx and are live-spent but confirmed-unspent. The rollback
        # is what is under test; the mask values say the flags were not left
        # half-written by the interrupted refresh.
        self.assertEqual(self.spent_flags(), {"A": 2, "B": 2, "C": 0})

    def test_an_eviction_does_not_cost_a_statement_per_stale_tx(self):
        # These txs spend nothing, so the spent-flag recompute has no work and
        # every statement here is one of the batched ones. (An eviction whose
        # txs do spend pays one UPDATE per output spent, which is inherent.)
        stale = self.fill_mempool(1000)
        seen = []
        real = self.db.conn.execute
        self.db.conn.execute = lambda sql, params=(): (
            seen.append(sql), real(sql, params))[1]
        self.db.remove_txs(stale)
        dml = [s for s in seen if s.lstrip().upper().startswith(
            ("SELECT", "DELETE", "INSERT", "UPDATE"))]
        self.assertLessEqual(len(dml), 20,
                             "%d statements for 1000 stale txs: the chunking"
                             " is not being used" % len(dml))

    def test_a_reorg_deeper_than_the_bind_cap_works(self):
        # clear_from(0) orphans the chain, so the scoped rebuild is handed every
        # script in it -- the same unbounded list, reached from the other side.
        n = db_module.SQL_VAR_CHUNK * 2
        with self.db.bulk():
            for i in range(n):
                self.db.add_tx(tx("T%05d" % i, i,
                                  [("addr%d" % i, POKE, i + 1)], coinbase=True))
        self.cap_variables(db_module.SQL_VAR_CHUNK)
        self.db.clear_from(0)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM blocks")[0][0], 0)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM scripts")[0][0], 0)
        self.assertEqual(
            self.db.query("SELECT COUNT(*) FROM vout")[0][0], 0,
            "vout rows for the orphaned txs are severed")

    def test_a_scoped_rebuild_matches_a_full_one_past_the_cap(self):
        hashes = [script_hash_of(script_hex(s)) for s in range(
            db_module.SQL_VAR_CHUNK * 2)]
        for s in range(len(hashes)):
            self.db.add_tx(tx("T%05d" % s, 100 + s,
                              [("addr%d" % s, POKE, s + 1)], coinbase=True))
        self.cap_variables(db_module.SQL_VAR_CHUNK)
        self.db.rebuild_scripts(hashes)
        scoped = self.db.query(
            "SELECT script_hash, type, req_sigs, addresses, created_height,"
            " last_height FROM scripts ORDER BY script_hash")
        self.db.rebuild_scripts()               # full table, no IN list
        self.assertEqual(
            scoped,
            self.db.query("SELECT script_hash, type, req_sigs, addresses,"
                          " created_height, last_height FROM scripts"
                          " ORDER BY script_hash"))
        self.assertEqual(len(scoped), len(hashes))


class MultisigCreditTest(DBTestCase):
    """A multi-address output is credited to the script, not to participants.

    This is a deliberate accounting decision, and the tests below pin both
    halves of it: participants get no addr_out row (so the value is counted
    once, not N times), and the script endpoint still reports the full amount
    along with the participants. /api/address 404s for a participant, which the
    README now documents as expected behaviour rather than a missing lookup.
    """

    def add_multisig(self, owners=("A", "B"), pokes=100 * POKE, req_sigs=2,
                     tag=3, txid="MS"):
        self.db.add_tx(tx(txid, 100, [("_", pokes, tag)], owners=list(owners),
                          req_sigs=req_sigs))
        return script_hash_of(script_hex(tag))

    def test_a_multisig_output_creates_no_addr_out_rows(self):
        self.add_multisig()
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM addr_out")[0][0], 0)

    def test_participant_addresses_return_nothing(self):
        self.add_multisig()
        # 404 at the API layer, which is the documented outcome: the explorer
        # knows the address, it just will not credit it.
        self.assertIsNone(self.explorer.address("A"))
        self.assertIsNone(self.explorer.address("B"))

    def test_the_value_is_counted_once_not_once_per_participant(self):
        h = self.add_multisig()
        credited = sum(self.explorer.address(a)["confirmed"]["balance"]
                       for a in ("A", "B")
                       if self.explorer.address(a))
        self.assertEqual(credited, 0)
        self.assertEqual(self.explorer.script(h)["confirmed"]["value_received"],
                         100 * POKE)

    def test_the_script_endpoint_still_lists_the_participants(self):
        h = self.add_multisig()
        s = self.explorer.script(h)
        self.assertEqual(s["addresses"], ["A", "B"])
        self.assertEqual(s["req_sigs"], 2)
        self.assertEqual(s["type"], "multisig")
        self.assertEqual(s["confirmed"]["balance"], 100 * POKE)

    def test_a_three_way_multisig_is_still_counted_once(self):
        h = self.add_multisig(owners=("A", "B", "C"), req_sigs=3)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM addr_out")[0][0], 0)
        self.assertEqual(self.explorer.script(h)["confirmed"]["value_received"],
                         100 * POKE)

    def test_a_single_address_output_is_credited_normally(self):
        # The exclusion is scoped to multi-address vouts, not to all of them.
        self.db.add_tx(tx("S", 100, [("solo", 100 * POKE, 1)], coinbase=True))
        a = self.explorer.address("solo")
        self.assertIsNotNone(a)
        self.assertEqual(a["confirmed"]["balance"], 100 * POKE)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM addr_out")[0][0], 1)

    def test_mixed_outputs_do_not_bleed_into_each_other(self):
        # One 100 PXC multisig and one 40 PXC single-address tx: the address
        # sees only its own 40, never any share of the multisig.
        self.add_multisig(owners=("A", "B"), pokes=100 * POKE, tag=3)
        self.db.add_tx(tx("S", 100, [("A", 40 * POKE, 1)], coinbase=True))
        self.assertEqual(self.explorer.address("A")["confirmed"]["balance"],
                         40 * POKE)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM addr_out")[0][0], 1)

    def test_txs_list_covers_both_the_receipt_and_the_spend(self):
        # The list used to be built from addr_out alone, so an address whose
        # every output was spent had its tx history end at the last payout; the
        # transaction moving its coins back out never appeared. Both sides must
        # be listed, and a tx counted once even when it both receives and
        # spends (the set union dedupes).
        self.db.add_tx(tx("PAY", 100, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("SPEND", None, [], spends=[("PAY", 0)]))
        self.db.add_tx(tx("CYCLE", None, [("addr0", POKE, 19)],
                          spends=[("PAY", 0)]))
        a = self.explorer.address("addr0")
        self.assertEqual(a["n_txs"], 3)
        self.assertFalse(a["txs_truncated"])
        self.assertIn("PAY", a["txs"])
        self.assertIn("SPEND", a["txs"])
        self.assertIn("CYCLE", a["txs"])


class ScriptBalanceTest(DBTestCase):
    def add_pair(self, b_height=None):
        """A confirmed 1.00 receive, plus a tx spending it at `b_height`."""
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("B", b_height, [("addr1", 9 * POKE // 10, 2)],
                          spends=[("A", 0)]))

    def balances(self, tag=1):
        return self.explorer.script(script_hash_of(script_hex(tag)))

    def test_unconfirmed_spend_leaves_the_confirmed_balance(self):
        self.add_pair()
        b = self.balances()
        self.assertEqual(b["confirmed"]["balance"], POKE)
        self.assertEqual(b["confirmed"]["value_spent"], 0)
        self.assertEqual(b["live"]["balance"], 0)

    def test_conflicting_spend_never_goes_negative(self):
        """A mempool conflict spends one output twice; it must count once."""
        self.add_pair()
        self.db.add_tx(tx("C", None, [("addr2", 9 * POKE // 10, 3)],
                          spends=[("A", 0)]))
        b = self.balances()
        self.assertEqual(b["live"]["value_spent"], POKE)
        self.assertEqual(b["live"]["n_spent"], 1)
        self.assertEqual(b["live"]["balance"], 0)
        self.assertGreaterEqual(b["live"]["balance"], 0)

    def test_eviction_restores_the_balance_exactly(self):
        self.add_pair()
        self.indexer.close_stale_mempool([])
        b = self.balances()
        self.assertEqual(b["live"]["balance"], POKE)
        self.assertEqual(b["confirmed"]["balance"], POKE)
        self.assertEqual(self.is_spent("A"), False)

    def test_reindexing_a_spend_does_not_double_count(self):
        self.add_pair(b_height=200)
        self.add_pair(b_height=200)  # same tx seen again
        b = self.balances()
        self.assertEqual(b["confirmed"]["value_received"], POKE)
        self.assertEqual(b["confirmed"]["value_spent"], POKE)
        self.assertEqual(b["confirmed"]["n_spent"], 1)
        self.assertEqual(b["confirmed"]["balance"], 0)

    def test_a_reindex_replaces_the_row_in_every_table(self):
        # _add_tx drops the previous version through the same helper an eviction
        # uses, so a tx seen again leaves one row per table, not one per sighting.
        for height in (200, 250, 300):
            self.db.add_tx(tx("A", height,
                              [("addr0", POKE, 1), ("addr1", POKE, 2)],
                              coinbase=True))
        self.assertEqual(self.db.query("SELECT height FROM txs")[0][0], 300)
        for table, want in (("txs", 1), ("vin", 1), ("vout", 2), ("addr_out", 2)):
            self.assertEqual(
                self.db.query("SELECT COUNT(*) FROM " + table)[0][0], want,
                "%s kept a row from a previous version of A" % table)

    def test_mempool_only_script_has_no_height(self):
        self.db.add_tx(tx("A", None, [("addr0", POKE, 1)], coinbase=True))
        b = self.balances()
        self.assertIsNone(b["created_height"])
        self.assertIsNone(b["last_height"])
        self.assertEqual(b["confirmed"]["balance"], 0)
        self.assertEqual(b["live"]["balance"], POKE)

    def test_height_appears_on_confirmation(self):
        self.db.add_tx(tx("A", None, [("addr0", POKE, 1)], coinbase=True))
        self.assertIsNone(self.balances()["created_height"])
        self.db.add_tx(tx("A", 300, [("addr0", POKE, 1)], coinbase=True))
        self.assertEqual(self.balances()["created_height"], 300)

    def test_reorg_restores_the_balance(self):
        self.add_pair(b_height=200)
        self.assertEqual(self.balances()["live"]["balance"], 0)
        self.db.clear_from(200)
        self.assertEqual(self.balances()["live"]["balance"], POKE)
        self.assertEqual(self.balances()["live"]["value_spent"], 0)

    def test_multisig_is_only_visible_on_the_script(self):
        owners = ["M1", "M2", "M3"]
        self.db.add_tx(tx("MS", 300, [(None, 5 * POKE, 4)], coinbase=True,
                          owners=owners, req_sigs=2))
        b = self.balances(tag=4)
        self.assertEqual(b["confirmed"]["balance"], 5 * POKE)
        self.assertEqual(b["addresses"], owners)
        for owner in owners:
            self.assertIsNone(
                self.explorer.address(owner),
                "a multisig output belongs to no single address")

    def test_multisig_mempool_spend_only_moves_the_live_balance(self):
        owners = ["M1", "M2", "M3"]
        self.db.add_tx(tx("MS", 300, [(None, 5 * POKE, 4)], coinbase=True,
                          owners=owners, req_sigs=2))
        self.db.add_tx(tx("MS2", None, [(None, 4 * POKE, 4)],
                          spends=[("MS", 0)], owners=owners, req_sigs=2))
        b = self.balances(tag=4)
        self.assertEqual(b["confirmed"]["balance"], 5 * POKE)
        self.assertEqual(b["live"]["balance"], 4 * POKE)

    def test_address_splits_the_same_way(self):
        self.add_pair()
        a = self.explorer.address("addr0")
        self.assertEqual(a["confirmed"]["balance"], POKE)
        self.assertEqual(a["live"]["balance"], 0)
        self.assertTrue(a["outputs"][0]["spent"])
        self.assertFalse(a["outputs"][0]["spent_confirmed"])
        self.indexer.close_stale_mempool([])
        self.assertFalse(self.explorer.address("addr0")["outputs"][0]["spent"])


class SpentMaskTest(DBTestCase):
    """The spent_by mask, and the balances that are read from it.

    spent_by replaced a boolean because the two balance views disagree about
    the *spender* as well as the owner: an output spent only by a mempool tx is
    confirmed-unspent, since a mempool spend can still evaporate. So the mask
    is what a boolean could not express, and these tests pin each of its four
    values plus the balances they produce.
    """

    def add_receiver(self, txid="A", height=100):
        self.db.add_tx(tx(txid, height, [("addr0", POKE, 1)], coinbase=True))

    def add_spender(self, txid, spends=("A", 0), height=None):
        self.db.add_tx(tx(txid, height, [("addr1", POKE // 2, 2)],
                          spends=[spends]))

    def test_each_spender_status_sets_its_own_bit(self):
        self.add_receiver()
        self.assertEqual(self.spent_by("A"), 0, "unspent")
        self.add_spender("M", height=None)
        self.assertEqual(self.spent_by("A"), 2, "mempool spender only")
        self.add_spender("C", height=101)
        self.assertEqual(self.spent_by("A"), 3, "both spenders")
        self.db.remove_tx("C")
        self.assertEqual(self.spent_by("A"), 2, "the mempool one remains")
        self.db.remove_tx("M")
        self.assertEqual(self.spent_by("A"), 0, "no spender left")

    def test_a_confirmed_spender_alone_sets_the_confirmed_bit(self):
        self.add_receiver()
        self.add_spender("C", height=101)
        self.assertEqual(self.spent_by("A"), 1)

    def test_the_mask_is_kept_on_vout_as_well(self):
        # vout is the only place a multisig's answer exists, so a mask on
        # addr_out alone would leave script_balances re-deriving the fact.
        self.add_receiver()
        self.add_spender("C", height=101)
        self.assertEqual(self.vout_spent_by("A"), 1)
        self.assertEqual(self.vout_spent_by("A"), self.spent_by("A"))

    def test_the_owner_bit_is_set_while_unconfirmed_and_cleared_on_confirm(self):
        # The bit is written once at insert, because an orphan's rows are
        # deleted rather than flagged -- so a row's owner cannot go from
        # mempool to confirmed without the row being rebuilt.
        self.add_receiver("M", height=None)
        self.assertEqual(
            self.db.query("SELECT mempool FROM addr_out WHERE txid='M'")[0][0], 1)
        self.add_receiver("M", height=100)          # same txid, now in a block
        self.assertEqual(
            self.db.query("SELECT mempool FROM addr_out WHERE txid='M'")[0][0], 0)

    def test_the_masks_survive_a_reorg(self):
        self.add_receiver("A", 100)
        self.add_spender("B", height=101)
        self.assertEqual(self.spent_by("A"), 1)
        self.db.clear_from(101)                    # drops B, orphaning nothing
        self.assertEqual(self.spent_by("A"), 0, "the only spencer was orphaned")


class BalanceParityTest(DBTestCase):
    """The new balance queries must agree with the SQL they replaced.

    The old implementation joined txs for the owner and ran a correlated EXISTS
    into vin for the spender, per output, per request -- 41s for the busiest
    address. It is kept here verbatim as ground truth, so the denormalized
    columns are checked against the derivation they exist to avoid rather than
    against hand-written expectations that would agree with a wrong mask too.
    """

    def legacy_balances(self, table, key_col, key, count_col):
        """The pre-mask implementation, unchanged, as the oracle."""
        out = {}
        for name, status in (("confirmed", "('confirmed')"),
                             ("live", "('confirmed','mempool')")):
            received, n = self.db.conn.execute(
                "SELECT COALESCE(SUM(v.value),0), COUNT(*) FROM %s v "
                "JOIN txs t ON t.txid = v.txid "
                "WHERE v.%s=? AND t.status IN %s" % (table, key_col, status),
                (key,)).fetchone()
            spent, n_spent = self.db.conn.execute(
                "SELECT COALESCE(SUM(v.value),0), COUNT(*) FROM %s v "
                "JOIN txs t ON t.txid = v.txid "
                "WHERE v.%s=? AND t.status IN %s "
                "AND EXISTS (SELECT 1 FROM vin i JOIN txs ti ON ti.txid=i.txid "
                "WHERE i.prev_txid=v.txid AND i.prev_vout=v.n "
                "AND ti.status IN %s)" % (table, key_col, status, status),
                (key,)).fetchone()
            out[name] = {"value_received": received, count_col: n,
                         "value_spent": spent, "n_spent": n_spent,
                         "balance": received - spent}
        return out

    def build_mixed_chain(self):
        """Every combination the two views disagree about, in one chain.

        A confirmed output spent by nothing, by a confirmed tx, by a mempool tx
        and by both; a mempool output spent by a confirmed tx and by a mempool
        one; and an unconfirmed output of each, which is what the owner bit
        exists to separate.
        """
        self.db.add_tx(tx("C1", 100, [("a", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("C2", 100, [("a", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("C3", 100, [("a", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("C4", 100, [("a", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("M1", None, [("a", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("M2", None, [("a", POKE, 1)], coinbase=True))
        # C1 spent by a confirmed tx only.
        self.db.add_tx(tx("S1", 101, [("z", POKE, 9)], spends=[("C1", 0)]))
        # C2 spent by a mempool tx only: live-spent, confirmed-unspent.
        self.db.add_tx(tx("S2", None, [("z", POKE, 9)], spends=[("C2", 0)]))
        # C3 spent by both.
        self.db.add_tx(tx("S3", 101, [("z", POKE, 9)], spends=[("C3", 0)]))
        self.db.add_tx(tx("S4", None, [("z", POKE, 9)], spends=[("C3", 0)]))
        # C4 and M2 unspent.
        # A confirmed tx spending a mempool output: the output is not received
        # in the confirmed view, so it must not be spent in it either.
        self.db.add_tx(tx("S5", 102, [("z", POKE, 9)], spends=[("M1", 0)]))
        # A mempool tx spending a mempool output.
        self.db.add_tx(tx("S6", None, [("z", POKE, 9)], spends=[("M2", 0)]))

    def test_address_balances_match_the_legacy_queries(self):
        self.build_mixed_chain()
        self.assertEqual(
            self.db.address_balances("a"),
            self.legacy_balances("addr_out", "address", "a", "n_outputs"))

    def test_script_balances_match_the_legacy_queries(self):
        self.build_mixed_chain()
        sh = script_hash_of(script_hex(1))
        self.assertEqual(
            self.db.script_balances(sh),
            self.legacy_balances("vout", "script_hash", sh, "n_vout"))

    def test_the_views_really_do_differ(self):
        # Guards the parity tests above from passing vacuously: if the two views
        # had collapsed into one, they would agree with each other and with the
        # legacy SQL no matter what the mask said.
        self.build_mixed_chain()
        bal = self.db.address_balances("a")
        self.assertNotEqual(bal["confirmed"], bal["live"])
        self.assertEqual(bal["confirmed"]["value_spent"], 2 * POKE,
                         "C1 and C3, not C2 (mempool-only spender)")
        self.assertEqual(bal["live"]["value_spent"], 5 * POKE,
                         "C1, C2, C3, M1 and M2")
        self.assertEqual(bal["confirmed"]["value_received"], 4 * POKE)
        self.assertEqual(bal["live"]["value_received"], 6 * POKE)

    def test_a_reorg_returns_both_views_to_the_legacy_answers(self):
        self.build_mixed_chain()
        self.db.clear_from(102)
        self.assertEqual(
            self.db.address_balances("a"),
            self.legacy_balances("addr_out", "address", "a", "n_outputs"))



class SchemaShapeTest(DBTestCase):
    """What a fresh database looks like, as a property of SCHEMA alone.

    There is no migration path to reconcile a fresh database against, so this
    is the only definition of the shape there is -- and the two properties
    below are what makes that safe to lean on: the bulk loader's index list is
    derived from the same DDL that created the tables, and the tables are
    named in exactly one place.
    """

    def test_the_tables_are_exactly_those_the_schema_declares(self):
        present = sorted(r[0] for r in self.db.conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname=current_schema()"))
        self.assertEqual(present, ["addr_out", "blocks", "meta", "scripts",
                                   "txs", "vin", "vout"])

    def test_all_indexes_is_the_set_the_schema_creates(self):
        # The bulk loader drops ALL_INDEXES before a COPY and puts them back
        # after, so a list that drifts from the DDL is a reorg path that comes
        # back unindexed. It drifted once and nothing noticed; deriving both
        # from SCHEMA is the fix, and this is the assertion that keeps them
        # derived.
        declared = [s for s in db_module.SCHEMA
                    if s.lstrip().upper().startswith("CREATE INDEX")]
        self.assertEqual(sorted(db_module.ALL_INDEXES), sorted(declared))

    def test_neither_redundant_index_is_created(self):
        # addr_out's key is (address, txid, n), so the address prefix is already
        # seekable and a second index on it stores that column twice. vout's
        # addresses is a JSON array no query filters on. Neither is in SCHEMA,
        # so neither is ever created -- there is no build that could have them.
        self.assertEqual(indexes_on(self.db, "addr_out"),
                         ["addr_out_pkey", "idx_addr_out_addr",
                          "idx_addr_out_txid_n"])
        # idx_vout_script supersedes idx_vout_script_hash: it leads with the same
        # column, so it answers everything the plain one did and the balance
        # query besides, and keeping both would double the write cost of every
        # output for no plan that the covering one cannot serve.
        self.assertEqual(indexes_on(self.db, "vout"),
                         ["idx_vout_script", "vout_pkey"])

    def test_the_balance_lookup_is_served_by_the_covering_index(self):
        # Why the covering index leads with value and not just address: the
        # balance query sums value over every output of one address, and without
        # value in the index each of those rows costs a lookup into the table to
        # fetch it. Checked as a permanent property rather than a one-off plan,
        # because the whole point of the index is the "COVERING" in the plan
        # line -- a plain address index would be a seek, and a seek is what this
        # replaced.
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        # VACUUM first: an Index Only Scan also needs the page marked
        # all-visible in the visibility map, and only VACUUM sets that. Without
        # it Postgres uses a Bitmap Heap Scan, which is correct but still does a
        # row fetch per match -- the opposite of what this asserts.
        # Autocommit makes the VACUUM legal, since it cannot run in a
        # transaction block.
        self.db.conn.execute("VACUUM")
        with force_index_use(self.db):
            plans = [r[-1] for r in self.db.conn.execute(
                "EXPLAIN SELECT COALESCE(SUM(value),0) FROM addr_out"
                " WHERE address = ?", ("addr0",))]
            self.assertTrue(any("Index Only Scan using idx_addr_out_addr" in p
                                for p in plans), plans)
            self.assertFalse(any("idx_addr_out_address" in p for p in plans),
                             plans)
            # Same for the script side, and for the spent half, which used to be
            # the expensive one: it must not mention vin or txs at all now.
            spent_plans = [r[-1] for r in self.db.conn.execute(
                "EXPLAIN SELECT COALESCE(SUM(value),0) FROM addr_out"
                " WHERE address = ? AND (spent_by & 1) <> 0", ("addr0",))]
        self.assertTrue(any("Index Only Scan using idx_addr_out_addr" in p
                            for p in spent_plans), spent_plans)
        self.assertFalse(any(" txs" in p or " vin" in p for p in spent_plans),
                         spent_plans)

    def test_a_freshly_built_table_has_nothing_to_backfill(self):
        # The property the rebuild policy rests on. Adding a column used to
        # mean ALTER TABLE ... ADD COLUMN with a default and a separate
        # backfill; between the two, spent_by read 0 -- "unspent" -- for every
        # output, so the balance queries reported the entire supply to every
        # address. There is no window now: a new column is declared in SCHEMA,
        # and every row in the table was written by code that knew about it.
        cols = {r[0] for r in self.db.conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema=current_schema() AND table_name='vout'")}
        self.assertEqual(cols, {
            "txid", "n", "value", "type", "addresses", "req_sigs",
            "script_asm", "script_hex", "script_hash", "mempool", "spent_by"})

    def test_each_output_is_hashed_once_on_the_write_path(self):
        # script_hash_of is SHA256+RIPEMD160, so hashing the same script twice
        # is the difference between one hash and two per output across a whole
        # chain. The backfill that used to be able to get this wrong is gone;
        # this pins the write path, which is the only place it happens now.
        real = db_module.script_hash_of
        calls = []

        def counting(hx):
            calls.append(hx)
            return real(hx)

        tags = (0x11, 0x22, 0x33, 0x44)
        with mock.patch.object(db_module, "script_hash_of", counting):
            for i, tag in enumerate(tags):
                self.db.add_tx(tx(chr(ord("A") + i), 100 + i,
                                  [("addr%d" % i, POKE, tag)], coinbase=True))
        # Once per output, and no script hashed twice: script_hex gives each tag
        # its own script, so four outputs are four distinct scripts.
        self.assertEqual(sorted(calls),
                         sorted(script_hex(t) for t in tags))
        self.assertEqual(len(calls), len(set(calls)))
        for hx in set(calls):
            self.assertIn(
                (real(hx),), self.db.query(
                    "SELECT script_hash FROM vout WHERE script_hex=?", (hx,)))


class RebuildTest(DBTestCase):
    """A shape change discards the chain instead of being migrated in place.

    The policy is DB._rebuild, and the property it has to hold is that it never
    half-applies. What it replaced could: a migration that added a column with
    a default and left it unbackfilled produced a database that read as correct
    and answered every balance with the whole supply. A rebuild cannot reach
    that state, because it either keeps a database whose shape is already right
    or empties the database entirely.
    """

    def _index_three(self):
        from indexer import Block
        for i, (addr, val) in enumerate((("addr0", 1), ("addr1", 2),
                                        ("addr2", 3))):
            self.db.add_block(Block({"height": 100 + i,
                                     "hash": "b%d" % (100 + i),
                                     "time": 1, "tx": []}))
            self.db.add_tx(tx(chr(ord("A") + i), 100 + i, [(addr, POKE, val)],
                              coinbase=True))
        self.db.conn.commit()

    def _set_fingerprint(self, value):
        if value is None:
            self.db.conn.execute(
                "DELETE FROM meta WHERE key=?", (db_module.FINGERPRINT_KEY,))
        else:
            self.db.conn.execute(
                "INSERT INTO meta(key, value) VALUES(?,?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (db_module.FINGERPRINT_KEY, value))
        self.db.conn.commit()
        self.db.conn.close()

    def _row_counts(self, db):
        return {t: db.conn.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0]
                for t in db_module.SCHEMA_TABLES if t != "meta"}

    def test_a_matching_fingerprint_keeps_the_chain(self):
        # The case that runs on every start after the first. If this ever
        # emptied the database, every restart would cost a full reindex.
        self._index_three()
        self._set_fingerprint(db_module.SCHEMA_FINGERPRINT)
        db = self.reopened(rebuild=True)
        self.assertEqual(self._row_counts(db),
                         {"blocks": 3, "txs": 3, "vin": 3, "vout": 3,
                          "addr_out": 3, "scripts": 3})
        self.assertEqual(db.tip_height(), 102)
        for key, sql in db_module.STATS:
            self.assertEqual(db._stat(key),
                             db.conn.execute(sql).fetchone()[0], key)

    def test_a_stale_fingerprint_empties_the_chain(self):
        # What a code change does. The database is thrown away and the indexer
        # re-syncs from genesis, which costs minutes and is always correct.
        self._index_three()
        self._set_fingerprint("0000000000000000")
        db = self.reopened(rebuild=True)
        self.assertEqual(set(self._row_counts(db).values()), {0})
        self.assertEqual(db.tip_height(), -1)
        # The counters went with the rows, not left describing a chain that is
        # no longer there -- that mismatch is what reports a supply figure for
        # an empty database.
        self.assertEqual(db._stat(db_module.COINBASE_TOTAL_KEY), 0)
        self.assertEqual(db._stat(db_module.N_BLOCKS_KEY), 0)
        self.assertEqual(db._stat(db_module.N_TXS_KEY), 0)

    def test_a_database_with_no_fingerprint_is_emptied(self):
        # Every database built before this policy existed, and every one a
        # failed build left behind. Nothing can be promised about their shape,
        # so they are rebuilt rather than inspected.
        self._index_three()
        self._set_fingerprint(None)
        db = self.reopened(rebuild=True)
        self.assertEqual(set(self._row_counts(db).values()), {0})
        self.assertEqual(db.tip_height(), -1)

    def test_the_web_side_can_never_empty_the_chain(self):
        # rebuild=False is what the web server opens with, so a schema mismatch
        # cannot cost a running site its indexed chain. It also does not repair
        # the mismatch -- that is the indexer's job, and the two processes are
        # expected to disagree about it briefly at startup.
        self._index_three()
        self._set_fingerprint("0000000000000000")
        db = self.reopened(rebuild=False)
        self.assertEqual(self._row_counts(db)["blocks"], 3)
        self.assertEqual(db.get_meta(db_module.FINGERPRINT_KEY),
                         "0000000000000000")

    def test_a_rebuild_removes_our_orphaned_index_but_not_a_foreign_table(self):
        # Two kinds of leftover, and the rebuild has to answer them differently.
        # An index on a table the schema owns goes with the table and is not
        # recreated, because CREATE TABLE IF NOT EXISTS never drops anything and
        # an index the schema no longer names would otherwise cost write time
        # on every block forever, with nothing to notice it. A table the schema
        # never named is left alone: it is not this schema's to delete, and a
        # rebuild that swept up unknown tables could destroy something that
        # merely shares the database.
        self._index_three()
        self.db.conn.execute("CREATE TABLE notes (x INTEGER)")
        self.db.conn.execute(
            "CREATE INDEX idx_vout_address ON vout(addresses)")
        self.db.conn.commit()
        self._set_fingerprint("0000000000000000")
        db = self.reopened(rebuild=True)
        self.assertEqual(indexes_on(db, "vout"), ["idx_vout_script", "vout_pkey"])
        self.assertEqual(indexes_on(db, "addr_out"),
                         ["addr_out_pkey", "idx_addr_out_addr",
                          "idx_addr_out_txid_n"])
        present = sorted(r[0] for r in db.conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname=current_schema()"))
        self.assertIn("notes", present)
        self.assertEqual([t for t in present if t != "notes"],
                         ["addr_out", "blocks", "meta", "scripts",
                          "txs", "vin", "vout"])

    def test_a_failed_open_closes_the_connection_it_cannot_return(self):
        # A constructor that raises never hands back the object holding the
        # connection, so nothing downstream can close it. Covered here because
        # the failure is otherwise only visible as a ResourceWarning, which a
        # normal run does not surface and so lets the gap sit unnoticed.
        #
        # The failure is injected at _rebuild because that is the shape that
        # matters and the one the constructor cannot recover from by itself.
        opened = []
        real_connect = psycopg.connect

        def spy(*a, **kw):
            conn = real_connect(*a, **kw)
            opened.append(conn)
            return conn

        dsn = self.add_schema_dsn()
        with mock.patch.object(db_module.psycopg, "connect", spy), \
             mock.patch.object(DB, "_rebuild",
                               side_effect=psycopg.OperationalError("boom")):
            with self.assertRaises(psycopg.OperationalError):
                DB.initialize(dsn, rebuild=True)
        self.assertEqual(len(opened), 1)
        # A closed connection refuses work, which is how this is asked without
        # depending on the warning the fix exists to prevent.
        with self.assertRaises(psycopg.OperationalError):
            opened[0].execute("SELECT 1")


class ColdStartRaceTest(_PGTestCase):
    """Two cold starts at once must not collide while creating the schema.

    explorer.sh launches the indexer and the web server together, and on a
    database built from scratch both reach initialize() within a second of
    each other with the schema still empty. CREATE TABLE IF NOT EXISTS is not
    race-safe in PostgreSQL -- the existence check and the catalog insert are
    not one atomic act -- so without the schema lock covering the DDL loop
    one of them dies with a pg_type collision on the very first table. That is
    not a cosmetic failure: the loser was the web server, and it died before
    binding, so the browser got connection-refused until the second run.
    """

    def initialize_concurrently(self, dsn, n=4):
        """Call DB.initialize() `n` times at once; return the errors raised.

        Real threads, not one after another: the collision only happens when
        the CREATE TABLEs actually overlap, and a serial loop would pass
        against the unfixed code.
        """
        errors = []
        start = threading.Barrier(n)
        made = []

        def run():
            start.wait(60)          # release them all into the DDL together
            try:
                db = DB.initialize(dsn)
                made.append(db)
            except Exception as e:  # noqa: BLE001 - the failure is the subject
                errors.append(e)

        threads = [threading.Thread(target=run) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(120)
        for db in made:
            self.addCleanup(db.conn.close)
        return errors

    def test_a_cold_start_race_loses_nobody(self):
        dsn = self.add_schema_dsn()
        errors = self.initialize_concurrently(dsn)
        self.assertEqual([str(e) for e in errors], [])
        # And the schema it built is the one every later process expects.
        db = DB.connect(dsn)
        self.addCleanup(db.conn.close)
        self.assertEqual(
            tables_in(db),
            {"meta", "blocks", "txs", "vin", "vout", "addr_out", "scripts"})

    def test_the_race_leaves_no_half_built_schema_behind(self):
        # A loser that rolled back mid-DDL would leave some tables and not
        # others. Every table, or the failure is worse than the collision.
        dsn = self.add_schema_dsn()
        self.initialize_concurrently(dsn, n=6)
        with contextlib.closing(psycopg.connect(dsn, autocommit=True)) as raw:
            names = {r[0] for r in raw.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = current_schema()")}
        self.assertEqual(
            names,
            {"meta", "blocks", "txs", "vin", "vout", "addr_out", "scripts"})

    def test_two_racing_cold_starts_agree_on_the_result(self):
        # Whoever created what, both callers must end up able to read and
        # write it: a schema one of them considers unfinished is the other
        # failure mode of this race.
        dsn = self.add_schema_dsn()
        self.initialize_concurrently(dsn, n=2)
        from indexer import Block
        db = DB.initialize(dsn)
        self.addCleanup(db.conn.close)
        db.add_block(Block({"height": 1, "hash": "b1", "time": 1, "tx": []}))
        db.add_tx(tx("T1", 1, [("a", POKE, 1)], coinbase=True))
        self.assertEqual(db.query("SELECT count(*) FROM txs")[0][0], 1)


class ConnectionSetupTest(DBTestCase):
    """initialize() owns DDL; connect() must never run it."""

    def tables(self, db):
        return tables_in(db)

    def test_connect_creates_no_schema(self):
        dsn = self.add_schema_dsn()
        self.assertEqual(tables_in(DB.connect(dsn)), set())

    def test_connect_does_not_rewrite_a_schema_it_does_not_own(self):
        # A database opened through connect() keeps whatever shape it has, even
        # one this code would reject: the read path must neither rebuild it nor
        # repair it, because the indexer owns that decision and the two are
        # expected to disagree about it briefly at startup.
        dsn = self.add_schema_dsn()
        # contextlib.closing, not `with`: psycopg's context manager ends the
        # transaction, it does not close the connection.
        with contextlib.closing(psycopg.connect(dsn, autocommit=True)) as raw:
            raw.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
            raw.execute("INSERT INTO meta VALUES ('schema_fingerprint','v2')")
            raw.execute("CREATE TABLE scripts (script_hash TEXT PRIMARY KEY,"
                        " value_received INTEGER)")
        db = DB.connect(dsn)
        self.addCleanup(db.conn.close)
        self.assertIn("value_received", columns_of(db, "scripts"))
        self.assertEqual(db.get_meta(db_module.FINGERPRINT_KEY), "v2")

    def test_connect_sees_writes_from_another_connection(self):
        db = self.fresh_db(self.db_path)
        self.db.add_tx(tx("A", None, [("addr0", POKE, 1)], coinbase=True))
        self.assertEqual(len(Explorer(db).address("addr0")["outputs"]), 1)

    def test_pool_hands_out_every_connection_and_returns_them(self):
        pool = DBPool(self.db_path, size=3)
        self.addCleanup(pool.close)
        with pool.borrow() as a, pool.borrow() as b, pool.borrow() as c:
            conns = {id(a.conn), id(b.conn), id(c.conn)}
            self.assertEqual(len(conns), 3)   # no connection lent twice
        with pool.borrow() as a:
            self.assertIn(id(a.conn), conns)  # all returned

    def test_pool_serves_a_request_from_any_thread(self):
        # Connections are opened by whoever builds the pool, but used by
        # request threads; check_same_thread=False plus checkout is the contract.
        pool = DBPool(self.db_path, size=2)
        self.addCleanup(pool.close)
        self.db.add_tx(tx("A", None, [("addr0", POKE, 1)], coinbase=True))
        found = []

        def query():
            with pool.borrow() as db:
                found.append(Explorer(db).address("addr0")["live"]["balance"])

        threads = [threading.Thread(target=query) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(found, [POKE] * 8)


class PoolTimeoutTest(DBTestCase):
    """An exhausted pool must answer 503, not strand request threads forever.
    And the response must be written after the connection is given back, so a
    slow client cannot pin a connection that another request is waiting on.
    """

    def _serve(self, pool, timeout):
        """Serve the real Handler over HTTP while `pool` stays the class pool."""
        import http.server
        server_module.Handler.pool = pool
        httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0),
                                                server_module.Handler)
        port = httpd.server_address[1]
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        self.addCleanup(httpd.server_close)
        self.addCleanup(httpd.shutdown)
        return mock.patch.object(server_module, "POOL_TIMEOUT", timeout), port

    def test_borrow_times_out_when_every_connection_is_on_loan(self):
        pool = DBPool(self.db_path, size=1)
        self.addCleanup(pool.close)
        with pool.borrow():                        # the only connection is taken
            with self.assertRaises(queue.Empty):
                with pool.borrow(timeout=0.05):
                    pass

    def test_the_api_answers_503_when_the_pool_is_exhausted(self):
        import urllib.request
        pool = DBPool(self.db_path, size=1)
        self.addCleanup(pool.close)
        with pool.borrow():
            patcher, port = self._serve(pool, 0.1)
            with patcher, self.assertRaises(urllib.error.HTTPError) as cm:
                urllib.request.urlopen(
                    "http://127.0.0.1:%d/api/summary" % port, timeout=5)
            self.assertEqual(cm.exception.code, 503)
            self.assertEqual(json.loads(cm.exception.read()),
                             {"error": "busy"})

    def test_the_api_still_answers_200_after_the_restructure(self):
        # The borrow now returns the handle to the pool before the response is
        # written; a normal request must still produce its response body.
        import urllib.request
        pool = DBPool(self.db_path, size=1)
        self.addCleanup(pool.close)
        with self.db.bulk():
            self.db.add_block(Block({"height": 0, "hash": "b0000", "tx": [],
                                     "previousblockhash": None}))
        patcher, port = self._serve(pool, 5)
        with patcher:
            body = urllib.request.urlopen(
                "http://127.0.0.1:%d/api/summary" % port,
                timeout=5).read()
        out = json.loads(body)
        self.assertEqual(out["tip"]["height"], 0)
        self.assertIn("n_blocks", out)


class BindAddressTest(DBTestCase):
    """Where this binds is a security decision, so it is pinned down.

    The explorer has no authentication at all: every balance, every address and
    every tx it holds is readable by whoever reaches the socket. The bind
    address is therefore the only access control there is, which makes "listen
    on every interface" something to have to ask for rather than something that
    happens because nobody changed a default.

    It did happen: the default was `::`, which is every interface on both
    families, while nginx-explorer.conf proxies to 127.0.0.1 and the README
    pointed at a loopback URL. In that combination the proxy was decorative --
    port 8080 answered directly and a visitor could skip nginx and get the
    explorer over plain HTTP.
    """

    def test_the_default_binds_loopback_and_not_every_interface(self):
        self.assertEqual(server_module.argument_parser().parse_args([]).host,
                         "127.0.0.1")

    def test_loopback_addresses_are_not_reported_as_reachable(self):
        for host in ("127.0.0.1", "127.0.0.2", "::1", "localhost"):
            self.assertFalse(server_module.reachable_off_machine(host),
                             "%s is loopback" % host)

    def test_anything_else_is_reported_as_reachable(self):
        # Deliberately including names we cannot resolve: a hostname could be
        # anything, and the whole point is not to under-warn.
        for host in ("::", "0.0.0.0", "", "192.168.2.8", "example.internal",
                     "localhost.localdomain"):
            self.assertTrue(server_module.reachable_off_machine(host),
                            "%s could be reached from outside" % host)

    def test_an_ipv6_url_is_bracketed(self):
        # An unbracketed IPv6 literal is not a URL anyone can open: the colons
        # are indistinguishable from the port separator.
        self.assertEqual(server_module.url_for("127.0.0.1", 8080),
                         "http://127.0.0.1:8080/")
        self.assertEqual(server_module.url_for("::1", 8080),
                         "http://[::1]:8080/")
        self.assertEqual(server_module.url_for("::", 8080),
                         "http://[::]:8080/")

    def test_the_socket_type_follows_the_address(self):
        # An IPv4 literal on an AF_INET6 socket cannot be bound at all, so the
        # default only works because main() drops to a plain IPv4 socket when
        # the address has no colon in it.
        self.assertNotIn(":", "127.0.0.1")
        self.assertEqual(server_module.DualStackHTTPServer.address_family,
                         socket.AF_INET6)

    def test_the_shell_default_agrees_with_the_python_one(self):
        # Two defaults that drift is how this went wrong in the first place, so
        # the script and the argument parser are read from the same source.
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "explorer.sh")) as f:
            script = f.read()
        m = re.search(r'WEBHOST="\$\{WEBHOST:-([^}]*)\}"', script)
        self.assertIsNotNone(m, "explorer.sh no longer sets a WEBHOST default")
        self.assertEqual(m.group(1), "127.0.0.1",
                         "explorer.sh and server.py disagree about the bind "
                         "address")


class LockTimeoutTest(DBTestCase):
    """Long only while the schema is being built, short afterwards."""

    _UNITS = {"us": 0.001, "ms": 1, "s": 1000, "min": 60_000, "h": 3_600_000,
              "d": 86_400_000}

    def timeout_of(self, db):
        """lock_timeout, in milliseconds.

        SHOW answers in whichever unit is exact -- "5s" for 5000ms, "30min"
        for 1800000ms -- so it is parsed back to a number rather than compared
        as text, which would pin these tests to Postgres's own formatting.
        The unit has to be matched longest-first: slicing one character off
        turns "1234ms" into "1234m" and "30min" into "30mi".
        """
        raw = db.conn.execute("SHOW lock_timeout").fetchone()[0]
        m = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([a-z]+)$", raw)
        self.assertIsNotNone(m, "unparsed lock_timeout %r" % raw)
        number, unit = m.groups()
        return int(float(number) * self._UNITS[unit])

    def test_a_read_connection_waits_seconds_not_minutes(self):
        db = DB.connect(self.db_path)
        self.addCleanup(db.conn.close)
        self.assertEqual(self.timeout_of(db), DB.NORMAL_LOCK_TIMEOUT_MS)
        self.assertLess(self.timeout_of(db), 60_000)

    def test_the_schema_window_gets_the_long_timeout(self):
        # Schema work must run under the long timeout, and the connection must
        # not keep it afterwards.
        seen = []
        real = DB._rebuild

        def spy(inner):
            seen.append(self.timeout_of(inner))
            return real(inner)

        with mock.patch.object(DB, "_rebuild", spy):
            db = DB.initialize(self.db_path, rebuild=True)  # runs regardless
        self.addCleanup(db.conn.close)
        self.assertEqual(seen, [DB.SCHEMA_LOCK_TIMEOUT_MS])
        self.assertEqual(self.timeout_of(db), DB.NORMAL_LOCK_TIMEOUT_MS)

    def test_an_explicit_timeout_is_honoured(self):
        db = DB(self.db_path, lock_timeout=1234)
        self.addCleanup(db.conn.close)
        self.assertEqual(self.timeout_of(db), 1234)

    def test_a_blocked_write_fails_fast_instead_of_hanging(self):
        # A row lock rather than a whole-database write lock: the holder takes
        # one meta row and keeps it, and the waiter blocks behind it there.
        #
        # The explicit begin() is what makes it a holder. The connection is in
        # autocommit, so a bare UPDATE would commit as soon as it ran and drop
        # the lock again before the waiter ever asked for it -- which is a test
        # that passes for the wrong reason, not one that does not block.
        # The fingerprint row, because it is the one row a fresh schema is
        # guaranteed to have: the counters are no longer seeded on open, so
        # there is no total_coinbase row to lock until something has minted.
        row = db_module.FINGERPRINT_KEY
        holder = self.fresh_db(self.db_path)
        holder.conn.begin()
        holder.conn.execute("UPDATE meta SET value=value WHERE key=?", (row,))
        self.addCleanup(holder.conn.rollback)
        waiter = DB(self.db_path, lock_timeout=50)   # 50ms, same code path
        self.addCleanup(waiter.conn.close)
        with self.assertRaises(psycopg.errors.LockNotAvailable) as cm:
            waiter.conn.execute(
                "UPDATE meta SET value=? WHERE key=?", ("x", row))
            waiter.conn.commit()
        self.assertIn("lock timeout", str(cm.exception))


class ScriptsRebuildTest(DBTestCase):
    """A scoped rebuild must land exactly where a full rebuild would."""

    def scripts_table(self):
        return {r[0]: r[1:] for r in self.db.query(
            "SELECT script_hash, type, req_sigs, addresses, created_height,"
            " last_height FROM scripts")}

    def add_chain(self, n_scripts, per_script=2, base=100):
        """Distinct scripts; script s gets outputs at base+10s, +1, ..."""
        for s in range(n_scripts):
            for k in range(per_script):
                self.db.add_tx(tx(
                    "%s%d" % (chr(ord("A") + s), k), base + s * 10 + k,
                    [("addr%d" % s, POKE, s + 1)], coinbase=True))

    def test_a_reorg_scoped_rebuild_matches_a_full_rebuild(self):
        self.add_chain(6)
        self.db.clear_from(120)             # severs scripts 2..5
        scoped = self.scripts_table()
        self.db.rebuild_scripts()            # the full table, from scratch
        self.assertEqual(scoped, self.scripts_table())
        self.assertEqual(len(scoped), 2)

    def test_a_deep_reorg_scoped_rebuild_matches_a_full_rebuild(self):
        self.add_chain(12, per_script=3)
        self.db.clear_from(180)             # severs most of the chain
        scoped = self.scripts_table()
        self.db.rebuild_scripts()
        self.assertEqual(scoped, self.scripts_table())
        self.assertTrue(scoped)             # and it is not trivially empty

    def test_a_reorg_shrinks_the_heights_it_should(self):
        # clear_from() truncates at a height, so anything it spares is spared
        # entirely: it can shorten a script's height range, never move its
        # earliest output without taking everything above it too.
        self.add_chain(2, per_script=3)   # 0: 100,101,102  1: 110,111,112
        h0 = script_hash_of(script_hex(1))
        h1 = script_hash_of(script_hex(2))
        self.assertEqual(self.scripts_table()[h0][-2:], (100, 102))
        self.assertEqual(self.scripts_table()[h1][-2:], (110, 112))
        self.db.clear_from(102)            # severs 102, and all of script 1
        self.assertEqual(self.scripts_table()[h0][-2:], (100, 101))
        self.assertNotIn(h1, self.scripts_table())

    def test_a_script_orphaned_entirely_leaves_the_table(self):
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        h = script_hash_of(script_hex(1))
        self.assertIn(h, self.scripts_table())
        self.db.clear_from(100)
        self.assertNotIn(h, self.scripts_table())
        self.assertEqual(self.scripts_table(), {})

    def test_a_reorg_that_orphans_nothing_does_not_rebuild(self):
        self.add_chain(2)
        before = self.scripts_table()
        got = []
        real = DB.rebuild_scripts

        def spy(db, script_hashes=None):
            got.append(script_hashes)
            return real(db, script_hashes)

        with mock.patch.object(DB, "rebuild_scripts", spy):
            self.db.clear_from(1000)        # nothing at that height
        self.assertEqual(got, [[]])         # empty scope, not a full rebuild
        self.assertEqual(self.scripts_table(), before)

    def test_a_reorg_rebuilds_only_the_scripts_it_orphaned(self):
        # The point of scoping: a reorg must not rewrite every script.
        self.add_chain(8)                   # heights 100..171
        got = []
        real = DB.rebuild_scripts

        def spy(db, script_hashes=None):
            got.append(None if script_hashes is None else list(script_hashes))
            return real(db, script_hashes)

        with mock.patch.object(DB, "rebuild_scripts", spy):
            self.db.clear_from(150)         # severs scripts 5,6,7
        self.assertEqual(len(got), 1)
        self.assertIsNotNone(got[0], "a reorg must not trigger a full rebuild")
        self.assertEqual(len(got[0]), 3)
        self.assertEqual(len(set(got[0])), 3, "no duplicate scripts")
        # the five untouched scripts kept their rows untouched
        self.assertEqual(len(self.scripts_table()), 5)

    def test_mempool_only_script_disappears_after_eviction(self):
        # A script is described entirely by its vout rows, so when the last one
        # is evicted the row has nothing left to describe. Kept anyway, it makes
        # /api/script/<hash> answer 200 with a real type and address list over
        # zero balances -- a script that exists nowhere in the index.
        h = script_hash_of(script_hex(1))
        self.db.add_tx(tx("A", None, [("addr0", POKE, 1)], coinbase=True))
        self.assertIn(h, self.scripts_table())
        self.indexer.close_stale_mempool([])
        self.assertNotIn(h, self.scripts_table())
        self.assertIsNone(self.explorer.script(h), "404, not a zero-balance ghost")
        self.assertEqual(self.scripts_table(), {})

    def test_an_eviction_keeps_a_script_that_still_has_an_output(self):
        self.add_chain(1)                                    # confirmed at 100
        self.db.add_tx(tx("A1", None, [("addr9", POKE, 1)]))  # mempool, same script
        h = script_hash_of(script_hex(1))
        self.indexer.close_stale_mempool([])
        scoped = self.scripts_table()
        self.assertIn(h, scoped, "one output is still there, so is the script")
        self.assertEqual(scoped[h][-2:], (100, 100), "a mempool tx adds no height")
        self.assertEqual(self.explorer.script(h)["live"]["balance"], POKE)
        self.db.rebuild_scripts()
        self.assertEqual(scoped, self.scripts_table())

    def test_an_eviction_rebuilds_only_the_scripts_it_removed(self):
        # The point of scoping: a mempool eviction must not re-aggregate the
        # whole table, for the same reason a reorg does not.
        self.add_chain(6)
        self.db.add_tx(tx("M0", None, [("addr9", POKE, 1)]))  # reuses script 0
        self.db.add_tx(tx("M5", None, [("addr9", POKE, 6)]))  # reuses script 5
        got = []
        real = DB.rebuild_scripts

        def spy(db, script_hashes=None):
            got.append(None if script_hashes is None else list(script_hashes))
            return real(db, script_hashes)

        with mock.patch.object(DB, "rebuild_scripts", spy):
            self.db.remove_txs(["M0", "M5"])
        self.assertEqual(len(got), 1)
        self.assertIsNotNone(got[0], "an eviction must not trigger a full rebuild")
        self.assertEqual(sorted(got[0]),
                         sorted(script_hash_of(script_hex(t)) for t in (1, 6)))
        # the other four scripts kept their rows untouched
        self.assertEqual(len(self.scripts_table()), 6)


class ReorgSpentFlagTest(DBTestCase):
    """Retracting only the freed outputs must equal a full recompute."""

    def spent_flags(self):
        return dict(self.db.query("SELECT txid, spent_by FROM addr_out"))

    def full_recompute(self):
        """The pre-existing whole-table version, for comparison."""
        full_spent_recompute(self.db)

    def build_random_chain(self, n_txs=40, seed=0):
        """A random spend graph over confirmed txs, plus mempool spends."""
        rnd = random.Random(seed)
        made = []
        for i in range(n_txs):
            spends = ()
            if made and rnd.random() < 0.6:
                spends = [(rnd.choice(made), rnd.randrange(2))]
            t = tx("T%02d" % i, 100 + i,
                   [("addr%d" % (i % 5), POKE, (i % 7) + 1)],
                   spends=spends, coinbase=not spends)
            self.db.add_tx(t)
            made.append("T%02d" % i)
        for i in range(6):        # mempool txs spending confirmed outputs
            prev = rnd.choice(made)
            self.db.add_tx(tx("M%d" % i, None,
                              [("addr%d" % i, POKE // 2, 8 + i)],
                              spends=[(prev, 0)]))
        return made

    def test_retraction_matches_a_full_recompute_over_random_reorgs(self):
        for seed in range(12):
            with self.subTest(seed=seed):
                db = self.fresh_db()
                self.db = db
                self.build_random_chain(seed=seed)
                self.db.clear_from(100 + random.Random(seed).randrange(5, 35))
                scoped = self.spent_flags()
                self.full_recompute()
                self.assertEqual(scoped, self.spent_flags(), "seed %d" % seed)
                db.conn.close()

    def test_a_reorg_unspends_an_output_it_held(self):
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("B", 101, [("addr1", POKE, 2)], spends=[("A", 0)]))
        self.assertEqual(self.is_spent("A", 0), True)
        self.db.clear_from(101)               # B is orphaned, A survives
        self.assertEqual(self.is_spent("A", 0), False)

    def test_a_reorg_leaves_an_output_spent_by_a_mempool_tx(self):
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("B", 101, [("addr1", POKE, 2)], spends=[("A", 0)]))
        self.db.add_tx(tx("M", None, [("addr2", POKE, 3)], spends=[("A", 0)]))
        self.db.clear_from(101)               # B dies, M still spends A
        self.assertEqual(self.is_spent("A", 0), True,
                         "a mempool spender must keep the output spent")

    def test_a_reorg_leaves_an_output_spent_by_a_surviving_confirmed_tx(self):
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("B", 100, [("addr1", POKE, 2)], spends=[("A", 0)]))
        self.db.add_tx(tx("C", 100, [("addr2", POKE, 3)], spends=[("A", 0)]))
        self.db.clear_from(100)               # severs B, C and A itself
        self.assertIsNone(self.is_spent("A", 0))   # A is gone entirely
        # a re-seen A is unspent again
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        self.assertEqual(self.is_spent("A", 0), False)

    def test_reorg_retracts_only_the_freed_outputs(self):
        # The cost claim: a one-block reorg must not rewrite every addr_out row.
        for i in range(30):
            self.db.add_tx(tx("T%02d" % i, 100 + i,
                              [("addr%d" % i, POKE, (i % 9) + 1)],
                              coinbase=True))
        self.db.add_tx(tx("S1", 150, [("addrx", POKE, 20)],
                          spends=[("T00", 0)]))
        self.db.add_tx(tx("S2", 150, [("addry", POKE, 21)],
                          spends=[("T01", 0)]))
        # conn.execute is read-only on the C object, so spy on the DB method
        # that issues the retraction instead.
        real_retract = DB._refresh_spent_flags
        calls = []

        def spy_retract(db, freed):
            calls.append(list(freed))
            return real_retract(db, freed)

        with mock.patch.object(DB, "_refresh_spent_flags", spy_retract):
            self.db.clear_from(150)
        self.assertEqual(len(calls), 1)
        self.assertEqual(sorted(calls[0]), [("T00", 0), ("T01", 0)])
        self.assertEqual(self.is_spent("T00", 0), False)
        self.assertEqual(self.is_spent("T01", 0), False)


class OrphanTxHistoryTest(DBTestCase):
    """/api/address treats orphans as history that no longer is.

    The tx list is current activity, and no other endpoint counts an orphaned
    tx anywhere (summary excludes them, blocks list only confirmed ones). A
    reorg already severs an orphan's vin/vout/addr_out rows, so the spend
    enumeration could never see one via the join -- the status filter states
    that contract at the query instead of relying on the severing alone.
    """

    def test_a_real_reorg_leaves_no_orphan_in_the_tx_list(self):
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("SPEND", 101, [("addr1", POKE, 2)],
                          spends=[("A", 0)]))
        self.db.clear_from(101)                     # orphans SPEND, keeps A
        self.assertEqual(self.db.query(
            "SELECT status FROM txs WHERE txid='SPEND'")[0][0], "orphaned",
            "SPEND survives as a tombstone, not a forgotten row")
        a = self.explorer.address("addr0")
        self.assertEqual(self.explorer.address("addr1"), None,
                         "the orphan's own outputs are gone, so no balance")
        self.assertIn("A", a["txs"])
        self.assertNotIn("SPEND", a["txs"],
                         "an orphaned spend is not active history")
        self.assertEqual(a["n_txs"], 1)
        self.assertEqual(a["outputs"][0]["spent"], False,
                         "the orphaned spend reverts the flag")
        self.assertEqual(a["outputs"][0]["spent_confirmed"], False)

    def test_an_orphan_left_with_rows_is_still_not_listed(self):
        # Belt-and-braces: if a data path ever stopped severing an orphan's
        # rows, the endpoint must still refuse to list it as activity. Forcing
        # the status directly skips db._clear_from's severing on purpose.
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("SPEND", 101, [("addr1", POKE, 2)],
                          spends=[("A", 0)]))
        self.db.conn.execute("UPDATE txs SET status='orphaned' WHERE txid=?",
                             ("SPEND",))
        self.db.conn.commit()
        a = self.explorer.address("addr0")
        self.assertIn("A", a["txs"])
        self.assertNotIn("SPEND", a["txs"],
                         "the filter, not the severing, is what keeps it out")
        self.assertEqual(a["n_txs"], 1)


class AddOrderSpentFlagTest(DBTestCase):
    """spent_by must follow the vin table, not the order txs were added in.

    Nothing promises an order: getrawmempool hands txs back as it likes, and a
    parent is re-indexed when it confirms while a child spending it may still be
    unconfirmed. Every order has to end up what a whole-table recompute says.
    """

    def parent(self, height=None):
        return tx("P", height, [("addr0", POKE, 1)], coinbase=True)

    def child(self, txid="C", spends=("P", 0)):
        return tx(txid, None, [("addr1", POKE, 2)], spends=[spends])

    def test_parent_before_child(self):
        self.db.add_tx(self.parent())
        self.db.add_tx(self.child())
        self.assertEqual(self.is_spent("P"), True)

    def test_child_before_parent(self):
        # The child's input UPDATE matched no row, because P:0 did not exist
        # yet, and the parent's INSERT then wrote the column default over a
        # spender that had been known all along.
        self.db.add_tx(self.child())
        self.db.add_tx(self.parent())
        self.assertEqual(self.is_spent("P"), True, "C spends P:0")

    def test_parent_confirmed_while_the_child_stays_mempool(self):
        self.db.add_tx(self.parent())
        self.db.add_tx(self.child())
        self.db.add_tx(self.parent(300))       # mined: re-indexed as confirmed
        self.assertEqual(self.is_spent("P"), True,
                         "a re-index must not unspend an output C still spends")

    def test_parent_confirmed_before_the_child_arrives(self):
        self.db.add_tx(self.parent(300))
        self.db.add_tx(self.child())
        self.assertEqual(self.is_spent("P"), True)

    def test_parent_reappears_after_its_own_eviction(self):
        self.db.add_tx(self.parent())
        self.db.add_tx(self.child())
        self.indexer.close_stale_mempool(["C"])   # P evicted, C still spends it
        self.assertIsNone(self.is_spent("P"), "P:0 went with P")
        self.db.add_tx(self.parent())             # and comes back
        self.assertEqual(self.is_spent("P"), True)

    def test_two_conflicting_spenders_survive_any_reindex(self):
        self.db.add_tx(self.parent(300))
        self.db.add_tx(self.child("C1"))
        self.db.add_tx(self.child("C2"))
        for again in (self.parent(300), self.child("C1"), self.child("C2")):
            self.db.add_tx(again)
            self.assertEqual(self.is_spent("P"), True,
                             "re-indexing %s forgot the other spender" % again.txid)

    def test_the_flags_do_not_depend_on_the_order_txs_are_added(self):
        txs = [self.parent(300), self.child("C1"), self.child("C2")]
        for order in itertools.permutations(txs):
            with self.subTest(order=[t.txid for t in order]):
                db = self.fresh_db()
                for t in order:
                    db.add_tx(t)
                scoped = dict(db.query("SELECT txid, spent_by FROM addr_out"))
                full_spent_recompute(db)
                self.assertEqual(scoped, dict(db.query("SELECT txid, spent_by FROM addr_out")))
                # Two spenders, one bit -- and it is bit 2, not bit 1: both
                # children are unconfirmed, so the output is live-spent but
                # still confirmed-unspent. Collapsing that distinction is
                # exactly what the mask exists to prevent, so the ordering test
                # pins the value rather than just its truthiness.
                self.assertEqual(scoped["P"], 2, "two mempool spenders, one bit")

    def test_a_reindex_that_drops_an_input_releases_the_output(self):
        # Unreachable while a txid pins its own content, so the release is
        # derived rather than assumed.
        self.db.add_tx(self.parent(300))
        self.db.add_tx(self.child())
        self.assertEqual(self.is_spent("P"), True)
        self.db.add_tx(tx("C", None, [("addr1", POKE, 2)]))   # same txid, no input
        self.assertEqual(self.is_spent("P"), False, "C no longer spends P:0")

    def spend_fan(self, n):
        """n confirmed txs, each with one output, and n mempool txs spending
        them one for one. Returns the spender txids."""
        spenders = ["S%05d" % i for i in range(n)]
        with self.db.bulk():
            for i in range(n):
                self.db.add_tx(tx("T%05d" % i, i, [("addr%d" % i, POKE, 1)],
                                 coinbase=True))
            for i, s in enumerate(spenders):
                self.db.add_tx(tx(s, None, [("addr%dx" % i, POKE, 1)],
                                 spends=[("T%05d" % i, 0)]))
        return spenders

    def test_a_refresh_past_one_chunk_agrees_with_a_full_recompute(self):
        # The refresh is batched, so the pairs have to survive being split
        # across statements. Ground truth is full_spent_recompute, an
        # independent implementation, not the same batching.
        n = db_module.SQL_VAR_CHUNK * 2
        spenders = self.spend_fan(n)
        self.db.remove_txs(spenders[:int(n * 0.6)])     # 600 pairs, 3 chunks
        scoped = dict(self.db.query("SELECT txid, spent_by FROM addr_out"))
        full_spent_recompute(self.db)
        self.assertEqual(
            scoped, dict(self.db.query("SELECT txid, spent_by FROM addr_out")))
        self.assertEqual(sum(1 for v in scoped.values() if v),
                         n - int(n * 0.6), "the survivors are still spent")

    def test_a_refresh_costs_a_statement_per_chunk_not_per_output(self):
        n = db_module.SQL_VAR_CHUNK * 2
        spenders = self.spend_fan(n)
        seen = []
        self.db.conn.set_trace_callback(seen.append)
        self.db.remove_txs(spenders)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM addr_out"
                                       " WHERE spent_by != 0")[0][0], 0)
        ups = [s for s in seen if "UPDATE addr_out" in s]
        self.assertLessEqual(
            len(ups), n // (db_module.SQL_VAR_CHUNK // 2) + 1,
            "%d statements to refresh %d outputs: the batching is not being used"
            % (len(ups), n))


class ShorterChainTest(DBTestCase):
    """The index must give blocks back when the daemon no longer has them.

    A daemon that rolls back, or comes back from an older backup, reports a
    lower tip than we hold. That used to be unreachable: sync_blocks() verified
    the tip by asking for getblockhash(synced), a height the daemon does not
    have, and the resulting error was retried forever without ever reaching the
    code that truncates.
    """

    def index_to(self, daemon, height):
        """Index blocks 0..height as the daemon describes them."""
        with self.db.bulk():
            for h in range(height + 1):
                self.db.add_block(Block(daemon.getblock(daemon.hash_at(h))))
        return self.db.tip_height()

    def test_a_daemon_behind_us_is_truncated_not_retried(self):
        daemon = FakeDaemon(990)
        self.assertEqual(self.index_to(daemon, 1000), 1000)
        self.indexer.rpc = daemon
        self.indexer.sync_blocks()            # must not raise
        self.assertEqual(self.db.tip_height(), 990)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM blocks")[0][0], 991)

    def test_the_tip_block_that_survives_is_the_one_the_daemon_has(self):
        # Truncating to the daemon's tip is only half of it: the block left at
        # that height has to be the daemon's, or the tip check would still see a
        # mismatch and truncate again on every cycle.
        daemon = FakeDaemon(990, hashes={990: "x0990"})
        self.index_to(daemon, 1000)
        self.indexer.rpc = daemon
        self.indexer.sync_blocks()
        self.assertEqual(self.db.tip_height(), 990)
        self.assertEqual(
            self.db.query("SELECT hash FROM blocks WHERE height=990")[0][0],
            "x0990")
        before = self.db.tip_hash()
        self.indexer.sync_blocks()            # idempotent: nothing left to do
        self.assertEqual(self.db.tip_hash(), before)

    def test_txs_above_the_new_tip_are_orphaned_not_deleted(self):
        daemon = FakeDaemon(990)
        self.index_to(daemon, 1000)
        self.db.add_tx(tx("X", 995, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("Y", 900, [("addr1", POKE, 2)], coinbase=True))
        self.indexer.rpc = daemon
        self.indexer.sync_blocks()
        self.assertEqual(self.db.query(
            "SELECT status FROM txs WHERE txid='X'")[0][0], "orphaned")
        self.assertEqual(self.rows_for("vout", "X"), 0)
        self.assertEqual(self.db.query(
            "SELECT status FROM txs WHERE txid='Y'")[0][0], "confirmed")
        self.assertEqual(self.is_spent("Y", 0), False)

    def test_a_daemon_with_no_chain_yet_does_not_truncate(self):
        # getblockcount below zero means the daemon is not serving a chain. It
        # is not evidence that our blocks are wrong, and truncating on it would
        # cost a full reindex.
        self.index_to(FakeDaemon(1000), 1000)
        self.indexer.rpc = FakeDaemon(-1)
        self.indexer.sync_blocks()
        self.assertEqual(self.db.tip_height(), 1000)

    def test_a_daemon_ahead_of_us_is_caught_up_not_truncated(self):
        daemon = FakeDaemon(1005)
        self.assertEqual(self.index_to(daemon, 1000), 1000)
        self.indexer.rpc = daemon
        self.indexer.sync_blocks()
        self.assertEqual(self.db.tip_height(), 1005)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM blocks")[0][0], 1006)

    def test_a_tip_that_moved_on_while_the_daemon_lost_blocks(self):
        # Truncate, then catch up: the blocks between the two tips are indexed
        # in one pass, and the tip check agrees on the last of them.
        daemon = FakeDaemon(1003)
        self.index_to(daemon, 1000)
        self.indexer.rpc = daemon
        self.indexer.sync_blocks()
        self.assertEqual(self.db.tip_height(), 1003)
        self.indexer.sync_blocks()
        self.assertEqual(self.db.tip_height(), 1003)


class MempoolRefreshTest(DBTestCase):
    """sync_mempool() must cost what the mempool costs, not what the index costs.

    Deciding which mempool txs are new used to read every confirmed txid in the
    index into a Python set, once a second, to compare against a mempool of a
    few dozen. The membership test is now a primary-key lookup per mempool txid,
    so a refresh no longer slows down as the chain grows.
    """

    def index_confirmed(self, n, base=0):
        for i in range(n):
            self.db.add_tx(tx("C%06d" % (base + i), base + i,
                              [("addr%d" % i, POKE, (i % 90) + 1)],
                              coinbase=True))

    def refresh(self, daemon):
        self.indexer.rpc = daemon
        return self.indexer.sync_mempool()

    def queries_during(self, daemon):
        """The SQL sync_mempool() issues, with its plans and row counts."""
        seen = []
        real = DB.query

        def spy(db, sql, params=()):
            rows = real(db, sql, params)
            seen.append((sql, list(params),
                         [r[-1] for r in db.conn.execute(
                             "EXPLAIN " + sql, params)], len(rows)))
            return rows

        with mock.patch.object(DB, "query", spy):
            # Under force_index_use, as the other plan assertions here are: the
            # property under test is which access path the lookup is served by,
            # and on a 500-row fixture the planner is free to prefer a seq scan
            # on cost grounds, which would make this assert on the row count
            # rather than on the statement.
            with force_index_use(self.db):
                self.indexer.rpc = daemon
                self.indexer.sync_mempool()
        return seen

    def test_the_membership_lookup_is_a_primary_key_seek(self):
        # The property that makes it cheap: no statement may fall back to
        # scanning the status index, which is a full pass over the chain.
        self.index_confirmed(500)
        for sql, _, plans, _ in self.queries_during(
                FakeDaemon(499, mempool=["m1", "m2"])):
            # Joined, not line by line: SQLite's EXPLAIN QUERY PLAN gave one row
            # per access path, so every line named the index. Postgres EXPLAIN
            # returns a tree, and only the leaf carries the index name -- the
            # Bitmap Heap Scan and Hash Join lines above it never mention it.
            plan = "\n".join(plans)
            self.assertNotIn("idx_txs_status_height", plan, sql)
            self.assertIn("txs_pkey", plan, sql)

    def test_a_refresh_does_not_read_the_whole_index(self):
        # One query is not the same as a cheap query: the old form asked for
        # every confirmed txid in one statement, so a 500-tx chain came back as
        # 500 rows to be turned into a set, once a second.
        self.index_confirmed(500)
        read = sum(rows for _, _, _, rows in
                   self.queries_during(FakeDaemon(499, mempool=["m1", "m2"])))
        self.assertLessEqual(read, 2,
                             "checking 2 mempool txs read %d rows" % read)

    def test_a_known_mempool_tx_is_not_refetched(self):
        self.db.add_tx(tx("M", None, [("addr1", POKE, 2)], spends=[("C", 0)]))
        daemon = FakeDaemon(9, mempool=["M"])
        self.assertEqual(self.refresh(daemon), ["M"])
        self.assertEqual(daemon.fetched, [],
                         "already tracked: refetching it every second is the cost")

    def test_a_new_mempool_tx_is_fetched_and_added(self):
        daemon = FakeDaemon(9, mempool=["M"])
        self.assertEqual(self.refresh(daemon), ["M"])
        self.assertEqual(daemon.fetched, ["M"])
        self.assertEqual(self.db.query(
            "SELECT status, height FROM txs WHERE txid='M'"), [("mempool", None)])

    def test_an_orphaned_tx_reappearing_is_refetched(self):
        # A reorg drops a tx to a tombstone; when it comes back it has to be
        # fetched again, which is why tombstones are not in the known set.
        self.db.add_tx(tx("M", 500, [("addr1", POKE, 2)], coinbase=True))
        self.db.clear_from(500)
        self.assertEqual(self.db.query(
            "SELECT status FROM txs WHERE txid='M'")[0][0], "orphaned")
        daemon = FakeDaemon(9, mempool=["M"])
        self.assertEqual(self.refresh(daemon), ["M"])
        self.assertEqual(daemon.fetched, ["M"])
        self.assertEqual(self.db.query(
            "SELECT status, height FROM txs WHERE txid='M'")[0][0], "mempool")

    def test_a_confirmed_tx_the_daemon_lists_stays_confirmed(self):
        # The daemon can name a tx we hold as confirmed only if it reorged it
        # out, which clear_from() has normally already turned into a tombstone.
        # Keeping it confirmed until then is the conservative reading: a flip
        # to mempool would let close_stale_mempool() delete a tx that is still
        # on the chain, and a deleted tx is never re-indexed.
        self.db.add_tx(tx("M", 500, [("addr1", POKE, 2)], coinbase=True))
        daemon = FakeDaemon(9, mempool=["M"])
        self.assertEqual(self.refresh(daemon), ["M"])
        self.assertEqual(daemon.fetched, [])
        self.assertEqual(self.db.query(
            "SELECT status, height FROM txs WHERE txid='M'")[0][0], "confirmed")

    def test_a_mempool_larger_than_one_chunk_still_matches_everything(self):
        live = ["M%03d" % i for i in range(1100)]
        for txid in live[:1000]:
            self.db.add_tx(tx(txid, None, [("addr1", POKE, 2)], coinbase=True))
        daemon = FakeDaemon(9, mempool=live)
        self.assertEqual(self.refresh(daemon), live)
        self.assertEqual(daemon.fetched, live[1000:],
                         "only the 100 untracked txs are worth an RPC")

    def test_a_failed_mempool_fetch_skips_the_stale_sweep(self):
        # sync_mempool() used to return [] on an RPCError, and run_once() fed
        # that straight to close_stale_mempool(), which cannot tell an empty
        # reply from an unanswered one and deleted every pending row -- with the
        # spends depending on them reverting. The failure has to leave the
        # previously indexed mempool standing until the daemon answers again.
        self.db.add_tx(tx("C", 500, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("M", None, [("addr1", POKE, 2)], spends=[("C", 0)]))
        daemon = FakeDaemon(500)
        daemon.getrawmempool = lambda: (_ for _ in ()).throw(RPCError("down"))
        self.indexer.rpc = daemon
        with mock.patch.object(Indexer, "sync_blocks") as sync:
            self.indexer.run_once()
        sync.assert_called_once()
        self.assertEqual(self.is_spent("C"), True,
                         "M still spends C:0, so the spend flag survives")
        self.assertEqual(self.db.query(
            "SELECT status FROM txs WHERE txid='M'")[0][0], "mempool")


class TxRetrievalDialogTest(DBTestCase):
    """How sync_blocks treats a getrawtransaction that refuses mid-window.

    A per-slot refusal inside a successful batch used to stub the tx forever;
    the daemon's "no information" (permanent) and a transient hiccup were
    indistinguishable. The stub answer first pass, the rest are re-asked.
    """

    def vers(self, txid):
        return {"txid": txid, "version": 1, "locktime": 0, "size": 100,
                "vin": [], "vout": [{"value": Decimal("3"), "n": 0,
                                     "scriptPubKey": {
                                         "type": "pubkeyhash",
                                         "addresses": ["addr1"], "reqSigs": 1,
                                         "asm": "OP_DUP", "hex": "76a914"}}]}

    def daemon(self, txids, fail):
        """Block 1 carries `txids`; fail(txid, call_no) raises or returns None."""
        daemon = BlockDaemon(1, {1: txids})
        calls = {}

        def getrawtransaction(txid, verbose=True):
            calls[txid] = calls.get(txid, 0) + 1
            err = fail(txid, calls[txid])
            if err is not None:
                raise err
            return self.vers(txid)

        daemon.getrawtransaction = getrawtransaction
        daemon.calls = calls
        return daemon

    def test_a_transient_refusal_is_retried_not_stubbed(self):
        daemon = self.daemon(
            ["A", "FLAKY", "B"],
            lambda t, n: RPCError("connection reset by peer")
                         if t == "FLAKY" and n == 1 else None)
        self.indexer.rpc = daemon
        with mock.patch("indexer.time.sleep"):
            self.indexer.sync_blocks()
        self.assertEqual(daemon.calls["FLAKY"], 2,
                         "the refused slot is re-asked on the next attempt")
        self.assertEqual(self.db.query(
            "SELECT COUNT(*) FROM vout WHERE txid='FLAKY'")[0][0], 1,
                         "detail was indexed, not lost to a stub")
        self.assertEqual(self.indexer.counters["tx_stub"], 0)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM txs")[0][0], 3)

    def test_a_tx_with_no_information_stubs_without_retrying(self):
        daemon = self.daemon(
            ["GONE"],
            lambda t, n: RPCError("No information available about transaction")
                         if t == "GONE" else None)
        self.indexer.rpc = daemon
        with mock.patch("indexer.time.sleep"):
            self.indexer.sync_blocks()
        self.assertEqual(daemon.calls["GONE"], 1,
                         "a refusal that will never clear is not re-asked")
        self.assertEqual(self.indexer.counters["tx_stub"], 1)
        self.assertEqual(self.db.query(
            "SELECT COUNT(*) FROM vout WHERE txid='GONE'")[0][0], 0)
        self.assertEqual(self.db.query(
            "SELECT COUNT(*) FROM txs WHERE txid='GONE'")[0][0], 1,
                         "the row survives, just without detail")

    def test_a_never_clearing_refusal_stubs_after_the_cap(self):
        daemon = self.daemon(
            ["STUCK"],
            lambda t, n: RPCError("internal server error")
                         if t == "STUCK" else None)
        self.indexer.rpc = daemon
        with mock.patch("indexer.time.sleep"):
            self.indexer.sync_blocks()
        self.assertEqual(daemon.calls["STUCK"],
                         indexer_module._TX_RETRIES,
                         "the window is not held open forever")
        self.assertEqual(self.indexer.counters["tx_stub"], 1)


class TotalCoinbaseCacheTest(DBTestCase):
    """The maintained total must never drift from the query that defines it.

    There is no cache to expire any more: db.py keeps the running total in
    the same transaction as the rows it sums, so what these tests pin is the
    stronger property -- that every path which can change the figure changes
    the counter too, and that the counter is readable without the 31s scan
    that used to sit behind every /api/summary.
    """

    def coinbase_block(self, height, tag, pokes=50 * POKE):
        """One block whose single coinbase is worth `pokes`."""
        from indexer import Block
        self.db.add_block(Block({"height": height, "hash": tag,
                                 "time": 1700000000 + height,
                                 "tx": ["C%d" % height]}))
        self.db.add_tx(tx("C%d" % height, height,
                          [("miner", pokes, 1)], coinbase=True))
        return tag

    def test_a_new_block_is_visible_immediately(self):
        # The old cache held its value for 60s regardless of the chain.
        self.coinbase_block(1, "h1")
        first = self.explorer.total_coinbase()
        self.coinbase_block(2, "h2")
        self.assertEqual(self.explorer.total_coinbase(),
                         first + 50 * POKE)

    def test_a_reorg_at_the_same_height_is_visible(self):
        # tip_height does not change here, so a height-keyed cache would serve
        # the pre-reorg total for its whole TTL.
        self.coinbase_block(1, "h1")
        self.coinbase_block(2, "h2")
        before = self.explorer.total_coinbase()
        self.assertEqual(before, 100 * POKE)
        self.db.clear_from(2)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM blocks")[0][0], 1)
        self.coinbase_block(2, "h2-replacement", pokes=80 * POKE)
        after = self.explorer.total_coinbase()
        self.assertEqual(after, 50 * POKE + 80 * POKE)
        self.assertNotEqual(before, after)

    def test_orphaning_a_coinbase_lowers_the_total(self):
        self.coinbase_block(1, "h1")
        self.coinbase_block(2, "h2")
        self.assertEqual(self.explorer.total_coinbase(), 100 * POKE)
        self.db.clear_from(2)
        self.assertEqual(self.explorer.total_coinbase(), 50 * POKE)

    def test_a_normal_tx_does_not_mint(self):
        # Only coinbases mint. A normal tx moves value that already exists, so
        # indexing one must leave the total exactly where it was. This is the
        # case the rest of this class cannot see: coinbase_block() indexes
        # nothing but coinbases, so a counter that counted every confirmed tx's
        # outputs agreed with the defining query in every one of them.
        self.coinbase_block(1, "h1")
        before = self.explorer.total_coinbase()
        self.assertEqual(before, 50 * POKE)
        self.db.add_tx(tx("T1", 1, [("alice", 80 * POKE, 1)],
                          spends=[("C1", 0)]))
        self.assertEqual(self.explorer.total_coinbase(), before,
                         "a normal tx was counted as newly minted")
        self.assertMatchesDerived("after a normal tx")

    def test_many_normal_txs_still_do_not_drift(self):
        # The gap grows with throughput rather than staying a constant offset, so
        # one is not enough to catch a regression in the shape of the error.
        self.coinbase_block(1, "h1")
        for i in range(40):
            self.db.add_tx(tx("T%d" % i, 1, [("alice", 7 * POKE, 1)],
                              spends=[("C1", 0)]))
        self.assertEqual(self.explorer.total_coinbase(), 50 * POKE)
        self.assertMatchesDerived("after 40 normal txs")

    def test_a_normal_tx_evicted_from_the_mempool_does_not_move_the_total(self):
        # The remove path only ever subtracted coinbase values, so the two sides
        # disagreed: adding counted every tx, subtracting counted only coinbases.
        self.coinbase_block(1, "h1")
        before = self.explorer.total_coinbase()
        self.db.add_tx(tx("M1", None, [("alice", 30 * POKE, 1)], spends=()))
        self.assertEqual(self.explorer.total_coinbase(), before)
        self.db.remove_txs(["M1"])
        self.assertEqual(self.explorer.total_coinbase(), before)
        self.assertMatchesDerived("after mempool eviction")

    def test_the_repeated_value_is_served_from_cache(self):
        # Repeated reads must not run the sum. This is the regression that
        # matters: the scan behind TOTAL_COINBASE_SQL took 31s on a 4.6M-block
        # chain, and it was re-run on the read path every time the indexer
        # moved the tip -- which is every page load on a chain being indexed.
        self.coinbase_block(1, "h1")
        self.explorer.total_coinbase()
        sums = []
        real = DB.query

        def spy(db, sql, params=()):
            if sql == server_module.TOTAL_COINBASE_SQL:
                sums.append(sql)
            return real(db, sql, params)

        with mock.patch.object(DB, "query", spy):
            for _ in range(5):
                self.explorer.total_coinbase()
        self.assertEqual(sums, [], "the sum was recomputed on the read path")
        self.assertMatchesDerived()

    def test_reindexing_a_coinbase_does_not_double_count(self):
        # _add_tx deletes and reinserts, so a naive "add the new value" would
        # pay the coinbase twice on every re-index of a height. The block and
        # tx counts have to survive the same round trip.
        self.coinbase_block(1, "h1")
        before = self.explorer.total_coinbase()
        self.coinbase_block(1, "h1")            # same txid, same block
        self.assertEqual(self.explorer.total_coinbase(), before)
        self.assertMatchesDerived()

    def test_reindexing_a_block_at_a_known_height_does_not_count_it_twice(self):
        # _add_block is INSERT OR REPLACE, which reports nothing about whether
        # it inserted or replaced. n_blocks is a count of heights, so a
        # re-indexed height has to leave it alone.
        from indexer import Block
        for _ in range(2):
            self.db.add_block(Block({"height": 7, "hash": "h7", "time": 1,
                                     "tx": ["C7"]}))
            self.db.add_tx(tx("C7", 7, [("miner", 50 * POKE, 0)],
                              coinbase=True))
        self.assertEqual(self.db.n_blocks(), 1)
        self.assertEqual(self.db.n_txs(), 1)
        self.assertMatchesDerived()

    def test_a_multi_output_coinbase_counts_every_output(self):
        # The figure is a sum over outputs, not over coinbase txs: a coinbase
        # paying two outputs is worth both.
        from indexer import Block
        self.db.add_block(Block({"height": 1, "hash": "h1", "time": 1,
                                 "tx": ["C1"]}))
        self.db.add_tx(tx("C1", 1, [("a", 30 * POKE, 0),
                                    ("b", 20 * POKE, 1)], coinbase=True))
        self.assertEqual(self.explorer.total_coinbase(), 50 * POKE)
        self.assertMatchesDerived()

    def test_an_unconfirmed_coinbase_is_not_minted(self):
        # A coinbase seen in the mempool has no height, so it is not minted
        # until a block claims it. The counter keys off height, not is_coinbase.
        self.db.add_tx(tx("C1", None, [("miner", 50 * POKE, 0)],
                          coinbase=True))
        self.assertEqual(self.explorer.total_coinbase(), 0)
        self.assertMatchesDerived()

    def test_removing_a_confirmed_coinbase_retracts_it(self):
        # remove_txs() is built for mempool eviction, where this returns 0.
        # It is public API though, so the counter must not assume the caller
        # only ever passes the kind of txid it means to.
        self.coinbase_block(1, "h1")
        self.coinbase_block(2, "h2")
        self.assertEqual(self.explorer.total_coinbase(), 100 * POKE)
        self.db.remove_txs(["C2"])
        self.assertEqual(self.explorer.total_coinbase(), 50 * POKE)
        self.assertEqual(self.db.n_txs(), 1)
        self.assertMatchesDerived()

    def test_orphaning_a_block_lowers_the_block_and_tx_counts(self):
        # clear_from() deletes blocks outright but tombstones their txs, so
        # n_blocks falls by the depth of the reorg while n_txs falls too --
        # an orphan is not counted, which is what the defining query says.
        for h in range(1, 5):
            self.coinbase_block(h, "h%d" % h)
        self.assertEqual((self.db.n_blocks(), self.db.n_txs()), (4, 4))
        self.db.clear_from(3)
        self.assertEqual((self.db.n_blocks(), self.db.n_txs()), (2, 2))
        self.assertMatchesDerived()

    def test_orphaned_tombstone_pruning_does_not_retract_twice(self):
        # An orphan is already out of the total when it is orphaned; deleting
        # the tombstone years later must move nothing, or a reorg would drive
        # the total negative.
        self.coinbase_block(1, "h1")
        for h in range(2, 12):
            self.coinbase_block(h, "h%d" % h)
        self.db.clear_from(2)
        self.assertEqual(self.explorer.total_coinbase(), 50 * POKE)
        self.assertMatchesDerived()
        db_module.ORPHAN_RETENTION = 1
        try:
            tip = self.db.tip_height()
            self.db.clear_from(tip)   # prunes orphans older than retention
        finally:
            db_module.ORPHAN_RETENTION = 20000
        self.assertMatchesDerived("pruning an orphan retracted it a second time")

    def test_a_reorg_then_replacement_nets_to_the_replacement(self):
        # The exact sequence the indexer runs on a reorg: truncate, re-index
        # the same heights with different hashes. Two retractions and two
        # additions have to leave the replacement's value and nothing else.
        self.coinbase_block(1, "h1")
        self.coinbase_block(2, "h2", pokes=10 * POKE)
        self.db.clear_from(2)
        self.assertMatchesDerived("after truncation")
        self.coinbase_block(2, "h2-new", pokes=80 * POKE)
        self.assertEqual(self.explorer.total_coinbase(), 50 * POKE + 80 * POKE)
        self.assertMatchesDerived("after the replacement block")

    def test_a_bulk_of_blocks_and_mempool_txs_keeps_the_counter_exact(self):
        # The indexer writes a whole window inside one transaction, mixing
        # confirmed coinbases with mempool churn. _bump_coinbase_total must not
        # commit that transaction early, and the total must survive it.
        from indexer import Block
        with self.db.bulk():
            for h in range(1, 6):
                self.db.add_block(Block({"height": h, "hash": "b%d" % h,
                                         "time": 1, "tx": ["C%d" % h, "P%d" % h]}))
                self.db.add_tx(tx("C%d" % h, h, [("miner", 50 * POKE, 0)],
                                  coinbase=True))
                self.db.add_tx(tx("P%d" % h, None, [("pay", 7 * POKE, 0)]))
        self.assertEqual(self.explorer.total_coinbase(), 250 * POKE)
        self.assertMatchesDerived()
        self.db.remove_txs(["P%d" % h for h in range(1, 6)])
        self.assertEqual(self.explorer.total_coinbase(), 250 * POKE)
        self.assertMatchesDerived()

    def test_recompute_repairs_a_counter_that_was_corrupted(self):
        # The counters are maintained, not derived, so a value that drifted
        # would be served forever. recompute_stats() is the hatch.
        self.coinbase_block(1, "h1")
        self.coinbase_block(2, "h2")
        self.db.conn.execute(
            "UPDATE meta SET value='1' WHERE key IN (?,?)",
            (db_module.COINBASE_TOTAL_KEY, db_module.N_BLOCKS_KEY))
        self.db.conn.commit()
        self.assertEqual(self.db.total_coinbase(), 1)
        self.assertEqual(self.db.n_blocks(), 1)
        self.db.recompute_stats()
        self.assertEqual(self.db.total_coinbase(), 100 * POKE)
        self.assertEqual(self.db.n_blocks(), 2)
        self.assertMatchesDerived()

    def test_the_read_path_works_before_the_counters_are_seeded(self):
        # A read-only connection against a database written before the
        # counters existed must not answer 0 for the chain's whole supply.
        self.coinbase_block(1, "h1")
        self.coinbase_block(2, "h2")
        self.db.conn.execute(
            "DELETE FROM meta WHERE key IN (?,?,?)",
            (db_module.COINBASE_TOTAL_KEY, db_module.N_BLOCKS_KEY,
             db_module.N_TXS_KEY))
        self.db.conn.commit()
        self.assertEqual(self.db.total_coinbase(), 100 * POKE)
        self.assertEqual(self.db.n_blocks(), 2)
        self.assertEqual(self.db.n_txs(), 2)

    def test_the_counter_does_not_leak_between_databases(self):
        # The old cache was a module global, so two servers on two chains
        # shared one entry and the second showed the first's supply. The
        # counter is a row in each database's own meta table.
        self.coinbase_block(1, "h1")
        other = self.fresh_db()
        from indexer import Block
        other.add_block(Block({"height": 1, "hash": "other", "time": 1,
                               "tx": ["O1"]}))
        other.add_tx(tx("O1", 1, [("miner", 7 * POKE, 1)], coinbase=True))
        self.assertEqual(self.explorer.total_coinbase(), 50 * POKE)
        self.assertEqual(Explorer(other).total_coinbase(), 7 * POKE)
        self.assertEqual(self.explorer.total_coinbase(), 50 * POKE)

    def test_an_empty_chain_totals_zero(self):
        self.assertEqual(self.explorer.total_coinbase(), 0)

    def test_a_surviving_orphan_coinbase_is_not_counted(self):
        # clear_from() deletes the vout rows of the txs it orphans, so this
        # state is unreachable through the public API. The status filter in
        # TOTAL_COINBASE_SQL is what guarantees the total anyway; this test
        # pins that guarantee rather than today's cleanup behaviour, so a
        # future path that orphans a tx without clearing its outputs cannot
        # inflate minted supply.
        self.coinbase_block(1, "h1")
        self.coinbase_block(2, "h2")
        self.assertEqual(self.explorer.total_coinbase(), 100 * POKE)
        self.db.clear_from(2)
        # Put back the vout clear_from() removed, leaving the tx orphaned.
        self.db.conn.execute(
            "INSERT INTO vout (txid, n, value, type, addresses, req_sigs) "
            "VALUES ('C2', 0, ?, 'pubkeyhash', '[\"miner\"]', 1)",
            (50 * POKE,))
        self.db.conn.commit()
        self.assertEqual(
            self.db.query("SELECT status FROM txs WHERE txid='C2'")[0][0],
            "orphaned")
        self.assertEqual(self.explorer.total_coinbase(), 50 * POKE)

    def test_summary_agrees_with_a_direct_query(self):
        self.coinbase_block(1, "h1")
        self.coinbase_block(2, "h2")
        s = self.explorer.summary()
        self.assertEqual(s["total_coinbase"], 100 * POKE)
        self.assertEqual(s["total_coinbase_pxc"], "100.00000000")


class AmountParsingTest(_PGTestCase):
    """Amounts must never pass through a binary float."""

    def test_ordinary_amounts(self):
        for coins, pokes in [("0", 0), ("0.00000001", 1), ("0.1", 10_000_000),
                             ("0.29", 29_000_000), ("50", 5_000_000_000),
                             ("8.19", 819_000_000), ("1.005", 100_500_000),
                             ("0.00000029", 29)]:
            self.assertEqual(amount_to_pokes(Decimal(coins)), pokes, coins)

    def test_past_the_float_mantissa(self):
        # 2**53 is where a double stops representing every integer. The old
        # int(round(float(x) * COIN)) was wrong by one poke on both of these,
        # silently, because round() launders the error into a plausible int.
        for coins, pokes in [("90071992.54740993", 9007199254740993),
                             ("100000000.00000001", 10000000000000001)]:
            self.assertEqual(amount_to_pokes(Decimal(coins)), pokes, coins)

    def test_the_old_float_path_would_have_been_wrong(self):
        # Pins that these cases are actually float-unsafe, so the test above is
        # not vacuous if someone reintroduces a float parser.
        for coins, pokes in [("90071992.54740993", 9007199254740993),
                             ("100000000.00000001", 10000000000000001)]:
            self.assertNotEqual(int(round(float(coins) * 100_000_000)), pokes)

    def test_accepts_the_types_the_daemon_may_send(self):
        # Decimal is what parse_float=Decimal hands us; str and int are
        # tolerated because a caller may pass either. 1 and 1.5 are whole
        # coins, so 1.5 pokes is not a case that should exist.
        for raw in (Decimal("1.5"), "1.5", 1.5):
            self.assertEqual(amount_to_pokes(raw), 150_000_000, repr(raw))
        self.assertEqual(amount_to_pokes(2), 200_000_000)
        self.assertEqual(amount_to_pokes("0"), 0)

    def test_a_sub_poke_amount_is_an_error_not_a_truncation(self):
        # 9 decimals is not representable in pokes. Truncating quietly would
        # be how a supply figure goes wrong with no trace.
        for bad in ("0.000000001", "1.123456789", Decimal("0.5e-8")):
            with self.assertRaises(ValueError):
                amount_to_pokes(Decimal(bad))

    def test_an_out_of_range_amount_is_rejected(self):
        for bad in ("92233720369.547709", "1e30", "-92233720369.547709"):
            with self.assertRaises(ValueError):
                amount_to_pokes(Decimal(bad))

    def test_the_rpc_client_decodes_floats_as_decimal(self):
        # json.load's default float parser corrupts the value before any
        # amount parser sees it, so the client has to opt in or the fix in
        # amount_to_pokes is unreachable. This drives the real _post against a
        # stub HTTP server, not a local json.loads, so dropping the
        # parse_float=Decimal keyword in rpc.py actually fails.
        import http.server
        import threading
        from rpc import RPC

        body = (b'{"result": {"value": 90071992.54740993}, "error": null,'
                b' "id": 1}')

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        httpd = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        port = httpd.server_address[1]
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        self.addCleanup(httpd.server_close)
        self.addCleanup(httpd.shutdown)

        got = RPC(port=port).call("getblockcount")
        self.assertIsInstance(got["value"], Decimal)
        self.assertEqual(amount_to_pokes(got["value"]), 9007199254740993)

    def test_tx_from_verbose_keeps_a_large_amount_exact(self):
        t = tx_from_verbose(
            {"txid": "t", "vin": [], "size": 100, "version": 1,
             "vout": [{"value": Decimal("90071992.54740993"),
                       "scriptPubKey": {"type": "pubkeyhash", "hex": "76a914",
                                        "reqSigs": 1, "addresses": ["a"]}}]},
            1, 0)
        self.assertEqual(t.vout[0].value, 9007199254740993)

    def test_poke_is_exact_past_the_float_mantissa(self):
        # "%.8f" % (v / COIN) rendered 9007199254740993 as ...94.
        self.assertEqual(poke(9007199254740993), "90071992.54740993")
        self.assertEqual(poke(-9007199254740993), "-90071992.54740993")
        self.assertEqual(poke(2 ** 63 - 1), "92233720368.54775807")

    def test_poke_keeps_the_old_formatting_everywhere_else(self):
        # The format is part of the API, so pin it rather than just the fix.
        for v, s in [(0, "0.00000000"), (1, "0.00000001"),
                     (99_999_999, "0.99999999"), (100_000_000, "1.00000000"),
                     (123_456_789, "1.23456789"), (-1, "-0.00000001"),
                     (-100_000_001, "-1.00000001"),
                     (123_456_789_012_345, "1234567.89012345")]:
            self.assertEqual(poke(v), s)
            self.assertEqual(len(poke(v).split(".")[1]), 8)

    def test_poke_round_trips_through_amount_to_pokes(self):
        for v in (0, 1, 99_999_999, 123_456_789, 2 ** 53 + 1, 2 ** 63 - 1,
                  -(2 ** 53 + 1)):
            self.assertEqual(amount_to_pokes(poke(v)), v)

    # A real header from phoenixcoind (regtest/mainnet alike), captured
    # verbatim. The difficulty value is the reason: parse_float=Decimal makes
    # it a Decimal, and no driver will bind one into a numeric column, which
    # broke indexing against a live daemon. Every test above builds its own
    # payload, so none of them saw this.
    REAL_BLOCK = {
        "bits": "1e00c58f", "confirmations": 1, "difficulty": Decimal("0.00506171"),
        "hash": "d88b18ceff594924be801c235a6333ae63312774", "height": 406882,
        "merkleroot": "e91376a2c6bbbea8be9c15ec9fc1dd08bbb0cb",
        "nonce": 3843639,
        "previousblockhash": "8e6c47a3f9d471f44db8a0c17f510b6911dfba85",
        "size": 118530, "time": 1790568841, "version": 2,
        "tx": ["320d815bb785" + "0" * 52],
    }

    def test_a_real_header_stores_without_a_binding_error(self):
        from indexer import Block
        db = DB(self.add_schema_dsn())
        self.addCleanup(db.conn.close)
        b = Block(self.REAL_BLOCK)
        db.add_block(b)          # used to raise on the Decimal difficulty
        row = db.query("SELECT height, difficulty, size, version FROM blocks")[0]
        self.assertEqual(row[0], 406882)
        self.assertAlmostEqual(row[1], 0.00506171, places=8)
        self.assertEqual(row[2], 118530)
        self.assertEqual(row[3], 2)

    def test_difficulty_is_a_float_not_a_decimal(self):
        from indexer import Block
        d = Block(self.REAL_BLOCK).difficulty
        self.assertIsInstance(d, float)
        self.assertNotIsInstance(d, Decimal)

    def test_a_missing_difficulty_is_none_not_an_error(self):
        from indexer import Block
        j = dict(self.REAL_BLOCK)
        del j["difficulty"]
        self.assertIsNone(Block(j).difficulty)

    def test_a_real_coinbase_value_parses_to_the_expected_pokes(self):
        # 22.69583665 PXC, from phoenixcoind's getrawtransaction. Exercises the
        # hybrid_pubkeyhash script type this fork actually returns.
        j = {"txid": "t", "vin": [{"coinbase": "03a%06x" % 1}], "size": 200,
             "version": 2, "locktime": 0,
             "vout": [{"value": Decimal("22.69583665"),
                       "scriptPubKey": {
                           "type": "hybrid_pubkeyhash",
                           "hex": "76a914" + "11" * 20 + "88ac",
                           "reqSigs": 1,
                           "addresses": ["QV8K3XeRo7dNCdQkvK5hjRURR5Uv8xa93g"]}}]}
        t = tx_from_verbose(j, 406882, 0)
        self.assertEqual(t.vout[0].value, 2269583665)
        self.assertEqual(t.vout[0].type, "hybrid_pubkeyhash")

    def test_a_zero_value_output_is_allowed(self):
        # The daemon sends 0E-8 for a zero-valued output; Decimal exponent form
        # must not be mistaken for a malformed amount.
        self.assertEqual(amount_to_pokes(Decimal("0E-8")), 0)


class RpcBatchAlignmentTest(unittest.TestCase):
    """The real batch client, against a stub HTTP server that misbehaves.

    A batch is answered by a list (or a lone object). Every reply must answer
    one of the calls we sent; duplicated, unexpected or non-object replies mean
    the frame cannot be trusted to line up with the calls, and must raise in
    either mode rather than silently mis-pair results.
    """

    def serve(self, payload):
        """Run a stub HTTP server answering every request with `payload`
        (a JSON-encodable object) and return an RPC pointed at it."""

        import http.server
        import threading
        from rpc import RPC

        data = json.dumps(payload).encode()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *a):
                pass

        httpd = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(httpd.server_close)
        self.addCleanup(httpd.shutdown)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        return RPC(port=httpd.server_address[1])

    def test_duplicate_ids_raise_in_either_mode(self):
        rpc = self.serve([{"id": 0, "result": "a"}, {"id": 0, "result": "b"}])
        for strict in (True, False):
            with self.assertRaises(RPCError):
                rpc.batch([("m", (1,)), ("m", (2,))], strict=strict)

    def test_an_unexpected_id_is_rejected(self):
        rpc = self.serve([{"id": 999, "result": "x"}])
        with self.assertRaises(RPCError):
            rpc.batch([("m", (1,))], strict=False)

    def test_a_non_object_reply_is_rejected(self):
        rpc = self.serve([None, {"id": 1, "result": "x"}])
        with self.assertRaises(RPCError):
            rpc.batch([("m", (1,)), ("m", (2,))], strict=False)

    def test_a_string_id_is_rejected(self):
        rpc = self.serve([{"id": "0", "result": "x"}])
        with self.assertRaises(RPCError):
            rpc.batch([("m", (1,))])

    def test_reordered_ids_still_line_up_with_their_calls(self):
        rpc = self.serve([{"id": 1, "result": "second"},
                          {"id": 0, "result": "first"}])
        self.assertEqual(rpc.batch([("m", (1,)), ("m", (2,))]),
                         ["first", "second"])

    def test_a_lone_object_reply_is_accepted(self):
        rpc = self.serve({"id": 0, "result": "solo"})
        self.assertEqual(rpc.batch([("m", (1,))]), ["solo"])

    def test_a_missing_response_is_a_slot_error_not_a_crash(self):
        rpc = self.serve([{"id": 0, "result": "only"}])
        out = rpc.batch([("m", (1,)), ("m", (2,))], strict=False)
        self.assertEqual(out[0], "only")
        self.assertIsInstance(out[1], CallError)


class RecentBlocksTest(DBTestCase):
    """recent_blocks() is one query, and agrees with block()."""

    def add_blocks(self, heights, txs_per=3):
        from indexer import Block
        for h in heights:
            txids = ["%064x" % (h * 100 + i) for i in range(txs_per)]
            self.db.add_block(Block({"height": h, "hash": "%064x" % h,
                                     "time": 1700000000 + h, "tx": txids}))
            for i, txid in enumerate(txids):
                self.db.add_tx(tx(txid, h, [("addr%d" % (h % 3), POKE, (h % 5) + 1)],
                                  coinbase=(i == 0)))

    def test_it_issues_one_query_not_one_per_block(self):
        self.add_blocks(range(100, 120))
        calls = []
        real = DB.query

        def spy(db, sql, params=()):
            calls.append(sql)
            return real(db, sql, params)

        with mock.patch.object(DB, "query", spy):
            blocks = self.explorer.recent_blocks()
        self.assertEqual(len(blocks), 20)
        self.assertEqual(len(calls), 1, [c[:60] for c in calls])

    def test_the_counts_match_block(self):
        self.add_blocks(range(100, 106), txs_per=4)
        for b in self.explorer.recent_blocks():
            self.assertEqual(b["n_txs"], len(self.explorer.block(str(b["height"]))["txs"]),
                             "height %d" % b["height"])

    def test_a_block_with_no_txs_is_still_listed(self):
        # Guards the count, not the join: a tx-less block must appear with 0.
        self.add_blocks(range(100, 104))
        from indexer import Block
        self.db.add_block(Block({"height": 104, "hash": "%064x" % 104,
                                 "time": 1, "tx": []}))
        rows = {b["height"]: b["n_txs"] for b in self.explorer.recent_blocks()}
        self.assertEqual(rows[104], 0)
        self.assertEqual(rows[103], 3)

    def test_orphaned_txs_do_not_count_toward_a_new_block(self):
        # clear_from() leaves tombstones carrying their old height. Once a
        # different block is indexed at that height, an unfiltered COUNT(*)
        # attributes the dead txs to the new block.
        from indexer import Block
        self.add_blocks([100, 101], txs_per=3)
        self.db.clear_from(101)
        self.db.add_block(Block({"height": 101, "hash": "%064x" % 101,
                                 "time": 1, "tx": []}))
        self.assertEqual(
            self.db.query("SELECT COUNT(*) FROM txs WHERE height=101")[0][0], 3,
            "the tombstones are still in txs at that height")
        b = self.explorer.recent_blocks()[0]
        self.assertEqual(b["n_txs"], 0)
        self.assertEqual(b["n_txs"], len(self.explorer.block("101")["txs"]))

    def test_mempool_txs_do_not_count_toward_a_block(self):
        self.add_blocks([100, 101], txs_per=2)
        self.db.add_tx(tx("M", None, [("addr0", POKE, 1)]))
        rows = {b["height"]: b["n_txs"] for b in self.explorer.recent_blocks()}
        self.assertEqual(rows[101], 2)
        self.assertEqual(rows[100], 2)

    def test_the_order_is_still_tip_first(self):
        self.add_blocks(range(100, 110), txs_per=1)
        heights = [b["height"] for b in self.explorer.recent_blocks()]
        self.assertEqual(heights, sorted(heights, reverse=True))
        self.assertEqual(heights[0], 109)

    def test_every_block_field_survives_the_rewrite(self):
        self.add_blocks([100, 101], txs_per=1)
        for b in self.explorer.recent_blocks():
            self.assertEqual(sorted(b), sorted(
                ["height", "hash", "version", "merkleroot", "time", "nonce",
                 "bits", "difficulty", "size", "prev_hash", "next_hash",
                 "n_txs"]))


if __name__ == "__main__":
    unittest.main()


class BulkWindowHazardTest(DBTestCase):
    """What may not happen to writes still sitting in an open bulk window.

    add_tx no longer writes as it goes. Inside `bulk()` a tx's rows, its
    spent-flag refresh and its counter deltas are all held back and written
    once at the end, which is where the speed came from -- and it means the
    window can be caught mid-flight by something that assumes the table
    already holds what it just handed over. These are the ways that can
    happen, each of which the buffering has to get right:

      - the same txid indexed twice in one window (last one must win, and
        must not collide with its own buffered rows);
      - a reorg truncating the chain from inside the window;
      - a mempool eviction removing rows added in this same window;
      - a read of the table or of a counter before the window closes;
      - the window raising, which must leave nothing behind.

    A wrong answer here is silent -- a doubled balance, a supply that drifts,
    rows resurrected by a truncation -- so each case is checked against the
    derived query as well as against a literal.
    """

    def block(self, height, tag=None):
        from indexer import Block
        tag = tag or "b%d" % height
        self.db.add_block(Block({"height": height, "hash": tag,
                                 "time": 1700000000 + height, "tx": []}))
        return tag

    def test_buffered_rows_reach_the_table_when_the_window_closes(self):
        # The rows are held in memory until the flush, so the table does not
        # have them yet -- that is the whole of what makes this fast. What has
        # to be true is that they arrive, complete, at commit.
        with self.db.bulk():
            self.block(1)
            self.db.add_tx(tx("T1", 1, [("a", 10 * POKE, 0)], [("prev", 0)]))
            self.assertEqual(
                self.db.conn.execute(
                    "SELECT count(*) FROM txs WHERE txid='T1'").fetchone()[0], 0)
        for table in ("txs", "vin", "vout"):
            self.assertEqual(
                self.db.conn.execute(
                    "SELECT count(*) FROM %s WHERE txid='T1'" % table
                ).fetchone()[0], 1, table)
        self.assertMatchesDerived("after the window committed")

    def test_an_uncommitted_window_is_invisible_to_another_connection(self):
        # Work in flight is not chain yet. A second connection must not see a
        # block the first is still indexing, or a reader would serve a chain
        # tip that a failed sync then rolls back.
        other = psycopg.connect(self.db_path, autocommit=True)
        self.addCleanup(other.close)
        with self.db.bulk():
            self.block(1)
            self.db.add_tx(tx("T1", 1, [("a", 10 * POKE, 0)], [("prev", 0)]))
            self.assertEqual(other.execute(
                "SELECT count(*) FROM blocks").fetchone()[0], 0)
            self.assertEqual(other.execute(
                "SELECT count(*) FROM txs WHERE txid='T1'").fetchone()[0], 0)
        self.assertEqual(other.execute(
            "SELECT count(*) FROM txs WHERE txid='T1'").fetchone()[0], 1)

    def test_a_window_that_raises_leaves_nothing_behind(self):
        self.block(1)
        with self.assertRaises(RuntimeError):
            with self.db.bulk():
                self.db.add_tx(tx("T1", 1, [("a", 10 * POKE, 0)],
                                  [("prev", 0)]))
                raise RuntimeError("sync aborted mid-window")
        self.assertEqual(
            self.db.conn.execute(
                "SELECT count(*) FROM txs WHERE txid='T1'").fetchone()[0], 0)
        self.assertEqual(self.db.conn.execute(
            "SELECT count(*) FROM vout WHERE txid='T1'").fetchone()[0], 0)
        self.assertMatchesDerived("after a rolled-back window")

    def test_the_same_txid_twice_in_one_window_keeps_only_the_last(self):
        # Re-index: the second add_tx replaces the first outright. Buffered
        # rows are keyed by txid for this -- two buffered versions of one txid
        # would insert twice and collide on the primary key, or leave the old
        # rows behind to be counted twice.
        self.block(1)
        self.db.add_tx(tx("T1", 1, [("a", 10 * POKE, 0)], [("prev", 0)]))
        with self.db.bulk():
            self.db.add_tx(tx("T1", 1, [("b", 20 * POKE, 1)], [("prev", 0)]))
            self.db.add_tx(tx("T1", 1, [("c", 30 * POKE, 2)], [("prev", 0)]))
        self.assertEqual(self.db.conn.execute(
            "SELECT count(*) FROM txs WHERE txid='T1'").fetchone()[0], 1)
        self.assertEqual(self.db.conn.execute(
            "SELECT count(*) FROM vout WHERE txid='T1'").fetchone()[0], 1)
        self.assertEqual(json.loads(self.db.conn.execute(
            "SELECT addresses FROM vout WHERE txid='T1'").fetchone()[0]),
            ["c"])
        self.assertMatchesDerived("after a double re-index in one window")

    def test_a_read_inside_the_window_sees_the_rows_handed_to_it(self):
        # A read inside the window would otherwise see the table without the
        # rows still sitting in the buffer, and answer as if the block never
        # arrived.
        self.block(1)
        with self.db.bulk():
            self.db.add_tx(tx("T1", 1, [("a", 10 * POKE, 0)], [("prev", 0)]))
            self.assertEqual(
                self.db.query("SELECT count(*) FROM vout WHERE txid='T1'"
                              )[0][0], 1)
            self.assertEqual(
                self.db.query("SELECT count(*) FROM vin WHERE txid='T1'"
                              )[0][0], 1)

    def test_a_counter_read_inside_the_window_sees_the_window(self):
        # Same hazard on the maintained counters: reading n_blocks with the
        # block still buffered would answer the height from before it.
        self.assertEqual(self.db.n_blocks(), 0)
        with self.db.bulk():
            self.block(1)
            self.assertEqual(self.db.n_blocks(), 1)
        self.assertEqual(self.db.n_blocks(), 1)

    def test_a_reorg_from_inside_the_window_truncates_what_the_window_wrote(self):
        # The dangerous one. A reorg calls clear_from while the window still
        # holds rows for the very blocks being truncated. If the truncation ran
        # first and the buffer flushed after, the flush would re-insert the
        # rows the truncation just removed, inside the same transaction.
        for h in (1, 2, 3):
            self.block(h)
            self.db.add_tx(tx("T%d" % h, h, [("a", h * POKE, 0)], [("prev", 0)]))
        self.assertEqual(self.db.n_blocks(), 3)
        with self.db.bulk():
            self.block(4)
            self.db.add_tx(tx("T4", 4, [("a", 4 * POKE, 0)], [("prev", 0)]))
            self.db.clear_from(3)
        self.assertEqual(self.db.n_blocks(), 2)
        # A truncated confirmed tx becomes a tombstone, not a deletion: the rows
        # that would leak into address queries are severed, the tx stays.
        self.assertEqual(self.db.conn.execute(
            "SELECT status FROM txs WHERE txid='T4'").fetchone()[0],
            "orphaned")
        for table in ("vin", "vout", "addr_out"):
            self.assertEqual(
                self.db.conn.execute(
                    "SELECT count(*) FROM %s WHERE txid='T4'" % table
                ).fetchone()[0], 0, table)
        self.assertMatchesDerived("after an in-window reorg")

    def test_an_eviction_from_inside_the_window_removes_what_the_window_wrote(self):
        # A mempool tx added in this window, then evicted before it closed: it
        # must end up gone, not re-inserted by the flush.
        with self.db.bulk():
            self.block(1)
            self.db.add_tx(tx("C1", 1, [("miner", 50 * POKE, 0)], coinbase=True))
            self.db.add_tx(tx("M1", None, [("pay", 7 * POKE, 0)], [("C1", 0)]))
            self.assertEqual(self.db.total_coinbase(), 50 * POKE)
            self.db.remove_txs(["M1"])
        self.assertEqual(
            self.db.conn.execute(
                "SELECT count(*) FROM txs WHERE txid='M1'").fetchone()[0], 0)
        self.assertEqual(self.db.total_coinbase(), 50 * POKE)
        # The coinbase the evicted tx was spending is unspent again, not left
        # marked spent by a spender that no longer exists.
        self.assertEqual(self.spent_by("C1", 0), 0)
        self.assertMatchesDerived("after an in-window eviction")

    def test_a_spend_chain_added_in_one_window_marks_spent_across_it(self):
        # The spent flags are derived by an EXISTS over the vin table, so
        # deferring them is only safe if they see every tx in the window, not
        # only the ones written before the flush started. Two txs where the
        # second spends the first is the case that would break: A->B added in
        # one window must leave A's output spent by B, exactly as if each add
        # had committed on its own.
        with self.db.bulk():
            for h in (1, 2, 3):
                self.block(h)
                self.db.add_tx(tx("C%d" % h, h, [("miner", 50 * POKE, 0)],
                                  coinbase=True))
            self.db.add_tx(tx("P1", 2, [("a", 7 * POKE, 0)], [("C1", 0)]))
            self.db.add_tx(tx("P2", 3, [("b", 7 * POKE, 1)], [("P1", 0)]))
        self.assertEqual(self.spent_by("C1", 0), 1)
        self.assertEqual(self.spent_by("P1", 0), 1)
        self.assertMatchesDerived("after a chained window")

    def test_the_same_window_written_twice_over_reaches_the_same_state(self):
        # The property that actually matters: the window is an optimisation,
        # so it must not change the answer. Index a chain in one transaction
        # and again tx by tx, and the two databases must agree completely --
        # rows, flags and counters.
        from indexer import Block

        def chain(conn_db):
            for h in (1, 2, 3):
                conn_db.add_block(Block({"height": h, "hash": "b%d" % h,
                                        "time": 1, "tx": []}))
                conn_db.add_tx(tx("C%d" % h, h, [("miner", 50 * POKE, 0)],
                                  coinbase=True))
            conn_db.add_tx(tx("P1", 2, [("a", 7 * POKE, 0)], [("C1", 0)]))
            conn_db.add_tx(tx("P2", 3, [("b", 7 * POKE, 1)], [("P1", 0)]))
            # And a re-index of P1 in place, which is the case the txid keying
            # of the buffer exists for.
            conn_db.add_tx(tx("P1", 2, [("z", 9 * POKE, 2)], [("C1", 0)]))

        bulk_db = self.fresh_db()
        with bulk_db.bulk():
            chain(bulk_db)
        chain(self.db)

        for sql in ("SELECT * FROM txs ORDER BY txid",
                    "SELECT * FROM vout ORDER BY txid, n",
                    "SELECT * FROM vin ORDER BY txid, n",
                    "SELECT * FROM addr_out ORDER BY address, txid, n",
                    "SELECT * FROM blocks ORDER BY height"):
            self.assertEqual(bulk_db.conn.execute(sql).fetchall(),
                             self.db.conn.execute(sql).fetchall(), sql)
        self.assertEqual(bulk_db.total_coinbase(), self.db.total_coinbase())
        self.assertEqual(bulk_db.n_blocks(), self.db.n_blocks())
        def flag(conn_db, sig, n):
            rows = conn_db.query(
                "SELECT spent_by FROM addr_out WHERE txid=? AND n=?", (sig, n))
            return None if not rows else rows[0][0]
        for sig in ("C1", "C2", "C3", "P1", "P2"):
            for n in (0, 1):
                self.assertEqual(flag(bulk_db, sig, n), flag(self.db, sig, n),
                                 "%s:%d" % (sig, n))
