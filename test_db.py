"""Regression tests. Run: python3 -m unittest test_db -v

The mempool-eviction cases matter because removing a chain of txs (A -> B -> C)
leaves addr_out.is_spent describing whichever mempool the index happened to be
holding when each delete ran. The property under test is that the final state is
the same whatever order the removals happen in, so the ordering test sweeps
every permutation rather than one hand-picked sequence.
"""

import contextlib
import itertools
import random
import os
import shutil
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

import db as db_module
import server as server_module
from decimal import Decimal
from db import DB, script_hash_of
from indexer import (Block, Indexer, In, Out, Tx, amount_to_pokes,
                     tx_from_verbose)
from rpc import RPCError
from server import DBPool, Explorer, poke

POKE = 100_000_000


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
        for method, params in calls:
            try:
                out.append(getattr(self, method)(*params))
            except RPCError:
                if strict:
                    raise
                out.append(None)
        return out


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
    """Ground truth for addr_out.is_spent, recomputed from vin for the whole
    table. Any write path, whatever order it ran in, must agree with this."""
    db.conn.execute("UPDATE addr_out SET is_spent=0")
    db.conn.execute(
        """UPDATE addr_out SET is_spent=1 WHERE (txid, n) IN
           (SELECT prev_txid, prev_vout FROM vin WHERE prev_txid IS NOT NULL)""")


class DBTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = self._new_dir()
        self.db_path = os.path.join(self.dir, "test.db")
        self.db = self.fresh_db(self.db_path)
        self.indexer = Indexer(self.db, rpc=None)
        self.explorer = Explorer(self.db)

    def _new_dir(self):
        path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, path, ignore_errors=True)
        return path

    def fresh_db(self, path=None):
        """A DB of its own, for tests that need more than one."""
        if path is None:
            path = os.path.join(self._new_dir(), "test.db")
        db = DB(path)
        self.addCleanup(db.conn.close)
        return db

    def is_spent(self, txid, n=0):
        """is_spent of an output, or None when the output is gone."""
        rows = self.db.query(
            "SELECT is_spent FROM addr_out WHERE txid=? AND n=?", (txid, n))
        return None if not rows else bool(rows[0][0])

    def spent_flags(self):
        return dict(self.db.query("SELECT txid, is_spent FROM addr_out"))

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
                    dict(db.query("SELECT txid, is_spent FROM addr_out")),
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
    """Id lists longer than SQLite's bind cap must not fail.

    SQLITE_LIMIT_VARIABLE_NUMBER is compiled in -- 999 on older builds, 32766
    since 3.32 -- and a statement over it raises "too many SQL variables" rather
    than degrading. The caller's transaction rolls that back whole, so the
    effect is a permanently stuck indexer: a mempool too big to evict, or a
    reorg too deep to apply, retried forever. The limit is lowered here instead
    of indexing a 32k-tx mempool, so the test bites on every host.
    """

    LIMIT = getattr(sqlite3, "SQLITE_LIMIT_VARIABLE_NUMBER", None)
    NEEDS_SETLIMIT = not hasattr(sqlite3.Connection, "setlimit")

    def setUp(self):
        super().setUp()
        if self.NEEDS_SETLIMIT:
            self.skipTest("needs Connection.setlimit (Python 3.11+)")
        self.capped = self.db.conn.getlimit(self.LIMIT)

    def cap_variables(self, n):
        self.db.conn.setlimit(self.LIMIT, n)

    def uncapped(self):
        self.db.conn.setlimit(self.LIMIT, self.capped)

    def fill_mempool(self, n):
        """n mempool txs, all one address, none spending anything."""
        with self.db.bulk():
            for i in range(n):
                self.db.add_tx(tx("M%05d" % i, None,
                                  [("addr%d" % (i % 97), POKE, i + 1)],
                                  coinbase=True))
        return ["M%05d" % i for i in range(n)]

    def counts(self):
        return [self.db.query("SELECT COUNT(*) FROM " + t)[0][0]
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
        self.assertEqual([db.query("SELECT COUNT(*) FROM " + t)[0][0]
                          for t in ("txs", "vin", "vout", "addr_out", "scripts")],
                         chunked)

    def test_a_chunked_eviction_is_still_one_transaction(self):
        # Chunking inside the transaction must not make a failure partial: the
        # spent flags and the scripts are only consistent because the whole
        # eviction either lands or does not.
        names = self.build_chain(3)
        self.cap_variables(2)                     # far below one chunk
        with mock.patch.object(DB, "_refresh_spent_flags",
                               side_effect=sqlite3.OperationalError("boom")):
            with self.assertRaises(sqlite3.OperationalError):
                self.db.remove_txs(list(names))
        self.assertEqual(self.counts(), [3, 3, 3, 3, 3], "half-evicted")
        self.assertEqual(self.spent_flags(), {"A": 1, "B": 1, "C": 0})

    def test_an_eviction_does_not_cost_a_statement_per_stale_tx(self):
        # These txs spend nothing, so the spent-flag recompute has no work and
        # every statement here is one of the batched ones. (An eviction whose
        # txs do spend pays one UPDATE per output spent, which is inherent.)
        stale = self.fill_mempool(1000)
        seen = []
        self.db.conn.set_trace_callback(seen.append)
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


class MigrationTest(DBTestCase):
    def _demote_to_v2(self, n_txs=3):
        """Put the DB back before the vout.script_hash backfill: no such column,
        every script_hex present and unhashed."""
        for i in range(n_txs):
            self.db.add_tx(tx(chr(ord("A") + i), 100 + i,
                              [("addr%d" % i, POKE, i + 1)], coinbase=True))
        self.db.conn.execute("DROP TABLE scripts")
        self.db.conn.execute(
            """CREATE TABLE scripts (
                 script_hash TEXT PRIMARY KEY, type TEXT, req_sigs INTEGER,
                 addresses TEXT, value_received INTEGER DEFAULT 0,
                 value_spent INTEGER DEFAULT 0, n_vout INTEGER DEFAULT 0,
                 n_spent INTEGER DEFAULT 0, created_height INTEGER,
                 last_height INTEGER)""")
        self.db.conn.execute("UPDATE meta SET value='scripts_v2' "
                             "WHERE key='schema_version'")
        # add_tx fills script_hash in; clear it so the backfill has work to do.
        self.db.conn.execute("UPDATE vout SET script_hash=NULL")
        self.db.conn.commit()
        self.db.conn.close()

    def test_the_backfill_hashes_each_script_once(self):
        # The vout backfill runs SHA256+RIPEMD160 per row on a large existing
        # database, so it must not hash twice per row.
        self._demote_to_v2(3)
        real = db_module.script_hash_of
        calls = []

        def counting(hx):
            calls.append(hx)
            return real(hx)

        with mock.patch.object(db_module, "script_hash_of", counting):
            db = self.fresh_db(self.db_path)
        self.addCleanup(db.conn.close)
        hashed = [r[0] for r in db.query(
            "SELECT script_hex FROM vout WHERE script_hash IS NOT NULL")]
        self.assertEqual(len(calls), len(hashed))
        self.assertEqual(len(calls), 3)
        # and the values are right, not just the count
        for hx in hashed:
            self.assertIn(
                (real(hx),), db.query(
                    "SELECT script_hash FROM vout WHERE script_hex=?", (hx,)))

    def indexes_on(self, db, table):
        return sorted(r[0] for r in db.query(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
            (table,)))

    def _demote_to_redundant_indexes(self):
        """Recreate the database these indexes came from: both present, neither
        meta flag set."""
        for sql in ("CREATE INDEX idx_addr_out_address ON addr_out(address)",
                    "CREATE INDEX idx_vout_address ON vout(addresses)"):
            self.db.conn.execute(sql)
        self.db.conn.execute(
            "DELETE FROM meta WHERE key IN"
            " ('addr_out_address_index','vout_address_index')")
        self.db.conn.commit()
        self.db.conn.close()

    def test_neither_redundant_index_is_created(self):
        # addr_out's key is (address, txid, n), so the address prefix is already
        # seekable and a second index on it stores that column twice. vout's
        # addresses is a JSON array no query filters on.
        self.assertEqual(self.indexes_on(self.db, "addr_out"),
                         ["idx_addr_out_txid_n", "sqlite_autoindex_addr_out_1"])
        self.assertEqual(self.indexes_on(self.db, "vout"),
                         ["idx_vout_script_hash", "sqlite_autoindex_vout_1"])

    def test_the_primary_key_serves_the_address_lookup(self):
        # Why the addr_out index is redundant, as a permanent check rather than a
        # one-off EXPLAIN QUERY PLAN: address_balances() must reach the primary
        # key's autoindex, which is what makes dropping the index safe.
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        plans = [r[-1] for r in self.db.conn.execute(
            "EXPLAIN QUERY PLAN SELECT SUM(a.value) FROM addr_out a"
            " JOIN txs t ON t.txid = a.txid WHERE a.address = ?", ("addr0",))]
        self.assertTrue(any("sqlite_autoindex_addr_out_1" in p for p in plans), plans)
        self.assertFalse(any("idx_addr_out_address" in p for p in plans), plans)

    def test_an_existing_database_loses_both_of_them(self):
        # A fresh schema alone would not remove them: CREATE INDEX IF NOT EXISTS
        # never drops anything, so an indexer that ran before this change keeps
        # both indexes until something migrates it.
        self._demote_to_redundant_indexes()
        db = self.fresh_db(self.db_path)
        self.assertEqual(self.indexes_on(db, "addr_out"),
                         ["idx_addr_out_txid_n", "sqlite_autoindex_addr_out_1"])
        self.assertEqual(self.indexes_on(db, "vout"),
                         ["idx_vout_script_hash", "sqlite_autoindex_vout_1"])
        for _, flag in DB.REDUNDANT_INDEXES:
            self.assertEqual(db.get_meta(flag), "dropped")

    def test_dropping_them_costs_no_scripts_rebuild(self):
        # Why the drops are keyed on their own meta flags: schema_version drives
        # a full rebuild_scripts(), which has no business running to reclaim
        # indexes on an up-to-date database.
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        self._demote_to_redundant_indexes()
        with mock.patch.object(DB, "rebuild_scripts") as rebuild:
            db = self.fresh_db(self.db_path)
        rebuild.assert_not_called()
        self.assertNotIn("idx_vout_address", self.indexes_on(db, "vout"))
        self.assertEqual(
            db.address_balances("addr0")["confirmed"]["n_outputs"], 1)

    def test_v1_counters_are_rebuilt_not_trusted(self):
        """A pre-v3 table's counters were derived data: discard, recompute."""
        self.db.add_tx(tx("A", 100, [("addr0", POKE, 1)], coinbase=True))
        self.db.add_tx(tx("B", None, [("addr1", 9 * POKE // 10, 2)],
                          spends=[("A", 0)]))
        self.db.conn.execute("DROP TABLE scripts")
        self.db.conn.execute(
            """CREATE TABLE scripts (
                 script_hash TEXT PRIMARY KEY, type TEXT, req_sigs INTEGER,
                 addresses TEXT, value_received INTEGER DEFAULT 0,
                 value_spent INTEGER DEFAULT 0, n_vout INTEGER DEFAULT 0,
                 n_spent INTEGER DEFAULT 0, created_height INTEGER,
                 last_height INTEGER)""")
        self.db.conn.execute(
            "INSERT INTO scripts VALUES (?,?,?,?,999,777,9,9,100,100)",
            (script_hash_of(script_hex(1)), "pubkeyhash", 1, '["addr0"]'))
        self.db.conn.execute("UPDATE meta SET value='scripts_v2' "
                             "WHERE key='schema_version'")
        # add_tx fills script_hash in; clear it so the backfill has work to do.
        self.db.conn.execute("UPDATE vout SET script_hash=NULL")
        self.db.conn.commit()
        self.db.conn.close()

        db = self.fresh_db(self.db_path)
        self.assertEqual(db.get_meta("schema_version"), DB.SCHEMA_VERSION)
        self.assertNotIn(
            "value_received",
            [r[1] for r in db.query("PRAGMA table_info(scripts)")])
        b = Explorer(db).script(script_hash_of(script_hex(1)))
        self.assertEqual(b["confirmed"]["value_received"], POKE)
        self.assertEqual(b["live"]["value_spent"], POKE)
        self.assertEqual(b["created_height"], 100)

    def test_reopening_a_migrated_db_is_a_no_op(self):
        path = self.db_path
        self.db.conn.close()
        db = self.fresh_db(path)
        self.assertEqual(db.get_meta("schema_version"), DB.SCHEMA_VERSION)
        db.conn.close()
        again = self.fresh_db(path)
        self.assertEqual(again.get_meta("schema_version"), DB.SCHEMA_VERSION)


class ConnectionSetupTest(DBTestCase):
    """initialize() owns DDL; connect() must never run it."""

    def tables(self, db):
        return {r[0] for r in db.query(
            "SELECT name FROM sqlite_master WHERE type='table'")}

    def test_connect_creates_no_schema(self):
        path = os.path.join(self._new_dir(), "cold.db")
        self.assertFalse(os.path.exists(path))
        db = DB.connect(path)          # read side on a DB that does not exist
        self.addCleanup(db.conn.close)
        self.assertEqual(self.tables(db), set())  # nothing was created

    def test_connect_does_not_migrate_an_old_db(self):
        # A v2 database opened through connect() keeps its old shape: the read
        # path must not rewrite the schema the indexer owns.
        path = os.path.join(self._new_dir(), "old.db")
        self.db.conn.close()
        # closing(), not `with`: sqlite3's context manager ends a transaction,
        # it does not close the connection.
        with contextlib.closing(sqlite3.connect(path)) as raw:
            raw.executescript("CREATE TABLE meta (key TEXT PRIMARY KEY,"
                              " value TEXT)")
            raw.execute("INSERT INTO meta VALUES ('schema_version','v2')")
            raw.execute("CREATE TABLE scripts (script_hash TEXT PRIMARY KEY,"
                        " value_received INTEGER)")
            raw.commit()
        db = DB.connect(path)
        self.addCleanup(db.conn.close)
        self.assertIn("value_received",
                      [r[1] for r in db.query("PRAGMA table_info(scripts)")])
        self.assertEqual(db.get_meta("schema_version"), "v2")

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


class BusyTimeoutTest(DBTestCase):
    """Long only while the schema is being migrated, short afterwards."""

    def timeout_of(self, db):
        return db.conn.execute("PRAGMA busy_timeout").fetchone()[0]

    def test_a_read_connection_waits_seconds_not_minutes(self):
        db = DB.connect(self.db_path)
        self.addCleanup(db.conn.close)
        self.assertEqual(self.timeout_of(db), DB.NORMAL_BUSY_TIMEOUT_MS)
        self.assertLess(self.timeout_of(db), 60_000)

    def test_the_migration_window_gets_the_long_timeout(self):
        # _migrate() must run under the long timeout, and the connection must
        # not keep it afterwards.
        seen = []
        real = DB._migrate

        def spy(inner):
            seen.append(self.timeout_of(inner))
            return real(inner)

        with mock.patch.object(DB, "_migrate", spy):
            db = DB.initialize(self.db_path)  # already current; still runs
        self.addCleanup(db.conn.close)
        self.assertEqual(seen, [DB.MIGRATION_BUSY_TIMEOUT_MS])
        self.assertEqual(self.timeout_of(db), DB.NORMAL_BUSY_TIMEOUT_MS)

    def test_an_explicit_timeout_is_honoured(self):
        db = DB(self.db_path, busy_timeout=1234)
        self.addCleanup(db.conn.close)
        self.assertEqual(self.timeout_of(db), 1234)

    def test_a_blocked_write_fails_fast_instead_of_hanging(self):
        holder = self.fresh_db(self.db_path)
        holder.conn.execute("BEGIN EXCLUSIVE")
        self.addCleanup(holder.conn.rollback)
        waiter = DB(self.db_path, busy_timeout=50)   # 50ms, same code path
        self.addCleanup(waiter.conn.close)
        with self.assertRaises(sqlite3.OperationalError) as cm:
            for i in range(50):
                waiter.conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
                                    ("k%d" % i, str(i)))
                waiter.conn.commit()
        self.assertIn("locked", str(cm.exception))


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
        return dict(self.db.query("SELECT txid, is_spent FROM addr_out"))

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


class AddOrderSpentFlagTest(DBTestCase):
    """is_spent must follow the vin table, not the order txs were added in.

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
                scoped = dict(db.query("SELECT txid, is_spent FROM addr_out"))
                full_spent_recompute(db)
                self.assertEqual(scoped, dict(db.query("SELECT txid, is_spent FROM addr_out")))
                self.assertEqual(scoped["P"], 1, "two spenders, one flag")

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
        scoped = dict(self.db.query("SELECT txid, is_spent FROM addr_out"))
        full_spent_recompute(self.db)
        self.assertEqual(
            scoped, dict(self.db.query("SELECT txid, is_spent FROM addr_out")))
        self.assertEqual(sum(1 for v in scoped.values() if v),
                         n - int(n * 0.6), "the survivors are still spent")

    def test_a_refresh_costs_a_statement_per_chunk_not_per_output(self):
        n = db_module.SQL_VAR_CHUNK * 2
        spenders = self.spend_fan(n)
        seen = []
        self.db.conn.set_trace_callback(seen.append)
        self.db.remove_txs(spenders)
        self.assertEqual(self.db.query("SELECT COUNT(*) FROM addr_out"
                                       " WHERE is_spent=1")[0][0], 0)
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
                             "EXPLAIN QUERY PLAN " + sql, params)], len(rows)))
            return rows

        with mock.patch.object(DB, "query", spy):
            self.indexer.rpc = daemon
            self.indexer.sync_mempool()
        return seen

    def test_the_membership_lookup_is_a_primary_key_seek(self):
        # The property that makes it cheap: no statement may fall back to
        # scanning the status index, which is a full pass over the chain.
        self.index_confirmed(500)
        for sql, _, plans, _ in self.queries_during(
                FakeDaemon(499, mempool=["m1", "m2"])):
            for plan in plans:
                self.assertNotIn("idx_txs_status_height", plan, sql)
                self.assertIn("sqlite_autoindex_txs_1", plan, sql)

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


class TotalCoinbaseCacheTest(DBTestCase):
    """The cache is keyed on the tip hash, so it cannot go stale."""

    def setUp(self):
        super().setUp()
        server_module._TOTAL_CACHE = None

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

    def test_the_repeated_value_is_served_from_cache(self):
        # Same tip hash => no rescan. Count the SUM statements, not the total
        # query count, so the tip-hash probe is not mistaken for a recompute.
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
        self.assertEqual(sums, [], "the sum was recomputed with a stable tip")

    def test_the_cache_does_not_leak_between_databases(self):
        # The cache is a module global; two DBs with different tips must not
        # share an entry, or a second server on another chain shows the first
        # chain's supply.
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


class AmountParsingTest(unittest.TestCase):
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
    # it a Decimal, and sqlite3 refuses to bind one, which broke indexing
    # against a live daemon with "Error binding parameter 8". Every test above
    # builds its own payload, so none of them saw this.
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
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        db = DB(os.path.join(d, "real.db"))
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
