"""SQLite storage for the PhoenixCoin Quantum explorer.

Amounts are stored as integers in "pokes" (COIN = 1e8), matching
src/util.h. Script types / addresses come verbatim from the daemon's
verbose getrawtransaction output, which already understands the hybrid
script templates and hybrid address prefixes.
"""

import hashlib
import json
import sqlite3

COIN = 100000000


def script_hash_of(script_hex):
    """Canonical script key: RIPEMD160(SHA256(scriptPubKey)), matching the
    daemon's CScriptID/Hash160. None for empty scripts."""
    if not script_hex:
        return None
    b = bytes.fromhex(script_hex)
    return hashlib.new("ripemd160", hashlib.sha256(b).digest()).hexdigest()


class _Bulk:
    """One big transaction opened on the DB connection."""

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        db = self.db
        assert not db._in_bulk
        db.conn.execute("BEGIN")
        db._in_bulk = True
        return self.db

    def __exit__(self, exc_type, exc, tb):
        db = self.db
        db._in_bulk = False
        if exc_type is None:
            db.conn.commit()
        else:
            db.conn.rollback()
        return False

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS blocks (
    height     INTEGER PRIMARY KEY,
    hash       TEXT UNIQUE NOT NULL,
    version    INTEGER,
    merkleroot TEXT,
    time       INTEGER,
    nonce      INTEGER,
    bits       TEXT,
    difficulty REAL,
    size       INTEGER,
    prev_hash  TEXT,
    next_hash  TEXT
);

CREATE TABLE IF NOT EXISTS txs (
    txid        TEXT PRIMARY KEY,
    height      INTEGER,          -- NULL while unconfirmed (mempool)
    tx_index    INTEGER,
    version     INTEGER,
    locktime    INTEGER,
    size        INTEGER,
    is_coinbase INTEGER,
    status      TEXT              -- 'confirmed' | 'mempool' | 'orphaned'
);

CREATE TABLE IF NOT EXISTS vin (
    txid       TEXT,
    n          INTEGER,
    prev_txid  TEXT,              -- NULL for coinbase
    prev_vout  INTEGER,           -- NULL for coinbase
    coinbase   TEXT,              -- hex, NULL unless coinbase
    script_asm TEXT,
    script_hex TEXT,
    sequence   INTEGER,
    PRIMARY KEY (txid, n)
);

CREATE TABLE IF NOT EXISTS vout (
    txid       TEXT,
    n          INTEGER,
    value      INTEGER,           -- in pokes (COIN = 1e8)
    type       TEXT,              -- e.g. pubkeyhash / scripthash / hybrid_pubkeyhash / ...
    addresses  TEXT,              -- JSON array of addresses (may include hybrid)
    req_sigs   INTEGER,
    script_asm TEXT,
    script_hex TEXT,
    PRIMARY KEY (txid, n)
);

CREATE TABLE IF NOT EXISTS addr_out (
    address TEXT,
    txid    TEXT,
    n       INTEGER,
    value   INTEGER,
    type    TEXT,
    is_spent INTEGER DEFAULT 0,   -- 1 once spent by a later input
    PRIMARY KEY (address, txid, n)
);

-- No index on addr_out(address) alone: the primary key is (address, txid, n),
-- so its autoindex already answers WHERE address=? as a prefix seek. The one on
-- (txid, n) is not redundant, because address leads the key and the primary key
-- cannot seek past it.
CREATE INDEX IF NOT EXISTS idx_addr_out_txid_n ON addr_out(txid, n);
-- No index on vout(addresses): it holds a JSON array and nothing filters on it.
-- Address accounting runs off addr_out, and multisig money is tracked per
-- script, so there is no lookup this could serve. See _drop_redundant_indexes.
CREATE INDEX IF NOT EXISTS idx_txs_height ON txs(height);
CREATE INDEX IF NOT EXISTS idx_vin_prev ON vin(prev_txid, prev_vout);
"""

# Declared apart from SCHEMA because the v3 migration has to recreate the table
# for existing databases. Balances are deliberately NOT columns: they are
# derived per request from vout/vin so that the confirmed and mempool-inclusive
# views can both be computed exactly (see DB.script_balances).
SCRIPTS_TABLE = """
CREATE TABLE IF NOT EXISTS scripts (
    script_hash    TEXT PRIMARY KEY,
    type           TEXT,          -- daemon classification for this template
    req_sigs       INTEGER,
    addresses      TEXT,          -- JSON array of addresses that OWN this script
    created_height INTEGER,       -- earliest confirmed output height, or NULL
    last_height    INTEGER        -- latest confirmed output height, or NULL
);
"""

SCRIPTS_INDEX = "CREATE INDEX IF NOT EXISTS idx_scripts_type ON scripts(type);"

SCHEMA += SCRIPTS_TABLE + "\n" + SCRIPTS_INDEX


ORPHAN_RETENTION = 20000  # tombstones kept this many blocks before pruning

# Bind parameters per statement, chunked. SQLITE_LIMIT_VARIABLE_NUMBER is
# compiled in: 999 before SQLite 3.32, 32766 after, 250000 in recent versions.
# An eviction or a reorg hands us an id list whose size we did not choose, and a
# statement over the cap does not degrade, it raises -- taking the enclosing
# transaction with it. A mempool past the cap would then never be cleanable, and
# a reorg past it would never apply, both forever. 500 fits inside the oldest
# cap with room to spare.
SQL_VAR_CHUNK = 500


def _chunks(seq, size=SQL_VAR_CHUNK):
    """Yield `seq` in slices of at most `size`, for building IN (...) lists."""
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


class DB:
    # scripts_v2: scripts.created_height/last_height are confirmed-only, so a
    # script seen solely in the mempool stores NULL instead of the 1<<31 / -1
    # sentinels v1 baked in. Bumping this re-runs the (idempotent) migration
    # and rebuilds scripts, which is what repairs the already-persisted rows.
    #
    # scripts_v3: drops the value_received/value_spent/n_vout/n_spent counter
    # columns. A single set of counters could not express the confirmed/live
    # split the API needs, and counting spends per *input* double-counted an
    # output that two known txs both spend (a mempool conflict drove the
    # balance negative). Balances are now derived per request from vout/vin,
    # so the table only holds metadata that cannot be recomputed from the rows.
    SCHEMA_VERSION = "scripts_v3"

    # How long a statement waits for a lock held by another process. The
    # migration window is generous because it is rare, bounded, and must not
    # fail halfway; ordinary work waits seconds and then errors, so a caller
    # retries on its next cycle instead of parking a request thread.
    NORMAL_BUSY_TIMEOUT_MS = 5000
    MIGRATION_BUSY_TIMEOUT_MS = 1800000

    @classmethod
    def initialize(cls, path):
        """Create or migrate the schema, and return a usable connection.

        For the indexer, and for any process that starts cold. Runs DDL, so it
        is the slow path -- once per process, not once per request.
        """
        return cls(path, schema=True)

    @classmethod
    def connect(cls, path):
        """Open an existing database without touching the schema.

        The read side (the web API) must never run CREATE TABLE or a migration:
        those are per-process costs, and paying them on every request dominated
        the handler's own work. The database is expected to exist already --
        call initialize() once at startup.

        check_same_thread=False is required because a pooled connection is
        created once and then used by whichever request thread borrows it; the
        pool guarantees only one thread holds it at a time.
        """
        return cls(path, schema=False, check_same_thread=False)

    def __init__(self, path, schema=True, check_same_thread=True,
                 busy_timeout=None):
        self._in_bulk = False
        self.conn = sqlite3.connect(path, check_same_thread=check_same_thread)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        # A migration may hold an EXCLUSIVE lock for minutes (the script_hash
        # backfill), and the other process opening this same file must wait for
        # it rather than die on a lock error -- so the long timeout applies to
        # schema creation and DDL, which must also precede it being lowered.
        # A connection that never migrates never gets the long timeout at all.
        self.conn.execute("PRAGMA busy_timeout=%d" % (
            busy_timeout or (self.MIGRATION_BUSY_TIMEOUT_MS if schema
                             else self.NORMAL_BUSY_TIMEOUT_MS)))
        if not schema:
            return
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()
        # Steady-state work -- indexing, and every web request -- waits only
        # briefly. A web reader that parks for MIGRATION_BUSY_TIMEOUT_MS turns
        # one long migration into 50 requests hanging for half an hour; a
        # blocked caller should fail fast and be retried instead.
        if busy_timeout is None:
            self.conn.execute("PRAGMA busy_timeout=%d"
                              % self.NORMAL_BUSY_TIMEOUT_MS)

    # (index, meta flag) for each index a table's own key or column set already
    # covers. See _drop_redundant_indexes.
    REDUNDANT_INDEXES = (
        ("idx_addr_out_address", "addr_out_address_index"),
        ("idx_vout_address", "vout_address_index"),
    )

    def _drop_redundant_indexes(self):
        """Drop indexes that nothing queries, and that a key already covers.

        * idx_addr_out_address -- addr_out's key is (address, txid, n), so
          sqlite_autoindex_addr_out_1 answers WHERE address=? as a prefix seek.
        * idx_vout_address -- a B-tree on a JSON array of addresses, which
          nothing filters on. It could not serve the substring search that would
          need it either: matching one address inside '["a","b"]' wants a row
          per owner, not an index. Per-owner multisig accounting, if it is ever
          wanted, is a table and not an index.

        Checked by EXPLAIN QUERY PLAN across every query that reads vout or
        addr_out: none chose either index, and dropping them changed no plan and
        no read latency (address_balances 19.2 vs 20.6 us). What it saved is
        1.1 MB and 1.3 MB per 60k rows, and 18% of bulk index write time.

        Carries its own meta flags rather than bumping schema_version: no table
        changes shape, and a bump would re-run _migrate()'s full
        rebuild_scripts() on every existing database, which is a lot of work to
        reclaim an index.
        """
        for index, flag in self.REDUNDANT_INDEXES:
            if self.get_meta(flag) == "dropped":
                continue
            self.conn.execute("DROP INDEX IF EXISTS " + index)
            self.set_meta(flag, "dropped")

    def _migrate(self):
        self._drop_redundant_indexes()
        if self.get_meta("schema_version") == self.SCHEMA_VERSION:
            return
        # Serialize schema changes across processes (indexer + web open this
        # same DB): first-comer migrates under EXCLUSIVE, the rest block on
        # busy_timeout and then see schema_version set.
        self.conn.execute("BEGIN EXCLUSIVE")
        try:
            if self.get_meta("schema_version") == self.SCHEMA_VERSION:
                self.conn.commit()
                return
            cols = [r[1] for r in self.conn.execute("PRAGMA table_info(txs)").fetchall()]
            if "status" not in cols:
                self.conn.execute("ALTER TABLE txs ADD COLUMN status TEXT")
                self.conn.execute(
                    "UPDATE txs SET status='mempool' WHERE height IS NULL")
                self.conn.execute(
                    "UPDATE txs SET status='confirmed' WHERE height IS NOT NULL")
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_txs_status_height "
                "ON txs(status, height)")
            vcols = [r[1] for r in self.conn.execute("PRAGMA table_info(vout)").fetchall()]
            if "script_hash" not in vcols:
                self.conn.execute("ALTER TABLE vout ADD COLUMN script_hash TEXT")
            nhash = self.conn.execute(
                "SELECT COUNT(*) FROM vout WHERE script_hash IS NULL").fetchone()[0]
            if nhash:
                rows = self.conn.execute(
                    "SELECT txid, n, script_hex FROM vout "
                    "WHERE script_hex IS NOT NULL").fetchall()
                upd = []
                for txid, n, hx in rows:
                    # One hash per row, not two: a comprehension that filters on
                    # script_hash_of(hx) and also returns it recomputes the
                    # SHA256+RIPEMD160 for every row that passes the filter.
                    sh = script_hash_of(hx)
                    if sh:
                        upd.append((sh, txid, n))
                self.conn.executemany(
                    "UPDATE vout SET script_hash=? WHERE txid=? AND n=?", upd)
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_vout_script_hash "
                "ON vout(script_hash)")
            self._migrate_scripts_columns()
            self.rebuild_scripts()
            self.set_meta("schema_version", self.SCHEMA_VERSION)
        except BaseException:
            self.conn.rollback()
            raise

    def _migrate_scripts_columns(self):
        """Reshape a v1/v2 scripts table (with counter columns) to v3.

        Recreate rather than ALTER TABLE ... DROP COLUMN, which needs SQLite
        3.35+. The counter columns are intentionally not copied: they are
        derived data and rebuild_scripts() recomputes what still belongs.
        Discrete execute()s, not executescript(): the latter commits any
        pending transaction, which would drop the EXCLUSIVE lock this
        migration is holding.
        """
        cols = [r[1] for r in
                self.conn.execute("PRAGMA table_info(scripts)").fetchall()]
        if "value_received" not in cols:
            return
        self.conn.execute("ALTER TABLE scripts RENAME TO scripts_v2")
        self.conn.execute(SCRIPTS_TABLE)
        self.conn.execute(
            """INSERT INTO scripts
               (script_hash, type, req_sigs, addresses)
               SELECT script_hash, type, req_sigs, addresses FROM scripts_v2""")
        # Dropping the old table also drops the index that followed it.
        self.conn.execute("DROP TABLE scripts_v2")
        self.conn.execute(SCRIPTS_INDEX)

    def bulk(self):
        """Context manager: wrap many add_block/add_tx in one transaction."""
        return _Bulk(self)

    def tip_height(self):
        row = self.conn.execute("SELECT MAX(height) FROM blocks").fetchone()
        return row[0] if row[0] is not None else -1

    def tip_hash(self):
        return self.conn.execute(
            "SELECT hash FROM blocks ORDER BY height DESC LIMIT 1").fetchone()

    def clear_from(self, height):
        """Truncate the chain at <height> (reorg support)."""
        if not self._in_bulk:
            with self.conn:
                self._clear_from(height)
        else:
            self._clear_from(height)

    def _refresh_spent_flags(self, outputs):
        """Recompute addr_out.is_spent for the given (txid, n) outputs.

        The surviving vin rows are the authority, so a conflict stays right: an
        output two known txs both spend still counts as spent after either one
        is re-indexed or removed. Every writer goes through here, which is what
        makes the flag independent of the order txs were added in.

        Batched, because this was the last statement left that scaled with the
        work: an eviction or a reorg hands us a pair per output affected, and a
        statement each made it the dominant cost of the operation. The pairs
        ride a CTE so the WHERE clause stays a join rather than a growing list
        of OR terms, and the EXISTS is still evaluated per row against the
        post-delete vin table -- the same answer the loop gave, in ~n/250
        statements.
        """
        outputs = list(dict.fromkeys(tuple(o) for o in outputs))
        per = max(1, SQL_VAR_CHUNK // 2)       # two binds per (txid, n) pair
        for i in range(0, len(outputs), per):
            chunk = outputs[i:i + per]
            self.conn.execute(
                """WITH p(txid, n) AS (VALUES %s)
                   UPDATE addr_out SET is_spent = EXISTS (
                       SELECT 1 FROM vin
                       WHERE vin.prev_txid = addr_out.txid
                         AND vin.prev_vout = addr_out.n)
                   WHERE (addr_out.txid, addr_out.n) IN (SELECT txid, n FROM p)"""
                % ",".join(["(?,?)"] * len(chunk)),
                [x for pair in chunk for x in pair])

    def _delete_tx_rows(self, txids):
        """Delete every row derived from these txids, and nothing else.

        The single place the four transaction tables are dropped, so a re-index
        and an eviction cannot drift apart on what a tx consists of. The caller
        owns the transaction: this is a slice of a larger atomic operation --
        a re-index that also re-inserts, an eviction that also repairs
        addr_out.is_spent and rebuilds the scripts -- not an operation of its
        own, which is why it does not repair anything.
        """
        for chunk in _chunks(txids):
            marks = ",".join("?" * len(chunk))
            for table in ("txs", "vin", "vout", "addr_out"):
                self.conn.execute(
                    "DELETE FROM %s WHERE txid IN (%s)" % (table, marks), chunk)

    def _clear_from(self, height):
        # Reorged-away confirmed txs become explicit tombstones instead of
        # disappearing; their vin/vout/addr_out rows are severed so no ghost
        # outputs leak into address queries.
        self.conn.execute(
            "UPDATE txs SET status='orphaned' WHERE height >= ? "
            "AND status='confirmed'", (height,))
        # Snapshot what the severing invalidates, while the rows still exist:
        # which scripts lose an output, and which outputs lose a spender. Both
        # are gone by the time the work below runs, and neither can be
        # recovered afterwards.
        touched = [r[0] for r in self.conn.execute(
            "SELECT DISTINCT v.script_hash FROM vout v "
            "JOIN txs t ON t.txid = v.txid "
            "WHERE t.status='orphaned' AND v.script_hash IS NOT NULL")]
        freed = self.conn.execute(
            "SELECT DISTINCT prev_txid, prev_vout FROM vin "
            "WHERE prev_txid IS NOT NULL AND txid IN "
            "(SELECT txid FROM txs WHERE status='orphaned')").fetchall()
        self.conn.execute("DELETE FROM blocks WHERE height >= ?", (height,))
        self.conn.execute(
            "DELETE FROM vin WHERE txid IN "
            "(SELECT txid FROM txs WHERE status='orphaned')")
        self.conn.execute(
            "DELETE FROM vout WHERE txid IN "
            "(SELECT txid FROM txs WHERE status='orphaned')")
        self.conn.execute(
            "DELETE FROM addr_out WHERE txid IN "
            "(SELECT txid FROM txs WHERE status='orphaned')")
        # Retract only the outputs the orphaned txs were spending, the same way
        # remove_txs does. Recomputing every addr_out row instead (clear all,
        # then set from vin) rewrote one row per address output on the whole
        # chain and scanned all of vin to do it, for a reorg that usually
        # touches a few dozen outputs.
        self._refresh_spent_flags(freed)
        # Rebuild the scripts the severed txs touched, not the whole table.
        self.rebuild_scripts(touched)
        # Bound tombstone growth: drop orphans older than the retention window.
        tip = self.conn.execute(
            "SELECT COALESCE(MAX(height),0) FROM blocks").fetchone()[0]
        self.conn.execute(
            "DELETE FROM txs WHERE status='orphaned' AND height < ?",
            (tip - ORPHAN_RETENTION,))

    def add_block(self, b):
        if not self._in_bulk:
            with self.conn:
                self._add_block(b)
        else:
            self._add_block(b)

    def _add_block(self, b):
        if b.prev_hash:
            self.conn.execute(
                "UPDATE blocks SET next_hash=? WHERE hash=?",
                (b.hash, b.prev_hash))
        self.conn.execute(
            """INSERT OR REPLACE INTO blocks
               (height, hash, version, merkleroot, time, nonce, bits,
                difficulty, size, prev_hash, next_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (b.height, b.hash, b.version, b.merkleroot, b.time, b.nonce,
             b.bits, b.difficulty, b.size, b.prev_hash, b.next_hash))

    def add_tx(self, t):
        if not self._in_bulk:
            with self.conn:
                self._add_tx(t)
        else:
            self._add_tx(t)

    def _add_tx(self, t):
        # Prevouts a previous version of this tx held, read before its vin rows
        # go away. A re-index spends the same set (a txid pins its content), so
        # this is normally empty -- but a changed set must not leave the old
        # ones marked spent.
        held = {(ipt.prev_txid, ipt.prev_vout) for ipt in t.vin
                if ipt.coinbase is None and ipt.prev_txid}
        released = [r for r in self.conn.execute(
            "SELECT DISTINCT prev_txid, prev_vout FROM vin "
            "WHERE txid=? AND prev_txid IS NOT NULL", (t.txid,))
            if r not in held]
        self._delete_tx_rows([t.txid])
        self.conn.execute(
            """INSERT INTO txs
               (txid, height, tx_index, version, locktime, size,
                is_coinbase, status)
               VALUES (?,?,?,?,?,?,?,?)""",
            (t.txid, t.height, t.tx_index, t.version, t.locktime, t.size,
             1 if t.is_coinbase else 0,
             'confirmed' if t.height is not None else 'mempool'))
        for i, ipt in enumerate(t.vin):
            self.conn.execute(
                """INSERT INTO vin
                   (txid, n, prev_txid, prev_vout, coinbase,
                    script_asm, script_hex, sequence)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (t.txid, i, ipt.prev_txid, ipt.prev_vout, ipt.coinbase,
                 ipt.script_asm, ipt.script_hex, ipt.sequence))
        for n, ot in enumerate(t.vout):
            sh = script_hash_of(ot.script_hex)
            self.conn.execute(
                """INSERT INTO vout
                   (txid, n, value, type, addresses, req_sigs,
                    script_asm, script_hex, script_hash)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (t.txid, n, ot.value, ot.type,
                 json.dumps(ot.addresses), ot.req_sigs,
                 ot.script_asm, ot.script_hex, sh))
            # Consensus accounting lives at SCRIPT level: the output belongs
            # to the whole script (all-of-N participants for multisig), not
            # to any single participant. So a multi-address vout credits no
            # one fully -- attes the value into the scripts entity instead.
            if len(ot.addresses) == 1:
                self.conn.execute(
                    """INSERT INTO addr_out
                       (address, txid, n, value, type)
                       VALUES (?,?,?,?,?)""",
                    (ot.addresses[0], t.txid, n, ot.value, ot.type))
            if sh is not None:
                # Metadata only. Balances are derived from vout/vin on read (see
                # DB.script_balances), so nothing here is a counter that a
                # re-index, a mempool eviction or a double-spend could skew.
                #
                # Heights are confirmed-only: a mempool tx arrives with a NULL
                # height and must neither fabricate a height nor erase a real
                # one. Scalar MIN()/MAX() over two arguments returns NULL if
                # EITHER is NULL -- unlike the aggregate form -- so each side is
                # coalesced into the other first: that yields the min/max when
                # both are known, and passes the known one through otherwise.
                self.conn.execute(
                    """INSERT INTO scripts
                       (script_hash, type, req_sigs, addresses,
                        created_height, last_height)
                       VALUES (?,?,?,?,?,?)
                       ON CONFLICT(script_hash) DO UPDATE SET
                         created_height = MIN(COALESCE(scripts.created_height,
                                                      excluded.created_height),
                                             COALESCE(excluded.created_height,
                                                      scripts.created_height)),
                         last_height = MAX(COALESCE(scripts.last_height,
                                                    excluded.last_height),
                                           COALESCE(excluded.last_height,
                                                    scripts.last_height))
                       """,
                    (sh, ot.type, ot.req_sigs,
                     json.dumps(ot.addresses), t.height, t.height))
        # is_spent is a fact about vin, not something the INSERTs above get to
        # decide, and this is the only place that writes it. Deriving it in the
        # input loop instead would miss a child indexed before its parent, and
        # miss a re-index too: the rows are deleted and rebuilt over the column
        # default. So recompute the three sets the INSERTs above could have got
        # wrong -- this version's inputs (marked spent by the vin rows just
        # written), its own outputs, and the prevouts a previous version held
        # that this one does not -- in one batched pass at the end.
        self._refresh_spent_flags(
            sorted(held) + [(t.txid, n) for n, _ in enumerate(t.vout)] + released)

    def remove_tx(self, txid):
        """Drop a tx and every row derived from it (stale mempool eviction).

        Public counterpart to add_tx for a tx that is leaving the index for
        good, rather than being replaced by a re-index. Callers should never
        have to DELETE from txs/vin/vout/addr_out themselves: addr_out.is_spent
        is maintained here, so a raw delete leaves an output looking spent when
        no spender is left. Script *balances* need no retraction -- they are
        derived from vout/vin on read -- but the scripts row is metadata, so it
        is rebuilt here.
        """
        self.remove_txs([txid])

    def remove_txs(self, txids):
        """Drop several txs at once, restoring addr_out.is_spent only at the end.

        Removing a whole mempool eviction in one call is the normal case, and
        the spent flags are recomputed in a single pass AFTER every delete.
        That makes the result independent of the order the txs are removed in:
        an output spent by two of them, or by one whose own output is also
        being removed, is re-evaluated against the final set of spending
        inputs instead of a half-emptied table. The scripts metadata is
        rebuilt from what survives, for the same order-independence.
        """
        txids = list(txids)
        if not txids:
            return
        if not self._in_bulk:
            with self.conn:
                self._remove_txs(txids)
        else:
            self._remove_txs(txids)

    def _remove_txs(self, txids):
        # Chunked because a mempool eviction can be longer than SQLite's bind
        # cap, and one statement cannot hold every stale id. The chunks share
        # the single transaction remove_txs() opened, so the eviction is still
        # all-or-nothing: either every stale tx goes or none of it does.
        spent_prev = []
        touched = []
        for chunk in _chunks(txids):
            marks = ",".join("?" * len(chunk))
            # Snapshot the prevouts first: the rows go away with the txs, and the
            # recompute below needs to know which outputs they were holding.
            spent_prev += self.conn.execute(
                "SELECT DISTINCT prev_txid, prev_vout FROM vin "
                "WHERE txid IN (%s) AND prev_txid IS NOT NULL" % marks,
                chunk).fetchall()
            # ...and the scripts these txs' outputs were evidence of, for the
            # same reason: vout is what rebuild_scripts() aggregates, and it is
            # about to be empty. Without this a script seen only in the mempool
            # keeps its row forever, and /api/script/<hash> answers 200 for a
            # script that no longer exists in the chain or the mempool -- zero
            # balances, but real type/addresses/height metadata.
            touched += [r[0] for r in self.conn.execute(
                "SELECT DISTINCT script_hash FROM vout "
                "WHERE txid IN (%s) AND script_hash IS NOT NULL" % marks,
                chunk)]
            # Four statements per chunk rather than per tx: the tables are
            # independent, and batching the deletes takes a whole-mempool
            # eviction from ~4 statements per stale tx to ~6 per 500.
            self._delete_tx_rows(chunk)
        # An output any of them spent may have no spender left. EXISTS runs
        # against the post-delete table, so it also covers the case where the
        # last spender was itself in this batch.
        self._refresh_spent_flags(spent_prev)
        # Recompute rather than delete: a script with a surviving output
        # elsewhere keeps its row, and the rebuild both restores its metadata
        # and drops the ones nothing is left for. Scoped for the same reason
        # clear_from scopes its own -- one mempool eviction must not re-aggregate
        # every output in the index. Chunked too: the scope is every script the
        # evicted txs touched, which is unbounded on the same terms.
        self.rebuild_scripts(touched)

    def query(self, sql, params=()):
        return self.conn.execute(sql, params).fetchall()

    def script_balances(self, script_hash):
        """Return {"confirmed": {...}, "live": {...}} for one script.

        Both views are derived here rather than stored, which is what makes
        the split exact:

        * confirmed -- only outputs of confirmed txs count as received, and
          only spends by confirmed txs count as spent. A confirmed output that
          some mempool tx also spends is still confirmed-unspent, because the
          mempool spend can still evaporate.
        * live -- every output the index knows about, minus every output some
          known tx spends. This is "what could be spent right now".

        Spends are matched with EXISTS over vin rather than joined, so an
        output counts once however many txs spend it: counting per spending
        input double-counted conflicting txs and drove the balance negative.
        n_spent therefore means "outputs of this script currently spent", not
        "spending inputs seen".

        This is the only balance view a multisig gets: a multi-address vout
        contributes to no single addr_out row, so the script is where all-of-N
        money is visible at all.
        """
        live = "('confirmed','mempool')"
        out = {}
        for key, status in (("confirmed", "('confirmed')"), ("live", live)):
            received, n_vout = self.conn.execute(
                "SELECT COALESCE(SUM(v.value),0), COUNT(*) FROM vout v "
                "JOIN txs t ON t.txid = v.txid "
                "WHERE v.script_hash=? AND t.status IN %s" % status,
                (script_hash,)).fetchone()
            spent, n_spent = self.conn.execute(
                "SELECT COALESCE(SUM(v.value),0), COUNT(*) FROM vout v "
                "JOIN txs t ON t.txid = v.txid "
                "WHERE v.script_hash=? AND t.status IN %s "
                "AND EXISTS (SELECT 1 FROM vin i JOIN txs ti ON ti.txid=i.txid "
                "WHERE i.prev_txid=v.txid AND i.prev_vout=v.n "
                "AND ti.status IN %s)" % (status, status),
                (script_hash,)).fetchone()
            out[key] = {
                "value_received": received, "n_vout": n_vout,
                "value_spent": spent, "n_spent": n_spent,
                "balance": received - spent,
            }
        return out

    def address_balances(self, address):
        """Return {"confirmed": {...}, "live": {...}} for one address.

        Same semantics as script_balances, but over addr_out, which only holds
        single-address outputs -- a multisig script has no addr_out rows at all.
        """
        out = {}
        for key, status in (("confirmed", "('confirmed')"),
                            ("live", "('confirmed','mempool')")):
            received, n_out = self.conn.execute(
                "SELECT COALESCE(SUM(a.value),0), COUNT(*) FROM addr_out a "
                "JOIN txs t ON t.txid = a.txid "
                "WHERE a.address=? AND t.status IN %s" % status,
                (address,)).fetchone()
            spent, n_spent = self.conn.execute(
                "SELECT COALESCE(SUM(a.value),0), COUNT(*) FROM addr_out a "
                "JOIN txs t ON t.txid = a.txid "
                "WHERE a.address=? AND t.status IN %s "
                "AND EXISTS (SELECT 1 FROM vin i JOIN txs ti ON ti.txid=i.txid "
                "WHERE i.prev_txid=a.txid AND i.prev_vout=a.n "
                "AND ti.status IN %s)" % (status, status),
                (address,)).fetchone()
            out[key] = {
                "value_received": received, "n_outputs": n_out,
                "value_spent": spent, "n_spent": n_spent,
                "balance": received - spent,
            }
        return out

    def rebuild_scripts(self, script_hashes=None):
        """Recompute the scripts table's metadata.

        With no argument, rebuild the whole table: on migration (backfill) that
        is the only option, since nothing is known about what is already there.

        With script_hashes, recompute only those scripts -- what a reorg needs.
        A reorg severs a handful of transactions, and re-aggregating every
        vout against every tx to fix them is a full scan of the chain per
        reorg. The reorg path collects the scripts losing an output first (the
        vout rows are gone by the time it calls this) and passes them in.

        Balances are not here: they are derived per request by script_balances.
        created_height/last_height track CONFIRMED appearances only -- height is
        a chain fact, and a mempool tx has none. The MIN/MAX aggregates skip
        NULLs, so a script that has only ever been seen unconfirmed gets NULL
        for both rather than a sentinel height.
        """
        if script_hashes is None:
            self.conn.execute("DELETE FROM scripts")
            scopes = [("WHERE v.script_hash IS NOT NULL", ())]
        else:
            script_hashes = list(dict.fromkeys(script_hashes))
            if not script_hashes:
                return
            # Same aggregate as the full rebuild, narrowed by an index lookup
            # instead of grouping the whole table. Chunked, because a deep reorg
            # hands us every script in the chain and one statement cannot bind
            # that many. The chunks partition the hashes, so no script aggregates
            # in two of them and the groups concatenate into exactly the rows the
            # single uncapped statement would have returned.
            scopes = [("WHERE v.script_hash IN (%s)" % ",".join("?" * len(c)), c)
                      for c in _chunks(script_hashes)]
        rows = []
        for where, params in scopes:
            rows += self.conn.execute(
                """SELECT v.script_hash, MAX(v.type), MAX(v.req_sigs),
                          MAX(v.addresses),
                          MIN(t.height), MAX(t.height)
                   FROM vout v
                   JOIN txs t ON t.txid = v.txid
                   %s
                   GROUP BY v.script_hash""" % where, params).fetchall()
        self.conn.executemany(
            """INSERT OR REPLACE INTO scripts
               (script_hash, type, req_sigs, addresses,
                created_height, last_height)
               VALUES (?,?,?,?,?,?)""", rows)
        if script_hashes is not None:
            # A script whose every output was severed has nothing left to
            # describe, so it leaves the table -- the same thing the full
            # rebuild's DELETE would have done to it. Batched like everything
            # else here: one DELETE per hash made an eviction cost a statement
            # per stale tx, which is the cost the chunking is here to remove.
            seen = {r[0] for r in rows}
            gone = [h for h in script_hashes if h not in seen]
            for chunk in _chunks(gone):
                self.conn.execute(
                    "DELETE FROM scripts WHERE script_hash IN (%s)"
                    % ",".join("?" * len(chunk)), chunk)

    def get_meta(self, key, default=None):
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_meta(self, key, value):
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES (?,?)",
                (key, value))