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

-- `mempool` and `spent_by` are denormalized facts about a row, not accounting.
-- Both are the answer to a question the balance queries used to re-derive per
-- output, which is what made a busy address take 41s (see _migrate_spent_mask).
--
-- `mempool` is 1 while the owning tx is unconfirmed. It is set from the
-- inserting tx's height and never revisited: orphaned rows are DELETED rather
-- than flagged (see _clear_from), so "not confirmed" only ever means mempool,
-- and a row's owner cannot change status without the row being rewritten.
-- Which means the owner-side status join the balance queries used to do is
-- answerable from this bit.
--
-- `spent_by` is a bitmask over the spender's status, and it is what replaces
-- the per-output EXISTS that dominated both lookups:
--     0 = unspent, 1 = spent by a confirmed tx, 2 = by a mempool tx,
--     3 = by both.  So `spent_by & 1` is "a confirmed tx spends this" and
-- `spent_by & 3` is "any known tx spends this" (the old is_spent column).
-- Two bits rather than one because the two balance views disagree about the
-- spender as well as the owner: an output spent only by a mempool tx is
-- confirmed-unspent, because a mempool spend can still evaporate.
CREATE TABLE IF NOT EXISTS vout (
    txid       TEXT,
    n          INTEGER,
    value      INTEGER,           -- in pokes (COIN = 1e8)
    type       TEXT,              -- e.g. pubkeyhash / scripthash / hybrid_pubkeyhash / ...
    addresses  TEXT,              -- JSON array of addresses (may include hybrid)
    req_sigs   INTEGER,
    script_asm TEXT,
    script_hex TEXT,
    script_hash TEXT,
    mempool    INTEGER NOT NULL DEFAULT 0,
    spent_by   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (txid, n)
);

CREATE TABLE IF NOT EXISTS addr_out (
    address TEXT,
    txid    TEXT,
    n       INTEGER,
    value   INTEGER,
    type    TEXT,
    mempool  INTEGER NOT NULL DEFAULT 0,
    spent_by INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (address, txid, n)
);

-- Covering indexes for the two balance lookups are created by
-- _migrate_spent_mask, not here. `value` is the reason they lead with it: the
-- balance queries sum value over every output of one address/script, and
-- without it in the index each of those rows costs a separate lookup into the
-- table to fetch it -- 3.7s for the 510k-output address, against 0.4us/row for
-- a bare scan of the same rows. The trailing (txid, n) serves the address
-- page's spender enumeration, which otherwise has to revisit each of those
-- rows in the table.
--
-- One index each, not a confirmed/mempool pair: a partial index would only
-- help the confirmed view, and with an empty mempool it would cover almost
-- every row anyway, so it would nearly double the write cost to save a
-- comparison on an index-only scan that is already sequential.
--
-- Not in SCHEMA because SCHEMA is executed before any migration, so naming a
-- column here would fail on exactly the databases that need adding it.

-- No index on addr_out(address) alone: the primary key is (address, txid, n),
-- so its autoindex already answers WHERE address=? as a prefix seek, and
-- idx_addr_out_addr answers it covering. The one on
-- (txid, n) is not redundant, because address leads the key and the primary
-- key cannot seek past it.
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

# Cumulative minted supply: every coinbase output ever indexed. This is NOT
# "outstanding/unspent" -- it is never decremented by spends.
#
# This is the DEFINITION of the figure, not how it is read. Deriving it per
# request meant joining every confirmed coinbase tx to its outputs: 4.6M
# random seeks into the vout key on a 4.6M-block chain, measured at 31s, on
# the /api/summary path of every page load. It stays here because it is what
# the backfill computes and what repairs the counter below.
#
# The status='confirmed' filter is load-bearing, not redundant: a mempool tx
# is not yet minted, and an orphaned one no longer counts.
TOTAL_COINBASE_SQL = (
    "SELECT COALESCE(SUM(v.value),0) FROM vout v JOIN txs t "
    "ON v.txid = t.txid WHERE t.is_coinbase = 1 AND t.status = 'confirmed'")

# The maintained running total. Unlike a balance, minted supply has no
# mempool/confirmed split to get wrong and no double-spend to net out: it is
# a plain accumulator over coinbase outputs, and the write path already knows
# each coinbase tx's output values at insert time. So it is kept in the same
# transaction as the rows it describes -- a reader takes one indexed meta
# lookup instead of a 31s scan, and sees the counter and the data from a
# single consistent snapshot because they commit together.
COINBASE_TOTAL_KEY = "total_coinbase"
N_BLOCKS_KEY = "n_blocks"
N_TXS_KEY = "n_txs"

# The other two figures /api/summary shows, and the queries that define them.
# COUNT(*) over a 4.6M-row table is a full index scan whatever it counts:
# 0.28s for blocks and 0.63s for txs, on every page load, growing with the
# chain. They are maintained for the same reason and on the same terms.
N_BLOCKS_SQL = "SELECT COUNT(*) FROM blocks"
N_TXS_SQL = "SELECT COUNT(*) FROM txs WHERE status != 'orphaned'"

# The three figures a reader asks for, and the SQL each is defined by. Kept as
# one table so the counter and its definition cannot drift apart in the source:
# a stat added here without a definition, or the reverse, is a mistake the
# backfill turns into a wrong number rather than a loud failure.
#
# Each is an accumulator over rows the write path already touches, and each is
# updated in the same transaction as those rows -- so a reader sees the counter
# and the data from one consistent snapshot, and a crash rolls back both.
STATS = (
    (COINBASE_TOTAL_KEY, TOTAL_COINBASE_SQL),
    (N_BLOCKS_KEY, N_BLOCKS_SQL),
    (N_TXS_KEY, N_TXS_SQL),
)
STATS_SEEDED_KEY = "stats_seeded"

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

    # Per-connection page cache, in KiB (the negative form SQLite uses). The
    # default is 2000 KiB, which against a 14 GB database means nearly every
    # page a query touches costs a read() syscall. This is a pool of a few
    # connections on the web side plus one in the indexer, not one per process
    # per request, so 64 MiB each is a few hundred MiB in total.
    CACHE_SIZE_KIB = 65536

    # Bytes of the database a connection reads through mmap instead of
    # read(). Repeat reads are then served by the OS page cache with no second
    # copy, and -- the reason it matters most here -- the indexer's
    # checkpoints no longer have to push pages through this process's own
    # cache to make them visible to a reader.
    MMAP_SIZE = 1 << 30

    # Cap on how large the write-ahead log is left after a checkpoint. The
    # default (-1) never truncates, and a WAL file is reused rather than
    # shrunk, so its high-water mark is permanent: this database's reached
    # 33 GB. That was a symptom, not the disease -- a reader holding a
    # snapshot for 31s (the per-request minted-supply scan) meant no
    # checkpoint could ever complete, so the log could never be reset at all.
    # With that fixed the log checkpoints continuously, and this is what turns
    # "checkpointed" into "reclaims the space": after each checkpoint the file
    # is truncated back to this size instead of staying at its largest.
    JOURNAL_SIZE_LIMIT = 64 << 20

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
        try:
            self._open(schema, busy_timeout)
        except BaseException:
            # Everything past sqlite3.connect can raise -- a pragma, or a
            # migration that fails partway, which is _migrate_spent_mask's
            # designed case. When it does, __init__ raises and the caller never
            # gets the object, so it has no handle to close the connection with:
            # leaving it to the collector is what makes a failed migration
            # announce itself as an unclosed database rather than as the
            # migration failure it is. Close it here, so the only thing the
            # caller sees is the error.
            self.conn.close()
            raise

    def _open(self, schema, busy_timeout):
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-%d" % self.CACHE_SIZE_KIB)
        self.conn.execute("PRAGMA mmap_size=%d" % self.MMAP_SIZE)
        self.conn.execute("PRAGMA journal_size_limit=%d"
                          % self.JOURNAL_SIZE_LIMIT)
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
        self._backfill_stats()
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
        # Ordered, and both unconditional. The version migration has to come
        # first because the spent-mask migration builds an index on
        # vout.script_hash, which an older database does not have until the
        # version migration adds it. Neither is skipped by the other's early
        # return: each carries its own meta flag, so a database can be on the
        # current version and still need the columns.
        if self.get_meta("schema_version") != self.SCHEMA_VERSION:
            self._migrate_version()
        self._migrate_spent_mask()

    def _migrate_version(self):
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

    def _migrate_spent_mask(self):
        """Add mempool/spent_by to addr_out and vout, and backfill them once.

        The balance lookups used to re-derive, per output and per request,
        two facts the write path already knows: who owns the output, and who
        spends it. On the busiest address that was 510k txs lookups plus 510k
        correlated EXISTS into vin, and /api/address took 41s. Both are now
        columns maintained by _refresh_spent_flags and by the insert, so this
        migration is what turns the existing rows into that shape.

        Carries its own meta flag rather than riding schema_version: a version
        bump re-runs the version migration's full rebuild_scripts() -- a
        re-aggregate over every vout in the chain -- on every existing
        database, which is a lot of work to add two columns. Same reasoning as
        _drop_redundant_indexes, except this one does change table shape.

        Carries it in a single transaction, so a database that fails partway
        through (an interrupted CREATE INDEX, a full disk) is left exactly as
        it was rather than with the columns added and the values missing --
        which would read as "every output is unspent" and report a balance of
        the full supply. The flag is only set on the way out, so the next
        start retries from the top.

        The backfill is the expensive part: it walks every vin row to work out
        each output's spender statuses, which is 10M rows on this chain and the
        only step here that is not a schema edit. It happens once, and it is
        the reason this migration wants the database to itself rather than
        sharing it with a live web request.
        """
        if self.get_meta("spent_mask") == "1":
            return
        self.conn.execute("BEGIN EXCLUSIVE")
        try:
            if self.get_meta("spent_mask") == "1":
                self.conn.commit()
                return
            for table in ("addr_out", "vout"):
                cols = {r[1] for r in
                        self.conn.execute("PRAGMA table_info(%s)" % table)}
                if "spent_by" not in cols:
                    # Both default to 0, which is right for mempool (there are
                    # no mempool rows on a database being migrated after a
                    # clean start, and the ones that do exist are fixed below)
                    # and for spent_by only as a starting point.
                    self.conn.execute(
                        "ALTER TABLE %s ADD COLUMN mempool INTEGER "
                        "NOT NULL DEFAULT 0" % table)
                    self.conn.execute(
                        "ALTER TABLE %s ADD COLUMN spent_by INTEGER "
                        "NOT NULL DEFAULT 0" % table)
            # mempool: driven from the mempool tx list rather than by scanning
            # addr_out for rows to change. The list is bounded by the mempool
            # and the (txid, n) indexes answer it, where the other direction --
            # one pass over 7M rows to test each -- is the whole cost of the
            # migration multiplied for nothing.
            mempool = [r[0] for r in self.conn.execute(
                "SELECT txid FROM txs WHERE status='mempool'")]
            for txid in mempool:
                self.conn.execute(
                    "UPDATE addr_out SET mempool=1 WHERE txid=?", (txid,))
                self.conn.execute(
                    "UPDATE vout SET mempool=1 WHERE txid=?", (txid,))
            # spent_by, derived from the spenders themselves through a temp
            # table of (output -> status mask). Keyed on the output, so one
            # lookup answers it, and built from vin rather than from either
            # output table, so the unspent rows -- the majority -- cost nothing
            # to skip.
            #
            # One path for both tables, and deliberately not a shortcut for
            # addr_out's old is_spent column: that column was never on vout, so
            # a shortcut keyed on it would leave every vout row at the default 0
            # and every script balance reading as unspent. The vin scan is
            # needed for vout regardless, so reading is_spent would save nothing
            # anyway -- it was an optimisation that cost a correctness bug.
            self.conn.execute(
                "CREATE TEMP TABLE _sp(txid TEXT, n INTEGER, "
                "st INTEGER, PRIMARY KEY(txid, n)) WITHOUT ROWID")
            # Both bits, or the mask is not a mask: an output spent only by a
            # mempool tx has to come out 2, and one spent by both has to come
            # out 3. MAX() over a boolean is 1 if any spender has that status,
            # so the two are independent tests and the mempool one is shifted
            # into the high bit.
            self.conn.execute(
                "INSERT OR REPLACE INTO _sp "
                "SELECT i.prev_txid, i.prev_vout, "
                "       MAX(t.status = 'confirmed') "
                "     | (MAX(t.status = 'mempool') << 1) "
                "FROM vin i JOIN txs t ON t.txid = i.txid "
                "WHERE i.prev_txid IS NOT NULL GROUP BY 1, 2")
            for table in ("addr_out", "vout"):
                # Restricted to the outputs _sp actually holds: everything else
                # is unspent, which is what the column was just defaulted to, so
                # rewriting those rows would be a full pass over the table to
                # store a value it already has.
                self.conn.execute(
                    "UPDATE %s SET spent_by = (SELECT st FROM _sp"
                    "  WHERE _sp.txid = %s.txid AND _sp.n = %s.n) "
                    "WHERE (txid, n) IN (SELECT txid, n FROM _sp)"
                    % (table, table, table))
            self.conn.execute("DROP TABLE _sp")
            # is_spent is now spent_by & 3 and nothing reads it, so it goes.
            # DROP COLUMN is a schema edit rather than a table rewrite for an
            # unindexed column, which is why it is safe to do on 7M rows here.
            # Absent on a database created from the current SCHEMA, which never
            # had it.
            if "is_spent" in {r[1] for r in self.conn.execute(
                    "PRAGMA table_info(addr_out)")}:
                self.conn.execute("ALTER TABLE addr_out DROP COLUMN is_spent")
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_addr_out_addr ON addr_out"
                "(address, value, spent_by, mempool, txid, n)")
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_vout_script ON vout"
                "(script_hash, value, spent_by, mempool, txid, n)")
            # Superseded by idx_vout_script, which leads with the same column
            # and so answers everything the old one did plus the balance query.
            self.conn.execute("DROP INDEX IF EXISTS idx_vout_script_hash")
            self.set_meta("spent_mask", "1")
            self.conn.commit()
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
        """Recompute spent_by for the given (txid, n) outputs, in both tables.

        The surviving vin rows are the authority, so a conflict stays right: an
        output two known txs both spend still counts as spent after either one
        is re-indexed or removed. Every writer goes through here, which is what
        makes the mask independent of the order txs were added in.

        Both addr_out and vout are updated from the same pairs, because the
        two balance views are asked the same question of an output and vout is
        the only place a multisig's answer exists: a multi-address output
        contributes to no single addr_out row, so before this, script_balances
        was the one lookup that had to re-derive the fact from vin on every
        request. Same pairs, same pass -- the addresses and the script of one
        output are not the same rows, so leaving vout behind meant a second
        derivation rather than a cheaper one.

        The mask carries the spender's status, not just its existence, because
        "spent" and "confirmed-spent" are different questions: an output spent
        only by a mempool tx is confirmed-unspent, since a mempool spend can
        still evaporate. Deriving the second from the first is not possible --
        it is the difference between a mempool-only spender and a confirmed
        one, which the old single boolean did not record.

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
            binds = [x for pair in chunk for x in pair]
            for table in ("addr_out", "vout"):
                # Two EXISTS rather than one scan of vin grouped by output: the
                # pairs are already bounded by the chunk, and a group-by would
                # have to walk every vin row sharing the output. The status is
                # read through txs because a vin row's own status is its
                # owner's, which is the thing being asked about here.
                self.conn.execute(
                    """WITH p(txid, n) AS (VALUES %s)
                       UPDATE %s SET spent_by =
                           (CASE WHEN EXISTS (
                                SELECT 1 FROM vin
                                JOIN txs ON txs.txid = vin.txid
                                WHERE vin.prev_txid = %s.txid
                                  AND vin.prev_vout = %s.n
                                  AND txs.status = 'confirmed')
                             THEN 1 ELSE 0 END)
                         | (CASE WHEN EXISTS (
                                SELECT 1 FROM vin
                                JOIN txs ON txs.txid = vin.txid
                                WHERE vin.prev_txid = %s.txid
                                  AND vin.prev_vout = %s.n
                                  AND txs.status = 'mempool')
                             THEN 2 ELSE 0 END)
                       WHERE (%s.txid, %s.n) IN (SELECT txid, n FROM p)"""
                    % (",".join(["(?,?)"] * len(chunk)), table,
                       table, table, table, table, table, table),
                    binds)

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
        # What this truncation retracts, read while the rows still hold their
        # old status: the supply the orphaned coinbases carried, how many live
        # txs they were, and how many blocks are about to go. Scoped to
        # height>=?, not to every orphan ever -- a reorg from a year ago was
        # already retracted, and retracting it again would drive the counts
        # negative. The status/height index makes this a seek from the reorg
        # point, so it costs the depth of the reorg, not the length of the
        # chain.
        orphaned_minted, orphaned_txs = self.conn.execute(
            "SELECT COALESCE(SUM(v.value),0), COUNT(*) FROM txs t "
            "JOIN vout v ON v.txid = t.txid "
            "WHERE t.status='confirmed' AND t.is_coinbase=1 AND t.height >= ?",
            (height,)).fetchone()
        live_txs = self.conn.execute(
            "SELECT COUNT(*) FROM txs WHERE status='confirmed' AND height >= ?",
            (height,)).fetchone()[0]
        dropped_blocks = self.conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE height >= ?",
            (height,)).fetchone()[0]
        # Reorged-away confirmed txs become explicit tombstones instead of
        # disappearing; their vin/vout/addr_out rows are severed so no ghost
        # outputs leak into address queries.
        self.conn.execute(
            "UPDATE txs SET status='orphaned' WHERE height >= ? "
            "AND status='confirmed'", (height,))
        self._bump(COINBASE_TOTAL_KEY, -orphaned_minted)
        self._bump(N_TXS_KEY, -live_txs)
        self._bump(N_BLOCKS_KEY, -dropped_blocks)
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
        # One primary-key seek to tell a new height from a re-index of one we
        # already hold: INSERT OR REPLACE below cannot report which it did,
        # and the block count is the number of heights, not of writes.
        new_height = self.conn.execute(
            "SELECT 1 FROM blocks WHERE height=?", (b.height,)).fetchone() is None
        self.conn.execute(
            """INSERT OR REPLACE INTO blocks
               (height, hash, version, merkleroot, time, nonce, bits,
                difficulty, size, prev_hash, next_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (b.height, b.hash, b.version, b.merkleroot, b.time, b.nonce,
             b.bits, b.difficulty, b.size, b.prev_hash, b.next_hash))
        self._bump(N_BLOCKS_KEY, 1 if new_height else 0)

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
        # What this tx already counted, read before its rows go away: the
        # status that decides whether it was a live tx, and -- for a coinbase
        # -- the supply it was counted at. is_coinbase is read off the stored
        # row rather than off the incoming tx because the figure is defined
        # over what is stored: a txid that somehow arrived with different
        # inputs still has to leave the counter matching the rows.
        prev = self.conn.execute(
            "SELECT status, is_coinbase FROM txs WHERE txid=?",
            (t.txid,)).fetchone()
        prev_minted = 0
        if prev is not None and prev[1] and prev[0] == "confirmed":
            prev_minted = self.conn.execute(
                "SELECT COALESCE(SUM(value),0) FROM vout WHERE txid=?",
                (t.txid,)).fetchone()[0]
        self._delete_tx_rows([t.txid])
        # The one place a row's confirmed-vs-mempool bit is decided. Set from
        # the incoming tx's height and never revisited afterwards, because
        # orphaned rows are deleted rather than flagged, so an output's owner
        # cannot change status without the output row itself being rebuilt
        # here. Deriving it per read instead meant a txs lookup per output,
        # which on the 510k-output address was 510k random seeks to learn
        # something every row already knew.
        mempool = 0 if t.height is not None else 1
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
                    script_asm, script_hex, script_hash, mempool)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (t.txid, n, ot.value, ot.type,
                 json.dumps(ot.addresses), ot.req_sigs,
                 ot.script_asm, ot.script_hex, sh, mempool))
            # Consensus accounting lives at SCRIPT level: the output belongs
            # to the whole script (all-of-N participants for multisig), not
            # to any single participant. So a multi-address vout credits no
            # one fully -- attes the value into the scripts entity instead.
            if len(ot.addresses) == 1:
                self.conn.execute(
                    """INSERT INTO addr_out
                       (address, txid, n, value, type, mempool)
                       VALUES (?,?,?,?,?,?)""",
                    (ot.addresses[0], t.txid, n, ot.value, ot.type, mempool))
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
        # Minted supply moves by exactly what this version added over what the
        # previous one had counted. height IS NULL while unconfirmed, so a
        # mempool coinbase adds nothing until it is indexed into a block.
        self._bump(COINBASE_TOTAL_KEY,
                   (sum(ot.value for ot in t.vout) if t.height is not None else 0)
                   - prev_minted)
        # n_txs counts non-orphaned rows: a re-index of a live tx replaces it
        # and nets to zero, while an orphan returning to the chain or the
        # mempool is newly counted.
        self._bump(N_TXS_KEY, 1 if prev is None or prev[0] == "orphaned" else 0)

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
        dropped_txs = 0
        dropped_minted = 0
        for chunk in _chunks(txids):
            marks = ",".join("?" * len(chunk))
            # ...and what they take out of the counters, read before the rows
            # go. remove_txs() is documented for mempool eviction, where the
            # supply half of this is 0 and a coinbase is never in the mempool
            # -- but a displayed count should not rest on the caller only ever
            # passing the kind of txid it means to. The count is of txs ROWS,
            # so it is a plain COUNT and still right for a coinbase stored
            # without outputs. Orphaned rows are already out of n_txs and
            # deleting one must not retract again.
            live, minted = self.conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(CASE WHEN t.is_coinbase=1 "
                "AND t.status='confirmed' THEN (SELECT COALESCE(SUM(v.value),0) "
                "FROM vout v WHERE v.txid = t.txid) ELSE 0 END), 0) "
                "FROM txs t WHERE t.status != 'orphaned' AND t.txid IN (%s)"
                % marks, chunk).fetchone()
            dropped_txs += live
            dropped_minted += minted
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
        self._bump(COINBASE_TOTAL_KEY, -dropped_minted)
        self._bump(N_TXS_KEY, -dropped_txs)

    def query(self, sql, params=()):
        return self.conn.execute(sql, params).fetchall()

    # --- maintained counters ---------------------------------------------
    #
    # Every write path that can change one of these updates it in the same
    # transaction as the rows it counts, so nothing here needs a cache and
    # nothing can be stale against the data it is read alongside. Each stat
    # has exactly one writer per operation; the ones that can move are
    # _add_tx (a tx indexed, re-indexed, or moved out of the orphan state),
    # _add_block (a new height), _clear_from (a reorg) and _remove_txs (a
    # drop). Tombstone pruning is deliberately not among them -- an orphan
    # left the total when it was orphaned, so deleting the row later moves
    # nothing, and retracting again would drive a count negative.

    def _stat(self, key, default=0):
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return int(row[0]) if row and row[0] is not None else default

    def _bump(self, key, delta):
        """Add `delta` to a counter, inside the caller's transaction.

        A bare execute(), never set_meta(): set_meta opens `with self.conn`,
        which COMMITs, and that would end the surrounding bulk transaction
        mid-block -- committing a half-indexed block and dropping the
        EXCLUSIVE lock a migration is holding. Same reason
        _migrate_scripts_columns avoids executescript().

        Read-modify-write in Python rather than SQL arithmetic, so the value
        is an int the whole way rather than a string that meta.value's TEXT
        affinity has to round-trip on every block.
        """
        if not delta:
            return
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(self._stat(key) + delta)))

    def total_coinbase(self):
        """Minted supply in pokes: one indexed lookup, not a scan."""
        return self._seeded_stat(COINBASE_TOTAL_KEY)

    def n_blocks(self):
        """Number of blocks held: one indexed lookup, not a table scan."""
        return self._seeded_stat(N_BLOCKS_KEY)

    def n_txs(self):
        """Number of non-orphaned txs: one indexed lookup, not a table scan."""
        return self._seeded_stat(N_TXS_KEY)

    def _seeded_stat(self, key):
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is not None and row[0] is not None:
            return int(row[0])
        # Not seeded yet: a read-only connection against a database written
        # before the counters existed. Derive the answer rather than serve a
        # wrong zero; the writer seeds it on its next open.
        return self._define_stat(key)

    def _define_stat(self, key):
        for name, sql in STATS:
            if name == key:
                return self.conn.execute(sql).fetchone()[0]
        raise KeyError(key)

    def recompute_stats(self):
        """Rescan the chain and overwrite every counter. Returns {key: value}.

        The repair hatch. Correctness never depends on the counters being
        trusted -- every write path maintains them from the same transaction
        as the rows -- but this is how you find out whether one drifted, and
        how a database backfilled by an older build gets fresh values without
        a scan running on a request.
        """
        out = {}
        for key, sql in STATS:
            value = self.conn.execute(sql).fetchone()[0]
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)))
            out[key] = value
        return out

    def _backfill_stats(self):
        """Seed the counters once per database, under the migration's lock.

        Carries its own meta flag rather than riding SCHEMA_VERSION: a version
        bump would re-run _migrate()'s full rebuild_scripts() on every existing
        database -- a 7M-row re-aggregate -- to store three integers.

        EXCLUSIVE, like _migrate, because the indexer and the web server both
        open this file: first comer seeds, the rest block on busy_timeout and
        then find the flag already set. Readers keep going throughout -- WAL
        lets them read the pre-backfill snapshot -- so a page load during the
        backfill waits on nothing and is served the old value rather than an
        error. The minted-supply scan is the slow part (31s on a 4.6M-block
        chain), which is why it happens once here and never on a request.
        """
        if self.get_meta(STATS_SEEDED_KEY) is not None:
            return
        self.conn.execute("BEGIN EXCLUSIVE")
        try:
            if self.get_meta(STATS_SEEDED_KEY) is not None:
                self.conn.commit()
                return
            self.recompute_stats()
            self.set_meta(STATS_SEEDED_KEY, "1")
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    # The two balance views, as one index-only scan each. Written once and
    # parameterised because addr_out and vout ask the identical question of an
    # output and only the table and the count column differ; the views used to
    # be four near-identical statements per lookup, which is how a 41s request
    # came to be four separate 3-12s scans of the same rows.
    #
    # `owner` is the condition the view puts on the output's own tx, and
    # `spender` the bit it tests on who spent it. Both are columns now rather
    # than joins to txs and a correlated EXISTS into vin, so neither view
    # touches a table outside the one being aggregated -- which is what lets
    # the covering index answer it without a row fetch.
    #
    # The "confirmed" and "live" differ only in whether mempool rows count, and
    # live is the unconditional one: an output's owner is confirmed or mempool
    # and never orphaned, because _clear_from deletes an orphan's rows instead
    # of keeping them flagged. That invariant is what lets the live view skip
    # the status test the confirmed view still needs, and it is why no
    # statement here mentions txs at all.
    _BALANCE_VIEWS = (
        # view        owner        spender
        ("confirmed", "mempool=0", "spent_by & 1"),
        ("live",      "1",         "spent_by & 3"),
    )

    def _balances(self, table, key_col, key, count_col):
        """Confirmed and live balances for one address or script_hash.

        One statement per view, aggregating received and spent together, so a
        view costs a single pass over the rows instead of two passes that
        differ only in a trailing EXISTS. `n_spent` counts outputs, not
        spending inputs: an output two txs both spend still counts once, which
        is what keeps a conflict from being charged to the balance twice.
        """
        out = {}
        for view, owner, spender in self._BALANCE_VIEWS:
            received, n_out, spent, n_spent = self.conn.execute(
                "SELECT COALESCE(SUM(CASE WHEN %s THEN value END),0), "
                "       COUNT(CASE WHEN %s THEN 1 END), "
                "       COALESCE(SUM(CASE WHEN %s AND (%s) THEN value END),0), "
                "       COUNT(CASE WHEN %s AND (%s) THEN 1 END) "
                "FROM %s WHERE %s=?" % (owner, owner, owner, spender,
                                        owner, spender, table, key_col),
                (key,)).fetchone()
            out[view] = {
                "value_received": received, count_col: n_out,
                "value_spent": spent, "n_spent": n_spent,
                "balance": received - spent,
            }
        return out

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

        Spends are matched by the row's own spent_by mask rather than joined or
        re-derived from vin, so an output counts once however many txs spend
        it: counting per spending input double-counted conflicting txs and
        drove the balance negative. n_spent therefore means "outputs of this
        script currently spent", not "spending inputs seen".

        This is the only balance view a multisig gets: a multi-address vout
        contributes to no single addr_out row, so the script is where all-of-N
        money is visible at all.
        """
        return self._balances("vout", "script_hash", script_hash, "n_vout")

    def address_balances(self, address):
        """Return {"confirmed": {...}, "live": {...}} for one address.

        Same semantics as script_balances, but over addr_out, which only holds
        single-address outputs -- a multisig script has no addr_out rows at all.
        """
        return self._balances("addr_out", "address", address, "n_outputs")

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
            # single uncapped statement would have returned. Each chunk is
            # written and dropped before the next is read, so a deep reorg never
            # holds more than one chunk's rows in memory -- plus the seen set the
            # deletion below needs.
            scopes = [("WHERE v.script_hash IN (%s)" % ",".join("?" * len(c)), c)
                      for c in _chunks(script_hashes)]
        need_seen = script_hashes is not None
        seen = set() if need_seen else None
        for where, params in scopes:
            rows = self.conn.execute(
                """SELECT v.script_hash, MAX(v.type), MAX(v.req_sigs),
                          MAX(v.addresses),
                          MIN(t.height), MAX(t.height)
                   FROM vout v
                   JOIN txs t ON t.txid = v.txid
                   %s
                   GROUP BY v.script_hash""" % where, params).fetchall()
            if need_seen:
                seen.update(r[0] for r in rows)
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