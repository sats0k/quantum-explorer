"""PostgreSQL storage for the PhoenixCoin Quantum explorer.

Amounts are stored as integers in "pokes" (COIN = 1e8), matching
src/util.h. Script types / addresses come verbatim from the daemon's
verbose getrawtransaction output, which already understands the hybrid
script templates and hybrid address prefixes.

Everything the rest of the explorer needs is reached through DB, which
wraps a single psycopg connection. Two deliberate choices are worth
knowing before reading the SQL below:

* Statements are written with '?' placeholders and rewritten to psycopg's
  '%s' by _bind, in one place. The SQL therefore reads the same as it did
  under sqlite3 and nothing outside this module knows which driver is
  underneath.

* Every SUM is cast back to bigint. Postgres sums bigint into NUMERIC and
  hands the driver a Decimal, which would make a supply figure a Decimal
  all the way to json.dumps() -- where it raises. The cast keeps every
  amount a Python int, which is what the rest of the code assumes.
"""

import difflib
import hashlib
import json
import os
import re

import psycopg

COIN = 100000000

# Connection string used when the command line does not name one. An
# environment variable rather than a built-in password, so a deployment can
# point both processes at the same database without it in the source.
DEFAULT_DSN = os.environ.get("EXPLORER_DSN", "dbname=explorer")


def script_hash_of(script_hex):
    """Canonical script key: RIPEMD160(SHA256(scriptPubKey)), matching the
    daemon's CScriptID/Hash160. None for empty scripts."""
    if not script_hex:
        return None
    b = bytes.fromhex(script_hex)
    return hashlib.new("ripemd160", hashlib.sha256(b).digest()).hexdigest()


def _bind(sql):
    """Rewrite qmark placeholders as the driver's format placeholders.

    '%' is escaped first: psycopg reads a '%' as the start of a
    placeholder, so a literal one (a LIKE pattern, say) would otherwise be
    a syntax error. No statement here has one -- every '%' in this module
    is consumed by Python-side % formatting before it gets here -- but
    escaping keeps that a property of the SQL rather than a rule every
    future statement has to remember.
    """
    return sql.replace("%", "%%").replace("?", "%s")


def _quote_ident(name):
    """Quote `name` as an SQL identifier.

    For the one statement whose table names are not literal in the source: the
    rebuild drop, which is built by joining names. A parameter cannot be used
    there -- parameters are values, and a table name in a DROP is a name -- so
    it is quoted here, embedded double quotes doubled.
    """
    return '"%s"' % name.replace('"', '""')


class _PgConn:
    """A psycopg connection with the transaction discipline db.py expects.

    The connection runs in autocommit so that BEGIN/COMMIT are explicit and
    the nesting rules are ours rather than the driver's. psycopg's own
    commit()/rollback() are unconditional server commands -- in autocommit
    with nothing open, the server answers a warning on every one -- so the
    open/closed state is tracked here and a stray commit is a no-op, the
    way it was against sqlite3.

    `with conn:` means "be in a transaction, and commit it on the way out".
    If one is already open it is left open and committed, which is what
    sqlite3's `with conn:` did and what set_meta() below relies on: it
    commits the caller's transaction, which is why the write paths that
    must not do that use a bare execute().
    """

    def __init__(self, dsn):
        self._c = psycopg.connect(dsn, autocommit=True)
        self._in_tx = False
        self._trace = None

    def execute(self, sql, params=()):
        self._trace_sql(sql)
        return self._c.execute(_bind(sql), params)

    def executemany(self, sql, params):
        # executemany() is a cursor method in psycopg3, not a connection one,
        # unlike sqlite3 where both lived on the connection.
        self._trace_sql(sql)
        with self._c.cursor() as cur:
            return cur.executemany(_bind(sql), params)

    def copy_from(self, sql, rows):
        """COPY `rows` in with the server-side COPY protocol. Returns the count.

        Takes the statement already built (COPY has no placeholders) so the
        one-off bulk loader can stream a whole
        chain through here without reaching past this class into the driver.
        """
        self._trace_sql(sql)
        n = 0
        with self._c.cursor() as cur:
            with cur.copy(sql) as cp:
                for row in rows:
                    cp.write_row(row)
                    n += 1
        return n

    def begin(self):
        """Open a transaction. Raises if one is already open."""
        if self._in_tx:
            raise psycopg.ProgrammingError(
                "a transaction is already open on this connection")
        self._c.execute("BEGIN")
        self._in_tx = True

    def commit(self):
        if self._in_tx:
            self._c.commit()
            self._in_tx = False

    def rollback(self):
        if self._in_tx:
            self._c.rollback()
            self._in_tx = False

    def in_transaction(self):
        return self._in_tx

    def __enter__(self):
        if not self._in_tx:
            self.begin()
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False

    def set_trace_callback(self, callback):
        """Send the SQL of every statement issued here to `callback`, or None.

        Recorded in this wrapper rather than taken from the driver's logging,
        because psycopg does not log statement text at all -- the first version
        of this shim subscribed to the "psycopg.sql" logger and never fired, so
        the one test that counts statements was asserting on an empty list and
        could not fail. What is counted is what goes over the connection from
        here, which is the number that actually costs a round trip.

        executemany() counts once, not once per row: psycopg pipelines it into
        a single round trip, and the chunking tests are about round trips.
        """
        self._trace = callback

    def _trace_sql(self, sql):
        if self._trace is not None:
            self._trace(sql)

    def close(self):
        self._trace = None
        # Never leave a transaction open on the way out: an open one holds
        # row locks and a snapshot until the session ends.
        try:
            self.rollback()
        except psycopg.Error:
            pass
        self._c.close()


class _Bulk:
    """One big transaction opened on the DB connection."""

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        db = self.db
        assert not db._in_bulk
        # Explicit, and so an error if something is already open: a bulk
        # block nested inside another transaction would commit the outer one
        # at its own boundary.
        db.conn.begin()
        db._in_bulk = True
        db._buf = {}
        db._buf_del = set()
        db._buf_flags = []
        db._buf_bumps = {}
        return db

    def __exit__(self, exc_type, exc, tb):
        db = self.db
        db._in_bulk = False
        if exc_type is None:
            # Before the commit, deliberately: the buffered rows and the
            # counters describing them are part of this transaction, and a
            # commit that left them unwritten would persist a window of blocks
            # with no rows in it and counts that match nothing.
            db._flush_writes()
            db.conn.commit()
        else:
            db._discard_writes()
            db.conn.rollback()
        return False


# Identifiers are quoted throughout and carry COLLATE "C". Postgres would
# otherwise order text by the database's locale, so the same rows could sort
# differently on a different server -- txid ordering drives /api/address's
# output list and /api/mempool's, and "C" makes that order the same
# bytewise comparison sqlite3's BINARY collation gave.
SCHEMA = (
    """
CREATE TABLE IF NOT EXISTS "meta" (
    "key"   TEXT COLLATE "C" PRIMARY KEY,
    "value" TEXT
)
""",
    """
CREATE TABLE IF NOT EXISTS "blocks" (
    "height"     BIGINT PRIMARY KEY,
    "hash"       TEXT COLLATE "C" UNIQUE NOT NULL,
    "version"    BIGINT,
    "merkleroot" TEXT COLLATE "C",
    "time"       BIGINT,
    "nonce"      BIGINT,
    "bits"       TEXT COLLATE "C",
    "difficulty" DOUBLE PRECISION,
    "size"       BIGINT,
    "prev_hash"  TEXT COLLATE "C",
    "next_hash"  TEXT COLLATE "C"
)
""",
    """
CREATE TABLE IF NOT EXISTS "txs" (
    "txid"        TEXT COLLATE "C" PRIMARY KEY,
    "height"      BIGINT,          -- NULL while unconfirmed (mempool)
    "tx_index"    BIGINT,
    "version"     BIGINT,
    "locktime"    BIGINT,
    "size"        BIGINT,
    "is_coinbase" INTEGER,
    "status"      TEXT COLLATE "C" -- 'confirmed' | 'mempool' | 'orphaned'
)
""",
    """
CREATE TABLE IF NOT EXISTS "vin" (
    "txid"       TEXT COLLATE "C",
    "n"          BIGINT,
    "prev_txid"  TEXT COLLATE "C",   -- NULL for coinbase
    "prev_vout"  BIGINT,            -- NULL for coinbase
    "coinbase"   TEXT COLLATE "C",   -- hex, NULL unless coinbase
    "script_asm" TEXT COLLATE "C",
    "script_hex" TEXT COLLATE "C",
    "sequence"   BIGINT,
    PRIMARY KEY ("txid", "n")
)
""",
    # `mempool` and `spent_by` are denormalized facts about a row, not
    # accounting. Both are the answer to a question the balance queries used
    # to re-derive per output, which is what made a busy address take 41s.
    #
    # `mempool` is 1 while the owning tx is unconfirmed. It is set from the
    # inserting tx's height and never revisited: orphaned rows are DELETED
    # rather than flagged (see _clear_from), so "not confirmed" only ever
    # means mempool, and a row's owner cannot change status without the row
    # being rewritten. Which means the owner-side status join the balance
    # queries used to do is answerable from this bit.
    #
    # `spent_by` is a bitmask over the spender's status, and it is what
    # replaces the per-output EXISTS that dominated both lookups:
    #     0 = unspent, 1 = spent by a confirmed tx, 2 = by a mempool tx,
    #     3 = by both.  So `spent_by & 1` is "a confirmed tx spends this" and
    #     `spent_by & 3` is "any known tx spends this" (the old is_spent
    #     column). Two bits rather than one because the two balance views
    #     disagree about the spender as well as the owner: an output spent
    #     only by a mempool tx is confirmed-unspent, because a mempool spend
    #     can still evaporate.
    """
CREATE TABLE IF NOT EXISTS "vout" (
    "txid"       TEXT COLLATE "C",
    "n"          BIGINT,
    "value"      BIGINT,          -- in pokes (COIN = 1e8)
    "type"       TEXT COLLATE "C", -- pubkeyhash / scripthash / hybrid_... /
    "addresses"  TEXT COLLATE "C", -- JSON array of addresses (incl. hybrid)
    "req_sigs"   INTEGER,
    "script_asm" TEXT COLLATE "C",
    "script_hex" TEXT COLLATE "C",
    "script_hash" TEXT COLLATE "C",
    "mempool"    INTEGER NOT NULL DEFAULT 0,
    "spent_by"   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY ("txid", "n")
)
""",
    """
CREATE TABLE IF NOT EXISTS "addr_out" (
    "address" TEXT COLLATE "C",
    "txid"    TEXT COLLATE "C",
    "n"       BIGINT,
    "value"   BIGINT,
    "type"    TEXT COLLATE "C",
    "mempool"  INTEGER NOT NULL DEFAULT 0,
    "spent_by" INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY ("address", "txid", "n")
)
""",
    # Balances are deliberately NOT columns on scripts: they are derived per
    # request from vout/vin so that the confirmed and mempool-inclusive views
    # can both be computed exactly (see DB.script_balances).
    """
CREATE TABLE IF NOT EXISTS "scripts" (
    "script_hash"    TEXT COLLATE "C" PRIMARY KEY,
    "type"           TEXT COLLATE "C",  -- daemon classification
    "req_sigs"       INTEGER,
    "addresses"      TEXT COLLATE "C",  -- JSON array of owning addresses
    "created_height" BIGINT,            -- earliest confirmed height, or NULL
    "last_height"    BIGINT             -- latest confirmed height, or NULL
)
""",
    # Every index follows every table, so this whole tuple is the one and only
    # definition of the schema: a database is either built from all of it or
    # rebuilt from scratch (see DB.initialize). Nothing is ever added to an
    # existing table, which is why no DDL here has to tolerate a column that
    # may not exist yet.

    # status leads because every read that wants it wants only status, and a
    # mempool is a few dozen rows against 700k txs: the index is what keeps
    # `WHERE status='mempool'` -- the stale-mempool sweep the indexer runs
    # every cycle, and the count behind /api/summary's mempool cell, so both a
    # page load and a sync cycle -- off a sequential scan of the whole table
    # (measured 118ms over 730k rows here, growing forever). height trails
    # because the same index also answers the per-height confirmed count
    # /api/summary and /api/recent_blocks ask for.
    'CREATE INDEX IF NOT EXISTS idx_txs_status_height ON txs("status", "height")',
    'CREATE INDEX IF NOT EXISTS idx_addr_out_txid_n ON addr_out("txid", "n")',
    'CREATE INDEX IF NOT EXISTS idx_txs_height ON txs("height")',
    'CREATE INDEX IF NOT EXISTS idx_vin_prev ON vin("prev_txid", "prev_vout")',
    'CREATE INDEX IF NOT EXISTS idx_scripts_type ON scripts("type")',

    # The two covering indexes for the balance lookups, which lead with `value`:
    # the balance queries sum value over every output of one address/script,
    # and without it in the index each of those rows costs a separate lookup
    # into the table to fetch it. The trailing (txid, n) serves the address
    # page's spender enumeration, which otherwise has to revisit each of those
    # rows in the table.
    #
    # One index each, not a confirmed/mempool pair: a partial index would only
    # help the confirmed view, and with an empty mempool it would cover almost
    # every row anyway, so it would nearly double the write cost to save a
    # comparison on an index-only scan that is already sequential.
    #
    # No index on addr_out(address) alone: the primary key is
    # (address, txid, n), so its unique index already answers WHERE address=?
    # as a prefix seek, and idx_addr_out_addr answers it covering. The one on
    # (txid, n) is not redundant, because address leads the key and the primary
    # key cannot seek past it.
    #
    # No index on vout(addresses): it holds a JSON array and nothing filters on
    # it. Address accounting runs off addr_out, and multisig money is tracked
    # per script, so there is no lookup this could serve.
    "CREATE INDEX IF NOT EXISTS idx_addr_out_addr ON addr_out"
    "(address, value, spent_by, mempool, txid, n)",
    "CREATE INDEX IF NOT EXISTS idx_vout_script ON vout"
    "(script_hash, value, spent_by, mempool, txid, n)",
)

# Every index the schema creates, as its DDL, so the bulk loader can drop them
# all before a COPY and put them back after rather than paying to maintain them
# row by row across a whole chain. Derived from SCHEMA by filtering rather than
# written out, because a hand-maintained list drifts: this one listed three of
# the six, and the loader silently came back with an addr_out missing its
# (txid, n) index and the whole reorg path unindexed.
#
# Not included: the UNIQUE constraints declared inside the blocks and txs
# CREATE TABLEs (blocks_hash_key, txs_pkey). Those belong to their
# tables and cannot be dropped without dropping the table, which is why the
# loader empties with TRUNCATE rather than DROP.
ALL_INDEXES = tuple(
    stmt for stmt in SCHEMA
    if stmt.lstrip().upper().startswith("CREATE INDEX")
)

# The tables this schema owns, for DB._rebuild to drop. Derived the same way
# from the same DDL rather than listed again, so a table added to SCHEMA is
# dropped on the next rebuild without anyone remembering to name it here -- a
# missed name would leave the old table in place and let the recreate fail on
# the duplicate.
SCHEMA_TABLES = tuple(
    m.group(1) for stmt in SCHEMA
    for m in [re.search(r'CREATE TABLE IF NOT EXISTS "(\w+)"', stmt)] if m)

# What identifies "this schema" for DB.initialize. Derived from SCHEMA rather
# than hand-written, so editing the DDL is enough to make every existing
# database rebuild -- there is no separate version number to forget to bump.
#
# Normalised to collapse whitespace, so reindenting the DDL or a comment block
# is not mistaken for a schema change.
#
# What it does *not* do is ignore comment text: a reworded `--` comment moves
# the hash exactly as a dropped column does, and both cost a full reindex. That
# is left deliberately. Hashing less of the DDL would avoid the expensive
# surprises, but only if the stripper were exactly right, and a stripper that is
# subtly wrong here maps two genuinely different schemas onto one fingerprint --
# which is the failure this whole mechanism exists to prevent: a column present
# with its values missing, read as a valid chain. A needless rebuild costs
# minutes and is recoverable; a missed one is silently wrong data. So the hash
# stays over the whole statement and DB._rebuild announces what changed, which
# makes a comment-only edit a thing you can see rather than infer.
SCHEMA_TEXT = "\n".join(" ".join(s.split()) for s in SCHEMA)

SCHEMA_FINGERPRINT = hashlib.sha256(SCHEMA_TEXT.encode()).hexdigest()[:16]

FINGERPRINT_KEY = "schema_fingerprint"

# The text that fingerprint was taken over. Not consulted to decide anything --
# only so that a mismatch can show what changed instead of merely that
# something did. A database predating this key simply reports no diff.
SCHEMA_DDL_KEY = "schema_ddl"




ORPHAN_RETENTION = 20000  # tombstones kept this many blocks before pruning

# The row INSERTs, hoisted out of the per-tx method so that a buffered bulk
# window can hold them alongside the rows they will eventually write. Same text
# as they always were; only the number of round trips changed.
SQL_INSERT_TXS = """INSERT INTO txs
       (txid, height, tx_index, version, locktime, size,
        is_coinbase, status)
       VALUES (?,?,?,?,?,?,?,?)"""

SQL_INSERT_VIN = """INSERT INTO vin
       (txid, n, prev_txid, prev_vout, coinbase,
        script_asm, script_hex, sequence)
       VALUES (?,?,?,?,?,?,?,?)"""

SQL_INSERT_VOUT = """INSERT INTO vout
       (txid, n, value, type, addresses, req_sigs,
        script_asm, script_hex, script_hash, mempool)
       VALUES (?,?,?,?,?,?,?,?,?,?)"""

SQL_INSERT_ADDR_OUT = """INSERT INTO addr_out
       (address, txid, n, value, type, mempool)
       VALUES (?,?,?,?,?,?)"""

SQL_INSERT_SCRIPTS = """INSERT INTO scripts
       (script_hash, type, req_sigs, addresses,
        created_height, last_height)
       VALUES (?,?,?,?,?,?)
       ON CONFLICT(script_hash) DO UPDATE SET
         created_height = LEAST(scripts.created_height,
                                EXCLUDED.created_height),
         last_height = GREATEST(scripts.last_height,
                                EXCLUDED.last_height)"""

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
#
# The ::bigint is load-bearing too: Postgres sums bigint into NUMERIC, and
# without the cast the driver hands back a Decimal that json.dumps() refuses.
TOTAL_COINBASE_SQL = (
    "SELECT COALESCE(SUM(v.value)::bigint, 0) FROM vout v JOIN txs t "
    "ON v.txid = t.txid WHERE t.is_coinbase = 1 AND t.status = 'confirmed'")

# The maintained running total. Unlike a balance, minted supply has no
# mempool/confirmed split to get wrong and no double-spend to net out: it
# is a plain accumulator over coinbase outputs, and the write path already
# knows each coinbase tx's output values at insert time. So it is kept in
# the same transaction as the rows it describes -- a reader takes one
# indexed meta lookup instead of a 31s scan, and sees the counter and the
# data from a single consistent snapshot because they commit together.
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

# Bind parameters per statement, chunked. Postgres allows 65535 parameters
# per statement, which is far more than any of these lists reaches, but the
# cap is a hard error rather than a degradation: an eviction or a reorg hands
# us an id list whose size we did not choose, and a statement over the cap
# would take the enclosing transaction with it. A mempool past the cap would
# then never be cleanable, and a reorg past it would never apply, both
# forever. 500 keeps the batches cache-friendly and is nowhere near the cap.
#
# Read through the module global at call time rather than captured as a
# default argument, so that patching it actually changes the chunking -- which
# is how the tests drive real chunking on a server whose own limit is too high
# to reach.
SQL_VAR_CHUNK = 500


def _chunks(seq, size=None):
    """Yield `seq` in slices of at most `size`, for building IN (...) lists."""
    seq = list(seq)
    size = SQL_VAR_CHUNK if size is None else size
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


class DB:
    # How long a statement waits for a lock held by another session. The
    # schema window is generous because it is rare, bounded, and must not fail
    # halfway; ordinary work waits seconds and then errors, so a caller retries
    # on its next cycle instead of parking a request thread.
    NORMAL_LOCK_TIMEOUT_MS = 5000
    SCHEMA_LOCK_TIMEOUT_MS = 1800000

    # Serializes schema work across sessions (the indexer and the web server
    # share this database): the first comer builds, the rest block on
    # lock_timeout and then see the fingerprint already written.
    #
    # Keyed on the schema name so two explorer databases in one server (the
    # test suite creates one per test) do not queue behind each other. Only
    # over-serialization, never under: a hash collision would make two
    # databases wait for each other, which is slower and still correct.
    SCHEMA_LOCK_SQL = "SELECT pg_advisory_xact_lock(" \
                      "hashtextextended(current_schema(), 0))"

    @classmethod
    def initialize(cls, dsn, rebuild=False):
        """Build the schema if absent, and return a usable connection.

        For the indexer, and for any process that starts cold. Runs DDL, so it
        is the slow path -- once per process, not once per request.

        rebuild=True additionally means "this database may be thrown away if its
        shape is not the one this code wants". A rebuild is a from-scratch
        reindex, so only the indexer may ask for one; the web side uses
        initialize(dsn) and never discards anything. See _rebuild for why the
        schema is never migrated in place.
        """
        return cls(dsn, schema=True, rebuild=rebuild)

    @classmethod
    def connect(cls, dsn):
        """Open an existing database without touching the schema.

        The read side (the web API) must never run CREATE TABLE: those are
        per-process costs, and paying them on every request dominated the
        handler's own work. The schema is expected to exist already --
        call initialize() once at startup.

        A connection is not bound to the thread that made it, which is what
        lets the web side pool one set and hand them to request threads.
        """
        return cls(dsn, schema=False)

    def __init__(self, dsn, schema=True, rebuild=False, lock_timeout=None):
        self._in_bulk = False
        # Bulk-window write buffer. See _flush_writes. All three are None
        # outside a bulk window, which is what makes "am I buffering?" a single
        # `is not None` at each write site rather than a flag to keep in step.
        self._buf = None        # txid -> {table: [rows]} for buffered INSERTs
        self._buf_del = None    # txids to DELETE, batched into one pass at flush
        self._buf_flags = None  # (txid, n) pairs awaiting _refresh_spent_flags
        self._buf_bumps = None  # meta key -> accumulated delta
        self.conn = _PgConn(dsn)
        try:
            self._open(schema, rebuild, lock_timeout)
        except BaseException:
            # Everything after the connection opens can raise -- a SET, or a
            # rebuild that fails partway (a full disk, a killed indexer). When
            # it does, __init__ raises and the caller never gets the object, so
            # it has no handle to close the connection with: leaving it to the
            # collector is what makes that failure announce itself as an
            # unclosed connection rather than as the error it is. Close it
            # here, so the only thing the caller sees is the error.
            self.conn.close()
            raise

    def _open(self, schema, rebuild, lock_timeout):
        # synchronous_commit=off is the durability/throughput trade SQLite's
        # synchronous=NORMAL made: a crash can lose recent commits, but the
        # server does not fsync each one. Every figure on screen is
        # reconstructible from the chain, and the indexer re-reads it.
        self.conn.execute("SET synchronous_commit = off")
        # Building a schema, or throwing one away and rebuilding it, can hold
        # this advisory lock for a long time. The other process opening this
        # same database must wait for it rather than die on a lock error -- so
        # the long timeout applies to schema work, which must also precede it
        # being lowered. A connection that never builds a schema never gets the
        # long timeout at all.
        self.conn.execute("SET lock_timeout = '%dms'" % (
            lock_timeout or (self.SCHEMA_LOCK_TIMEOUT_MS if schema
                             else self.NORMAL_LOCK_TIMEOUT_MS)))
        if not schema:
            return
        # Postgres has no executescript(): psycopg raises on a multi-statement
        # execute(). More to the point, DDL is transactional here, so the
        # whole schema can go in one transaction and a failure leaves no
        # half-created tables behind -- which the sqlite3 version could not
        # offer, since executescript() committed as it went.
        with self.conn:
            # Serialize the WHOLE schema build, not just a few steps. CREATE
            # TABLE IF NOT EXISTS is not race-safe in PostgreSQL: the existence
            # check and the catalog insert are not one atomic act, so two
            # sessions creating the same table at the same moment can still
            # collide in pg_type and one of them dies with "duplicate key value
            # violates unique constraint pg_type_typname_nsp_index" -- IF NOT
            # EXISTS and all.
            #
            # That is not hypothetical: on a cold start the indexer and the web
            # server both call initialize() within a second of each other, the
            # web server was the one that lost, and it died before binding, so
            # the site was unreachable until the second run.
            #
            # Re-entrant within this transaction, so anything below taking it
            # again is harmless; it is released at the commit either way.
            self.conn.execute(self.SCHEMA_LOCK_SQL)
            # Whether this database was already there. It decides who is allowed
            # to vouch for the shape, which is the one thing the fingerprint is.
            existed = self.conn.execute(
                "SELECT to_regclass('meta') IS NOT NULL").fetchone()[0]
            if rebuild:
                self._rebuild()
            for statement in SCHEMA:
                self.conn.execute(statement)
            # Stamp the fingerprint only when this connection can actually
            # vouch for the shape: either it was asked to rebuild, so it checked
            # or made it correct, or it built the schema itself just now.
            #
            # The read side stamps nothing on a database it merely found. That
            # matters because the two processes race at startup: if the web
            # server claimed a pre-existing database as current, the indexer
            # arriving a moment later would read a matching fingerprint, skip
            # its rebuild, and the old chain would survive a schema this code
            # cannot read. A missing fingerprint costs the indexer one rebuild;
            # a wrong one costs it silently serving the wrong data.
            if rebuild or not existed:
                self.set_meta(FINGERPRINT_KEY, SCHEMA_FINGERPRINT)
                # Alongside the fingerprint, not instead of the check: this is
                # only ever read to explain a mismatch, so writing it only
                # where the fingerprint is stamped costs nothing and keeps a
                # read-only opener from claiming a database it did not check.
                self.set_meta(SCHEMA_DDL_KEY, SCHEMA_TEXT)
        # Steady-state work -- indexing, and every web request -- waits only
        # briefly. A web reader that parks for SCHEMA_LOCK_TIMEOUT_MS turns one
        # long build into 50 requests hanging for half an hour; a blocked caller
        # should fail fast and be retried instead.
        if lock_timeout is None:
            self.conn.execute("SET lock_timeout = '%dms'"
                              % self.NORMAL_LOCK_TIMEOUT_MS)

    def _announce_rebuild(self):
        """Say that the chain is about to be discarded, and what changed.

        A rebuild is the one operation here that destroys indexed data, and it
        is reached by a hash comparison that nothing prints. Without this the
        only symptom is an explorer that has gone empty and is slowly filling
        again, minutes later, which reads as a crash rather than as a schema
        change -- and the fingerprint covers comment text, so a reworded
        comment discards the chain exactly as a dropped column does. Those two
        want very different reactions, so the announcement carries the diff that
        tells them apart.
        """
        print("warning: schema fingerprint %s != %s; discarding the indexed "
              "chain and reindexing from genesis"
              % (self.get_meta(FINGERPRINT_KEY), SCHEMA_FINGERPRINT), flush=True)
        stored = self.get_meta(SCHEMA_DDL_KEY)
        if stored is None:
            print("  no schema text on record for this database, so the "
                  "difference cannot be shown", flush=True)
            return
        for line in difflib.unified_diff(
                stored.splitlines(), SCHEMA_TEXT.splitlines(),
                fromfile="schema on disk", tofile="schema in this code",
                lineterm="", n=1):
            print("  " + line, flush=True)


    def _rebuild(self):
        """Throw the database away if it is not the schema this code wants.

        The policy is that the schema is never migrated in place: a shape
        change costs a from-scratch reindex, which on this chain is a few
        minutes of wall clock and no downtime. What that buys is the removal of
        every in-place migration path, and with them the entire class of bugs
        where a migration left a column present but its values missing. That is
        not hypothetical either: adding `spent_by` as a column defaulting to 0
        without backfilling it reads as "nothing is spent", so the balance
        queries report the entire supply to every address.

        The cost is that a deployment which edits SCHEMA discards its indexed
        chain. That is the deliberate trade -- it is the one operation here that
        destroys data, so it is not reachable from the web side, which opens
        with rebuild=False and can therefore never trigger one.

        Only objects this schema owns are dropped, and meta is the gate: a
        database that predates the fingerprint (or carries a stale one) is not
        known to be a shape this code understands, so it is emptied outright
        rather than inspected for compatibility.

        Nothing is dropped when the fingerprint already matches, which is every
        start after the first. The indexer re-syncs only when the chain is
        actually behind, so this is a no-op on a healthy database and a full
        reindex on a code change.
        """
        # A database that has no meta table yet has nothing to lose; a database
        # that does not record its fingerprint predates this policy and cannot
        # be vouched for. Either way, only ask the question when meta exists.
        present = self.conn.execute(
            "SELECT to_regclass('meta') IS NOT NULL").fetchone()[0]
        if not present:
            return
        if self.get_meta(FINGERPRINT_KEY) == SCHEMA_FINGERPRINT:
            return
        self._announce_rebuild()
        # Only tables this schema owns are dropped, and meta is the gate: a
        # database that predates the fingerprint (or carries a stale one) is not
        # known to be a shape this code understands, so it is emptied outright
        # rather than inspected for compatibility.
        #
        # DROP TABLE, not TRUNCATE, because this is the from-scratch path and
        # truncate would leave behind an index the schema no longer declares --
        # and a leftover index costs write time on every block forever, with
        # nothing to notice it.
        #
        # Qualified with current_schema() rather than left to search_path, and
        # emphatically not `pg_temp`: that alias names the session's *temp*
        # schema, so `DROP TABLE IF EXISTS pg_temp."blocks"` finds nothing there
        # and skips it in silence, which reads as a successful rebuild and
        # leaves the old chain in place. Dropping by name without CASCADE means
        # an unexpected dependency surfaces as an error rather than as a
        # silently deleted object.
        #
        # PostgreSQL DDL is transactional, so this and the recreate in the
        # caller are one atomic step: a failure here leaves the old chain
        # exactly as it was.
        schema = self.conn.execute("SELECT current_schema()").fetchone()[0]
        self.conn.execute("DROP TABLE IF EXISTS %s" % ", ".join(
            "%s.%s" % (_quote_ident(schema), _quote_ident(t))
            for t in SCHEMA_TABLES))

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
        # Write the window out first. A reorg discovered mid-window truncates
        # everything from here on, and rows still buffered for those very blocks
        # would be inserted again by the flush at __exit__ -- undoing the
        # truncation the caller just asked for, inside the same transaction.
        self._flush_writes()
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
                #
                # The ::bigint on each VALUES row is what gives the CTE its
                # column types: Postgres infers an undecorated parameter as
                # text, and n has to come out bigint to compare with the
                # bigint columns it is matched against.
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
                    % (",".join(["(?,?::bigint)"] * len(chunk)), table,
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
            # One statement, not four, through data-modifying CTEs. Every one of
            # them deletes the same txid set from a different table, none of them
            # reads what another wrote, and an unreferenced data-modifying CTE is
            # still executed -- so this is four deletes for the price of one round
            # trip, which is the whole cost at a fresh height where all four
            # match nothing. Kept as DELETEs rather than skipped when the txid is
            # new: "no txs row" is not proof of "no child rows", and a re-index
            # that left a stray vin/vout behind would double-count the balance.
            self.conn.execute(
                "WITH d1 AS (DELETE FROM txs      WHERE txid = ANY(?::text[])), "
                "     d2 AS (DELETE FROM vin      WHERE txid = ANY(?::text[])), "
                "     d3 AS (DELETE FROM vout     WHERE txid = ANY(?::text[])), "
                "     d4 AS (DELETE FROM addr_out WHERE txid = ANY(?::text[])) "
                "SELECT 1", (chunk, chunk, chunk, chunk))

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
            "SELECT COALESCE(SUM(v.value)::bigint, 0), COUNT(*) FROM txs t "
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
        # already hold: the upsert below cannot report which it did, and the
        # block count is the number of heights, not of writes.
        new_height = self.conn.execute(
            "SELECT 1 FROM blocks WHERE height=?", (b.height,)).fetchone() is None
        # ON CONFLICT (height) rather than the INSERT OR REPLACE this replaces:
        # REPLACE would also swallow a violation of blocks.hash's unique
        # index, deleting whichever row held the hash. That can only happen if
        # a hash is indexed at two heights, and a reorg reaches that through
        # _clear_from, which deletes the tail before anything is re-added --
        # so a height conflict is the only one left to handle, and letting the
        # other one raise is better than silently dropping a block row.
        self.conn.execute(
            """INSERT INTO blocks
               (height, hash, version, merkleroot, time, nonce, bits,
                difficulty, size, prev_hash, next_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT (height) DO UPDATE SET
                 hash=EXCLUDED.hash, version=EXCLUDED.version,
                 merkleroot=EXCLUDED.merkleroot, time=EXCLUDED.time,
                 nonce=EXCLUDED.nonce, bits=EXCLUDED.bits,
                 difficulty=EXCLUDED.difficulty, size=EXCLUDED.size,
                 prev_hash=EXCLUDED.prev_hash, next_hash=EXCLUDED.next_hash""",
            (b.height, b.hash, b.version, b.merkleroot, b.time, b.nonce,
             b.bits, b.difficulty, b.size, b.prev_hash, b.next_hash))
        self._bump(N_BLOCKS_KEY, 1 if new_height else 0)

    def _discard_writes(self):
        """Drop everything buffered, for a bulk window that failed."""
        self._buf = None
        self._buf_del = None
        self._buf_flags = None
        self._buf_bumps = None

    def _flush_writes(self):
        """Write out a bulk window: deletes, rows, flags, counters, in order.

        The order is the dependency order and must not be rearranged. Deletes
        before the inserts, or a window that re-indexes a txid would collide
        with its own buffered rows on the primary key. The rows before the
        flags, because the flags are derived from the vin rows by EXISTS and
        would read an empty table if they went first. The flags before the
        counters, because nothing in a counter depends on them but a caller
        that reads a balance inside the same transaction would see rows with
        stale masks if the flags were still pending.

        Safe to call when nothing is buffered, and safe to call repeatedly: both
        are how the paths that must not see buffered rows (a read, an eviction, a
        re-index of a txid already held) force the window down early. Note that
        flushing EMPTIES the buffers rather than dismantling them -- a window
        can be flushed several times and must keep buffering after each one, or
        the rest of the window would silently drop back to writing per tx.
        """
        if self._buf is None:
            return
        if self._buf_del:
            self._delete_tx_rows(sorted(self._buf_del))
        rows_by_table = {}
        for table_rows in self._buf.values():
            for table, sql, rows in table_rows:
                rows_by_table.setdefault(table, (sql, []))[1].extend(rows)
        for sql, rows in rows_by_table.values():
            for chunk in _chunks(rows):
                self.conn.executemany(sql, chunk)
        flags = sorted(self._buf_flags)
        bumps = dict(self._buf_bumps)
        self._buf.clear()
        self._buf_del.clear()
        self._buf_flags.clear()
        self._buf_bumps.clear()
        # Take the window down before applying what it held, and put it back
        # after. This is not tidiness: the buffers are now empty but not None,
        # and _bump treats a non-None buffer as "a window is open", so writing
        # the counters while the window was still standing would re-buffer every
        # delta and the flush would finish having written nothing to meta. Same
        # for the flag refresh if it ever bumps anything.
        standing = (self._buf, self._buf_del, self._buf_flags, self._buf_bumps)
        self._buf = self._buf_del = self._buf_flags = self._buf_bumps = None
        try:
            if flags:
                self._refresh_spent_flags(flags)
            for key, delta in bumps.items():
                self._bump(key, delta)
        finally:
            # The window is still open, so buffering has to resume for whatever
            # comes next -- even if applying what it held just failed.
            (self._buf, self._buf_del,
             self._buf_flags, self._buf_bumps) = standing

    def _write_rows(self, txid, table_rows):
        """Replace `txid`'s rows with `table_rows`, now or at the flush.

        Owns the delete for both paths, because the two have to agree on when
        it happens: outside a window the old rows go now, before the new ones;
        inside one, the txid joins `_buf_del` and the flush does both in that
        order. Splitting the delete out to the caller is what let the sets drift
        apart -- a flush triggered from here empties `_buf_del`, so a txid
        re-buffered immediately afterwards has to put itself back in.
        """
        if self._buf is None:
            self._delete_tx_rows([txid])
            for _, sql, rows in table_rows:
                if rows:
                    self.conn.executemany(sql, rows)
            return
        # A txid already buffered in this window: its rows are not in the table
        # yet, so the SELECTs _add_tx is about to make -- what this txid used to
        # hold, and whether it is a re-index -- would miss them. Put the window
        # down first rather than reason about which of the two answers is right.
        # That flush also writes the version now being replaced, so re-adding
        # the txid below is what gives the replacement a delete of its own.
        if txid in self._buf:
            self._flush_writes()
        self._buf_del.add(txid)
        self._buf[txid] = table_rows

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
                "SELECT COALESCE(SUM(value)::bigint, 0) FROM vout WHERE txid=?",
                (t.txid,)).fetchone()[0]
        # The one place a row's confirmed-vs-mempool bit is decided. Set from
        # the incoming tx's height and never revisited afterwards, because
        # orphaned rows are deleted rather than flagged, so an output's owner
        # cannot change status without the output row itself being rebuilt
        # here. Deriving it per read instead meant a txs lookup per output,
        # which on the 510k-output address was 510k random seeks to learn
        # something every row already knew.
        mempool = 0 if t.height is not None else 1
        tx_rows = [(t.txid, t.height, t.tx_index, t.version, t.locktime,
                    t.size, 1 if t.is_coinbase else 0,
                    'confirmed' if t.height is not None else 'mempool')]
        vin_rows = []
        vout_rows = []
        addr_rows = []
        script_rows = []
        for i, ipt in enumerate(t.vin):
            vin_rows.append(
                (t.txid, i, ipt.prev_txid, ipt.prev_vout, ipt.coinbase,
                 ipt.script_asm, ipt.script_hex, ipt.sequence))
        for n, ot in enumerate(t.vout):
            sh = script_hash_of(ot.script_hex)
            vout_rows.append(
                (t.txid, n, ot.value, ot.type, json.dumps(ot.addresses),
                 ot.req_sigs, ot.script_asm, ot.script_hex, sh, mempool))
            # Consensus accounting lives at SCRIPT level: the output belongs
            # to the whole script (all-of-N participants for multisig), not
            # to any single participant. So a multi-address vout credits no
            # one fully -- attests the value into the scripts entity instead.
            if len(ot.addresses) == 1:
                addr_rows.append(
                    (ot.addresses[0], t.txid, n, ot.value, ot.type, mempool))
            if sh is not None:
                # Metadata only. Balances are derived from vout/vin on read (see
                # DB.script_balances), so nothing here is a counter that a
                # re-index, a mempool eviction or a double-spend could skew.
                #
                # Heights are confirmed-only: a mempool tx arrives with a NULL
                # height and must neither fabricate a height nor erase a real
                # one. LEAST/GREATEST is what carries that, and the dialect
                # difference matters if this is ever ported back: Postgres
                # skips NULL arguments and returns NULL only when every
                # argument is, whereas SQLite's two-argument min()/max()
                # propagates NULL and so erases a known height whenever an
                # unconfirmed one arrives. Under Postgres the bare form is the
                # whole fix -- NULL loses to the real value either way -- and
                # the COALESCE pairs SQLite needed are dropped with it.
                script_rows.append(
                    (sh, ot.type, ot.req_sigs, json.dumps(ot.addresses),
                     t.height, t.height))
        self._write_rows(t.txid, (
            ("txs", SQL_INSERT_TXS, tx_rows),
            ("vin", SQL_INSERT_VIN, vin_rows),
            ("vout", SQL_INSERT_VOUT, vout_rows),
            ("addr_out", SQL_INSERT_ADDR_OUT, addr_rows),
            ("scripts", SQL_INSERT_SCRIPTS, script_rows),
        ))
        # is_spent is a fact about vin, not something the INSERTs above get to
        # decide, and this is the only place that writes it. Deriving it in the
        # input loop instead would miss a child indexed before its parent, and
        # miss a re-index too: the rows are deleted and rebuilt over the column
        # default. So recompute the three sets the INSERTs above could have got
        # wrong -- this version's inputs (marked spent by the vin rows just
        # written), its own outputs, and the prevouts a previous version held
        # that this one does not -- in one batched pass at the end.
        if self._buf_flags is not None:
            # Deferred to the flush: the flags are an EXISTS over the vin table,
            # so they cannot be computed until this tx's vin rows are actually
            # in it -- and recomputing them once for the whole window is both
            # correct (they are derived, and idempotent) and far cheaper than
            # once per tx.
            self._buf_flags.extend(sorted(held))
            self._buf_flags.extend((t.txid, n) for n, _ in enumerate(t.vout))
            self._buf_flags.extend(released)
        else:
            self._refresh_spent_flags(
                sorted(held) + [(t.txid, n) for n, _ in enumerate(t.vout)]
                + released)
        # Minted supply moves by exactly what this version added over what the
        # previous one had counted. height IS NULL while unconfirmed, so a
        # mempool coinbase adds nothing until it is indexed into a block.
        #
        # The is_coinbase test is load-bearing and must match the prev_minted test
        # above: a normal tx redistributes value already in circulation, so it mints
        # nothing, and only prev_minted was gated on it. Counting every confirmed
        # tx's outputs here while only subtracting coinbases left the counter
        # tracking total confirmed throughput instead of supply -- on this rig, 4.46x
        # too high, growing with every transaction.
        self._bump(COINBASE_TOTAL_KEY,
                   (sum(ot.value for ot in t.vout)
                    if t.is_coinbase and t.height is not None else 0)
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
        Flushed first, for the same reason as clear_from: an eviction must see
        the rows an open window is holding, or it deletes nothing and the flush
        then re-inserts what was just evicted.
        That makes the result independent of the order the txs are removed in:
        an output spent by two of them, or by one whose own output is also
        being removed, is re-evaluated against the final set of spending
        inputs instead of a half-emptied table. The scripts metadata is
        rebuilt from what survives, for the same order-independence.
        """
        self._flush_writes()
        txids = list(txids)
        if not txids:
            return
        if not self._in_bulk:
            with self.conn:
                self._remove_txs(txids)
        else:
            self._remove_txs(txids)

    def _remove_txs(self, txids):
        # Chunked because a mempool eviction can be long, and one statement
        # cannot hold every stale id. The chunks share the single transaction
        # remove_txs() opened, so the eviction is still all-or-nothing: either
        # every stale tx goes or none of it does.
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
                "AND t.status='confirmed' THEN (SELECT COALESCE(SUM(v.value)"
                "::bigint, 0) "
                "FROM vout v WHERE v.txid = t.txid) ELSE 0 END)::bigint, 0) "
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
        # A read inside an open window would otherwise see the table without the
        # rows still sitting in the buffer, which is the one thing a reader
        # cannot be allowed to do. Cheap when nothing is buffered.
        self._flush_writes()
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
        # Same reasoning as query(): a counter read inside an open window would
        # miss the deltas still buffered for it. _bump only reaches this when
        # nothing is buffered, so the flush here cannot re-enter.
        self._flush_writes()
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return int(row[0]) if row and row[0] is not None else default

    def _bump(self, key, delta):
        """Add `delta` to a counter, inside the caller's transaction.

        A bare execute(), never set_meta(): set_meta opens `with self.conn`,
        which COMMITs, and that would end the surrounding bulk transaction
        mid-block -- committing a half-indexed block. Same reason
        rebuild_scripts avoids set_meta.

        Read-modify-write in Python rather than SQL arithmetic, so the value
        is an int the whole way rather than a string that meta.value's TEXT
        column has to round-trip on every block.
        """
        if not delta:
            return
        if self._buf_bumps is not None:
            # Accumulated rather than read-and-written per tx: a window of 500
            # txs would otherwise do 500 read-modify-writes of the same three
            # meta rows, which is two round trips each to move a number that
            # only has to be right at the end. The flush writes the sum once.
            self._buf_bumps[key] = self._buf_bumps.get(key, 0) + delta
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
        self._flush_writes()
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

        Deliberately does NOT commit: the caller owns the transaction, and
        Nothing inside the explorer calls it: every write path maintains the
        counters in the same transaction as the rows, and a database built from
        scratch has them from block 0. So a mismatch means something has
        diverged, and rescanning is how you find out what. The cost is that a
        caller which runs this and exits without committing silently discards
        the repair. So commit after calling this.
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
    # `spent_by` is a bitmask, so the test has to be a comparison and not a bare
    # mask: SQLite treats any nonzero integer as true, Postgres insists on a
    # real boolean and would reject `CASE WHEN spent_by & 1 THEN` outright.
    # `& 0 <> 0` is written longhand rather than `::bool` so the condition
    # stays true/false on NULL the same way it did under SQLite -- a NULL
    # mask is falsy there, and `<> 0` against NULL is NULL, so the row is
    # skipped either way.
    _BALANCE_VIEWS = (
        # view        owner        spender
        ("confirmed", "mempool=0", "(spent_by & 1) <> 0"),
        ("live",      "TRUE",      "(spent_by & 3) <> 0"),
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
                "SELECT COALESCE(SUM(CASE WHEN %s THEN value END)::bigint, 0), "
                "       COUNT(CASE WHEN %s THEN 1 END), "
                "       COALESCE(SUM(CASE WHEN %s AND (%s) "
                "                    THEN value END)::bigint, 0), "
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

        With no argument, rebuild the whole table: on a reorg that
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
        self._flush_writes()
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
            # ON CONFLICT rather than INSERT OR REPLACE: the columns are all
            # overwritten wholesale either way, and upsert leaves the existing
            # row's identity alone, where REPLACE would delete and reinsert it.
            self.conn.executemany(
                """INSERT INTO scripts
                   (script_hash, type, req_sigs, addresses,
                    created_height, last_height)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT (script_hash) DO UPDATE SET
                     type=EXCLUDED.type, req_sigs=EXCLUDED.req_sigs,
                     addresses=EXCLUDED.addresses,
                     created_height=EXCLUDED.created_height,
                     last_height=EXCLUDED.last_height""", rows)
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
                "INSERT INTO meta(key, value) VALUES (?,?) "
                "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                (key, value))