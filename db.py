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

CREATE INDEX IF NOT EXISTS idx_addr_out_address ON addr_out(address);
CREATE INDEX IF NOT EXISTS idx_addr_out_txid_n ON addr_out(txid, n);
CREATE INDEX IF NOT EXISTS idx_vout_address ON vout(addresses);
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

    def _migrate(self):
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

    def _retract_spent_flags(self, freed):
        """Recompute is_spent for the given (txid, n) outputs only.

        `freed` are the outputs whose spending inputs have just gone away, so
        each may have no spender left -- but the surviving vin rows are the
        authority, not the assumption. EXISTS keeps a conflict right: an output
        two removed txs both spent, or one removed tx and one mempool tx spent,
        still has a spender and stays spent.
        """
        for txid, n in freed:
            self.conn.execute(
                """UPDATE addr_out SET is_spent = EXISTS (
                     SELECT 1 FROM vin
                     WHERE vin.prev_txid=? AND vin.prev_vout=?)
                   WHERE txid=? AND n=?""",
                (txid, n, txid, n))

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
        self._retract_spent_flags(freed)
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
        self.conn.execute("DELETE FROM txs WHERE txid=?", (t.txid,))
        self.conn.execute("DELETE FROM vin WHERE txid=?", (t.txid,))
        self.conn.execute("DELETE FROM vout WHERE txid=?", (t.txid,))
        self.conn.execute("DELETE FROM addr_out WHERE txid=?", (t.txid,))
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
            if ipt.coinbase is None and ipt.prev_txid:
                # mark the previous output as spent
                self.conn.execute(
                    "UPDATE addr_out SET is_spent=1 "
                    "WHERE txid=? AND n=?", (ipt.prev_txid, ipt.prev_vout))
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
        # Snapshot the prevouts first: the rows go away with the txs, and the
        # recompute below needs to know which outputs they were holding.
        spent_prev = self.conn.execute(
            "SELECT DISTINCT prev_txid, prev_vout FROM vin "
            "WHERE txid IN (%s) AND prev_txid IS NOT NULL"
            % ",".join("?" * len(txids)), txids).fetchall()
        # ...and the scripts these txs' outputs were evidence of, for the same
        # reason: vout is what rebuild_scripts() aggregates, and it is about to
        # be empty. Without this a script seen only in the mempool keeps its
        # row forever, and /api/script/<hash> answers 200 for a script that no
        # longer exists in the chain or the mempool -- zero balances, but real
        # type/addresses/height metadata.
        touched = [r[0] for r in self.conn.execute(
            "SELECT DISTINCT script_hash FROM vout "
            "WHERE txid IN (%s) AND script_hash IS NOT NULL"
            % ",".join("?" * len(txids)), txids)]
        for txid in txids:
            self.conn.execute("DELETE FROM txs WHERE txid=?", (txid,))
            self.conn.execute("DELETE FROM vin WHERE txid=?", (txid,))
            self.conn.execute("DELETE FROM vout WHERE txid=?", (txid,))
            self.conn.execute("DELETE FROM addr_out WHERE txid=?", (txid,))
        # An output any of them spent may have no spender left. EXISTS runs
        # against the post-delete table, so it also covers the case where the
        # last spender was itself in this batch.
        for ptid, pn in spent_prev:
            self.conn.execute(
                """UPDATE addr_out SET is_spent = EXISTS (
                     SELECT 1 FROM vin
                     WHERE vin.prev_txid=? AND vin.prev_vout=?)
                   WHERE txid=? AND n=?""",
                (ptid, pn, ptid, pn))
        # Recompute rather than delete: a script with a surviving output
        # elsewhere keeps its row, and the rebuild both restores its metadata
        # and drops the ones nothing is left for. Scoped for the same reason
        # clear_from scopes its own -- one mempool eviction must not re-aggregate
        # every output in the index.
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
        if script_hashes is not None:
            script_hashes = list(dict.fromkeys(script_hashes))
            if not script_hashes:
                return
            # Same aggregate as the full rebuild, narrowed by an index lookup
            # instead of grouping the whole table.
            where = "WHERE v.script_hash IN (%s)" % ",".join(
                "?" * len(script_hashes))
        else:
            self.conn.execute("DELETE FROM scripts")
            where = "WHERE v.script_hash IS NOT NULL"
        rows = self.conn.execute(
            """SELECT v.script_hash, MAX(v.type), MAX(v.req_sigs),
                      MAX(v.addresses),
                      MIN(t.height), MAX(t.height)
               FROM vout v
               JOIN txs t ON t.txid = v.txid
               %s
               GROUP BY v.script_hash""" % where,
            script_hashes or ()).fetchall()
        self.conn.executemany(
            """INSERT OR REPLACE INTO scripts
               (script_hash, type, req_sigs, addresses,
                created_height, last_height)
               VALUES (?,?,?,?,?,?)""", rows)
        if script_hashes is not None:
            # A script whose every output was severed has nothing left to
            # describe, so it leaves the table -- the same thing the full
            # rebuild's DELETE would have done to it.
            seen = {r[0] for r in rows}
            self.conn.executemany(
                "DELETE FROM scripts WHERE script_hash=?",
                [(h,) for h in script_hashes if h not in seen])

    def get_meta(self, key, default=None):
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_meta(self, key, value):
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES (?,?)",
                (key, value))