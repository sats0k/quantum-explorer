# PhoenixCoin Quantum Block Explorer

A minimal block explorer for the PhoenixCoin Quantum network, written in
pure Python with PostgreSQL storage. The only dependency is `psycopg` (psycopg3);
everything else is the standard library.

Hybrid transactions are supported **for free**: the indexer stores the
daemon's own verbose `getrawtransaction` output, which already decodes the
hybrid script templates (`hybrid_pubkey`, `hybrid_pubkeyhash`,
`hybrid_multisig`) and hybrid address prefixes (`0x3A` / `0x6A`). No
script parsing is reimplemented on the explorer side.

## Layout

| File              | Purpose                                        |
| ----------------- | ---------------------------------------------- |
| `indexer.py`      | Syncs chain + mempool from the daemon into PostgreSQL |
| `db.py`           | Schema and queries (`blocks`, `txs`, `vin`, `vout`, `addr_out`, `scripts`, `meta`) |
| `rpc.py`          | JSON-RPC client (basic auth)                   |
| `server.py`       | Read-only JSON API + static web UI             |
| `explorer.sh`     | Start/stop/status wrapper for indexer + web UI |
| `web/index.html`  | Single-page explorer frontend                  |
| `test_db.py`      | Regression tests for indexing + accounting     |

## Setup

```
pip install psycopg          # psycopg3; the only dependency
createdb explorer            # the indexer's database
```

The explorer needs a role that can create tables and indexes in that database
and nothing more:

```sql
CREATE ROLE explorer LOGIN PASSWORD '...';
CREATE DATABASE explorer OWNER explorer;
```

## Tests

```
EXPLORER_TEST_DSN='dbname=explorer_test' python3 -m unittest test_db -v
```

A PostgreSQL server is required but no daemon is. Each test builds its own
schema inside the test database, so the suite can run against a live server and
one test's leftovers cannot collide with the next one's.

## Usage

### Quick start: `explorer.sh`

Runs the indexer and the web server together in the background (logs to
`indexer.log` / `server_web.log`, pids in `.run/`):

```bash
./explorer.sh start     # start both (stops any previous run first)
./explorer.sh status    # show what's running
./explorer.sh stop      # SIGTERM both, SIGKILL after 10s
tail -f indexer.log     # follow sync progress
```

Any bare argument (or no argument) means `start`. The web UI lands on
`http://127.0.0.1:8080/` -- loopback only, so it is not reachable from another
machine unless you set `WEBHOST`. Settings come from the environment:

| Variable    | Default     | Meaning                    |
| ----------- | ----------- | -------------------------- |
| `DB`        | `dbname=explorer` | libpq DSN or URI       |
| `RPCUSER`   | `user`    | daemon RPC user            |
| `RPCPASSWORD` | `pass`      | daemon RPC password        |
| `RPCHOST`   | `127.0.0.1` | daemon RPC host            |
| `RPCPORT`   | `9554`      | daemon RPC port            |
| `EXPLORER_DSN` | `dbname=explorer` | DSN used when none is passed on the command line |
| `WEBHOST`   | `127.0.0.1` | web bind address; anything else exposes the port |
| `WEBPORT`   | `8080`      | web port                   |

```bash
RPCPORT=9554 WEBPORT=8080 ./explorer.sh start
# to serve it to the network directly, without a proxy:
WEBHOST=0.0.0.0 ./explorer.sh start
```

Running the two by hand instead:

### 1. Sync the chain

Point the indexer at the daemon (RPC port `9554` by default):

The first argument is the DSN, not a filename:

```bash
cd explorer
python3 -u indexer.py 'dbname=explorer' --rpcuser <user> --rpcpassword <pass> --host 127.0.0.1 --port 9554
```

Add `--once` to do a single catch-up pass and exit; otherwise it keeps
syncing new blocks and the mempool until interrupted. Reorgs are handled
(chain truncated at the mismatching height and re-synced).

### 2. Serve the web UI

```bash
python3 server.py 'dbname=explorer' --port 8080
# open http://127.0.0.1:8080/
# (--host ::1 for IPv6 loopback instead)
```

The web server binds **loopback only** (`127.0.0.1`) by default. This
explorer has no authentication of any kind, so the only thing standing between
it and the network is the address it binds. Pass `--host 0.0.0.0` (or `--host ::`
for IPv4 and IPv6) to expose it deliberately; the server prints a warning when
it is bound anywhere but loopback, and `explorer.sh` says the same when
`WEBHOST` is not a loopback address.

### Optional: nginx in front

`nginx-explorer.conf` proxies port 80 (IPv4 + IPv6) to the Python server on
`127.0.0.1:8080` (including an optional HTTPS block). Install it:

```bash
sudo cp nginx-explorer.conf /etc/nginx/sites-available/explorer
sudo ln -s /etc/nginx/sites-available/explorer /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

Leave `WEBHOST` unset, or at `127.0.0.1`, when nginx is in front. This matters
more than it looks: with `WEBHOST=::` the Python server still listens on every
interface, so port 8080 answers directly and a visitor can skip nginx entirely
-- getting the explorer over plain HTTP with no TLS, and any access control the
nginx block carried. Proxying a wide-open backend does not close it.

### API

```
GET /api/summary              tip height/hash, block/tx/mempool counts
GET /api/block/<height|hash>  block + its txids
GET /api/tx/<txid>            full tx (vin/vout, decoded types, addresses)
GET /api/address/<addr>       balances, outputs, related txs
GET /api/script/<hash>        script-level balances (multisig etc.)
GET /api/mempool              current mempool txids
```

All amounts are returned in integer pokes (`COIN = 1e8`) plus a `*_hex`
field formatted with 8 decimals.

`/api/address` and `/api/script` report two balances rather than one, because
a single number cannot say whether unconfirmed activity is included:

- `confirmed` — only confirmed transactions on both sides. A confirmed output
  that some mempool transaction also spends still counts as unspent here.
- `live` — every transaction the index knows, mempool included: the balance
  that could be spent right now.

The split matters most for multisig, where the script is the only place the
money appears: a multi-address output is credited to no single address.

Multisig and hybrid-multisig participant addresses are represented through
`/api/script` rather than being credited individually. An output with two or
more addresses is stored once against its script hash, and `/api/address` for
any of those participants returns 404 — not because the address is unknown to
the explorer, but because crediting each participant would mint the value
several times over. A 100 PXC `A + B` multisig output would otherwise read as
100 PXC received by A *and* 100 PXC by B, for 200 PXC total.

So a client that walks a multisig participant address gets a 404 from
`/api/address` and should look the address up in the `addresses` array of
`/api/script/<hash>` instead, where it is listed but not credited. The
`req_sigs` and `type` fields there say how much of the script is required, which
`/api/address` cannot express.

## Notes

- The explorer is read-only; it never submits anything to the daemon.
- The web server opens its schema **once** at startup (`DB.initialize`) and then
  serves requests from a small fixed pool of connections (`DB.connect`), which
  skips the schema path entirely. Opening a fresh connection per
  request made `CREATE TABLE IF NOT EXISTS` for every table run on every
  `/api/...` call -- about 0.5 ms and 89% of the per-request overhead, against
  ~0.06 ms for the connect itself.
- `ThreadingHTTPServer` runs a thread per request, so a per-thread connection
  would be no better than one per request; the pool is a `LifoQueue` of
  connections handed out one at a time. A borrow must not nest. A borrow times
  out after 5 s; the handler computes its `(status, payload)` inside the borrow
  and sends after releasing, so a request that cannot get a connection within
  the window is answered `503 {"error": "busy"}` instead of parking the thread.
- Run with `-rpcuser`/`-rpcpassword` in `phoenixcoin.conf` (or the process
  flags); the RPC server must be reachable on `127.0.0.1`.
- The indexer and web server are separate processes against one PostgreSQL
  database. They are never in the same transaction, so concurrent access needs
  no coordination beyond PostgreSQL's own.
- **There are no migrations.** The schema is never altered in place. Changing
  it means a shape change is detected on the next indexer start, the tables are
  dropped, and the chain is re-synced from genesis (a few minutes; this chain is
  ~409k blocks). What that buys is the deletion of every in-place migration path
  and with it the failure mode where one adds a column with a default and
  leaves it unbackfilled -- which reads as a correct database and answers every
  balance with the entire supply.
- The trigger is `SCHEMA_FINGERPRINT`, a hash of the `SCHEMA` tuple in `db.py`,
  so editing that DDL is all it takes; there is no version number to remember to
  bump. The fingerprint is written into `meta` by whichever process can vouch for
  the shape: the indexer (`DB.initialize(dsn, rebuild=True)`) always, and the web
  server only when it built the schema itself. The web side therefore cannot
  discard a chain, and cannot stamp a database it has not checked either -- if
  it could, the indexer arriving a second later would read a matching
  fingerprint, skip the rebuild, and serve the old chain against new code.
- Only tables named in `SCHEMA` are dropped. Anything else in the schema is left
  alone, so a rebuild cannot destroy a table that merely shares the database.
- `lock_timeout` is 5 s for ordinary work and 30 min only while schema work is
  running, then the connection drops back to 5 s. A web request must not park
  for half an hour because the indexer is rebuilding. It is a write that waits,
  and it should fail fast and retry on the next cycle.
- Schema work takes `pg_advisory_xact_lock` keyed on the current schema, so two
  processes starting at once cannot build over each other, and two schemas (as
  the tests use) do not block one another. This lock covers the whole build, not
  just parts of it: `CREATE TABLE IF NOT EXISTS` is not race-safe in
  PostgreSQL, so two sessions creating the same table at once can still collide
  in `pg_type`. That was a real cold-start failure here -- the web server lost
  and died before binding, so the browser got connection-refused until the
  second run.
- Identifier columns are declared `COLLATE "C"` so ordering is bytewise, which
  is what the SQLite `BINARY` collation did. The database's own locale does not
  change how hashes or addresses sort.
- The Phoenixcoin Quantum daemon has no `-txindex`, so
  `getrawtransaction` can only resolve confirmed txs that still have
  unspent outputs. A tx whose outputs are all spent (the genesis coinbase
  is the guaranteed one) cannot be resolved; the indexer retries it briefly
  (3 tries, 0.5 s backoff) to ride out transient failures, then stores it by
  txid alone with no inputs/outputs, logging how many it had to store that way.
