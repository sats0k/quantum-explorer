# PhoenixCoin Quantum Block Explorer

A minimal block explorer for the PhoenixCoin Quantum network, written in
pure Python (stdlib only) with SQLite storage.

Hybrid transactions are supported **for free**: the indexer stores the
daemon's own verbose `getrawtransaction` output, which already decodes the
hybrid script templates (`hybrid_pubkey`, `hybrid_pubkeyhash`,
`hybrid_multisig`) and hybrid address prefixes (`0x3A` / `0x6A`). No
script parsing is reimplemented on the explorer side.

## Layout

| File              | Purpose                                        |
| ----------------- | ---------------------------------------------- |
| `indexer.py`      | Syncs chain + mempool from the daemon into SQLite |
| `db.py`           | SQLite schema (`blocks`, `txs`, `vin`, `vout`, `addr_out`) |
| `rpc.py`          | JSON-RPC client (basic auth)                   |
| `server.py`       | Read-only JSON API + static web UI             |
| `explorer.sh`     | Start/stop/status wrapper for indexer + web UI |
| `web/index.html`  | Single-page explorer frontend                  |
| `test_db.py`      | Regression tests for indexing + accounting     |

## Tests

```
python3 -m unittest test_db -v
```

No dependencies beyond the standard library, and no daemon needed: the tests
build a temp database and drive `DB` directly.

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
`http://[::1]:8080/`. Settings come from the environment:

| Variable    | Default     | Meaning                    |
| ----------- | ----------- | -------------------------- |
| `DB`        | `explorer.db` | SQLite file               |
| `RPCUSER`   | `user`    | daemon RPC user            |
| `RPCPASSWORD` | `pass`      | daemon RPC password        |
| `RPCHOST`   | `127.0.0.1` | daemon RPC host            |
| `RPCPORT`   | `9554`      | daemon RPC port            |
| `WEBHOST`   | `::`        | web bind address           |
| `WEBPORT`   | `8080`      | web port                   |

```bash
RPCPORT=9554 WEBPORT=8080 ./explorer.sh start
```

Running the two by hand instead:

### 1. Sync the chain

Point the indexer at the daemon (RPC port `9554` by default):

```bash
cd explorer
python3 -u indexer.py explorer.db --rpcuser <user> --rpcpassword <pass> --host 127.0.0.1 --port 9554
```

Add `--once` to do a single catch-up pass and exit; otherwise it keeps
syncing new blocks and the mempool until interrupted. Reorgs are handled
(chain truncated at the mismatching height and re-synced).

### 2. Serve the web UI

```bash
python3 server.py explorer.db --port 8080
# open http://127.0.0.1:8080/  (or http://[::1]:8080/ over IPv6)
```

The web server binds dual-stack to `::` by default (IPv4 + IPv6). Pass
`--host 127.0.0.1` for IPv4-only, or `--host 0.0.0.0` to expose it on the
local network.

### Optional: nginx in front

`nginx-explorer.conf` proxies port 80 (IPv4 + IPv6) to the Python server on
`127.0.0.1:8080` (including an optional HTTPS block). Install it:

```bash
sudo cp nginx-explorer.conf /etc/nginx/sites-available/explorer
sudo ln -s /etc/nginx/sites-available/explorer /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

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
  skips the schema/migration path entirely. Opening a fresh connection per
  request made `CREATE TABLE IF NOT EXISTS` for every table run on every
  `/api/...` call -- about 0.5 ms and 89% of the per-request overhead, against
  ~0.06 ms for the connect itself.
- `ThreadingHTTPServer` runs a thread per request, so a per-thread connection
  would be no better than one per request; the pool is a `LifoQueue` of
  connections handed out one at a time. A borrow must not nest.
- Run with `-rpcuser`/`-rpcpassword` in `phoenixcoin.conf` (or the process
  flags); the RPC server must be reachable on `127.0.0.1`.
- `db.py` sets WAL mode; the indexer and web server may run concurrently
  against the same file.
- `busy_timeout` is 5 s for ordinary work and 30 min only while a migration is
  running, then the connection drops back to 5 s. A web request must not park
  for half an hour because the indexer is rewriting the schema. In WAL mode
  reads never block on the indexer's writes anyway; it is a write that waits,
  and it should fail fast and retry on the next cycle.
- The Phoenixcoin Quantum daemon has no `-txindex`, so
  `getrawtransaction` can only resolve confirmed txs that still have
  unspent outputs. A tx whose outputs are all spent (the genesis coinbase
  is the guaranteed one) is indexed by txid alone, with no inputs/outputs;
  the indexer logs how many it had to store that way.
