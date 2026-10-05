# PhoenixCoin Quantum Block Explorer

A minimal block explorer for the PhoenixCoin Quantum network, written in
pure Python (stdlib only) with SQLite storage.

Hybrid transactions are supported **for free**: the indexer stores the
daemon's own decoded `getblock` output, which already decodes the
hybrid script templates (`hybrid_pubkey`, `hybrid_pubkeyhash`,
`hybrid_multisig`) and hybrid address prefixes (`0x3A` / `0x6A`). No
script parsing is reimplemented on the explorer side.

## Layout

| File              | Purpose                                        |
| ----------------- | ---------------------------------------------- |
| `indexer.py`      | Syncs chain + mempool from the daemon into SQLite |
| `db.py`           | SQLite schema (`blocks`, `txs`, `vin`, `vout`, `addr_out`, `scripts`, `meta`) |
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
`http://127.0.0.1:8080/` -- loopback only, so it is not reachable from another
machine unless you set `WEBHOST`. Settings come from the environment:

| Variable    | Default     | Meaning                    |
| ----------- | ----------- | -------------------------- |
| `DB`        | `explorer.db` | SQLite file               |
| `RPCUSER`   | `user`    | daemon RPC user            |
| `RPCPASSWORD` | `pass`      | daemon RPC password        |
| `RPCHOST`   | `127.0.0.1` | daemon RPC host            |
| `RPCPORT`   | `9554`      | daemon RPC port            |
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
# open http://127.0.0.1:8080/
# (--host ::1 for IPv6 loopback instead)
```

The web server binds **loopback only** (`127.0.0.1`) by default. This
explorer has no authentication of any kind, so the only thing standing between
it and the network is the address it binds. Pass `--host 0.0.0.0` (or `--host
::` for IPv4 and IPv6) to expose it deliberately; the server prints a warning
when it is bound anywhere but loopback, and `explorer.sh` says the same when
`WEBHOST` is not a loopback address.

Leave `WEBHOST` unset, or at `127.0.0.1`, when nginx is in front. This matters
more than it looks: with `WEBHOST=::` the Python server still listens on every
interface, so port 8080 answers directly and a visitor can skip nginx entirely
-- getting the explorer over plain HTTP with no TLS, and any access control the
nginx block carried. Proxying a wide-open backend does not close it.

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

`/api/address` lists at most 2,000 outputs and 2,000 transactions, because the
caller picks the address and a high-activity one would otherwise make the
response cost whatever its owner made it cost. `n_outputs` and `n_txs` are the
true totals, not the lengths of the lists beside them, so `outputs_truncated`
and `txs_truncated` say which of the two you are looking at. The transactions
are the first 2,000 in bytewise txid order, counting both directions: the ones
that paid the address and the ones that spent what it was paid. There is no page
parameter, so a client that needs more than the window has to narrow the
address rather than ask twice.

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
  connections handed out one at a time. A borrow must not nest. A borrow times
  out after 5 s; the handler computes its `(status, payload)` inside the borrow
  and sends after releasing, so a request that cannot get a connection within
  the window is answered `503 {"error": "busy"}` instead of parking the thread.
- What a tx must read before it can write -- which prevouts a previous version
  of that txid held, what status it was stored at, and for a coinbase what it
  already counted towards the supply -- costs two statements per transaction.
  Those reads decide `released`, `prev_minted` and the `n_txs` delta, so they
  cannot be skipped or assumed; they can only be asked for a whole window at
  once, which `DB.prefetch_txs` does, in three statements per window instead of
  two per tx (measured on 200 blocks of this chain: 8,900 -> 8,050 statements).
  On a chain indexed from genesis the answer is "nothing held" every time, which
  is exactly the case a per-tx query pays full price for. Each answer is
  consumed by popping it, so a txid added twice in one window finds its entry
  gone and reads the live table instead.
  `clear_from` and `_remove_txs` drop the cache rather than reasoning about what
  it described, since a stale answer to "what did this txid hold" would
  resurrect a deleted version's inputs and its share of the supply. Those are
  the only two places that drop it: `_refresh_spent_flags` also deletes rows,
  but it runs after *every* `add_tx`, so invalidating there would discard the
  cache before it had answered anything.
- Run with `-rpcuser`/`-rpcpassword` in `phoenixcoin.conf` (or the process
  flags); the RPC server must be reachable on `127.0.0.1`.
- `db.py` sets WAL mode; the indexer and web server may run concurrently
  against the same file.
- `busy_timeout` is 5 s for ordinary work and 30 min only while a migration is
  running, then the connection drops back to 5 s. A web request must not park
  for half an hour because the indexer is rewriting the schema. In WAL mode
  reads never block on the indexer's writes anyway; it is a write that waits,
  and it should fail fast and retry on the next cycle.
- The Phoenixcoin Quantum daemon has no `-txindex`, so `getrawtransaction`
  can only resolve confirmed txs that still have unspent outputs: a tx whose
  outputs are all spent (the genesis coinbase is the guaranteed one) cannot be
  resolved by txid at all.
- Chain transactions are therefore taken from the block reply rather than
  looked up one by one: the indexer asks for `getblock <hash> 2`, so a window
  of 100 blocks costs one RPC call per block instead of one per block plus one
  per transaction, and nothing can fail to resolve because nothing is being
  resolved by txid. That argument is a node-side change, so a daemon without it
  is detected once (rejected argument, or a reply still carrying txids) and
  remembered for the rest of the run.
- Only the fallback path is affected by the missing txindex. It retries a
  refused lookup briefly (3 tries, 0.5 s backoff) to ride out transient
  failures, then stores the tx by txid alone with no inputs/outputs, logging
  how many it had to store that way.
