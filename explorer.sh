#!/usr/bin/env bash
# PhoenixCoin Quantum explorer — run indexer + web server together.
#
# Usage:
#   ./explorer.sh start      start both (indexer + web server)
#   ./explorer.sh stop       stop both (SIGTERM, then SIGKILL after timeout)
#   ./explorer.sh status     show running processes from the pidfiles
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB="${DB:-explorer.db}"
RPCUSER="${RPCUSER:-user}"
RPCPASSWORD="${RPCPASSWORD:-pass}"
RPCHOST="${RPCHOST:-127.0.0.1}"
RPCPORT="${RPCPORT:-9554}"
WEBHOST="${WEBHOST:-::}"
WEBPORT="${WEBPORT:-8080}"

PIDDIR="$DIR/.run"
IDX_PID="$PIDDIR/indexer.pid"
WEB_PID="$PIDDIR/web.pid"
STOP_TIMEOUT=10

mkdir -p "$PIDDIR"

alive() { kill -0 "$1" 2>/dev/null; }

read_pid() {
  if [ -s "$1" ]; then cat "$1"; else echo ""; fi
}

stop_one() {
  local pidf="$1" name="$2"
  local pid
  pid="$(read_pid "$pidf")"
  if [ -z "$pid" ]; then
    rm -f "$pidf"
    return 0
  fi
  if ! alive "$pid"; then
    echo "stopped: $name was already exited (stale pidfile removed)"
    rm -f "$pidf"
    return 0
  fi
  kill -TERM "$pid" 2>/dev/null || true
  local waited=0
  while alive "$pid" && [ "$waited" -lt "$STOP_TIMEOUT" ]; do
    sleep 0.2
    waited=$((waited + 1))
  done
  if alive "$pid"; then
    echo "stopped: $name (pid $pid) still alive after ${STOP_TIMEOUT}s, SIGKILL"
    kill -KILL "$pid" 2>/dev/null || true
    while alive "$pid"; do sleep 0.1; done
  else
    echo "stopped: $name (pid $pid) exited cleanly"
  fi
  rm -f "$pidf"
}

stop_all() {
  stop_one "$IDX_PID" "indexer"
  stop_one "$WEB_PID" "web"
}

cmdline() {
  ps -p "$1" -o args= 2>/dev/null || true
}

status_all() {
  local pair pidf name pid
  for pair in "$IDX_PID:indexer" "$WEB_PID:web"; do
    pidf="${pair%%:*}"
    name="${pair##*:}"
    pid="$(read_pid "$pidf")"
    if [ -n "$pid" ] && alive "$pid"; then
      echo "$name: running (pid $pid) $(cmdline "$pid")"
    else
      echo "$name: not running"
      [ -f "$pidf" ] && rm -f "$pidf"
    fi
  done
}

start_all() {
  stop_all
  sleep 0.5
  setsid nohup python3 -u indexer.py "$DB" \
    --rpcuser "$RPCUSER" --rpcpassword "$RPCPASSWORD" \
    --host "$RPCHOST" --port "$RPCPORT" \
    >> "$DIR/indexer.log" 2>&1 &
  echo $! > "$IDX_PID"
  echo "indexer:  pid $! -> log $DIR/indexer.log"
  setsid nohup python3 -u server.py "$DB" \
    --host "$WEBHOST" --port "$WEBPORT" \
    >> "$DIR/server_web.log" 2>&1 &
  echo $! > "$WEB_PID"
  echo "web:      pid $! -> log $DIR/server_web.log (http://${WEBHOST}:${WEBPORT}/)"
}

cmd="${1:-start}"
cd "$DIR"

case "$cmd" in
  stop) stop_all ;;
  status) status_all ;;
  *) start_all ;;
esac