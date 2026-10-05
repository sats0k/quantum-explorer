#!/usr/bin/env bash
# PhoenixCoin Quantum explorer — run indexer + web server together.
#
# Usage:
#   ./explorer.sh start      start both (indexer + web server)
#   ./explorer.sh stop       stop both (SIGTERM, then SIGKILL after timeout)
#   ./explorer.sh status     show running processes from the pidfiles
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# A libpq connection string or URI, passed straight to psycopg. This used to be
# a SQLite filename; the explorer now keeps its index in PostgreSQL and the
# argument is the DSN rather than a path. Override with DB= if your database
# needs credentials, a socket directory or a non-default port:
#   DB='postgresql://explorer@/explorer?host=/run/postgresql' ./explorer.sh start
# user= is not optional here: without it libpq connects as the OS account, and
# since PostgreSQL 15 that account no longer owns `public` in a database
# belonging to someone else, so CREATE TABLE is refused with InsufficientPrivilege
# on schema public. The named user must own the database.
DB="${DB:-dbname=explorer user=user}"
RPCUSER="${RPCUSER:-user}"
RPCPASSWORD="${RPCPASSWORD:-pass}"
RPCHOST="${RPCHOST:-127.0.0.1}"
RPCPORT="${RPCPORT:-9554}"
# Loopback, not every interface. The nginx setup in README.md proxies to
# 127.0.0.1:8080, so the Python server is only ever meant to be reached from
# this machine; binding "::" would also open 8080 to the network directly, which
# serves the whole explorer over plain HTTP and steps around nginx and its TLS.
# Set WEBHOST=:: (or =0.0.0.0) to expose it without a proxy on purpose.
WEBHOST="${WEBHOST:-127.0.0.1}"
WEBPORT="${WEBPORT:-8080}"

PIDDIR="$DIR/.run"
IDX_PID="$PIDDIR/indexer.pid"
WEB_PID="$PIDDIR/web.pid"
STOP_TIMEOUT=10
# Launched below, and matched against /proc/<pid>/cmdline when stopping, so the
# two cannot drift apart.
IDX_SCRIPT="indexer.py"
WEB_SCRIPT="server.py"

mkdir -p "$PIDDIR"

alive() { kill -0 "$1" 2>/dev/null; }

# True when $1 is a live process whose command line mentions $2. A pidfile
# holds a bare pid and pids get recycled, so a stale one can name a stranger and
# kill -TERM would reach an unrelated process. Refusing to signal what we cannot
# identify is the whole point; the cost is that a stop needs a manual kill where
# the check cannot be made at all (no /proc), which is loud rather than silent.
# $2 must be a substring of what start_all actually launches.
is_ours() {
  local pid="$1" marker="$2" cmd
  alive "$pid" || return 1
  [ -r "/proc/$pid/cmdline" ] || return 1
  cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)" || return 1
  case "$cmd" in *"$marker"*) return 0 ;; *) return 1 ;; esac
}

read_pid() {
  if [ -s "$1" ]; then cat "$1"; else echo ""; fi
}

stop_one() {
  local pidf="$1" name="$2" marker="$3"
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
  if ! is_ours "$pid" "$marker"; then
    echo "refused: $name pidfile holds pid $pid, which is not $marker -- left alone"
    echo "         running: $(cmdline "$pid")"
    echo "         if that is not ours, remove $pidf; if it is, kill $pid by hand"
    rm -f "$pidf"
    return 0
  fi
  kill -TERM "$pid" 2>/dev/null || true
  # SECONDS is a clock, not a loop count. Counting sleep 0.2 iterations against
  # a seconds timeout gave 2s under a name that claimed 10, and the message
  # repeated the nominal figure rather than the elapsed one.
  local started=$SECONDS deadline=$((SECONDS + STOP_TIMEOUT))
  while alive "$pid" && [ "$SECONDS" -lt "$deadline" ]; do
    sleep 0.2
  done
  if alive "$pid"; then
    if is_ours "$pid" "$marker"; then
      echo "stopped: $name (pid $pid) still alive after $((SECONDS - started))s, SIGKILL"
      kill -KILL "$pid" 2>/dev/null || true
      while alive "$pid"; do sleep 0.1; done
    else
      # It exited and something else took the pid while we were waiting. The
      # post-KILL wait would spin on that stranger forever, so stop here.
      echo "refused: $name pid $pid was recycled while stopping -- left alive"
    fi
  else
    echo "stopped: $name (pid $pid) exited cleanly"
  fi
  rm -f "$pidf"
}

stop_all() {
  stop_one "$IDX_PID" "indexer" "$IDX_SCRIPT"
  stop_one "$WEB_PID" "web" "$WEB_SCRIPT"
}

cmdline() {
  ps -p "$1" -o args= 2>/dev/null || true
}

status_one() {
  local pidf="$1" name="$2" marker="$3" pid
  pid="$(read_pid "$pidf")"
  if [ -n "$pid" ] && alive "$pid"; then
    if is_ours "$pid" "$marker"; then
      echo "$name: running (pid $pid) $(cmdline "$pid")"
    else
      echo "$name: pidfile holds pid $pid, which is not $marker"
      echo "         running: $(cmdline "$pid")"
    fi
  else
    echo "$name: not running"
    # An if, not `test && rm`: as the last command in the branch that form
    # returns 1 when there is no stale pidfile to remove, and set -e turns that
    # into status exiting 1 on a system where nothing is running.
    if [ -f "$pidf" ]; then rm -f "$pidf"; fi
  fi
}

status_all() {
  status_one "$IDX_PID" "indexer" "$IDX_SCRIPT"
  status_one "$WEB_PID" "web" "$WEB_SCRIPT"
}

start_all() {
  stop_all
  sleep 0.5
  setsid nohup python3 -u "$IDX_SCRIPT" "$DB" \
    --rpcuser "$RPCUSER" --rpcpassword "$RPCPASSWORD" \
    --host "$RPCHOST" --port "$RPCPORT" \
    >> "$DIR/indexer.log" 2>&1 &
  echo $! > "$IDX_PID"
  echo "indexer:  pid $! -> log $DIR/indexer.log"
  setsid nohup python3 -u "$WEB_SCRIPT" "$DB" \
    --host "$WEBHOST" --port "$WEBPORT" \
    >> "$DIR/server_web.log" 2>&1 &
  echo $! > "$WEB_PID"
  echo "web:      pid $! -> log $DIR/server_web.log ($(web_url))"
  case "$WEBHOST" in
    # Anything but a loopback literal is on the network, and worth saying so
    # here as well as in the server's own log: this is the line an operator
    # reads when deciding whether the explorer is public.
    127.*|::1|localhost) ;;
    *) echo "warning: WEBHOST=$WEBHOST is not loopback, so port $WEBPORT is open to the network" ;;
  esac
}

# An IPv6 literal has to be bracketed to sit in a URL, or the port cannot be
# told from the address. Only called after WEBHOST/WEBPORT are set.
web_url() {
  case "$WEBHOST" in
    *:*) echo "http://[$WEBHOST]:$WEBPORT/" ;;
    *)   echo "http://$WEBHOST:$WEBPORT/" ;;
  esac
}

cmd="${1:-start}"
cd "$DIR"

case "$cmd" in
  stop) stop_all ;;
  status) status_all ;;
  *) start_all ;;
esac