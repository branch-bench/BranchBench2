#!/usr/bin/env bash
# Run all six macrobench scenarios on every backend, into a fresh dated
# result directory, starting and stopping the local servers as needed.
#
# Usage:
#   ./scripts/run_macro_all.sh [--mini] [--max-runtime-sec N] [--outroot DIR] \
#       [--no-manage-servers] [--storage-sample-interval SEC] [--measure-storage] \
#       [--scenarios a,b,...] [--native-only] [--seed N] [--parallel] [--colocate-drivers] [backend ...]
#
#   backend ...   subset of: dolt dolt_mysql seekdb matrixone neon file_copy chronos
#                 (default: all seven; bauplan and xata are not run by default)
#   --scenarios   comma-separated subset of: rl_env context_mgmt multi_agent
#                 dev_agent ops_agent data_agent (default: all six)
#   --native-only every operation an adapter simulates runs as unsupported and
#                 ends its run (DBFORK_NATIVE_ONLY=1), to measure the engines' own primitives
#   --parallel    run the backends at the same time, each going through its
#                 scenarios in order (for one server per machine, or Neon next
#                 to local servers); implies --no-manage-servers, so every
#                 server must already be up. Neon counts as one backend and
#                 should not be run twice at once (its quotas are per project).
#
# Results land in <outroot>/<backend>/ with one log per run under
# <outroot>/logs/ and a driver log with start/end stamps. The default
# outroot is run_stats/macro_<UTC date>; if that exists already, a
# _<HHMM> suffix is added so an earlier set is never overwritten.
#
# Servers: before a local backend's runs, its server is started if its port
# is not listening, and a server this script started is stopped after that
# backend's runs (and on Ctrl-C / exit). A server that was already running
# is used as is and left running. --no-manage-servers skips all of that and
# only checks reachability. Neon is remote and needs NEON_API_KEY_ORG (from
# the environment or .env).
# Remote servers: with DBFORK_SERVER_HOST=<ip> (environment or .env) the
# servers are on another machine, or one per machine with each backend's
# own host variable (DOLT_HOST, DOLT_MYSQL_HOST, SEEKDB_HOST, MO_HOST,
# FILE_COPY_HOST, CHRONOS_HOST; PGSQL_HOST in .env must be changed too).
# Remote servers are only probed, never started or stopped.
# Co-located driver: by default every driver process runs here, so a
# client-side library such as Chronos pays the network round trip on each
# of its versioning statements (the deployment where the application is
# not on the database host). --colocate-drivers runs a backend's driver on
# the machine named by <BACKEND>_DRIVER_SSH (e.g. CHRONOS_DRIVER_SSH=
# dbf-chronos in .env) instead, next to its server, which is Chronos's
# best case and the layout its authors evaluate. That machine needs a copy
# of the repo with its own .venv and .env (scripts/remote/setup_driver.sh
# <backend>); results are rsynced back after every scenario and the log is
# streamed here, so progress.sh works unchanged. The driver log records
# which layout ran (START ... (driver on <host>) / colocate_drivers=...).
# DBFORK_SERVER_SSH=user@host (or DOLT_SSH, DOLT_MYSQL_SSH, SEEKDB_SSH,
# MO_SSH, FILE_COPY_SSH per machine) lets the storage measurements run
# du/df there (see docs/remote_servers.md). When the servers are
# started here, DBFORK_BIND_HOST (default 127.0.0.1) is the address Doltgres
# and Dolt bind to; use 0.0.0.0 to serve a driver on another machine.
# file_copy and chronos share the PostgreSQL 18 server (PGSQL_* in .env,
# cluster at $PGSQL_DATA_DIR/pgdata, run as the postgres user via sudo); it
# is started once for both and stopped with pg_ctl after the last of them.
#
# Storage: every run records the backend's storage size at four workflow
# points (after setup, after the workflow, after GC, after cleanup) and
# samples it every 5 s during the scenario (--storage-sample-interval
# changes that; 0 turns sampling off). --measure-storage additionally
# measures around every verb and exec on the local backends (two directory
# walks per operation, slow, and not attributable under concurrent workers)
# and on Neon fetches the project's consumption metrics after the run,
# which waits 15 min for them to appear. Off by default.
#
# Run from the repo root inside tmux: a full set takes hours.

# The whole script is one { ... } block: bash parses it completely before
# running it, so editing this file during a sweep cannot break the run in
# progress (bash otherwise reads scripts incrementally by byte offset).
{
set -uo pipefail
cd "$(dirname "$0")/.."
REPO="$PWD"
# Export .env so backend host/ssh settings (DBFORK_SERVER_HOST, DOLT_HOST,
# SEEKDB_SSH, ...) reach every adapter, not only the two that load it
# themselves. Variables already set in the environment win.
if [[ -f "$REPO/.env" ]]; then
    while IFS= read -r line; do
        [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
        k=${line%%=*}; v=${line#*=}; v=${v%\"}; v=${v#\"}
        [[ -n "${!k:-}" ]] || export "$k=$v"
    done < "$REPO/.env"
fi

MINI=""
MAX_RUNTIME_SEC=14400
SEED=""
OUTROOT=""
MANAGE_SERVERS=true
PARALLEL=false
COLOCATE_DRIVERS=false
STORAGE_FLAGS=(--storage-sample-interval 5)
SCENARIOS=(rl_env context_mgmt multi_agent dev_agent ops_agent data_agent)
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mini)              MINI="--mini"; shift ;;
        --max-runtime-sec)   MAX_RUNTIME_SEC="$2"; shift 2 ;;
        --outroot)           OUTROOT="$2"; shift 2 ;;
        --no-manage-servers) MANAGE_SERVERS=false; shift ;;
        --storage-sample-interval) STORAGE_FLAGS=(--storage-sample-interval "$2"); shift 2 ;;
        --measure-storage)   STORAGE_FLAGS+=(--measure-storage); shift ;;
        --scenarios)         IFS=',' read -r -a SCENARIOS <<< "$2"; shift 2 ;;
        --native-only)       STORAGE_FLAGS+=(--native-only); shift ;;
        --seed)              STORAGE_FLAGS+=(--seed "$2"); SEED="$2"; shift 2 ;;
        --parallel)          PARALLEL=true; MANAGE_SERVERS=false; shift ;;
        --colocate-drivers)  COLOCATE_DRIVERS=true; shift ;;
        --*)                 echo "unknown option $1"; exit 1 ;;
        *)                   break ;;
    esac
done

BACKENDS=("$@")
if [[ ${#BACKENDS[@]} -eq 0 ]]; then
    BACKENDS=(dolt dolt_mysql seekdb matrixone neon file_copy chronos)
fi
for sc in "${SCENARIOS[@]}"; do
    case "$sc" in rl_env|context_mgmt|multi_agent|dev_agent|ops_agent|data_agent) ;;
        *) echo "unknown scenario '$sc'"; exit 1 ;; esac
done

if [[ -z "$OUTROOT" ]]; then
    OUTROOT="run_stats/macro_$(date -u +%Y%m%d)${MINI:+_mini}"
    if [[ -e "$OUTROOT" ]]; then
        OUTROOT="${OUTROOT}_$(date -u +%H%M)"
    fi
fi
mkdir -p "$OUTROOT/logs"
DRIVER="$OUTROOT/logs/_driver.log"

stamp() { date -u +%FT%TZ; }
log()   { echo "=== $* $(stamp)" | tee -a "$DRIVER"; }

# --------------------------------------------------------------------------
# Local servers
# --------------------------------------------------------------------------

# PostgreSQL 18 server shared by file_copy and chronos (settings from .env).
envval() { local v; v=$(grep -E "^$1=" .env 2>/dev/null | tail -1 | cut -d= -f2-); echo "${v:-$2}"; }
PGSQL_HOST=$(envval PGSQL_HOST 127.0.0.1)
PGSQL_PORT=$(envval PGSQL_PORT 5434)
PGSQL_USER=$(envval PGSQL_USER postgres)
PGSQL_PASSWORD=$(envval PGSQL_PASSWORD password)
PGSQL_DATA_DIR=$(envval PGSQL_DATA_DIR /mnt/pgbench)
SERVER_HOST=${DBFORK_SERVER_HOST:-$(envval DBFORK_SERVER_HOST 127.0.0.1)}
BIND_HOST=${DBFORK_BIND_HOST:-127.0.0.1}
export DBFORK_SERVER_HOST="$SERVER_HOST"
[[ -n "${DBFORK_SERVER_SSH:-}" ]] || DBFORK_SERVER_SSH=$(envval DBFORK_SERVER_SSH "")
[[ -n "$DBFORK_SERVER_SSH" ]] && export DBFORK_SERVER_SSH
# Host of one backend's server: its own variable (DOLT_HOST, DOLT_MYSQL_HOST,
# SEEKDB_HOST, MO_HOST, FILE_COPY_HOST/CHRONOS_HOST then PGSQL_HOST), else
# DBFORK_SERVER_HOST, so each server may sit on its own machine.
host_of() {
    local v
    case "$1" in
        dolt)       v=${DOLT_HOST:-$(envval DOLT_HOST "")} ;;
        dolt_mysql) v=${DOLT_MYSQL_HOST:-$(envval DOLT_MYSQL_HOST "")} ;;
        seekdb)     v=${SEEKDB_HOST:-$(envval SEEKDB_HOST "")} ;;
        matrixone)  v=${MO_HOST:-$(envval MO_HOST "")} ;;
        file_copy)  v=${FILE_COPY_HOST:-$(envval FILE_COPY_HOST "")}; v=${v:-$PGSQL_HOST} ;;
        chronos)    v=${CHRONOS_HOST:-$(envval CHRONOS_HOST "")}; v=${v:-$PGSQL_HOST} ;;
    esac
    echo "${v:-$SERVER_HOST}"
}
is_local_host() { [[ "$1" == 127.0.0.1 || "$1" == localhost ]]; }
backend_local() { is_local_host "$(host_of "$1")"; }
PG_BIN=/usr/lib/postgresql/18/bin
PG_DATA="$PGSQL_DATA_DIR/pgdata"

# The server a backend runs on: itself, or pg18 for the two Postgres-based
# backends; neon is remote.
server_of() {
    case "$1" in
        file_copy|chronos) echo pg18 ;;
        *)                 echo "$1" ;;
    esac
}

# Port each local backend listens on; neon is remote.
port_of() {
    case "$1" in
        dolt)              echo 5432 ;;
        dolt_mysql)        echo 3306 ;;
        seekdb)            echo 2881 ;;
        matrixone)         echo 6001 ;;
        file_copy|chronos) echo "$PGSQL_PORT" ;;
        *)                 echo "" ;;
    esac
}

# Takes a backend. A remote server cannot be inspected with ss; probe()
# decides there.
listening() { backend_local "$1" || return 0; ss -ltn 2>/dev/null | grep -q ":$(port_of "$1") "; }

# A real connection + query, since a port can accept before the server is
# ready to serve (SeekDB and MatrixOne bootstrap for a while after binding).
probe() {
    case "$1" in
        dolt)
            uv run python -c "import psycopg2; c=psycopg2.connect(host='$(host_of dolt)', port=5432, user='postgres', password='password', dbname='postgres', connect_timeout=5); c.cursor().execute('SELECT 1')" ;;
        dolt_mysql)
            uv run python -c "import pymysql; pymysql.connect(host='$(host_of dolt_mysql)', port=3306, user='root', password='', connect_timeout=5).cursor().execute('SELECT 1')" ;;
        seekdb)
            uv run python -c "import pymysql; pymysql.connect(host='$(host_of seekdb)', port=2881, user='root', password='', connect_timeout=5).cursor().execute('SELECT 1')" ;;
        matrixone)
            uv run python -c "import pymysql; pymysql.connect(host='$(host_of matrixone)', port=6001, user='root', password='111', connect_timeout=5).cursor().execute('SELECT 1')" ;;
        file_copy|chronos)
            PGHOST="$(host_of "$1")" PGPORT="$PGSQL_PORT" PGUSER="$PGSQL_USER" PGPASSWORD="$PGSQL_PASSWORD" \
                uv run python -c "import psycopg2; psycopg2.connect(dbname='postgres', connect_timeout=5).cursor().execute('SELECT 1')" ;;
        *) return 1 ;;
    esac
} 2>/dev/null

# Servers this script started: "<server>=<pid>" entries (see server_of).
STARTED=()

start_server() {
    local b="$1" srv slog pid
    srv=$(server_of "$b")
    slog="$OUTROOT/logs/${srv}_server.log"
    case "$srv" in
        dolt)
            # Doltgres 1.4.0 release binary; the bind address only comes
            # from a config file.
            printf 'listener:\n  host: %s\n  port: 5432\ndata_dir: %s\n' "$BIND_HOST" "$HOME/doltgres-1.4.0/databases" \
                > "$HOME/doltgres-1.4.0/config.yaml"
            (cd ~/doltgres-1.4.0 && exec ./dl/doltgresql-linux-amd64/bin/doltgres -config config.yaml) >> "$slog" 2>&1 &
            pid=$! ;;
        dolt_mysql)
            (cd ~/dolt && exec ~/dolt/bin/dolt sql-server --host "$BIND_HOST" --port 3306 --data-dir ~/dolt/databases) >> "$slog" 2>&1 &
            pid=$! ;;
        seekdb)
            # Daemonizes itself; its own log is ~/seekdb/log/seekdb.log and
            # the pid lands in ~/seekdb/run/seekdb.pid.
            (cd ~/seekdb && ./bin/seekdb) >> "$slog" 2>&1
            sleep 2
            pid=$(cat ~/seekdb/run/seekdb.pid 2>/dev/null || echo "") ;;
        matrixone)
            # Appends to the server's own log: never truncate it, it holds
            # earlier runs' server-side history.
            (cd ~/mo/matrixone && exec ./mo-service -launch ./etc/launch/launch.toml) >> ~/mo/mo-service.log 2>&1 &
            pid=$! ;;
        pg18)
            # The cluster lives on a loop-mounted XFS image (reflink clones);
            # re-mount it after a reboot. pg_ctl daemonizes; its own log is
            # $PGSQL_DATA_DIR/pg.log and the pid is in postmaster.pid.
            if ! mountpoint -q "$PGSQL_DATA_DIR" && [[ -f "$HOME/pgbench.img" ]]; then
                log "MOUNTING $HOME/pgbench.img on $PGSQL_DATA_DIR"
                sudo -n mount -o loop "$HOME/pgbench.img" "$PGSQL_DATA_DIR" >> "$slog" 2>&1
            fi
            sudo -n -u postgres "$PG_BIN/pg_ctl" -D "$PG_DATA" -l "$PGSQL_DATA_DIR/pg.log" -w start >> "$slog" 2>&1
            pid=$(sudo -n head -1 "$PG_DATA/postmaster.pid" 2>/dev/null || echo "") ;;
        *) return 1 ;;
    esac
    if [[ -z "$pid" ]]; then
        log "FAILED to start $srv (no pid)"
        return 1
    fi
    STARTED+=("$srv=$pid")
    log "STARTED $srv server pid=$pid (log: $slog)"
    local i
    for i in $(seq 1 120); do
        if listening "$b" && probe "$b"; then
            log "READY $srv after ~$((i * 2))s"
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            log "FAILED $srv server exited during startup (see $slog)"
            return 1
        fi
        sleep 2
    done
    log "FAILED $srv server not ready after 240s"
    return 1
}

# Takes a server name (see server_of) and the pid recorded at start.
stop_server() {
    local srv="$1" pid="$2" port
    case "$srv" in
        dolt)       port=5432 ;;
        dolt_mysql) port=3306 ;;
        seekdb)     port=2881 ;;
        matrixone)  port=6001 ;;
        pg18)       port=$PGSQL_PORT ;;
    esac
    log "STOPPING $srv server pid=$pid"
    if [[ "$srv" == pg18 ]]; then
        # A fast shutdown (TERM alone is a smart shutdown that waits for
        # every session, and KILL would force crash recovery next time).
        sudo -n -u postgres "$PG_BIN/pg_ctl" -D "$PG_DATA" -m fast -t 60 -w stop >> "$OUTROOT/logs/pg18_server.log" 2>&1 \
            && log "STOPPED $srv" || log "FAILED to stop $srv (see logs/pg18_server.log)"
        return 0
    fi
    kill -TERM "$pid" 2>/dev/null
    local i
    for i in $(seq 1 60); do
        if ! kill -0 "$pid" 2>/dev/null && ! ss -ltn 2>/dev/null | grep -q ":${port} "; then
            log "STOPPED $srv"
            return 0
        fi
        sleep 1
    done
    log "KILLING $srv (did not stop in 60s)"
    kill -KILL "$pid" 2>/dev/null
}

stop_started() {
    local entry
    for entry in "${STARTED[@]+"${STARTED[@]}"}"; do
        stop_server "${entry%%=*}" "${entry##*=}"
    done
    STARTED=()
}
trap stop_started EXIT

# Ensure the backend is usable; starts its server when allowed.
ensure_backend() {
    local b="$1"
    if [[ "$b" == neon ]]; then
        [[ -n "${NEON_API_KEY_ORG:-$(envval NEON_API_KEY_ORG)}" ]] && return 0
        log "SKIP neon: NEON_API_KEY_ORG is not set"
        return 1
    fi
    if listening "$b"; then
        if probe "$b"; then
            log "USING running $(server_of "$b") server on $(host_of "$b"):$(port_of "$b") (left running at the end)"
            return 0
        fi
        log "SKIP $b: $(host_of "$b"):$(port_of "$b") is open but a query failed"
        return 1
    fi
    if ! backend_local "$b"; then
        log "SKIP $b: $(host_of "$b"):$(port_of "$b") did not answer (remote servers are not started from here)"
        return 1
    fi
    if ! $MANAGE_SERVERS; then
        log "SKIP $b: not listening on :$(port_of "$b") and --no-manage-servers given"
        return 1
    fi
    start_server "$b"
}

# --------------------------------------------------------------------------
# Runs
# --------------------------------------------------------------------------

# ssh target of the machine that runs a backend's driver process, if not here.
driver_of() {
    $COLOCATE_DRIVERS || return 0
    local v; v="$(echo "$1" | tr '[:lower:]' '[:upper:]')_DRIVER_SSH"
    echo "${!v:-$(envval "$v" "")}"
}
REMOTE_REPO="${DBFORK_REMOTE_REPO:-db-fork}"   # repo path on a driver machine, relative to its $HOME

run_scenarios() {
    local b="$1" s rc drv
    drv="$(driver_of "$b")"
    for s in "${SCENARIOS[@]}"; do
        if [[ -n "$drv" ]]; then
            log "START $s $b (driver on $drv)"
            mkdir -p "$OUTROOT/$b"
            ssh -o BatchMode=yes -o ServerAliveInterval=60 -o ServerAliveCountMax=10 "$drv" \
                "export PATH=\$HOME/.local/bin:\$PATH; cd '$REMOTE_REPO' && mkdir -p '$OUTROOT/$b' && ./scripts/run_macrobench.sh $MINI --outdir '$OUTROOT/$b' \
                 --max-runtime-sec '$MAX_RUNTIME_SEC' ${STORAGE_FLAGS[*]} '$s' '$b'" > "$OUTROOT/logs/${s}_${b}.log" 2>&1
            rc=$?
            rsync -az "$drv:$REMOTE_REPO/$OUTROOT/$b/" "$OUTROOT/$b/" >> "$OUTROOT/logs/${s}_${b}.log" 2>&1 \
                || log "WARN $s $b: rsync of the results from $drv failed"
        else
            log "START $s $b"
            ./scripts/run_macrobench.sh $MINI --outdir "$OUTROOT/$b" --max-runtime-sec "$MAX_RUNTIME_SEC" \
                "${STORAGE_FLAGS[@]}" "$s" "$b" > "$OUTROOT/logs/${s}_${b}.log" 2>&1
            rc=$?
        fi
        log "END $s $b rc=$rc"
    done
}

log "RUN SET outroot=$OUTROOT backends=${BACKENDS[*]} mini=${MINI:-no} cap=${MAX_RUNTIME_SEC}s seed=${SEED:-config} manage_servers=$MANAGE_SERVERS parallel=$PARALLEL colocate_drivers=$COLOCATE_DRIVERS"
if $PARALLEL; then
    PIDS=()
    for b in "${BACKENDS[@]}"; do
        ensure_backend "$b" || continue
        run_scenarios "$b" &
        PIDS+=($!)
        log "LAUNCHED $b pid=$!"
    done
    wait "${PIDS[@]+"${PIDS[@]}"}"
    log "ALL DONE"
    exit 0
fi
for idx in "${!BACKENDS[@]}"; do
    b="${BACKENDS[$idx]}"
    ensure_backend "$b" || continue
    run_scenarios "$b"
    # Stop the server this script started for this backend, unless a later
    # backend in the list runs on the same server (file_copy and chronos).
    srv=$(server_of "$b")
    still_needed=false
    for later in "${BACKENDS[@]:idx+1}"; do
        [[ "$(server_of "$later")" == "$srv" ]] && still_needed=true
    done
    if ! $still_needed; then
        for i in "${!STARTED[@]}"; do
            if [[ "${STARTED[$i]%%=*}" == "$srv" ]]; then
                stop_server "$srv" "${STARTED[$i]##*=}"
                unset 'STARTED[i]'
            fi
        done
        STARTED=("${STARTED[@]+"${STARTED[@]}"}")
    fi
done
log "ALL DONE"

exit
}
