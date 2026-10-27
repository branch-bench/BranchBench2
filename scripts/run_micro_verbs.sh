#!/usr/bin/env bash
# Run the branch-verb microbenchmark (python -m microbench.verbs) on every
# backend and data size, into a dated output root.
#
# Usage:
#   ./scripts/run_micro_verbs.sh [--outroot DIR] [--scales 1,2,4,8] [--reps N] [--warmup N] \
#       [--verbs a,b,...] [--axes a,b,...] [--native-only] [--parallel] [backend ...]
#
#   backend ...   subset of: dolt dolt_mysql seekdb matrixone file_copy chronos neon
#                 (default: all seven)
#   --scales      data sizes, one run each (default 1,2,4,8; the base size 1
#                 runs every point, the others only the data-size points)
#   --parallel    run the backends at the same time (one server per machine,
#                 as in run_macro_all.sh --parallel); sequential otherwise
#   --supplement DIR  measure, into an existing output root, only the points
#                 of the current grid that its stats files do not hold yet
#                 (after the grid gained values); results go to files with a
#                 dated suffix next to the originals, which the plot merges
#
# The servers must already be up (.env names their hosts); nothing is
# started or stopped here. Results: <outroot>/<backend>/micro_verbs_<backend>_w<W>.parquet
# and _stats.json, logs under <outroot>/logs/. The driver runs here; to
# co-locate Chronos's driver with its PostgreSQL, run this script on that
# machine for chronos alone.
{
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
if [[ -f "$REPO/.env" ]]; then
    while IFS= read -r line; do
        [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
        k=${line%%=*}; v=${line#*=}; v=${v%\"}; v=${v#\"}
        [[ -n "${!k:-}" ]] || export "$k=$v"
    done < "$REPO/.env"
fi

OUTROOT="run_stats/micro_verbs_$(date -u +%Y%m%d_%H%M)"
SCALES="1,2,4,8"
PARALLEL=false
SUPPLEMENT=false
EXTRA=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --outroot)     OUTROOT="$2"; shift 2 ;;
        --supplement)  OUTROOT="$2"; SUPPLEMENT=true; shift 2 ;;
        --scales)      SCALES="$2"; shift 2 ;;
        --reps|--warmup|--verbs|--axes|--storage-reps) EXTRA+=("$1" "$2"); shift 2 ;;
        --no-gc)       EXTRA+=("$1"); shift ;;
        --native-only) export DBFORK_NATIVE_ONLY=1; shift ;;
        --parallel)    PARALLEL=true; shift ;;
        -h|--help)     sed -n 2,20p "$0"; exit 0 ;;
        --*)           echo "unknown flag $1"; exit 1 ;;
        *)             break ;;
    esac
done
BACKENDS=("$@")
[[ ${#BACKENDS[@]} -gt 0 ]] || BACKENDS=(dolt dolt_mysql seekdb matrixone file_copy chronos neon)
IFS=',' read -r -a SCALE_LIST <<< "$SCALES"
mkdir -p "$OUTROOT/logs"
echo "micro_verbs: backends=${BACKENDS[*]} scales=${SCALE_LIST[*]} outroot=$OUTROOT parallel=$PARALLEL native_only=${DBFORK_NATIVE_ONLY:-0}" | tee -a "$OUTROOT/logs/_driver.log"

run_backend() {
    local b="$1"
    for w in "${SCALE_LIST[@]}"; do
        local log="$OUTROOT/logs/${b}_w${w}.log" sup=()
        if $SUPPLEMENT; then
            local stamp; stamp="_s$(date -u +%m%d%H%M)"
            sup=(--suffix "$stamp")
            for f in "$OUTROOT/$b"/micro_verbs_"${b}"_w"${w}"*_stats.json; do
                [[ -f "$f" ]] && sup+=(--skip-done "$f")
            done
            log="$OUTROOT/logs/${b}_w${w}${stamp}.log"
        fi
        echo "$(date -u +%FT%TZ) START $b W=$w${sup[0]:+ (supplement)}" | tee -a "$OUTROOT/logs/_driver.log"
        if PYTHONUNBUFFERED=1 uv run python -m microbench.verbs --backend "$b" --scale "$w" \
                --outdir "$OUTROOT/$b" ${sup[@]+"${sup[@]}"} ${EXTRA[@]+"${EXTRA[@]}"} > "$log" 2>&1; then
            echo "$(date -u +%FT%TZ) DONE  $b W=$w" | tee -a "$OUTROOT/logs/_driver.log"
        else
            echo "$(date -u +%FT%TZ) FAIL  $b W=$w (rc=$?, see $log)" | tee -a "$OUTROOT/logs/_driver.log"
        fi
    done
}

if $PARALLEL; then
    pids=()
    for b in "${BACKENDS[@]}"; do
        run_backend "$b" &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p" || true; done
else
    for b in "${BACKENDS[@]}"; do run_backend "$b"; done
fi
echo "$(date -u +%FT%TZ) ALL DONE" | tee -a "$OUTROOT/logs/_driver.log"
exit
}
