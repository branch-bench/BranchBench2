#!/usr/bin/env bash
# Run one macrobenchmark scenario on a backend.
#
# Usage:
#   ./scripts/run_macrobench.sh [--mini] [--outdir DIR] [--max-runtime-sec N] [--op-stall-sec N] \
#       [--measure-storage] [--storage-sample-interval SEC] [--branch-intensity X] [--data-intensity Y] \
#       [--native-only] [--seed N] \
#       <scenario> <backend> [scale_factor]
#
# Arguments:
#   scenario      rl_env | context_mgmt | multi_agent | dev_agent | ops_agent | data_agent
#   backend       dolt | dolt_mysql | seekdb | matrixone | bauplan | neon | xata | file_copy | chronos
#   scale_factor  W warehouses for the generated CH-benCHmark data (default: the config's)
#
# The base config is macrobench/configs/<scenario>[_mini].textproto; run_id,
# backend, scale_factor and seed are patched into a temporary copy. --seed N
# sets every random choice of the run (data generation, the scenario's and
# its workers' streams, the spine load; the config's seed, or 42, otherwise)
# and appends _s<N> to the run id.
#
# Examples:
#   ./scripts/run_macrobench.sh --mini rl_env dolt
#   ./scripts/run_macrobench.sh --outdir run_stats --max-runtime-sec 7200 data_agent dolt 5
#
# Without --max-runtime-sec the run is capped at one hour times the largest
# of the scale factor and the intensity multipliers (3600 s at scale 1,
# intensity 1); a run that reaches the cap is stopped and recorded as
# interrupted, with its rows and stats written. --max-runtime-sec 0 lifts it.
# Independently, one operation (a verb or an exec) running for more than
# --op-stall-sec (default 600) is cancelled: it is recorded FAILED with a
# stall note and the run goes on (the e2e stats list the stalls).

# The whole script is one { ... } block: bash parses it completely before
# running it, so editing this file during a sweep cannot break the run in
# progress (bash otherwise reads scripts incrementally by byte offset).
{
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
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

MINI=false
MEASURE_STORAGE=false
NATIVE_ONLY=false
OUTDIR="run_stats/"
BRANCH_INTENSITY=""
DATA_INTENSITY=""
SEED=""
MAX_RUNTIME_SEC=""
DEFAULT_CAP_SEC=3600
STORAGE_SAMPLE_INTERVAL=""
OP_STALL_SEC=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mini)            MINI=true; shift ;;
        --measure-storage) MEASURE_STORAGE=true; shift ;;
        --native-only)     NATIVE_ONLY=true; shift ;;
        --outdir)          OUTDIR="$2"; shift 2 ;;
        --branch-intensity) BRANCH_INTENSITY="$2"; shift 2 ;;
        --data-intensity)   DATA_INTENSITY="$2"; shift 2 ;;
        --seed)            SEED="$2"; shift 2 ;;
        --max-runtime-sec) MAX_RUNTIME_SEC="$2"; shift 2 ;;
        --op-stall-sec)    OP_STALL_SEC="$2"; shift 2 ;;
        --storage-sample-interval) STORAGE_SAMPLE_INTERVAL="$2"; shift 2 ;;
        *)                 break ;;
    esac
done

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Usage: $0 [--mini] [--outdir DIR] [--max-runtime-sec N] [--op-stall-sec N] [--measure-storage] [--native-only] [--storage-sample-interval SEC] [--branch-intensity X] [--data-intensity Y] [--seed N] <scenario> <backend> [scale_factor]"
    echo "  scenario:  rl_env | context_mgmt | multi_agent | dev_agent | ops_agent | data_agent"
    echo "  backend:   dolt | dolt_mysql | seekdb | matrixone | bauplan | neon | xata | file_copy | chronos"
    exit 1
fi

SCENARIO="$1"
BACKEND="$2"
SCALE="${3:-}"

VALID_SCENARIOS="rl_env context_mgmt multi_agent dev_agent ops_agent data_agent"
if ! echo "$VALID_SCENARIOS" | grep -qw "$SCENARIO"; then
    echo "Error: invalid scenario '$SCENARIO' (one of: $VALID_SCENARIOS)"
    exit 1
fi
VALID_BACKENDS="dolt dolt_mysql seekdb matrixone bauplan neon xata file_copy chronos"
if ! echo "$VALID_BACKENDS" | grep -qw "$BACKEND"; then
    echo "Error: invalid backend '$BACKEND' (one of: $VALID_BACKENDS)"
    exit 1
fi

BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')
if $MINI; then
    SUFFIX="_mini"
else
    SUFFIX=""
fi
RUN_ID="macro_${SCENARIO}${SUFFIX}_${BACKEND}${SCALE:+_w$SCALE}${BRANCH_INTENSITY:+_b$BRANCH_INTENSITY}${DATA_INTENSITY:+_d$DATA_INTENSITY}${SEED:+_s$SEED}"
BASE_CONFIG="macrobench/configs/${SCENARIO}${SUFFIX}.textproto"
if [[ ! -f "$BASE_CONFIG" ]]; then
    echo "Error: base config not found: $BASE_CONFIG"
    exit 1
fi

TMP_CONFIG=$(mktemp /tmp/macrobench_$$_XXXXXX)
trap 'rm -f "$TMP_CONFIG"' EXIT

SED_ARGS=(-e "s|^run_id:.*|run_id: \"${RUN_ID}\"|" -e "s|^backend:.*|backend: ${BACKEND_UPPER}|")
if [[ -n "$SCALE" ]]; then
    SED_ARGS+=(-e "s|scale_factor: [0-9]*|scale_factor: ${SCALE}|")
fi
sed "${SED_ARGS[@]}" "$BASE_CONFIG" > "$TMP_CONFIG"
# The seed is a top-level field: drop the config's own, if any, and append ours.
if [[ -n "$SEED" ]]; then
    sed -i -e '/^seed:/d' "$TMP_CONFIG"
    printf 'seed: %s\n' "$SEED" >> "$TMP_CONFIG"
fi
# Intensity multipliers are inserted as the first lines of the workload block.
if [[ -n "$BRANCH_INTENSITY" || -n "$DATA_INTENSITY" ]]; then
    INTENSITY_LINES=""
    [[ -n "$BRANCH_INTENSITY" ]] && INTENSITY_LINES+="  branch_intensity: ${BRANCH_INTENSITY}\n"
    [[ -n "$DATA_INTENSITY" ]] && INTENSITY_LINES+="  data_intensity: ${DATA_INTENSITY}\n"
    sed -i -e "s|^workload {|workload {\n${INTENSITY_LINES%\\n}|" "$TMP_CONFIG"
fi

if [[ -z "$MAX_RUNTIME_SEC" ]]; then
    # One hour at scale 1 / intensity 1, scaled by the largest multiplier.
    MAX_RUNTIME_SEC=$(awk -v cap="$DEFAULT_CAP_SEC" -v s="${SCALE:-1}" -v b="${BRANCH_INTENSITY:-1}" -v d="${DATA_INTENSITY:-1}" \
        'BEGIN { m = s; if (b > m) m = b; if (d > m) m = d; if (m < 1) m = 1; printf "%d", cap * m }')
    CAP_NOTE=" (default: ${DEFAULT_CAP_SEC}s x max(scale, intensity))"
else
    CAP_NOTE=""
fi

echo "=== Macrobench Run ==="
echo "  Scenario:  $SCENARIO${SUFFIX}"
echo "  Backend:   $BACKEND_UPPER"
echo "  Scale:     ${SCALE:-config default}"
echo "  Intensity: branch x${BRANCH_INTENSITY:-1} data x${DATA_INTENSITY:-1}"
echo "  Run ID:    $RUN_ID"
echo "  Output:    $OUTDIR"
echo "  Timeout:   ${MAX_RUNTIME_SEC}s (0 = no limit)${CAP_NOTE}"
echo "  Config:    $TMP_CONFIG (patched from $BASE_CONFIG)"
echo "======================"

EXTRA_FLAGS=()
if $MEASURE_STORAGE; then
    EXTRA_FLAGS+=(--measure-storage)
fi
if [[ -n "$STORAGE_SAMPLE_INTERVAL" ]]; then
    EXTRA_FLAGS+=(--storage-sample-interval "$STORAGE_SAMPLE_INTERVAL")
fi
if [[ -n "$OP_STALL_SEC" ]]; then
    EXTRA_FLAGS+=(--op-stall-sec "$OP_STALL_SEC")
fi

if $NATIVE_ONLY; then
    export DBFORK_NATIVE_ONLY=1
    echo "Native-only mode: simulated operations run as unsupported (the run ends at the first one)"
fi
PYTHONUNBUFFERED=1 uv run python -m macrobench.runner \
    --config "$TMP_CONFIG" \
    --outdir "$OUTDIR" \
    --max-runtime-sec "$MAX_RUNTIME_SEC" \
    ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

exit
}
