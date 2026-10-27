#!/usr/bin/env bash
# Run every macrobench scenario with storage measurement on the given backends.
#
# Results go to run_stats/<backend>_storage/.
#
# Usage:
#   ./scripts/run_storage_all.sh [--mini] [--scale N] [--backends "dolt neon"]
#
# Defaults: full configs, the config's scale factor, backends "dolt neon".

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

MINI_FLAG=()
SCALE=""
BACKENDS=(dolt neon)

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mini)     MINI_FLAG=(--mini); shift ;;
        --scale)    SCALE="$2"; shift 2 ;;
        --backends) read -r -a BACKENDS <<< "$2"; shift 2 ;;
        *)          echo "Unknown flag: $1"; exit 1 ;;
    esac
done

SCENARIOS=(rl_env context_mgmt multi_agent dev_agent ops_agent data_agent)

TOTAL=$(( ${#SCENARIOS[@]} * ${#BACKENDS[@]} ))
COUNT=0

for BACKEND in "${BACKENDS[@]}"; do
    OUTDIR="run_stats/${BACKEND}_storage"
    mkdir -p "$OUTDIR"

    for SCENARIO in "${SCENARIOS[@]}"; do
        COUNT=$((COUNT + 1))
        echo ""
        echo "========================================"
        echo "  [$COUNT/$TOTAL] $SCENARIO / $BACKEND"
        echo "========================================"

        "$SCRIPT_DIR/run_macrobench.sh" \
            ${MINI_FLAG[@]+"${MINI_FLAG[@]}"} \
            --measure-storage \
            --outdir "$OUTDIR" \
            "$SCENARIO" "$BACKEND" ${SCALE:+"$SCALE"}

        echo "  Done: $SCENARIO / $BACKEND"
    done
done

echo ""
echo "All storage benchmarks complete; results under run_stats/<backend>_storage/."
