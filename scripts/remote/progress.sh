#!/usr/bin/env bash
# Progress of a run set: scripts/remote/progress.sh [outroot]
# One line per backend: finished scenarios (with pass/fail counts), the
# running scenario's progress bar, and anything that ended with an error.
# Default outroot: the newest run_stats/macro_* directory.
# Repeat every 30 s with:  watch -n 30 scripts/remote/progress.sh
set -uo pipefail
cd "$(dirname "$0")/../.."
OUT=${1:-$(ls -d run_stats/macro_* 2>/dev/null | sort | tail -1)}
[[ -d "$OUT/logs" ]] || { echo "no logs under $OUT"; exit 1; }
D="$OUT/logs/_driver.log"
echo "run set: $OUT   started $(head -1 "$D" | grep -oE '[0-9]{4}-[0-9-]+T[0-9:]+Z')   now $(date -u +%FT%TZ)"
grep -q "ALL DONE" "$D" && echo "ALL DONE $(grep 'ALL DONE' "$D" | grep -oE '[0-9]{4}-[0-9-]+T[0-9:]+Z')"
for b in $(grep -oE "START [a-z_]+ [a-z_]+" "$D" | awk '{print $3}' | sort -u); do
    done_list=""
    for s in rl_env context_mgmt multi_agent dev_agent ops_agent data_agent; do
        L="$OUT/logs/${s}_${b}.log"; [[ -f "$L" ]] || continue
        # The last START/END line for this cell decides whether it is
        # finished (a rerun after a failure shows as running, not failed).
        last=$(grep -E "(START|END) $s $b( |$)" "$D" | tail -1)
        if [[ "$last" == *" END $s $b rc="* ]]; then
            rc=$(grep -oE 'rc=[0-9]+' <<< "$last" | cut -d= -f2)
            inv=$(grep -oE "Invariants: [0-9]+ passed, [0-9]+ failed, [0-9]+ not applicable" "$L" | tail -1 | sed -E 's/Invariants: ([0-9]+) passed, ([0-9]+) failed, ([0-9]+) not applicable/\1p\/\2f\/\3na/')
            el=$(grep -oE "elapsed[_ ]sec[\"': ]+[0-9.]+" "$L" | tail -1 | grep -oE '[0-9.]+$' | cut -d. -f1)
            [[ "$rc" == 0 ]] && mark="" || mark="!rc=$rc"
            done_list+=" $s(${inv:-?}${el:+,${el}s}$mark)"
        else
            cur=$(tr '\r' '\n' < "$L" | grep -E '%\|' | tail -1 | sed -E 's/^[^:]*: *//; s/ *\[.*//' | cut -c1-40)
            [[ -z "$cur" ]] && cur=$(tail -1 "$L" | cut -c1-60)
            running=" >> $s: $cur"
        fi
    done
    printf "%-11s done:%s%s\n" "$b" "${done_list:- -}" "${running:-}"
    running=""
done
