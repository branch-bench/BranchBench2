#!/bin/bash
# run_throughput_bench.sh - Dedicated script for throughput experiments
#
# Usage:
#   ./scripts/run_throughput_bench.sh <backend> <sql_dump_path> --sweep-concurrency --threads <N> --branches <N> [options]
#   ./scripts/run_throughput_bench.sh <backend> <sql_dump_path> --sweep-branches --threads <N> [options]
#   ./scripts/run_throughput_bench.sh <backend> <sql_dump_path> --sweep-proportional [options]
#
# Runs in async mode by default: each thread keeps --concurrent-requests ops
# in flight through exec_async(), one pooled connection each. Use --mode sync
# to run one op at a time per thread through exec().
#
# Examples:
#   # Fix threads at 8 and branches at 1, vary concurrency: 1,2,4,8,16,32
#   ./scripts/run_throughput_bench.sh dolt db.sql --sweep-concurrency --threads 8 --branches 1 --concurrency-list "1,2,4,8,16,32"
#
#   # Fix 4 threads x 16 concurrent requests, vary branches: 4 stay active, the rest idle
#   ./scripts/run_throughput_bench.sh dolt db.sql --sweep-branches --threads 4 --concurrent-requests 16
#
#   # One thread per branch with 4 requests in flight each, 1-128 branches
#   ./scripts/run_throughput_bench.sh dolt db.sql --sweep-proportional --threads-per-branch 1 --concurrent-requests 4

set -e

# Parse arguments
BACKEND=""
SQL_DUMP_PATH=""
SWEEP_MODE=""  # "concurrency", "branches", or "proportional"
MODE="async"   # "async" or "sync"
FIXED_THREADS=""
FIXED_BRANCHES=""
BRANCH_LIST=""
CONCURRENCY_LIST=""
THREADS_PER_BRANCH="4"  # Default ratio for proportional mode
OPERATIONS=""
NUM_OPS_OVERRIDE=""
POINT_OPS_OVERRIDE=""
RANGE_OPS_OVERRIDE=""
WARMUP_OPS=""
WARMUP_FRACTION=""
CONCURRENT_REQUESTS="1"
OUTPUT_DIR="/tmp/run_stats"
TABLE_NAME=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --sweep-concurrency)
            SWEEP_MODE="concurrency"
            shift
            ;;
        --sweep-branches)
            SWEEP_MODE="branches"
            shift
            ;;
        --sweep-proportional)
            SWEEP_MODE="proportional"
            shift
            ;;
        --mode)
            MODE="$2"
            shift 2
            ;;
        --threads)
            FIXED_THREADS="$2"
            shift 2
            ;;
        --branches)
            FIXED_BRANCHES="$2"
            shift 2
            ;;
        --threads-per-branch)
            THREADS_PER_BRANCH="$2"
            shift 2
            ;;
        --branch-list)
            BRANCH_LIST="$2"
            shift 2
            ;;
        --concurrency-list)
            CONCURRENCY_LIST="$2"
            shift 2
            ;;
        --num-ops)
            NUM_OPS_OVERRIDE="$2"
            shift 2
            ;;
        --point-ops)
            POINT_OPS_OVERRIDE="$2"
            shift 2
            ;;
        --range-ops)
            RANGE_OPS_OVERRIDE="$2"
            shift 2
            ;;
        --warmup-ops)
            WARMUP_OPS="$2"
            shift 2
            ;;
        --warmup-fraction)
            WARMUP_FRACTION="$2"
            shift 2
            ;;
        --concurrent-requests)
            CONCURRENT_REQUESTS="$2"
            shift 2
            ;;
        --table-name)
            TABLE_NAME="$2"
            shift 2
            ;;
        --operations)
            OPERATIONS="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -*)
            echo "Error: Unknown option '$1'"
            exit 1
            ;;
        *)
            if [ -z "$BACKEND" ]; then
                BACKEND="$1"
            elif [ -z "$SQL_DUMP_PATH" ]; then
                SQL_DUMP_PATH="$1"
            else
                echo "Error: Unexpected argument '$1'"
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate required arguments
if [ -z "$BACKEND" ] || [ -z "$SQL_DUMP_PATH" ] || [ -z "$SWEEP_MODE" ]; then
    echo "Usage: $0 <backend> <sql_dump_path> {--sweep-concurrency | --sweep-branches | --sweep-proportional} [options]"
    echo ""
    echo "Required arguments:"
    echo "  backend: dolt, dolt_mysql, seekdb, neon, xata, file_copy"
    echo "  sql_dump_path: Path to SQL dump file"
    echo "  --sweep-concurrency: Fix threads/branches, vary concurrent requests (requires --threads and --branches; async only)"
    echo "  --sweep-branches: Fix threads, vary branches (requires --threads)"
    echo "  --sweep-proportional: Vary both threads and branches proportionally"
    echo ""
    echo "Options:"
    echo "  --mode <async|sync>: Runner to use (default: async). Sync runs one op at a time per thread."
    echo "  --threads <N>: Fixed thread count (for --sweep-concurrency and --sweep-branches modes)"
    echo "  --branches <N>: Fixed branch count (for --sweep-concurrency mode)"
    echo "  --threads-per-branch <N>: Threads per branch ratio for --sweep-proportional (default: 4)"
    echo "  --branch-list <list>: Comma-separated branch counts (e.g., '1,2,4,8,16')"
    echo "  --concurrency-list <list>: Comma-separated concurrency levels (e.g., '1,2,4,8,16')"
    echo "  --concurrent-requests <n>: Ops in flight per thread, one pooled connection each (default: 1; async only if > 1)"
    echo "  --num-ops <n>: Number of operations per thread (overrides all operation-specific settings)"
    echo "  --point-ops <n>: Number of operations for point operations (READ, INSERT, UPDATE, DELETE)"
    echo "  --range-ops <n>: Number of operations for range operations (RANGE_READ, RANGE_UPDATE)"
    echo "  --warmup-ops <n>: Number of warm-up operations per thread (not counted in throughput)"
    echo "  --warmup-fraction <f>: Warm-up as fraction of num-ops (e.g., 0.2 for 20%)"
    echo "  --operations <ops>: Comma-separated list (e.g., READ,RANGE_READ)"
    echo "  --output-dir <dir>: Output directory (default: /tmp/run_stats)"
    echo ""
    echo "Examples:"
    echo "  # 8 threads, 1 branch, varying concurrency"
    echo "  $0 dolt db.sql --sweep-concurrency --threads 8 --branches 1 --concurrency-list '1,2,4,8,16,32'"
    echo ""
    echo "  # 4 threads x 16 concurrent requests, varying branches (1,2,4,...,1024)"
    echo "  $0 dolt db.sql --sweep-branches --threads 4 --concurrent-requests 16"
    echo ""
    echo "  # One thread per branch, 4 requests in flight each, 1-128 branches"
    echo "  $0 neon db.sql --sweep-proportional --threads-per-branch 1 --concurrent-requests 4"
    echo ""
    echo "  # One op at a time per thread"
    echo "  $0 xata db.sql --sweep-branches --threads 16 --mode sync"
    exit 1
fi

# Validate sweep mode requirements
if [ "$SWEEP_MODE" = "concurrency" ]; then
    if [ -z "$FIXED_THREADS" ]; then
        echo "Error: --sweep-concurrency requires --threads <N>"
        exit 1
    fi
    if [ -z "$FIXED_BRANCHES" ]; then
        echo "Error: --sweep-concurrency requires --branches <N>"
        exit 1
    fi
fi

if [ "$SWEEP_MODE" = "branches" ] && [ -z "$FIXED_THREADS" ]; then
    echo "Error: --sweep-branches requires --threads <N>"
    exit 1
fi

# Convert backend to uppercase/lowercase
BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')
BACKEND_LOWER=$(echo "$BACKEND" | tr '[:upper:]' '[:lower:]')

# Validate backend
if [[ ! "$BACKEND_UPPER" =~ ^(DOLT|DOLT_MYSQL|SEEKDB|NEON|XATA|FILE_COPY)$ ]]; then
    echo "Error: Invalid backend '$BACKEND'"
    exit 1
fi

# Validate execution mode.
if [ "$MODE" = "async" ]; then
    USE_ASYNC="true"
elif [ "$MODE" = "sync" ]; then
    if [ "$SWEEP_MODE" = "concurrency" ] || [ "$CONCURRENT_REQUESTS" -gt 1 ]; then
        echo "Error: concurrent requests need async mode; drop --mode sync"
        exit 1
    fi
    USE_ASYNC="false"
else
    echo "Error: Invalid --mode '$MODE' (expected async or sync)"
    exit 1
fi

# Check SQL dump file
if [ ! -f "$SQL_DUMP_PATH" ]; then
    echo "Error: SQL dump file not found: $SQL_DUMP_PATH"
    exit 1
fi

# Default operation lists
if [ -z "$OPERATIONS" ]; then
    OPERATIONS="READ,RANGE_READ"
fi

# Convert operations to array
IFS=',' read -ra OPS_ARRAY <<< "$OPERATIONS"

# Determine thread, branch and concurrency lists based on sweep mode
if [ "$SWEEP_MODE" = "concurrency" ]; then
    # Fix threads and branches, vary concurrency
    if [ -n "$CONCURRENCY_LIST" ]; then
        IFS=',' read -ra CONCURRENCY_LEVELS <<< "$CONCURRENCY_LIST"
    else
        # Default concurrency levels
        CONCURRENCY_LEVELS=(1 2 4 8 16 32 64 128 256 512 1024)
    fi

    echo "==================================================="
    echo "Throughput Benchmark: SWEEP CONCURRENCY"
    echo "Fixed threads: $FIXED_THREADS"
    echo "Fixed branches: $FIXED_BRANCHES"
    echo "Concurrency levels: ${CONCURRENCY_LEVELS[*]}"
elif [ "$SWEEP_MODE" = "branches" ]; then
    # Fix threads, vary branches
    if [ -n "$BRANCH_LIST" ]; then
        IFS=',' read -ra BRANCH_COUNTS <<< "$BRANCH_LIST"
    else
        # Default branch counts
        BRANCH_COUNTS=(1 2 4 8 16 32 64 128 256 512 1024)
    fi

    echo "==================================================="
    echo "Throughput Benchmark: SWEEP BRANCHES"
    echo "Fixed threads: $FIXED_THREADS"
    echo "Branch counts: ${BRANCH_COUNTS[*]}"
elif [ "$SWEEP_MODE" = "proportional" ]; then
    # Vary both threads and branches proportionally:
    # threads = branches * threads_per_branch
    if [ -n "$BRANCH_LIST" ]; then
        IFS=',' read -ra BRANCH_COUNTS <<< "$BRANCH_LIST"
    else
        # Default branch counts for proportional mode
        BRANCH_COUNTS=(1 2 4 8 16 32 64 128)
    fi

    echo "==================================================="
    echo "Throughput Benchmark: SWEEP PROPORTIONAL"
    echo "Threads per branch: $THREADS_PER_BRANCH"
    echo "Branch counts: ${BRANCH_COUNTS[*]}"
fi

echo "Backend: $BACKEND"
echo "SQL Dump: $SQL_DUMP_PATH"
echo "Operations: ${OPS_ARRAY[*]}"
echo "Execution mode: $MODE"
if [ "$SWEEP_MODE" != "concurrency" ]; then
    echo "Concurrent Requests per Thread: $CONCURRENT_REQUESTS"
fi
if [ -n "$NUM_OPS_OVERRIDE" ]; then
    echo "Num Ops (override): $NUM_OPS_OVERRIDE"
fi
if [ -n "$POINT_OPS_OVERRIDE" ]; then
    echo "Point Ops (override): $POINT_OPS_OVERRIDE"
fi
if [ -n "$RANGE_OPS_OVERRIDE" ]; then
    echo "Range Ops (override): $RANGE_OPS_OVERRIDE"
fi
echo "==================================================="

# Fixed config values
TABLE_NAME="${TABLE_NAME:-orders}"
DB_NAME="throughput_bench"
INSERTS_PER_BRANCH=0
UPDATES_PER_BRANCH=0
DELETES_PER_BRANCH=0
RANGE_SIZE=100
SHAPE_UPPER="FAN_OUT"

# Create temporary config file
TEMP_CONFIG=$(mktemp /tmp/${BACKEND}_throughput_bench_config_XXXXXX)

cleanup() {
    rm -f "$TEMP_CONFIG"
}
trap cleanup EXIT

# Extract first 4 chars of sql_dump filename for run_id
SQL_BASENAME=$(basename "$SQL_DUMP_PATH" .sql)
SQL_PREFIX=${SQL_BASENAME:0:4}

# get_num_ops OPERATION NUM_THREADS
# Default number of timed operations per thread.
get_num_ops() {
    local op=$1
    local num_threads=$2
    case $op in
        BRANCH_CREATE)
            echo 1
            ;;
        BRANCH_CONNECT)
            # Scale connect ops with the number of threads (2x)
            echo $((num_threads * 2))
            ;;
        RANGE_UPDATE|RANGE_READ)
            echo 1000
            ;;
        DDL_ADD_INDEX|DDL_REMOVE_INDEX|DDL_VACUUM|DDL_ADD_COLUMN|DDL_REMOVE_COLUMN|DDL_BACKFILL|DDL_ADD_COLUMN_WITH_DEFAULT|DDL_TYPE_CHANGE|DDL_RENAME_COLUMN|DDL_RENAME_TABLE|DDL_ADD_FK|DDL_ADD_CHECK|DDL_SET_DEFAULT|DDL_SET_NOT_NULL|DDL_EXPAND_CONTRACT)
            echo 10
            ;;
        *)
            echo 5000
            ;;
    esac
}

# cleanup_dropped_databases
# Clean up dropped databases to prevent disk space explosion (Dolt only).
cleanup_dropped_databases() {
    local dolt_dir
    if [ "$BACKEND_LOWER" = "dolt" ]; then
        dolt_dir="${DOLT_DATA_DIR:-$HOME/doltgres/databases}"
    elif [ "$BACKEND_LOWER" = "dolt_mysql" ]; then
        dolt_dir="${DOLT_MYSQL_DATA_DIR:-$HOME/dolt/databases}"
    else
        return 0
    fi
    if [ -d "$dolt_dir/.dolt_dropped_databases" ]; then
        local dropped_count
        dropped_count=$(ls -1 "$dolt_dir/.dolt_dropped_databases" 2>/dev/null | wc -l)
        if [ "$dropped_count" -gt 0 ]; then
            echo "Cleaning up $dropped_count dropped database(s) from $dolt_dir/.dolt_dropped_databases"
            rm -rf "$dolt_dir/.dolt_dropped_databases"/*
            echo "Cleanup complete"
        fi
    fi
    # Explicit success: with set -e, a failed test above would end the sweep.
    return 0
}

# run_config NUM_THREADS NUM_BRANCHES CONCURRENT_REQUESTS
# Runs every operation in OPS_ARRAY for one (threads, branches, concurrency)
# configuration.
run_config() {
    local num_threads=$1
    local num_branches=$2
    local concurrent_requests=$3

    # run_id carries thread, branch and (async only) concurrency counts. Sync
    # runs keep the plain name, which plotting treats as concurrency 1.
    local run_id="${BACKEND}_${SQL_PREFIX}_tp_t${num_threads}_b${num_branches}"
    if [ "$USE_ASYNC" = "true" ]; then
        run_id="${run_id}_cr${concurrent_requests}"
    fi

    echo ""
    echo "==================================================="
    echo "Configuration: $num_threads threads, $num_branches branches, $concurrent_requests concurrent requests ($MODE)"
    # Each thread works on the first branch assigned to it (round-robin).
    if [ "$num_branches" -eq 0 ]; then
        echo "Distribution: no setup branches; all threads work on the root branch"
    elif [ "$num_threads" -lt "$num_branches" ]; then
        echo "Distribution: one thread per branch on $num_threads branches; the other $((num_branches - num_threads)) branches get no load"
    elif [ "$num_threads" -eq "$num_branches" ]; then
        echo "Distribution: one thread per branch"
    else
        echo "Distribution: ~$((num_threads / num_branches)) threads share each branch (cyclic)"
    fi
    echo "==================================================="

    local operation num_ops warmup_ops setup_num_branches
    for operation in "${OPS_ARRAY[@]}"; do
        # Use override if provided
        if [ -n "$NUM_OPS_OVERRIDE" ]; then
            num_ops="$NUM_OPS_OVERRIDE"
        # Use range-ops override for range operations
        elif [ -n "$RANGE_OPS_OVERRIDE" ] && [[ "$operation" =~ ^RANGE ]]; then
            num_ops="$RANGE_OPS_OVERRIDE"
        # Use point-ops override for point operations
        elif [ -n "$POINT_OPS_OVERRIDE" ] && [[ "$operation" =~ ^(READ|INSERT|UPDATE|DELETE)$ ]]; then
            num_ops="$POINT_OPS_OVERRIDE"
        else
            num_ops=$(get_num_ops "$operation" "$num_threads")
        fi

        # Calculate warmup_ops
        warmup_ops=0
        if [ -n "$WARMUP_OPS" ]; then
            warmup_ops=$WARMUP_OPS
        elif [ -n "$WARMUP_FRACTION" ]; then
            warmup_ops=$(awk "BEGIN {print int($num_ops * $WARMUP_FRACTION)}")
        fi

        # For BRANCH_CREATE, num_branches in setup should be 0
        # For all other operations, setup num_branches matches the target
        if [ "$operation" = "BRANCH_CREATE" ]; then
            setup_num_branches=0
        else
            setup_num_branches=$num_branches
        fi

        echo ""
        echo "---------------------------------------------------"
        echo "Running: $run_id, Operation: $operation"
        echo "  Num Ops: $num_ops, Warmup Ops: $warmup_ops, Setup Branches: $setup_num_branches"
        echo "  Threads: $num_threads, Branches: $num_branches, Concurrency: $concurrent_requests"
        echo "---------------------------------------------------"

        # Generate config file (task2.proto format for runner2.py)
        cat > "$TEMP_CONFIG" << EOF
# Auto-generated config for throughput benchmark (task2.proto)
run_id: "${run_id}"
backend: ${BACKEND_UPPER}
table_name: "${TABLE_NAME}"
scale_factor: 1

database_setup {
  db_name: "${DB_NAME}"
  cleanup: true
  sql_dump {
    sql_dump_path: "${SQL_DUMP_PATH}"
  }
}

num_threads: ${num_threads}
measure_storage: false
concurrent_requests: ${concurrent_requests}
use_async: ${USE_ASYNC}

operation_benchmark {
  operation: ${operation}
  num_ops: ${num_ops}
  warmup_ops: ${warmup_ops}

  setup {
    num_branches: ${setup_num_branches}
    branch_shape: ${SHAPE_UPPER}
    inserts_per_branch: ${INSERTS_PER_BRANCH}
    updates_per_branch: ${UPDATES_PER_BRANCH}
    deletes_per_branch: ${DELETES_PER_BRANCH}
  }

  range_config {
    range_size: ${RANGE_SIZE}
  }
}
EOF

        # Run the benchmark
        echo "Starting benchmark..."
        uv run python -m microbench.runner2 --config "$TEMP_CONFIG" --output-dir "$OUTPUT_DIR"

        cleanup_dropped_databases

        echo "Completed: $run_id, Operation: $operation"
    done
}

# Main loop: iterate through all configurations
if [ "$SWEEP_MODE" = "concurrency" ]; then
    for concurrency in "${CONCURRENCY_LEVELS[@]}"; do
        run_config "$FIXED_THREADS" "$FIXED_BRANCHES" "$concurrency"
    done
elif [ "$SWEEP_MODE" = "proportional" ]; then
    for branches in "${BRANCH_COUNTS[@]}"; do
        run_config "$((branches * THREADS_PER_BRANCH))" "$branches" "$CONCURRENT_REQUESTS"
    done
else
    for branches in "${BRANCH_COUNTS[@]}"; do
        run_config "$FIXED_THREADS" "$branches" "$CONCURRENT_REQUESTS"
    done
fi

echo ""
echo "==================================================="
echo "All throughput benchmarks completed!"
echo "Results are in $OUTPUT_DIR/"
echo "==================================================="
