#!/bin/bash
# run_single_thread_bench.sh - Single-threaded benchmark script using runner2.py
#
# Usage: ./scripts/run_single_thread_bench.sh <backend> <sql_dump_path> <branch_counts> [OPTIONS]
#
# Required Arguments:
#   backend:        dolt, dolt_mysql, seekdb, neon, xata, file_copy
#   sql_dump_path:  Path to SQL dump file (e.g., schemas/tpcc_mini.sql)
#   branch_counts:  Comma-separated list of branch counts (e.g., 8,16,32) or single value (e.g., 16)
#
# Options:
#   --seed <seed>         Random seed for reproducibility
#   --shape <shape>       Branch tree shape: spine, bushy, or fan_out (default: spine)
#   --measure-storage     Measure disk size before/after each update
#   --operations <ops>    Comma-separated list (e.g., READ,UPDATE; default: all)
#   --range-size <n>      Range size for RANGE_UPDATE operation (default: 200)
#   --table-name <name>   Table to run operations against (default: orders)
#   --num-ops <n>         Number of operations to perform (overrides defaults)
#   --output-dir <dir>    Output directory for results (default: ./run_stats)
#
# Examples:
#   ./scripts/run_single_thread_bench.sh dolt schemas/tpcc_mini.sql 16
#   ./scripts/run_single_thread_bench.sh neon schemas/tpcc_mini.sql 8,16,32
#   ./scripts/run_single_thread_bench.sh dolt schemas/tpcc_mini.sql 8 --measure-storage
#   ./scripts/run_single_thread_bench.sh neon schemas/tpcc_mini.sql 16,32 --operations READ,UPDATE

set -e

# Parse arguments
BACKEND=""
SQL_DUMP_PATH=""
NUM_BRANCHES=""
SEED=""
SHAPE="spine"
MEASURE_STORAGE=false
OPS_STRING=""
NUM_OPS_OVERRIDE=""
OUTPUT_DIR="./run_stats"
RANGE_SIZE=200
TABLE_NAME=""
BACKFILL_FRACTION=1.0


while [[ $# -gt 0 ]]; do
    case $1 in
        --seed)
            SEED="$2"
            shift 2
            ;;
        --shape)
            SHAPE="$2"
            shift 2
            ;;
        --measure-storage)
            MEASURE_STORAGE=true
            shift
            ;;
        --operations)
            OPS_STRING="$2"
            shift 2
            ;;
        --range-size)
            RANGE_SIZE="$2"
            shift 2
            ;;
        --table-name)
            TABLE_NAME="$2"
            shift 2
            ;;
        --backfill-fraction)
            BACKFILL_FRACTION="$2"
            shift 2
            ;;
        --num-ops)
            NUM_OPS_OVERRIDE="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        *)
            if [ -z "$BACKEND" ]; then
                BACKEND="$1"
            elif [ -z "$SQL_DUMP_PATH" ]; then
                SQL_DUMP_PATH="$1"
            elif [ -z "$NUM_BRANCHES" ]; then
                NUM_BRANCHES="$1"
            else
                echo "Error: Unexpected argument '$1'"
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate required arguments
if [ -z "$BACKEND" ] || [ -z "$SQL_DUMP_PATH" ] || [ -z "$NUM_BRANCHES" ]; then
    echo "Usage: $0 <backend> <sql_dump_path> <branch_counts> [options]"
    echo ""
    echo "Required:"
    echo "  backend: dolt, dolt_mysql, seekdb, neon, xata, file_copy"
    echo "  sql_dump_path: Path to SQL dump file (e.g., db_setup/tpcc_schema.sql)"
    echo "  branch_counts: Comma-separated list of branch counts (e.g., 8,16,32) or single value (e.g., 16)"
    echo ""
    echo "Options:"
    echo "  --seed <seed>: Random seed for reproducibility (default: random)"
    echo "  --shape <shape>: Branch tree shape: spine, bushy, or fan_out (default: spine)"
    echo "  --measure-storage: Measure disk_size_before/after for each update"
    echo "  --operations <ops>: Comma-separated list (e.g., READ,UPDATE; default: all)"
    echo "  --range-size <n>: Range size for RANGE_UPDATE operation (default: 200)"
    echo "  --table-name <name>: Table to run operations against (default: orders)"
    echo "  --num-ops <n>: Number of operations (overrides defaults)"
    echo "  --output-dir <dir>: Output directory for results (default: ./run_stats)"
    echo ""
    echo "Examples:"
    echo "  $0 dolt schemas/tpcc_mini.sql 8                      # Single branch count"
    echo "  $0 neon schemas/tpcc_mini.sql 8,16,32                # Multiple branch counts"
    echo "  $0 dolt schemas/tpcc_mini.sql 16 --operations READ   # Only READ operations"
    echo "  $0 dolt schemas/tpcc_mini.sql 8 --measure-storage    # Storage measurement"
    exit 1
fi

# Convert backend to uppercase for proto config
BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')

# Validate backend
if [[ ! "$BACKEND_UPPER" =~ ^(DOLT|DOLT_MYSQL|SEEKDB|NEON|XATA|FILE_COPY)$ ]]; then
    echo "Error: Invalid backend '$BACKEND'. Must be one of: dolt, dolt_mysql, seekdb, neon, xata, file_copy"
    exit 1
fi

# Convert shape to uppercase for proto config and validate
SHAPE_UPPER=$(echo "$SHAPE" | tr '[:lower:]' '[:upper:]')
if [[ ! "$SHAPE_UPPER" =~ ^(SPINE|BUSHY|FAN_OUT)$ ]]; then
    echo "Error: Invalid shape '$SHAPE'. Must be one of: spine, bushy, fan_out"
    exit 1
fi

# Check if SQL dump file exists
if [ ! -f "$SQL_DUMP_PATH" ]; then
    echo "Error: SQL dump file not found: $SQL_DUMP_PATH"
    exit 1
fi

# Generate random seed if not provided
if [ -z "$SEED" ]; then
    SEED=$(( (RANDOM * 32768 + RANDOM) % 2147483647 ))
fi

# Default operations (using runner2 operation names)
OPERATIONS=(BRANCH_CREATE READ BRANCH_CONNECT INSERT UPDATE RANGE_READ RANGE_UPDATE)

# Override OPERATIONS if --operations was provided
if [ -n "$OPS_STRING" ]; then
    IFS=',' read -ra OPERATIONS <<< "$OPS_STRING"
fi

# Parse branch counts into array
IFS=',' read -ra BRANCH_COUNTS <<< "$NUM_BRANCHES"

# Other fixed config values
TABLE_NAME="${TABLE_NAME:-orders}"
DB_NAME="microbench"
INSERTS_PER_BRANCH=0
UPDATES_PER_BRANCH=0
DELETES_PER_BRANCH=0

# Create temporary config file
TEMP_CONFIG=$(mktemp /tmp/${BACKEND}_bench_config_XXXXXX)

cleanup() {
    rm -f "$TEMP_CONFIG"
}
trap cleanup EXIT

# Extract first 4 chars of sql_dump filename for run_id
SQL_BASENAME=$(basename "$SQL_DUMP_PATH" .sql)
SQL_PREFIX=${SQL_BASENAME:0:4}

# Function to get num_ops based on operation type
get_num_ops() {
    local op=$1
    case $op in
        BRANCH_CREATE|BRANCH_DELETE)
            echo 1
            ;;
        RANGE_UPDATE)
            echo 200
            ;;
        BRANCH_COMMIT|BRANCH_DIFF|BRANCH_LOG|BRANCH_MERGE|BRANCH_REBASE|BRANCH_REVERT|BRANCH_RESET)
            echo 100
            ;;
        BRANCH_CONNECT|READ|INSERT|UPDATE|DELETE|RANGE_READ)
            echo 1000
            ;;
                DDL_ADD_INDEX|DDL_REMOVE_INDEX|DDL_VACUUM|DDL_ADD_COLUMN|DDL_REMOVE_COLUMN|DDL_BACKFILL|DDL_ADD_COLUMN_WITH_DEFAULT|DDL_TYPE_CHANGE|DDL_RENAME_COLUMN|DDL_RENAME_TABLE|DDL_ADD_FK|DDL_ADD_CHECK|DDL_SET_DEFAULT|DDL_SET_NOT_NULL|DDL_EXPAND_CONTRACT)
            echo 10
            ;;
        *)
            echo 1000
            ;;
    esac
}

echo "==================================================="
echo "Single-Threaded Microbenchmark (runner2.py)"
echo "Backend: $BACKEND"
echo "SQL Dump: $SQL_DUMP_PATH"
echo "Operations: ${OPERATIONS[*]}"
echo "Branch Counts: ${BRANCH_COUNTS[*]}"
echo "Branch Shape: $SHAPE"
echo "Random Seed: $SEED"
echo "Measure Storage: $MEASURE_STORAGE"
echo "Backfill Fraction: $BACKFILL_FRACTION"
if [ -n "$NUM_OPS_OVERRIDE" ]; then
    echo "Num Ops (override): $NUM_OPS_OVERRIDE"
fi
echo "==================================================="

# Loop through all operations, then all branch counts
for OPERATION in "${OPERATIONS[@]}"; do
    # Use override if provided
    if [ -n "$NUM_OPS_OVERRIDE" ]; then
        NUM_OPS="$NUM_OPS_OVERRIDE"
    else
        NUM_OPS=$(get_num_ops "$OPERATION")
    fi

    for NUM_BRANCHES in "${BRANCH_COUNTS[@]}"; do
        SHAPE_LOWER=$(echo "$SHAPE" | tr '[:upper:]' '[:lower:]')
        RUN_ID="${BACKEND}_${SQL_PREFIX}_${NUM_BRANCHES}_${SHAPE_LOWER}"

        # For BRANCH_CREATE operation, num_branches in setup should be 0
        # For all other operations, num_branches matches target
        if [ "$OPERATION" = "BRANCH_CREATE" ]; then
            SETUP_NUM_BRANCHES=0
        else
            SETUP_NUM_BRANCHES=$NUM_BRANCHES
        fi

        echo ""
        echo "---------------------------------------------------"
        echo "Running: $RUN_ID"
        echo "  Operation: $OPERATION, Num Ops: $NUM_OPS, Setup Branches: $SETUP_NUM_BRANCHES"
        echo "---------------------------------------------------"

    # Generate config file (task2.proto format for runner2.py)
    cat > "$TEMP_CONFIG" << EOF
# Auto-generated config for runner2.py
run_id: "${RUN_ID}"
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

num_threads: 1
measure_storage: ${MEASURE_STORAGE}

operation_benchmark {
  operation: ${OPERATION}
  num_ops: ${NUM_OPS}

  setup {
    num_branches: ${SETUP_NUM_BRANCHES}
    branch_shape: ${SHAPE_UPPER}
    inserts_per_branch: ${INSERTS_PER_BRANCH}
    updates_per_branch: ${UPDATES_PER_BRANCH}
    deletes_per_branch: ${DELETES_PER_BRANCH}
  }

  range_config {
    range_size: ${RANGE_SIZE}
  }
  ddl_config {
    backfill_fraction: ${BACKFILL_FRACTION}
  }
}
EOF

    echo "Config generated at: $TEMP_CONFIG"
    cat "$TEMP_CONFIG"
    echo ""

    # Run the benchmark
    echo "Starting benchmark..."
    uv run python -m microbench.runner2 --config "$TEMP_CONFIG" --output-dir "$OUTPUT_DIR"

        # Clean up dropped databases to prevent disk space explosion (Dolt only)
        if [ "$BACKEND" = "dolt" ] || [ "$BACKEND" = "dolt_mysql" ]; then
            if [ "$BACKEND" = "dolt_mysql" ]; then
                DOLT_DIR="${DOLT_MYSQL_DATA_DIR:-$HOME/dolt/databases}"
            else
                DOLT_DIR="${DOLT_DATA_DIR:-$HOME/doltgres/databases}"
            fi
            if [ -d "$DOLT_DIR/.dolt_dropped_databases" ]; then
                DROPPED_COUNT=$(ls -1 "$DOLT_DIR/.dolt_dropped_databases" 2>/dev/null | wc -l)
                if [ "$DROPPED_COUNT" -gt 0 ]; then
                    echo "Cleaning up $DROPPED_COUNT dropped database(s) from $DOLT_DIR/.dolt_dropped_databases"
                    rm -rf "$DOLT_DIR/.dolt_dropped_databases"/*
                    echo "Cleanup complete"
                fi
            fi
        fi

        echo "Completed: $RUN_ID"
    done  # BRANCH_COUNT loop
done  # OPERATION loop

echo ""
echo "==================================================="
echo "All benchmarks completed!"
echo "Results are in $OUTPUT_DIR/"
echo "==================================================="
