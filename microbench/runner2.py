"""
Runner2: Clean microbenchmark runner for single-operation benchmarks.

This module provides a cleaner, more maintainable architecture for running
database microbenchmarks with clear separation of concerns:
- Configuration loading and validation
- Prepared-target execution
- Untimed setup phase (branches + data)
- Timed execution phase (measure operations)
- Result collection and reporting

Standalone usage:
    python -m microbench.runner2 --config task.textproto --output-dir results

Embedding applications can supply a dblib.target.PreparedTarget:
    from microbench import runner2
    metrics = runner2.run(task_config, target, "MySystem", output_dir)

Design principles:
- Single Responsibility: Each class has one clear purpose
- Extensibility: Easy to add new operation types
- Testability: Components can be tested independently
- Clarity: Clear execution flow from config to results
"""

from __future__ import annotations

import asyncio
import argparse
import json
import os
import random
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, Dict, List
from contextlib import contextmanager
from pathlib import Path

from google.protobuf import text_format
from tqdm import tqdm

# Protobuf imports
from microbench import task2_pb2 as tp
from dblib import result_pb2 as rslt

from dblib.db_api import Ref
from dblib import result_collector as rc
from dblib.connector import prepared_database
from dblib.target import ExecutionState, PreparedTarget, SuiteRequest

# Operation imports
from microbench.operations import (
    OperationRegistry,
    ReadOperation,
    InsertOperation,
    UpdateOperation,
    DeleteOperation,
    RangeReadOperation,
    RangeUpdateOperation,
    BranchCreateOperation,
    BranchConnectOperation,
    BranchDeleteOperation,
    BranchCommitOperation,
    BranchDiffOperation,
    BranchLogOperation,
    BranchMergeOperation,
    BranchRebaseOperation,
    BranchRevertOperation,
    BranchResetOperation,
    AddIndexOperation,
    RemoveIndexOperation,
    VacuumOperation,
    AddColumnOperation,
    RemoveColumnOperation,
    BackfillOperation,
    AddColumnWithDefaultOperation,
    TypeChangeOperation,
    RenameColumnOperation,
    RenameTableOperation,
    AddForeignKeyOperation,
    AddCheckOperation,
    SetDefaultOperation,
    SetNotNullOperation,
    ExpandContractOperation,
)

# Git-like verbs measured on a state the operation prepares itself
VERB_OPERATIONS = frozenset({
    tp.OperationType.BRANCH_COMMIT,
    tp.OperationType.BRANCH_DIFF,
    tp.OperationType.BRANCH_LOG,
    tp.OperationType.BRANCH_MERGE,
    tp.OperationType.BRANCH_REBASE,
    tp.OperationType.BRANCH_REVERT,
    tp.OperationType.BRANCH_RESET,
})

# Utility imports
from microbench.datagen import DynamicDataGenerator
from util import db_helpers as dbh


# ============================================================================
# Configuration
# ============================================================================


class BenchmarkConfig:
    """Loads and validates task configuration from protobuf.

    This class encapsulates all configuration logic, making it easy to
    access configuration values throughout the benchmark code.
    """

    def __init__(self, config_proto: tp.TaskConfig):
        """Initialize from parsed protobuf config.

        Args:
            config_proto: Parsed TaskConfig protobuf message
        """
        self._proto = config_proto
        self._validate()

    def _validate(self) -> None:
        """Validate configuration values.

        Raises:
            ValueError: If configuration is invalid
        """
        # Basic required fields
        if not self._proto.run_id:
            raise ValueError("run_id is required")
        # Validate benchmark mode
        if not self._proto.HasField("operation_benchmark"):
            raise ValueError("operation_benchmark is required")

        op_bench = self._proto.operation_benchmark
        if op_bench.num_ops <= 0:
            raise ValueError("operation_benchmark.num_ops must be > 0")

        # Validate storage measurement
        if self._proto.measure_storage and self._proto.num_threads > 1:
            raise ValueError(
                "measure_storage is incompatible with num_threads > 1. "
                "Concurrent writes pollute per-thread storage deltas."
            )

        # Validate operation is registered
        op_type = op_bench.operation
        if not OperationRegistry.is_registered(op_type):
            raise ValueError(
                f"Operation type {tp.OperationType.Name(op_type)} not registered"
            )

    # Configuration properties
    @property
    def run_id(self) -> str:
        return self._proto.run_id

    @property
    def table_name(self) -> str:
        return self._proto.table_name

    @property
    def scale_factor(self) -> int:
        return self._proto.scale_factor

    @property
    def num_threads(self) -> int:
        return max(1, self._proto.num_threads)

    @property
    def measure_storage(self) -> bool:
        return self._proto.measure_storage

    @property
    def concurrent_requests(self) -> int:
        """Number of concurrent requests per thread (default 1)."""
        return max(1, self._proto.concurrent_requests)

    @property
    def use_async(self) -> bool:
        """Whether to run the async runner (use_async or concurrent_requests > 1)."""
        return self._proto.use_async or self.concurrent_requests > 1

    @property
    def operation_type(self) -> tp.OperationType:
        return self._proto.operation_benchmark.operation

    @property
    def operation_name(self) -> str:
        return tp.OperationType.Name(self._proto.operation_benchmark.operation)

    @property
    def num_ops(self) -> int:
        return self._proto.operation_benchmark.num_ops

    @property
    def warmup_ops(self) -> int:
        return self._proto.operation_benchmark.warmup_ops

    @property
    def setup_config(self) -> tp.SetupConfig:
        return self._proto.operation_benchmark.setup

    @property
    def num_branches(self) -> int:
        return self._proto.operation_benchmark.setup.num_branches

    @property
    def branch_shape(self) -> tp.BranchShape:
        return self._proto.operation_benchmark.setup.branch_shape

    @property
    def range_config(self) -> tp.RangeConfig:
        return self._proto.operation_benchmark.range_config

    @property
    def range_size(self) -> int:
        return self._proto.operation_benchmark.range_config.range_size or 10

    @property
    def ddl_config(self) -> tp.DdlConfig:
        return self._proto.operation_benchmark.ddl_config

    @property
    def branch_op_config(self) -> tp.BranchOpConfig:
        return self._proto.operation_benchmark.branch_op_config

    def __str__(self) -> str:
        return (
            f"BenchmarkConfig(run_id={self.run_id}, "
            f"operation={self.operation_name}, "
            f"num_ops={self.num_ops}, "
            f"num_branches={self.num_branches}, "
            f"scale_factor={self.scale_factor})"
        )

# ============================================================================
# Setup Phase (UNTIMED)
# ============================================================================


class SetupPhase:
    """Manages the untimed setup phase.

    Creates branches and performs data operations to prepare the database
    for measurement. Timing data is saved to a separate _setup.parquet file.
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        target: PreparedTarget,
        state: ExecutionState,
        output_dir: str = "/tmp/run_stats"
    ):
        self.config = config
        self.target = target
        self.state = state
        self.output_dir = output_dir

    def execute(self) -> ExecutionState:
        """Run setup and return the ref state for timed execution.

        Returns:
            The final working ref and all refs created during setup.

        Raises:
            Exception: If setup fails
        """
        setup = self.config.setup_config
        num_branches = setup.num_branches
        shape = setup.branch_shape
        inserts_per_branch = setup.inserts_per_branch
        updates_per_branch = setup.updates_per_branch
        deletes_per_branch = setup.deletes_per_branch

        print(f"\n{'='*60}")
        print(f"Setup Phase (UNTIMED)")
        print(f"{'='*60}")
        print(f"Creating {num_branches} branches (shape: {tp.BranchShape.Name(shape)})")
        print(f"Per branch: {inserts_per_branch} inserts, {updates_per_branch} updates, {deletes_per_branch} deletes")

        # Create result collector for setup timing
        setup_result_collector = rc.ResultCollector(
            run_id=f"{self.config.run_id}_setup",
            output_dir=self.output_dir,
        )

        # Create shared branch manager
        setup_branch_manager = SharedBranchManager()

        # Create a worker context for setup operations
        with self._create_setup_context(
            setup_result_collector, setup_branch_manager
        ) as setup_ctx:
            last_branch_name, _ = self._perform_setup_operations(
                setup_ctx,
                num_branches,
                shape,
                inserts_per_branch,
                updates_per_branch,
                deletes_per_branch,
            )

        # Get all created branches
        setup_branches = setup_branch_manager.get_all_branches()

        # Write setup timing to separate parquet file
        setup_result_collector.write_to_parquet(
            filename=f"{self.config.run_id}_setup.parquet"
        )

        print(f"Setup complete: {len(setup_branches)} branches created")
        print(f"Last branch: {last_branch_name}")
        print(f"{'='*60}\n")

        return ExecutionState(last_branch_name, tuple(setup_branches))

    @contextmanager
    def _create_setup_context(
        self,
        result_collector: rc.ResultCollector,
        branch_manager: 'SharedBranchManager'
    ):
        """Create a WorkerContext for setup operations."""
        # This is a simplified version just for setup
        # We'll define WorkerContext below
        from microbench.runner2 import WorkerContext

        ctx = WorkerContext(
            config=self.config,
            target=self.target,
            state=self.state,
            thread_id=0,
            seed=42,  # Fixed seed for reproducible setup
            result_collector=result_collector,
            branch_manager=branch_manager,
            shared_progress=None,
            assigned_branches=[],
        )

        try:
            ctx.__enter__()
            yield ctx
        finally:
            ctx.__exit__(None, None, None)

    def _perform_setup_operations(
        self,
        ctx: 'WorkerContext',
        num_branches: int,
        shape: tp.BranchShape,
        inserts: int,
        updates: int,
        deletes: int,
    ) -> Tuple[str, str]:
        """Perform branch creation and data operations.

        Returns:
            Tuple of (last_branch_name, last_branch_id)
        """
        # Get benchmark table
        benchmark_table = self.config.table_name
        if not benchmark_table:
            all_tables = dbh.get_all_tables(ctx.db_tools.get_current_connection())
            benchmark_table = ctx.rnd.choice(all_tables)

        # Initialize data generator
        table_schema = ctx.db_tools.get_table_schema(benchmark_table)
        if not table_schema:
            raise ValueError(f"Could not fetch DDL for table {benchmark_table}")

        ctx._table_datagen = DynamicDataGenerator(table_schema)
        ctx._pk_columns = dbh.get_pk_column_names(
            ctx.db_tools.get_current_connection(), benchmark_table
        )

        root_branch_name = ctx.current_branch
        current_parent = root_branch_name

        # Branches created so far (parents for the BUSHY shape)
        branches = [root_branch_name]

        # Perform setup on root branch
        print(f"Performing setup ops on root branch...")
        self._perform_branch_data_ops(
            ctx, benchmark_table, inserts, updates, deletes
        )

        # Create branches with progress bar. Branch creation is timed into
        # the setup collector; the data ops on each branch are not.
        for i in tqdm(range(num_branches), desc="Creating branches"):
            branch_name = f"setup_branch_{i + 1}"

            if shape == tp.BranchShape.SPINE:
                parent = current_parent  # Linear chain
            elif shape == tp.BranchShape.FAN_OUT:
                parent = root_branch_name  # All from root
            else:  # BUSHY
                parent = ctx.rnd.choice(branches)  # Random parent

            ctx.db_tools.branch(
                branch_name, from_ref=parent,
                storage=self.config.measure_storage,
            ).raise_for_status()
            ctx.connect(branch_name)
            current_parent = branch_name
            branches.append(branch_name)

            # Perform data operations on this branch
            self._perform_branch_data_ops(
                ctx, benchmark_table, inserts, updates, deletes
            )

            # Track the branch
            ctx.add_branch(branch_name)

        last_branch_name = ctx.current_branch
        return last_branch_name, last_branch_name

    def _perform_branch_data_ops(
        self,
        ctx: 'WorkerContext',
        table_name: str,
        inserts: int,
        updates: int,
        deletes: int
    ) -> None:
        """Perform inserts, updates, and deletes on current branch (untimed)."""
        # Perform inserts first
        for _ in range(inserts):
            ctx._insert_without_timing(table_name)

        # Shuffle and perform updates/deletes
        ops = ["update"] * updates + ["delete"] * deletes
        ctx.rnd.shuffle(ops)

        for op in ops:
            if op == "update":
                ctx._update_without_timing(table_name)
            else:
                ctx._delete_without_timing(table_name)


# ============================================================================
# Shared State Management
# ============================================================================


class SharedBranchManager:
    """Thread-safe manager for branch IDs and branch list."""

    def __init__(self, initial_branches: Optional[List[str]] = None):
        self._next_branch_id = 1
        self._branches: List[str] = list(initial_branches) if initial_branches else []
        self._lock = threading.Lock()

    def get_next_branch_id(self) -> int:
        """Atomically get next branch ID and increment."""
        with self._lock:
            current = self._next_branch_id
            self._next_branch_id += 1
            return current

    def add_branch(self, branch_name: str) -> None:
        """Add a branch to the shared list (once)."""
        with self._lock:
            if branch_name not in self._branches:
                self._branches.append(branch_name)

    def remove_branch(self, branch_name: str) -> None:
        """Remove a branch from the shared list."""
        with self._lock:
            if branch_name in self._branches:
                self._branches.remove(branch_name)

    def get_random_branch(self, rnd: random.Random) -> Optional[str]:
        """Get a random branch from the list."""
        with self._lock:
            if not self._branches:
                return None
            return rnd.choice(self._branches)

    def get_all_branches(self) -> List[str]:
        """Get a copy of all branches."""
        with self._lock:
            return list(self._branches)

    def __len__(self) -> int:
        with self._lock:
            return len(self._branches)


class SharedProgress:
    """Thread-safe shared progress bar."""

    def __init__(self, total: int, desc: str = "Progress", disable: bool = False):
        self._pbar = tqdm(total=total, desc=desc, position=0, leave=True, disable=disable)
        self._lock = threading.Lock()

    def update(self, n: int = 1) -> None:
        """Thread-safe update of progress bar."""
        with self._lock:
            self._pbar.update(n)

    def close(self) -> None:
        """Close the progress bar."""
        self._pbar.close()

    def write(self, msg: str) -> None:
        """Thread-safe write message above progress bar."""
        tqdm.write(msg)


# ============================================================================
# Worker Context
# ============================================================================


class TimedPhase:
    """Start barrier and timed window shared by all worker threads.

    Each worker connects, opens its async pool (if any) and runs its warm-up,
    then calls wait(). The timed phase starts when the last worker arrives,
    so all workers start their timed ops together and per-worker setup is
    kept out of the throughput window. Each worker calls finish() when its
    timed ops are done; the window ends at the last one.
    """

    def __init__(self, num_workers: int):
        self.start_time: Optional[float] = None
        self._end_times: List[float] = []
        self._lock = threading.Lock()
        self._barrier = threading.Barrier(num_workers, action=self._mark_start)

    def _mark_start(self) -> None:
        # Runs once, in the last worker to arrive, before any are released.
        self.start_time = time.time()

    def wait(self) -> None:
        self._barrier.wait()

    def abort(self) -> None:
        """Release workers blocked in wait() with BrokenBarrierError.

        Called when a worker fails before reaching the barrier, so the others
        fail instead of waiting forever.
        """
        self._barrier.abort()

    def finish(self) -> None:
        with self._lock:
            self._end_times.append(time.time())

    @property
    def end_time(self) -> Optional[float]:
        with self._lock:
            return max(self._end_times) if self._end_times else None


class WorkerContext:
    """Context object providing services to operations.

    This replaces the monolithic BenchmarkSuite with a cleaner interface
    that operations can use to access database connections, random number
    generation, data generation, etc.
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        target: PreparedTarget,
        state: ExecutionState,
        thread_id: int,
        seed: int,
        result_collector: rc.ResultCollector,
        branch_manager: SharedBranchManager,
        shared_progress: Optional[SharedProgress],
        assigned_branches: List[str],
    ):
        self.config = config
        self.target = target
        self.state = state
        self.thread_id = thread_id
        self.seed = seed
        self.result_collector = result_collector
        self.branch_manager = branch_manager
        self.shared_progress = shared_progress
        self.assigned_branches = assigned_branches
        self.measure_storage = config.measure_storage

        # Thread-local RNG
        self.rnd = random.Random(seed)

        # Database tools (initialized in __enter__)
        self.db_tools = None

        # Data generation and caching
        self._table_datagen: Optional[DynamicDataGenerator] = None
        self._pk_columns: List[str] = []
        self._existing_pks: List[Tuple] = []
        self._modified_keys: Dict[str, List[Tuple]] = {}

        # Index tracking for DDL operations
        self._created_indexes: Dict[str, List[str]] = {}  # table -> [index_names]
        self._created_columns: Dict[str, List[str]] = {}  # table -> [column_names]

    def __enter__(self) -> 'WorkerContext':
        """Initialize database connection and tools."""
        # Set thread ID for result collection
        rc.set_current_thread_id(self.thread_id)

        self.db_tools = self.target.open_suite(SuiteRequest(
            result_collector=self.result_collector,
            initial_ref=self.state.working_ref,
            worker_index=self.thread_id,
            setup_refs=self.state.setup_refs,
            measure_storage=self.measure_storage,
        ))
        self.branch_manager.add_branch(self.state.working_ref)

        # Connect to assigned branch if multi-threaded
        if self.assigned_branches:
            initial_branch = self.assigned_branches[0]
            if self.thread_id == 0:
                print(f"Thread {self.thread_id} connecting to: {initial_branch}")
            self.connect(initial_branch, timed=False)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close database connection."""
        if self.db_tools:
            self.db_tools.close_connection()

    # ========================================================================
    # Services for Operations
    # ========================================================================

    @property
    def current_ref(self) -> Ref:
        """Ref the worker's connection is on."""
        return self.db_tools.current_ref

    @property
    def current_branch(self) -> str:
        ref = self.db_tools.current_ref
        return ref.branch if ref else ""

    def connect(self, branch: str, timed: bool = True) -> None:
        """Switch to ``branch`` (an exec with an empty script, so the
        CONNECT row carries the switch cost)."""
        self.db_tools.exec([], refs=[branch], timed=timed).pop().raise_for_status()

    async def connect_async(self, branch: str, timed: bool = True) -> None:
        res = await self.db_tools.exec_async([], refs=[branch], timed=timed)
        res[0].raise_for_status()

    def run_sql(self, sql: str, params=None, timed: bool = True):
        """Run one statement on the current branch through exec() and
        return its rows. Raises if the statement failed."""
        res = self.db_tools.exec(
            [(sql, params)], refs=[self.current_ref], timed=timed,
            storage=self.measure_storage if timed else False,
        )[0]
        res.raise_for_status()
        return res.rows

    async def run_sql_async(self, sql: str, params=None, timed: bool = True):
        res = await self.db_tools.exec_async(
            [(sql, params)], refs=[self.current_ref], timed=timed,
            storage=self.measure_storage if timed else False,
        )
        res[0].raise_for_status()
        return res[0].rows

    def run_script(self, script, timed: bool = True):
        """Run a multi-statement script (or Python source) on the current
        branch as one exec(); raises if it failed."""
        res = self.db_tools.exec(
            script, refs=[self.current_ref], timed=timed,
            storage=self.measure_storage if timed else False,
        )[0]
        res.raise_for_status()
        return res

    async def run_script_async(self, script, timed: bool = True):
        res = await self.db_tools.exec_async(
            script, refs=[self.current_ref], timed=timed,
            storage=self.measure_storage if timed else False,
        )
        res[0].raise_for_status()
        return res[0]

    def get_pk_columns(self, table_name: str) -> List[str]:
        """Get primary key column names for a table."""
        if not self._pk_columns:
            self._pk_columns = dbh.get_pk_column_names(
                self.db_tools.get_current_connection(), table_name
            )
        return self._pk_columns

    def select_random_key(self, table_name: str) -> Optional[Tuple]:
        """Select a random primary key from the table."""
        cur_branch_id = self.current_branch

        pk_columns = self.get_pk_columns(table_name)
        existing_pks = self._existing_pks or dbh.get_pk_values(
            self.db_tools.get_current_connection(), table_name, pk_columns
        )

        # Cache PKs
        if not self._existing_pks and existing_pks:
            self._existing_pks = existing_pks

        if not existing_pks:
            return None

        # 50% chance to select from modified keys if available
        modified = self._modified_keys.get(cur_branch_id, [])
        if modified and self.rnd.random() < 0.5:
            return self.rnd.choice(modified)
        else:
            return self.rnd.choice(existing_pks)

    def generate_row(self, table_name: str) -> dict:
        """Generate a fake data row for the table."""
        if not self._table_datagen:
            table_schema = self.db_tools.get_table_schema(table_name)
            self._table_datagen = DynamicDataGenerator(table_schema)
        return self._table_datagen.generate_row()

    def track_modified_key(self, key: Tuple) -> None:
        """Track a modified primary key."""
        cur_branch_id = self.current_branch
        if key not in self._modified_keys.get(cur_branch_id, []):
            self._modified_keys.setdefault(cur_branch_id, []).append(key)

    def untrack_modified_key(self, key: Tuple) -> None:
        """Remove a key from modified tracking (e.g., after delete)."""
        cur_branch_id = self.current_branch
        if cur_branch_id in self._modified_keys:
            if key in self._modified_keys[cur_branch_id]:
                self._modified_keys[cur_branch_id].remove(key)

    def record_keys_touched(self, num_keys: int) -> None:
        """Record number of keys touched for this operation."""
        self.db_tools.result_collector.record_num_keys_touched(num_keys)

    def get_next_branch_id(self) -> int:
        """Get next unique branch ID."""
        return self.branch_manager.get_next_branch_id()

    def add_branch(self, branch_name: str) -> None:
        """Add a branch to the shared branch list."""
        self.branch_manager.add_branch(branch_name)

    def remove_branch(self, branch_name: str) -> None:
        """Remove a branch from the shared branch list."""
        self.branch_manager.remove_branch(branch_name)

    def get_random_branch(self) -> Optional[str]:
        """Get a random branch from the shared list."""
        return self.branch_manager.get_random_branch(self.rnd)

    def get_all_branches(self) -> List[str]:
        """Get all branches."""
        return self.branch_manager.get_all_branches()

    @property
    def root_branch(self) -> str:
        """The backend's default branch (never a deletion target)."""
        return self.target.root_ref

    def clear_pk_cache(self) -> None:
        """Clear cached primary keys (e.g., after branch switch)."""
        self._existing_pks = []

    def get_existing_key_count(self, table_name: str) -> int:
        """Number of existing primary keys in the table.

        Used to turn a backfill fraction into a concrete row count.
        """
        pk_columns = self.get_pk_columns(table_name)
        existing_pks = self._existing_pks or dbh.get_pk_values(
            self.db_tools.get_current_connection(), table_name, pk_columns
        )
        if not self._existing_pks and existing_pks:
            self._existing_pks = existing_pks
        return len(existing_pks) if existing_pks else 0

    def prepare_range_query(
        self, table_name: str, range_size: int, operation_name: str
    ) -> dict:
        """Prepare range query components.

        Returns:
            dict with keys_in_range, where_clause, params
        """
        pk_columns = self.get_pk_columns(table_name)

        # Get all PKs sorted
        existing_pks = self._existing_pks or dbh.get_pk_values(
            self.db_tools.get_current_connection(), table_name, pk_columns
        )

        if not self._existing_pks and existing_pks:
            self._existing_pks = existing_pks

        if not existing_pks:
            raise ValueError(f"No existing keys for {operation_name}")

        sorted_pks = sorted(existing_pks)
        total_keys = len(sorted_pks)
        range_size = min(range_size, total_keys)

        # Pick random range
        max_start_idx = max(0, total_keys - range_size)
        start_idx = self.rnd.randint(0, max_start_idx)
        end_idx = min(start_idx + range_size - 1, total_keys - 1)

        start_key = sorted_pks[start_idx]
        end_key = sorted_pks[end_idx]
        keys_in_range = sorted_pks[start_idx : end_idx + 1]

        # Build tuple comparison for composite PKs
        pk_tuple_sql = f"({', '.join(pk_columns)})"
        placeholders_start = ", ".join(
            [f"%(_start_{i})s" for i in range(len(pk_columns))]
        )
        placeholders_end = ", ".join(
            [f"%(_end_{i})s" for i in range(len(pk_columns))]
        )

        # Build params
        params = {}
        for i, val in enumerate(start_key):
            params[f"_start_{i}"] = val
        for i, val in enumerate(end_key):
            params[f"_end_{i}"] = val

        where_clause = (
            f"{pk_tuple_sql} >= ({placeholders_start}) AND "
            f"{pk_tuple_sql} <= ({placeholders_end})"
        )

        return {
            "keys_in_range": keys_in_range,
            "where_clause": where_clause,
            "params": params,
        }

    def track_created_index(self, table_name: str, index_name: str) -> None:
        """Track a created index for later querying/dropping."""
        self._created_indexes.setdefault(table_name, []).append(index_name)

    def untrack_index(self, table_name: str, index_name: str) -> None:
        """Remove an index from tracking."""
        if table_name in self._created_indexes:
            if index_name in self._created_indexes[table_name]:
                self._created_indexes[table_name].remove(index_name)

    def track_created_column(self, table_name: str, column_name: str) -> None:
        """Track a benchmark-created column so it can be safely dropped later."""
        self._created_columns.setdefault(table_name, []).append(column_name)

    def untrack_column(self, table_name: str, column_name: str) -> None:
        """Remove a column from tracking."""
        if table_name in self._created_columns:
            if column_name in self._created_columns[table_name]:
                self._created_columns[table_name].remove(column_name)

    def get_random_created_column(self, table_name: str) -> Optional[str]:
        """Get a random benchmark-created column for this table.

        Only returns columns this benchmark added, never original schema
        columns, so DROP COLUMN can never corrupt the base dataset.
        """
        columns = self._created_columns.get(table_name, [])
        if columns:
            return self.rnd.choice(columns)
        return None

    def get_random_index(self, table_name: str) -> Optional[str]:
        """Get a random index name from the table."""
        indexes = self._created_indexes.get(table_name, [])
        if indexes:
            return self.rnd.choice(indexes)

        # Query database for indexes
        # This is database-specific, simplified version:
        try:
            conn = self.db_tools.get_current_connection()
            cur = conn.cursor()
            cur.execute(f"""
                SELECT indexname FROM pg_indexes
                WHERE tablename = %s
                AND indexname NOT LIKE '%%_pkey'
            """, (table_name,))
            db_indexes = [row[0] for row in cur.fetchall()]
            if db_indexes:
                return self.rnd.choice(db_indexes)
        except Exception:
            pass

        return None

    # ========================================================================
    # Untimed operations for setup phase
    # ========================================================================

    def _insert_without_timing(self, table_name: str) -> None:
        """Insert without timing (for setup)."""
        cur_branch_id = self.current_branch

        col_names = dbh.get_all_columns(
            self.db_tools.get_current_connection(), table_name
        )
        pk_columns = self.get_pk_columns(table_name)

        placeholders = ", ".join([f"%({name})s" for name in col_names])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders});"

        for _ in range(5):
            row_data = self.generate_row(table_name)
            pk_tuple = tuple(row_data[pk] for pk in pk_columns)

            try:
                self.run_sql(insert_sql, row_data, timed=False)
                self._modified_keys.setdefault(cur_branch_id, []).append(pk_tuple)
                return
            except Exception:
                continue

        raise ValueError("Failed to insert row after 5 attempts")

    def _update_without_timing(self, table_name: str) -> None:
        """Update without timing (for setup)."""
        key_to_update = self.select_random_key(table_name)
        if not key_to_update:
            return  # Skip if no keys

        pk_columns = self.get_pk_columns(table_name)
        all_columns = dbh.get_all_columns(
            self.db_tools.get_current_connection(), table_name
        )
        non_pk_columns = [col for col in all_columns if col not in pk_columns]

        if not non_pk_columns:
            return

        row_data = self.generate_row(table_name)
        set_clause = ", ".join([f"{col} = %({col})s" for col in non_pk_columns])
        where_clause = " AND ".join(
            [f"{pk_name} = %({pk_name})s" for pk_name in pk_columns]
        )
        update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"

        for i, pk_col in enumerate(pk_columns):
            row_data[pk_col] = key_to_update[i]

        self.run_sql(update_sql, row_data, timed=False)
        self.track_modified_key(key_to_update)

    def _delete_without_timing(self, table_name: str) -> None:
        """Delete without timing (for setup)."""
        key_to_delete = self.select_random_key(table_name)
        if not key_to_delete:
            return

        pk_columns = self.get_pk_columns(table_name)
        where_clause = " AND ".join([f"{pk_name} = %s" for pk_name in pk_columns])
        delete_sql = f"DELETE FROM {table_name} WHERE {where_clause};"

        self.run_sql(delete_sql, key_to_delete, timed=False)
        self.untrack_modified_key(key_to_delete)


# ============================================================================
# Operation Execution
# ============================================================================


class OperationRunner:
    """Creates and executes operations using the registry pattern."""

    def __init__(self, config: BenchmarkConfig, context: WorkerContext):
        self.config = config
        self.context = context

        # Create the operation instance
        self.operation = self._create_operation()

    def _create_operation(self):
        """Create the operation instance based on config."""
        op_type = self.config.operation_type
        table_name = self.config.table_name

        # Operation-specific parameters
        if op_type in [tp.OperationType.RANGE_READ, tp.OperationType.RANGE_UPDATE]:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                range_size=self.config.range_size
            )
        elif op_type == tp.OperationType.DDL_ADD_INDEX:
            column_name = self.config.ddl_config.index_column_name or None
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=column_name
            )
        elif op_type == tp.OperationType.DDL_REMOVE_INDEX:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.DDL_VACUUM:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.DDL_ADD_COLUMN:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                column_type=(
                    self.config.ddl_config.column_type or "INTEGER"),)
        elif op_type == tp.OperationType.DDL_REMOVE_COLUMN:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,)
        elif op_type == tp.OperationType.DDL_BACKFILL:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                backfill_fraction=(
                    self.config.ddl_config.backfill_fraction or 1.0),
                column_name=self.config.ddl_config.column_name or None,)
        elif op_type == tp.OperationType.DDL_ADD_COLUMN_WITH_DEFAULT:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                column_type=(
                    self.config.ddl_config.column_type or "INTEGER"
                ),
                default_value=self.config.ddl_config.default_value or "0",)
        elif op_type == tp.OperationType.DDL_TYPE_CHANGE:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                new_type=self.config.ddl_config.column_type or "BIGINT",
            )
        elif op_type == tp.OperationType.DDL_RENAME_COLUMN:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                new_column_name=self.config.ddl_config.new_column_name or None,
            )
        elif op_type == tp.OperationType.DDL_RENAME_TABLE:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                new_table_name=self.config.ddl_config.new_table_name or None,
            )
        elif op_type == tp.OperationType.DDL_ADD_FK:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                ref_table_name=self.config.ddl_config.ref_table_name or None,
                ref_column_name=self.config.ddl_config.ref_column_name or None,
            )
        elif op_type == tp.OperationType.DDL_ADD_CHECK:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                check_expression=self.config.ddl_config.check_expression or "1=1",
            )
        elif op_type == tp.OperationType.DDL_SET_DEFAULT:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                default_value=self.config.ddl_config.default_value or "0",
            )
        elif op_type == tp.OperationType.DDL_SET_NOT_NULL:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
            )
        elif op_type == tp.OperationType.DDL_EXPAND_CONTRACT:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_type=self.config.ddl_config.column_type or "INTEGER",
            )
        elif op_type in [
            tp.OperationType.READ,
            tp.OperationType.INSERT,
            tp.OperationType.UPDATE,
            tp.OperationType.DELETE,
        ]:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.BRANCH_LOG:
            cfg = self.config.branch_op_config
            return OperationRegistry.create(
                op_type, table_name=table_name,
                changes_per_op=cfg.changes_per_op or 1,
                history_commits=cfg.history_commits or 10,
                log_limit=cfg.log_limit or 10,
            )
        elif op_type in VERB_OPERATIONS:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                changes_per_op=self.config.branch_op_config.changes_per_op or 1,
            )
        else:
            # Branch operations and others without parameters
            return OperationRegistry.create(op_type)

    def execute_single(self) -> None:
        """Execute the operation once."""
        try:
            self.operation.execute(self.context)
        except Exception as e:
            if self.context.shared_progress:
                self.context.shared_progress.write(
                    f"[Thread {self.context.thread_id}] Operation failed: {e}"
                )
            raise

    def execute_multiple(
        self,
        num_ops: int,
        warmup_ops: int = 0,
        timed_phase: Optional['TimedPhase'] = None,
    ) -> None:
        """Execute the operation N times with optional warm-up.

        Args:
            num_ops: Number of operations to execute (timed)
            warmup_ops: Number of warm-up operations to execute first (not counted in results)
            timed_phase: Shared start barrier and timed window. The timed ops
                start only once every worker has finished its warm-up.
        """
        # Execute warm-up operations (if any). They are not recorded, so the
        # results only ever hold timed ops.
        if warmup_ops > 0:
            if self.context.thread_id == 0:
                print(f"Executing {warmup_ops} warm-up operations per thread...")

            self.context.result_collector.set_recording(False)
            try:
                for i in range(warmup_ops):
                    try:
                        self.operation.execute(self.context)
                    except Exception as e:
                        # Build detailed error message for warmup
                        op_name = self.context.config.operation_name
                        error_type = type(e).__name__
                        msg = f"[Thread {self.context.thread_id}] Warm-up operation {i+1}/{warmup_ops} failed: {op_name} - {error_type}: {e}"
                        if self.context.shared_progress:
                            self.context.shared_progress.write(msg)
                        else:
                            print(msg)
            finally:
                self.context.result_collector.set_recording(True)

            if self.context.thread_id == 0:
                print(f"Warm-up complete. Starting timed measurement of {num_ops} operations...")

        if timed_phase:
            timed_phase.wait()

        # Execute timed operations
        for i in range(num_ops):
            try:
                self.operation.execute(self.context)

                # Update progress
                if self.context.shared_progress:
                    self.context.shared_progress.update(1)

            except Exception as e:
                # Record the failure with details
                self.context.result_collector.record_failure(error=e, operation_number=i+1)

                # Build detailed error message
                op_name = self.context.config.operation_name
                error_type = type(e).__name__
                msg = f"[Thread {self.context.thread_id}] Operation {i+1}/{num_ops} failed: {op_name} - {error_type}: {e}"

                if self.context.shared_progress:
                    self.context.shared_progress.write(msg)
                else:
                    print(msg)

                # Update progress even for failed ops
                if self.context.shared_progress:
                    self.context.shared_progress.update(1)

        if timed_phase:
            timed_phase.finish()


class AsyncOperationRunner:
    """Executes operations asynchronously with configurable concurrency.

    This runner uses asyncio to execute multiple operations concurrently,
    controlled by a semaphore to limit the number of concurrent requests.
    Each worker opens a pool with one connection per concurrent request, so
    the requests run in parallel on the server rather than queueing on a
    single connection.

    Each operation is timed independently with accurate start/end timestamps,
    ensuring correct performance measurement even with overlapping execution.
    Latency starts after a pool connection is acquired; the wait for one is
    recorded separately as pool_wait_time.
    """

    def __init__(self, config: BenchmarkConfig, context: WorkerContext):
        self.config = config
        self.context = context
        self.operation = self._create_operation()
        self.concurrent_limit = config.concurrent_requests

    def _create_operation(self):
        """Create the operation instance based on config."""
        # Reuse the same operation creation logic as OperationRunner
        op_type = self.config.operation_type
        table_name = self.config.table_name

        # Operation-specific parameters
        if op_type in [tp.OperationType.RANGE_READ, tp.OperationType.RANGE_UPDATE]:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                range_size=self.config.range_size
            )
        elif op_type == tp.OperationType.DDL_ADD_INDEX:
            column_name = self.config.ddl_config.index_column_name or None
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=column_name
            )
        elif op_type == tp.OperationType.DDL_REMOVE_INDEX:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.DDL_VACUUM:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.DDL_ADD_COLUMN:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                column_type=(
                    self.config.ddl_config.column_type or "INTEGER"
                ),
            )
        elif op_type == tp.OperationType.DDL_REMOVE_COLUMN:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
            )
        elif op_type == tp.OperationType.DDL_BACKFILL:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                backfill_fraction=(
                    self.config.ddl_config.backfill_fraction or 1.0),
                column_name=self.config.ddl_config.column_name or None,
            )
        elif op_type == tp.OperationType.DDL_ADD_COLUMN_WITH_DEFAULT:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                column_type=(
                    self.config.ddl_config.column_type or "INTEGER"
                ),
                default_value=self.config.ddl_config.default_value or "0",
            )
        elif op_type == tp.OperationType.DDL_TYPE_CHANGE:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                new_type=self.config.ddl_config.column_type or "BIGINT",
            )
        elif op_type == tp.OperationType.DDL_RENAME_COLUMN:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                new_column_name=self.config.ddl_config.new_column_name or None,
            )
        elif op_type == tp.OperationType.DDL_RENAME_TABLE:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                new_table_name=self.config.ddl_config.new_table_name or None,
            )
        elif op_type == tp.OperationType.DDL_ADD_FK:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                ref_table_name=self.config.ddl_config.ref_table_name or None,
                ref_column_name=self.config.ddl_config.ref_column_name or None,
            )
        elif op_type == tp.OperationType.DDL_ADD_CHECK:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                check_expression=self.config.ddl_config.check_expression or "1=1",
            )
        elif op_type == tp.OperationType.DDL_SET_DEFAULT:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                default_value=self.config.ddl_config.default_value or "0",
            )
        elif op_type == tp.OperationType.DDL_SET_NOT_NULL:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
            )
        elif op_type == tp.OperationType.DDL_EXPAND_CONTRACT:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_type=self.config.ddl_config.column_type or "INTEGER",
            )
        elif op_type in [
            tp.OperationType.READ,
            tp.OperationType.INSERT,
            tp.OperationType.UPDATE,
            tp.OperationType.DELETE,
        ]:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.BRANCH_LOG:
            cfg = self.config.branch_op_config
            return OperationRegistry.create(
                op_type, table_name=table_name,
                changes_per_op=cfg.changes_per_op or 1,
                history_commits=cfg.history_commits or 10,
                log_limit=cfg.log_limit or 10,
            )
        elif op_type in VERB_OPERATIONS:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                changes_per_op=self.config.branch_op_config.changes_per_op or 1,
            )
        else:
            # Branch operations and others without parameters
            return OperationRegistry.create(op_type)

    async def _execute_single_async(
        self, semaphore: asyncio.Semaphore, op_number: int, warmup: bool = False
    ) -> dict:
        """Execute a single operation asynchronously with semaphore control.

        Args:
            semaphore: Semaphore to limit concurrent execution
            op_number: Operation number for error reporting
            warmup: Warm-up op; not counted in the progress bar

        Returns:
            dict with status information for debugging
        """
        progress = None if warmup else self.context.shared_progress
        async with semaphore:
            try:
                # Check if operation has async version
                if hasattr(self.operation, 'execute_async'):
                    await self.operation.execute_async(self.context)
                else:
                    # Fall back to running sync operation in thread pool
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self.operation.execute, self.context)

                # Update progress
                if progress:
                    progress.update(1)

                return {"status": "success", "op_number": op_number}

            except Exception as e:
                # Record the failure
                self.context.result_collector.record_failure(error=e, operation_number=op_number)

                # Build detailed error message
                op_name = self.context.config.operation_name
                error_type = type(e).__name__
                msg = f"[Thread {self.context.thread_id}] Async operation {op_number} failed: {op_name} - {error_type}: {e}"

                if self.context.shared_progress:
                    self.context.shared_progress.write(msg)
                else:
                    print(msg)

                # Update progress even for failed ops
                if progress:
                    progress.update(1)

                return {"status": "failed", "op_number": op_number, "error": str(e)}

    async def _ensure_async_pool(self):
        """Open the worker's async pool: one connection per concurrent
        request, so a request never waits for a connection."""
        if self.context.db_tools.async_pool:
            return
        await self.context.db_tools.open_async_pool(self.concurrent_limit)

    async def execute_multiple_async(
        self,
        num_ops: int,
        warmup_ops: int = 0,
        timed_phase: Optional['TimedPhase'] = None,
    ) -> None:
        """Execute operations asynchronously with configurable concurrency.

        Args:
            num_ops: Number of operations to execute (timed)
            warmup_ops: Number of warm-up operations to execute first (not counted in results)
            timed_phase: Shared start barrier and timed window. The timed ops
                start only once every worker has opened its pool and finished
                its warm-up.
        """
        # Ensure async connection pool is initialized
        await self._ensure_async_pool()

        # Create semaphore to limit concurrent executions
        semaphore = asyncio.Semaphore(self.concurrent_limit)

        # Execute warm-up operations (if any)
        if warmup_ops > 0:
            if self.context.thread_id == 0:
                print(f"Executing {warmup_ops} warm-up operations with concurrency={self.concurrent_limit}...")

            warmup_tasks = [
                self._execute_single_async(semaphore, i+1, warmup=True)
                for i in range(warmup_ops)
            ]
            # Warm-up ops are not recorded. The tasks run on this thread, so
            # the per-thread switch covers all of them.
            self.context.result_collector.set_recording(False)
            try:
                await asyncio.gather(*warmup_tasks, return_exceptions=True)
            finally:
                self.context.result_collector.set_recording(True)

            if self.context.thread_id == 0:
                print(f"Warm-up complete. Starting timed measurement of {num_ops} operations...")

        if timed_phase:
            # Wait off the event loop so the pool's background tasks keep running.
            await asyncio.get_running_loop().run_in_executor(None, timed_phase.wait)

        # Execute timed operations concurrently
        tasks = [
            self._execute_single_async(semaphore, i+1)
            for i in range(num_ops)
        ]

        # Wait for all operations to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if timed_phase:
            timed_phase.finish()

        # Analyze results
        successes = [r for r in results if isinstance(r, dict) and r.get("status") == "success"]
        failures = [r for r in results if isinstance(r, dict) and r.get("status") == "failed"]
        exceptions = [r for r in results if isinstance(r, Exception)]
        nones = [r for r in results if r is None]

        # Record any uncaught exceptions as failures
        # These are exceptions that escaped the try/except in _execute_single_async
        if exceptions:
            for exc in exceptions:
                self.context.result_collector.record_failure(
                    error=exc,
                    operation_number=None
                )

        # Check for issues
        has_failures = len(failures) > 0
        has_exceptions = len(exceptions) > 0
        has_nones = len(nones) > 0
        total_returned = len(results)
        total_accounted = len(successes) + len(failures) + len(exceptions) + len(nones)
        has_accounting_error = total_accounted != total_returned
        has_missing_results = total_returned != num_ops

        # Only print detailed output if this thread has issues
        has_issues = (has_failures or has_exceptions or has_nones or
                     has_accounting_error or has_missing_results)

        if has_issues:
            print(f"[Thread {self.context.thread_id}] WARNING: Issues detected during async execution")
            print(f"  Tasks created: {num_ops}")
            print(f"  Successes: {len(successes)}/{num_ops} ({100*len(successes)/num_ops:.1f}%)")

            if has_failures:
                print(f"  Failures (caught): {len(failures)}")

            if has_exceptions:
                print(f"  Exceptions (uncaught): {len(exceptions)}")
                # Print first 5 exceptions with stack traces
                for i, exc in enumerate(exceptions[:5]):
                    print(f"    Exception {i+1}: {type(exc).__name__}: {exc}")
                    import traceback
                    traceback.print_exception(type(exc), exc, exc.__traceback__)

            if has_nones:
                print(f"  None values: {len(nones)} (unexpected!)")

            if has_accounting_error:
                print(f"  ERROR: Accounted results ({total_accounted}) != returned results ({total_returned})")

            if has_missing_results:
                print(f"  WARNING: Results returned ({total_returned}) != tasks created ({num_ops})")

        # Return stats for aggregation
        return {
            "thread_id": self.context.thread_id,
            "num_ops": num_ops,
            "successes": len(successes),
            "failures": len(failures),
            "exceptions": len(exceptions),
            "nones": len(nones),
            "has_issues": has_issues
        }

    def execute_multiple(
        self,
        num_ops: int,
        warmup_ops: int = 0,
        timed_phase: Optional['TimedPhase'] = None,
    ) -> dict:
        """Synchronous wrapper that runs async execution in event loop.

        Args:
            num_ops: Number of operations to execute (timed)
            warmup_ops: Number of warm-up operations to execute first
            timed_phase: Shared start barrier and timed window

        Returns:
            dict with execution statistics (successes, failures, etc.)
        """
        # Create and manage event loop explicitly to avoid cleanup issues in threads
        # This prevents "_ssock not found" errors in Python 3.12+ when running
        # multiple event loops in ThreadPoolExecutor workers
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                self.execute_multiple_async(num_ops, warmup_ops, timed_phase)
            )
        finally:
            # Close the async pool on its own loop, before that loop is
            # closed, so the server sees clean disconnects. This must come
            # before cancelling leftover tasks: the pool's background workers
            # are tasks on this loop, and close() waits for them to stop.
            try:
                loop.run_until_complete(self.context.db_tools.close_async_pool())
            except (Exception, asyncio.CancelledError) as e:
                print(f"[Thread {self.context.thread_id}] Failed to close async pool: {e!r}")
            try:
                # Cancel any remaining tasks
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                # Run loop once more to process cancellations
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            finally:
                loop.close()


# ============================================================================
# Benchmark Execution
# ============================================================================


class BenchmarkExecutor:
    """Orchestrates benchmark execution with thread management."""

    def __init__(
        self,
        config: BenchmarkConfig,
        target: PreparedTarget,
        state: ExecutionState,
        result_collector: rc.ResultCollector,
    ):
        self.config = config
        self.target = target
        self.state = state
        self.result_collector = result_collector

        # Shared state
        self.branch_manager = SharedBranchManager(initial_branches=list(state.setup_refs))

    def execute(self) -> dict:
        """Execute the benchmark and return metrics.

        Returns:
            dict with timing and throughput metrics
        """
        num_threads = self.config.num_threads
        num_ops = self.config.num_ops
        total_ops = num_ops * num_threads

        print(f"\n{'='*60}")
        print(f"Benchmark Execution (TIMED)")
        print(f"{'='*60}")
        print(f"Operation: {self.config.operation_name}")
        print(f"Threads: {num_threads}")
        print(f"Operations per thread: {num_ops}")
        print(f"Total operations: {total_ops}")

        # Create shared progress bar
        shared_progress = SharedProgress(
            total=total_ops,
            desc=f"Benchmark ({num_threads} threads)",
            disable=False,
        )

        # Partition branches among threads
        thread_branch_assignments = self._assign_branches_to_threads()

        # Fixed seed for reproducibility
        fixed_seed = int(time.time())
        print(f"Seed: {fixed_seed}")
        print(f"{'='*60}\n")

        # All workers start their timed ops together; see TimedPhase.
        self._timed_phase = TimedPhase(num_threads)
        wall_start = time.time()

        try:
            if num_threads == 1:
                # Single-threaded execution
                self._execute_single_threaded(
                    fixed_seed, thread_branch_assignments.get(0, []), shared_progress
                )
            else:
                # Multi-threaded execution
                self._execute_multi_threaded(
                    fixed_seed, thread_branch_assignments, shared_progress
                )
        finally:
            shared_progress.close()

        # Throughput window: from the start barrier (every worker connected
        # and warmed up) to the last worker finishing its timed ops. Thread
        # start-up, connection/pool setup and teardown are excluded.
        start_time = self._timed_phase.start_time
        end_time = self._timed_phase.end_time
        if start_time is None or end_time is None:
            raise RuntimeError(
                "No worker completed the timed phase; see the worker errors above."
            )
        elapsed_time = end_time - start_time
        wall_time = time.time() - wall_start

        metrics = self._calculate_metrics(total_ops, elapsed_time)
        metrics["wall_time"] = wall_time
        self._print_metrics(metrics)

        return metrics

    def _assign_branches_to_threads(self) -> Dict[int, List[str]]:
        """Assign branches to threads for multi-threaded execution."""
        num_threads = self.config.num_threads
        if not self.state.setup_refs or num_threads == 1:
            return {}

        assignments = {}
        num_branches = len(self.state.setup_refs)

        if num_threads <= num_branches:
            # Round-robin assignment
            for tid in range(num_threads):
                assignments[tid] = [
                    self.state.setup_refs[idx]
                    for idx in range(tid, num_branches, num_threads)
                ]
        else:
            # More threads than branches: cyclic assignment
            for tid in range(num_threads):
                assigned_branch = self.state.setup_refs[tid % num_branches]
                assignments[tid] = [assigned_branch]

        return assignments

    def _execute_single_threaded(
        self, seed: int, assigned_branches: List[str], progress: SharedProgress
    ) -> None:
        """Execute benchmark in single thread."""
        rc.set_current_thread_id(0)

        with WorkerContext(
            config=self.config,
            target=self.target,
            state=self.state,
            thread_id=0,
            seed=seed,
            result_collector=self.result_collector,
            branch_manager=self.branch_manager,
            shared_progress=progress,
            assigned_branches=assigned_branches,
        ) as ctx:
            # Initialize table name if needed
            if not self.config.table_name:
                all_tables = dbh.get_all_tables(ctx.db_tools.get_current_connection())
                self.config._proto.table_name = ctx.rnd.choice(all_tables)

            # Set context for result collection
            table_name = self.config.table_name
            table_schema = ctx.db_tools.get_table_schema(table_name)
            initial_db_size = 0
            try:
                initial_db_size = dbh.get_db_size(ctx.db_tools.get_current_connection())
            except Exception:
                pass

            ctx.db_tools.result_collector.set_context(
                table_name=table_name,
                table_schema=table_schema,
                initial_db_size=initial_db_size,
                seed=seed,
            )

            # Create and execute operations
            # Use async runner if use_async or concurrent_requests > 1
            if self.config.use_async:
                runner = AsyncOperationRunner(self.config, ctx)
                stats = runner.execute_multiple(
                    self.config.num_ops, self.config.warmup_ops, self._timed_phase
                )
                # Print aggregate summary for async execution
                self._print_aggregate_summary([stats])
            else:
                runner = OperationRunner(self.config, ctx)
                runner.execute_multiple(
                    self.config.num_ops, self.config.warmup_ops, self._timed_phase
                )

    def _execute_multi_threaded(
        self,
        fixed_seed: int,
        branch_assignments: Dict[int, List[str]],
        progress: SharedProgress,
    ) -> None:
        """Execute benchmark with multiple threads."""

        def worker_fn(thread_id: int, assigned_branches: List[str]):
            """Worker function for each thread.

            Returns:
                dict with stats if using async runner, None otherwise
            """
            try:
                return run_worker(thread_id, assigned_branches)
            except BaseException:
                # If this worker never reaches the start barrier, release the
                # others instead of leaving them waiting forever. No-op once
                # the timed phase has started.
                self._timed_phase.abort()
                raise

        def run_worker(thread_id: int, assigned_branches: List[str]):
            rc.set_current_thread_id(thread_id)
            worker_seed = fixed_seed + thread_id

            with WorkerContext(
                config=self.config,
                target=self.target,
                state=self.state,
                thread_id=thread_id,
                seed=worker_seed,
                result_collector=self.result_collector,
                branch_manager=self.branch_manager,
                shared_progress=progress,
                assigned_branches=assigned_branches,
            ) as ctx:
                # Initialize table name if needed
                if not self.config.table_name:
                    all_tables = dbh.get_all_tables(ctx.db_tools.get_current_connection())
                    self.config._proto.table_name = ctx.rnd.choice(all_tables)

                # Set context
                table_name = self.config.table_name
                table_schema = ctx.db_tools.get_table_schema(table_name)
                initial_db_size = 0
                try:
                    initial_db_size = dbh.get_db_size(ctx.db_tools.get_current_connection())
                except Exception:
                    pass

                ctx.db_tools.result_collector.set_context(
                    table_name=table_name,
                    table_schema=table_schema,
                    initial_db_size=initial_db_size,
                    seed=worker_seed,
                )

                # Create and execute operations
                # Use async runner if use_async or concurrent_requests > 1
                if self.config.use_async:
                    runner = AsyncOperationRunner(self.config, ctx)
                    return runner.execute_multiple(
                        self.config.num_ops, self.config.warmup_ops, self._timed_phase
                    )
                else:
                    runner = OperationRunner(self.config, ctx)
                    runner.execute_multiple(
                        self.config.num_ops, self.config.warmup_ops, self._timed_phase
                    )
                    return None

        # Execute with thread pool
        num_threads = self.config.num_threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(
                    worker_fn,
                    thread_id=tid,
                    assigned_branches=branch_assignments.get(tid, []),
                )
                for tid in range(num_threads)
            ]

            # Wait for completion and collect stats
            thread_stats = []
            for future in as_completed(futures):
                try:
                    stats = future.result()
                    if stats is not None:  # Async runner returns stats
                        thread_stats.append(stats)
                except Exception as e:
                    print(f"Worker thread failed: {type(e).__name__}: {e}")

            # Print aggregate summary for async execution
            if thread_stats:
                self._print_aggregate_summary(thread_stats)

    def _calculate_metrics(self, total_ops: int, elapsed_time: float) -> dict:
        """Calculate throughput and latency metrics.

        Note: total_ops is the intended count (num_ops * num_threads).
        Actual successful operation count is len(self.result_collector.results).
        """
        # One verb row (BRANCH, DELETE, ...) or one EXEC row per operation;
        # the statement rows inside an exec are its breakdown.
        rows = self.result_collector.results
        ok_rows = [r for r in rows if r.status == rslt.OpStatus.OK]
        actual_ops = sum(1 for r in ok_rows if r.op_type in rc.VERB_OP_TYPES)
        failed_ops = len(self.result_collector.failed_operations)
        throughput = actual_ops / elapsed_time if elapsed_time > 0 else 0

        # Data operation throughput: the SQL statements themselves
        data_ops = [r for r in ok_rows if r.op_type in rc.STATEMENT_OP_TYPES]
        data_ops_count = len(data_ops)
        data_ops_time = sum(r.latency for r in data_ops) if data_ops else 0
        data_throughput = data_ops_count / data_ops_time if data_ops_time > 0 else 0

        return {
            "intended_ops": total_ops,  # Intended number of operations
            "total_ops": actual_ops,  # Successful verb/exec operations
            "failed_ops": failed_ops,  # Operations that raised in the runner
            "elapsed_time": elapsed_time,
            "throughput": throughput,  # Based on actual successful ops
            "data_ops_count": data_ops_count,
            "data_ops_time": data_ops_time,
            "data_throughput": data_throughput,
            "support": self.result_collector.support_summary(),
        }

    def _print_aggregate_summary(self, thread_stats: list) -> None:
        """Print aggregate summary of async execution across all threads.

        Args:
            thread_stats: List of dicts with per-thread statistics
        """
        if not thread_stats:
            return

        # Aggregate stats across all threads
        total_ops = sum(s['num_ops'] for s in thread_stats)
        total_successes = sum(s['successes'] for s in thread_stats)
        total_failures = sum(s['failures'] for s in thread_stats)
        total_exceptions = sum(s['exceptions'] for s in thread_stats)
        total_nones = sum(s['nones'] for s in thread_stats)
        threads_with_issues = sum(1 for s in thread_stats if s['has_issues'])

        print(f"\n{'='*80}")
        print(f"Async Execution Summary ({len(thread_stats)} thread(s)):")
        print(f"  Total operations: {total_ops:,}")
        print(f"  Successes: {total_successes:,} ({100*total_successes/total_ops:.2f}%)" if total_ops > 0 else "  Successes: 0")

        if total_failures > 0:
            print(f"  Failures (caught): {total_failures:,} ({100*total_failures/total_ops:.2f}%)")

        if total_exceptions > 0:
            print(f"  Exceptions (uncaught): {total_exceptions:,} ({100*total_exceptions/total_ops:.2f}%)")

        if total_nones > 0:
            print(f"  None values: {total_nones:,}")

        if threads_with_issues > 0:
            print(f"  Threads with issues: {threads_with_issues}/{len(thread_stats)}")

        print(f"{'='*80}\n")

    def _print_metrics(self, metrics: dict) -> None:
        """Print benchmark metrics."""
        print(f"\n{'='*60}")
        print(f"Benchmark Results:")
        print(f"  Intended operations: {metrics['intended_ops']}")
        print(f"  Successful operations: {metrics['total_ops']}")
        print(f"  Failed operations: {metrics['failed_ops']}", end="")
        if metrics['failed_ops'] > 0 and metrics['intended_ops'] > 0:
            print(f" ({100*metrics['failed_ops']/metrics['intended_ops']:.1f}%)")
        else:
            print()
        print(f"  Timed window: {metrics['elapsed_time']:.2f}s (all workers started together)")
        print(f"  Wall time incl. worker setup: {metrics['wall_time']:.2f}s")
        print(f"  Throughput: {metrics['throughput']:.2f} ops/sec (successful ops only)")
        print(f"\n  SQL statements only (excluding branch ops):")
        print(f"    Count: {metrics['data_ops_count']}")
        print(f"    Time: {metrics['data_ops_time']:.2f}s")
        print(f"    Throughput: {metrics['data_throughput']:.2f} ops/sec")
        support = metrics.get("support", {})
        if support and not support.get("workflow_supported", True):
            print(f"  UNSUPPORTED on this backend: {support['unsupported_ops']}")
        print(f"{'='*60}\n")


# ============================================================================
# Result Management
# ============================================================================


class ResultManager:
    """Manages result writing to parquet and JSON."""

    def __init__(
        self,
        config: BenchmarkConfig,
        target: PreparedTarget,
        system_name: str,
        result_collector: rc.ResultCollector,
        metrics: dict,
        output_dir: str,
    ):
        self.config = config
        self.target = target
        self.system_name = system_name
        self.result_collector = result_collector
        self.metrics = metrics
        self.output_dir = output_dir

    def write_results(self) -> None:
        """Write all results to files."""
        # Write main parquet file
        self.result_collector.write_to_parquet()

        # Write summary JSON
        self._write_summary_json()

        # Write failure details if any failures occurred
        if self.result_collector.failed_operations:
            self._write_failures_json()

    def _write_summary_json(self) -> None:
        """Write summary JSON with throughput metrics."""
        summary = {
            "run_id": self.config.run_id,
            "backend": self.system_name,
            "operation": self.config.operation_name,
            "num_ops": self.config.num_ops,
            "num_threads": self.config.num_threads,
            "num_branches": self.config.num_branches,
            "execution_mode": "async" if self.config.use_async else "sync",
            "concurrent_requests": self.config.concurrent_requests,
            "scale_factor": self.config.scale_factor,
            "intended_ops": self.metrics["intended_ops"],  # num_ops * num_threads
            "total_ops": self.metrics["total_ops"],  # Actual successful operations
            "failed_ops": self.metrics["failed_ops"],  # Failed operations
            "elapsed_time": self.metrics["elapsed_time"],  # Timed window (start barrier to last worker done)
            "wall_time": self.metrics["wall_time"],  # Incl. worker connect/warm-up/teardown
            "throughput": self.metrics["throughput"],  # Based on successful ops only
            "data_ops_count": self.metrics["data_ops_count"],
            "data_ops_time": self.metrics["data_ops_time"],
            "data_throughput": self.metrics["data_throughput"],
            "support": self.metrics.get("support", {}),
        }
        # Server behaviour the backend worked around (retries and the like)
        observed = getattr(self.target.suite_type, "OBSERVED", None)
        if observed:
            summary["backend_observations"] = dict(observed)

        # Build filename
        filename_parts = [
            self.config.run_id,
            self.config.operation_name,
            f"threads{self.config.num_threads}",
        ]
        summary_filename = "_".join(filename_parts) + "_summary.json"
        summary_path = os.path.join(self.output_dir, summary_filename)

        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        print(f"Summary written to: {summary_path}")

    def _write_failures_json(self) -> None:
        """Write detailed failure information to a separate JSON file."""
        failures_data = {
            "run_id": self.config.run_id,
            "backend": self.system_name,
            "operation": self.config.operation_name,
            "total_failures": len(self.result_collector.failed_operations),
            "failures": self.result_collector.failed_operations,
        }

        # Build filename
        filename_parts = [
            self.config.run_id,
            self.config.operation_name,
            f"threads{self.config.num_threads}",
        ]
        failures_filename = "_".join(filename_parts) + "_failures.json"
        failures_path = os.path.join(self.output_dir, failures_filename)

        with open(failures_path, "w") as f:
            json.dump(failures_data, f, indent=2)

        print(f"Failure details written to: {failures_path}")


# ============================================================================
# Operation Registration
# ============================================================================


def register_all_operations() -> None:
    """Register all available operations with the registry."""
    # CRUD operations
    OperationRegistry.register(tp.OperationType.READ, ReadOperation)
    OperationRegistry.register(tp.OperationType.INSERT, InsertOperation)
    OperationRegistry.register(tp.OperationType.UPDATE, UpdateOperation)
    OperationRegistry.register(tp.OperationType.DELETE, DeleteOperation)
    OperationRegistry.register(tp.OperationType.RANGE_READ, RangeReadOperation)
    OperationRegistry.register(tp.OperationType.RANGE_UPDATE, RangeUpdateOperation)

    # Branch operations
    OperationRegistry.register(tp.OperationType.BRANCH_CREATE, BranchCreateOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_CONNECT, BranchConnectOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_DELETE, BranchDeleteOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_COMMIT, BranchCommitOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_DIFF, BranchDiffOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_LOG, BranchLogOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_MERGE, BranchMergeOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_REBASE, BranchRebaseOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_REVERT, BranchRevertOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_RESET, BranchResetOperation)

    # DDL operations
    OperationRegistry.register(tp.OperationType.DDL_ADD_INDEX, AddIndexOperation)
    OperationRegistry.register(tp.OperationType.DDL_REMOVE_INDEX, RemoveIndexOperation)
    OperationRegistry.register(tp.OperationType.DDL_VACUUM, VacuumOperation)
    OperationRegistry.register(tp.OperationType.DDL_ADD_COLUMN, AddColumnOperation)
    OperationRegistry.register(tp.OperationType.DDL_REMOVE_COLUMN, RemoveColumnOperation)
    OperationRegistry.register(tp.OperationType.DDL_BACKFILL, BackfillOperation)
    OperationRegistry.register(tp.OperationType.DDL_ADD_COLUMN_WITH_DEFAULT,AddColumnWithDefaultOperation,)
    OperationRegistry.register(tp.OperationType.DDL_TYPE_CHANGE, TypeChangeOperation)
    OperationRegistry.register(tp.OperationType.DDL_RENAME_COLUMN, RenameColumnOperation)
    OperationRegistry.register(tp.OperationType.DDL_RENAME_TABLE, RenameTableOperation)
    OperationRegistry.register(tp.OperationType.DDL_ADD_FK, AddForeignKeyOperation)
    OperationRegistry.register(tp.OperationType.DDL_ADD_CHECK, AddCheckOperation)
    OperationRegistry.register(tp.OperationType.DDL_SET_DEFAULT, SetDefaultOperation)
    OperationRegistry.register(tp.OperationType.DDL_SET_NOT_NULL, SetNotNullOperation)
    OperationRegistry.register(tp.OperationType.DDL_EXPAND_CONTRACT, ExpandContractOperation)


# ============================================================================
# Entry point
# ============================================================================


def run(
    task: tp.TaskConfig,
    target: PreparedTarget,
    system_name: str,
    output_dir: str = "/tmp/run_stats",
) -> dict:
    """Run one microbenchmark against a target the embedding application
    provisioned: the untimed setup phase (when the task asks for setup
    branches), the timed phase, and the result files under ``output_dir``.
    ``system_name`` is recorded in the summary. Returns the metrics."""
    register_all_operations()
    config = BenchmarkConfig(task)
    os.makedirs(output_dir, exist_ok=True)
    result_collector = rc.ResultCollector(
        run_id=config.run_id, output_dir=output_dir
    )

    state = ExecutionState(target.root_ref)
    if config.num_branches > 0:
        state = SetupPhase(config, target, state, output_dir).execute()

    metrics = BenchmarkExecutor(config, target, state, result_collector).execute()
    ResultManager(
        config, target, system_name, result_collector, metrics, output_dir
    ).write_results()
    return metrics


def main(argv: list[str] | None = None) -> dict:
    """Prepare the configured database, run one benchmark, and clean up."""
    parser = argparse.ArgumentParser(description="Run database microbenchmarks from task2 config.")
    parser.add_argument("--config", required=True, help="TaskConfig textproto")
    parser.add_argument("--output-dir", default="/tmp/run_stats")
    parser.add_argument("--backend", help="Database integration (overrides the config backend)")
    parser.add_argument("--db-name", help="Database name (overrides database_setup.db_name)")
    args = parser.parse_args(argv)
    task = tp.TaskConfig()
    try:
        text_format.Parse(Path(args.config).read_text(), task)
        register_all_operations()
        BenchmarkConfig(task)
    except (OSError, text_format.ParseError, ValueError) as error:
        parser.error(str(error))
    if not args.backend and not task.HasField("backend"):
        parser.error("set --backend or the config backend explicitly")
    backend = args.backend or tp.Backend.Name(task.backend)
    if args.db_name is not None:
        task.database_setup.db_name = args.db_name
    with prepared_database(backend.lower(), task.database_setup) as target:
        return run(task, target, backend.upper(), args.output_dir)


if __name__ == "__main__":
    main()
