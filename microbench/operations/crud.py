"""
CRUD (Create, Read, Update, Delete) operations for benchmarking.

This module implements the standard database CRUD operations including
point operations (single row) and range operations (multiple rows).
"""

from typing import TYPE_CHECKING

from dblib import result_pb2 as rslt
from microbench.operations.base import Operation
from util import db_helpers as dbh

if TYPE_CHECKING:
    from microbench.runner2 import WorkerContext


class ReadOperation(Operation):
    """Read a single row by primary key.

    Selects a random existing primary key and performs a SELECT operation.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def _prepare_read(self, context: 'WorkerContext'):
        """Shared logic: prepare the read query and parameters.

        Returns:
            Tuple of (sql_query, params, key_to_read)
        """
        # Select a random existing key
        key_to_read = context.select_random_key(self.table_name)
        if not key_to_read:
            raise ValueError("No existing keys found for read operation")

        # Build the SELECT query
        pk_columns = context.get_pk_columns(self.table_name)
        where_clause = " AND ".join([f"{pk_name} = %s" for pk_name in pk_columns])
        select_sql = f"SELECT * FROM {self.table_name} WHERE {where_clause};"

        # Record that we're touching 1 key
        context.record_keys_touched(1)

        return (select_sql, key_to_read)

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed point read operation."""
        select_sql, key_to_read = self._prepare_read(context)

        # Execute the timed read
        result = context.run_sql(select_sql, key_to_read)
        if not result:
            raise ValueError("Read operation returned no results")

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version using shared preparation logic."""
        select_sql, key_to_read = self._prepare_read(context)

        # Execute the timed read asynchronously
        result = await context.run_sql_async(select_sql, key_to_read)
        if not result:
            raise ValueError("Read operation returned no results")

    def requires_setup_data(self) -> bool:
        return True  # Needs existing rows to read from

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.READ


class InsertOperation(Operation):
    """Insert a new row into the table.

    Generates a new row with fake data and inserts it. A single integer key
    counts up from the table's maximum, so it never collides; any other key
    shape is retried a few times on a collision.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def _generate_insert_data(self, context: 'WorkerContext'):
        """Shared logic: generate insert SQL and row data.

        Returns:
            Tuple of (sql_query, col_names, pk_columns)
        """
        col_names = dbh.get_all_columns(
            context.db_tools.get_current_connection(), self.table_name
        )
        pk_columns = context.get_pk_columns(self.table_name)

        placeholders = ", ".join([f"%({name})s" for name in col_names])
        insert_sql = f"INSERT INTO {self.table_name} ({', '.join(col_names)}) VALUES ({placeholders});"

        # Record that we're touching 1 key
        context.record_keys_touched(1)

        return (insert_sql, col_names, pk_columns)

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed insert operation."""
        insert_sql, col_names, pk_columns = self._generate_insert_data(context)

        # Try to insert with retries for PK collisions
        inserted = False
        for attempt in range(5):
            row_data = context.generate_row(self.table_name)
            if len(pk_columns) == 1 and isinstance(row_data[pk_columns[0]], int):
                row_data[pk_columns[0]] = context.next_insert_key(self.table_name, pk_columns[0])
            pk_tuple = tuple(row_data[pk] for pk in pk_columns)

            try:
                context.run_sql(insert_sql, row_data)
                context.track_modified_key(pk_tuple)
                inserted = True

                break
            except Exception as e:
                if attempt == 4:  # Last attempt
                    raise
                continue  # Retry with new data

        if not inserted:
            raise ValueError("Failed to insert row after 5 attempts")

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version using shared preparation logic."""
        insert_sql, col_names, pk_columns = self._generate_insert_data(context)

        # Try to insert with retries for PK collisions
        inserted = False
        for attempt in range(5):
            row_data = context.generate_row(self.table_name)
            if len(pk_columns) == 1 and isinstance(row_data[pk_columns[0]], int):
                row_data[pk_columns[0]] = await context.next_insert_key_async(self.table_name, pk_columns[0])
            pk_tuple = tuple(row_data[pk] for pk in pk_columns)

            try:
                await context.run_sql_async(insert_sql, row_data)
                context.track_modified_key(pk_tuple)
                inserted = True

                break
            except Exception as e:
                if attempt == 4:  # Last attempt
                    raise
                continue  # Retry with new data

        if not inserted:
            raise ValueError("Failed to insert row after 5 attempts")

    def requires_setup_data(self) -> bool:
        return False  # Can insert without existing data

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.INSERT


class UpdateOperation(Operation):
    """Update an existing row by primary key.

    Selects a random existing primary key and updates all non-PK columns
    with new generated values.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def _prepare_update(self, context: 'WorkerContext'):
        """Shared logic: prepare the update query and parameters.

        Returns:
            Tuple of (sql_query, params, key_to_update)
        """
        # Select a random existing key
        key_to_update = context.select_random_key(self.table_name)
        if not key_to_update:
            raise ValueError("No existing keys found for update operation")

        # Get column information
        pk_columns = context.get_pk_columns(self.table_name)
        all_columns = dbh.get_all_columns(
            context.db_tools.get_current_connection(), self.table_name
        )
        non_pk_columns = [col for col in all_columns if col not in pk_columns]

        if not non_pk_columns:
            raise ValueError(f"No non-PK columns to update in {self.table_name}")

        # Generate new values for non-PK columns
        row_data = context.generate_row(self.table_name)

        # Build UPDATE query
        set_clause = ", ".join([f"{col} = %({col})s" for col in non_pk_columns])
        where_clause = " AND ".join(
            [f"{pk_name} = %({pk_name})s" for pk_name in pk_columns]
        )
        update_sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause};"

        # Add PK values to row_data for WHERE clause
        for i, pk_col in enumerate(pk_columns):
            row_data[pk_col] = key_to_update[i]

        # Record that we're touching 1 key
        context.record_keys_touched(1)

        return (update_sql, row_data, key_to_update)

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed point update operation."""
        update_sql, row_data, key_to_update = self._prepare_update(context)

        # Execute the timed update
        context.run_sql(update_sql, row_data)


        # Track the modified key
        context.track_modified_key(key_to_update)

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version using shared preparation logic."""
        update_sql, row_data, key_to_update = self._prepare_update(context)

        # Execute the timed update asynchronously
        await context.run_sql_async(update_sql, row_data)


        # Track the modified key
        context.track_modified_key(key_to_update)

    def requires_setup_data(self) -> bool:
        return True  # Needs existing rows to update

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.UPDATE


class DeleteOperation(Operation):
    """Delete an existing row by primary key.

    Selects a random existing primary key and deletes the row.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def _prepare_delete(self, context: 'WorkerContext'):
        """Shared logic: prepare the delete query and parameters.

        Returns:
            Tuple of (sql_query, params, key_to_delete)
        """
        # Select a random existing key
        key_to_delete = context.select_random_key(self.table_name)
        if not key_to_delete:
            raise ValueError("No existing keys found for delete operation")

        # Build DELETE query
        pk_columns = context.get_pk_columns(self.table_name)
        where_clause = " AND ".join([f"{pk_name} = %s" for pk_name in pk_columns])
        delete_sql = f"DELETE FROM {self.table_name} WHERE {where_clause};"

        # Record that we're touching 1 key
        context.record_keys_touched(1)

        return (delete_sql, key_to_delete)

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed delete operation."""
        delete_sql, key_to_delete = self._prepare_delete(context)

        # Execute the timed delete
        context.run_sql(delete_sql, key_to_delete)


        # Remove from modified keys tracking
        context.untrack_modified_key(key_to_delete)

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version using shared preparation logic."""
        delete_sql, key_to_delete = self._prepare_delete(context)

        # Execute the timed delete asynchronously
        await context.run_sql_async(delete_sql, key_to_delete)


        # Remove from modified keys tracking
        context.untrack_modified_key(key_to_delete)

    def requires_setup_data(self) -> bool:
        return True  # Needs existing rows to delete

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DELETE_ROWS


class RangeReadOperation(Operation):
    """Read multiple rows using a range query.

    Selects a random range of rows based on primary key ordering and
    performs a range SELECT operation.
    """

    def __init__(self, table_name: str, range_size: int = 10):
        self.table_name = table_name
        self.range_size = range_size

    def _prepare_range_read(self, context: 'WorkerContext'):
        """Shared logic: prepare the range read query.

        Returns:
            Tuple of (sql_query, params, num_keys)
        """
        # Prepare range query components
        range_info = context.prepare_range_query(
            self.table_name, self.range_size, "range read"
        )

        # Build SELECT query with range condition
        select_sql = f"SELECT * FROM {self.table_name} WHERE {range_info['where_clause']};"

        # Record the actual number of keys in the range
        num_keys = len(range_info["keys_in_range"])
        context.record_keys_touched(num_keys)

        return (select_sql, range_info["params"], num_keys)

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed range read operation."""
        select_sql, params, num_keys = self._prepare_range_read(context)
        # Execute the timed range read
        context.run_sql(select_sql, params)

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version using shared preparation logic."""
        select_sql, params, num_keys = self._prepare_range_read(context)
        # Execute the timed range read asynchronously
        await context.run_sql_async(select_sql, params)

    def requires_setup_data(self) -> bool:
        return True  # Needs existing rows to read from

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.READ


class RangeUpdateOperation(Operation):
    """Update multiple rows using a range query.

    Selects a random range of rows based on primary key ordering and
    updates all non-PK columns in that range.
    """

    def __init__(self, table_name: str, range_size: int = 10):
        self.table_name = table_name
        self.range_size = range_size

    def _prepare_range_update(self, context: 'WorkerContext'):
        """Shared logic: prepare the range update query.

        Returns:
            Tuple of (sql_query, params, keys_in_range, num_keys)
        """
        # Prepare range query components
        range_info = context.prepare_range_query(
            self.table_name, self.range_size, "range update"
        )

        # Get column information
        pk_columns = context.get_pk_columns(self.table_name)
        all_columns = dbh.get_all_columns(
            context.db_tools.get_current_connection(), self.table_name
        )
        non_pk_columns = [col for col in all_columns if col not in pk_columns]

        if not non_pk_columns:
            raise ValueError(f"No non-PK columns to update in {self.table_name}")

        # Generate new values for non-PK columns
        row_data = context.generate_row(self.table_name)

        # Build UPDATE query with range condition
        set_clause = ", ".join([f"{col} = %({col})s" for col in non_pk_columns])
        row_data.update(range_info["params"])
        update_sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {range_info['where_clause']};"

        # Record the actual number of keys in the range
        num_keys = len(range_info["keys_in_range"])
        context.record_keys_touched(num_keys)

        return (update_sql, row_data, range_info["keys_in_range"], num_keys)

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed range update operation."""
        update_sql, row_data, keys_in_range, num_keys = self._prepare_range_update(context)

        # Execute the timed range update
        context.run_sql(update_sql, row_data)


        # Track all keys in the range as modified
        for key in keys_in_range:
            context.track_modified_key(key)

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version using shared preparation logic."""
        update_sql, row_data, keys_in_range, num_keys = self._prepare_range_update(context)

        # Execute the timed range update asynchronously
        await context.run_sql_async(update_sql, row_data)


        # Track all keys in the range as modified
        for key in keys_in_range:
            context.track_modified_key(key)

    def requires_setup_data(self) -> bool:
        return True  # Needs existing rows to update

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.UPDATE
