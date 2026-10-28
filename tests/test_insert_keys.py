"""Generated inserts never collide: integer keys count up from the table's maximum, interleaved by worker."""

from types import SimpleNamespace
from unittest import mock

from microbench.operations.crud import InsertOperation
from microbench.runner2 import WorkerContext


def context(thread_id: int, threads: int, top, seen: list) -> WorkerContext:
    ctx = WorkerContext.__new__(WorkerContext)
    ctx.thread_id, ctx.config, ctx._insert_keys = thread_id, SimpleNamespace(num_threads=threads), {}
    ctx.run_sql = lambda sql, params=None, timed=True: seen.append(sql) or [(top,)]
    return ctx


def test_keys_start_above_the_table_maximum_and_interleave_workers() -> None:
    seen = []
    first, second = context(0, 2, 1000, seen), context(1, 2, 1000, seen)
    assert [first.next_insert_key("item", "i_id") for _ in range(3)] == [1002, 1004, 1006]
    assert [second.next_insert_key("item", "i_id") for _ in range(3)] == [1003, 1005, 1007]
    assert seen == ["SELECT MAX(i_id) FROM item"] * 2, "the maximum is read once per worker and table"
    assert context(0, 1, None, []).next_insert_key("item", "i_id") == 1, "an empty table starts at 1"


def test_insert_operation_writes_the_allocated_key() -> None:
    inserted = []
    stub = SimpleNamespace(
        db_tools=SimpleNamespace(get_current_connection=lambda: None),
        generate_row=lambda table: {"i_id": 7, "i_name": "x"},
        get_pk_columns=lambda table: ["i_id"],
        next_insert_key=lambda table, column: 1000 + len(inserted) + 1,
        record_keys_touched=lambda n: None,
        track_modified_key=lambda key: None,
        run_sql=lambda sql, params: inserted.append(params["i_id"]),
    )
    with mock.patch("microbench.operations.crud.dbh.get_all_columns", return_value=["i_id", "i_name"]):
        for _ in range(3):
            InsertOperation("item").execute(stub)
    assert inserted == [1001, 1002, 1003]
