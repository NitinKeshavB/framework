"""One writer for the registry. Auto Loader calls this from the collector job."""

from __future__ import annotations

import time
from typing import Any, Callable

from lakeflow_framework.metadata import (
    LEDGER_COLUMNS,
    OBJECT_COLUMNS,
    ddl_statements,
    ingest_steps,
    new_object_rows,
    object_keys_sql,
    registered_sql,
    split_payloads,
    step_key,
    step_keys_sql,
)
from lakeflow_framework.pipeline import MetadataNotFound


def apply_ddl(spark, catalog: str) -> None:
    for statement in ddl_statements(catalog):
        if statement.upper().startswith("CREATE BLOOMFILTER"):
            try:
                spark.sql(statement)
            except Exception:
                continue
            continue
        spark.sql(statement)


def append_events(
    spark,
    payloads: list[dict],
    seen_steps: set | None = None,
    seen_objects: set | None = None,
    catalog: str = "",
) -> tuple[int, int]:
    steps, definitions = split_payloads(payloads)
    return (
        append_steps(spark, steps, seen_steps, catalog),
        append_definitions(spark, definitions, seen_objects, catalog),
    )


def append_steps(spark, payloads: list[dict], seen: set | None = None, catalog: str = "") -> int:
    candidates = [payload for payload in payloads if seen is None or step_key(payload) not in seen]
    if catalog and candidates:
        already = _existing_step_keys(spark, catalog, candidates)
        candidates = [payload for payload in candidates if step_key(payload) not in already]
    staged: set[tuple[str, int, str]] = set()
    rows = ingest_steps(candidates, staged)
    if not rows:
        return 0
    written = _insert(
        spark,
        _table(catalog, "run_ledger"),
        rows,
        LEDGER_COLUMNS,
        timestamp_columns=("started_at", "ended_at", "recorded_at"),
        date_columns=("event_date",),
        int_columns=("step_no",),
    )
    if seen is not None:
        seen.update(staged)
    return written


def append_definitions(spark, rows: list[dict], seen: set | None = None, catalog: str = "") -> int:
    held = seen if seen is not None else set()
    fresh = new_object_rows(rows, set())
    if catalog and fresh:
        already = _existing_object_keys(spark, catalog, fresh)
        fresh = [
            row
            for row in fresh
            if (row["object_kind"], row["full_name"], row["content_hash"]) not in already
        ]
    fresh = [
        row
        for row in fresh
        if (row["object_kind"], row["full_name"], row["content_hash"]) not in held
    ]
    if not fresh:
        return 0
    written = _insert(
        spark,
        _table(catalog, "object_registry"),
        fresh,
        OBJECT_COLUMNS,
        timestamp_columns=("recorded_at",),
    )
    held.update((row["object_kind"], row["full_name"], row["content_hash"]) for row in fresh)
    return written


def await_registered(
    spark,
    pipeline_ids: list[str],
    catalog: str,
    timeout_seconds: float = 300,
    pause_seconds: float = 5,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    pending = list(pipeline_ids)
    deadline = clock() + timeout_seconds
    while pending:
        found = _names(spark.sql(registered_sql(pending, catalog)))
        pending = [pipeline_id for pipeline_id in pending if pipeline_id not in found]
        if not pending:
            return
        if clock() >= deadline:
            raise MetadataNotFound("registry is missing " + ", ".join(pending))
        sleep(pause_seconds)


def _table(catalog: str, name: str) -> str:
    return f"{catalog}.framework_meta.{name}" if catalog else f"framework_meta.{name}"


def _existing_step_keys(spark, catalog: str, payloads: list[dict]) -> set[tuple[str, int, str]]:
    try:
        statement = step_keys_sql(catalog, [str(payload["run_id"]) for payload in payloads])
    except ValueError:
        return set()
    return {
        (str(row["run_id"]), int(row["step_no"]), str(row["status"]))
        for row in _rows(spark.sql(statement))
    }


def _existing_object_keys(spark, catalog: str, rows: list[dict]) -> set[tuple[str, str, str]]:
    try:
        statement = object_keys_sql(catalog, [str(row["content_hash"]) for row in rows])
    except ValueError:
        return set()
    return {
        (str(row["object_kind"]), str(row["full_name"]), str(row["content_hash"]))
        for row in _rows(spark.sql(statement))
    }


def _insert(
    spark,
    table: str,
    rows: list[dict],
    columns: tuple[str, ...],
    timestamp_columns: tuple[str, ...] = (),
    date_columns: tuple[str, ...] = (),
    int_columns: tuple[str, ...] = (),
) -> int:
    from pyspark.sql.types import StringType, StructField, StructType

    string_rows = [
        {column: None if row.get(column) is None else str(row.get(column)) for column in columns}
        for row in rows
    ]
    schema = StructType([StructField(column, StringType(), True) for column in columns])
    spark.createDataFrame(string_rows, schema).createOrReplaceTempView("_framework_append")
    select = ", ".join(
        _cast(column, timestamp_columns, date_columns, int_columns) for column in columns
    )
    spark.sql(f"INSERT INTO {table} SELECT {select} FROM _framework_append")
    return len(rows)


def _cast(
    column: str,
    timestamp_columns: tuple[str, ...],
    date_columns: tuple[str, ...],
    int_columns: tuple[str, ...],
) -> str:
    if column in timestamp_columns:
        return f"CAST({column} AS TIMESTAMP) AS {column}"
    if column in date_columns:
        return f"CAST({column} AS DATE) AS {column}"
    if column in int_columns:
        return f"CAST({column} AS INT) AS {column}"
    return column


def _rows(frame: Any) -> list[dict]:
    collected = frame.collect() if hasattr(frame, "collect") else list(frame)
    rows = []
    for row in collected:
        rows.append(row if isinstance(row, dict) else row.asDict())
    return rows


def _names(frame: Any) -> set[str]:
    return {str(row["full_name"]) for row in _rows(frame)}
