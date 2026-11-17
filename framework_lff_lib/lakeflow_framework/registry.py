"""The registry writer. registry_sync calls it at deploy; framework_ddl calls apply_ddl."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Mapping

from lakeflow_framework.metadata import (
    LEDGER_COLUMNS,
    OBJECT_COLUMNS,
    OWNER_KINDS,
    SCD_COLUMNS,
    current_objects_sql,
    ddl_statements,
    ingest_steps,
    new_object_rows,
    owners_sql,
    ownership_conflicts,
    step_key,
    step_keys_sql,
    step_payload,
)

RUN_KEYS = ("owner_kind", "owner", "object_kind", "full_name")
OBJECT_KEYS = ("object_kind", "full_name")
# Columns the writer computes from the staged columns.
OBJECT_DERIVED = {"config": "parse_json(config_json)"}
OBJECT_TABLE_COLUMNS = (*OBJECT_COLUMNS, *OBJECT_DERIVED)
_STEP_TYPES = {
    "timestamp_columns": ("started_at", "ended_at", "recorded_at"),
    "date_columns": ("event_date",),
    "int_columns": ("step_no",),
}


def apply_ddl(spark, catalog: str, text: str | None = None, force: bool = False) -> int:
    """Apply registry DDL. ``text`` is the bundle's SQL file when the job runs on a cluster.

    Every statement is idempotent, so running them again changes nothing, but it takes minutes.
    The hash of the SQL last applied is kept in framework_meta.ddl_applied, and a run whose SQL
    is already applied stops after that one lookup. Returns the number of statements run.
    """

    statements = ddl_statements(catalog, text)
    digest = ddl_hash(statements)
    if not force and _ddl_applied(spark, catalog, digest):
        return 0
    for statement in statements:
        spark.sql(statement)
    spark.sql(
        f"INSERT INTO {_table(catalog, 'ddl_applied')} (sql_hash, applied_at) "
        f"VALUES ('{digest}', current_timestamp())"
    )
    return len(statements)


def ddl_hash(statements: list[str]) -> str:
    return hashlib.sha256("\n;\n".join(statements).encode("utf-8")).hexdigest()


def _ddl_applied(spark, catalog: str, digest: str) -> bool:
    try:
        found = _rows(
            spark.sql(
                f"SELECT 1 FROM {_table(catalog, 'ddl_applied')} "
                f"WHERE sql_hash = '{digest}' LIMIT 1"
            )
        )
    except Exception:
        # The first run, before framework_ddl has created the table.
        return False
    return bool(found)


def append_steps(spark, payloads: list[dict], seen: set | None = None, catalog: str = "") -> int:
    candidates = [payload for payload in payloads if seen is None or step_key(payload) not in seen]
    if catalog and candidates:
        already = _existing_step_keys(spark, catalog, candidates)
        candidates = [payload for payload in candidates if step_key(payload) not in already]
    staged: set[tuple[str, int, str]] = set()
    rows = ingest_steps(candidates, staged)
    if not rows:
        return 0
    written = _insert(spark, _table(catalog, "run_ledger"), rows, LEDGER_COLUMNS, **_STEP_TYPES)
    _merge_current(
        spark,
        _table(catalog, "run_current"),
        rows,
        LEDGER_COLUMNS,
        RUN_KEYS,
        newer_than="recorded_at",
        **_STEP_TYPES,
    )
    if seen is not None:
        seen.update(staged)
    return written


def append_definitions(spark, rows: list[dict], seen: set | None = None, catalog: str = "") -> int:
    """Write new and changed definitions as SCD Type 2 versions.

    A changed object's current version is closed (``valid_to``, ``is_current = false``) and the
    new version is inserted in the same MERGE, so a retry cannot leave history half written.
    """

    held = seen if seen is not None else set()
    # Copy so a failed write does not mark this batch as the current version.
    fresh = new_object_rows(rows, set(held))
    if catalog and fresh:
        # Ownership comes first: another owner's identical definition must be refused and
        # recorded, not skipped as already current.
        fresh = _refuse_other_owners(spark, catalog, fresh)
    if catalog and fresh:
        current = _current_object_hashes(spark, catalog, fresh)
        fresh = [
            row
            for row in fresh
            if current.get((row["object_kind"], row["full_name"])) != row["content_hash"]
        ]
    if not fresh:
        return 0
    written = _scd2_merge(spark, _table(catalog, "object_registry"), fresh)
    _merge_current(
        spark,
        _table(catalog, "object_current"),
        fresh,
        OBJECT_COLUMNS,
        OBJECT_KEYS,
        timestamp_columns=("recorded_at",),
        derived=OBJECT_DERIVED,
    )
    for row in fresh:
        held.difference_update(
            key for key in list(held) if key[0] == row["object_kind"] and key[1] == row["full_name"]
        )
        held.add((row["object_kind"], row["full_name"], row["content_hash"]))
    return written


def retire_definitions(
    spark, deployed: list[dict], catalog: str, owner_kind: str = "source_system"
) -> None:
    """Close every current version of ``owner_kind`` that this deploy no longer defines.

    Each retired object gets a deletion version (``is_deleted = true``) and leaves
    object_current, so its name is free and v_object_current no longer lists it. A deploy with
    no definitions retires nothing: an empty bundle is a mistake, not a removal.
    """

    if owner_kind not in OWNER_KINDS:
        raise ValueError(f"owner_kind must be one of {', '.join(OWNER_KINDS)}")
    keys = sorted({(str(row["object_kind"]), str(row["full_name"])) for row in deployed})
    if not keys:
        raise ValueError("a deploy with no definitions cannot retire anything")
    if spark is None or not hasattr(spark, "createDataFrame"):
        return
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField(key, StringType(), False) for key in OBJECT_KEYS])
    rows = [dict(zip(OBJECT_KEYS, key)) for key in keys]
    spark.createDataFrame(rows, schema).createOrReplaceTempView("_framework_deployed")
    for statement in retire_statements(catalog, "_framework_deployed", owner_kind):
        spark.sql(statement)


def scd2_merge_sql(table: str, staged: str) -> str:
    """One MERGE that closes changed or deleted current versions and inserts the new ones.

    Each staged row appears with its key, to close the current version, and, when that
    version differs, again without a key, so it is inserted as the new current version.
    """

    key = "concat(object_kind, '|', full_name)"
    columns = OBJECT_TABLE_COLUMNS
    latest = (
        f"SELECT * FROM ({staged}) QUALIFY row_number() OVER "
        "(PARTITION BY object_kind, full_name ORDER BY recorded_at DESC) = 1"
    )
    source = (
        f"SELECT s.*, {key} AS merge_key FROM ({latest}) s "
        f"UNION ALL "
        f"SELECT s.*, NULL AS merge_key FROM ({latest}) s "
        f"JOIN {table} r ON r.object_kind = s.object_kind AND r.full_name = s.full_name "
        f"AND r.is_current "
        f"WHERE r.content_hash <> s.content_hash OR coalesce(r.is_deleted, false)"
    )
    inserted = ", ".join((*columns, *SCD_COLUMNS))
    values = ", ".join(
        [f"u.{column}" for column in columns] + ["u.recorded_at", "NULL", "true", "false"]
    )
    return (
        f"MERGE INTO {table} t USING ({source}) u "
        f"ON concat(t.object_kind, '|', t.full_name) = u.merge_key AND t.is_current "
        f"WHEN MATCHED AND (t.content_hash <> u.content_hash OR coalesce(t.is_deleted, false)) "
        f"THEN UPDATE SET t.valid_to = u.recorded_at, t.is_current = false "
        f"WHEN NOT MATCHED THEN INSERT ({inserted}) VALUES ({values})"
    )


def retire_statements(catalog: str, deployed: str, owner_kind: str) -> list[str]:
    """SQL that retires current objects of ``owner_kind`` missing from the ``deployed`` view."""

    if not re.fullmatch(r"[A-Za-z0-9_-]+", catalog or ""):
        raise ValueError("catalog is required")
    registry = _table(catalog, "object_registry")
    current = _table(catalog, "object_current")
    gone = (
        f"SELECT r.* FROM {registry} r "
        f"LEFT ANTI JOIN {deployed} d ON d.object_kind = r.object_kind AND d.full_name = r.full_name "
        f"WHERE r.is_current AND NOT coalesce(r.is_deleted, false) "
        f"AND r.owner_kind = '{owner_kind}'"
    )
    marked = (
        f"SELECT g.* EXCEPT (recorded_at, source, valid_from, valid_to, is_current, is_deleted), "
        f"'framework' AS source, current_timestamp() AS recorded_at FROM ({gone}) g"
    )
    key = "concat(object_kind, '|', full_name)"
    columns = OBJECT_TABLE_COLUMNS
    inserted = ", ".join((*columns, *SCD_COLUMNS))
    values = ", ".join(
        [f"u.{column}" for column in columns] + ["u.recorded_at", "NULL", "true", "true"]
    )
    close_and_mark = (
        f"MERGE INTO {registry} t USING ("
        f"SELECT m.*, {key} AS merge_key FROM ({marked}) m "
        f"UNION ALL SELECT m.*, NULL AS merge_key FROM ({marked}) m"
        f") u ON concat(t.object_kind, '|', t.full_name) = u.merge_key AND t.is_current "
        f"WHEN MATCHED THEN UPDATE SET t.valid_to = u.recorded_at, t.is_current = false "
        f"WHEN NOT MATCHED THEN INSERT ({inserted}) VALUES ({values})"
    )
    leave_current = (
        f"DELETE FROM {current} c WHERE c.owner_kind = '{owner_kind}' "
        f"AND NOT EXISTS (SELECT 1 FROM {deployed} d "
        f"WHERE d.object_kind = c.object_kind AND d.full_name = c.full_name)"
    )
    return [close_and_mark, leave_current]


def _refuse_other_owners(spark, catalog: str, rows: list[dict]) -> list[dict]:
    """Drop definitions for a table or pipeline another owner already holds.

    A refused definition is not inserted. It is recorded as a FAILED ownership_check step in the
    run ledger, so it shows in v_ownership_refusals and the deploy writes the rest. An unsafe name
    raises instead of skipping the check: skipping it would insert the row.
    """

    statement = owners_sql(catalog, [str(row["full_name"]) for row in rows])
    current = {
        (str(row["object_kind"]), str(row["full_name"])): (
            str(row["owner_kind"]),
            str(row["owner"]),
        )
        for row in _rows(spark.sql(statement))
    }
    accepted, refused = ownership_conflicts(rows, current)
    if refused:
        steps = []
        for row in refused:
            holder = current[(str(row["object_kind"]), str(row["full_name"]))]
            steps.append(
                step_payload(
                    owner_kind=str(row["owner_kind"]),
                    owner=str(row["owner"]),
                    full_name=str(row["full_name"]),
                    object_kind=str(row["object_kind"]),
                    run_id=f"ownership-{str(row['content_hash'])[:16]}",
                    step_no=1,
                    step_name="ownership_check",
                    status="FAILED",
                    failure_reason=(
                        f"{row['full_name']} is owned by {holder[0]} {holder[1]}; "
                        f"{row['owner_kind']} {row['owner']} cannot register it"
                    ),
                )
            )
        append_steps(spark, steps, None, catalog)
    return accepted


def _table(catalog: str, name: str) -> str:
    return f"{catalog}.framework_meta.{name}" if catalog else f"framework_meta.{name}"


def _existing_step_keys(spark, catalog: str, payloads: list[dict]) -> set[tuple[str, int, str]]:
    recorded = [str(payload.get("recorded_at") or "") for payload in payloads]
    dates = [value[:10] for value in recorded if len(value) >= 10]
    statement = step_keys_sql(
        catalog,
        [str(payload["run_id"]) for payload in payloads],
        owners=[str(payload.get("owner") or "") for payload in payloads],
        event_dates=dates if len(dates) == len(payloads) else None,
    )
    return {
        (str(row["run_id"]), int(row["step_no"]), str(row["status"]))
        for row in _rows(spark.sql(statement))
    }


def _current_object_hashes(spark, catalog: str, rows: list[dict]) -> dict[tuple[str, str], str]:
    # A failed lookup must not look like "nothing stored", or the batch is appended again.
    statement = current_objects_sql(catalog, rows)
    return {
        (str(row["object_kind"]), str(row["full_name"])): str(row["content_hash"])
        for row in _rows(spark.sql(statement))
    }


def _merge_current(
    spark,
    table: str,
    rows: list[dict],
    columns: tuple[str, ...],
    keys: tuple[str, ...],
    timestamp_columns: tuple[str, ...] = (),
    date_columns: tuple[str, ...] = (),
    int_columns: tuple[str, ...] = (),
    newer_than: str | None = None,
    derived: Mapping[str, str] | None = None,
) -> None:
    """Replace the one current row for each key. History tables are not updated."""

    if not rows or spark is None or not hasattr(spark, "createDataFrame"):
        return
    staged = _stage(
        spark,
        "_framework_current",
        rows,
        columns,
        timestamp_columns,
        date_columns,
        int_columns,
        derived or {},
    )
    written = (*columns, *(derived or {}))
    spark.sql(_merge_sql(table, staged, keys, written, newer_than))


def _scd2_merge(spark, table: str, rows: list[dict]) -> int:
    if spark is None or not hasattr(spark, "createDataFrame"):
        return 0
    staged = _stage(
        spark,
        "_framework_append",
        rows,
        OBJECT_COLUMNS,
        ("recorded_at",),
        (),
        (),
        OBJECT_DERIVED,
    )
    spark.sql(scd2_merge_sql(table, staged))
    return len(rows)


def _insert(
    spark,
    table: str,
    rows: list[dict],
    columns: tuple[str, ...],
    timestamp_columns: tuple[str, ...] = (),
    date_columns: tuple[str, ...] = (),
    int_columns: tuple[str, ...] = (),
    derived: Mapping[str, str] | None = None,
) -> int:
    staged = _stage(
        spark,
        "_framework_append",
        rows,
        columns,
        timestamp_columns,
        date_columns,
        int_columns,
        derived or {},
    )
    written = ", ".join((*columns, *(derived or {})))
    spark.sql(f"INSERT INTO {table} ({written}) {staged}")
    return len(rows)


def _stage(
    spark,
    view: str,
    rows: list[dict],
    columns: tuple[str, ...],
    timestamp_columns: tuple[str, ...],
    date_columns: tuple[str, ...],
    int_columns: tuple[str, ...],
    derived: Mapping[str, str],
) -> str:
    """Register ``rows`` as a string-typed view and return the SELECT that types them."""

    from pyspark.sql.types import StringType, StructField, StructType

    string_rows = [
        {column: None if row.get(column) is None else str(row.get(column)) for column in columns}
        for row in rows
    ]
    schema = StructType([StructField(column, StringType(), True) for column in columns])
    spark.createDataFrame(string_rows, schema).createOrReplaceTempView(view)
    select = ", ".join(
        [_cast(column, timestamp_columns, date_columns, int_columns) for column in columns]
        + [f"{expression} AS {column}" for column, expression in derived.items()]
    )
    return f"SELECT {select} FROM {view}"


def _merge_sql(
    table: str,
    source: str,
    keys: tuple[str, ...],
    columns: tuple[str, ...],
    newer_than: str | None = None,
) -> str:
    on = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    matched = f" AND s.{newer_than} >= t.{newer_than}" if newer_than else ""
    assignments = ", ".join(f"t.{column} = s.{column}" for column in columns)
    values = ", ".join(f"s.{column}" for column in columns)
    return (
        f"MERGE INTO {table} t USING ({source}) s ON {on} "
        f"WHEN MATCHED{matched} THEN UPDATE SET {assignments} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({values})"
    )


def _cast(
    column: str,
    timestamp_columns: tuple[str, ...],
    date_columns: tuple[str, ...],
    int_columns: tuple[str, ...],
) -> str:
    if column in timestamp_columns:
        return f"{_event_time(column)} AS {column}"
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


def _event_time(column: str) -> str:
    # The framework writes UTC timestamps as 2026-11-09T00:00:00Z. CAST alone drops the T/Z form
    # on some runtimes and stores NULL, which breaks history ordering.
    return (
        f"COALESCE(to_timestamp({column}, 'yyyy-MM-dd''T''HH:mm:ss''Z'''), "
        f"CAST({column} AS TIMESTAMP))"
    )
