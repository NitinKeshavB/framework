"""One writer for the registry. Auto Loader calls this from the collector job."""

from __future__ import annotations

import json
import re
import time
from typing import Any, Callable

from lakeflow_framework.metadata import (
    LEDGER_COLUMNS,
    OBJECT_COLUMNS,
    OWNED_KINDS,
    current_objects_sql,
    ddl_statements,
    ingest_steps,
    new_object_rows,
    owners_sql,
    ownership_conflicts,
    recorded_versions_sql,
    registered_sql,
    split_payloads,
    step_key,
    step_keys_sql,
    step_payload,
)
from lakeflow_framework.pipeline import MetadataNotFound


def apply_ddl(spark, catalog: str, text: str | None = None) -> None:
    """Apply registry DDL. ``text`` is the bundle's SQL file when the job runs on a cluster."""

    for statement in ddl_statements(catalog, text):
        if statement.upper().startswith("CREATE BLOOMFILTER"):
            try:
                spark.sql(statement)
            except Exception:
                continue
            continue
        spark.sql(statement)


def decode_events(texts: list[str]) -> list[dict]:
    """Turn the text of event files into payloads. A file that is not a JSON object is skipped.

    Files are read whole and parsed here, so the collector needs no inferred schema: step files
    and definition files have different fields, and a new field must not stop the stream.
    One unreadable file must not block every file behind it either.
    """

    payloads: list[dict] = []
    for text in texts:
        try:
            payload = json.loads(text)
        except (TypeError, ValueError):
            print(f"skipped an event file that is not JSON: {str(text)[:80]!r}")
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
        else:
            print("skipped an event file that is not a JSON object")
    return payloads


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
    _merge_current(
        spark,
        _table(catalog, "run_current"),
        rows,
        LEDGER_COLUMNS,
        ("owner_kind", "owner", "object_kind", "full_name"),
        timestamp_columns=("started_at", "ended_at", "recorded_at"),
        date_columns=("event_date",),
        int_columns=("step_no",),
        newer_than="recorded_at",
    )
    if seen is not None:
        seen.update(staged)
    return written


def append_definitions(spark, rows: list[dict], seen: set | None = None, catalog: str = "") -> int:
    held = seen if seen is not None else set()
    # Copy so a failed insert does not mark this batch as the current version.
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
    # History is checked by the exact version, not only by the current hash. A retry after the
    # insert has landed and the current merge has not still repairs object_current, and does not
    # append the same version again.
    to_insert = fresh
    if catalog:
        recorded = _recorded_versions(spark, catalog, fresh)
        to_insert = [
            row
            for row in fresh
            if (
                row["object_kind"],
                row["full_name"],
                row["content_hash"],
                str(row["recorded_at"]),
            )
            not in recorded
        ]
    written = 0
    if to_insert:
        written = _insert(
            spark,
            _table(catalog, "object_registry"),
            to_insert,
            OBJECT_COLUMNS,
            timestamp_columns=("recorded_at",),
        )
    _merge_current(
        spark,
        _table(catalog, "object_current"),
        fresh,
        OBJECT_COLUMNS,
        ("object_kind", "full_name"),
        timestamp_columns=("recorded_at",),
    )
    for row in fresh:
        held.difference_update(
            key for key in list(held) if key[0] == row["object_kind"] and key[1] == row["full_name"]
        )
        held.add((row["object_kind"], row["full_name"], row["content_hash"]))
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


def _refuse_other_owners(spark, catalog: str, rows: list[dict]) -> list[dict]:
    """Drop definitions for a table or pipeline another owner already holds.

    A refused definition is not inserted. It is recorded as a FAILED ownership_check step in the
    run ledger, so it shows in the run views and the collector keeps running. An unsafe name
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


def _recorded_versions(spark, catalog: str, rows: list[dict]) -> set[tuple[str, str, str, str]]:
    found: set[tuple[str, str, str, str]] = set()
    for row in _rows(spark.sql(recorded_versions_sql(catalog, rows))):
        found.add(
            (
                str(row["object_kind"]),
                str(row["full_name"]),
                str(row["content_hash"]),
                str(row["recorded_at"]),
            )
        )
    return found


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
) -> None:
    """Replace the one current row for each key. History tables are not updated."""

    if not rows or spark is None or not hasattr(spark, "createDataFrame"):
        return
    from pyspark.sql.types import StringType, StructField, StructType

    string_rows = [
        {column: None if row.get(column) is None else str(row.get(column)) for column in columns}
        for row in rows
    ]
    schema = StructType([StructField(column, StringType(), True) for column in columns])
    spark.createDataFrame(string_rows, schema).createOrReplaceTempView("_framework_current")
    select = ", ".join(
        _cast(column, timestamp_columns, date_columns, int_columns) for column in columns
    )
    assignments = ", ".join(f"t.{column} = s.{column}" for column in columns)
    inserted = ", ".join(columns)
    values = ", ".join(f"s.{column}" for column in columns)
    matched = ""
    if newer_than:
        matched = f" AND s.{newer_than} >= t.{newer_than}"
    on = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    spark.sql(
        f"MERGE INTO {table} t "
        f"USING (SELECT {select} FROM _framework_current) s "
        f"ON {on} "
        f"WHEN MATCHED{matched} THEN UPDATE SET {assignments} "
        f"WHEN NOT MATCHED THEN INSERT ({inserted}) VALUES ({values})"
    )


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
        # The framework writes UTC timestamps as 2026-11-09T00:00:00Z. CAST alone drops the T/Z form
        # on some runtimes and stores NULL, which breaks history ordering.
        return (
            f"COALESCE(to_timestamp({column}, 'yyyy-MM-dd''T''HH:mm:ss''Z'''), "
            f"CAST({column} AS TIMESTAMP)) AS {column}"
        )
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


def _event_time(column: str) -> str:
    return (
        f"COALESCE(to_timestamp({column}, 'yyyy-MM-dd''T''HH:mm:ss''Z'''), "
        f"CAST({column} AS TIMESTAMP))"
    )


def event_statements(catalog: str) -> list[str]:
    """Land one Auto Loader batch in Spark.

    The driver never collects the batch. History is insert-only and checked for two days.
    Current tables are merged so a status read is one row.
    """

    if not re.fullmatch(r"[A-Za-z0-9_-]+", catalog or ""):
        raise ValueError("catalog is required")
    meta = f"{catalog}.framework_meta"
    recorded = _event_time("e.recorded_at")
    started = _event_time("e.started_at")
    ended = _event_time("e.ended_at")
    owned = ", ".join(f"'{kind}'" for kind in sorted(OWNED_KINDS))
    latest_step = (
        "SELECT * FROM _framework_events WHERE kind = 'step' "
        "QUALIFY row_number() OVER ("
        "PARTITION BY run_id, step_no, status ORDER BY recorded_at DESC) = 1"
    )
    current_step = (
        "SELECT * FROM _framework_events WHERE kind = 'step' "
        "QUALIFY row_number() OVER ("
        "PARTITION BY owner_kind, owner, object_kind, full_name "
        "ORDER BY recorded_at DESC) = 1"
    )
    latest_definition = (
        "SELECT * FROM _framework_events WHERE kind = 'definition' "
        "QUALIFY row_number() OVER ("
        "PARTITION BY object_kind, full_name ORDER BY recorded_at DESC) = 1"
    )
    step_columns = (
        f"e.run_id, e.parent_run_id, e.object_kind, e.full_name, e.owner_kind, e.owner, "
        f"CAST(e.step_no AS INT), e.step_name, e.status, {started}, {ended}, "
        f"e.failure_reason, e.recon_json, {recorded}, "
        "CAST(substr(e.recorded_at, 1, 10) AS DATE)"
    )
    ledger_insert = (
        f"INSERT INTO {meta}.run_ledger "
        f"SELECT {step_columns} FROM ({latest_step}) e "
        f"WHERE NOT EXISTS ("
        f"SELECT 1 FROM {meta}.run_ledger r "
        f"WHERE r.run_id = e.run_id AND r.step_no = CAST(e.step_no AS INT) "
        f"AND r.status = e.status AND r.event_date >= date_sub(current_date(), 2))"
    )
    step_select = (
        f"SELECT e.run_id, e.parent_run_id, e.object_kind, e.full_name, e.owner_kind, e.owner, "
        f"CAST(e.step_no AS INT) AS step_no, e.step_name, e.status, "
        f"{started} AS started_at, {ended} AS ended_at, e.failure_reason, e.recon_json, "
        f"{recorded} AS recorded_at, CAST(substr(e.recorded_at, 1, 10) AS DATE) AS event_date "
        f"FROM ({current_step}) e"
    )
    run_assignments = ", ".join(f"t.{column} = s.{column}" for column in LEDGER_COLUMNS)
    run_values = ", ".join(f"s.{column}" for column in LEDGER_COLUMNS)
    run_merge = (
        f"MERGE INTO {meta}.run_current t USING ({step_select}) s "
        f"ON t.owner_kind = s.owner_kind AND t.owner = s.owner "
        f"AND t.object_kind = s.object_kind AND t.full_name = s.full_name "
        f"WHEN MATCHED AND s.recorded_at >= t.recorded_at THEN UPDATE SET {run_assignments} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(LEDGER_COLUMNS)}) VALUES ({run_values})"
    )
    ownership = (
        f"INSERT INTO {meta}.run_ledger "
        f"SELECT concat('ownership-', substr(e.content_hash, 1, 16)), "
        f"CAST(NULL AS STRING), e.object_kind, "
        f"e.full_name, e.owner_kind, e.owner, 1, 'ownership_check', 'FAILED', "
        f"{recorded}, {recorded}, "
        f"concat(e.full_name, ' is owned by ', c.owner_kind, ' ', c.owner, '; ', "
        f"e.owner_kind, ' ', e.owner, ' cannot register it'), "
        f"CAST(NULL AS STRING), {recorded}, CAST(substr(e.recorded_at, 1, 10) AS DATE) "
        f"FROM ({latest_definition}) e "
        f"JOIN {meta}.object_current c "
        f"ON c.object_kind = e.object_kind AND c.full_name = e.full_name "
        f"WHERE e.object_kind IN ({owned}) "
        f"AND (c.owner_kind <> e.owner_kind OR c.owner <> e.owner) "
        f"AND NOT EXISTS ("
        f"SELECT 1 FROM {meta}.run_ledger r "
        f"WHERE r.run_id = concat('ownership-', substr(e.content_hash, 1, 16)) "
        f"AND r.step_name = 'ownership_check' "
        f"AND r.event_date >= date_sub(current_date(), 2))"
    )
    ownership_reason = (
        "concat(e.full_name, ' is owned by ', c.owner_kind, ' ', c.owner, '; ', "
        "e.owner_kind, ' ', e.owner, ' cannot register it')"
    )
    ownership_current = (
        f"MERGE INTO {meta}.run_current t USING ("
        f"SELECT concat('ownership-', substr(e.content_hash, 1, 16)) AS run_id, "
        f"CAST(NULL AS STRING) AS parent_run_id, e.object_kind, e.full_name, e.owner_kind, "
        f"e.owner, CAST(1 AS INT) AS step_no, 'ownership_check' AS step_name, 'FAILED' AS status, "
        f"{recorded} AS started_at, {recorded} AS ended_at, {ownership_reason} AS failure_reason, "
        f"CAST(NULL AS STRING) AS recon_json, {recorded} AS recorded_at, "
        f"CAST(substr(e.recorded_at, 1, 10) AS DATE) AS event_date "
        f"FROM ({latest_definition}) e "
        f"JOIN {meta}.object_current c "
        f"ON c.object_kind = e.object_kind AND c.full_name = e.full_name "
        f"WHERE e.object_kind IN ({owned}) "
        f"AND (c.owner_kind <> e.owner_kind OR c.owner <> e.owner)"
        f") s ON t.owner_kind = s.owner_kind AND t.owner = s.owner "
        f"AND t.object_kind = s.object_kind AND t.full_name = s.full_name "
        f"WHEN MATCHED AND s.recorded_at >= t.recorded_at THEN UPDATE SET {run_assignments} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(LEDGER_COLUMNS)}) VALUES ({run_values})"
    )
    accepted = (
        f"SELECT e.object_kind, e.full_name, e.owner_kind, e.owner, e.requested_by, e.delivery, "
        f"e.parent_full_name, e.relation, e.config_json, e.content_hash, e.source, "
        f"{recorded} AS recorded_at "
        f"FROM ({latest_definition}) e "
        f"LEFT JOIN {meta}.object_current c "
        f"ON c.object_kind = e.object_kind AND c.full_name = e.full_name "
        f"WHERE (c.full_name IS NULL OR (c.content_hash <> e.content_hash "
        f"AND {recorded} >= c.recorded_at)) "
        f"AND NOT (e.object_kind IN ({owned}) AND c.full_name IS NOT NULL "
        f"AND (c.owner_kind <> e.owner_kind OR c.owner <> e.owner))"
    )
    object_insert = (
        f"INSERT INTO {meta}.object_registry "
        f"SELECT object_kind, full_name, owner_kind, owner, requested_by, delivery, "
        f"parent_full_name, relation, config_json, content_hash, source, recorded_at "
        f"FROM ({accepted}) a "
        f"WHERE NOT EXISTS ("
        f"SELECT 1 FROM {meta}.object_registry h "
        f"WHERE h.owner = a.owner AND h.object_kind = a.object_kind "
        f"AND h.full_name = a.full_name AND h.content_hash = a.content_hash "
        f"AND h.recorded_at = a.recorded_at)"
    )
    object_assignments = ", ".join(f"t.{column} = s.{column}" for column in OBJECT_COLUMNS)
    object_values = ", ".join(f"s.{column}" for column in OBJECT_COLUMNS)
    object_merge = (
        f"MERGE INTO {meta}.object_current t USING ({accepted}) s "
        f"ON t.object_kind = s.object_kind AND t.full_name = s.full_name "
        f"WHEN MATCHED AND s.recorded_at >= t.recorded_at THEN UPDATE SET {object_assignments} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(OBJECT_COLUMNS)}) VALUES ({object_values})"
    )
    return [
        ledger_insert,
        run_merge,
        ownership,
        ownership_current,
        object_insert,
        object_merge,
    ]


def land_events(spark, catalog: str) -> None:
    """Apply one micro-batch that is already registered as _framework_events."""

    for statement in event_statements(catalog):
        spark.sql(statement)
