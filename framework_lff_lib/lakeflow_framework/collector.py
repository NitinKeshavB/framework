"""One writer for the registry. Auto Loader calls this from the collector job."""

from __future__ import annotations

import re
import time
from typing import Any, Callable, Mapping

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
    step_key,
    step_keys_sql,
    step_payload,
)
from lakeflow_framework.pipeline import MetadataNotFound

RUN_KEYS = ("owner_kind", "owner", "object_kind", "full_name")
OBJECT_KEYS = ("object_kind", "full_name")
# Columns the collector computes from the event columns rather than reads from an event file.
OBJECT_DERIVED = {"config": "parse_json(config_json)"}
OBJECT_TABLE_COLUMNS = (*OBJECT_COLUMNS, *OBJECT_DERIVED)
_ALREADY_EXISTS = ("FIELD_ALREADY_EXISTS", "already exists")
_STEP_TYPES = {
    "timestamp_columns": ("started_at", "ended_at", "recorded_at"),
    "date_columns": ("event_date",),
    "int_columns": ("step_no",),
}


def apply_ddl(spark, catalog: str, text: str | None = None) -> None:
    """Apply registry DDL. ``text`` is the bundle's SQL file when the job runs on a cluster."""

    for statement in ddl_statements(catalog, text):
        if statement.upper().startswith("CREATE BLOOMFILTER"):
            try:
                spark.sql(statement)
            except Exception:
                continue
            continue
        if _adds_columns(statement):
            # ADD COLUMNS has no IF NOT EXISTS, and a new table already has the column.
            try:
                spark.sql(statement)
            except Exception as exc:
                if not any(marker in str(exc) for marker in _ALREADY_EXISTS):
                    raise
            continue
        spark.sql(statement)


def _adds_columns(statement: str) -> bool:
    return bool(re.match(r"ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMNS?\b", statement, re.IGNORECASE))


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
            derived=OBJECT_DERIVED,
        )
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


def await_registered(
    spark,
    expected: Mapping[str, str],
    catalog: str,
    timeout_seconds: float = 300,
    pause_seconds: float = 5,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Wait until each pipeline's current registry row has the content hash this run synced.

    A row with the right name but an older hash is an outdated spec, so it does not count.
    """

    pending = list(expected)
    deadline = clock() + timeout_seconds
    while pending:
        found = {
            str(row["full_name"]): str(row["content_hash"])
            for row in _rows(spark.sql(registered_sql(pending, catalog)))
        }
        pending = [name for name in pending if found.get(name) != expected[name]]
        if not pending:
            return
        if clock() >= deadline:
            raise MetadataNotFound(
                "registry does not have the synced version of " + ", ".join(pending)
            )
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


def _latest(kind: str, keys: str) -> str:
    return (
        f"SELECT * FROM _framework_events WHERE kind = '{kind}' "
        f"QUALIFY row_number() OVER (PARTITION BY {keys} ORDER BY recorded_at DESC) = 1"
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
    event_date = "CAST(substr(e.recorded_at, 1, 10) AS DATE) AS event_date"
    recent = "r.event_date >= date_sub(current_date(), 2)"
    ledger = ", ".join(LEDGER_COLUMNS)
    owned = ", ".join(f"'{kind}'" for kind in sorted(OWNED_KINDS))
    other_owner = (
        f"e.object_kind IN ({owned}) AND (c.owner_kind <> e.owner_kind OR c.owner <> e.owner)"
    )
    latest_definition = _latest("definition", "object_kind, full_name")

    def steps(source: str) -> str:
        return (
            f"SELECT e.run_id, e.parent_run_id, e.object_kind, e.full_name, e.owner_kind, "
            f"e.owner, CAST(e.step_no AS INT) AS step_no, e.step_name, e.status, "
            f"{_event_time('e.started_at')} AS started_at, "
            f"{_event_time('e.ended_at')} AS ended_at, e.failure_reason, e.recon_json, "
            f"{recorded} AS recorded_at, {event_date} FROM ({source}) e"
        )

    ledger_insert = (
        f"INSERT INTO {meta}.run_ledger "
        f"SELECT {ledger} FROM ({steps(_latest('step', 'run_id, step_no, status'))}) s "
        f"WHERE NOT EXISTS (SELECT 1 FROM {meta}.run_ledger r "
        f"WHERE r.run_id = s.run_id AND r.step_no = s.step_no AND r.status = s.status "
        f"AND {recent})"
    )
    run_merge = _merge_sql(
        f"{meta}.run_current",
        steps(_latest("step", ", ".join(RUN_KEYS))),
        RUN_KEYS,
        LEDGER_COLUMNS,
        "recorded_at",
    )
    # A definition another owner already holds becomes a FAILED ownership_check step.
    refused = (
        f"SELECT concat('ownership-', substr(e.content_hash, 1, 16)) AS run_id, "
        f"CAST(NULL AS STRING) AS parent_run_id, e.object_kind, e.full_name, e.owner_kind, "
        f"e.owner, CAST(1 AS INT) AS step_no, 'ownership_check' AS step_name, 'FAILED' AS status, "
        f"{recorded} AS started_at, {recorded} AS ended_at, "
        f"concat(e.full_name, ' is owned by ', c.owner_kind, ' ', c.owner, '; ', "
        f"e.owner_kind, ' ', e.owner, ' cannot register it') AS failure_reason, "
        f"CAST(NULL AS STRING) AS recon_json, {recorded} AS recorded_at, {event_date} "
        f"FROM ({latest_definition}) e "
        f"JOIN {meta}.object_current c "
        f"ON c.object_kind = e.object_kind AND c.full_name = e.full_name "
        f"WHERE {other_owner}"
    )
    ownership = (
        f"INSERT INTO {meta}.run_ledger SELECT {ledger} FROM ({refused}) s "
        f"WHERE NOT EXISTS (SELECT 1 FROM {meta}.run_ledger r "
        f"WHERE r.run_id = s.run_id AND r.step_name = 'ownership_check' AND {recent})"
    )
    ownership_current = _merge_sql(
        f"{meta}.run_current", refused, RUN_KEYS, LEDGER_COLUMNS, "recorded_at"
    )
    definition_columns = ", ".join(
        [
            f"{recorded} AS recorded_at" if column == "recorded_at" else f"e.{column}"
            for column in OBJECT_COLUMNS
        ]
        + ["parse_json(e.config_json) AS config"]
    )
    object_columns = ", ".join(OBJECT_TABLE_COLUMNS)
    accepted = (
        f"SELECT {definition_columns} "
        f"FROM ({latest_definition}) e "
        f"LEFT JOIN {meta}.object_current c "
        f"ON c.object_kind = e.object_kind AND c.full_name = e.full_name "
        f"WHERE (c.full_name IS NULL OR (c.content_hash <> e.content_hash "
        f"AND {recorded} >= c.recorded_at)) "
        f"AND NOT (c.full_name IS NOT NULL AND {other_owner})"
    )
    object_insert = (
        f"INSERT INTO {meta}.object_registry ({object_columns}) "
        f"SELECT {object_columns} FROM ({accepted}) a "
        f"WHERE NOT EXISTS ("
        f"SELECT 1 FROM {meta}.object_registry h "
        f"WHERE h.owner = a.owner AND h.object_kind = a.object_kind "
        f"AND h.full_name = a.full_name AND h.content_hash = a.content_hash "
        f"AND h.recorded_at = a.recorded_at)"
    )
    object_merge = _merge_sql(
        f"{meta}.object_current", accepted, OBJECT_KEYS, OBJECT_TABLE_COLUMNS, "recorded_at"
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
