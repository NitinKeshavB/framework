"""The registry writer. registry_sync calls it at deploy; framework_ddl calls apply_ddl."""

from __future__ import annotations

import hashlib
import logging
import re
import time
from collections import Counter
from collections.abc import Mapping
from typing import Any

from lakeflow_framework.logs import statement_summary
from lakeflow_framework.metadata import (
    LOOKUP_KEYS_VIEW,
    OBJECT_COLUMNS,
    OWNER_KINDS,
    OWNER_NAMES_VIEW,
    REFUSAL_COLUMNS,
    SCD_COLUMNS,
    _sql_in,
    current_objects_sql,
    ddl_statements,
    ingest_steps,
    new_object_rows,
    owners_sql,
    ownership_conflicts,
    require_safe_names,
    step_key,
    step_keys_sql,
    step_payload,
)

log = logging.getLogger(__name__)

OBJECT_KEYS = ("object_kind", "full_name")
# Columns the writer computes from the staged columns.
OBJECT_DERIVED = {"config": "parse_json(config_json)"}
OBJECT_TABLE_COLUMNS = (*OBJECT_COLUMNS, *OBJECT_DERIVED)
_REFUSAL_TYPES = {
    "timestamp_columns": ("recorded_at",),
}


def apply_ddl(spark, catalog: str, text: str | None = None, force: bool = False) -> int:
    """Apply registry DDL. ``text`` is the bundle's SQL file when the job runs on a cluster.

    Every statement is idempotent, so running them again changes nothing, but it takes minutes.
    The hash of the SQL last applied is kept in framework_meta.ddl_applied, and a run whose SQL
    is already applied stops after that one lookup. Returns the number of statements run.
    """

    statements = ddl_statements(catalog, text)
    digest = ddl_hash(statements)
    log.info("DDL for %s: %d statement(s), sql hash %s", catalog, len(statements), digest[:12])
    if force:
        log.info("force is set: applying even if this hash was applied before")
    elif _ddl_applied(spark, catalog, digest):
        log.info("sql hash %s is already in ddl_applied: nothing to apply", digest[:12])
        return 0
    for number, statement in enumerate(statements, 1):
        started = time.monotonic()
        try:
            spark.sql(statement)
        except Exception:
            log.error(
                "statement %d/%d failed: %s", number, len(statements), statement_summary(statement)
            )
            raise
        log.info(
            "statement %d/%d in %.1fs: %s",
            number,
            len(statements),
            time.monotonic() - started,
            statement_summary(statement),
        )
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
    """Append ownership refusals. Name kept for call-site compatibility."""

    candidates = [payload for payload in payloads if seen is None or step_key(payload) not in seen]
    if catalog and candidates:
        already = _existing_refusal_keys(spark, catalog, candidates)
        candidates = [payload for payload in candidates if step_key(payload) not in already]
    staged: set[tuple[str, str, str]] = set()
    rows = ingest_steps(candidates, staged)
    if not rows:
        return 0
    written = _insert(
        spark,
        _table(catalog, "ownership_refusals"),
        rows,
        REFUSAL_COLUMNS,
        **_REFUSAL_TYPES,
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
    _reject_duplicate_object_keys(fresh)
    log.info("definitions: %d declared (%s)", len(rows), _by_kind(rows))
    current: dict[tuple[str, str], str | None] = {}
    if catalog and fresh:
        # Ownership comes first: another owner's identical definition must be refused and
        # recorded, not skipped as already current.
        fresh = _refuse_other_owners(spark, catalog, fresh)
    if catalog and fresh:
        current = _current_object_hashes(spark, catalog, fresh)
        candidates = len(fresh)
        fresh = [
            row
            for row in fresh
            if current.get((row["object_kind"], row["full_name"])) != row["content_hash"]
        ]
        log.info(
            "definitions: %d unchanged since the current version, %d new or changed",
            candidates - len(fresh),
            len(fresh),
        )
    if not fresh:
        log.info("definitions: nothing to write")
        return 0
    if len(fresh) <= 20:
        for row in fresh:
            change = "changed" if (row["object_kind"], row["full_name"]) in current else "new"
            log.info("definition %s %s: %s", row["object_kind"], row["full_name"], change)
    else:
        log.info(
            "definitions: writing %d version(s) (%s)",
            len(fresh),
            _by_kind(fresh),
        )
    written = _scd2_merge(spark, _table(catalog, "asset_registry"), fresh)
    log.info(
        "definitions: wrote %d version(s) to asset_registry and asset_registry_current", written
    )
    _merge_current(
        spark,
        _table(catalog, "asset_registry_current"),
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
    """Close current versions this bundle no longer defines, scoped to its owners only.

    Retirement is limited to ``owner`` values present in ``deployed``, so one ingestion
    bundle can be sharded per source system without deleting another bundle's rows.
    Each retired object gets a deletion version (``is_deleted = true``) and leaves
    asset_registry_current. A deploy with no definitions retires nothing.
    """

    if owner_kind not in OWNER_KINDS:
        raise ValueError(f"owner_kind must be one of {', '.join(OWNER_KINDS)}")
    keys = sorted({(str(row["object_kind"]), str(row["full_name"])) for row in deployed})
    if not keys:
        raise ValueError("a deploy with no definitions cannot retire anything")
    owners = _deployed_owners(deployed, owner_kind)
    from lakeflow_framework.spark_types import require_writer, string_schema

    writer = require_writer(spark)
    rows = [dict(zip(OBJECT_KEYS, key, strict=True)) for key in keys]
    writer.createDataFrame(rows, string_schema(OBJECT_KEYS)).createOrReplaceTempView(
        "_framework_deployed"
    )
    owner_in = _sql_in(owners)
    gone = _rows(
        spark.sql(
            f"SELECT c.object_kind, c.full_name FROM {_table(catalog, 'asset_registry_current')} c "
            "LEFT ANTI JOIN _framework_deployed d "
            "ON d.object_kind = c.object_kind AND d.full_name = c.full_name "
            f"WHERE c.owner_kind = '{owner_kind}' AND c.owner IN ({owner_in}) "
            "ORDER BY c.object_kind, c.full_name"
        )
    )
    if not gone:
        log.info(
            "retire: every current %s definition for owner(s) %s is still deployed",
            owner_kind,
            ", ".join(owners),
        )
        return
    for row in gone:
        log.info("retire: %s %s is no longer deployed", row["object_kind"], row["full_name"])
    for statement in retire_statements(catalog, "_framework_deployed", owner_kind, owners):
        spark.sql(statement)
    log.info("retire: wrote %d deletion version(s)", len(gone))


def scd2_merge_sql(table: str, staged: str) -> str:
    """One MERGE that closes changed or deleted current versions and inserts the new ones.

    Each staged row appears with its key, to close the current version, and, when that
    version differs, again without a key, so it is inserted as the new current version.
    """

    key = "concat(object_kind, '|', full_name)"
    columns = OBJECT_TABLE_COLUMNS
    latest = (
        f"SELECT * FROM ({staged}) QUALIFY row_number() OVER "
        "(PARTITION BY object_kind, full_name ORDER BY recorded_at DESC, content_hash DESC) = 1"
    )
    source = (
        f"SELECT s.*, {key} AS merge_key FROM ({latest}) s "
        f"UNION ALL "
        f"SELECT s.*, NULL AS merge_key FROM ({latest}) s "
        f"JOIN {table} r ON r.object_kind = s.object_kind AND r.full_name = s.full_name "
        f"AND r.is_current "
        f"WHERE {_content_hash_changed('r', 's')}"
    )
    inserted = ", ".join((*columns, *SCD_COLUMNS))
    values = ", ".join(
        [f"u.{column}" for column in columns] + ["u.recorded_at", "NULL", "true", "false"]
    )
    return (
        f"MERGE INTO {table} t USING ({source}) u "
        f"ON concat(t.object_kind, '|', t.full_name) = u.merge_key AND t.is_current "
        f"WHEN MATCHED AND ({_content_hash_changed('t', 'u')}) "
        f"THEN UPDATE SET t.valid_to = u.recorded_at, t.is_current = false "
        f"WHEN NOT MATCHED THEN INSERT ({inserted}) VALUES ({values})"
    )


def retire_statements(
    catalog: str,
    deployed: str,
    owner_kind: str,
    owners: list[str],
) -> list[str]:
    """SQL that retires current objects for ``owners`` missing from the ``deployed`` view."""

    if not re.fullmatch(r"[A-Za-z0-9_-]+", catalog or ""):
        raise ValueError("catalog is required")
    if not owners:
        raise ValueError("owners are required to scope retirement")
    if any(not re.fullmatch(r"[A-Za-z0-9_-]+", owner or "") for owner in owners):
        raise ValueError("owner is required")
    owner_in = _sql_in(owners)
    registry = _table(catalog, "asset_registry")
    current = _table(catalog, "asset_registry_current")
    gone = (
        f"SELECT r.* FROM {registry} r "
        f"LEFT ANTI JOIN {deployed} d ON d.object_kind = r.object_kind AND d.full_name = r.full_name "
        f"WHERE r.is_current AND NOT coalesce(r.is_deleted, false) "
        f"AND r.owner_kind = '{owner_kind}' AND r.owner IN ({owner_in})"
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
        f"AND c.owner IN ({owner_in}) "
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

    _stage_owner_names(spark, [str(row["full_name"]) for row in rows])
    statement = owners_sql(catalog, OWNER_NAMES_VIEW)
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
            log.warning(
                "refused %s %s for %s %s: it is owned by %s %s",
                row["object_kind"],
                row["full_name"],
                row["owner_kind"],
                row["owner"],
                holder[0],
                holder[1],
            )
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
                    content_hash=str(row["content_hash"]),
                    holder_owner_kind=holder[0],
                    holder_owner=holder[1],
                    failure_reason=(
                        f"{row['full_name']} is owned by {holder[0]} {holder[1]}; "
                        f"{row['owner_kind']} {row['owner']} cannot register it"
                    ),
                )
            )
        append_steps(spark, steps, None, catalog)
    return accepted


def _by_kind(rows: list[dict]) -> str:
    counts = Counter(str(row["object_kind"]) for row in rows)
    return ", ".join(f"{kind} {count}" for kind, count in sorted(counts.items()))


def _table(catalog: str, name: str) -> str:
    return f"{catalog}.framework_meta.{name}" if catalog else f"framework_meta.{name}"


def _existing_refusal_keys(spark, catalog: str, payloads: list[dict]) -> set[tuple[str, str, str]]:
    statement = step_keys_sql(
        catalog,
        [str(payload.get("content_hash") or "") for payload in payloads],
        full_names=[str(payload["full_name"]) for payload in payloads],
    )
    return {
        (str(row["object_kind"]), str(row["full_name"]), str(row["content_hash"]))
        for row in _rows(spark.sql(statement))
    }


def _current_object_hashes(
    spark, catalog: str, rows: list[dict]
) -> dict[tuple[str, str], str | None]:
    # A failed lookup must not look like "nothing stored", or the batch is appended again.
    # Keep SQL NULL as None — str(None) == "None" would wedge NULL hashes forever.
    _stage_lookup_keys(spark, rows)
    statement = current_objects_sql(catalog, LOOKUP_KEYS_VIEW)
    hashes: dict[tuple[str, str], str | None] = {}
    for row in _rows(spark.sql(statement)):
        digest = row["content_hash"]
        hashes[(str(row["object_kind"]), str(row["full_name"]))] = (
            None if digest is None else str(digest)
        )
    return hashes


def _stage_lookup_keys(spark, rows: list[dict], view: str = LOOKUP_KEYS_VIEW) -> None:
    """Temp view of (object_kind, full_name) for JOIN lookups — no IN-list scaling wall."""

    from lakeflow_framework.metadata import _object_keys
    from lakeflow_framework.spark_types import require_writer, string_schema

    _object_keys(rows)
    keys = sorted({(str(row["object_kind"]), str(row["full_name"])) for row in rows})
    payload = [{"object_kind": kind, "full_name": name} for kind, name in keys]
    require_writer(spark).createDataFrame(
        payload, string_schema(("object_kind", "full_name"))
    ).createOrReplaceTempView(view)


def _stage_owner_names(spark, full_names: list[str], view: str = OWNER_NAMES_VIEW) -> None:
    """Temp view of full_name for ownership JOINs."""

    from lakeflow_framework.spark_types import require_writer, string_schema

    names = require_safe_names(full_names)
    payload = [{"full_name": name} for name in sorted(set(names))]
    require_writer(spark).createDataFrame(
        payload, string_schema(("full_name",))
    ).createOrReplaceTempView(view)


def _deployed_owners(deployed: list[dict], owner_kind: str) -> list[str]:
    owners = sorted(
        {
            str(row["owner"])
            for row in deployed
            if str(row.get("owner_kind") or "") == owner_kind and row.get("owner")
        }
    )
    if not owners or any(not re.fullmatch(r"[A-Za-z0-9_-]+", owner) for owner in owners):
        raise ValueError("owner is required")
    return owners


def _content_hash_changed(left: str, right: str) -> str:
    """NULL-safe hash compare: ``<>`` is unknown when either side is NULL."""

    return (
        f"NOT ({left}.content_hash <=> {right}.content_hash) OR coalesce({left}.is_deleted, false)"
    )


def _reject_duplicate_object_keys(rows: list[dict]) -> None:
    """Fail loudly when one deploy batch has two rows for the same object key."""

    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (str(row["object_kind"]), str(row["full_name"]))
        if key in seen:
            kind, name = key
            raise ValueError(
                f"duplicate definition in one deploy: {kind} {name} — "
                "fix the colliding specs (often two source systems naming the same schema)"
            )
        seen.add(key)


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

    if not rows:
        return
    from lakeflow_framework.spark_types import require_writer

    require_writer(spark)
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
    from lakeflow_framework.spark_types import require_writer

    require_writer(spark)
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

    from lakeflow_framework.spark_types import require_writer, string_schema

    string_rows = [
        {column: None if row.get(column) is None else str(row.get(column)) for column in columns}
        for row in rows
    ]
    require_writer(spark).createDataFrame(
        string_rows, string_schema(columns)
    ).createOrReplaceTempView(view)
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
    from lakeflow_framework.sqlnames import merge_sql

    return merge_sql(table, source, keys, columns, newer_than=newer_than, dedupe_source=True)


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
    from lakeflow_framework.sqlnames import spark_rows

    return spark_rows(frame)


def _event_time(column: str) -> str:
    # The framework writes UTC timestamps as 2026-11-09T00:00:00Z, which CAST reads as UTC. A
    # datetime pattern would need quoted T and Z, and Databricks rejects the quoting that
    # open-source Spark accepts.
    return f"CAST({column} AS TIMESTAMP)"
