"""Daily file and commit figures for each bronze table, kept in framework_meta.table_health."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

# Commits that rewrite or inspect files without loading rows. The last load is the commit wanted.
MAINTENANCE = {"OPTIMIZE", "VACUUM START", "VACUUM END", "ANALYZE", "REORG", "SET TBLPROPERTIES"}
HISTORY_DEPTH = 20
HEALTH_COLUMNS = (
    "table_name",
    "owner_kind",
    "owner",
    "num_files",
    "size_in_bytes",
    "last_version",
    "last_operation",
    "last_commit_at",
    "last_operation_metrics",
    "error",
    "recorded_at",
    "event_date",
)
_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+){2}$")


def quoted(full_name: str) -> str:
    if not _SAFE_NAME.match(full_name):
        raise ValueError(f"not a catalog.schema.table name: {full_name}")
    return ".".join(f"`{part}`" for part in full_name.split("."))


def health_row(spark, table: dict[str, Any], recorded: datetime) -> dict[str, Any]:
    """One table's figures. A table that cannot be described is recorded with its error."""

    row: dict[str, Any] = {column: None for column in HEALTH_COLUMNS}
    row.update(
        table_name=table["table_name"],
        owner_kind=table["owner_kind"],
        owner=table["owner"],
        recorded_at=recorded,
        event_date=recorded.date(),
    )
    errors: list[str] = []
    try:
        name = quoted(str(table["table_name"]))
    except ValueError as exc:
        row["error"] = str(exc)
        return row
    try:
        detail = _as_dict(spark.sql(f"DESCRIBE DETAIL {name}").first())
        row["num_files"] = detail.get("numFiles")
        row["size_in_bytes"] = detail.get("sizeInBytes")
    except Exception as exc:
        errors.append(f"DESCRIBE DETAIL: {exc}")
    try:
        history = spark.sql(f"DESCRIBE HISTORY {name} LIMIT {HISTORY_DEPTH}").collect()
        last = next(
            (
                commit
                for commit in map(_as_dict, history)
                if str(commit.get("operation")) not in MAINTENANCE
            ),
            None,
        )
        if last is not None:
            row["last_version"] = last.get("version")
            row["last_operation"] = last.get("operation")
            row["last_commit_at"] = last.get("timestamp")
            metrics = last.get("operationMetrics") or {}
            row["last_operation_metrics"] = {str(k): str(v) for k, v in dict(metrics).items()}
    except Exception as exc:
        errors.append(f"DESCRIBE HISTORY: {exc}")
    if errors:
        row["error"] = "; ".join(errors)[:2000]
    return row


def record_table_health(spark, catalog: str) -> int:
    """Append one row per bronze table in the registry. Returns the number of rows."""

    if not re.fullmatch(r"[A-Za-z0-9_-]+", catalog or ""):
        raise ValueError("catalog is required")
    meta = f"{catalog}.framework_meta"
    tables = [
        _as_dict(item)
        for item in spark.sql(
            f"SELECT table_name, owner_kind, owner FROM {meta}.v_bronze_tables"
        ).collect()
    ]
    recorded = datetime.now(timezone.utc).replace(microsecond=0)
    log.info("table health: %d bronze table(s) in v_bronze_tables", len(tables))
    rows = []
    for table in tables:
        if not table.get("table_name"):
            continue
        row = health_row(spark, table, recorded)
        if row["error"]:
            log.warning("table health: %s: %s", row["table_name"], row["error"])
        else:
            log.info(
                "table health: %s: %s file(s), %s byte(s), last %s at %s",
                row["table_name"],
                row["num_files"],
                row["size_in_bytes"],
                row["last_operation"],
                row["last_commit_at"],
            )
        rows.append(row)
    if not rows:
        return 0
    spark.createDataFrame(rows, _schema()).write.mode("append").saveAsTable(f"{meta}.table_health")
    log.info("table health: appended %d row(s) to %s.table_health", len(rows), meta)
    return len(rows)


def _schema():
    from lakeflow_framework.spark_types import types_module

    t = types_module()
    if t is None:
        return None
    types = {
        "num_files": t.LongType(),
        "size_in_bytes": t.LongType(),
        "last_version": t.LongType(),
        "last_commit_at": t.TimestampType(),
        "last_operation_metrics": t.MapType(t.StringType(), t.StringType()),
        "recorded_at": t.TimestampType(),
        "event_date": t.DateType(),
    }
    return t.StructType(
        [
            t.StructField(column, types.get(column, t.StringType()), True)
            for column in HEALTH_COLUMNS
        ]
    )


def _as_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    return row if isinstance(row, dict) else row.asDict()
