"""Read only what is new in an upstream table, without scanning it, by whichever way it allows.

One call, ``read_new``, gives a reader the rows that were added, changed, or removed since that
reader last consumed a source table. It has two ways to find them:

``change_feed``   the source keeps a Delta change feed (every bronze and quarantine table the
                  framework builds does). The reader gets exactly the commits it has not read,
                  including deletes, and skips an update that did not change the row's content.
``ingest_timestamp``  the source has no change feed. The reader gets rows stamped
                  ``_ingest_timestamp`` after the last read. Framework tables are clustered on
                  that column, so Delta skips the older files instead of scanning them.

Either way the frame has the table's columns plus ``_change_type``: ``insert``, ``update`` or
``delete`` from the change feed; ``upsert`` (new or changed) or ``delete`` (a row flagged
``_is_deleted``) from the load time. ``Changes.commit()`` appends a ``SUCCEEDED`` row to
``framework_meta.flow_checkpoint`` only once the rows have been used. Target merges must be
idempotent: a run that stops before ``commit()`` reads the same rows again.

``mode="auto"`` picks the change feed when the table has it, else the load time.

Limits, stated so nobody finds them in production:
- The load time cannot see a row that was removed (only a flagged ``_is_deleted`` row), nor an SCD2
  version being closed. A table with SCD2 columns therefore needs the change feed; this refuses.
- A table whose rows are all replaced (full refresh, or a rebuild) gives every row as new.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from lakeflow_framework.config import INGEST_TIMESTAMP, IS_DELETED, SRC_ROW_HASH
from lakeflow_framework.sqlnames import IDENTIFIER, sql_string

MODES = ("auto", "change_feed", "ingest_timestamp")
CHANGE_TYPE = "_change_type"
COMMIT_VERSION = "_commit_version"
SCD2_END = "__END_AT"
_PROPERTY = "delta.enableChangeDataFeed"
_REBUILDS = ("REFRESH", "REPLACE", "RESTORE", "TRUNCATE")
CHECKPOINT_TABLE = "flow_checkpoint"
CHECKPOINT_VIEW = "v_flow_checkpoint"
# Back-compat alias during rename.
PROGRESS_TABLE = CHECKPOINT_TABLE


@dataclass
class Changes:
    """What is new, and how to say it has been used."""

    frame: Any
    mode: str
    commit: Callable[[], None]


def read_new(
    spark: Any,
    source_table: str,
    reader_table: str,
    *,
    keys: list[str] | None = None,
    mode: str = "auto",
    job_name: str | None = None,
    pipeline_id: str | None = None,
    update_id: str | None = None,
    batch_id: str | None = None,
    load_kind: str = "regular",
) -> Changes:
    """The rows of ``source_table`` that ``reader_table`` has not consumed yet.

    ``keys`` are the business keys. With them, an update whose content did not change (the
    pipeline stamped it again, nothing else) is left out of the change feed's rows.

    Argument order is source then reader (upstream then curated consumer). Optional job/pipeline
    identifiers are injected into the checkpoint row on ``commit()`` — no hand edits to meta.
    """

    source = _table(source_table)
    reader = _table(reader_table)
    if mode not in MODES:
        raise ValueError(f"mode must be one of {', '.join(MODES)}")
    meta = f"{source.split('.')[0]}.framework_meta"
    columns = list(spark.read.table(source).columns)
    feed = change_feed_is_on(spark, source)
    chosen = choose_mode(mode, feed, columns)
    latest = _latest_version(spark, source)
    progress = _progress(spark, meta, reader, source)
    context = {
        "job_name": job_name,
        "pipeline_id": pipeline_id,
        "update_id": update_id,
        "batch_id": batch_id,
        "load_kind": load_kind,
        "progress_kind": chosen,
        "watermark_column": COMMIT_VERSION if chosen == "change_feed" else INGEST_TIMESTAMP,
    }
    if chosen == "change_feed":
        frame = _change_feed_rows(spark, source, progress.get("version"), latest, keys)

        def commit() -> None:
            _save(
                spark,
                meta,
                reader,
                source,
                version=latest,
                timestamp=None,
                **context,
            )

    else:
        frame = _load_time_rows(spark, source, progress.get("timestamp"), latest, columns)

        def commit() -> None:
            newest = frame.selectExpr(f"max({INGEST_TIMESTAMP}) AS newest").collect()[0]["newest"]
            if newest is not None:
                _save(
                    spark,
                    meta,
                    reader,
                    source,
                    version=None,
                    timestamp=newest,
                    **context,
                )

    return Changes(frame=frame, mode=chosen, commit=commit)


def choose_mode(requested: str, feed_on: bool, columns: list[str]) -> str:
    """The mode to read with, or an error when the table cannot be read that way."""

    chosen = (
        ("change_feed" if feed_on else "ingest_timestamp") if requested == "auto" else requested
    )
    if chosen == "change_feed" and not feed_on:
        raise ValueError(f"the change feed is not on for this table ({_PROPERTY} is not true)")
    if chosen == "ingest_timestamp":
        if SCD2_END in columns:
            raise ValueError(
                f"{SCD2_END} marks an SCD2 table, whose closed versions only the change feed "
                "shows; turn the change feed on for this table"
            )
        if INGEST_TIMESTAMP not in columns:
            raise ValueError(f"the table has no {INGEST_TIMESTAMP} column to read by")
    return chosen


def change_feed_is_on(spark: Any, table: str) -> bool:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return any(str(row["key"]) == _PROPERTY and str(row["value"]).lower() == "true" for row in rows)


def _rebuilt_since(spark: Any, table: str, last: int, latest: int) -> str | None:
    """The operation that rebuilt the table after version ``last``, if one did."""

    if latest <= last:
        return None
    for row in spark.sql(f"DESCRIBE HISTORY {table} LIMIT {latest - last}").collect():
        operation = str(row["operation"])
        if any(word in operation.upper() for word in _REBUILDS):
            return f"{operation} at version {row['version']}"
    return None


def _latest_version(spark: Any, table: str) -> int:
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]["version"])


def _change_feed_rows(
    spark: Any, table: str, last_version: int | None, latest: int, keys: list[str] | None
) -> Any:
    if last_version is None:
        snapshot = spark.read.option("versionAsOf", latest).table(table)
        return snapshot.selectExpr(
            "*", f"'insert' AS {CHANGE_TYPE}", f"{latest} AS {COMMIT_VERSION}"
        )
    start = last_version + 1
    if start > latest:
        return (
            spark.read.option("versionAsOf", latest)
            .table(table)
            .where("1 = 0")
            .selectExpr("*", f"'insert' AS {CHANGE_TYPE}", f"{latest} AS {COMMIT_VERSION}")
        )
    rebuilt = _rebuilt_since(spark, table, last_version, latest)
    if rebuilt:
        raise ValueError(
            f"{table} was rebuilt since the reader last consumed it ({rebuilt}), and the change "
            "feed cannot be followed across a rebuild. Delete SUCCEEDED rows for this pair in "
            f"framework_meta.{CHECKPOINT_TABLE} (or ignore via a new reader) and rebuild the "
            "reader table; the next read then starts again from the source as it is."
        )
    feed = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start)
        .option("endingVersion", latest)
        .table(table)
    )
    after = feed.where(f"{CHANGE_TYPE} != 'update_preimage'")
    if keys:
        same = [*keys, COMMIT_VERSION, SRC_ROW_HASH]
        before = feed.where(f"{CHANGE_TYPE} = 'update_preimage'").select(*same)
        after = after.join(before, on=same, how="left_anti")
    return after.selectExpr(
        f"* EXCEPT ({CHANGE_TYPE})",
        f"CASE WHEN {CHANGE_TYPE} = 'update_postimage' THEN 'update' ELSE {CHANGE_TYPE} END "
        f"AS {CHANGE_TYPE}",
    )


def _load_time_rows(
    spark: Any, table: str, last: datetime | None, latest: int, columns: list[str]
) -> Any:
    frame = spark.read.option("versionAsOf", latest).table(table)
    if last is not None:
        frame = frame.where(f"{INGEST_TIMESTAMP} > {_timestamp(last)}")
    kind = (
        f"CASE WHEN coalesce({IS_DELETED}, false) THEN 'delete' ELSE 'upsert' END"
        if IS_DELETED in columns
        else "'upsert'"
    )
    return frame.selectExpr("*", f"{kind} AS {CHANGE_TYPE}")


def _progress(spark: Any, meta: str, reader: str, source: str) -> dict[str, Any]:
    """Current mark from ``v_flow_checkpoint`` (latest SUCCEEDED only)."""

    rows = spark.sql(
        f"SELECT watermark_ts, watermark_id FROM {meta}.{CHECKPOINT_VIEW} "
        f"WHERE reader_table = {sql_string(reader)} AND source_table = {sql_string(source)}"
    ).collect()
    if not rows:
        return {}
    row = rows[0]
    version = None
    mark = row["watermark_id"]
    if mark is not None and str(mark).isdigit():
        version = int(mark)
    return {"timestamp": row["watermark_ts"], "version": version}


def _save(
    spark: Any,
    meta: str,
    reader: str,
    source: str,
    *,
    version: int | None,
    timestamp: datetime | None,
    job_name: str | None = None,
    pipeline_id: str | None = None,
    update_id: str | None = None,
    batch_id: str | None = None,
    load_kind: str = "regular",
    progress_kind: str = "change_feed",
    watermark_column: str | None = None,
) -> None:
    """Append a SUCCEEDED checkpoint — only call after the target write has committed."""

    ts = _timestamp(timestamp) if timestamp is not None else "CAST(NULL AS TIMESTAMP)"
    wid = "CAST(NULL AS STRING)" if version is None else sql_string(str(int(version)))
    spark.sql(
        f"INSERT INTO {meta}.{CHECKPOINT_TABLE} ("
        "reader_table, source_table, recorded_at, status, load_kind, progress_kind, "
        "watermark_column, watermark_ts, watermark_id, job_name, pipeline_id, update_id, batch_id"
        ") VALUES ("
        f"{sql_string(reader)}, {sql_string(source)}, current_timestamp(), 'SUCCEEDED', "
        f"{sql_string(load_kind)}, {sql_string(progress_kind)}, "
        f"{sql_string(watermark_column) if watermark_column else 'CAST(NULL AS STRING)'}, "
        f"{ts}, {wid}, "
        f"{sql_string(job_name) if job_name else 'CAST(NULL AS STRING)'}, "
        f"{sql_string(pipeline_id) if pipeline_id else 'CAST(NULL AS STRING)'}, "
        f"{sql_string(update_id) if update_id else 'CAST(NULL AS STRING)'}, "
        f"{sql_string(batch_id) if batch_id else 'CAST(NULL AS STRING)'}"
        ")"
    )


def _timestamp(value: datetime) -> str:
    return f"TIMESTAMP {sql_string(value.strftime('%Y-%m-%d %H:%M:%S.%f'))}"


def _table(name: str) -> str:
    """A three-part table name, checked, because it is put into SQL text."""

    parts = (name or "").split(".")
    if len(parts) != 3 or not all(IDENTIFIER.match(part) for part in parts):
        raise ValueError("a table must be named catalog.schema.table")
    return name


__all__ = ["Changes", "choose_mode", "read_new", "CHECKPOINT_TABLE", "CHECKPOINT_VIEW"]
