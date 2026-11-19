"""Read only what is new in a bronze table, without scanning it, by whichever way it allows.

One call, ``read_new``, gives a silver job the rows that were added, changed, or removed since that
silver table last read this bronze table. It has two ways to find them:

``change_feed``   the bronze table keeps a Delta change feed (every bronze and quarantine table
                  the framework builds does). Silver gets exactly the commits it has not read,
                  including deletes, and skips an update that did not change the row's content.
``ingest_timestamp``  the table has no change feed. Silver gets the rows stamped
                  ``_ingest_timestamp`` after the last time it read. Bronze is clustered on that
                  column, so Delta skips the older files instead of scanning them.

Either way the frame has the table's columns plus ``_change_type``: ``insert``, ``update`` or
``delete`` from the change feed; ``upsert`` (new or changed) or ``delete`` (a row flagged
``_is_deleted``) from the load time. ``Changes.commit()`` records how far silver has read, in
``framework_meta.silver_progress``, only once silver has used the rows. A silver table must merge
what it takes idempotently: a run that stops before ``commit()`` reads the same rows again.

``mode="auto"`` picks the change feed when the table has it, else the load time.

Limits, stated so nobody finds them in production:
- The load time cannot see a row that was removed (only a flagged ``_is_deleted`` row), nor an SCD2
  version being closed. A table with SCD2 columns therefore needs the change feed; this refuses.
- A table whose rows are all replaced (full refresh, or a bronze rebuild) gives every row as new.
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


@dataclass
class Changes:
    """What is new, and how to say it has been used."""

    frame: Any
    mode: str
    commit: Callable[[], None]


def read_new(
    spark: Any,
    bronze_table: str,
    silver_table: str,
    *,
    keys: list[str] | None = None,
    mode: str = "auto",
) -> Changes:
    """The rows of ``bronze_table`` that ``silver_table`` has not read yet.

    ``keys`` are the business keys. With them, an update whose content did not change (the
    pipeline stamped it again, nothing else) is left out of the change feed's rows.
    """

    bronze = _table(bronze_table)
    silver = _table(silver_table)
    if mode not in MODES:
        raise ValueError(f"mode must be one of {', '.join(MODES)}")
    meta = f"{bronze.split('.')[0]}.framework_meta"
    columns = list(spark.read.table(bronze).columns)
    feed = change_feed_is_on(spark, bronze)
    chosen = choose_mode(mode, feed, columns)
    latest = _latest_version(spark, bronze)
    progress = _progress(spark, meta, silver, bronze)
    if chosen == "change_feed":
        frame = _change_feed_rows(spark, bronze, progress.get("version"), latest, keys)

        def commit() -> None:
            _save(spark, meta, silver, bronze, version=latest, timestamp=None)

    else:
        frame = _load_time_rows(spark, bronze, progress.get("timestamp"), latest, columns)

        def commit() -> None:
            newest = frame.selectExpr(f"max({INGEST_TIMESTAMP}) AS newest").collect()[0]["newest"]
            if newest is not None:
                _save(spark, meta, silver, bronze, version=None, timestamp=newest)

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
    """The operation that rebuilt the table after version ``last``, if one did.

    A full refresh, a replace, a restore or a truncate rewrites the table's history, and Delta
    cannot read a change feed across it. The history is newest first, so the newest ``latest - last``
    rows are exactly the commits silver has not read.
    """

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
        # The first read has no history to follow: take the table as it is now, as inserts.
        snapshot = spark.read.option("versionAsOf", latest).table(table)
        return snapshot.selectExpr(
            "*", f"'insert' AS {CHANGE_TYPE}", f"{latest} AS {COMMIT_VERSION}"
        )
    start = last_version + 1
    if start > latest:
        # Nothing was committed since; asking the feed for a later version would be an error.
        return (
            spark.read.option("versionAsOf", latest)
            .table(table)
            .where("1 = 0")
            .selectExpr("*", f"'insert' AS {CHANGE_TYPE}", f"{latest} AS {COMMIT_VERSION}")
        )
    rebuilt = _rebuilt_since(spark, table, last_version, latest)
    if rebuilt:
        raise ValueError(
            f"{table} was rebuilt since silver last read it ({rebuilt}), and the change feed "
            "cannot be followed across a rebuild. Delete this pair's row in "
            "framework_meta.silver_progress and rebuild the silver table; the next read then "
            "starts again from the table as it is."
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
        # A row the pipeline wrote again with the same content shows as an update. Drop it.
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
    # Pinned to one table version, so the rows read and the mark saved afterwards agree even if
    # the pipeline commits more in between.
    frame = spark.read.option("versionAsOf", latest).table(table)
    if last is not None:
        frame = frame.where(f"{INGEST_TIMESTAMP} > {_timestamp(last)}")
    kind = (
        f"CASE WHEN coalesce({IS_DELETED}, false) THEN 'delete' ELSE 'upsert' END"
        if IS_DELETED in columns
        else "'upsert'"
    )
    return frame.selectExpr("*", f"{kind} AS {CHANGE_TYPE}")


def _progress(spark: Any, meta: str, silver: str, bronze: str) -> dict[str, Any]:
    rows = spark.sql(
        f"SELECT last_ingest_timestamp, last_commit_version FROM {meta}.silver_progress "
        f"WHERE silver_table = {sql_string(silver)} AND bronze_table = {sql_string(bronze)}"
    ).collect()
    if not rows:
        return {}
    row = rows[0]
    return {"timestamp": row["last_ingest_timestamp"], "version": row["last_commit_version"]}


def _save(
    spark: Any,
    meta: str,
    silver: str,
    bronze: str,
    *,
    version: int | None,
    timestamp: datetime | None,
) -> None:
    ts = _timestamp(timestamp) if timestamp is not None else "CAST(NULL AS TIMESTAMP)"
    spark.sql(
        f"MERGE INTO {meta}.silver_progress t USING (SELECT {sql_string(silver)} AS silver_table, "
        f"{sql_string(bronze)} AS bronze_table, {ts} AS last_ingest_timestamp, "
        f"CAST({'NULL' if version is None else int(version)} AS BIGINT) AS last_commit_version) s "
        "ON t.silver_table = s.silver_table AND t.bronze_table = s.bronze_table "
        "WHEN MATCHED THEN UPDATE SET t.last_ingest_timestamp = "
        "coalesce(s.last_ingest_timestamp, t.last_ingest_timestamp), t.last_commit_version = "
        "coalesce(s.last_commit_version, t.last_commit_version), t.processed_at = current_timestamp() "
        "WHEN NOT MATCHED THEN INSERT (silver_table, bronze_table, last_ingest_timestamp, "
        "last_commit_version, processed_at) VALUES (s.silver_table, s.bronze_table, "
        "s.last_ingest_timestamp, s.last_commit_version, current_timestamp())"
    )


def _timestamp(value: datetime) -> str:
    return f"TIMESTAMP {sql_string(value.strftime('%Y-%m-%d %H:%M:%S.%f'))}"


def _table(name: str) -> str:
    """A three-part table name, checked, because it is put into SQL text."""

    parts = (name or "").split(".")
    if len(parts) != 3 or not all(IDENTIFIER.match(part) for part in parts):
        raise ValueError("a table must be named catalog.schema.table")
    return name


__all__ = ["Changes", "choose_mode", "read_new"]
