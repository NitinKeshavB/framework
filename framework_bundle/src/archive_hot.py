"""Move run/load facts older than 7 days into forever archive tables (insert then delete).

Idempotent: anti-join on natural keys so reruns do not duplicate archive rows. Never truncates
or drops archive. Dashboards use v_*_90d over hot ∪ archive.
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.logs import job_logging, step

# (hot table, archive table, key columns for anti-join, age column)
MOVES = (
    (
        "load_ledger",
        "load_ledger_archive",
        ("job_name", "layer", "update_id", "dataflow", "recorded_at"),
        "coalesce(started_at, recorded_at)",
    ),
    (
        "job_runs",
        "job_runs_archive",
        ("job_id", "run_id"),
        "coalesce(started_at, recorded_at)",
    ),
    (
        "job_task_runs",
        "job_task_runs_archive",
        ("job_run_id", "task_run_id"),
        "coalesce(started_at, recorded_at)",
    ),
    (
        "pipeline_updates",
        "pipeline_updates_archive",
        ("pipeline_id", "update_id"),
        "coalesce(started_at, recorded_at)",
    ),
    (
        "table_health",
        "table_health_archive",
        ("table_name", "recorded_at"),
        "coalesce(cast(event_date as timestamp), recorded_at)",
    ),
)


def main() -> None:
    catalog = sys.argv[1]
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    log = job_logging("archive_hot")
    log.info("catalog %s, hot window %d day(s)", catalog, days)
    spark = SparkSession.builder.getOrCreate()
    meta = f"{catalog}.framework_meta"
    for hot, archive, keys, age in MOVES:
        with step(log, f"archive {hot} -> {archive}"):
            moved = _move(spark, meta, hot, archive, keys, age, days)
        log.info("%s: moved %d row(s)", hot, moved)


def _move(
    spark, meta: str, hot: str, archive: str, keys: tuple[str, ...], age: str, days: int
) -> int:
    on = " AND ".join(f"a.{key} <=> h.{key}" for key in keys)
    spark.sql(
        f"""
        INSERT INTO {meta}.{archive}
        SELECT h.*, current_timestamp() AS archived_at
        FROM {meta}.{hot} h
        WHERE {age} < current_timestamp() - INTERVAL {days} DAYS
          AND NOT EXISTS (
            SELECT 1 FROM {meta}.{archive} a WHERE {on}
          )
        """
    )
    before = spark.sql(f"SELECT count(*) AS n FROM {meta}.{hot}").collect()[0]["n"]
    spark.sql(
        f"""
        DELETE FROM {meta}.{hot} h
        WHERE {age} < current_timestamp() - INTERVAL {days} DAYS
          AND EXISTS (
            SELECT 1 FROM {meta}.{archive} a WHERE {on}
          )
        """
    )
    after = spark.sql(f"SELECT count(*) AS n FROM {meta}.{hot}").collect()[0]["n"]
    return int(before) - int(after)


if __name__ == "__main__":
    main()
