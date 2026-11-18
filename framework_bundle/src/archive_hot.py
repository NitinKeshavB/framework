"""Move run/load facts older than 7 days into forever archive tables (insert then delete).

Idempotent: anti-join on natural keys so reruns do not duplicate archive rows. The archive
side of the anti-join is bounded by the same age cutoff so cost does not grow with the
forever archive. The returned count is the number of hot rows deleted (not a full-table
before/after scan that concurrent inserts can make wrong or negative).
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.logs import job_logging, step

# (hot table, archive table, key columns for anti-join, age expression)
MOVES = (
    (
        "load_ledger",
        "load_ledger_archive",
        ("job_name", "layer", "update_id", "dataflow", "recorded_at"),
        "coalesce(started_at, recorded_at)",
    ),
    (
        "load_metric",
        "load_metric_archive",
        ("job_name", "layer", "update_id", "dataflow", "metric_name", "recorded_at"),
        "recorded_at",
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
    cutoff = f"current_timestamp() - INTERVAL {days} DAYS"
    # Bound archive scan to the same age window so the anti-join does not grow forever.
    if age == "recorded_at":
        archive_age = f"a.recorded_at < {cutoff}"
        hot_pred = f"h.recorded_at < {cutoff}"
    else:
        archive_age = f"coalesce(a.started_at, a.recorded_at) < {cutoff}"
        hot_pred = f"coalesce(h.started_at, h.recorded_at) < {cutoff}"

    spark.sql(
        f"""
        INSERT INTO {meta}.{archive}
        SELECT h.*, current_timestamp() AS archived_at
        FROM {meta}.{hot} h
        WHERE {hot_pred}
          AND NOT EXISTS (
            SELECT 1 FROM {meta}.{archive} a
            WHERE {on}
              AND {archive_age}
          )
        """
    )
    # Count only the doomed hot rows, then delete them — not a full-table before/after.
    doomed = spark.sql(
        f"""
        SELECT count(*) AS n
        FROM {meta}.{hot} h
        WHERE {hot_pred}
          AND EXISTS (
            SELECT 1 FROM {meta}.{archive} a
            WHERE {on}
              AND {archive_age}
          )
        """
    ).collect()[0]["n"]
    spark.sql(
        f"""
        DELETE FROM {meta}.{hot} h
        WHERE {hot_pred}
          AND EXISTS (
            SELECT 1 FROM {meta}.{archive} a
            WHERE {on}
              AND {archive_age}
          )
        """
    )
    return int(doomed)


if __name__ == "__main__":
    main()
