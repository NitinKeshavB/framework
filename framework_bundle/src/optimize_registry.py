"""Daily compaction for registry and recon tables.

Hot tables use liquid clustering / auto-optimize. Archive tables are Hive-partitioned by
archive_quarter; OPTIMIZE … ZORDER keeps within-quarter files efficient.
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.logs import job_logging, step

HOT_TABLES = (
    "asset_registry",
    "asset_registry_current",
    "load_ledger",
    "ledger_detail",
    "job_runs",
    "job_task_runs",
    "pipeline_updates",
)

# (table, optional ZORDER columns for partitioned archives)
ARCHIVE_ZORDER = (
    ("load_ledger_archive", ("layer", "job_name", "dataflow", "started_at")),
    ("ledger_detail_archive", ("dataflow", "status", "recorded_at")),
    ("job_runs_archive", ("job_name", "started_at")),
    ("job_task_runs_archive", ("job_name", "started_at")),
    ("pipeline_updates_archive", ("job_name", "started_at")),
)


def main() -> None:
    catalog = sys.argv[1]
    log = job_logging("optimize_registry")
    log.info("catalog %s", catalog)
    spark = SparkSession.builder.getOrCreate()
    meta = f"{catalog}.framework_meta"
    for table in HOT_TABLES:
        with step(log, f"OPTIMIZE {meta}.{table}"):
            _optimize(spark, log, f"{meta}.{table}")
    for table, zorder in ARCHIVE_ZORDER:
        cols = ", ".join(zorder)
        with step(log, f"OPTIMIZE {meta}.{table} ZORDER BY ({cols})"):
            _optimize(spark, log, f"{meta}.{table}", zorder=cols)


def _optimize(spark, log, full_name: str, zorder: str | None = None) -> None:
    sql = f"OPTIMIZE {full_name}" + (f" ZORDER BY ({zorder})" if zorder else "")
    result = spark.sql(sql).collect()
    metrics = result[0].asDict().get("metrics") if result else None
    if metrics is not None:
        metrics = metrics.asDict() if hasattr(metrics, "asDict") else dict(metrics)
        log.info(
            "%s: %s file(s) removed, %s added",
            full_name,
            metrics.get("numFilesRemoved"),
            metrics.get("numFilesAdded"),
        )


if __name__ == "__main__":
    main()
