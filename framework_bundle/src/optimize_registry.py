"""Daily compaction for the registry tables. This does not delete process history.

With predictive optimization on for the catalog, these OPTIMIZE runs find little to do.
"""

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.logs import job_logging, step

TABLES = (
    "asset_registry",
    "run_ledger",
    "asset_registry_current",
    "run_current",
    "load_ledger",
    "job_runs",
    "job_task_runs",
    "pipeline_updates",
    "load_ledger_archive",
    "job_runs_archive",
)


def main() -> None:
    catalog = sys.argv[1]
    log = job_logging("optimize_registry")
    log.info("catalog %s", catalog)
    spark = SparkSession.builder.getOrCreate()
    for table in TABLES:
        with step(log, f"OPTIMIZE {catalog}.framework_meta.{table}"):
            result = spark.sql(f"OPTIMIZE {catalog}.framework_meta.{table}").collect()
        metrics = result[0].asDict().get("metrics") if result else None
        if metrics is not None:
            metrics = metrics.asDict() if hasattr(metrics, "asDict") else dict(metrics)
            log.info(
                "%s: %s file(s) removed, %s added",
                table,
                metrics.get("numFilesRemoved"),
                metrics.get("numFilesAdded"),
            )


if __name__ == "__main__":
    main()
