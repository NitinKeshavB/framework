"""Record job runs (system.lakeflow, Jobs API fallback) and pipeline updates from event logs."""

import sys

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from lakeflow_framework.logs import job_logging, step
from lakeflow_framework.runs import record_job_runs, record_pipeline_updates


def main() -> None:
    catalog, part = sys.argv[1], sys.argv[2]
    log = job_logging(f"record_runs.{part}")
    log.info("catalog %s, part %s", catalog, part)
    spark = SparkSession.builder.getOrCreate()
    if part == "job_runs":
        with step(log, "record job runs from system.lakeflow or Jobs API"):
            read = record_job_runs(spark, WorkspaceClient(), catalog)
        log.info("recorded %d job run(s)", read)
    elif part == "pipeline_updates":
        with step(log, "record pipeline updates from the event logs"):
            read = record_pipeline_updates(spark, catalog)
        log.info("read pipeline updates from %d event log(s)", read)
    else:
        raise SystemExit(f"unknown part: {part}")


if __name__ == "__main__":
    main()
