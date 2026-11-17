"""Record the framework's job runs from the Jobs API and pipeline updates from the event logs."""

import sys

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from lakeflow_framework.runs import record_job_runs, record_pipeline_updates


def main() -> None:
    catalog, part = sys.argv[1], sys.argv[2]
    spark = SparkSession.builder.getOrCreate()
    if part == "job_runs":
        read = record_job_runs(spark, WorkspaceClient(), catalog)
        print(f"recorded {read} job run(s)")
    elif part == "pipeline_updates":
        read = record_pipeline_updates(spark, catalog)
        print(f"read pipeline updates from {read} event log(s)")
    else:
        raise SystemExit(f"unknown part: {part}")


if __name__ == "__main__":
    main()
