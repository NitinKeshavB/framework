"""Merge this delivery job run into job_runs / job_task_runs for near-real-time status.

Runs after record_load_metrics with run_if ALL_DONE so success and failure both land in
v_job_status without waiting for the hourly framework_runs catch-up.
"""

import sys

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from lakeflow_framework.logs import job_logging, step
from lakeflow_framework.runs import record_this_run


def main() -> None:
    catalog, job_name, run_id = sys.argv[1], sys.argv[2], sys.argv[3]
    log = job_logging("record_this_run")
    log.info("catalog %s, job %s, run_id %s", catalog, job_name, run_id)
    with step(log, "merge this job run into job_runs"):
        written = record_this_run(
            SparkSession.builder.getOrCreate(),
            WorkspaceClient(),
            catalog,
            job_name,
            run_id,
        )
    log.info("recorded %d run(s)", written)


if __name__ == "__main__":
    main()
