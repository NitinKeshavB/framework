"""Append rows_read / bronze / quarantine counts for this delivery's latest pipeline update.

Runs after refresh_pipeline. Insert only: no MERGE, and no scan of bronze or quarantine tables.
"""

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.load_metrics import record_bronze_loads
from lakeflow_framework.logs import job_logging, step


def main() -> None:
    catalog, job_name = sys.argv[1], sys.argv[2]
    log = job_logging("record_load_metrics")
    log.info("catalog %s, job %s", catalog, job_name)
    with step(log, "append bronze load metrics"):
        written = record_bronze_loads(SparkSession.builder.getOrCreate(), catalog, job_name)
    log.info("appended %d bronze load row(s)", written)


if __name__ == "__main__":
    main()
