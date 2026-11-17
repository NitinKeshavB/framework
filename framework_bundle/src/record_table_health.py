"""Record file count, size, and the last load commit of every bronze table."""

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.health import record_table_health
from lakeflow_framework.logs import job_logging, step


def main() -> None:
    catalog = sys.argv[1]
    log = job_logging("record_table_health")
    log.info("catalog %s", catalog)
    with step(log, "record table health"):
        written = record_table_health(SparkSession.builder.getOrCreate(), catalog)
    log.info("recorded health for %d bronze table(s)", written)


if __name__ == "__main__":
    main()
