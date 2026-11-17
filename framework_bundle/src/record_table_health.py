"""Record file count, size, and the last load commit of every bronze table."""

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.health import record_table_health


def main() -> None:
    catalog = sys.argv[1]
    written = record_table_health(SparkSession.builder.getOrCreate(), catalog)
    print(f"recorded health for {written} bronze table(s)")


if __name__ == "__main__":
    main()
