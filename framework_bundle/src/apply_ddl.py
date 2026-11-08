"""Apply registry DDL. The bloom-filter statements are skipped when the runtime rejects them."""

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.collector import apply_ddl


def main() -> None:
    catalog = sys.argv[1]
    apply_ddl(SparkSession.builder.getOrCreate(), catalog)


if __name__ == "__main__":
    main()
