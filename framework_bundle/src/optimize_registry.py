"""Daily compaction for the two registry tables. This does not delete process history."""

import sys

from pyspark.sql import SparkSession


def main() -> None:
    catalog = sys.argv[1]
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"OPTIMIZE {catalog}.framework_meta.object_registry")
    spark.sql(f"OPTIMIZE {catalog}.framework_meta.run_ledger")


if __name__ == "__main__":
    main()
