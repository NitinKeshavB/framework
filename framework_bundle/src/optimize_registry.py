"""Daily compaction for the registry tables. This does not delete process history.

With predictive optimization on for the catalog, these OPTIMIZE runs find little to do.
"""

import sys

from pyspark.sql import SparkSession


def main() -> None:
    catalog = sys.argv[1]
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"OPTIMIZE {catalog}.framework_meta.object_registry")
    spark.sql(f"OPTIMIZE {catalog}.framework_meta.run_ledger")
    spark.sql(f"OPTIMIZE {catalog}.framework_meta.object_current")
    spark.sql(f"OPTIMIZE {catalog}.framework_meta.run_current")
    spark.sql(f"OPTIMIZE {catalog}.framework_meta.table_health")


if __name__ == "__main__":
    main()
