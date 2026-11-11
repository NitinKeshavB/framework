"""Apply the registry DDL shipped in this bundle."""

import sys
from pathlib import Path

from pyspark.sql import SparkSession

from lakeflow_framework.collector import apply_ddl


def main() -> None:
    catalog = sys.argv[1]
    sql_file = Path(__file__).resolve().parent.parent / "sql" / "framework_meta.sql"
    apply_ddl(
        SparkSession.builder.getOrCreate(),
        catalog,
        sql_file.read_text(encoding="utf-8"),
    )


if __name__ == "__main__":
    main()
