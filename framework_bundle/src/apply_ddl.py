"""Apply the registry DDL shipped in this bundle."""

import sys
from pathlib import Path

from pyspark.sql import SparkSession

from lakeflow_framework.collector import apply_ddl


def main() -> None:
    # Serverless runs this file through exec(), so __file__ does not exist. The job passes the SQL
    # path as the second argument; running the file directly still works without it.
    catalog = sys.argv[1]
    if len(sys.argv) > 2:
        sql_file = Path(sys.argv[2])
    else:
        sql_file = Path(__file__).resolve().parent.parent / "sql" / "framework_meta.sql"
    apply_ddl(
        SparkSession.builder.getOrCreate(),
        catalog,
        sql_file.read_text(encoding="utf-8"),
    )


if __name__ == "__main__":
    main()
