"""Apply the registry DDL shipped in this bundle."""

import sys
from pathlib import Path

from pyspark.sql import SparkSession

from lakeflow_framework.logs import job_logging, step
from lakeflow_framework.registry import apply_ddl


def main() -> None:
    # Serverless runs this file through exec(), so __file__ does not exist. The job passes the SQL
    # path as the second argument; running the file directly still works without it.
    catalog = sys.argv[1]
    if len(sys.argv) > 2:
        sql_file = Path(sys.argv[2])
    else:
        sql_file = Path(__file__).resolve().parent.parent / "sql" / "framework_meta.sql"
    # A third argument, force, applies the SQL even when it was applied before.
    force = len(sys.argv) > 3 and sys.argv[3] == "force"
    log = job_logging("apply_ddl")
    log.info("catalog %s, sql file %s, force %s", catalog, sql_file, force)
    with step(log, f"apply {sql_file.name}"):
        ran = apply_ddl(
            SparkSession.builder.getOrCreate(),
            catalog,
            sql_file.read_text(encoding="utf-8"),
            force=force,
        )
    if ran:
        log.info("applied %d statement(s)", ran)
    else:
        log.info("already applied; nothing to do")


if __name__ == "__main__":
    main()
