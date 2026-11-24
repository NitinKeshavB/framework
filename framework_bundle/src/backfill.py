"""Reload files of one table, or refresh it, through the table's own rules (framework_backfill job).

Arguments: catalog table mode files date_from date_to dry_run confirm on_fail. A blocked file does not
fail the run: it is recorded FAILED in the ledger and reported as a warning here. on_fail, when given,
judges the reloaded files by that action instead of the table's own, for this run only.
"""

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.backfill import BackfillError, parse_selection, run_backfill, warning_text
from lakeflow_framework.logs import job_logging, step


def _flag(value: str) -> bool:
    return value.strip().lower() in {"true", "1", "yes"}


def main() -> None:
    catalog, table, mode, files, date_from, date_to, dry_run, confirm, on_fail = (
        sys.argv[1:] + [""] * 9
    )[:9]
    log = job_logging(f"backfill.{table}")
    spark = SparkSession.builder.getOrCreate()
    try:
        selection = parse_selection(mode, files, date_from, date_to)
        with step(log, f"backfill {table} ({selection.mode})"):
            result = run_backfill(
                spark,
                catalog,
                table,
                selection,
                dry_run=_flag(dry_run),
                confirm=_flag(confirm),
                on_fail=on_fail,
            )
    except BackfillError as exc:
        raise SystemExit(f"backfill refused: {exc}") from exc
    for line in warning_text(result).splitlines():
        (log.warning if line.startswith("WARNING") else log.info)(line)


if __name__ == "__main__":
    main()
