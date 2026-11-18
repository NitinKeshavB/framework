"""Append rows_read / written / quarantine counts for this job's latest pipeline update.

Runs after refresh_pipeline. Insert only into the central load_ledger: no MERGE, and no scan of
target tables. Optional third argument is the medallion layer (default bronze).
"""

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.load_metrics import record_loads
from lakeflow_framework.logs import job_logging, step


def main() -> None:
    catalog, job_name = sys.argv[1], sys.argv[2]
    layer = sys.argv[3] if len(sys.argv) > 3 else "bronze"
    log = job_logging("record_load_metrics")
    log.info("catalog %s, job %s, layer %s", catalog, job_name, layer)
    with step(log, f"append {layer} load metrics"):
        written = record_loads(SparkSession.builder.getOrCreate(), catalog, job_name, layer=layer)
    log.info("appended %d load ledger row(s)", written)


if __name__ == "__main__":
    main()
