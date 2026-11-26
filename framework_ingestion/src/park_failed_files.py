"""Move the files a ``fail`` rule blocked into a ``_failed`` folder at the root of their volume.

Usage: park_failed_files.py --catalog <catalog> <pipeline_id> [<pipeline_id> ...]

Runs after refresh_pipeline in a generated job, whether the refresh succeeded or not: a blocked file
fails the update, and this still has to run. It first records the job's own update in the ledger if
the pipeline's event hook did not, then moves every blocked file (the ledger says FAILED, or the
pipeline's per-file table does) to ``<volume>/_failed/``. Auto Loader skips a folder that starts
with an underscore, so those files are never read again.
"""

import argparse
import logging

from pyspark.sql import SparkSession

from lakeflow_framework.failed_files import park_failed_files
from lakeflow_framework.load_catchup import catch_up_loads

log = logging.getLogger("park_failed_files")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("pipelines", nargs="+")
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    try:
        # The ledger row of this run, when the event hook was cut off before it wrote it.
        written = catch_up_loads(spark, args.catalog, pipeline_ids=args.pipelines, keep_kind=True)
        log.info("recorded %d ledger row(s) the pipeline had not", written)
    except Exception:
        log.exception("could not record this run in the ledger; parking goes on")
    for pipeline_id, moved in park_failed_files(spark, args.catalog, args.pipelines).items():
        log.info("%s: moved %d blocked file(s) to _failed", pipeline_id, len(moved))
