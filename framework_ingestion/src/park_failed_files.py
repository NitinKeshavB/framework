"""Move the files a ``fail`` rule blocked into a ``_failed`` folder at the root of their volume.

Usage: park_failed_files.py --catalog <catalog> <pipeline_id> [<pipeline_id> ...]

Runs after refresh_pipeline in a generated job, whether the refresh succeeded or not: a blocked file
fails the update, and this still has to run. It reads the ledger for files whose latest status is
FAILED because a fail rule blocked them, and moves each to ``<volume>/_failed/``. Auto Loader
skips a folder that starts with an underscore, so those files are never read again.
"""

import argparse
import logging

from pyspark.sql import SparkSession

from lakeflow_framework.failed_files import park_failed_files

log = logging.getLogger("park_failed_files")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("pipelines", nargs="+")
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    for pipeline_id, moved in park_failed_files(spark, args.catalog, args.pipelines).items():
        log.info("%s: moved %d blocked file(s) to _failed", pipeline_id, len(moved))
