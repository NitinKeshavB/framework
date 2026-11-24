"""Register the columns of tables that declare none, after their first load.

Usage: infer_columns.py --catalog <catalog> [--refresh] <pipeline_id> [<pipeline_id> ...]

Runs after refresh_pipeline in each generated job. For a table whose registry holds only the
framework's own columns, it reads bronze's columns from Unity Catalog and registers them. For any
other table it does one SELECT and stops. It never fails the job: a load that worked must not be
marked failed because a column could not be registered, and the next run tries again.
"""

import argparse
import logging

from pyspark.sql import SparkSession

from lakeflow_framework.infer_columns import infer_columns

log = logging.getLogger("infer_columns")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="register again a table whose columns were inferred earlier, to pick up new ones",
    )
    parser.add_argument("pipelines", nargs="+")
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    try:
        done = infer_columns(spark, args.catalog, args.pipelines, refresh=args.refresh)
        for pipeline_id, result in done.items():
            log.info("%s: %s", pipeline_id, result)
    except Exception:
        log.exception("could not register the inferred columns; the load itself is unaffected")
