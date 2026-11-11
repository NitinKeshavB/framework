"""Run COPY INTO or CTAS for tables whose raw table is not owned by Auto Loader.

Usage: run_batch.py --catalog <catalog> <spec.yaml> [<spec.yaml> ...]
"""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from lakeflow_framework.config import validate_configs
from lakeflow_framework.pipeline import assemble

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("specs", nargs="+", type=Path)
    args = parser.parse_args()
    configs = validate_configs(args.specs, catalog=args.catalog)
    spark = SparkSession.builder.getOrCreate()
    for config in configs:
        for statement in assemble(config).batch_sql:
            spark.sql(statement)
