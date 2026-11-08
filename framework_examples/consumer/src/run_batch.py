"""Run COPY INTO or CTAS for pipelines whose raw table is not owned by Auto Loader."""

import sys
from pathlib import Path

from pyspark.sql import SparkSession

from lakeflow_framework.config import validate_configs
from lakeflow_framework.pipeline import assemble

if __name__ == "__main__":
    configs = validate_configs([Path(argument) for argument in sys.argv[1:]])
    spark = SparkSession.builder.getOrCreate()
    for config in configs:
        for statement in assemble(config).batch_sql:
            spark.sql(statement)
