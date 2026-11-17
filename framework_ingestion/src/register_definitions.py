"""Write this bundle's specs into the registry, then their tags and comments into Unity Catalog.

Deploy runs this after the upload. It is the only writer of definitions.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession

from lakeflow_framework.ingestion import deployed_configs
from lakeflow_framework.metadata import definition_rows
from lakeflow_framework.registry import append_definitions
from lakeflow_framework.unity import apply_metadata


def main() -> None:
    catalog = sys.argv[1]
    # Serverless runs this file through exec(), so __file__ may not exist. The job passes the
    # deployed bundle root; running the file directly uses the bundle next to src/.
    bundle = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).resolve().parent.parent
    spark = SparkSession.builder.getOrCreate()
    configs = deployed_configs(bundle, catalog)
    written = append_definitions(spark, definition_rows(configs), set(), catalog)
    print(f"registered {written} definition(s) in {catalog}.framework_meta")
    # A table whose pipeline has not run yet is skipped and tagged on the next deploy.
    applied = apply_metadata(spark, configs, catalog)
    print(f"applied {len(applied)} Unity Catalog metadata change(s)")


if __name__ == "__main__":
    main()
