"""Write this bundle's definitions into the registry, then their tags into Unity Catalog.

Deploy runs this after the upload. It is the only writer of definitions. A changed spec gets a
new SCD Type 2 version; a spec removed from the bundle gets a deletion version.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession

from lakeflow_framework.ingestion import registry_rows
from lakeflow_framework.registry import append_definitions, retire_definitions
from lakeflow_framework.unity import apply_metadata


def main() -> None:
    catalog = sys.argv[1]
    # Serverless runs this file through exec(), so __file__ may not exist. The job passes the
    # deployed bundle root; running the file directly uses the bundle next to src/.
    bundle = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).resolve().parent.parent
    spark = SparkSession.builder.getOrCreate()
    rows = registry_rows(bundle, catalog)
    written = append_definitions(spark, rows, set(), catalog)
    print(f"registered {written} definition version(s) in {catalog}.framework_meta")
    # This bundle owns every source system in the catalog, so anything it no longer
    # deploys was removed from it.
    retire_definitions(spark, rows, catalog)
    # A schema or table that does not exist yet is skipped and tagged on the next deploy.
    applied = apply_metadata(spark, rows, catalog)
    print(f"applied {len(applied)} Unity Catalog metadata change(s)")


if __name__ == "__main__":
    main()
