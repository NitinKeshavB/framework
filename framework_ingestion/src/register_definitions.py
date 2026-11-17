"""Write this bundle's specs into the registry. Deploy runs this after the upload."""

import sys
from pathlib import Path

from pyspark.sql import SparkSession

from lakeflow_framework.collector import append_definitions
from lakeflow_framework.ingestion import registry_rows


def main() -> None:
    catalog = sys.argv[1]
    # Serverless runs this file through exec(), so __file__ may not exist. The job passes the
    # deployed bundle root; running the file directly uses the bundle next to src/.
    bundle = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).resolve().parent.parent
    rows = registry_rows(bundle, catalog)
    written = append_definitions(SparkSession.builder.getOrCreate(), rows, set(), catalog)
    print(f"registered {written} definition(s) in {catalog}.framework_meta")


if __name__ == "__main__":
    main()
