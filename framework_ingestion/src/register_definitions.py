"""Write this bundle's definitions into the registry.

Deploy runs this after the upload. It is the only writer of definitions. A changed spec gets a
new SCD Type 2 version; a spec removed from the bundle gets a deletion version. Unity Catalog
tag assignment is parked for phase 1; see APPLY_TAGS in unity.py.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession

from lakeflow_framework.ingestion import registry_rows
from lakeflow_framework.logs import job_logging, step
from lakeflow_framework.registry import append_definitions, retire_definitions
from lakeflow_framework.unity import apply_metadata


def main() -> None:
    catalog = sys.argv[1]
    # Serverless runs this file through exec(), so __file__ may not exist. The job passes the
    # deployed bundle root; running the file directly uses the bundle next to src/.
    bundle = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).resolve().parent.parent
    log = job_logging("register_definitions")
    log.info("catalog %s, bundle %s", catalog, bundle)
    spark = SparkSession.builder.getOrCreate()
    with step(log, "read the bundle's definitions"):
        rows = registry_rows(bundle, catalog)
    systems = sorted(row["full_name"] for row in rows if row["object_kind"] == "source_system")
    log.info("%d definition(s) from source system(s) %s", len(rows), ", ".join(systems))
    with step(log, "write new and changed definitions"):
        written = append_definitions(spark, rows, set(), catalog)
    log.info("registered %d definition version(s) in %s.framework_meta", written, catalog)
    # This bundle owns every source system in the catalog, so anything it no longer
    # deploys was removed from it.
    with step(log, "retire definitions no longer deployed"):
        retire_definitions(spark, rows, catalog)
    # Phase 1 parks tag assignment (tag policies). The call is a no-op until APPLY_TAGS is true.
    with step(log, "apply tags and comments in Unity Catalog"):
        applied = apply_metadata(spark, rows, catalog)
    if applied:
        log.info("applied %d Unity Catalog metadata change(s)", len(applied))
    else:
        log.info("Unity Catalog tags parked for phase 1; registry is up to date")


if __name__ == "__main__":
    main()
