"""Write this bundle's definitions into the registry.

Deploy runs this after the upload. It is the only writer of definitions. A changed spec gets a
new SCD Type 2 version; a spec removed from this bundle's owners gets a deletion version.
Retirement is scoped to the source systems (owners) this bundle deploys, so the ingestion
layout can be sharded per source or domain without wiping another bundle's registry rows.
Unity Catalog tag assignment is parked for phase 1; see APPLY_TAGS in unity.py.
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
    bundle_id = sys.argv[3] if len(sys.argv) > 3 else bundle.name
    log = job_logging("register_definitions")
    log.info("catalog %s, bundle %s, bundle_id %s", catalog, bundle, bundle_id)
    spark = SparkSession.builder.getOrCreate()
    with step(log, "read the bundle's definitions"):
        rows = registry_rows(bundle, catalog, bundle_id=bundle_id)
    systems = sorted(row["full_name"] for row in rows if row["object_kind"] == "source_system")
    log.info("%d definition(s) from source system(s) %s", len(rows), ", ".join(systems))
    with step(log, "write new and changed definitions"):
        written = append_definitions(spark, rows, set(), catalog)
    log.info("registered %d definition version(s) in %s.framework_meta", written, catalog)
    # Retire only among this bundle's owners — never another shard's source systems.
    with step(log, "retire definitions no longer deployed by this bundle's owners"):
        retire_definitions(spark, rows, catalog)
    # Column comments (the descriptions) are applied. Tag assignment is parked in phase 1 (tag
    # policies) until APPLY_TAGS is true.
    with step(log, "apply column comments in Unity Catalog (tags are parked)"):
        applied = apply_metadata(spark, rows, catalog)
    if applied:
        log.info("applied %d Unity Catalog metadata change(s)", len(applied))
    else:
        log.info("Unity Catalog column comments are up to date; tags are parked for phase 1")


if __name__ == "__main__":
    main()
