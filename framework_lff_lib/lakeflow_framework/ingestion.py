"""Source-system structure of the ingestion bundle, and the map generated from it.

Layout under ``<bundle>/sources``::

    <source_system>/source.yaml                 kind, owner, deliveries
    <source_system>/<table>.yaml                a spec with no project
    <source_system>/<project>/<table>.yaml      a spec that project is bringing in

``requested_by`` matches the project folder. A spec directly under the source system omits it.
The map is derived only from these files, so it cannot disagree with what is deployed. No
cluster is used.

Implementation is split across ``sources``, ``generate``, and ``scaffold``; this module
re-exports the public surface used by the CLI, projects, and tests.
"""

from __future__ import annotations

from lakeflow_framework.generate import (
    generate,
    job_tags,
    node_id,
    render_files,
    render_map,
    write_generated,
)
from lakeflow_framework.scaffold import new_source, new_table
from lakeflow_framework.sources import (
    BUILD_DIR,
    ENVIRONMENT_VERSION,
    EVENT_LOG_SUFFIX,
    GENERATED,
    GENERATED_DIRS,
    KINDS,
    LOAD_METHODS_BY_KIND,
    MAP_NAME,
    PACKAGE_VERSION,
    PERFORMANCE_TARGET,
    PLACEHOLDER_CATALOG,
    SOURCE_FILE,
    WHEEL,
    WORKFLOW_TAG,
    SourceSystem,
    SourceTable,
    deployed_configs,
    identifier_problems,
    load_sources,
    refuse_existing,
    registry_rows,
    schema_tags,
    source_record,
    source_rows,
    source_schemas,
    workflow_details,
)

__all__ = [
    "BUILD_DIR",
    "ENVIRONMENT_VERSION",
    "EVENT_LOG_SUFFIX",
    "GENERATED",
    "GENERATED_DIRS",
    "KINDS",
    "LOAD_METHODS_BY_KIND",
    "MAP_NAME",
    "PACKAGE_VERSION",
    "PERFORMANCE_TARGET",
    "PLACEHOLDER_CATALOG",
    "SOURCE_FILE",
    "SourceSystem",
    "SourceTable",
    "WHEEL",
    "WORKFLOW_TAG",
    "deployed_configs",
    "generate",
    "identifier_problems",
    "job_tags",
    "load_sources",
    "new_source",
    "new_table",
    "node_id",
    "refuse_existing",
    "registry_rows",
    "render_files",
    "render_map",
    "schema_tags",
    "source_record",
    "source_rows",
    "source_schemas",
    "workflow_details",
    "write_generated",
]
