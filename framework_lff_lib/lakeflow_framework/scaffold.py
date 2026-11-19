"""Scaffold source.yaml and table specs under the ingestion sources tree."""

from __future__ import annotations

from pathlib import Path

import yaml

from lakeflow_framework.config import (
    DATABASE_KIND,
    EMAIL,
    FILE_KINDS,
    ConfigValidationError,
)
from lakeflow_framework.sources import (
    ENGINES,
    KINDS,
    SOURCE_FILE,
    identifier_problems,
    refuse_existing,
)


def new_source(
    bundle: Path, domain: str, source_system: str, *, owner: str, kind: str, delivery: str
) -> Path:
    """Create ``source.yaml`` for a new source system, in the folder the layout expects."""

    _names({"domain": domain, "source_system": source_system, "delivery": delivery})
    if kind not in KINDS:
        raise ConfigValidationError([f"kind must be one of {', '.join(KINDS)}"])
    if not EMAIL.match(owner):
        raise ConfigValidationError(["owner must be an email address"])
    target = bundle / "sources" / source_system / SOURCE_FILE
    refuse_existing(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    head = (
        f"source_system: {source_system}\ndomain: {domain}\nowner: {owner}\nkind: {kind}\n"
        "# Optional. Application id of this source's service principal. When set, the generated\n"
        "# job and pipeline run_as that principal — grant it only this source's raw volume,\n"
        "# foreign catalog, and bronze schema.\n"
        "# run_as: 00000000-0000-0000-0000-000000000000\n\n"
    )
    if kind in FILE_KINDS:
        body = (
            "# Files land in volumes of this schema as delivered. Bronze reads them from there.\n"
            "# Each table spec names its own volume, next to its file settings.\n"
            f"raw_volume:\n  schema: {source_system}_raw\n\n"
            "# A delivery fires when files arrive in the volume its tables read, or on a\n"
            "# schedule instead:\n"
            '#   schedule: "<quartz cron>"\n'
            f"deliveries:\n  {delivery}:\n    trigger: file_arrival\n"
        )
    elif kind == DATABASE_KIND:
        body = (
            "# The catalog that exposes the database (Lakehouse Federation).\n"
            "foreign_catalog: TODO\n"
            f"engine: postgresql  # one of {', '.join(ENGINES)}\n\n"
            f'deliveries:\n  {delivery}:\n    schedule: "0 0 2 * * ?"\n'
        )
    else:
        body = (
            f"deliveries:\n  {delivery}:\n    trigger: file_arrival\n"
            f"    path: abfss://landing@TODO.dfs.core.windows.net/{source_system}/\n"
        )
    target.write_text(head + body, encoding="utf-8")
    return target


def new_table(
    bundle: Path,
    domain: str,
    source_system: str,
    table: str,
    *,
    requested_by: str | None,
    delivery: str | None,
) -> Path:
    """Create a table spec under the project, or next to source.yaml when there is no project."""

    _names({"domain": domain, "source_system": source_system, "table": table})
    if requested_by:
        _names({"requested_by": requested_by})
    folder = bundle / "sources" / source_system
    source_file = folder / SOURCE_FILE
    if not source_file.exists():
        raise ConfigValidationError(
            [f"{source_file} is missing; run new-source for {source_system} first"]
        )
    found = yaml.safe_load(source_file.read_text(encoding="utf-8")) or {}
    deliveries = sorted((found.get("deliveries") or {}).keys())
    if delivery is None:
        if len(deliveries) != 1:
            raise ConfigValidationError(
                [
                    f"{source_system} has several deliveries ({', '.join(deliveries)}); pass --delivery"
                ]
            )
        delivery = deliveries[0]
    elif delivery not in deliveries:
        raise ConfigValidationError([f"delivery must be one of {', '.join(deliveries)}"])
    owner = str(found.get("owner") or "TODO@example.com")
    target_dir = folder / requested_by if requested_by else folder
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{source_system}_{table}.yaml"
    refuse_existing(target)
    kind = str(found.get("kind") or "")
    template = {
        "database": _DATABASE_TEMPLATE,
        "sftp_files": _VOLUME_TEMPLATE,
        "sharepoint_files": _VOLUME_TEMPLATE,
    }.get(kind, _TABLE_TEMPLATE)
    volume = found.get("raw_volume") if isinstance(found.get("raw_volume"), dict) else {}
    target.write_text(
        template.format(
            source=source_system,
            table=table,
            domain=domain,
            owner=owner,
            requested_by=requested_by or source_system,
            requested_by_line=(f"requested_by: {requested_by}\n" if requested_by else ""),
            delivery=delivery,
            delivery_line=(f"delivery: {delivery}\n" if len(deliveries) > 1 else ""),
            volume=str(volume.get("volume") or "TODO_volume"),
        ),
        encoding="utf-8",
    )
    return target


_TABLE_TEMPLATE = """\
pipeline_id: {source}_{table}
domain: {domain}
data_domain: {table}
description: TODO describe the table
team: {requested_by}
technical_owner: {owner}
layer: bronze
job_name: {source}_{delivery}
delivery: {delivery}
{requested_by_line}
data:
  classification: internal

source_to_raw:
  method: autoloader
  source:
    system: {source}
    format: csv
    path: abfss://landing@TODO.dfs.core.windows.net/{source}/{table}/
    delimiter: ","
    header: true
  raw:
    schema: {source}_raw
    table: {table}

raw_to_bronze:
  bronze:
    schema: {source}_bronze
    table: {table}
  columns:
    - name: TODO_column_name
      type: string
      description: TODO describe the column

# Rows that fail a rule go to <bronze table>_qrntn in the bronze schema. Add a quarantine:
# block only to override its table, schema, classification, or pii.
"""


# domain, owner, team, layer, job, and requester come from source.yaml and the folder.
_VOLUME_TEMPLATE = """\
pipeline_id: {source}_{table}
data_domain: {table}
description: TODO describe the table
{delivery_line}
data:
  classification: internal

# The files for this table: a volume in the source system's raw schema, and a folder in it.
source:
  volume: {volume}
  raw_path: {table}/
  format: csv
  delimiter: ","
  header: true

raw_to_bronze:
  bronze:
    schema: {source}_bronze
    table: {table}
  columns:
    - name: TODO_column_name
      type: string
      description: TODO describe the column

# Rows that fail a rule go to <bronze table>_qrntn in the bronze schema. Add a quarantine:
# block only to override its table, schema, classification, or pii.
"""

_DATABASE_TEMPLATE = """\
pipeline_id: {source}_{table}
data_domain: {table}
description: TODO describe the table
{delivery_line}
data:
  classification: internal

# The table in the source database. snapshot mirrors the source each run. With primary_keys under
# raw_to_bronze, a row the source no longer has is kept and closed, not deleted (soft delete).
# merge keeps bronze in step with the source and needs primary_keys.
source:
  table: TODO_schema.{table}
  mode: snapshot

raw_to_bronze:
  bronze:
    schema: {source}_bronze
    table: {table}
  columns:
    - name: TODO_column_name
      type: string
      description: TODO describe the column

# Rows that fail a rule go to <bronze table>_qrntn in the bronze schema. Add a quarantine:
# block only to override its table, schema, classification, or pii.
"""


def _names(values: dict[str, str]) -> None:
    bad = identifier_problems(values)
    if bad:
        raise ConfigValidationError(bad)
