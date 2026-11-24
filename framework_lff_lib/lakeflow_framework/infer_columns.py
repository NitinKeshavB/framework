"""Register the columns of a table whose spec declares none, once, from what bronze really has.

A spec that leaves out ``columns`` registers only the columns the framework adds (``_batch_id``
and the like), because nothing else is known until Auto Loader has read a file. After the first
load, ``infer_columns`` reads the table's columns from Unity Catalog and registers them, marked
``source = inferred``. It does nothing when the registry already holds a column that is not one of
the framework's own, whether the spec declared it or an earlier run inferred it, so it works once.

``registry_sync`` runs on every deploy and rebuilds column rows from the spec. For a spec with no
columns that would retire the inferred ones, so it passes its rows through ``keep_inferred_columns``
first. A spec that starts declaring columns is not touched: its own rows win, and the inferred ones
are retired like any other definition the spec no longer has.

Registered columns are documentation (comments, tags, lineage). They never feed back into how
Auto Loader reads the next file. For csv, delimited and json the pipeline runs Auto Loader in
``rescue`` mode, so a column that appears later is not added to bronze: its values go to
``_rescued_data``, and the registered columns stay right. They are registered once and not
refreshed, so a format whose table does gain columns (parquet, avro) would leave them stale.
"""

from __future__ import annotations

import logging
from typing import Any

from lakeflow_framework.metadata import (
    INFERRED_SOURCE,
    OBJECT_COLUMNS,
    metadata_column_names,
    observed_column_rows,
)
from lakeflow_framework.registry import append_definitions
from lakeflow_framework.sqlnames import require_catalog, require_identifier, sql_in, sql_string
from lakeflow_framework.sqlnames import spark_rows as _rows

log = logging.getLogger(__name__)


def only_metadata_columns(names: list[str]) -> bool:
    """True when every registered column is one the framework adds, or none is registered."""

    return all(name in metadata_column_names() for name in names)


def column_name(row: dict[str, Any]) -> str:
    """The column's own name: the registry names a column ``<table>.<column>``."""

    parent = row.get("parent_full_name") or ""
    full = str(row["full_name"])
    return full[len(parent) + 1 :] if parent and full.startswith(f"{parent}.") else full


def registered_columns(
    spark: Any, catalog: str, tables: list[str]
) -> dict[str, list[tuple[str, str]]]:
    """The (column name, source) pairs the registry holds now, by table."""

    found: dict[str, list[tuple[str, str]]] = {table: [] for table in tables}
    if not tables:
        return found
    rows = _rows(
        spark.sql(
            "SELECT parent_full_name, full_name, source "
            f"FROM {require_catalog(catalog)}.framework_meta.asset_registry_current "
            f"WHERE object_kind = 'column' AND parent_full_name IN ({sql_in(tables)})"
        )
    )
    for row in rows:
        found[str(row["parent_full_name"])].append((column_name(row), str(row.get("source"))))
    return found


def observed_columns(spark: Any, table: str) -> list[tuple[str, str]]:
    """(name, type) of each column of a table, in table order. Empty when the table is not there."""

    catalog, schema, name = table.split(".")
    rows = _rows(
        spark.sql(
            "SELECT column_name, full_data_type "
            f"FROM {require_catalog(catalog)}.information_schema.columns "
            f"WHERE table_schema = {sql_string(require_identifier(schema))} "
            f"AND table_name = {sql_string(require_identifier(name))} "
            "ORDER BY ordinal_position"
        )
    )
    return [(str(row["column_name"]), str(row["full_data_type"])) for row in rows]


def infer_columns(
    spark: Any, catalog: str, pipeline_ids: list[str], *, refresh: bool = False
) -> dict[str, str]:
    """Register the columns of each table that has none of its own yet. Returns what happened.

    The result maps a pipeline id to ``registered``, ``already registered``, ``declared in the
    spec``, ``not loaded yet``, or ``nothing to register``. One SELECT decides it for a table that
    is already done.

    ``refresh`` registers again a table whose columns were inferred earlier, so a column added to
    bronze since then is picked up (a column that changed type gets a new version). It never
    touches a table whose spec declares columns, and it does not retire a column bronze has lost.
    """

    from lakeflow_framework.backfill import load_config

    outcome: dict[str, str] = {}
    for pipeline_id in pipeline_ids:
        config = load_config(spark, catalog, pipeline_id)
        layers = [layer for layer in ("bronze", "quarantine") if layer in config.layers]
        tables = [config.table_name(layer) for layer in layers]
        held = [
            pair for pairs in registered_columns(spark, catalog, tables).values() for pair in pairs
        ]
        if any(
            name not in metadata_column_names() and source != INFERRED_SOURCE
            for name, source in held
        ):
            outcome[pipeline_id] = "declared in the spec"
            continue
        if not refresh and not only_metadata_columns([name for name, _source in held]):
            outcome[pipeline_id] = "already registered"
            continue
        observed = {layer: observed_columns(spark, config.table_name(layer)) for layer in layers}
        observed = {layer: found for layer, found in observed.items() if found}
        if "bronze" not in observed:
            outcome[pipeline_id] = "not loaded yet"
            continue
        if only_metadata_columns([name for name, _type in observed["bronze"]]):
            outcome[pipeline_id] = "nothing to register"
            continue
        rows = observed_column_rows(config, observed)
        written = append_definitions(spark, rows, set(), catalog)
        log.info(
            "%s: registered %d column version(s) from %s", pipeline_id, written, ", ".join(tables)
        )
        outcome[pipeline_id] = "registered"
    return outcome


def keep_inferred_columns(
    spark: Any, rows: list[dict[str, Any]], catalog: str
) -> list[dict[str, Any]]:
    """The rows a deploy should write: the spec's, except that a table which declares no columns
    keeps the inferred ones the registry already holds.

    Without this, the next deploy would see the inferred columns as no longer defined and retire
    them. A table whose spec now declares columns keeps the spec's rows alone.
    """

    spec_columns: dict[str, list[str]] = {}
    for row in rows:
        if row["object_kind"] == "column":
            spec_columns.setdefault(str(row["parent_full_name"]), []).append(column_name(row))
    bare = [table for table, names in spec_columns.items() if only_metadata_columns(names)]
    if not bare:
        return rows
    inferred = _rows(
        spark.sql(
            f"SELECT {', '.join(OBJECT_COLUMNS)} "
            f"FROM {require_catalog(catalog)}.framework_meta.asset_registry_current "
            f"WHERE object_kind = 'column' AND source = {sql_string(INFERRED_SOURCE)} "
            f"AND parent_full_name IN ({sql_in(bare)})"
        )
    )
    if not inferred:
        return rows
    taken = {str(row["parent_full_name"]) for row in inferred}
    kept = [
        row
        for row in rows
        if not (row["object_kind"] == "column" and str(row["parent_full_name"]) in taken)
    ]
    for row in inferred:
        row = {key: row.get(key) for key in OBJECT_COLUMNS}
        stamp = row["recorded_at"]
        row["recorded_at"] = stamp.isoformat() if hasattr(stamp, "isoformat") else stamp
        kept.append(row)
    log.info("kept inferred columns of %d table(s): %s", len(taken), ", ".join(sorted(taken)))
    return kept
