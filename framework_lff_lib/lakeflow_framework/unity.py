"""Apply each spec's table tags, column tags, and column comments to Unity Catalog.

The registry rows are the declared metadata. A job task runs this after the pipeline refresh,
when the tables exist. It reads what Unity Catalog already holds and changes only what
differs, so an unchanged spec costs three lookups and no ALTER statements.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

from lakeflow_framework.config import PipelineConfig
from lakeflow_framework.metadata import object_rows

# The tags this framework owns. Other tags on the same tables are left alone.
TABLE_TAGS = ("classification", "domain", "owner", "pii")
COLUMN_TAGS = ("classification", "pii")
ALTER_KEYWORD = {
    "table": "TABLE",
    "streaming_table": "STREAMING TABLE",
    "materialized_view": "MATERIALIZED VIEW",
}
_SAFE_PART = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass
class Declared:
    kinds: dict[str, str] = field(default_factory=dict)
    table_tags: dict[str, dict[str, str]] = field(default_factory=dict)
    column_tags: dict[tuple[str, str], dict[str, str]] = field(default_factory=dict)
    comments: dict[tuple[str, str], str] = field(default_factory=dict)
    # Column names as the spec spells them, for the ALTER statement.
    spelled: dict[tuple[str, str], str] = field(default_factory=dict)


@dataclass
class Applied:
    table_tags: dict[str, dict[str, str]] = field(default_factory=dict)
    column_tags: dict[tuple[str, str], dict[str, str]] = field(default_factory=dict)
    comments: dict[tuple[str, str], str] = field(default_factory=dict)


def declared_metadata(configs: Iterable[PipelineConfig]) -> Declared:
    declared = Declared()
    for config in configs:
        for row in object_rows(config):
            record = json.loads(row["config_json"])
            if row["object_kind"] in ALTER_KEYWORD:
                table = row["full_name"].lower()
                declared.kinds[table] = row["object_kind"]
                declared.table_tags[table] = _present(
                    {
                        "classification": record.get("classification"),
                        "domain": record.get("domain"),
                        "owner": row["owner"],
                        "pii": record.get("pii"),
                    }
                )
            elif row["object_kind"] == "column":
                key = (str(row["parent_full_name"]).lower(), str(record["name"]).lower())
                declared.spelled[key] = str(record["name"])
                declared.column_tags[key] = _present(
                    {"classification": record.get("classification"), "pii": record.get("pii")}
                )
                if record.get("description"):
                    declared.comments[key] = str(record["description"])
    return declared


def lookup_sql(catalog: str, tables: Iterable[str]) -> dict[str, str]:
    """The information_schema reads for these tables: table tags, column tags, and columns."""

    names = sorted({name.lower() for name in tables})
    if not _SAFE_PART.match(catalog or ""):
        raise ValueError("catalog is required")
    if not names:
        raise ValueError("no tables to look up")
    for name in names:
        _quoted(name)
    listed = ", ".join(f"'{name}'" for name in names)
    schema = f"{catalog}.information_schema"

    def full(catalog_column: str, schema_column: str) -> str:
        return f"lower(concat_ws('.', {catalog_column}, {schema_column}, table_name))"

    tags = ", ".join(f"'{tag}'" for tag in TABLE_TAGS)
    return {
        "table_tags": (
            f"SELECT {full('catalog_name', 'schema_name')} AS table_name, tag_name, tag_value "
            f"FROM {schema}.table_tags "
            f"WHERE {full('catalog_name', 'schema_name')} IN ({listed}) AND tag_name IN ({tags})"
        ),
        "column_tags": (
            f"SELECT {full('catalog_name', 'schema_name')} AS table_name, "
            f"lower(column_name) AS column_name, tag_name, tag_value "
            f"FROM {schema}.column_tags "
            f"WHERE {full('catalog_name', 'schema_name')} IN ({listed}) AND tag_name IN ({tags})"
        ),
        "columns": (
            f"SELECT {full('table_catalog', 'table_schema')} AS table_name, "
            f"lower(column_name) AS column_name, comment "
            f"FROM {schema}.columns "
            f"WHERE {full('table_catalog', 'table_schema')} IN ({listed})"
        ),
    }


def applied_metadata(rows: dict[str, list[dict[str, Any]]]) -> tuple[Applied, set]:
    """What Unity Catalog holds, and the (table, column) pairs that exist."""

    applied = Applied()
    for row in rows["table_tags"]:
        applied.table_tags.setdefault(row["table_name"], {})[row["tag_name"]] = _text(
            row["tag_value"]
        )
    for row in rows["column_tags"]:
        key = (row["table_name"], row["column_name"])
        applied.column_tags.setdefault(key, {})[row["tag_name"]] = _text(row["tag_value"])
    existing = set()
    for row in rows["columns"]:
        key = (row["table_name"], row["column_name"])
        existing.add(key)
        if row.get("comment"):
            applied.comments[key] = str(row["comment"])
    return applied, existing


def plan_statements(declared: Declared, applied: Applied, existing: set) -> list[str]:
    """ALTER statements that make Unity Catalog match the declared metadata.

    A table or column that does not exist yet is skipped; the next run applies it.
    """

    tables = {table for table, _column in existing}
    statements: list[str] = []
    for table in sorted(declared.kinds):
        if table not in tables:
            continue
        target = f"ALTER {ALTER_KEYWORD[declared.kinds[table]]} {_quoted(table)}"
        statements += _tag_changes(
            target, declared.table_tags[table], applied.table_tags.get(table, {}), TABLE_TAGS
        )
        for key in sorted(k for k in declared.column_tags if k[0] == table):
            if key not in existing:
                continue
            column = f"{target} ALTER COLUMN `{declared.spelled[key]}`"
            comment = declared.comments.get(key)
            if comment and comment != applied.comments.get(key):
                statements.append(f"{column} COMMENT {_literal(comment)}")
            statements += _tag_changes(
                column, declared.column_tags[key], applied.column_tags.get(key, {}), COLUMN_TAGS
            )
    return statements


def apply_metadata(spark, configs: list[PipelineConfig], catalog: str) -> list[str]:
    declared = declared_metadata(configs)
    if not declared.kinds:
        return []
    queries = lookup_sql(catalog, declared.kinds)
    rows = {name: _rows(spark.sql(statement)) for name, statement in queries.items()}
    applied, existing = applied_metadata(rows)
    statements = plan_statements(declared, applied, existing)
    for statement in statements:
        spark.sql(statement)
    return statements


def _tag_changes(
    target: str, wanted: dict[str, str], held: dict[str, str], managed: tuple[str, ...]
) -> list[str]:
    changes: list[str] = []
    stale = sorted(tag for tag in held if tag in managed and tag not in wanted)
    if stale:
        changes.append(f"{target} UNSET TAGS ({', '.join(_literal(tag) for tag in stale)})")
    changed = {tag: value for tag, value in sorted(wanted.items()) if held.get(tag) != value}
    if changed:
        pairs = ", ".join(f"{_literal(tag)} = {_literal(value)}" for tag, value in changed.items())
        changes.append(f"{target} SET TAGS ({pairs})")
    return changes


def _present(values: dict[str, Any]) -> dict[str, str]:
    return {key: str(value) for key, value in values.items() if value not in (None, "", [])}


def _quoted(full_name: str) -> str:
    parts = full_name.split(".")
    if len(parts) != 3 or not all(_SAFE_PART.match(part) for part in parts):
        raise ValueError(f"not a catalog.schema.table name: {full_name}")
    return ".".join(f"`{part}`" for part in parts)


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _rows(frame: Any) -> list[dict[str, Any]]:
    collected = frame.collect() if hasattr(frame, "collect") else list(frame)
    return [row if isinstance(row, dict) else row.asDict() for row in collected]
