"""Apply column comments, and schema, table, and column tags, to Unity Catalog.

The registry rows are the declared metadata. registry_sync calls this at deploy after the
definitions are written. Column comments (the descriptions in the specs) are applied. Tags are
parked in phase 1: some workspaces have tag policies on the same keys, and the deploy identity is
not allowed to set those assignments. Set APPLY_TAGS to True (or pass apply=True) once an account
admin has granted that, or removed the policies. Until then the declared tags still land in the
registry and show in v_metadata_drift.

A comment lives on the table, so a full refresh or rebuild of a table drops it. Run registry_sync
again (``databricks bundle run registry_sync``) and it puts them back.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from lakeflow_framework.logs import statement_summary

log = logging.getLogger(__name__)

# Phase 1: do not ALTER tags. Tag policies on these keys fail the deploy identity.
APPLY_TAGS = False
# Column comments need no tag policy, only the right to alter the table, so they are applied.
APPLY_COMMENTS = True

# The tags this framework owns. Other tags on the same schemas, tables, and columns stay.
SCHEMA_TAGS = (
    "domain",
    "owner",
    "owner_email",
    "source_kind",
    "layer",
    "foreign_catalog",
    "engine",
)
# Table record fields that become tags, besides owner, requested_by, and delivery, which are
# registry columns.
TABLE_RECORD_TAGS = (
    "classification",
    "domain",
    "pii",
    "data_domain",
    "team",
    "technical_owner",
    "layer",
    "pipeline_id",
    "job_name",
    "load_method",
)
TABLE_TAGS = (*TABLE_RECORD_TAGS, "owner", "requested_by", "delivery")
COLUMN_TAGS = ("classification", "pii")
ALTER_KEYWORD = {
    "table": "TABLE",
    "streaming_table": "STREAMING TABLE",
    "materialized_view": "MATERIALIZED VIEW",
}
_SAFE_PART = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass
class Declared:
    schema_tags: dict[str, dict[str, str]] = field(default_factory=dict)
    kinds: dict[str, str] = field(default_factory=dict)
    table_tags: dict[str, dict[str, str]] = field(default_factory=dict)
    column_tags: dict[tuple[str, str], dict[str, str]] = field(default_factory=dict)
    comments: dict[tuple[str, str], str] = field(default_factory=dict)
    # Column names as the spec spells them, for the ALTER statement.
    spelled: dict[tuple[str, str], str] = field(default_factory=dict)


@dataclass
class Applied:
    schema_tags: dict[str, dict[str, str]] = field(default_factory=dict)
    table_tags: dict[str, dict[str, str]] = field(default_factory=dict)
    column_tags: dict[tuple[str, str], dict[str, str]] = field(default_factory=dict)
    comments: dict[tuple[str, str], str] = field(default_factory=dict)


def declared_metadata(rows: Iterable[dict[str, Any]]) -> Declared:
    """The tags and comments the registry rows declare."""

    declared = Declared()
    for row in rows:
        record = json.loads(row["config_json"])
        if row["object_kind"] == "schema":
            declared.schema_tags[row["full_name"].lower()] = _present(record.get("tags") or {})
        elif row["object_kind"] in ALTER_KEYWORD:
            table = row["full_name"].lower()
            declared.kinds[table] = row["object_kind"]
            declared.table_tags[table] = _present(
                {
                    **{tag: record.get(tag) for tag in TABLE_RECORD_TAGS},
                    "owner": row["owner"],
                    "requested_by": row.get("requested_by"),
                    "delivery": row.get("delivery"),
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


def lookup_sql(catalog: str, tables: Iterable[str], schemas: Iterable[str] = ()) -> dict[str, str]:
    """The information_schema reads: schemas and their tags, table tags, column tags, columns."""

    names = sorted({name.lower() for name in tables})
    schema_names = sorted({name.lower() for name in schemas})
    if not _SAFE_PART.match(catalog or ""):
        raise ValueError("catalog is required")
    if not names and not schema_names:
        raise ValueError("no schemas or tables to look up")
    for name in names:
        _quoted(name)
    for name in schema_names:
        _quoted_schema(name)
    # A query needs a non-empty IN list, and '' matches no real name.
    listed = ", ".join(f"'{name}'" for name in names) or "''"
    schemas_listed = ", ".join(f"'{name}'" for name in schema_names) or "''"
    schema = f"{catalog}.information_schema"
    schema_tags = ", ".join(f"'{tag}'" for tag in SCHEMA_TAGS)

    def full(catalog_column: str, schema_column: str) -> str:
        return f"lower(concat_ws('.', {catalog_column}, {schema_column}, table_name))"

    tags = ", ".join(f"'{tag}'" for tag in TABLE_TAGS)
    return {
        "schemas": (
            f"SELECT lower(concat_ws('.', catalog_name, schema_name)) AS schema_name "
            f"FROM {schema}.schemata "
            f"WHERE lower(concat_ws('.', catalog_name, schema_name)) IN ({schemas_listed})"
        ),
        "schema_tags": (
            f"SELECT lower(concat_ws('.', catalog_name, schema_name)) AS schema_name, "
            f"tag_name, tag_value FROM {schema}.schema_tags "
            f"WHERE lower(concat_ws('.', catalog_name, schema_name)) IN ({schemas_listed}) "
            f"AND tag_name IN ({schema_tags})"
        ),
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
    """What Unity Catalog holds, and what exists: (table, column) pairs and schema names."""

    applied = Applied()
    for row in rows.get("schema_tags", []):
        applied.schema_tags.setdefault(row["schema_name"], {})[row["tag_name"]] = _text(
            row["tag_value"]
        )
    for row in rows.get("table_tags", []):
        applied.table_tags.setdefault(row["table_name"], {})[row["tag_name"]] = _text(
            row["tag_value"]
        )
    for row in rows.get("column_tags", []):
        key = (row["table_name"], row["column_name"])
        applied.column_tags.setdefault(key, {})[row["tag_name"]] = _text(row["tag_value"])
    existing = set()
    for row in rows["columns"]:
        key = (row["table_name"], row["column_name"])
        existing.add(key)
        if row.get("comment"):
            applied.comments[key] = str(row["comment"])
    existing.update(("schema", row["schema_name"]) for row in rows.get("schemas", []))
    return applied, existing


def plan_statements(
    declared: Declared,
    applied: Applied,
    existing: set,
    *,
    tags: bool = True,
    comments: bool = True,
) -> list[str]:
    """ALTER statements that make Unity Catalog match the declared metadata.

    ``tags`` and ``comments`` choose which kinds are planned. A table or column that does not
    exist yet is skipped; the next run applies it.
    """

    tables = {table for table, _column in existing}
    statements: list[str] = []
    for schema in sorted(declared.schema_tags if tags else []):
        if ("schema", schema) not in existing:
            continue
        statements += _tag_changes(
            f"ALTER SCHEMA {_quoted_schema(schema)}",
            declared.schema_tags[schema],
            applied.schema_tags.get(schema, {}),
            SCHEMA_TAGS,
        )
    for table in sorted(declared.kinds):
        if table not in tables:
            continue
        target = f"ALTER {ALTER_KEYWORD[declared.kinds[table]]} {_quoted(table)}"
        if tags:
            statements += _tag_changes(
                target, declared.table_tags[table], applied.table_tags.get(table, {}), TABLE_TAGS
            )
        for key in sorted(k for k in declared.column_tags if k[0] == table):
            if key not in existing:
                continue
            column = f"{target} ALTER COLUMN `{declared.spelled[key]}`"
            comment = declared.comments.get(key)
            if comments and comment and comment != applied.comments.get(key):
                statements.append(f"{column} COMMENT {_literal(comment)}")
            if tags:
                statements += _tag_changes(
                    column,
                    declared.column_tags[key],
                    applied.column_tags.get(key, {}),
                    COLUMN_TAGS,
                )
    return statements


def apply_metadata(
    spark,
    rows: list[dict[str, Any]],
    catalog: str,
    *,
    apply: bool | None = None,
    comments: bool | None = None,
) -> list[str]:
    """Make Unity Catalog hold the column comments and tags ``rows`` declare. Returns the statements.

    ``apply`` (default ``APPLY_TAGS``) decides tags. ``comments`` (default ``APPLY_COMMENTS``)
    decides column comments. Phase 1 leaves tags false so deploy does not need tag-policy grants,
    and comments true, which need only the right to alter the table. With tags off the tag
    information schemas are not read.
    """

    tags_on = APPLY_TAGS if apply is None else apply
    comments_on = APPLY_COMMENTS if comments is None else comments
    if not (tags_on or comments_on):
        log.info(
            "Unity Catalog tags and comments are parked; "
            "definitions are in the registry, nothing is set on schemas or tables"
        )
        return []
    declared = declared_metadata(rows)
    if not declared.kinds and not (tags_on and declared.schema_tags):
        log.info("Unity Catalog: no table declares tags or comments")
        return []
    if not tags_on:
        log.info("Unity Catalog tags are parked for phase 1; applying column comments only")
    log.info(
        "Unity Catalog: %d schema(s) and %d table(s) declare tags or comments",
        len(declared.schema_tags),
        len(declared.kinds),
    )
    queries = lookup_sql(catalog, declared.kinds, declared.schema_tags if tags_on else [])
    if not tags_on:
        queries = {"columns": queries["columns"]}
    rows = {name: _rows(spark.sql(statement)) for name, statement in queries.items()}
    applied, existing = applied_metadata(rows)
    tables = {table for table, _column in existing}
    for schema in sorted(declared.schema_tags if tags_on else []):
        if ("schema", schema) not in existing:
            log.info("Unity Catalog: schema %s does not exist yet; tagged on a later run", schema)
    for table in sorted(declared.kinds):
        if table not in tables:
            log.info("Unity Catalog: table %s does not exist yet; set on a later run", table)
    statements = plan_statements(declared, applied, existing, tags=tags_on, comments=comments_on)
    if not statements:
        log.info("Unity Catalog: no change to apply")
    for number, statement in enumerate(statements, 1):
        log.info(
            "Unity Catalog change %d/%d: %s",
            number,
            len(statements),
            statement_summary(statement, 200),
        )
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
    present = {key: tag_value(value) for key, value in values.items()}
    return {key: value for key, value in present.items() if value}


def tag_value(value: Any) -> str:
    """A value Unity Catalog and job tags accept: trimmed and at most 255 characters."""

    if value is None:
        return ""
    text = ", ".join(map(str, value)) if isinstance(value, (list, tuple)) else str(value)
    return text.strip()[:255].strip()


def _quoted(full_name: str) -> str:
    parts = full_name.split(".")
    if len(parts) != 3 or not all(_SAFE_PART.match(part) for part in parts):
        raise ValueError(f"not a catalog.schema.table name: {full_name}")
    return ".".join(f"`{part}`" for part in parts)


def _quoted_schema(full_name: str) -> str:
    parts = full_name.split(".")
    if len(parts) != 2 or not all(_SAFE_PART.match(part) for part in parts):
        raise ValueError(f"not a catalog.schema name: {full_name}")
    return ".".join(f"`{part}`" for part in parts)


def _literal(value: str) -> str:
    from lakeflow_framework.sqlnames import sql_string

    return sql_string(value)


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _rows(frame: Any) -> list[dict[str, Any]]:
    from lakeflow_framework.sqlnames import spark_rows

    return spark_rows(frame)
