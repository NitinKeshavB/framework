"""Shared SQL identifier and literal helpers.

One place for catalog/identifier checks and SQL string quoting so writers do not diverge
(backslashes vs doubled quotes) and regex copies do not drift.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# decimal(10, 2) and similar — name, optional precision/scale only.
TYPE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*(\(\d+(,\s*\d+)?\))?$")
SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
SAFE_NAME = re.compile(r"^[A-Za-z0-9_.>-]+$")


def require_catalog(catalog: str) -> str:
    if not catalog or not IDENTIFIER.match(catalog):
        raise ValueError("catalog must be an identifier")
    return catalog


def require_identifier(value: str, *, label: str = "name") -> str:
    if not value or not IDENTIFIER.match(value):
        raise ValueError(f"{label} must be an identifier")
    return value


def require_token(value: str, *, label: str = "token") -> str:
    if not value or not SAFE_TOKEN.match(value):
        raise ValueError(f"{label} is required")
    return value


def quote_name(name: str) -> str:
    """Backtick-quote a SQL identifier; escape embedded backticks."""

    return "`" + str(name).replace("`", "``") + "`"


def sql_string(value: str) -> str:
    """Single-quoted SQL string literal (standard doubled quotes, not backslash escapes)."""

    return "'" + str(value).replace("'", "''") + "'"


def sql_in(values: Iterable[str]) -> str:
    """Comma-joined quoted literals for an IN list (values already validated)."""

    return ", ".join(sql_string(value) for value in values)


def spark_rows(frame) -> list[dict]:
    """Collect a Spark DataFrame (or list) into plain dicts."""

    collected = frame.collect() if hasattr(frame, "collect") else list(frame)
    return [row if isinstance(row, dict) else row.asDict() for row in collected]


def merge_sql(
    table: str,
    source: str,
    keys: tuple[str, ...],
    columns: tuple[str, ...],
    *,
    newer_than: str | None = None,
    dedupe_source: bool = True,
) -> str:
    """MERGE helper. When ``dedupe_source`` is true, QUALIFY keeps one source row per key."""

    on = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    matched = f" AND s.{newer_than} >= t.{newer_than}" if newer_than else ""
    assignments = ", ".join(f"t.{column} = s.{column}" for column in columns)
    values = ", ".join(f"s.{column}" for column in columns)
    if dedupe_source:
        order = newer_than or keys[0]
        partition = ", ".join(keys)
        source = (
            f"SELECT * FROM ({source}) _src "
            f"QUALIFY row_number() OVER (PARTITION BY {partition} ORDER BY {order} DESC) = 1"
        )
    return (
        f"MERGE INTO {table} t USING ({source}) s ON {on} "
        f"WHEN MATCHED{matched} THEN UPDATE SET {assignments} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({values})"
    )
