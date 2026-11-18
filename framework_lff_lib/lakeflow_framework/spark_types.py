"""Optional ``pyspark.sql.types`` for code that also runs in package tests without Spark."""

from __future__ import annotations

from types import ModuleType


def types_module() -> ModuleType | None:
    """Return ``pyspark.sql.types``, or ``None`` when Spark is not installed."""

    try:
        from pyspark.sql import types
    except ImportError:
        return None
    return types
