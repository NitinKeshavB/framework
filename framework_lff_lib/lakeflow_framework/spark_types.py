"""Optional ``pyspark.sql.types`` and a narrow writer check for registry writes.

Package tests run without Spark installed. Production paths must refuse a silent no-op
when ``createDataFrame`` is missing (e.g. ``retire_definitions`` reporting success).
"""

from __future__ import annotations

from types import ModuleType
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class DataFrameWriter(Protocol):
    """Minimum surface registry writers need from a SparkSession (or test double)."""

    def createDataFrame(self, data: Any, schema: Any = None) -> Any: ...


def types_module() -> ModuleType | None:
    """Return ``pyspark.sql.types``, or ``None`` when Spark is not installed."""

    try:
        from pyspark.sql import types
    except ImportError:
        return None
    return types


def require_writer(spark: Any) -> DataFrameWriter:
    """Return ``spark`` when it can stage DataFrames; raise otherwise (no silent skip)."""

    if spark is None or not hasattr(spark, "createDataFrame"):
        raise RuntimeError(
            "a SparkSession with createDataFrame is required for registry and metrics writes"
        )
    return spark


def string_schema(columns: list[str] | tuple[str, ...]) -> Any | None:
    """Nullable-string StructType when pyspark is present; else ``None`` for test doubles.

    Callers must pass the result to ``createDataFrame`` on a session that already passed
    ``require_writer``. Inferring a schema in production is avoided whenever types load.
    """

    t = types_module()
    if t is None:
        return None
    return t.StructType([t.StructField(column, t.StringType(), True) for column in columns])
