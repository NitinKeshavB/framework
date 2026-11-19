"""Guards for public API surface, package data, and shared SQL helpers."""

from __future__ import annotations

from lakeflow_framework import LoadMethod
from lakeflow_framework.metadata import ddl_text
from lakeflow_framework.sources import PACKAGE_VERSION, PERFORMANCE_TARGET
from lakeflow_framework.spark_types import require_writer
from lakeflow_framework.sqlnames import TYPE_NAME, sql_string


def test_load_method_exports_mvp_and_frozen_names():
    assert LoadMethod.VOLUME_FILES == "volume_files"
    assert LoadMethod.DATABASE_TABLE == "database_table"
    assert LoadMethod.AUTOLOADER == "autoloader"
    assert LoadMethod.COPY_INTO == "copy_into"
    assert LoadMethod.CTAS == "ctas"


def test_ddl_text_ships_as_package_data():
    text = ddl_text()
    assert "CREATE TABLE IF NOT EXISTS" in text
    assert "asset_registry" in text
    assert "ownership_refusals" in text
    assert "CREATE TABLE IF NOT EXISTS ${catalog}.framework_meta.run_ledger" not in text


def test_jobs_default_to_performance_target_and_pinned_wheel_version():
    # TEMPORARY until go-live: PERFORMANCE_OPTIMIZED. Back to "STANDARD" at go-live.
    assert PERFORMANCE_TARGET == "PERFORMANCE_OPTIMIZED"
    assert PACKAGE_VERSION == "0.1.11"


def test_sql_string_doubles_quotes_not_backslashes():
    assert sql_string("it's") == "'it''s'"
    assert TYPE_NAME.match("decimal(10, 2)")
    assert not TYPE_NAME.match("decimal(10;2)")


def test_require_writer_rejects_silent_no_op():
    try:
        require_writer(None)
    except RuntimeError as exc:
        assert "createDataFrame" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
