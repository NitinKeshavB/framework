"""Spark-free tests for applying spec tags and column comments to Unity Catalog."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakeflow_framework.config import config_from_document
from lakeflow_framework.metadata import ddl_statements, framework_object, object_rows
from lakeflow_framework.unity import (
    apply_metadata,
    declared_metadata,
    lookup_sql,
    plan_statements,
)
from test_framework import _billing

BRONZE = "framework_dev.billing_bronze.invoices"
QUOTED = "`framework_dev`.`billing_bronze`.`invoices`"
BUNDLE = Path(__file__).resolve().parents[1] / "framework_bundle"


def _config(**overrides):
    return config_from_document(_billing(**overrides))


def _rows(**overrides):
    return object_rows(_config(**overrides))


def _schema_row(tags):
    scope = {"owner_kind": "source_system", "owner": "billing", "requested_by": None}
    record = {"catalog": "framework_dev", "layers": ["bronze"], "tags": tags}
    return framework_object(
        "schema",
        "framework_dev.billing_bronze",
        {**scope, "delivery": None},
        "billing",
        "contains",
        record,
        "2026-11-17T00:00:00Z",
    )


class _Spark:
    """information_schema answers for bronze only: amount exists and is already in step."""

    def __init__(self, table_tags=None, column_tags=None, columns=None, schema_tags=None):
        self.answers = {
            "schemata": [{"schema_name": "framework_dev.billing_bronze"}],
            "schema_tags": schema_tags or [],
            "table_tags": table_tags or [],
            "column_tags": column_tags or [],
            "columns": columns
            if columns is not None
            else [
                {"table_name": BRONZE, "column_name": "invoice_id", "comment": None},
                {"table_name": BRONZE, "column_name": "amount", "comment": None},
            ],
        }
        self.ran: list[str] = []

    def sql(self, statement):
        for name, rows in self.answers.items():
            if f"information_schema.{name} " in statement:
                return rows
        if "information_schema." in statement:
            raise AssertionError(f"unexpected lookup: {statement}")
        self.ran.append(statement)
        return []


def test_a_new_table_gets_its_tags_and_column_tags():
    spark = _Spark()
    statements = apply_metadata(spark, _rows(), "framework_dev", apply=True)
    assert statements == spark.ran
    (table_tags,) = [s for s in statements if s.startswith(f"ALTER STREAMING TABLE {QUOTED} SET")]
    for pair in (
        "'classification' = 'confidential'",
        "'domain' = 'finance'",
        "'owner' = 'billing'",
        "'pii' = 'pii_finance'",
        "'layer' = 'bronze'",
        "'pipeline_id' = 'billing_invoices'",
        "'load_method' = 'autoloader'",
        "'requested_by' = 'billing'",
    ):
        assert pair in table_tags
    assert (
        f"ALTER STREAMING TABLE {QUOTED} ALTER COLUMN `amount` SET TAGS "
        "('classification' = 'confidential', 'pii' = 'pii_finance')"
    ) in statements
    # Raw and quarantine are not in information_schema yet, so they wait for the next run.
    assert not any("billing_raw" in statement or "qrntn" in statement for statement in statements)


def test_an_unchanged_spec_changes_nothing():
    rows = [*_rows(), _schema_row({"domain": "finance", "owner": "billing"})]
    declared = declared_metadata(rows)
    spark = _Spark(
        schema_tags=[
            {"schema_name": "framework_dev.billing_bronze", "tag_name": name, "tag_value": value}
            for name, value in {"domain": "finance", "owner": "billing"}.items()
        ],
        table_tags=[
            {"table_name": BRONZE, "tag_name": name, "tag_value": value}
            for name, value in declared.table_tags[BRONZE].items()
        ],
        column_tags=[
            {"table_name": BRONZE, "column_name": column, "tag_name": name, "tag_value": value}
            for column in ("invoice_id", "amount")
            for name, value in (("classification", "confidential"), ("pii", "pii_finance"))
        ],
    )
    assert apply_metadata(spark, rows, "framework_dev", apply=True) == []


def test_a_removed_tag_is_unset_and_a_changed_comment_is_set():
    declared = declared_metadata(_rows())
    declared.comments[(BRONZE, "amount")] = "Invoice amount, it's in AUD"
    declared.table_tags[BRONZE].pop("pii")
    applied_tags = [
        {"table_name": BRONZE, "tag_name": "pii", "tag_value": "pii_finance"},
        {"table_name": BRONZE, "tag_name": "cost_centre", "tag_value": "x"},
    ]
    spark = _Spark(table_tags=applied_tags)
    from lakeflow_framework.unity import applied_metadata

    applied, existing = applied_metadata(spark.answers)
    statements = plan_statements(declared, applied, existing)
    assert f"ALTER STREAMING TABLE {QUOTED} UNSET TAGS ('pii')" in statements
    # A tag the framework does not own is left alone.
    assert not any("cost_centre" in statement for statement in statements)
    assert (
        f"ALTER STREAMING TABLE {QUOTED} ALTER COLUMN `amount` COMMENT "
        "'Invoice amount, it''s in AUD'"
    ) in statements


def test_a_batch_table_is_altered_as_a_materialized_view():
    declared = declared_metadata(_rows(load_method="ctas", dedup=False, watermark_column=None))
    assert declared.kinds["framework_dev.billing_bronze.invoices"] == "materialized_view"


def test_lookups_are_limited_to_the_declared_tables_and_reject_unsafe_names():
    queries = lookup_sql("framework_dev", [BRONZE], ["framework_dev.billing_bronze"])
    assert set(queries) == {"schemas", "schema_tags", "table_tags", "column_tags", "columns"}
    assert all("framework_dev.information_schema." in sql for sql in queries.values())
    for name in ("table_tags", "column_tags", "columns"):
        assert f"IN ('{BRONZE}')" in queries[name]
    for name in ("schemas", "schema_tags"):
        assert "IN ('framework_dev.billing_bronze')" in queries[name]
    with pytest.raises(ValueError):
        lookup_sql("framework_dev", ["a.b.c' OR '1'='1"])
    with pytest.raises(ValueError):
        lookup_sql("framework_dev", [], ["a.b' OR '1'='1"])
    with pytest.raises(ValueError):
        lookup_sql("bad catalog", [BRONZE])


def test_a_schema_gets_its_source_tags_and_a_stale_one_is_unset():
    held = [
        {"schema_name": "framework_dev.billing_bronze", "tag_name": "engine", "tag_value": "x"},
        {"schema_name": "framework_dev.billing_bronze", "tag_name": "steward", "tag_value": "y"},
    ]
    spark = _Spark(schema_tags=held, columns=[])
    statements = apply_metadata(
        spark, [_schema_row({"domain": "finance", "owner": "billing"})], "framework_dev", apply=True
    )
    assert statements == [
        "ALTER SCHEMA `framework_dev`.`billing_bronze` UNSET TAGS ('engine')",
        "ALTER SCHEMA `framework_dev`.`billing_bronze` SET TAGS "
        "('domain' = 'finance', 'owner' = 'billing')",
    ]


def test_a_schema_that_does_not_exist_yet_waits_for_the_next_deploy():
    spark = _Spark(columns=[])
    spark.answers["schemata"] = []
    assert (
        apply_metadata(spark, [_schema_row({"domain": "finance"})], "framework_dev", apply=True)
        == []
    )


def test_drift_and_observed_lineage_views_are_created():
    registry = "\n".join(ddl_statements("framework_dev"))
    assert "CREATE OR REPLACE VIEW framework_dev.framework_meta.v_metadata_drift" in registry
    assert "framework_dev.information_schema.table_tags" in registry
    assert "framework_dev.information_schema.column_tags" in registry
    assert "framework_dev.information_schema.schema_tags" in registry
    system = "\n".join(
        ddl_statements("framework_dev", (BUNDLE / "sql" / "system_views.sql").read_text("utf-8"))
    )
    assert "CREATE OR REPLACE VIEW framework_dev.framework_meta.v_observed_lineage" in system
    assert "system.access.table_lineage" in system
    for status in ("'declared'", "'undeclared'", "'not_observed'"):
        assert status in system


def test_sql_comments_have_no_semicolon_because_the_ddl_task_splits_on_it():
    for path in (BUNDLE / "sql").glob("*.sql"):
        for line in path.read_text("utf-8").splitlines():
            assert not (line.strip().startswith("--") and ";" in line), f"{path.name}: {line}"


def test_apply_metadata_does_nothing_when_tags_and_comments_are_both_parked():
    assert apply_metadata(object(), _rows(), "framework_dev", apply=False, comments=False) == []


def test_column_comments_are_applied_while_tags_stay_parked():
    wanted = "Identifier of the pipeline run that wrote this row; joins load_ledger.batch_id"
    spark = _Spark(
        columns=[
            {"table_name": BRONZE, "column_name": "invoice_id", "comment": None},
            {"table_name": BRONZE, "column_name": "_batch_id", "comment": None},
            {"table_name": BRONZE, "column_name": "_src_row_hash", "comment": "stale text"},
            {"table_name": BRONZE, "column_name": "_ingest_timestamp", "comment": None},
        ]
    )
    # With tags parked, reading a tag view would be a bug (the deploy identity may not).
    for name in ("schemata", "schema_tags", "table_tags", "column_tags"):
        del spark.answers[name]
    statements = apply_metadata(spark, _rows(), "framework_dev")  # the defaults
    assert statements == spark.ran and statements
    assert not any("TAGS" in statement for statement in statements)
    assert (
        f"ALTER STREAMING TABLE {QUOTED} ALTER COLUMN `_batch_id` COMMENT '{wanted}'" in statements
    )
    # A stale comment is replaced, and a column with nothing to say gets no statement.
    assert any("`_src_row_hash` COMMENT 'SHA-256" in statement for statement in statements)
    assert not any("`invoice_id`" in statement for statement in statements)
    # Run again with the comments in place: nothing to change.
    done = _Spark(
        columns=[
            {"table_name": BRONZE, "column_name": "_batch_id", "comment": wanted},
        ]
    )
    for name in ("schemata", "schema_tags", "table_tags", "column_tags"):
        del done.answers[name]
    assert not [s for s in apply_metadata(done, _rows(), "framework_dev") if "_batch_id" in s]


def test_tags_can_still_be_turned_on_without_comments():
    spark = _Spark()
    statements = apply_metadata(spark, _rows(), "framework_dev", apply=True, comments=False)
    assert statements and not any(" COMMENT " in statement for statement in statements)
