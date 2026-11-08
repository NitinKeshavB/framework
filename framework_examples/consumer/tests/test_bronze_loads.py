"""Sample tests for a bronze load.

Copy this file next to your own configs. Assert the YAML, the raw table, and the
bronze table. These tests do not start Spark or a Databricks cluster.
"""

from pathlib import Path

import pytest

from lakeflow_framework import ConfigValidationError, validate_configs
from lakeflow_framework.config import config_from_document
from lakeflow_framework.pipeline import assemble

CONFIGS = Path(__file__).resolve().parents[1] / "configs" / "bronze"


def _load(name: str):
    return validate_configs([CONFIGS / name])[0]


def test_billing_publishes_raw_to_bronze():
    config = _load("billing_invoices.yaml")
    graph = assemble(config)

    assert config.layer == "bronze"
    assert config.table_name("raw") == "framework_dev.billing_raw.invoices"
    assert config.table_name("bronze") == "framework_dev.billing_bronze.invoices"
    assert config.table_name("quarantine") == "framework_dev.billing_quarantine.invoices"
    assert [dataset.kind for dataset in graph.datasets] == ["raw", "valid", "quarantine", "bronze"]
    assert graph.auto_cdc["keys"] == ["invoice_id"]
    assert graph.auto_cdc["sequence_by"] == "updated_at"
    assert "cloudFiles.schemaLocation" not in graph.datasets[0].detail["options"]


def test_orders_copy_into_lands_in_raw():
    config = _load("orders_events.yaml")
    graph = assemble(config)
    statement = graph.batch_sql[0]

    assert config.load_method == "copy_into"
    assert graph.auto_cdc is None
    assert statement.startswith("COPY INTO framework_dev.orders_raw.events")
    assert "'force' = 'false'" in statement


def test_ledger_ctas_replaces_raw_and_builds_a_bronze_view():
    config = _load("ledger_entries.yaml")
    graph = assemble(config)
    bronze = next(dataset for dataset in graph.datasets if dataset.kind == "bronze")

    assert graph.batch_sql[0].startswith("CREATE OR REPLACE TABLE framework_dev.ledger_raw.entries")
    assert "substring(value, 1, 10) AS account_id" in graph.batch_sql[0]
    assert bronze.relation == "batch"
    assert graph.auto_cdc is None


def test_payroll_excel_options_stay_on_the_raw_hop():
    config = _load("payroll_timesheets.yaml")
    options = assemble(config).datasets[0].detail["options"]

    assert options["cloudFiles.format"] == "excel"
    assert options["headerRows"] == "1"
    assert options["dataAddress"] == "Timesheets!A1"
    assert "cloudFiles.schemaLocation" not in options


def test_price_book_limits_the_parquet_batch():
    options = assemble(_load("price_book.yaml")).datasets[0].detail["options"]

    assert options["pathGlobFilter"] == "*.parquet"
    assert options["cloudFiles.maxFilesPerTrigger"] == "100"
    assert "rescuedDataColumn" not in options


def test_excel_copy_into_is_rejected_before_a_cluster_starts():
    document = _load("payroll_timesheets.yaml").canonical()
    document["source_to_raw"]["method"] = "copy_into"

    with pytest.raises(ConfigValidationError, match="excel cannot use copy_into"):
        config_from_document(document)
