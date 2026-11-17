"""Spark-free tests for config, SQL, rules, registry rows, and pipeline wiring."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from lakeflow_framework import ConfigValidationError, build_pipeline, validate_configs
from lakeflow_framework.cli import main
from lakeflow_framework.config import config_from_document, email_notifications
from lakeflow_framework.load import get_strategy
from lakeflow_framework.metadata import (
    LEDGER_COLUMNS,
    OBJECT_COLUMNS,
    current_objects_sql,
    ddl_statements,
    ingest_steps,
    new_object_rows,
    object_rows,
    owners_sql,
    ownership_conflicts,
    step_keys_sql,
    step_payload,
)
from lakeflow_framework.pipeline import MetadataNotFound, UnityMetadata, assemble, build_pipelines
from lakeflow_framework.quality import compile_rules, fail_sql
from lakeflow_framework.registry import (
    _cast,
    append_definitions,
    apply_ddl,
)

INGESTION = Path(__file__).resolve().parents[1] / "framework_ingestion"
EXAMPLES = (
    Path(__file__).resolve().parents[1] / "framework_examples" / "consumer" / "configs" / "bronze"
)


def _billing(**overrides):
    document = {
        "pipeline_id": "billing_invoices",
        "domain": "finance",
        "project": "billing",
        "data_domain": "invoices",
        "team": "billing_squad",
        "technical_owner": "billing.owner@jll.com",
        "layer": "bronze",
        "catalog": "framework_dev",
        "data": {"classification": "confidential", "pii": ["pii_finance"]},
        "source_to_raw": {
            "method": "autoloader",
            "source": {
                "system": "billing",
                "format": "csv",
                "path": "abfss://landing@storage.dfs.core.windows.net/billing/invoices/",
                "delimiter": ",",
                "header": True,
            },
            "raw": {"schema": "billing_raw", "table": "invoices"},
        },
        "raw_to_bronze": {
            "bronze": {"schema": "billing_bronze", "table": "invoices"},
            "dedup": True,
            "primary_keys": ["invoice_id"],
            "watermark_column": "updated_at",
            "columns": [
                {"name": "invoice_id", "type": "string"},
                {"name": "amount", "type": "decimal(18,2)"},
                {"name": "updated_at", "type": "timestamp"},
            ],
            "rules": [{"name": "amount_non_negative", "expression": "amount >= 0"}],
        },
        "quarantine": {"schema": "billing_quarantine"},
    }
    _apply(document, overrides)
    return document


def _ledger():
    return {
        "pipeline_id": "ledger_entries",
        "domain": "finance",
        "project": "ledger",
        "data_domain": "entries",
        "team": "ledger_squad",
        "technical_owner": "ledger.owner@jll.com",
        "layer": "bronze",
        "catalog": "framework_dev",
        "data": {"classification": "confidential", "pii": ["pii_finance"]},
        "source_to_raw": {
            "method": "ctas",
            "source": {
                "system": "ledger",
                "format": "fixed_width",
                "path": "s3://finance-landing/ledger/entries/",
                "fixed_columns": [
                    {"name": "account_id", "start": 1, "length": 10},
                    {"name": "amount", "start": 11, "length": 12},
                ],
            },
            "raw": {"schema": "ledger_raw", "table": "entries"},
        },
        "raw_to_bronze": {
            "bronze": {"schema": "ledger_bronze", "table": "entries"},
            "dedup": False,
            "primary_keys": ["account_id"],
            "rules": [{"name": "account_present", "expression": "length(trim(account_id)) > 0"}],
        },
        "quarantine": {"schema": "ledger_quarantine"},
    }


def _apply(document: dict, overrides: dict) -> None:
    source_to_raw = document["source_to_raw"]
    source = source_to_raw["source"]
    bronze_hop = document["raw_to_bronze"]
    places = {
        "pipeline_id": (document, "pipeline_id"),
        "project": (document, "project"),
        "catalog": (document, "catalog"),
        "job_name": (document, "job_name"),
        "load_method": (source_to_raw, "method"),
        "format": (source, "format"),
        "path": (source, "path"),
        "source_system": (source, "system"),
        "delimiter": (source, "delimiter"),
        "header": (source, "header"),
        "header_rows": (source, "header_rows"),
        "data_address": (source, "data_address"),
        "fixed_columns": (source, "fixed_columns"),
        "dedup": (bronze_hop, "dedup"),
        "primary_keys": (bronze_hop, "primary_keys"),
        "watermark_column": (bronze_hop, "watermark_column"),
        "columns": (bronze_hop, "columns"),
        "rules": (bronze_hop, "rules"),
    }
    for key, value in overrides.items():
        if key == "table":
            # The quarantine table name is derived from the bronze table, never authored.
            for section in (source_to_raw["raw"], bronze_hop["bronze"]):
                section["table"] = value
            continue
        mapping, field = places[key]
        if value is None:
            mapping.pop(field, None)
        else:
            mapping[field] = value


def test_only_bronze_loads_are_accepted():
    with pytest.raises(ConfigValidationError) as caught:
        config_from_document(
            {"pipeline_id": "later", "project": "finance", "layer": "silver", "catalog": "c"}
        )
    assert "layer must be bronze" in str(caught.value)
    config = config_from_document(_billing())
    assert config.table_name("raw") == "framework_dev.billing_raw.invoices"
    assert config.table_name("bronze") == "framework_dev.billing_bronze.invoices"
    assert config.canonical()["source_to_raw"]["raw"]["schema"] == "billing_raw"
    assert config.canonical()["raw_to_bronze"]["bronze"]["schema"] == "billing_bronze"
    reloaded = config_from_document(config.canonical())
    assert reloaded.content_hash() == config.content_hash()
    assert reloaded.notify_failure == ["billing.owner@jll.com"]


def test_one_domain_holds_many_projects_and_jobs_mail_their_owners():
    configs = validate_configs(sorted(EXAMPLES.glob("*.yaml")), allow_frozen=True)
    finance = [config for config in configs if config.domain == "finance"]
    assert {config.requested_by for config in finance} == {"billing", "ledger"}
    billing = next(config for config in finance if config.requested_by == "billing")
    assert billing.data_domain == "invoices"
    assert billing.tags["raw"].classification == "confidential"
    assert billing.tags["raw"].pii == ["pii_finance", "pii_personal"]
    assert billing.tags["quarantine"].classification == "highly_confidential"
    assert billing.notify_success == ["billing.owner@jll.com"]
    ledger = next(config for config in finance if config.requested_by == "ledger")
    assert ledger.notify_failure == ["ledger.owner@jll.com"]
    assert ledger.notify_success == []
    notes = email_notifications(finance)
    assert notes["on_failure"] == ["billing.owner@jll.com", "ledger.owner@jll.com"]
    assert notes["on_success"] == ["billing.owner@jll.com"]
    job = (
        Path(__file__).resolve().parents[1]
        / "framework_examples"
        / "consumer"
        / "resources"
        / "finance_ingest.job.yml"
    ).read_text(encoding="utf-8")
    for address in notes["on_failure"] + notes["on_success"]:
        assert address in job


def test_failure_mail_defaults_to_the_technical_owner():
    document = _billing()
    document.pop("notify", None)
    config = config_from_document(document)
    assert config.notify_failure == ["billing.owner@jll.com"]
    assert config.notify_success == []


def test_quarantine_table_defaults_to_bronze_plus_suffix_and_can_be_named():
    document = _billing()
    document["quarantine"].pop("table", None)
    config = config_from_document(document)
    assert config.quarantine_table == "invoices_qrntn"

    document["quarantine"]["table"] = "invoice_rejects"
    assert config_from_document(document).quarantine_table == "invoice_rejects"

    del document["quarantine"]
    assert config_from_document(document).quarantine_table == "invoices_qrntn"

    document["quarantine"] = {"table": "bad name"}
    with pytest.raises(ConfigValidationError) as caught:
        config_from_document(document)
    assert "quarantine table must be an identifier" in str(caught.value)


def test_unknown_pii_and_classification_are_rejected():
    document = _billing()
    document["data"] = {"classification": "secret", "pii": ["ssn"]}
    with pytest.raises(ConfigValidationError) as caught:
        config_from_document(document)
    text = "\n".join(caught.value.errors)
    assert "classification must be one of" in text
    assert "pii_health" in text


def test_example_configs_validate():
    configs = validate_configs(sorted(EXAMPLES.glob("*.yaml")), allow_frozen=True)
    assert {config.pipeline_id for config in configs} == {
        "billing_invoices",
        "ledger_entries",
        "orders_events",
        "payroll_timesheets",
        "price_book",
    }
    assert {config.layer for config in configs} == {"bronze"}


def test_validation_reports_every_error():
    with pytest.raises(ConfigValidationError) as caught:
        config_from_document(
            {
                "layer": "bronze",
                "password": "nope",
                "catalog": "${var.catalog}",
                "source_to_raw": {
                    "method": "copy_into",
                    "source": {
                        "system": "billing",
                        "format": "excel",
                        "path": "https://not-cloud/file.xlsx",
                        "header_rows": 1,
                        "data_address": "Sheet1!A1",
                    },
                    "raw": {"schema": "billing_raw", "table": "invoices"},
                },
                "raw_to_bronze": {
                    "bronze": {"schema": "billing_bronze", "table": "invoices"},
                    "dedup": True,
                },
                "quarantine": {"schema": "billing_quarantine"},
            }
        )
    text = "\n".join(caught.value.errors)
    assert "excel cannot use copy_into" in text
    assert "abfss:// or s3://" in text
    assert "dedup requires" in text
    assert "password" in text
    assert "unresolved" in text


def test_dedup_needs_a_declared_type_to_sequence_on():
    document = _billing()
    document["raw_to_bronze"]["columns"].pop()
    with pytest.raises(ConfigValidationError, match="watermark_column 'updated_at'"):
        config_from_document(document)


def test_the_valid_view_casts_the_sequence_column_for_auto_cdc():
    from lakeflow_framework.pipeline import _typed_sequence

    class Frame:
        columns = ["invoice_id", "updated_at", "_source_file"]

        def selectExpr(self, *expressions):
            return list(expressions)

    config = config_from_document(_billing())
    assert config.sequence_type == "timestamp"
    assert _typed_sequence(Frame(), config) == [
        "`invoice_id`",
        "CAST(`updated_at` AS timestamp) AS `updated_at`",
        "`_source_file`",
    ]
    unsequenced = Frame()
    assert _typed_sequence(unsequenced, config_from_document(_ledger())) is unsequenced


def test_two_pipelines_cannot_share_a_table(tmp_path: Path):
    first = tmp_path / "a.yaml"
    second = tmp_path / "b.yaml"
    shared = _billing()
    other = _billing(pipeline_id="billing_copy", table="invoices")
    first.write_text(_dump(shared), encoding="utf-8")
    second.write_text(_dump(other), encoding="utf-8")
    with pytest.raises(ConfigValidationError) as caught:
        validate_configs([first, second], allow_frozen=True)
    assert "billing_invoices and billing_copy" in "\n".join(caught.value.errors)


def test_autoloader_options_omit_checkpoint_and_schema_location():
    options = get_strategy("autoloader").options(config_from_document(_billing()))
    assert options["cloudFiles.format"] == "csv"
    assert "cloudFiles.schemaLocation" not in options
    assert "checkpoint" not in options
    assert options["rescuedDataColumn"] == "_rescued_data"
    assert options["cloudFiles.maxFilesPerTrigger"] == "1000"
    json_options = get_strategy("autoloader").options(
        config_from_document(_billing(format="json", delimiter=None))
    )
    assert "multiLine" not in json_options
    multiline = _billing(format="json", delimiter=None)
    multiline["source_to_raw"]["source"]["multiline"] = True
    assert (
        get_strategy("autoloader").options(config_from_document(multiline))["multiLine"] == "true"
    )


def test_copy_into_and_ctas_sql():
    copy_config = config_from_document(
        _billing(load_method="copy_into", dedup=False, watermark_column=None)
    )
    copy_sql = get_strategy("copy_into").batch_sql(copy_config)[0]
    assert copy_sql.startswith("COPY INTO framework_dev.billing_raw.invoices")
    assert "force" in copy_sql and "false" in copy_sql

    ctas_sql = get_strategy("ctas").batch_sql(config_from_document(_ledger()))[0]
    assert "CREATE OR REPLACE TABLE framework_dev.ledger_raw.entries" in ctas_sql
    assert "substring(value, 1, 10) AS account_id" in ctas_sql


def test_excel_is_rejected_for_copy_into():
    with pytest.raises(ConfigValidationError):
        config_from_document(
            _billing(
                format="excel",
                load_method="copy_into",
                dedup=False,
                watermark_column=None,
                delimiter=None,
                header_rows=1,
                data_address="Sheet1!A1",
            )
        )


def test_rule_compilation_treats_null_as_failure():
    rules = compile_rules(config_from_document(_billing()))
    names = [rule.name for rule in rules]
    assert "amount_non_negative" in names
    assert "not_null_invoice_id" in names
    assert "cast_amount" in names
    assert "schema_conformance" in names
    predicate = fail_sql(rules)
    assert "COALESCE" in predicate
    ledger_rules = compile_rules(config_from_document(_ledger()))
    assert "schema_conformance" not in [rule.name for rule in ledger_rules]


def test_registry_hash_is_stable_and_steps_do_not_duplicate():
    config = config_from_document(_billing())
    assert config.content_hash() == config_from_document(_billing()).content_hash()
    seen: set = set()
    first = new_object_rows(object_rows(config), seen)
    second = new_object_rows(object_rows(config), seen)
    assert first and second == []
    assert any(row["object_kind"] == "link" for row in first)

    running = step_payload(
        owner="billing",
        full_name="billing_invoices",
        run_id="100",
        step_no=10,
        step_name="mark_running",
        status="RUNNING",
    )
    success = step_payload(
        owner="billing",
        full_name="billing_invoices",
        run_id="100",
        step_no=50,
        step_name="record_success",
        status="SUCCEEDED",
    )
    keys: set = set()
    assert len(ingest_steps([running, success], keys)) == 2
    assert ingest_steps([success], keys) == []
    assert "recon" not in success and "recon_json" not in success


def test_pipeline_wiring_uses_auto_cdc_only_when_dedup_is_on():
    spark = _FakeSpark()
    pipelines = _FakePipelines()
    graph = build_pipeline(
        "billing_invoices",
        spark=spark,
        pipelines=pipelines,
        config=config_from_document(_billing()),
    )
    kinds = [dataset.kind for dataset in graph.datasets]
    assert kinds == ["raw", "staged", "valid", "bronze", "quarantine"]
    assert graph.auto_cdc["stored_as_scd_type"] == 1
    assert pipelines.cdc[0]["keys"] == ["invoice_id"]
    assert "cloudFiles.schemaLocation" not in spark.options
    assert any(name == "billing_invoices" or name.endswith("invoices") for name in pipelines.names)

    rules = {rule.name: rule.expression for rule in compile_rules(config_from_document(_billing()))}
    assert rules["not_null_updated_at"] == "updated_at IS NOT NULL"

    ledger = assemble(config_from_document(_ledger()))
    assert ledger.auto_cdc is None
    bronze = next(dataset for dataset in ledger.datasets if dataset.kind == "bronze")
    assert bronze.relation == "batch"


def test_missing_registry_row_is_an_error():
    class Empty:
        def get_pipeline(self, pipeline_id):
            return None

    with pytest.raises(MetadataNotFound):
        build_pipeline("billing_invoices", metadata=Empty())


def test_one_line_pipeline_file_uses_the_running_session(monkeypatch: pytest.MonkeyPatch):
    """build_pipeline("<id>") with nothing else reads the registry and registers the datasets."""

    spark = _FakeSpark()
    pipelines = _FakePipelines()

    class Registry:
        def __init__(self, _spark):
            assert _spark is spark

        def get_pipeline(self, pipeline_id):
            return _billing()

    monkeypatch.setattr("lakeflow_framework.pipeline._cluster_session", lambda: spark)
    monkeypatch.setattr("lakeflow_framework.pipeline._cluster_pipelines", lambda: pipelines)
    monkeypatch.setattr("lakeflow_framework.pipeline.UnityMetadata", Registry)
    build_pipeline("billing_invoices")
    assert pipelines.names
    assert pipelines.cdc[0]["keys"] == ["invoice_id"]
    assert spark.readStream.loads == 1
    assert any("_source_file" in expr for exprs in spark.readStream.exprs for expr in exprs)


def test_one_line_pipeline_file_off_cluster_says_why(monkeypatch):
    # pyspark 4.1 and later ship pyspark.pipelines, so hide it to stand in for an off-cluster run.
    monkeypatch.setitem(sys.modules, "pyspark.pipelines", None)
    if "pyspark" in sys.modules:
        monkeypatch.delattr(sys.modules["pyspark"], "pipelines", raising=False)
    with pytest.raises(MetadataNotFound, match="only a Lakeflow pipeline provides"):
        build_pipeline("billing_invoices")


def test_current_lookup_prunes_by_owner_kind_and_name():
    statement = current_objects_sql(
        "framework_dev",
        [
            {
                "owner_kind": "source_system",
                "owner": "billing",
                "object_kind": "column",
                "full_name": "framework_dev.billing_bronze.invoices.amount",
            }
        ],
    )
    assert "FROM framework_dev.framework_meta.object_current" in statement
    assert "owner_kind IN ('source_system')" in statement
    assert "owner IN ('billing')" in statement
    assert "object_kind IN ('column')" in statement
    assert "full_name IN ('framework_dev.billing_bronze.invoices.amount')" in statement
    assert "QUALIFY" not in statement
    assert "v_object_current" not in statement


def test_ddl_clusters_both_tables_and_names_the_catalog():
    statements = ddl_statements("framework_dev")
    text = "\n".join(statements)
    assert "CLUSTER BY (owner, object_kind, full_name)" in text
    assert "delta.autoOptimize.autoCompact" in text
    assert "CLUSTER BY (owner, event_date, run_id)" in text
    keys = [
        line.split("(", 1)[1].rstrip(")") for line in text.splitlines() if "CLUSTER BY (" in line
    ]
    assert keys and all(len(key.split(",")) <= 3 for key in keys)
    assert "'delta.autoOptimize.autoCompact' = 'false'" in text
    assert "v_ownership_conflicts" in text
    assert "project" not in text.replace("project for silver", "")
    assert "${catalog}" not in text
    assert "v_workflows" in text
    assert "v_pipelines" in text
    assert "v_dataflows" in text
    assert "v_flows" in text
    assert "v_job_status" not in text
    assert "framework_events" not in text
    assert "v_tables" in text
    assert "v_object_history" in text
    assert "PARTITION BY object_kind, full_name" in text
    assert "CREATE TABLE IF NOT EXISTS framework_dev.framework_meta.object_current" in text
    assert "CREATE TABLE IF NOT EXISTS framework_dev.framework_meta.run_current" in text
    current_view = next(statement for statement in statements if "v_object_current" in statement)
    assert "object_current" in current_view
    assert "QUALIFY" not in current_view
    assert "DROP VIEW IF EXISTS framework_dev.framework_meta.v_recon" in text
    assert "CREATE OR REPLACE VIEW framework_dev.framework_meta.v_recon" not in text
    run_current = next(
        statement
        for statement in statements
        if "VIEW" in statement and "v_run_current AS" in statement
    )
    assert "date_sub" not in run_current
    assert "get_json_object" not in text
    assert "config:pipeline_ids::ARRAY<STRING>" in text
    assert "framework_meta.table_health" in text
    history = text.split("v_object_history", 1)[1]
    assert "PARTITION BY owner_kind, owner, object_kind, full_name" in history
    assert "CREATE BLOOMFILTER INDEX" not in text
    assert "v_columns" in text
    assert "v_column_history" in text
    assert "valid_from" in text
    assert "valid_to" in text
    assert "is_current" in text
    assert not any(statement.upper().startswith("DELETE") for statement in statements)
    # Only object_current, which is not history, is updated: its older rows get config parsed.
    updates = [statement for statement in statements if statement.upper().startswith("UPDATE")]
    assert [statement.split()[1] for statement in updates] == [
        "framework_dev.framework_meta.object_current"
    ]
    assert _column_list(text, "object_registry") == [*OBJECT_COLUMNS, "config"]
    assert _column_list(text, "run_ledger") == list(LEDGER_COLUMNS)


def test_ctas_securables_match_the_graph():
    rows = object_rows(config_from_document(_ledger()))
    kinds = {row["full_name"]: row["object_kind"] for row in rows}
    assert kinds["framework_dev.ledger_raw.entries"] == "table"
    assert kinds["framework_dev.ledger_bronze.entries"] == "materialized_view"
    assert kinds["framework_dev.ledger_quarantine.entries_qrntn"] == "materialized_view"
    table = next(row for row in rows if row["full_name"] == "framework_dev.ledger_bronze.entries")
    recorded = json.loads(table["config_json"])
    assert recorded["domain"] == "finance"
    assert recorded["data_domain"] == "entries"
    assert recorded["classification"] == "confidential"
    assert recorded["pii"] == "pii_finance"
    assert recorded["technical_owner"] == "ledger.owner@jll.com"


def test_column_metadata_versions_type_comment_and_tags():
    config = config_from_document(_billing())
    rows = [
        row
        for row in object_rows(config)
        if row["object_kind"] == "column"
        and row["parent_full_name"] == "framework_dev.billing_bronze.invoices"
    ]
    assert [row["full_name"] for row in rows] == [
        "framework_dev.billing_bronze.invoices.invoice_id",
        "framework_dev.billing_bronze.invoices.amount",
        "framework_dev.billing_bronze.invoices.updated_at",
    ]
    raw_names = {
        row["full_name"]
        for row in object_rows(config)
        if row["parent_full_name"] == "framework_dev.billing_raw.invoices"
    }
    assert "framework_dev.billing_raw.invoices._source_file" in raw_names
    assert "framework_dev.billing_raw.invoices.amount" in raw_names
    quarantine_names = {
        row["full_name"]
        for row in object_rows(config)
        if row["parent_full_name"] == "framework_dev.billing_quarantine.invoices_qrntn"
    }
    assert "framework_dev.billing_quarantine.invoices_qrntn.failed_rules" in quarantine_names
    amount = json.loads(rows[1]["config_json"])
    assert amount == {
        "classification": "confidential",
        "data_type": "decimal(18,2)",
        "description": "",
        "name": "amount",
        "ordinal": 2,
        "pii": "pii_finance",
    }
    assert rows[1]["parent_full_name"] == "framework_dev.billing_bronze.invoices"
    assert rows[1]["relation"] == "column_of"

    changed = _billing()
    changed["raw_to_bronze"]["columns"][1]["type"] = "decimal(20,4)"
    changed["raw_to_bronze"]["columns"][1]["description"] = "Invoice amount"
    changed["raw_to_bronze"]["columns"][1]["classification"] = "highly_confidential"
    changed["raw_to_bronze"]["columns"][1]["pii"] = ["pii_finance", "pii_personal"]
    revised = object_rows(config_from_document(changed))
    revised_amount = next(row for row in revised if row["full_name"].endswith(".invoices.amount"))
    assert revised_amount["content_hash"] != rows[1]["content_hash"]
    payload = json.loads(revised_amount["config_json"])
    assert payload["data_type"] == "decimal(20,4)"
    assert payload["description"] == "Invoice amount"
    assert payload["classification"] == "highly_confidential"
    assert payload["pii"] == "pii_finance, pii_personal"

    seen: set = set()
    assert new_object_rows(rows, seen)
    assert new_object_rows(rows, seen) == []
    fresh = new_object_rows([revised_amount], seen)
    assert [row["full_name"] for row in fresh] == [revised_amount["full_name"]]

    renamed_job = _billing()
    renamed_job["job_name"] = "finance_billing_retry"
    untouched = next(
        row
        for row in object_rows(config_from_document(renamed_job))
        if row["object_kind"] == "column" and row["full_name"].endswith(".amount")
    )
    assert untouched["content_hash"] == rows[1]["content_hash"]

    ledger_rows = object_rows(config_from_document(_ledger()))
    assert [
        row["full_name"]
        for row in ledger_rows
        if row["parent_full_name"] == "framework_dev.ledger_bronze.entries"
    ] == []
    assert [
        row["full_name"]
        for row in ledger_rows
        if row["parent_full_name"] == "framework_dev.ledger_raw.entries"
    ] == [
        "framework_dev.ledger_raw.entries.account_id",
        "framework_dev.ledger_raw.entries.amount",
        "framework_dev.ledger_raw.entries._source_file",
        "framework_dev.ledger_raw.entries._ingested_at",
    ]


def test_column_tags_inherit_the_bronze_table_unless_overridden():
    document = _billing()
    document["raw_to_bronze"]["columns"][0]["description"] = "  Source invoice id  "
    document["raw_to_bronze"]["columns"][0]["pii"] = []
    config = config_from_document(document)
    invoice_id, amount, _updated_at = config.columns
    assert invoice_id.description == "Source invoice id"
    assert invoice_id.classification == "confidential"
    assert invoice_id.pii == []
    assert amount.pii == ["pii_finance"]
    reloaded = config_from_document(config.canonical())
    assert reloaded.content_hash() == config.content_hash()
    assert reloaded.columns[0].description == "Source invoice id"
    assert reloaded.columns[0].pii == []


def test_column_sensitivity_and_duplicates_are_rejected():
    secret = _billing()
    secret["raw_to_bronze"]["columns"][0]["classification"] = "secret"
    with pytest.raises(ConfigValidationError) as caught:
        config_from_document(secret)
    assert "classification must be one of" in str(caught.value)

    unknown = _billing()
    unknown["raw_to_bronze"]["columns"][0]["pii"] = ["ssn"]
    with pytest.raises(ConfigValidationError) as caught:
        config_from_document(unknown)
    assert "pii value 'ssn'" in str(caught.value)

    duplicated = _billing()
    duplicated["raw_to_bronze"]["columns"].append({"name": "invoice_id", "type": "int"})
    with pytest.raises(ConfigValidationError) as caught:
        config_from_document(duplicated)
    assert "column 'invoice_id' is duplicated" in str(caught.value)

    prose = _billing()
    prose["raw_to_bronze"]["columns"][0]["description"] = {"text": "nope"}
    with pytest.raises(ConfigValidationError) as caught:
        config_from_document(prose)
    assert "description must be text" in str(caught.value)


def test_example_columns_keep_descriptions_and_sensitivity():
    configs = validate_configs(sorted(EXAMPLES.glob("billing_invoices.yaml")), allow_frozen=True)
    billing = configs[0]
    by_name = {column.name: column for column in billing.columns}
    assert by_name["invoice_id"].description == "Invoice identifier from the billing system"
    assert by_name["invoice_id"].classification == "confidential"
    assert by_name["invoice_id"].pii == ["pii_finance", "pii_personal"]
    assert by_name["amount"].pii == ["pii_finance"]
    assert by_name["amount"].type_name == "decimal(18,2)"
    recorded = json.loads(
        next(
            row["config_json"]
            for row in object_rows(billing)
            if row["full_name"] == "framework_dev.billing_bronze.invoices.amount"
        )
    )
    assert recorded["description"] == "Invoice amount in the source currency"
    assert recorded["pii"] == "pii_finance"
    assert recorded["classification"] == "confidential"


def test_a_step_file_written_with_counts_still_lands():
    row = ingest_steps(
        [
            {
                "run_id": "1",
                "step_no": 50,
                "status": "SUCCEEDED",
                "full_name": "billing_daily",
                "owner_kind": "source_system",
                "owner": "billing",
                "step_name": "record_success",
                "recon_json": '{"source_count": 10}',
            }
        ]
    )[0]
    assert row["recon_json"] == '{"source_count": 10}'


def test_a_table_version_stays_open_when_only_the_job_name_changes():
    original = object_rows(config_from_document(_billing(job_name="finance_billing")))
    renamed = object_rows(config_from_document(_billing(job_name="finance_billing_retry")))

    def hashes(rows, kind, name):
        return next(
            row["content_hash"]
            for row in rows
            if row["object_kind"] == kind and row["full_name"] == name
        )

    assert hashes(original, "streaming_table", "framework_dev.billing_bronze.invoices") == hashes(
        renamed, "streaming_table", "framework_dev.billing_bronze.invoices"
    )
    assert hashes(original, "pipeline", "billing_invoices") != hashes(
        renamed, "pipeline", "billing_invoices"
    )


def test_repeated_definition_is_inserted_once(monkeypatch: pytest.MonkeyPatch):
    inserted: list[list[dict]] = []

    def fake_insert(_spark, _table, rows, _columns, **_kwargs):
        inserted.append(rows)
        return len(rows)

    monkeypatch.setattr("lakeflow_framework.registry._insert", fake_insert)
    row = {
        "object_kind": "pipeline",
        "full_name": "billing_invoices",
        "owner_kind": "source_system",
        "owner": "billing",
        "requested_by": "billing",
        "delivery": "daily",
        "parent_full_name": None,
        "relation": None,
        "config_json": "{}",
        "content_hash": "a" * 64,
        "source": "framework",
        "recorded_at": "2026-11-09T00:00:00Z",
    }
    seen: set = set()
    assert append_definitions(None, [row, dict(row)], seen) == 1
    assert append_definitions(None, [dict(row)], seen) == 0
    restored = dict(row)
    changed = dict(row, content_hash="b" * 64)
    assert append_definitions(None, [changed], seen) == 1
    assert append_definitions(None, [restored], seen) == 1
    assert append_definitions(None, [restored], seen) == 0
    assert len(inserted) == 3


def _owned_row(**changes):
    row = {
        "object_kind": "streaming_table",
        "full_name": "framework_dev.billing_bronze.invoices",
        "owner_kind": "source_system",
        "owner": "billing",
        "requested_by": "billing",
        "delivery": "daily",
        "parent_full_name": None,
        "relation": None,
        "config_json": "{}",
        "content_hash": "b" * 64,
        "source": "framework",
        "recorded_at": "2026-11-09T00:00:00Z",
    }
    row.update(changes)
    return row


def test_bronze_is_owned_by_its_source_system_not_the_requesting_team():
    document = _billing(project="collections")
    document["requested_by"] = "collections"
    config = config_from_document(document)
    assert (config.owner_kind, config.owner) == ("source_system", "billing")
    assert config.requested_by == "collections"
    for row in object_rows(config):
        assert (row["owner_kind"], row["owner"]) == ("source_system", "billing")
        assert row["requested_by"] == "collections"
    assert object_rows(config)[0]["delivery"] is None
    assert config_from_document(config.canonical()).requested_by == "collections"


def test_registry_rows_carry_the_delivery_and_older_specs_still_load():
    older = config_from_document(_billing())
    assert older.requested_by == "billing"
    document = _billing()
    document["delivery"] = "daily"
    assert {row["delivery"] for row in object_rows(config_from_document(document))} == {"daily"}


def test_a_name_held_by_another_owner_is_refused_and_the_same_owner_may_update():
    mine = _owned_row()
    other = _owned_row(owner="erp", content_hash="c" * 64)
    current = {("streaming_table", mine["full_name"]): ("source_system", "billing")}
    accepted, refused = ownership_conflicts([mine, other], current)
    assert accepted == [mine] and refused == [other]
    column = _owned_row(object_kind="column", owner="erp")
    assert ownership_conflicts([column], current) == ([column], [])
    free = _owned_row(full_name="framework_dev.billing_bronze.free")
    assert ownership_conflicts([free], current) == ([free], [])


def test_owner_lookup_is_limited_to_owned_kinds_and_safe_names():
    sql = owners_sql("framework_dev", ["framework_dev.billing_bronze.invoices", "x' OR '1'='1"])
    assert "framework_dev.framework_meta.v_object_current" in sql
    assert "x' OR" not in sql
    assert "'column'" not in sql
    with pytest.raises(ValueError):
        owners_sql("framework_dev", ["x' OR '1'='1"])


def test_collector_records_a_failed_check_instead_of_writing_another_owners_table(
    monkeypatch: pytest.MonkeyPatch,
):
    written: dict[str, list[dict]] = {}

    def fake_insert(_spark, table, rows, _columns, **_kwargs):
        written.setdefault(table, []).extend(rows)
        return len(rows)

    class Frame:
        def __init__(self, rows):
            self.rows = rows

        def collect(self):
            return self.rows

    class Spark:
        def sql(self, statement):
            if "v_object_current" in statement and "owner_kind" in statement.split("FROM")[0]:
                return Frame(
                    [
                        {
                            "object_kind": "streaming_table",
                            "full_name": "framework_dev.billing_bronze.invoices",
                            "owner_kind": "source_system",
                            "owner": "billing",
                        }
                    ]
                )
            return Frame([])

    monkeypatch.setattr("lakeflow_framework.registry._insert", fake_insert)
    taken = _owned_row(owner="erp", content_hash="d" * 64)
    fine = _owned_row(
        full_name="framework_dev.erp_bronze.orders", owner="erp", content_hash="e" * 64
    )
    assert append_definitions(Spark(), [taken, fine], set(), "framework_dev") == 1
    registry = written["framework_dev.framework_meta.object_registry"]
    assert [row["full_name"] for row in registry] == ["framework_dev.erp_bronze.orders"]
    (step,) = written["framework_dev.framework_meta.run_ledger"]
    assert step["step_name"] == "ownership_check" and step["status"] == "FAILED"
    assert (step["owner_kind"], step["owner"]) == ("source_system", "erp")
    assert "owned by source_system billing" in step["failure_reason"]


def test_a_step_needs_a_known_owner_kind():
    with pytest.raises(ValueError):
        step_payload(owner="x", owner_kind="team", full_name="x", run_id="1", step_no=1,
                     step_name="a", status="RUNNING")  # fmt: skip


def test_per_run_commands_are_gone(capsys: pytest.CaptureFixture[str]):
    # Definitions are written at deploy by registry_sync, and run outcomes come from the
    # system tables, so a job no longer syncs, records steps, or waits for the registry.
    for command in ("sync", "step", "await-registry"):
        with pytest.raises(SystemExit):
            main([command, "--help"])
        assert "invalid choice" in capsys.readouterr().err


def test_a_delivery_reads_its_specs_from_the_pipeline_configuration():
    paths = [INGESTION / "sources" / "abs" / "fdh" / "abs_accounting.yaml"]

    class Conf:
        def get(self, key, default=None):
            return {
                "lff.specs": ",".join(str(path) for path in paths),
                "lff.catalog": "framework_test",
            }.get(key, default)

    class Reader:
        def get_pipelines(self, pipeline_ids):
            raise AssertionError("the registry is not read when lff.specs is set")

    spark, pipelines = _FakeSpark(), _FakePipelines()
    spark.conf = Conf()
    graphs = build_pipelines(["abs_accounting"], spark=spark, pipelines=pipelines)
    assert [graph.pipeline_id for graph in graphs] == ["abs_accounting"]
    assert any(name.startswith("framework_test.") for name in pipelines.names)
    with pytest.raises(MetadataNotFound, match="not in lff.specs"):
        build_pipelines(["not_deployed"], spark=spark, pipelines=pipelines)
    with pytest.raises(AssertionError):
        build_pipelines(["abs_accounting"], spark=spark, pipelines=pipelines, metadata=Reader())


def test_a_delivery_reads_every_pipeline_document_once():
    calls: list[list[str]] = []

    class Reader:
        def get_pipelines(self, pipeline_ids):
            calls.append(list(pipeline_ids))
            return {
                "billing_invoices": _billing(),
                "ledger_entries": _ledger(),
            }

    spark, pipelines = _FakeSpark(), _FakePipelines()
    graphs = build_pipelines(
        ["billing_invoices", "ledger_entries"],
        spark=spark,
        pipelines=pipelines,
        metadata=Reader(),
    )
    assert calls == [["billing_invoices", "ledger_entries"]]
    assert [graph.pipeline_id for graph in graphs] == ["billing_invoices", "ledger_entries"]
    # Auto Loader lists files once. Bronze and quarantine read the staging table, not raw again.
    assert spark.readStream.loads == 1
    assert spark.read.tables == [
        "framework_dev.ledger_raw.entries",
        "ledger_entries_staged",
        "ledger_entries_valid",
        "ledger_entries_staged",
    ]


def test_step_lookup_prunes_by_owner_and_event_date():
    statement = step_keys_sql(
        "framework_dev",
        ["100", "100"],
        owners=["billing", "billing"],
        event_dates=["2026-11-09"],
    )
    assert "run_id IN ('100')" in statement
    assert "owner IN ('billing')" in statement
    assert "event_date IN (DATE '2026-11-09')" in statement
    widened = step_keys_sql("framework_dev", ["100"], owners=["bad owner"])
    assert "owner IN" not in widened
    assert "to_timestamp" in _cast("recorded_at", ("recorded_at",), (), ())


def test_a_retried_definition_repairs_the_current_row_without_a_second_history_row(monkeypatch):
    inserted: list[list[dict]] = []
    merged: list[list[dict]] = []

    def fake_insert(_spark, _table, rows, *_args, **_kwargs):
        inserted.append(rows)
        return len(rows)

    def fake_merge(_spark, _table, rows, *_args, **_kwargs):
        merged.append(rows)

    monkeypatch.setattr("lakeflow_framework.registry._insert", fake_insert)
    monkeypatch.setattr("lakeflow_framework.registry._merge_current", fake_merge)
    row = _owned_row()

    class Frame:
        def __init__(self, rows):
            self.rows = rows

        def collect(self):
            return self.rows

    class Spark:
        def sql(self, statement):
            if "object_registry" in statement and "content_hash" in statement:
                return Frame(
                    [
                        {
                            "object_kind": row["object_kind"],
                            "full_name": row["full_name"],
                            "content_hash": row["content_hash"],
                            "recorded_at": row["recorded_at"],
                        }
                    ]
                )
            return Frame([])

    assert append_definitions(Spark(), [row], set(), "framework_dev") == 0
    assert inserted == []
    assert merged == [[row]]


def test_ddl_skips_a_column_the_table_already_has_and_raises_other_errors():
    ran: list[str] = []

    class Spark:
        def __init__(self, failure: str):
            self.failure = failure

        def sql(self, statement):
            ran.append(statement)
            if "ADD COLUMNS" in statement:
                raise RuntimeError(self.failure)

    apply_ddl(Spark("[FIELD_ALREADY_EXISTS] Cannot add column config"), "framework_dev")
    assert sum("ADD COLUMNS" in statement for statement in ran) == 2
    assert any("v_bronze_tables" in statement for statement in ran)
    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        apply_ddl(Spark("PERMISSION_DENIED"), "framework_dev")


def test_definitions_written_from_python_parse_config_to_variant():
    statements: list[str] = []

    class Frame:
        def createOrReplaceTempView(self, _name):
            pass

    class Spark:
        def createDataFrame(self, _rows, _schema):
            return Frame()

        def sql(self, statement):
            statements.append(statement)

    from lakeflow_framework.registry import OBJECT_DERIVED, _insert, _merge_current

    row = _owned_row()
    _insert(Spark(), "t", [row], OBJECT_COLUMNS, derived=OBJECT_DERIVED)
    _merge_current(Spark(), "c", [row], OBJECT_COLUMNS, ("object_kind",), derived=OBJECT_DERIVED)
    insert, merge = statements
    assert insert.startswith("INSERT INTO t (object_kind, ")
    assert "recorded_at, config) SELECT" in insert
    assert "parse_json(config_json) AS config" in insert
    assert "t.config = s.config" in merge


def test_an_unsafe_name_is_not_inserted_by_skipping_the_check():
    with pytest.raises(ValueError):
        append_definitions(object(), [_owned_row(full_name="not a name")], set(), "framework_dev")


def test_cloud_files_accept_a_unity_catalog_volume_path():
    config = config_from_document(
        _billing(path="/Volumes/framework_dev/billing_raw/landing/invoices/")
    )
    assert config.load_method == "autoloader"
    assert config.path == "/Volumes/framework_dev/billing_raw/landing/invoices/"


def test_batch_sql_escapes_quotes_in_paths_and_excel_addresses():
    copy_sql = get_strategy("copy_into").batch_sql(
        config_from_document(
            _billing(
                load_method="copy_into",
                dedup=False,
                watermark_column=None,
                path="abfss://landing@storage.dfs.core.windows.net/a'b/",
            )
        )
    )[0]
    assert "a''b" in copy_sql
    excel = _billing(
        format="excel",
        load_method="ctas",
        dedup=False,
        watermark_column=None,
        delimiter=None,
        header_rows=1,
        data_address="'Sheet1'!A1",
    )
    assert "'''Sheet1''!A1'" in get_strategy("ctas").batch_sql(config_from_document(excel))[0]


def test_pipeline_lookup_binds_the_id_and_rejects_sql():
    class Frame:
        def collect(self):
            return []

    statements = []

    class Spark:
        # Inside a pipeline, spark.sql is wrapped by Lakeflow and accepts no args= keyword.
        def sql(self, statement):
            statements.append(statement)
            return Frame()

    assert UnityMetadata(Spark()).get_pipeline("billing_invoices") is None
    assert "full_name IN ('billing_invoices')" in statements[0]
    for hostile in ("billing' OR '1'='1", "a;b", "x y", "", "a\n"):
        with pytest.raises(MetadataNotFound):
            UnityMetadata(Spark()).get_pipeline(hostile)
    assert len(statements) == 1  # nothing hostile reached spark.sql


def _column_list(text: str, table: str) -> list[str]:
    marker = f"framework_meta.{table} ("
    start = text.index(marker) + len(marker)
    end = text.index(")", start)
    names = []
    for line in text[start:end].splitlines():
        part = line.strip().rstrip(",")
        if part:
            names.append(part.split()[0])
    return names


def _dump(document: dict) -> str:
    import yaml

    return yaml.safe_dump(document)


class _Frame:
    def __init__(self, reader: "_Reader"):
        self.reader = reader

    def filter(self, where: str) -> "_Frame":
        self.reader.wheres.append(where)
        return self

    def selectExpr(self, *exprs: str) -> "_Frame":
        self.reader.exprs.append(exprs)
        return self


class _Reader:
    def __init__(self):
        self.options = {}
        self.format_name = None
        self.loads = 0
        self.tables: list[str] = []
        self.wheres: list[str] = []
        self.exprs: list[tuple[str, ...]] = []

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self, path):
        self.loads += 1
        self.path = path
        return _Frame(self)

    def table(self, name):
        self.tables.append(name)
        return _Frame(self)


class _FakeSpark:
    def __init__(self):
        self.readStream = _Reader()
        self.read = _Reader()

    @property
    def options(self):
        return self.readStream.options


class _FakePipelines:
    def __init__(self):
        self.names = []
        self.cdc = []
        self.expectations = []

    def table(self, **kwargs):
        self.names.append(kwargs.get("name"))

        def decorator(func):
            func()
            return func

        return decorator

    def temporary_view(self, **kwargs):
        self.names.append(kwargs.get("name"))

        def decorator(func):
            func()
            return func

        return decorator

    def materialized_view(self, **kwargs):
        self.names.append(kwargs.get("name"))

        def decorator(func):
            func()
            return func

        return decorator

    def expect(self, name, expression):
        self.expectations.append((name, expression))
        return lambda func: func

    def expect_all(self, constraints):
        self.expectations.extend(constraints.items())
        return lambda func: func

    def create_streaming_table(self, **kwargs):
        self.names.append(kwargs.get("name"))

    def create_auto_cdc_flow(self, **kwargs):
        self.cdc.append(kwargs)
