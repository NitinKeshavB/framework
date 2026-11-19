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
    SCD_COLUMNS,
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
from lakeflow_framework.quality import compile_rules
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
    # NULL predicates fail: every compiled rule wraps with COALESCE(..., false).
    assert all("COALESCE" in rule.holds() for rule in rules)
    ledger_rules = compile_rules(config_from_document(_ledger()))
    assert "schema_conformance" not in [rule.name for rule in ledger_rules]


def test_rule_expressions_are_constrained_to_columns_and_approved_functions():
    from lakeflow_framework.quality import assert_safe_rule_expression

    errors: list[str] = []
    assert_safe_rule_expression(
        "amount >= 0 AND length(trim(status)) > 0",
        {"amount", "status"},
        label="spec",
        name="ok",
        errors=errors,
    )
    assert errors == []
    assert_safe_rule_expression(
        "amount >= (SELECT max(amount) FROM other)",
        {"amount"},
        label="spec",
        name="subquery",
        errors=errors,
    )
    assert any("subqueries" in item for item in errors)
    errors.clear()
    assert_safe_rule_expression(
        "secret_col IS NOT NULL /* poke */",
        {"amount"},
        label="spec",
        name="comment",
        errors=errors,
    )
    assert any("comments" in item for item in errors)
    bad = _billing()
    bad["raw_to_bronze"]["rules"] = [
        {"name": "hostile", "expression": "EXISTS (SELECT 1 FROM system.access.table_lineage)"}
    ]
    with pytest.raises(ConfigValidationError, match="subqueries|denied"):
        config_from_document(bad)


def test_registry_hash_is_stable_and_steps_do_not_duplicate():
    config = config_from_document(_billing())
    assert config.content_hash() == config_from_document(_billing()).content_hash()
    seen: set = set()
    first = new_object_rows(object_rows(config), seen)
    second = new_object_rows(object_rows(config), seen)
    assert first and second == []
    assert any(row["object_kind"] == "link" for row in first)

    first = step_payload(
        owner="erp",
        full_name="billing_invoices",
        run_id="ownership-aaa",
        step_no=1,
        step_name="ownership_check",
        status="FAILED",
        content_hash="a" * 64,
        holder_owner_kind="source_system",
        holder_owner="billing",
        failure_reason="owned by source_system billing",
    )
    second = step_payload(
        owner="claims",
        full_name="billing_invoices",
        run_id="ownership-bbb",
        step_no=1,
        step_name="ownership_check",
        status="FAILED",
        content_hash="b" * 64,
        holder_owner_kind="source_system",
        holder_owner="billing",
        failure_reason="owned by source_system billing",
    )
    keys: set = set()
    assert len(ingest_steps([first, second], keys)) == 2
    assert ingest_steps([second], keys) == []


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
    assert kinds == ["raw", "staged", "valid", "files", "bronze", "quarantine"]
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
    assert any("_src_file_path" in expr for exprs in spark.readStream.exprs for expr in exprs)


def test_one_line_pipeline_file_off_cluster_says_why(monkeypatch):
    # pyspark 4.1 and later ship pyspark.pipelines, so hide it to stand in for an off-cluster run.
    monkeypatch.setitem(sys.modules, "pyspark.pipelines", None)
    if "pyspark" in sys.modules:
        monkeypatch.delattr(sys.modules["pyspark"], "pipelines", raising=False)
    with pytest.raises(MetadataNotFound, match="only a Lakeflow pipeline provides"):
        build_pipeline("billing_invoices")


def test_current_lookup_joins_a_staged_view_not_an_in_list():
    statement = current_objects_sql("framework_dev")
    assert "FROM framework_dev.framework_meta.asset_registry_current c" in statement
    assert "INNER JOIN _framework_lookup_keys s" in statement
    assert "c.object_kind = s.object_kind AND c.full_name = s.full_name" in statement
    assert "full_name IN (" not in statement
    assert "QUALIFY" not in statement
    assert "v_asset_registry_current" not in statement


def test_ddl_clusters_both_tables_and_names_the_catalog():
    statements = ddl_statements("framework_dev")
    text = "\n".join(statements)
    assert "CLUSTER BY (owner, object_kind, full_name)" in text
    assert "delta.autoOptimize.autoCompact" in text
    assert "CLUSTER BY (full_name, recorded_at)" in text
    keys = [
        line.split("(", 1)[1].rstrip(")") for line in text.splitlines() if "CLUSTER BY (" in line
    ]
    assert keys and all(len(key.split(",")) <= 4 for key in keys)
    assert "'delta.autoOptimize.autoCompact' = 'false'" not in text
    # Every statement can run again. ADD COLUMN has no IF NOT EXISTS, so apply_ddl skips it when
    # the column is already there; a constraint is dropped IF EXISTS before it is added.
    alters = [statement for statement in statements if statement.upper().startswith("ALTER")]
    assert alters and all(
        " ADD COLUMN " in statement.upper()
        or " DROP CONSTRAINT IF EXISTS " in statement.upper()
        or " ADD CONSTRAINT " in statement.upper()
        for statement in alters
    )
    assert "VIEW framework_dev.framework_meta.v_ownership_refusals" in text
    assert "ownership_refusals" in text
    assert "run_ledger" not in text and "run_current" not in text
    assert "v_ownership_conflicts" in text
    assert "asset_id" in text
    assert "PRIMARY KEY" in text
    assert "ref_object_kind" in text
    assert "update_pipeline_uuid" in text
    assert "v_load_metrics" in text
    assert "load_metric" in text
    assert "drift_enabled" in text and "false AS enabled" in text
    # ``project`` appears only as an owner_kind enum value, not as a remnant object_kind.
    assert "object_kind = 'project'" not in text
    assert "('project')" in text  # ref_owner_kind seed
    assert "${catalog}" not in text
    assert "v_workflows" in text
    assert "v_pipelines" in text
    assert "v_dataflows" in text
    assert "v_flows" in text
    assert "system.lakeflow" not in text
    assert "CREATE SCHEMA IF NOT EXISTS framework_dev.framework_events" in text
    assert "v_tables" in text
    assert "v_asset_registry_history" in text
    assert "CREATE TABLE IF NOT EXISTS framework_dev.framework_meta.asset_registry_current" in text
    current_view = next(
        statement for statement in statements if "v_asset_registry_current" in statement
    )
    assert "asset_registry_current" in current_view
    assert "QUALIFY" not in current_view
    assert "v_recon" not in text
    assert "v_run_current" not in text
    assert "v_process" not in text
    assert "v_bronze_loads" not in text
    assert "load_daily_rollup" not in text
    assert "get_json_object" not in text
    assert "config:pipeline_ids::ARRAY<STRING>" in text
    assert "framework_meta.table_health" not in text
    assert "v_bronze_tables" in text
    history = text.split("v_asset_registry_history", 1)[1]
    assert "is_deleted" in history.split(";")[0] and "LEAD(" not in history.split(";")[0]
    assert "CREATE BLOOMFILTER INDEX" not in text
    assert "v_columns" in text
    assert "v_column_history" in text
    assert "valid_from" in text
    assert "valid_to" in text
    assert "is_current" in text
    # Fresh schema: CREATE tables/views plus idempotent INSERT seeds for enum refs.
    assert {statement.split()[0].upper() for statement in statements} <= {
        "CREATE",
        "INSERT",
        "ALTER",  # only ADD COLUMN, which apply_ddl skips when the column exists
    }
    assert _column_list(text, "asset_registry") == [*OBJECT_COLUMNS, "config", *SCD_COLUMNS]
    assert _column_list(text, "ownership_refusals") == list(LEDGER_COLUMNS)


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
    names = [row["full_name"] for row in rows]
    # The declared columns come first, then the audit columns every bronze table carries.
    assert names[:3] == [
        "framework_dev.billing_bronze.invoices.invoice_id",
        "framework_dev.billing_bronze.invoices.amount",
        "framework_dev.billing_bronze.invoices.updated_at",
    ]
    assert {name.rsplit(".", 1)[1] for name in names[3:]} >= {
        "_src_file_path",
        "_src_file_name",
        "_src_row_hash",
        "_batch_id",
        "_ingest_timestamp",
    }
    audit = next(row for row in rows if row["full_name"].endswith("._batch_id"))
    assert "pipeline run" in json.loads(audit["config_json"])["description"]
    raw_names = {
        row["full_name"]
        for row in object_rows(config)
        if row["parent_full_name"] == "framework_dev.billing_raw.invoices"
    }
    assert "framework_dev.billing_raw.invoices._src_file_path" in raw_names
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
    ] == [
        # No declared columns, so only the audit columns are registered for bronze.
        "framework_dev.ledger_bronze.entries._src_file_path",
        "framework_dev.ledger_bronze.entries._src_file_name",
        "framework_dev.ledger_bronze.entries._src_row_hash",
        "framework_dev.ledger_bronze.entries._batch_id",
        "framework_dev.ledger_bronze.entries._ingest_timestamp",
    ]
    assert [
        row["full_name"]
        for row in ledger_rows
        if row["parent_full_name"] == "framework_dev.ledger_raw.entries"
    ] == [
        "framework_dev.ledger_raw.entries.account_id",
        "framework_dev.ledger_raw.entries.amount",
        "framework_dev.ledger_raw.entries._src_file_path",
        "framework_dev.ledger_raw.entries._src_file_name",
        "framework_dev.ledger_raw.entries._src_row_hash",
        "framework_dev.ledger_raw.entries._ingest_timestamp",
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


def test_ownership_refusal_row_lands():
    row = ingest_steps(
        [
            {
                "object_kind": "pipeline",
                "full_name": "billing_invoices",
                "owner_kind": "source_system",
                "owner": "erp",
                "holder_owner_kind": "source_system",
                "holder_owner": "billing",
                "failure_reason": "owned by source_system billing",
                "content_hash": "a" * 64,
                "run_id": "ownership-aaaaaaaa",
                "step_no": 1,
                "step_name": "ownership_check",
                "status": "FAILED",
            }
        ]
    )[0]
    assert row["refused_owner"] == "erp"
    assert row["holder_owner"] == "billing"
    assert "owned by source_system billing" in row["failure_reason"]


def test_a_renamed_job_versions_the_table_tag_but_not_its_columns():
    original = object_rows(config_from_document(_billing(job_name="finance_billing")))
    renamed = object_rows(config_from_document(_billing(job_name="finance_billing_retry")))

    def hashes(rows, kind, name):
        return next(
            row["content_hash"]
            for row in rows
            if row["object_kind"] == kind and row["full_name"] == name
        )

    # job_name is a table tag, so the table gets a new version. Its columns do not.
    assert hashes(original, "streaming_table", "framework_dev.billing_bronze.invoices") != hashes(
        renamed, "streaming_table", "framework_dev.billing_bronze.invoices"
    )
    original_columns = sorted(r["content_hash"] for r in original if r["object_kind"] == "column")
    renamed_columns = sorted(r["content_hash"] for r in renamed if r["object_kind"] == "column")
    assert original_columns and original_columns == renamed_columns
    assert hashes(original, "pipeline", "billing_invoices") != hashes(
        renamed, "pipeline", "billing_invoices"
    )


def test_repeated_definition_is_inserted_once(monkeypatch: pytest.MonkeyPatch):
    inserted: list[list[dict]] = []

    def fake_merge(_spark, _table, rows):
        inserted.append(rows)
        return len(rows)

    monkeypatch.setattr("lakeflow_framework.registry._scd2_merge", fake_merge)
    monkeypatch.setattr("lakeflow_framework.registry._merge_current", lambda *a, **k: None)
    row = {
        "asset_id": "e" * 64,
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
        "bundle_id": "framework_ingestion",
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
        "asset_id": "f" * 64,
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
        "bundle_id": "framework_ingestion",
        "recorded_at": "2026-11-09T00:00:00Z",
    }
    row.update(changes)
    return row


def test_asset_id_survives_a_pipeline_id_rename():
    from lakeflow_framework.metadata import pipeline_stable_key

    original = config_from_document(_billing())
    renamed = config_from_document(_billing(pipeline_id="billing_invoice_v2"))
    assert pipeline_stable_key(original) == pipeline_stable_key(renamed)
    orig_ids = {row["object_kind"]: row["asset_id"] for row in object_rows(original)}
    renamed_ids = {row["object_kind"]: row["asset_id"] for row in object_rows(renamed)}
    assert orig_ids["pipeline"] == renamed_ids["pipeline"]
    assert orig_ids["dataflow"] == renamed_ids["dataflow"]
    assert orig_ids["pipeline"] != orig_ids["dataflow"]


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


def test_owner_lookup_joins_staged_names_and_rejects_unsafe_ones():
    from lakeflow_framework.metadata import require_safe_names

    sql = owners_sql("framework_dev")
    assert "framework_dev.framework_meta.v_asset_registry_current c" in sql
    assert "INNER JOIN _framework_owner_names s" in sql
    assert "full_name IN (" not in sql
    assert "'column'" not in sql
    assert require_safe_names(["framework_dev.billing_bronze.invoices"])
    with pytest.raises(ValueError):
        require_safe_names(["framework_dev.billing_bronze.invoices", "x' OR '1'='1"])
    with pytest.raises(ValueError):
        require_safe_names(["x' OR '1'='1"])


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
        def createDataFrame(self, _rows, _schema=None):
            class Staged:
                def createOrReplaceTempView(self, _name):
                    return None

            return Staged()

        def sql(self, statement):
            if "v_asset_registry_current" in statement:
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

    def fake_merge(_spark, table, rows):
        return fake_insert(_spark, table, rows, None)

    monkeypatch.setattr("lakeflow_framework.registry._insert", fake_insert)
    monkeypatch.setattr("lakeflow_framework.registry._scd2_merge", fake_merge)
    taken = _owned_row(owner="erp", content_hash="d" * 64)
    fine = _owned_row(
        full_name="framework_dev.erp_bronze.orders", owner="erp", content_hash="e" * 64
    )
    assert append_definitions(Spark(), [taken, fine], set(), "framework_dev") == 1
    registry = written["framework_dev.framework_meta.asset_registry"]
    assert [row["full_name"] for row in registry] == ["framework_dev.erp_bronze.orders"]
    (step,) = written["framework_dev.framework_meta.ownership_refusals"]
    assert (step["refused_owner_kind"], step["refused_owner"]) == ("source_system", "erp")
    assert (step["holder_owner_kind"], step["holder_owner"]) == ("source_system", "billing")
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


def test_a_delivery_reads_its_manifest_from_the_pipeline_configuration(tmp_path: Path):
    path = INGESTION / "sources" / "abs" / "fdh" / "abs_accounting.yaml"
    (config,) = validate_configs([path], catalog="framework_test")
    manifest = tmp_path / "abs_daily.json"
    manifest.write_text(
        json.dumps(
            {
                "delivery": "abs_daily",
                "pipelines": [
                    {
                        "pipeline_id": config.pipeline_id,
                        "content_hash": config.content_hash(),
                        "config": config.canonical(),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    class Conf:
        def get(self, key, default=None):
            return {
                "lff.manifest": str(manifest),
                "lff.catalog": "framework_test",
            }.get(key, default)

    class Reader:
        def get_pipelines(self, pipeline_ids):
            raise AssertionError("the registry is not read when lff.manifest is set")

    spark, pipelines = _FakeSpark(), _FakePipelines()
    spark.conf = Conf()
    graphs = build_pipelines(["abs_accounting"], spark=spark, pipelines=pipelines)
    assert [graph.pipeline_id for graph in graphs] == ["abs_accounting"]
    assert any(name.startswith("framework_test.") for name in pipelines.names)
    # A missing/bad sibling is skipped so healthy tables still load; all missing raises.
    with pytest.raises(MetadataNotFound, match="every table in this delivery failed"):
        build_pipelines(["not_deployed"], spark=spark, pipelines=pipelines)
    both = build_pipelines(["abs_accounting", "not_deployed"], spark=spark, pipelines=pipelines)
    assert [graph.pipeline_id for graph in both] == ["abs_accounting"]
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


def test_refusal_lookup_dedupes_by_content_hash():
    digest = "a" * 64
    statement = step_keys_sql(
        "framework_dev",
        [digest, digest],
        full_names=["billing_invoices"],
    )
    assert "ownership_refusals" in statement
    assert f"content_hash IN ('{digest}')" in statement
    assert "full_name IN ('billing_invoices')" in statement
    cast = _cast("recorded_at", ("recorded_at",), (), ())
    assert cast == "CAST(recorded_at AS TIMESTAMP) AS recorded_at"
    # Databricks rejects a datetime pattern with quoted T and Z.
    assert "to_timestamp" not in cast and "''" not in cast


def test_one_merge_closes_the_changed_version_and_inserts_the_new_one():
    from lakeflow_framework.registry import scd2_merge_sql

    sql = scd2_merge_sql("c.framework_meta.asset_registry", "SELECT * FROM staged")
    assert sql.startswith("MERGE INTO c.framework_meta.asset_registry t")
    # A retry after asset_registry_current failed finds the same hash current: nothing is closed and
    # nothing is inserted, so history gets no second row.
    assert "WHERE NOT (r.content_hash <=> s.content_hash) OR coalesce(r.is_deleted, false)" in sql
    assert "WHEN MATCHED AND (NOT (t.content_hash <=> u.content_hash)" in sql
    assert "UPDATE SET t.valid_to = u.recorded_at, t.is_current = false" in sql
    assert "valid_from, valid_to, is_current, is_deleted) VALUES" in sql
    assert "u.recorded_at, NULL, true, false)" in sql
    assert "ORDER BY recorded_at DESC, content_hash DESC" in sql
    assert "QUALIFY row_number()" in sql


def test_a_removed_definition_gets_a_deletion_version_and_leaves_asset_registry_current():
    from lakeflow_framework.registry import retire_definitions, retire_statements

    close, delete = retire_statements("c", "_deployed", "source_system", ["billing"])
    assert "LEFT ANTI JOIN _deployed d" in close
    assert "r.owner_kind = 'source_system'" in close
    assert "r.owner IN ('billing')" in close
    assert "NOT coalesce(r.is_deleted, false)" in close
    assert "u.recorded_at, NULL, true, true)" in close
    assert delete.startswith("DELETE FROM c.framework_meta.asset_registry_current c")
    assert "c.owner_kind = 'source_system'" in delete
    assert "c.owner IN ('billing')" in delete
    with pytest.raises(ValueError, match="no definitions"):
        retire_definitions(object(), [], "c")
    with pytest.raises(ValueError, match="owner_kind"):
        retire_definitions(object(), [_owned_row()], "c", owner_kind="x' OR 1=1")
    with pytest.raises(ValueError, match="catalog"):
        retire_statements("not a catalog", "_deployed", "source_system", ["billing"])
    with pytest.raises(ValueError, match="owners"):
        retire_statements("c", "_deployed", "source_system", [])


def test_ddl_already_applied_runs_only_the_lookup_unless_forced():
    ran: list[str] = []
    recorded: set[str] = set()

    class Spark:
        def sql(self, statement):
            ran.append(statement)
            if statement.startswith("INSERT INTO framework_dev.framework_meta.ddl_applied"):
                recorded.add(statement.split("'")[1])
            if statement.startswith("SELECT 1 FROM framework_dev.framework_meta.ddl_applied"):
                found = statement.split("'")[1] in recorded
                return [{"1": 1}] if found else []
            return []

    first = apply_ddl(Spark(), "framework_dev")
    assert first == len(ddl_statements("framework_dev"))
    assert "ddl_applied" in ran[-1]

    ran.clear()
    assert apply_ddl(Spark(), "framework_dev") == 0
    assert len(ran) == 1

    ran.clear()
    assert apply_ddl(Spark(), "framework_dev", "SELECT 2") == 1

    ran.clear()
    assert apply_ddl(Spark(), "framework_dev", force=True) == first


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

    from lakeflow_framework.registry import OBJECT_DERIVED, _merge_current, _scd2_merge

    row = _owned_row()
    _scd2_merge(Spark(), "t", [row])
    _merge_current(Spark(), "c", [row], OBJECT_COLUMNS, ("object_kind",), derived=OBJECT_DERIVED)
    insert, merge = statements
    assert insert.startswith("MERGE INTO t t USING")
    assert "recorded_at, config, valid_from" in insert
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
    names = []
    depth = 1
    i = start
    while i < len(text) and depth:
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
        i += 1
    body = text[start : i - 1]
    for line in body.splitlines():
        part = line.strip().rstrip(",")
        if not part or part.startswith("CONSTRAINT"):
            continue
        names.append(part.split()[0])
    return names


def _dump(document: dict) -> str:
    import yaml

    return yaml.safe_dump(document)


class _Frame:
    def __init__(self, reader: _Reader):
        self.reader = reader

    def filter(self, where: str) -> _Frame:
        self.reader.wheres.append(where)
        return self

    def selectExpr(self, *exprs: str) -> _Frame:
        self.reader.exprs.append(exprs)
        return self

    def select(self, *columns: str) -> _Frame:
        return self

    def join(self, other, on=None, how=None) -> _Frame:
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

    def sql(self, statement):
        return _Frame(self.read)

    def createDataFrame(self, data, schema=None):  # noqa: N802 (Spark's name)
        return _Frame(self.read)

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
