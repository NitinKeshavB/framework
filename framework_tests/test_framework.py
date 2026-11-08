"""Spark-free tests for config, SQL, rules, registry rows, and pipeline wiring."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakeflow_framework import ConfigValidationError, build_pipeline, validate_configs
from lakeflow_framework.cli import main
from lakeflow_framework.collector import await_registered
from lakeflow_framework.config import config_from_document
from lakeflow_framework.load import get_strategy
from lakeflow_framework.metadata import (
    LEDGER_COLUMNS,
    OBJECT_COLUMNS,
    build_recon,
    ddl_statements,
    ingest_steps,
    new_object_rows,
    object_rows,
    split_payloads,
    step_file_name,
    step_payload,
    write_definition_files,
)
from lakeflow_framework.pipeline import MetadataNotFound, assemble
from lakeflow_framework.quality import compile_rules, fail_sql

EXAMPLES = (
    Path(__file__).resolve().parents[1] / "framework_examples" / "consumer" / "configs" / "bronze"
)


def _billing(**overrides):
    document = {
        "pipeline_id": "billing_invoices",
        "project": "finance",
        "layer": "bronze",
        "catalog": "framework_dev",
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
            ],
            "rules": [{"name": "amount_non_negative", "expression": "amount >= 0"}],
        },
        "quarantine": {"schema": "billing_quarantine", "table": "invoices"},
    }
    _apply(document, overrides)
    return document


def _ledger():
    return {
        "pipeline_id": "ledger_entries",
        "project": "finance",
        "layer": "bronze",
        "catalog": "framework_dev",
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
        "quarantine": {"schema": "ledger_quarantine", "table": "entries"},
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
            for section in (source_to_raw["raw"], bronze_hop["bronze"], document["quarantine"]):
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


def test_example_configs_validate():
    configs = validate_configs(sorted(EXAMPLES.glob("*.yaml")))
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
                "quarantine": {"schema": "billing_quarantine", "table": "invoices"},
            }
        )
    text = "\n".join(caught.value.errors)
    assert "project is required" in text
    assert "excel cannot use copy_into" in text
    assert "abfss:// or s3://" in text
    assert "dedup requires" in text
    assert "password" in text
    assert "unresolved" in text


def test_two_pipelines_cannot_share_a_table(tmp_path: Path):
    first = tmp_path / "a.yaml"
    second = tmp_path / "b.yaml"
    shared = _billing()
    other = _billing(pipeline_id="billing_copy", table="invoices")
    first.write_text(_dump(shared), encoding="utf-8")
    second.write_text(_dump(other), encoding="utf-8")
    with pytest.raises(ConfigValidationError) as caught:
        validate_configs([first, second])
    assert "billing_invoices and billing_copy" in "\n".join(caught.value.errors)


def test_autoloader_options_omit_checkpoint_and_schema_location():
    options = get_strategy("autoloader").options(config_from_document(_billing()))
    assert options["cloudFiles.format"] == "csv"
    assert "cloudFiles.schemaLocation" not in options
    assert "checkpoint" not in options
    assert options["rescuedDataColumn"] == "_rescued_data"


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
        project="finance",
        full_name="billing_invoices",
        run_id="100",
        step_no=10,
        step_name="mark_running",
        status="RUNNING",
    )
    success = step_payload(
        project="finance",
        full_name="billing_invoices",
        run_id="100",
        step_no=50,
        step_name="record_success",
        status="SUCCEEDED",
        recon=build_recon(source_count=10, inserted=8, updated=1, rejected=1),
    )
    keys: set = set()
    assert len(ingest_steps([running, success], keys)) == 2
    assert ingest_steps([success], keys) == []
    assert step_file_name(running).endswith("100_10_RUNNING.json")
    assert build_recon(source_count=10, inserted=3, rejected=1)["recon_result"] == "mismatch"
    assert build_recon(source_count=10, output_rows=9, rejected=1)["recon_result"] == "matched"


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
    assert kinds == ["raw", "valid", "quarantine", "bronze"]
    assert graph.auto_cdc["stored_as_scd_type"] == 1
    assert pipelines.cdc[0]["keys"] == ["invoice_id"]
    assert "cloudFiles.schemaLocation" not in spark.options
    assert any(name == "billing_invoices" or name.endswith("invoices") for name in pipelines.names)

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


def test_ddl_clusters_both_tables_and_names_the_catalog():
    statements = ddl_statements("framework_dev")
    text = "\n".join(statements)
    assert "CLUSTER BY (project, object_kind, full_name)" in text
    assert "CLUSTER BY (project, event_date, full_name, run_id)" in text
    assert "${catalog}" not in text
    assert "CREATE BLOOMFILTER INDEX" in text
    assert not any(statement.upper().startswith("DELETE") for statement in statements)
    assert _column_list(text, "object_registry") == list(OBJECT_COLUMNS)
    assert _column_list(text, "run_ledger") == list(LEDGER_COLUMNS)


def test_ctas_securables_match_the_graph():
    rows = object_rows(config_from_document(_ledger()))
    kinds = {row["full_name"]: row["object_kind"] for row in rows}
    assert kinds["framework_dev.ledger_raw.entries"] == "table"
    assert kinds["framework_dev.ledger_bronze.entries"] == "materialized_view"
    assert kinds["framework_dev.ledger_quarantine.entries"] == "materialized_view"


def test_definition_files_are_one_object_each_and_steps_stay_separate(tmp_path: Path):
    rows = object_rows(config_from_document(_billing()))
    written = write_definition_files(tmp_path, rows)
    assert len(written) == len(rows)
    payload = json.loads(written[0].read_text(encoding="utf-8"))
    assert payload["kind"] == "definition"
    steps, definitions = split_payloads(
        [payload, {"kind": "step", "run_id": "1"}, {"kind": "other"}]
    )
    assert len(steps) == 1
    assert len(definitions) == 1
    assert "kind" not in definitions[0]


def test_cli_writes_a_step_file_with_recon(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    code = main(
        [
            "step",
            "--project",
            "finance",
            "--full-name",
            "billing_invoices",
            "--run-id",
            "100",
            "--step-no",
            "50",
            "--step-name",
            "record_success",
            "--status",
            "SUCCEEDED",
            "--recon-json",
            '{"source_count": 10, "output_rows": 9, "rejected": 1}',
            "--out",
            str(tmp_path),
        ]
    )
    assert code == 0
    payload = json.loads(Path(capsys.readouterr().out.strip()).read_text(encoding="utf-8"))
    assert payload["recon"]["source_count"] == 10
    assert payload["recon"]["rejected"] == 1


def test_await_registry_polls_until_the_pipeline_row_exists():
    calls = {"n": 0}

    class Frame:
        def collect(self):
            calls["n"] += 1
            if calls["n"] == 1:
                return []
            return [{"full_name": "billing_invoices"}]

    class Spark:
        def sql(self, text):
            assert "framework_dev.framework_meta.v_object_current" in text
            return Frame()

    clock = {"t": 0.0}

    def now():
        return clock["t"]

    def pause(seconds):
        clock["t"] += seconds

    await_registered(
        Spark(),
        ["billing_invoices"],
        "framework_dev",
        timeout_seconds=30,
        pause_seconds=5,
        sleep=pause,
        clock=now,
    )
    assert calls["n"] == 2


def test_await_registry_fails_when_the_row_never_arrives():
    class Frame:
        def collect(self):
            return []

    class Spark:
        def sql(self, _text):
            return Frame()

    with pytest.raises(MetadataNotFound):
        await_registered(
            Spark(),
            ["billing_invoices"],
            "framework_dev",
            timeout_seconds=0,
            pause_seconds=5,
            sleep=lambda _seconds: None,
            clock=lambda: 0,
        )


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


class _Reader:
    def __init__(self):
        self.options = {}
        self.format_name = None

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self, path):
        return {"format": self.format_name, "path": path, "options": dict(self.options)}

    def table(self, name):
        return {"table": name}


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

    def create_streaming_table(self, **kwargs):
        self.names.append(kwargs.get("name"))

    def create_auto_cdc_flow(self, **kwargs):
        self.cdc.append(kwargs)
