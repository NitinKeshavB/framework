"""Source-system structure and the files generated from it. No Spark."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import yaml

from lakeflow_framework.cli import main
from lakeflow_framework.config import ConfigValidationError
from lakeflow_framework.ingestion import MAP_NAME, generate, load_sources, new_source, new_table
from lakeflow_framework.metadata import object_rows, workflow_rows
from lakeflow_framework.pipeline import build_pipeline

SAMPLE = Path(__file__).resolve().parents[1] / "framework_ingestion"

LEGACY_SOURCE = """\
source_system: legacy
domain: finance
owner: legacy.owner@jll.com
kind: cloud_files
deliveries:
  daily:
    trigger: file_arrival
    path: abfss://landing@storage.dfs.core.windows.net/legacy/
"""

LEGACY_SPEC = """\
pipeline_id: legacy_credits
domain: finance
data_domain: credits
team: legacy_squad
technical_owner: legacy.owner@jll.com
layer: bronze
job_name: legacy_daily
delivery: daily
requested_by: legacy
data:
  classification: internal
source_to_raw:
  method: copy_into
  source:
    system: legacy
    format: csv
    path: abfss://landing@storage.dfs.core.windows.net/legacy/credits/
    delimiter: ","
    header: true
  raw:
    schema: legacy_raw
    table: credits
raw_to_bronze:
  bronze:
    schema: legacy_bronze
    table: credits
  columns:
    - name: credit_id
      type: string
quarantine:
  schema: legacy_quarantine
  table: credits
"""


@pytest.fixture()
def bundle(tmp_path: Path) -> Path:
    """The billing source only: one SFTP source with one table."""

    shutil.copytree(SAMPLE / "sources" / "billing", tmp_path / "sources" / "billing")
    return tmp_path


def _add_table(bundle: Path, name: str, requested_by: str, **changes: str) -> Path:
    """A second table in the billing source, as another team would add it."""

    folder = bundle / "sources" / "billing" / requested_by
    folder.mkdir(parents=True, exist_ok=True)
    text = (bundle / "sources" / "billing" / "billing" / "billing_invoices.yaml").read_text(
        encoding="utf-8"
    )
    text = text.replace("billing_invoices", f"billing_{name}")
    text = text.replace("requested_by: billing", f"requested_by: {requested_by}")
    text = text.replace("table: invoices", f"table: {name}")
    text = text.replace("raw_path: invoices/", f"raw_path: {name}/")
    for old, new in changes.items():
        text = text.replace(old, new)
    path = folder / f"billing_{name}.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def _job(bundle: Path, name: str = "billing_daily") -> dict:
    document = yaml.safe_load((bundle / "resources" / f"{name}.job.yml").read_text("utf-8"))
    return document["resources"]["jobs"][name]


def _tasks(bundle: Path, name: str = "billing_daily") -> dict:
    return {task["task_key"]: task for task in _job(bundle, name)["tasks"]}


def test_every_source_kind_is_in_the_sample(tmp_path: Path) -> None:
    systems = {system.name: system for system in load_sources(SAMPLE)}
    assert {name: system.kind for name, system in systems.items()} == {
        "billing": "sftp_files",
        "erp": "database",
        "hr_docs": "sharepoint_files",
    }
    assert [t.config.load_method for t in systems["billing"].tables] == ["volume_files"]
    assert {t.config.load_method for t in systems["erp"].tables} == {"database_table"}
    assert len(systems["erp"].tables) == 2


def test_sample_loads_with_its_delivery_and_requester(bundle: Path) -> None:
    (system,) = load_sources(bundle)
    assert system.name == "billing" and system.domain == "finance"
    assert system.raw_volume == {"schema": "billing_raw", "volume": "landing"}
    (table,) = system.tables
    assert table.delivery == "daily"
    assert table.requested_by == "billing"


def test_two_teams_can_add_tables_to_one_source_system(bundle: Path) -> None:
    _add_table(bundle, "credits", "collections")
    (system,) = load_sources(bundle)
    assert sorted(table.requested_by for table in system.tables) == ["billing", "collections"]
    generate(bundle)
    assert "| collections | 1 | billing |" in (bundle / MAP_NAME).read_text("utf-8")


def test_generate_writes_then_is_up_to_date(bundle: Path) -> None:
    written = generate(bundle)
    assert written == [
        "SOURCE_MAP.md",
        "pipelines/billing_daily.py",
        "resources/billing_daily.job.yml",
        "resources/billing_daily.pipeline.yml",
    ]
    assert generate(bundle, check=True) == []
    assert generate(bundle) == []


def test_resource_keys_are_unique_and_the_job_refreshes_its_pipeline(bundle: Path) -> None:
    generate(bundle)
    pipelines = yaml.safe_load((bundle / "resources/billing_daily.pipeline.yml").read_text("utf-8"))
    (pipeline_key,) = pipelines["resources"]["pipelines"]
    assert pipeline_key != "billing_daily"  # the job's key
    refresh = _tasks(bundle)["refresh_pipeline"]
    assert refresh["pipeline_task"]["pipeline_id"] == f"${{resources.pipelines.{pipeline_key}.id}}"


def test_check_fails_when_a_table_is_added_without_regenerating(bundle: Path) -> None:
    generate(bundle)
    _add_table(bundle, "credits", "collections")
    assert generate(bundle, check=True)
    assert main(["generate", str(bundle), "--check"]) == 1
    assert main(["generate", str(bundle)]) == 0
    assert main(["generate", str(bundle), "--check"]) == 0


def test_one_delivery_is_one_pipeline_and_one_job_for_every_team(bundle: Path) -> None:
    _add_table(bundle, "credits", "collections")
    generate(bundle)
    source = (bundle / "pipelines" / "billing_daily.py").read_text("utf-8")
    assert 'build_pipeline("billing_credits")' in source
    assert 'build_pipeline("billing_invoices")' in source
    tasks = _tasks(bundle)
    assert tasks["sync"]["spark_python_task"]["parameters"].count("--out") == 1
    awaited = tasks["await_registry"]["spark_python_task"]["parameters"]
    assert awaited.count("--pipeline") == 2
    assert "pipeline_task" in tasks["refresh_pipeline"]
    failed_after = {item["task_key"] for item in tasks["record_failure"]["depends_on"]}
    assert failed_after == {
        "mark_running",
        "sync",
        "await_registry",
        "refresh_pipeline",
        "record_success",
    }
    running = tasks["mark_running"]["spark_python_task"]["parameters"]
    assert running[running.index("--object-kind") + 1] == "workflow"
    assert running[running.index("--full-name") + 1] == "billing_daily"


def test_file_sources_fire_when_files_land_in_the_raw_volume(bundle: Path) -> None:
    generate(bundle)
    url = _job(bundle)["trigger"]["file_arrival"]["url"]
    assert url == "/Volumes/${var.catalog}/billing_raw/landing/"
    assert "batch_load" not in _tasks(bundle)


def test_database_job_has_a_schedule_and_syncs_both_tables(tmp_path: Path) -> None:
    shutil.copytree(SAMPLE / "sources" / "erp", tmp_path / "sources" / "erp")
    generate(tmp_path)
    job = _job(tmp_path, "erp_nightly")
    assert job["schedule"]["quartz_cron_expression"] == "0 0 2 * * ?"
    assert "trigger" not in job
    synced = _tasks(tmp_path, "erp_nightly")["sync"]["spark_python_task"]["parameters"]
    assert sum(item.endswith(".yaml") for item in synced) == 2
    source = (tmp_path / "pipelines" / "erp_nightly.py").read_text("utf-8")
    assert 'build_pipeline("erp_customers")' in source and 'build_pipeline("erp_orders")' in source


def test_scheduled_delivery_gets_a_schedule_not_a_file_trigger(bundle: Path) -> None:
    folder = bundle / "sources" / "billing"
    text = (folder / "source.yaml").read_text("utf-8")
    text = text.replace("trigger: file_arrival", 'schedule: "0 0 2 * * ?"')
    (folder / "source.yaml").write_text(text, "utf-8")
    generate(bundle)
    job = _job(bundle)
    assert job["schedule"]["quartz_cron_expression"] == "0 0 2 * * ?"
    assert "trigger" not in job


def test_cloud_files_source_keeps_its_raw_table_and_adds_a_batch_load_step(bundle: Path) -> None:
    folder = bundle / "sources" / "legacy" / "legacy"
    folder.mkdir(parents=True)
    (folder.parent / "source.yaml").write_text(LEGACY_SOURCE, "utf-8")
    (folder / "legacy_credits.yaml").write_text(LEGACY_SPEC, "utf-8")
    generate(bundle)
    tasks = _tasks(bundle, "legacy_daily")
    assert list(tasks["batch_load"]["spark_python_task"]["parameters"]) == [
        "--catalog",
        "${var.catalog}",
        "${workspace.file_path}/sources/legacy/legacy/legacy_credits.yaml",
    ]
    assert tasks["refresh_pipeline"]["depends_on"] == [{"task_key": "batch_load"}]
    assert _job(bundle, "legacy_daily")["trigger"]["file_arrival"]["url"].startswith("abfss://")


def test_specs_do_not_name_a_catalog_and_jobs_pass_the_environment_catalog(bundle: Path) -> None:
    spec = bundle / "sources/billing/billing/billing_invoices.yaml"
    assert "catalog:" not in spec.read_text("utf-8")
    generate(bundle)
    parameters = _tasks(bundle)["sync"]["spark_python_task"]["parameters"]
    assert parameters[parameters.index("--catalog") + 1] == "${var.catalog}"
    (system,) = load_sources(bundle)
    assert all(table.config.catalog == "catalog" for table in system.tables)


def test_removed_table_removes_its_generated_files_and_leaves_others(bundle: Path) -> None:
    generate(bundle)
    (bundle / "resources" / "hand_written.job.yml").write_text("resources: {}\n", "utf-8")
    folder = bundle / "sources" / "billing"
    text = (folder / "source.yaml").read_text("utf-8").replace("daily:", "nightly:")
    (folder / "source.yaml").write_text(text, "utf-8")
    spec_path = folder / "billing" / "billing_invoices.yaml"
    spec = spec_path.read_text("utf-8")
    spec_path.write_text(
        spec.replace("delivery: daily", "delivery: nightly").replace(
            "billing_daily", "billing_nightly"
        ),
        "utf-8",
    )
    generate(bundle)
    assert not (bundle / "resources" / "billing_daily.job.yml").exists()
    assert (bundle / "resources" / "billing_nightly.job.yml").exists()
    assert (bundle / "resources" / "hand_written.job.yml").exists()


@pytest.mark.parametrize("kind", ["sftp_files", "sharepoint_files", "database", "cloud_files"])
def test_scaffolds_work_for_every_kind_and_are_refused_until_finished(
    tmp_path: Path, kind: str
) -> None:
    source = new_source(
        tmp_path, "retail", "orders", owner="orders.owner@jll.com", kind=kind, delivery="hourly"
    )
    assert source == tmp_path / "sources/orders/source.yaml"
    table = new_table(tmp_path, "retail", "orders", "events", requested_by="sales", delivery=None)
    assert table == tmp_path / "sources/orders/sales/orders_events.yaml"
    text = table.read_text("utf-8")
    assert "delivery: hourly" in text and "requested_by: sales" in text
    with pytest.raises(ConfigValidationError) as caught:
        generate(tmp_path)
    assert any("replace every TODO" in error for error in caught.value.errors)


def test_a_finished_scaffold_generates(tmp_path: Path) -> None:
    new_source(
        tmp_path, "retail", "orders", owner="orders.owner@jll.com", kind="sftp_files",
        delivery="hourly",
    )  # fmt: skip
    new_table(tmp_path, "retail", "orders", "events", requested_by="sales", delivery=None)
    folder = tmp_path / "sources/orders"
    for path in folder.rglob("*.yaml"):
        text = path.read_text("utf-8").replace("TODO_column_name", "event_id")
        path.write_text(text.replace("TODO describe the column", "Event identifier"), "utf-8")
    assert "resources/orders_hourly.job.yml" in generate(tmp_path)


def test_scaffold_refuses_to_overwrite_and_to_guess_a_delivery(tmp_path: Path) -> None:
    kwargs = {"owner": "orders.owner@jll.com", "kind": "database", "delivery": "daily"}
    new_source(tmp_path, "retail", "orders", **kwargs)
    with pytest.raises(ConfigValidationError):
        new_source(tmp_path, "retail", "orders", **kwargs)
    with pytest.raises(ConfigValidationError):
        new_source(tmp_path, "retail", "orders", **{**kwargs, "kind": "ftp"})
    with pytest.raises(ConfigValidationError):
        new_table(tmp_path, "retail", "missing", "events", requested_by="sales", delivery=None)
    with pytest.raises(ConfigValidationError):
        new_table(tmp_path, "retail", "orders", "bad name", requested_by="sales", delivery=None)
    source = tmp_path / "sources/orders/source.yaml"
    source.write_text(
        source.read_text("utf-8") + "  weekly:\n    schedule: '0 0 3 ? * MON'\n", "utf-8"
    )
    with pytest.raises(ConfigValidationError) as caught:
        new_table(tmp_path, "retail", "orders", "events", requested_by="sales", delivery=None)
    assert "pass --delivery" in caught.value.errors[0]


def test_duplicate_pipeline_id_is_rejected(bundle: Path) -> None:
    folder = bundle / "sources" / "billing" / "billing"
    shutil.copy(folder / "billing_invoices.yaml", folder / "billing_invoices_copy.yaml")
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    assert any("already used" in error or "written by" in error for error in caught.value.errors)


def test_same_target_table_in_two_pipelines_is_rejected(bundle: Path) -> None:
    _add_table(bundle, "again", "collections", **{"table: again": "table: invoices"})
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    assert any("is written by" in error for error in caught.value.errors)


def test_source_folder_without_source_yaml_is_rejected(bundle: Path) -> None:
    (bundle / "sources" / "billing" / "source.yaml").unlink()
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    assert any("source.yaml is missing" in error for error in caught.value.errors)


def test_table_must_name_a_known_delivery(bundle: Path) -> None:
    _add_table(bundle, "credits", "collections", **{"delivery: daily": "delivery: weekly"})
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    assert "delivery must be one of daily" in " ".join(caught.value.errors)


def test_table_must_say_who_requested_it(bundle: Path) -> None:
    path = _add_table(bundle, "credits", "collections")
    text = path.read_text(encoding="utf-8").replace("requested_by: collections\n", "")
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    assert "requested_by must be collections" in " ".join(caught.value.errors)


def test_specs_under_sources_use_requested_by_not_project(bundle: Path) -> None:
    path = _add_table(bundle, "credits", "collections")
    text = path.read_text(encoding="utf-8").replace(
        "requested_by: collections", "project: collections"
    )
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    assert "project is replaced by requested_by" in " ".join(caught.value.errors)


def test_job_name_must_match_the_generated_job(bundle: Path) -> None:
    _add_table(bundle, "credits", "collections", **{"job_name: billing_daily": "job_name: other"})
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    assert any("job_name must be billing_daily" in error for error in caught.value.errors)


def _source_problems(bundle: Path, find: str, replace: str) -> str:
    path = bundle / "sources" / "billing" / "source.yaml"
    path.write_text(path.read_text("utf-8").replace(find, replace), "utf-8")
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    return " ".join(caught.value.errors)


def test_source_kind_rules_are_enforced(bundle: Path) -> None:
    assert "kind must be one of" in _source_problems(bundle, "kind: sftp_files", "kind: ftp")


def test_file_source_needs_a_raw_volume(bundle: Path) -> None:
    errors = _source_problems(bundle, "  volume: landing\n", "")
    assert "needs raw_volume with a schema and a volume" in errors


def test_fetch_is_refused_until_it_is_built(bundle: Path) -> None:
    errors = _source_problems(bundle, "deliveries:", "fetch: {from: sftp}\ndeliveries:")
    assert "fetch is not built" in errors


def test_database_source_needs_a_foreign_catalog_and_a_schedule() -> None:
    folder = SAMPLE / "sources" / "erp"
    document = yaml.safe_load((folder / "source.yaml").read_text("utf-8"))
    assert document["foreign_catalog"] and "schedule" in document["deliveries"]["nightly"]


def test_database_delivery_cannot_wait_for_a_file(tmp_path: Path) -> None:
    folder = tmp_path / "sources" / "erp"
    shutil.copytree(SAMPLE / "sources" / "erp", folder)
    path = folder / "source.yaml"
    text = path.read_text("utf-8").replace('schedule: "0 0 2 * * ?"', "trigger: file_arrival")
    path.write_text(text, "utf-8")
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(tmp_path)
    assert "runs on a schedule" in " ".join(caught.value.errors)


def test_spec_shape_must_match_the_source_kind(bundle: Path) -> None:
    folder = bundle / "sources" / "legacy"
    folder.mkdir()
    (folder / "source.yaml").write_text(
        LEGACY_SOURCE.replace("cloud_files", "sftp_files").replace(
            "    path: abfss://landing@storage.dfs.core.windows.net/legacy/\n", ""
        )
        + "raw_volume: {schema: legacy_raw, volume: landing}\n",
        "utf-8",
    )
    project = folder / "legacy"
    project.mkdir()
    (project / "legacy_credits.yaml").write_text(LEGACY_SPEC, "utf-8")
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    assert any("a sftp_files source loads with volume_files" in e for e in caught.value.errors)


def test_table_in_the_wrong_source_folder_is_rejected(bundle: Path) -> None:
    folder = bundle / "sources" / "billing"
    other = bundle / "sources" / "ledger"
    other.mkdir()
    text = (folder / "source.yaml").read_text(encoding="utf-8").replace("billing", "ledger")
    (other / "source.yaml").write_text(text, encoding="utf-8")
    project = other / "billing"
    project.mkdir()
    shutil.copy(folder / "billing" / "billing_invoices.yaml", project / "ledger_invoices.yaml")
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    assert any("pipeline_id must start with ledger_" in error for error in caught.value.errors)


class _Frame:
    def __init__(self, reader: "_Reader") -> None:
        self.reader = reader

    def filter(self, where: str) -> "_Frame":
        self.reader.wheres.append(where)
        return self

    def selectExpr(self, *_parts: str) -> "_Frame":
        return self


class _Reader:
    def __init__(self) -> None:
        self.options: dict[str, str] = {}
        self.loads = 0
        self.wheres: list[str] = []

    def format(self, name: str) -> "_Reader":
        self.format_name = name
        return self

    def option(self, key: str, value: str) -> "_Reader":
        self.options[key] = value
        return self

    def load(self, path: str) -> _Frame:
        self.loads += 1
        self.path = path
        return _Frame(self)

    def table(self, name: str) -> _Frame:
        return _Frame(self)


class _Spark:
    def __init__(self) -> None:
        self.readStream = _Reader()
        self.read = _Reader()


class _Pipelines:
    def __init__(self) -> None:
        self.names: list[str] = []

    def _decorate(self, name: str):
        self.names.append(name)

        def decorate(func):
            func()
            return func

        return decorate

    def table(self, **kwargs):
        return self._decorate(kwargs["name"])

    def temporary_view(self, **kwargs):
        return self._decorate(kwargs["name"])

    def materialized_view(self, **kwargs):
        return self._decorate(kwargs["name"])

    def create_streaming_table(self, **kwargs):
        self.names.append(kwargs["name"])

    def create_auto_cdc_flow(self, **kwargs):
        self.cdc = kwargs

    def create_auto_cdc_from_snapshot_flow(self, **kwargs):
        self.snapshot = kwargs


def test_billing_raw_to_bronze_is_pipeline_job_dataflow_and_flows(bundle: Path) -> None:
    generate(bundle)
    assert 'build_pipeline("billing_invoices")' in (
        bundle / "pipelines" / "billing_daily.py"
    ).read_text("utf-8")
    refresh = _tasks(bundle)["refresh_pipeline"]["pipeline_task"]["pipeline_id"]
    assert refresh == "${resources.pipelines.billing_daily_pipeline.id}"

    (system,) = load_sources(bundle)
    config = system.tables[0].config
    rows = object_rows(config)
    flows = {row["full_name"]: row for row in rows if row["object_kind"] == "flow"}
    assert set(flows) == {
        "billing_invoices.raw",
        "billing_invoices.bronze",
        "billing_invoices.quarantine",
    }
    assert flows["billing_invoices.raw"]["relation"] == "reads"
    assert json.loads(flows["billing_invoices.raw"]["config_json"])["path"].endswith(
        "/billing_raw/landing/invoices/"
    )
    assert any(
        row["object_kind"] == "dataflow" and row["full_name"] == "billing_invoices" for row in rows
    )
    securables = {
        row["full_name"]
        for row in rows
        if row["object_kind"] in {"table", "streaming_table", "materialized_view"}
    }
    assert not any("billing_raw" in name for name in securables)
    assert any(name.endswith("billing_bronze.invoices") for name in securables)
    assert any(name.endswith("billing_quarantine.invoices") for name in securables)
    (job,) = workflow_rows([config])
    assert job["object_kind"] == "workflow" and job["full_name"] == "billing_daily"
    assert json.loads(job["config_json"]) == {
        "pipeline_ids": ["billing_invoices"],
        "requested_by": ["billing"],
    }

    spark, pipelines = _Spark(), _Pipelines()
    build_pipeline("billing_invoices", spark=spark, pipelines=pipelines, config=config)
    assert spark.readStream.loads == 1
    assert spark.readStream.format_name == "cloudFiles"
    assert "billing_invoices_landing" in pipelines.names
    assert any("amount >= 0" in where for where in spark.readStream.wheres)
    assert any(where.startswith("NOT (") for where in spark.readStream.wheres)

    _add_table(bundle, "credits", "billing")
    configs = [table.config for table in load_sources(bundle)[0].tables]
    (job,) = workflow_rows(configs)
    assert json.loads(job["config_json"]) == {
        "pipeline_ids": ["billing_credits", "billing_invoices"],
        "requested_by": ["billing"],
    }


def test_a_spec_with_no_project_sits_next_to_source_yaml(bundle: Path) -> None:
    source = bundle / "sources" / "billing"
    text = (source / "billing" / "billing_invoices.yaml").read_text("utf-8")
    text = text.replace("requested_by: billing\n", "")
    text = text.replace("billing_invoices", "billing_reference")
    text = text.replace("table: invoices", "table: reference")
    text = text.replace("raw_path: invoices/", "raw_path: reference/")
    (source / "billing_reference.yaml").write_text(text, "utf-8")
    (system,) = load_sources(bundle)
    plain = next(
        table for table in system.tables if table.config.pipeline_id == "billing_reference"
    )
    assert plain.requested_by == ""
    text = text.replace("data:", "requested_by: billing\ndata:")
    (source / "billing_reference.yaml").write_text(text, "utf-8")
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    assert "belongs in billing/billing/" in " ".join(caught.value.errors)
