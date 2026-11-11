"""Source kinds that go straight to bronze: files from a raw volume, and database tables."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from lakeflow_framework import ConfigValidationError, build_pipeline, validate_configs
from lakeflow_framework.config import config_from_document
from lakeflow_framework.metadata import object_rows
from lakeflow_framework.pipeline import assemble

COMMON = """\
domain: finance
data_domain: things
team: reporting_squad
technical_owner: reporting.owner@jll.com
layer: bronze
requested_by: reporting
data:
  classification: internal
raw_to_bronze:
  bronze: {{schema: {system}_bronze, table: things}}
  columns:
    - {{name: thing_id, type: string}}
{extra}
quarantine: {{schema: {system}_quarantine, table: things}}
"""

FILE_SOURCE = """\
source_system: files
domain: finance
owner: files.owner@jll.com
kind: sftp_files
raw_volume: {schema: files_raw, volume: landing}
deliveries:
  daily: {trigger: file_arrival}
"""

DATABASE_SOURCE = """\
source_system: db
domain: finance
owner: db.owner@jll.com
kind: database
foreign_catalog: db_federated
engine: postgresql
deliveries:
  nightly: {schedule: "0 0 2 * * ?"}
"""


def _write(folder: Path, source: str, name: str, spec: str) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "source.yaml").write_text(source, encoding="utf-8")
    path = folder / name
    path.write_text(spec, encoding="utf-8")
    return path


def _volume_spec(system="files", extra="", source="  raw_path: things/\n  format: csv\n"):
    head = f"pipeline_id: {system}_things\njob_name: {system}_daily\ndelivery: daily\n"
    body = COMMON.format(system=system, extra=extra)
    return head + body + "source:\n" + source + '  delimiter: ","\n'


def _database_spec(mode="snapshot", extra="", table="dbo.things"):
    head = "pipeline_id: db_things\njob_name: db_nightly\ndelivery: nightly\n"
    body = COMMON.format(system="db", extra=extra)
    return head + body + f"source:\n  table: {table}\n  mode: {mode}\n"


def _config(tmp_path: Path, source: str, name: str, spec: str, catalog="framework_dev"):
    path = _write(
        tmp_path / "finance" / yaml.safe_load(source)["source_system"], source, name, spec
    )
    (config,) = validate_configs([path], catalog=catalog)
    return config


class _Reader:
    def __init__(self):
        self.options, self.format_name = {}, None

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


class _Spark:
    def __init__(self):
        self.readStream, self.read = _Reader(), _Reader()


class _Pipelines:
    def __init__(self):
        self.names, self.cdc, self.snapshot_cdc, self.expectations, self.kinds = [], [], [], [], {}

    def _decorator(self, kind, kwargs):
        self.names.append(kwargs["name"])
        self.kinds[kwargs["name"]] = kind

        def decorate(func):
            func()
            return func

        return decorate

    def table(self, **kwargs):
        return self._decorator("table", kwargs)

    def temporary_view(self, **kwargs):
        return self._decorator("view", kwargs)

    def materialized_view(self, **kwargs):
        return self._decorator("materialized_view", kwargs)

    def expect(self, name, expression):
        self.expectations.append(name)
        return lambda func: func

    def create_streaming_table(self, **kwargs):
        self.names.append(kwargs["name"])

    def create_auto_cdc_flow(self, **kwargs):
        self.cdc.append(kwargs)

    def create_auto_cdc_from_snapshot_flow(self, **kwargs):
        self.snapshot_cdc.append(kwargs)


def test_volume_spec_reads_the_raw_volume_and_has_no_raw_table(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    assert config.load_method == "volume_files"
    assert config.path == "/Volumes/framework_dev/files_raw/landing/things/"
    assert not config.has_raw and config.layers == ("bronze", "quarantine")
    with pytest.raises(ValueError):
        config.table_name("raw")
    graph = assemble(config)
    assert [dataset.kind for dataset in graph.datasets] == ["valid", "quarantine", "bronze"]
    assert graph.datasets[0].detail["source"] == config.path


def test_database_spec_reads_the_foreign_catalog(tmp_path: Path) -> None:
    config = _config(tmp_path, DATABASE_SOURCE, "db_things.yaml", _database_spec())
    assert config.load_method == "database_table"
    assert config.source_reference() == "db_federated.dbo.things"
    assert config.is_batch and not config.uses_auto_cdc
    graph = assemble(config)
    assert [dataset.relation for dataset in graph.datasets] == ["temporary_view", "batch", "batch"]


def test_merge_mode_uses_snapshot_cdc_on_the_keys(tmp_path: Path) -> None:
    extra = "  primary_keys: [thing_id]\n"
    spec = _database_spec(mode="merge").replace("  columns:", extra + "  columns:")
    config = _config(tmp_path, DATABASE_SOURCE, "db_things.yaml", spec)
    assert config.uses_auto_cdc
    spark, pipelines = _Spark(), _Pipelines()
    build_pipeline("db_things", spark=spark, pipelines=pipelines, config=config)
    assert pipelines.snapshot_cdc == [
        {
            "target": "framework_dev.db_bronze.things",
            "source": "db_things_valid",
            "keys": ["thing_id"],
            "stored_as_scd_type": 1,
        }
    ]
    assert not pipelines.cdc


def test_snapshot_mode_replaces_bronze_with_a_materialized_view(tmp_path: Path) -> None:
    config = _config(tmp_path, DATABASE_SOURCE, "db_things.yaml", _database_spec())
    pipelines = _Pipelines()
    build_pipeline("db_things", spark=_Spark(), pipelines=pipelines, config=config)
    assert pipelines.kinds["framework_dev.db_bronze.things"] == "materialized_view"
    assert pipelines.kinds["framework_dev.db_quarantine.things"] == "materialized_view"


def test_volume_files_register_streaming_tables_without_a_raw_table(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    spark, pipelines = _Spark(), _Pipelines()
    build_pipeline("files_things", spark=spark, pipelines=pipelines, config=config)
    assert pipelines.kinds["framework_dev.files_bronze.things"] == "table"
    assert pipelines.kinds["framework_dev.files_quarantine.things"] == "table"
    assert not any(name.endswith("_raw.things") for name in pipelines.names)
    assert not pipelines.expectations
    assert spark.readStream.format_name == "cloudFiles"
    assert spark.readStream.options["cloudFiles.format"] == "csv"


def test_registry_rows_skip_the_raw_layer_and_round_trip(tmp_path: Path) -> None:
    for source, name, spec in (
        (FILE_SOURCE, "files_things.yaml", _volume_spec()),
        (DATABASE_SOURCE, "db_things.yaml", _database_spec()),
    ):
        config = _config(tmp_path, source, name, spec)
        rows = object_rows(config)
        tables = [
            row["full_name"] for row in rows if row["object_kind"].endswith(("table", "view"))
        ]
        assert len(tables) == 2 and not any("_raw." in table for table in tables)
        assert {row["full_name"] for row in rows if row["object_kind"] == "flow"} == {
            f"{config.pipeline_id}.bronze",
            f"{config.pipeline_id}.quarantine",
        }
        again = config_from_document(config.canonical())
        assert again.load_method == config.load_method
        assert again.content_hash() == config.content_hash()


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        (_database_spec(table="things"), "<schema>.<table>"),
        (_database_spec(mode="stream"), "source.mode must be one of"),
        (_database_spec(mode="merge"), "merge requires primary_keys"),
        (_database_spec(extra="  dedup: true\n"), "not dedup"),
    ],
)
def test_database_spec_problems_are_named(tmp_path: Path, spec: str, message: str) -> None:
    path = _write(tmp_path / "db", DATABASE_SOURCE, "db_things.yaml", spec)
    with pytest.raises(ConfigValidationError) as caught:
        validate_configs([path], catalog="framework_dev")
    assert message in " ".join(caught.value.errors)


def test_volume_spec_problems_are_named(tmp_path: Path) -> None:
    folder = tmp_path / "files"
    path = _write(folder, FILE_SOURCE, "files_things.yaml", _volume_spec(source="  format: csv\n"))
    with pytest.raises(ConfigValidationError) as caught:
        validate_configs([path], catalog="framework_dev")
    assert "raw_path is required" in " ".join(caught.value.errors)
    fixed = _volume_spec(source="  raw_path: things/\n  format: fixed_width\n")
    path = _write(folder, FILE_SOURCE, "files_things.yaml", fixed)
    with pytest.raises(ConfigValidationError) as caught:
        validate_configs([path], catalog="framework_dev")
    assert "not supported from a raw volume" in " ".join(caught.value.errors)


def test_spec_with_a_source_section_needs_its_source_yaml(tmp_path: Path) -> None:
    path = tmp_path / "files_things.yaml"
    path.write_text(_volume_spec(), encoding="utf-8")
    with pytest.raises(ConfigValidationError) as caught:
        validate_configs([path], catalog="framework_dev")
    assert "source.yaml is missing" in " ".join(caught.value.errors)
