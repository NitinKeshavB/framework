"""Source kinds that go straight to bronze: files from a raw volume, and database tables."""

from __future__ import annotations

import json
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
quarantine: {{schema: {system}_quarantine}}
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


class _Frame:
    """DataFrame-like double: production pipeline code requires .filter / .selectExpr."""

    def __init__(self, reader, payload=None):
        self.reader = reader
        self.payload = payload or {}

    def filter(self, where):
        self.reader.wheres = getattr(self.reader, "wheres", []) + [where]
        return self

    def selectExpr(self, *parts):
        self.reader.selects = getattr(self.reader, "selects", []) + [parts]
        return self

    def select(self, *parts):
        return self

    def join(self, other, on=None, how=None):
        self.reader.joins = getattr(self.reader, "joins", []) + [(on, how)]
        return self

    @property
    def columns(self):
        return list(self.payload.get("columns") or [])


class _Reader:
    def __init__(self, spark):
        self.spark, self.options, self.format_name = spark, {}, None

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self, path):
        self.loads = getattr(self, "loads", 0) + 1
        self.spark.reads.append((self.spark.building, "files", path))
        return _Frame(
            self, {"format": self.format_name, "path": path, "options": dict(self.options)}
        )

    def table(self, name):
        self.spark.reads.append((self.spark.building, "table", name))
        return _Frame(self, {"table": name})


class _Spark:
    """Records each read with the dataset whose function made it."""

    def __init__(self):
        self.reads, self.building = [], None
        self.readStream, self.read = _Reader(self), _Reader(self)
        self.statements: list[str] = []

    def createDataFrame(self, data, schema=None):  # noqa: N802 (Spark's name)
        self.reads.append((self.building, "rows", None))
        return _Frame(self.read)

    def sql(self, statement):
        self.statements.append(statement)
        self.reads.append((self.building, "sql", None))
        return _Frame(self.read)


class _Pipelines:
    def __init__(self, spark=None):
        self.spark = spark
        self.names, self.cdc, self.snapshot_cdc, self.expectations, self.kinds = [], [], [], [], {}
        self.options = {}

    def _decorator(self, kind, kwargs):
        self.names.append(kwargs["name"])
        self.kinds[kwargs["name"]] = kind
        self.options[kwargs["name"]] = kwargs

        def decorate(func):
            if self.spark is not None:
                self.spark.building = kwargs["name"]
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

    def expect_all(self, constraints):
        self.expectations.extend(constraints)
        return lambda func: func

    def create_streaming_table(self, **kwargs):
        self.names.append(kwargs["name"])
        self.kinds[kwargs["name"]] = "streaming_table"
        self.options[kwargs["name"]] = kwargs

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
    assert [dataset.kind for dataset in graph.datasets] == [
        "staged",
        "valid",
        "files",
        "bronze",
        "quarantine",
    ]
    assert graph.datasets[0].detail["source"] == config.path
    assert graph.datasets[4].detail == {
        "source": "files_things_staged",
        "where": "_dq_result IN ('fail', 'quarantine')",
    }


SCD1 = "  load_type: scd1\n  primary_keys: [thing_id]\n  watermark_column: thing_id"


def _build(tmp_path: Path, spec: str):
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", spec)
    spark = _Spark()
    pipelines = _Pipelines(spark)
    build_pipeline("files_things", spark=spark, pipelines=pipelines, config=config)
    return config, spark, pipelines


def _staged_select(spark) -> tuple[str, ...]:
    (select,) = [s for s in spark.readStream.selects if s and s[-1].endswith("AS _dq_failed_rules")]
    return select


def test_every_file_row_is_stamped_with_the_audit_columns(tmp_path: Path) -> None:
    from lakeflow_framework.pipeline import batch_id

    _config_, spark, _pipelines = _build(tmp_path, _volume_spec())
    read = next(s for s in spark.readStream.selects if "current_timestamp()" in " ".join(s))
    # The hash is computed where * is only what the source gave, before the audit columns exist.
    assert read == (
        "*",
        "sha2(to_json(struct(*)), 256) AS _src_row_hash",
        "_metadata.file_path AS _src_file_path",
        "_metadata.file_name AS _src_file_name",
        "current_timestamp() AS _ingest_timestamp",
    )
    staged = _staged_select(spark)
    assert staged[0] == "*" and staged[1] == f"'{batch_id()}' AS _batch_id" and len(staged) == 3


def test_the_batch_id_is_one_value_for_the_whole_run() -> None:
    import uuid

    from lakeflow_framework.pipeline import batch_id

    assert batch_id() == batch_id()
    assert str(uuid.UUID(batch_id())) == batch_id()


def test_load_type_is_derived_from_the_older_settings_and_checked(tmp_path: Path) -> None:
    assert _config(tmp_path, FILE_SOURCE, "a.yaml", _volume_spec()).load_type == "insert_only"
    dedup = _volume_spec(
        extra="  dedup: true\n  primary_keys: [thing_id]\n  watermark_column: thing_id"
    )
    assert _config(tmp_path, FILE_SOURCE, "b.yaml", dedup).load_type == "scd1"
    scd2 = _config(
        tmp_path, FILE_SOURCE, "c.yaml", _volume_spec(extra=SCD1.replace("scd1", "scd2"))
    )
    assert scd2.load_type == "scd2" and scd2.dedup and scd2.scd_type == 2

    for bad, message in (
        ("  load_type: nope", "load_type must be one of"),
        ("  load_type: full_refresh", "only for database tables"),
        ("  load_type: scd1", "dedup requires primary_keys"),
        ("  load_type: insert_only\n  dedup: true", "conflicts with dedup"),
    ):
        with pytest.raises(ConfigValidationError, match=message):
            _config(tmp_path, FILE_SOURCE, "d.yaml", _volume_spec(extra=bad))
    config = _config(tmp_path, FILE_SOURCE, "e.yaml", _volume_spec(extra=SCD1))
    assert config_from_document(config.canonical()).load_type == "scd1"


def test_scd2_opens_a_version_only_when_the_data_changes(tmp_path: Path) -> None:
    config, _spark, pipelines = _build(tmp_path, _volume_spec(extra=SCD1.replace("scd1", "scd2")))
    assert pipelines.cdc[0]["stored_as_scd_type"] == 2
    # The load stamps must not look like a change: a replayed file would open a version each time.
    assert pipelines.cdc[0]["track_history_except_column_list"] == [
        "_src_file_path",
        "_src_file_name",
        "_batch_id",
        "_ingest_timestamp",
    ]
    assert config.uses_auto_cdc


def test_delete_when_keeps_the_row_and_flags_it(tmp_path: Path) -> None:
    spec = _volume_spec(extra=SCD1 + "\n  delete_when: thing_id = 'x'")
    config, spark, pipelines = _build(tmp_path, spec)
    assert (
        _staged_select(spark)[2]
        == "coalesce(CAST((thing_id = 'x') AS BOOLEAN), false) AS _is_deleted"
    )
    assert "apply_as_deletes" not in pipelines.cdc[0]  # flagged, never removed
    registered = {
        row["full_name"].rsplit(".", 1)[1]
        for row in object_rows(config)
        if row["object_kind"] == "column" and row["parent_full_name"].endswith("things")
    }
    assert "_is_deleted" in registered
    with pytest.raises(ConfigValidationError, match="only for load_type scd1"):
        _config(
            tmp_path, FILE_SOURCE, "f.yaml", _volume_spec(extra="  delete_when: thing_id = 'x'")
        )


def test_bronze_and_quarantine_keep_a_change_feed_and_cluster_on_load_time(tmp_path: Path) -> None:
    _config_, _spark, pipelines = _build(tmp_path, _volume_spec())
    for table in (
        "framework_dev.files_bronze.things",
        "framework_dev.files_quarantine.things_qrntn",
    ):
        options = pipelines.options[table]
        properties = options["table_properties"]
        assert properties["delta.enableChangeDataFeed"] == "true"
        assert properties["delta.deletedFileRetentionDuration"] == "interval 30 days"
        assert options["cluster_by"] == ["_ingest_timestamp"]
    staged = pipelines.options["files_things_staged"]
    assert "delta.enableChangeDataFeed" not in staged["table_properties"]

    _c, _s, keyed = _build(tmp_path, _volume_spec(extra=SCD1))
    assert keyed.options["framework_dev.files_bronze.things"]["cluster_by"] == [
        "_ingest_timestamp",
        "thing_id",
    ]


def test_table_descriptions_are_captured_and_become_the_table_comment(tmp_path: Path) -> None:
    spec = _volume_spec().replace("_quarantine}", "_quarantine, description: Rows that failed}")
    config, _spark, pipelines = _build(tmp_path, spec + "description: Things from the files\n")
    assert config.description == "Things from the files"
    assert config.quarantine_description == "Rows that failed"
    assert (
        pipelines.options["framework_dev.files_bronze.things"]["comment"] == "Things from the files"
    )
    assert (
        pipelines.options["framework_dev.files_quarantine.things_qrntn"]["comment"]
        == "Rows that failed"
    )
    again = config_from_document(config.canonical())
    assert (again.description, again.quarantine_description) == (
        "Things from the files",
        "Rows that failed",
    )
    records = {
        row["full_name"]: json.loads(row["config_json"])
        for row in object_rows(config)
        if row["object_kind"] in {"streaming_table", "table", "materialized_view"}
    }
    assert records["framework_dev.files_bronze.things"]["description"] == "Things from the files"
    assert records["framework_dev.files_quarantine.things_qrntn"]["load_type"] == "insert_only"
    assert (
        config.content_hash()
        != _config(tmp_path, FILE_SOURCE, "g.yaml", _volume_spec()).content_hash()
    )
    # Without one, the generated text is the comment, as before.
    _c, _s, plain = _build(tmp_path, _volume_spec())
    assert (
        "from source system files" in plain.options["framework_dev.files_bronze.things"]["comment"]
    )
    with pytest.raises(ConfigValidationError, match="description is longer than 1024"):
        _config(
            tmp_path, FILE_SOURCE, "h.yaml", _volume_spec() + "description: " + "x" * 1025 + "\n"
        )
    with pytest.raises(ConfigValidationError, match="description must be text"):
        _config(tmp_path, FILE_SOURCE, "i.yaml", _volume_spec() + "description: [a]\n")


def test_a_missing_description_is_a_warning_and_never_an_error(tmp_path: Path) -> None:
    from lakeflow_framework.sources import SourceSystem, SourceTable, description_warnings

    def system(config) -> SourceSystem:
        table = SourceTable(config, Path("sources/files/things.yaml"), "daily", "reporting")
        return SourceSystem("files", "finance", "o@x.com", "sftp_files", {"daily": {}}, [table])

    bare = _config(tmp_path, FILE_SOURCE, "a.yaml", _volume_spec())
    assert description_warnings([system(bare)]) == [
        "files/things.yaml: the table has no description",
        "files/things.yaml: no description for column(s) thing_id",
    ]
    spec = _volume_spec().replace("type: string}", "type: string, description: The id}")
    described = _config(tmp_path, FILE_SOURCE, "b.yaml", spec + "description: Things\n")
    assert description_warnings([system(described)]) == []


def test_audit_columns_are_registered_with_their_descriptions(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    rows = {
        (row["parent_full_name"].rsplit(".", 1)[1], row["full_name"].rsplit(".", 1)[1]): json.loads(
            row["config_json"]
        )
        for row in object_rows(config)
        if row["object_kind"] == "column"
    }
    for table in ("things", "things_qrntn"):
        for name in (
            "_src_file_path",
            "_src_file_name",
            "_src_row_hash",
            "_batch_id",
            "_ingest_timestamp",
        ):
            assert rows[(table, name)]["description"], (table, name)
    assert rows[("things", "_ingest_timestamp")]["data_type"] == "timestamp"
    assert ("things", "_is_deleted") not in rows
    assert rows[("things", "thing_id")]["description"] == ""  # a declared column, none written yet


def test_database_spec_reads_the_foreign_catalog(tmp_path: Path) -> None:
    config = _config(tmp_path, DATABASE_SOURCE, "db_things.yaml", _database_spec())
    assert config.load_method == "database_table"
    assert config.source_reference() == "db_federated.dbo.things"
    assert config.is_batch and not config.uses_auto_cdc
    graph = assemble(config)
    assert [dataset.relation for dataset in graph.datasets] == [
        "batch",
        "temporary_view",
        "batch",
        "batch",
    ]


def test_merge_mode_stages_once_then_uses_snapshot_cdc_on_the_keys(tmp_path: Path) -> None:
    extra = "  primary_keys: [thing_id]\n"
    spec = _database_spec(mode="merge").replace("  columns:", extra + "  columns:")
    config = _config(tmp_path, DATABASE_SOURCE, "db_things.yaml", spec)
    assert config.uses_auto_cdc
    spark = _Spark()
    pipelines = _Pipelines(spark)
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
    assert pipelines.kinds["db_things_staged"] == "materialized_view"
    assert pipelines.options["db_things_staged"]["private"] is True
    assert pipelines.options["framework_dev.db_bronze.things"]["cluster_by"] == [
        "_ingest_timestamp",
        "thing_id",
    ]
    foreign = [read for read in spark.reads if read[2] == "db_federated.dbo.things"]
    assert foreign == [("db_things_staged", "table", "db_federated.dbo.things")]
    assert ("framework_dev.db_quarantine.things_qrntn", "table", "db_things_staged") in spark.reads


def test_a_snapshot_with_keys_keeps_a_removed_row_closed_instead_of_deleting_it(
    tmp_path: Path,
) -> None:
    spec = _database_spec(mode="snapshot").replace(
        "  columns:", "  primary_keys: [thing_id]\n  columns:"
    )
    config = _config(tmp_path, DATABASE_SOURCE, "db_things.yaml", spec)
    assert config.load_type == "full_refresh" and config.soft_deletes_rows and config.scd_type == 2
    spark = _Spark()
    pipelines = _Pipelines(spark)
    build_pipeline("db_things", spark=spark, pipelines=pipelines, config=config)
    # Type 2 closes a key the source dropped (proven on a real pipeline); type 1 would delete it.
    (flow,) = pipelines.snapshot_cdc
    assert flow["stored_as_scd_type"] == 2 and flow["keys"] == ["thing_id"]
    assert flow["track_history_except_column_list"] == [
        "_src_file_path",
        "_src_file_name",
        "_batch_id",
        "_ingest_timestamp",
    ]
    assert pipelines.kinds["framework_dev.db_bronze.things"] == "streaming_table"
    # Without keys there is nothing to follow a row by, so it is still replaced each run.
    keyless = _config(tmp_path, DATABASE_SOURCE, "db_other.yaml", _database_spec(mode="snapshot"))
    assert keyless.load_type == "full_refresh" and not keyless.soft_deletes_rows
    assert keyless.scd_type == 1 and not keyless.uses_auto_cdc
    with pytest.raises(ConfigValidationError, match="follows source.mode"):
        _config(
            tmp_path,
            DATABASE_SOURCE,
            "db_bad.yaml",
            _database_spec(mode="snapshot").replace("  columns:", "  load_type: scd2\n  columns:"),
        )


def test_snapshot_mode_replaces_bronze_with_a_materialized_view(tmp_path: Path) -> None:
    config = _config(tmp_path, DATABASE_SOURCE, "db_things.yaml", _database_spec())
    spark = _Spark()
    pipelines = _Pipelines(spark)
    build_pipeline("db_things", spark=spark, pipelines=pipelines, config=config)
    assert pipelines.kinds["framework_dev.db_bronze.things"] == "materialized_view"
    assert pipelines.kinds["framework_dev.db_quarantine.things_qrntn"] == "materialized_view"
    assert spark.reads == [
        ("db_things_staged", "table", "db_federated.dbo.things"),
        ("db_things_valid", "table", "db_things_staged"),
        ("framework_dev.db_bronze.things", "table", "db_things_valid"),
        ("framework_dev.db_quarantine.things_qrntn", "table", "db_things_staged"),
    ]


def test_volume_files_read_the_volume_once_into_the_staging_table(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    spark = _Spark()
    pipelines = _Pipelines(spark)
    build_pipeline("files_things", spark=spark, pipelines=pipelines, config=config)
    bronze, quarantine = (
        "framework_dev.files_bronze.things",
        "framework_dev.files_quarantine.things_qrntn",
    )
    staged = "files_things_staged"
    assert pipelines.kinds == {
        staged: "table",
        "files_things_valid": "view",
        "files_things_files": "materialized_view",
        bronze: "table",
        quarantine: "table",
    }
    assert pipelines.options[staged]["private"] is True
    assert spark.reads == [
        (staged, "files", config.path),
        ("files_things_valid", "table", staged),
        ("files_things_files", "sql", None),
        (bronze, "table", "files_things_valid"),
        (quarantine, "table", staged),
    ]
    assert pipelines.expectations == ["cast_thing_id", "schema_conformance"]
    assert spark.readStream.format_name == "cloudFiles"
    assert spark.readStream.options["cloudFiles.format"] == "csv"
    assert spark.readStream.options["cloudFiles.schemaHints"] == "`thing_id` string"
    assert spark.readStream.options["cloudFiles.useNotifications"] == "false"
    assert "cloudFiles.useManagedFileEvents" not in spark.readStream.options
    assert pipelines.options[bronze]["cluster_by"] == ["_ingest_timestamp"]
    assert pipelines.options[bronze]["table_properties"]["quality"] == "bronze"
    assert "files_things" in pipelines.options[bronze]["comment"]


def test_file_events_and_cluster_keys_come_from_the_spec(tmp_path: Path) -> None:
    source = "  raw_path: things/\n  format: csv\n  file_events: true\n"
    spec = _volume_spec(source=source).replace(
        "table: things}", "table: things, cluster_by: [thing_id]}"
    )
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", spec)
    spark = _Spark()
    pipelines = _Pipelines(spark)
    build_pipeline("files_things", spark=spark, pipelines=pipelines, config=config)
    assert spark.readStream.options["cloudFiles.useManagedFileEvents"] == "true"
    assert "cloudFiles.useNotifications" not in spark.readStream.options
    assert pipelines.options["framework_dev.files_bronze.things"]["cluster_by"] == [
        "_ingest_timestamp",
        "thing_id",
    ]
    assert config_from_document(config.canonical()).cluster_by == ["thing_id"]

    too_many = spec.replace("cluster_by: [thing_id]", "cluster_by: [a, b, c, d, e]")
    with pytest.raises(ConfigValidationError, match="at most 4"):
        _config(tmp_path, FILE_SOURCE, "files_things.yaml", too_many)


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
        flows = {row["full_name"] for row in rows if row["object_kind"] == "flow"}
        expected = {f"{config.pipeline_id}.bronze", f"{config.pipeline_id}.quarantine"}
        if config.load_method == "volume_files":
            expected.add(f"{config.pipeline_id}.raw")
        assert flows == expected
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
    # No raw_path means the files are in the volume itself.
    root = _write(folder, FILE_SOURCE, "files_things.yaml", _volume_spec(source="  format: csv\n"))
    (config,) = validate_configs([root], catalog="framework_dev")
    assert config.path == "/Volumes/framework_dev/files_raw/landing/"
    # A full path would be joined onto the volume path and point nowhere.
    full = _volume_spec(
        source="  raw_path: /Volumes/framework_dev/files_raw/landing\n  format: csv\n"
    )
    path = _write(folder, FILE_SOURCE, "files_things.yaml", full)
    with pytest.raises(ConfigValidationError) as caught:
        validate_configs([path], catalog="framework_dev")
    assert "not a full path" in " ".join(caught.value.errors)
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
