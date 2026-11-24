"""A spec with no columns: infer_schema picks the types, and the columns are registered once."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakeflow_framework import ConfigValidationError
from lakeflow_framework import infer_columns as module
from lakeflow_framework.config import config_from_document
from lakeflow_framework.infer_columns import infer_columns, keep_inferred_columns
from lakeflow_framework.load import get_strategy
from lakeflow_framework.metadata import INFERRED_SOURCE, object_rows, observed_column_rows
from test_sources import FILE_SOURCE, _config, _volume_spec

BRONZE = "framework_dev.files_bronze.things"
QUARANTINE = "framework_dev.files_quarantine.things_qrntn"
NO_COLUMNS = "  columns:\n    - {name: thing_id, type: string}\n"


def bare_spec(source: str = "  raw_path: things/\n  format: csv\n") -> str:
    """The volume spec with its one declared column taken out."""

    return _volume_spec(source=source).replace(NO_COLUMNS, "")


def bare_config(tmp_path: Path, source: str = "  raw_path: things/\n  format: csv\n"):
    return _config(tmp_path, FILE_SOURCE, "files_things.yaml", bare_spec(source))


def infer_option(config) -> str:
    return get_strategy("volume_files").options(config)["cloudFiles.inferColumnTypes"]


# --- the option -------------------------------------------------------------------------------


def test_without_the_option_a_spec_with_no_columns_still_infers_real_types(tmp_path: Path) -> None:
    config = bare_config(tmp_path)
    assert config.infer_schema is None and infer_option(config) == "true"
    assert "infer_schema" not in config.canonical()["source_to_raw"]["source"]


def test_typed_infers_real_types_and_strings_keeps_every_column_a_string(tmp_path: Path) -> None:
    typed = bare_config(tmp_path, "  raw_path: things/\n  format: csv\n  infer_schema: typed\n")
    strings = bare_config(tmp_path, "  raw_path: things/\n  format: csv\n  infer_schema: strings\n")
    assert infer_option(typed) == "true"
    assert infer_option(strings) == "false"
    assert "cloudFiles.schemaHints" not in get_strategy("volume_files").options(strings)


def test_the_option_survives_the_registry_round_trip(tmp_path: Path) -> None:
    config = bare_config(tmp_path, "  raw_path: things/\n  format: csv\n  infer_schema: strings\n")
    again = config_from_document(config.canonical())
    assert again.infer_schema == "strings" and again.content_hash() == config.content_hash()


def test_an_unknown_value_is_refused_with_the_choices(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="infer_schema must be one of typed, strings"):
        bare_config(tmp_path, "  raw_path: things/\n  format: csv\n  infer_schema: guess\n")


def test_a_format_that_carries_its_own_types_refuses_it(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="parquet carries its own column types"):
        bare_config(tmp_path, "  raw_path: things/\n  format: parquet\n  infer_schema: strings\n")


def test_declared_columns_and_infer_schema_do_not_mix(tmp_path: Path) -> None:
    source = "  raw_path: things/\n  format: csv\n  infer_schema: strings\n"
    with pytest.raises(ConfigValidationError, match="infer_schema is for a spec that declares no"):
        _config(tmp_path, FILE_SOURCE, "a.yaml", _volume_spec(source=source))


# --- registering the columns once --------------------------------------------------------------


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def collect(self) -> list[dict]:
        return self.rows


class _Spark:
    """Answers the registry and information_schema reads of infer_columns."""

    def __init__(self, registered=(), observed=None, inferred=()) -> None:
        self.registered = list(registered)
        self.observed = observed or {}
        self.inferred = list(inferred)
        self.queries: list[str] = []

    def sql(self, statement: str) -> _Result:
        self.queries.append(statement)
        if "information_schema.columns" in statement:
            table = re.search(r"table_name = '([^']+)'", statement).group(1)
            return _Result(
                [{"column_name": n, "full_data_type": t} for n, t in self.observed.get(table, [])]
            )
        if "source = 'inferred'" in statement:
            return _Result(self.inferred)
        return _Result(self.registered)


AUDIT = [("_src_file_path", "STRING"), ("_batch_id", "STRING")]
LOADED = {
    "things": [("id", "STRING"), ("amount", "DOUBLE"), *AUDIT],
    "things_qrntn": [
        ("id", "STRING"),
        ("amount", "DOUBLE"),
        *AUDIT,
        ("failed_rules", "ARRAY<STRING>"),
    ],
}


def registered(table: str, names: list[str], source: str = "framework") -> list[dict]:
    return [
        {"parent_full_name": table, "full_name": f"{table}.{name}", "source": source}
        for name in names
    ]


@pytest.fixture
def written(monkeypatch, tmp_path: Path):
    """Run infer_columns against a spec with no columns, capturing what it would register."""

    config = bare_config(tmp_path)
    captured: list[dict] = []
    monkeypatch.setattr("lakeflow_framework.backfill.load_config", lambda *_args: config)
    monkeypatch.setattr(
        module, "append_definitions", lambda _s, rows, _seen, _c: captured.extend(rows) or len(rows)
    )
    return config, captured


def test_columns_are_registered_from_bronze_when_the_registry_has_only_metadata(written) -> None:
    config, captured = written
    only_metadata = registered(BRONZE, ["_src_file_path", "_batch_id"])
    spark = _Spark(only_metadata, LOADED)
    assert infer_columns(spark, "framework_dev", ["files_things"]) == {"files_things": "registered"}
    bronze = {row["full_name"]: row for row in captured if row["parent_full_name"] == BRONZE}
    assert set(bronze) == {
        f"{BRONZE}.{name}" for name in ("id", "amount", "_src_file_path", "_batch_id")
    }
    assert {row["source"] for row in captured} == {INFERRED_SOURCE}
    amount = __import__("json").loads(bronze[f"{BRONZE}.amount"]["config_json"])
    assert (amount["data_type"], amount["ordinal"], amount["description"]) == ("double", 2, "")
    batch = __import__("json").loads(bronze[f"{BRONZE}._batch_id"]["config_json"])
    assert batch["description"].startswith("Identifier of the pipeline run")  # the framework's own
    assert any(row["parent_full_name"] == QUARANTINE for row in captured)


def test_a_table_whose_columns_are_registered_does_one_select_and_stops(written) -> None:
    _config_, captured = written
    held = registered(BRONZE, ["id", "_batch_id"], source=INFERRED_SOURCE)
    spark = _Spark(held, LOADED)
    assert infer_columns(spark, "framework_dev", ["files_things"]) == {
        "files_things": "already registered"
    }
    assert captured == [] and len(spark.queries) == 1  # never looked at bronze


def test_a_table_not_loaded_yet_is_left_for_the_next_run(written) -> None:
    _config_, captured = written
    spark = _Spark(registered(BRONZE, ["_batch_id"]), observed={})
    assert infer_columns(spark, "framework_dev", ["files_things"]) == {
        "files_things": "not loaded yet"
    }
    assert captured == []


def test_a_table_with_only_the_frameworks_columns_has_nothing_to_register(written) -> None:
    _config_, captured = written
    spark = _Spark(registered(BRONZE, ["_batch_id"]), {"things": AUDIT})
    assert infer_columns(spark, "framework_dev", ["files_things"])["files_things"] == (
        "nothing to register"
    )
    assert captured == []


def test_refresh_registers_an_inferred_table_again_but_never_a_declared_one(written) -> None:
    _config_, captured = written
    inferred = registered(BRONZE, ["id", "_batch_id"], source=INFERRED_SOURCE)
    spark = _Spark(inferred, LOADED)
    result = infer_columns(spark, "framework_dev", ["files_things"], refresh=True)
    assert result == {"files_things": "registered"} and captured
    captured.clear()
    declared = registered(BRONZE, ["id", "_batch_id"], source="framework")
    spark = _Spark(declared, LOADED)
    result = infer_columns(spark, "framework_dev", ["files_things"], refresh=True)
    assert result == {"files_things": "declared in the spec"} and captured == []


# --- keeping them across a deploy ---------------------------------------------------------------


def _inferred_rows(config) -> list[dict]:
    rows = observed_column_rows(config, {"bronze": LOADED["things"]})
    for row in rows:
        row["recorded_at"] = datetime(2026, 11, 20, tzinfo=UTC)  # Spark hands back a timestamp
    return rows


def test_a_deploy_keeps_the_inferred_columns_of_a_table_that_declares_none(tmp_path: Path) -> None:
    config = bare_config(tmp_path)
    deployed = object_rows(config)
    spark = _Spark(inferred=_inferred_rows(config))
    kept = keep_inferred_columns(spark, deployed, "framework_dev")
    columns = [
        row for row in kept if row["object_kind"] == "column" and row["parent_full_name"] == BRONZE
    ]
    assert {row["source"] for row in columns} == {INFERRED_SOURCE}
    assert f"{BRONZE}.amount" in {row["full_name"] for row in columns}
    # the spec's own rows for that table are gone, so nothing flaps; other rows are untouched
    assert len(columns) == len(LOADED["things"])
    assert [r for r in kept if r["object_kind"] != "column"] == [
        r for r in deployed if r["object_kind"] != "column"
    ]
    assert all(isinstance(row["recorded_at"], str) for row in columns)


def test_a_spec_that_now_declares_columns_keeps_only_its_own_rows(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    deployed = object_rows(config)
    spark = _Spark(inferred=_inferred_rows(config))
    assert keep_inferred_columns(spark, deployed, "framework_dev") == deployed
    assert not any("source = 'inferred'" in query for query in spark.queries)


def test_nothing_inferred_leaves_the_deploy_alone(tmp_path: Path) -> None:
    config = bare_config(tmp_path)
    deployed = object_rows(config)
    assert keep_inferred_columns(_Spark(), deployed, "framework_dev") == deployed
