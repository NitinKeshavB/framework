"""A quarantine table exists only when the spec has rules or a quarantine block, and its rows have
bronze's shape (types included) plus failed_rules."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from lakeflow_framework import build_pipeline
from lakeflow_framework.config import ConfigValidationError, config_from_document
from lakeflow_framework.metadata import object_rows
from lakeflow_framework.pipeline import (
    assemble,
    files_sql,
    has_dropping,
    has_file_counts,
    rescued_cast_exprs,
)
from lakeflow_framework.quality import compile_rules
from test_pipeline_metrics import dataflow, recorder, run, update_state
from test_sources import FILE_SOURCE, _config, _Pipelines, _Spark, _volume_spec

NO_BLOCK = "quarantine: {schema: files_quarantine}\n"
RULE = '  rules:\n    - { name: id_present, expression: "thing_id IS NOT NULL" }\n'


def spec(*, rules: bool = False, block: bool = False, extra: str = "") -> str:
    text = _volume_spec(extra=extra + (RULE if rules else ""))
    return text if block else text.replace(NO_BLOCK, "")


def registered(tmp_path: Path, text: str) -> tuple[object, list[str]]:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", text)
    pipelines = _Pipelines(_Spark())
    build_pipeline("files_things", spark=pipelines.spark, pipelines=pipelines, config=config)
    return config, pipelines.names


def test_nothing_defined_means_no_quarantine_at_all(tmp_path: Path) -> None:
    config, names = registered(tmp_path, spec())
    assert not config.has_quarantine
    assert "quarantine" not in config.layers
    assert not any("qrntn" in name or "quarantine" in name for name in names)
    assert "quarantine" not in {dataset.kind for dataset in assemble(config).datasets}
    # Nothing is dropped: a value that does not fit, or a new column, is rescued and the row loads.
    builtin = {r.name: r.action for r in compile_rules(config) if r.origin != "declared"}
    assert builtin == {
        "cast_thing_id": "warn",
        "schema_conformance": "warn",
        "new_columns": "warn",
    }
    # Registry: no quarantine table, flow, or link; the dataflow records none.
    rows = object_rows(config)
    assert not any("quarantine" in row["full_name"] for row in rows)
    (record,) = [row for row in rows if row["object_kind"] == "dataflow"]
    assert (
        '"quarantine": null' in record["config_json"] or "quarantine" not in record["config_json"]
    )


def test_a_new_column_is_rescued_and_loaded_when_there_is_no_quarantine(tmp_path: Path) -> None:
    config, _ = registered(tmp_path, spec())
    actions = {r.name: r.action for r in compile_rules(config)}
    assert actions["schema_conformance"] == "warn"


def test_the_user_asking_for_quarantine_or_drop_decides_what_happens_to_a_rescued_row(
    tmp_path: Path,
) -> None:
    quarantined, _ = registered(tmp_path, spec(block=True))
    assert {r.name: r.action for r in compile_rules(quarantined)}["schema_conformance"] == (
        "quarantine"
    )
    dropping = dataclasses.replace(quarantined, on_fail="drop")
    assert {r.name: r.action for r in compile_rules(dropping)}["schema_conformance"] == "drop"


def test_only_a_row_auto_cdc_cannot_apply_is_dropped_and_counted_per_file(tmp_path: Path) -> None:
    config, _ = registered(tmp_path, spec())
    assert not has_dropping(config)  # append: a null key still loads, with a warning
    keyed = dataclasses.replace(config, load_type="scd1", primary_keys=["thing_id"])
    actions = {r.name: r.action for r in compile_rules(keyed)}
    assert actions["not_null_thing_id"] == "drop" and has_dropping(keyed)


def test_schema_evolution_chooses_what_auto_loader_does_with_a_new_column(tmp_path: Path) -> None:
    from lakeflow_framework.load import get_strategy
    from test_infer_columns import bare_config

    expected = {
        "rescue": "rescue",
        "add_new_columns": "addNewColumns",
        "fail": "failOnNewColumns",
        "none": "none",
    }
    default = bare_config(tmp_path)
    assert default.schema_evolution == "rescue"
    assert "schema_evolution" not in default.canonical()["source_to_raw"]["source"]
    for name, mode in expected.items():
        config = bare_config(
            tmp_path, f"  raw_path: things/\n  format: csv\n  schema_evolution: {name}\n"
        )
        options = get_strategy("volume_files").options(config)
        assert options["cloudFiles.schemaEvolutionMode"] == mode
        assert config_from_document(config.canonical()).schema_evolution == name


def test_schema_evolution_is_refused_for_a_value_it_does_not_know_or_a_format_with_its_own_columns(
    tmp_path: Path,
) -> None:
    from lakeflow_framework import ConfigValidationError
    from test_infer_columns import bare_config

    with pytest.raises(ConfigValidationError, match="schema_evolution must be one of"):
        bare_config(tmp_path, "  raw_path: things/\n  format: csv\n  schema_evolution: guess\n")
    with pytest.raises(ConfigValidationError, match="schema_evolution applies to"):
        bare_config(tmp_path, "  raw_path: things/\n  format: parquet\n  schema_evolution: fail\n")


def test_a_table_whose_only_added_check_warns_needs_no_file_dataset(tmp_path: Path) -> None:
    config, _ = registered(tmp_path, spec())
    bare = dataclasses.replace(config, columns=[])
    assert not has_dropping(bare) and not has_file_counts(bare)


def test_the_manifest_round_trip_does_not_invent_a_quarantine(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "a.yaml", spec())
    assert "quarantine" not in config.canonical()
    again = config_from_document(config.canonical())
    assert not again.has_quarantine and again.content_hash() == config.content_hash()


def test_only_a_quarantine_block_keeps_quarantine_as_the_default(tmp_path: Path) -> None:
    config, names = registered(tmp_path, spec(block=True))
    assert config.has_quarantine and not config.rules
    assert "framework_dev.files_quarantine.things_qrntn" in names
    builtin = {r.name: r.action for r in compile_rules(config) if r.origin != "declared"}
    assert builtin == {
        "cast_thing_id": "quarantine",
        "schema_conformance": "quarantine",
        "new_columns": "warn",
    }
    again = config_from_document(config.canonical())
    assert again.has_quarantine


def test_rules_without_a_block_get_the_default_table_beside_bronze(tmp_path: Path) -> None:
    config, names = registered(tmp_path, spec(rules=True))
    assert config.has_quarantine
    assert config.table_name("quarantine") == "framework_dev.files_bronze.things_qrntn"
    assert "framework_dev.files_bronze.things_qrntn" in names
    assert {r.name: r.action for r in compile_rules(config)}["id_present"] == "quarantine"


@pytest.mark.parametrize("action", ["quarantine", "drop", "warn", "fail"])
def test_on_fail_without_rules_is_refused_before_anything_is_generated(
    tmp_path: Path, action: str
) -> None:
    for block in (False, True):
        with pytest.raises(ConfigValidationError, match="raw_to_bronze.on_fail needs rules"):
            _config(
                tmp_path, FILE_SOURCE, "a.yaml", spec(block=block, extra=f"  on_fail: {action}\n")
            )


@pytest.mark.parametrize("action", ["quarantine", "drop", "warn", "fail"])
def test_on_fail_with_rules_creates_the_quarantine_table(tmp_path: Path, action: str) -> None:
    config = _config(
        tmp_path, FILE_SOURCE, "a.yaml", spec(rules=True, extra=f"  on_fail: {action}\n")
    )
    assert config.has_quarantine and config.on_fail == action
    assert config_from_document(config.canonical()).has_quarantine


def test_a_failed_files_bad_rows_count_as_dropped_when_there_is_no_quarantine() -> None:
    kept = files_sql("staged", ["id_present"])
    lost = files_sql("staged", ["id_present"], quarantine=False)
    assert "IN ('fail', 'quarantine')) AS rows_quarantined" in kept
    assert "IN ('drop')) AS rows_dropped" in kept
    assert "IN ('quarantine')) AS rows_quarantined" in lost
    assert "IN ('drop', 'fail')) AS rows_dropped" in lost


def test_an_unconvertible_value_is_null_in_its_column_and_kept_in_rescued_data() -> None:
    merged = rescued_cast_exprs(["id", "seq", "_rescued_data"], "seq", "TIMESTAMP")
    assert merged[1] == "TRY_CAST(`seq` AS TIMESTAMP) AS `seq`"
    assert "map_concat(" in merged[2] and merged[2].endswith("AS _rescued_data")
    assert len(merged) == 3  # same columns, same order
    added = rescued_cast_exprs(["id", "seq"], "seq", "TIMESTAMP")
    assert added[-1] == (
        "CASE WHEN `seq` IS NOT NULL AND TRY_CAST(`seq` AS TIMESTAMP) IS NULL "
        "THEN to_json(map('seq', CAST(`seq` AS STRING))) END AS _rescued_data"
    )


def test_a_table_without_quarantine_does_not_take_another_tables_error() -> None:
    mine = dataflow(quarantine="")
    other = dataflow(
        pipeline_id="abs_other",
        staged="abs_other_staged",
        target="aal.abs_bronze.other",
        quarantine="aal.abs_bronze.other_qrntn",
    )
    error = {
        "event_type": "flow_progress",
        "timestamp": "2026-11-19T10:00:05.000Z",
        "level": "ERROR",
        "message": "other table broke",
        "origin": {"update_id": "u-1", "flow_name": "aal.abs_bronze.other"},
        "details": {"flow_progress": {"status": "FAILED"}},
    }
    rows = run(
        recorder(mine, other), [update_state("RUNNING", 0), error, update_state("FAILED", 6)]
    )
    by_flow = {row["dataflow"]: row for row in rows}
    assert "other table broke" not in by_flow["abs_accounting"]["failure_reason"]
    assert "other table broke" in by_flow["abs_other"]["failure_reason"]


def test_a_file_that_loaded_rows_with_rescued_values_is_flagged_warning() -> None:
    from lakeflow_framework.pipeline_metrics import read_files_sql, warning_text

    row = {
        "_file_load": True,
        "batch_id": "b1",
        "target_table": "c.s.t",
        "started_at": None,
    }
    assert "_rescued_data IS NOT NULL" in read_files_sql(row, rescued=True)
    assert "_rescued_data" not in read_files_sql(row, rescued=False)
    assert "sum(warned) AS rows_warned" in read_files_sql(row, rescued=True)
    item = {
        "status": "WARNING",
        "rows_warned": 9,
        "failed_rules": "new_columns",
        "rescued_keys": "sample_column_1, sample_column_2",
    }
    said = warning_text(item)
    assert said.startswith(
        "9 row(s) loaded with new column(s) kept in _rescued_data: sample_column_1, sample_column_2."
    )
    # no type is invented for a rescued column: the user is told how to give it one
    assert "infer_schema: strings" in said and "declare them under columns:" in said
    assert "loaded with a warning" not in said
    plain = {"status": "WARNING", "rows_warned": 2, "failed_rules": "schema_conformance"}
    assert warning_text(plain).endswith("(values kept in _rescued_data)")
    assert warning_text({"status": "PROCESSED", "rows_warned": 0}) is None
    assert "rows_warned" in files_sql("c.s.staged", [])


# --- a column added to the table: guidance, the hash, and the warning with its type --------------


class _Added:
    """A table with these (name, type) columns, and a registry that knows these columns."""

    def __init__(self, fields: list[tuple[str, str]], registered: list[str]) -> None:
        self.fields = [
            type(
                "Field",
                (),
                {
                    "name": name,
                    "dataType": type("Type", (), {"simpleString": lambda _s, k=kind: k})(),
                },
            )()
            for name, kind in fields
        ]
        self.registered = registered

    def table(self, _name: str):
        fields = self.fields
        return type("Table", (), {"schema": type("Schema", (), {"fields": fields})()})()

    def sql(self, _query: str):
        rows = [{"full_name": f"c.s.t.{name}"} for name in self.registered]
        return type("Rows", (), {"collect": lambda _s: rows})()


def test_add_new_columns_comes_with_advice_on_how_to_type_the_new_column(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from lakeflow_framework.sources import schema_warnings
    from test_infer_columns import bare_config

    def table(source: str):
        config = bare_config(tmp_path, source)
        return SimpleNamespace(path=Path("t.yaml"), config=config)

    typed = table("  raw_path: things/\n  format: csv\n  schema_evolution: add_new_columns\n")
    strings = table(
        "  raw_path: things/\n  format: csv\n  schema_evolution: add_new_columns\n"
        "  infer_schema: strings\n"
    )
    rescued = table("  raw_path: things/\n  format: csv\n")
    systems = [SimpleNamespace(name="src", tables=[typed, strings, rescued])]
    (advice,) = schema_warnings(systems)
    assert advice.startswith("src/t.yaml: schema_evolution add_new_columns with inferred types")
    assert "infer_schema: strings" in advice and "declare the column under columns:" in advice


def test_a_declared_table_that_adds_columns_is_told_to_declare_them_to_give_them_a_type(
    tmp_path: Path,
) -> None:
    from dataclasses import replace
    from types import SimpleNamespace

    from lakeflow_framework.sources import schema_warnings

    config = replace(
        _config(tmp_path, FILE_SOURCE, "a.yaml", spec()), schema_evolution="add_new_columns"
    )
    systems = [
        SimpleNamespace(name="src", tables=[SimpleNamespace(path=Path("a.yaml"), config=config)])
    ]
    (advice,) = schema_warnings(systems)
    assert "adds a new column as text" in advice and "to give it a type" in advice


def test_the_row_hash_covers_every_column_the_source_gave_so_a_new_column_is_part_of_it() -> None:
    import inspect

    from lakeflow_framework import backfill
    from lakeflow_framework.load import ROW_HASH_EXPR

    # struct(*) is every column of the frame at that point: a column added later is in it, and so is
    # _rescued_data, which holds the values of one that was rescued
    assert ROW_HASH_EXPR == "sha2(to_json(struct(*)), 256) AS _src_row_hash"
    source = inspect.getsource(backfill._load)
    # a reload folds the columns the table lacks into _rescued_data before the hash is taken
    assert source.index("fold_into_rescued(") < source.index("file_columns(")


def test_a_column_added_to_the_table_is_named_with_its_type_on_the_runs_files() -> None:
    from lakeflow_framework.pipeline_metrics import apply_added_columns, read_added_columns

    spark = _Added(
        [
            ("ride_id", "string"),
            ("new_text", "string"),
            ("new_num", "double"),
            ("_rescued_data", "string"),
            ("_batch_id", "string"),
        ],
        registered=["ride_id"],
    )
    row = {"_adds_columns": True, "target_table": "c.s.t"}
    added = read_added_columns(spark, "c", row)
    assert added == [("new_text", "string"), ("new_num", "double")]
    row["_files"] = [
        {"status": "PROCESSED", "error_message": None},
        {"status": "WARNING", "error_message": "9 row(s) loaded with a warning"},
        {"status": "FAILED", "error_message": "blocked by fail rule(s): x"},
    ]
    apply_added_columns(row, added)
    first, second, blocked = row["_files"]
    assert first["status"] == "WARNING"
    assert (
        first["error_message"]
        == "new column(s) added to the table: new_text string, new_num double"
    )
    assert second["error_message"].startswith("9 row(s) loaded with a warning; new column(s) added")
    assert blocked["status"] == "FAILED" and blocked["error_message"].startswith("blocked by")


def test_nothing_is_said_on_a_first_load_or_for_a_table_that_does_not_add_columns() -> None:
    from lakeflow_framework.pipeline_metrics import apply_added_columns, read_added_columns

    fields = [("ride_id", "string"), ("col2", "string")]
    first_load = _Added(fields, registered=[])  # the registry holds no column of its own yet
    assert (
        read_added_columns(first_load, "c", {"_adds_columns": True, "target_table": "c.s.t"}) == []
    )
    rescuing = _Added(fields, registered=["ride_id"])
    assert read_added_columns(rescuing, "c", {"target_table": "c.s.t"}) == []  # not add_new_columns
    row = {"_files": [{"status": "PROCESSED", "error_message": None}]}
    assert apply_added_columns(row, [])["_files"][0]["status"] == "PROCESSED"


# --- the SQL the pipeline sends must at least be well formed -------------------------------------


def _balanced(sql: str) -> bool:
    """Brackets close in order, outside string literals. A parse error here fails a whole update."""

    stack: list[str] = []
    in_text = False
    for char in sql:
        if char == "'":
            in_text = not in_text
        elif not in_text and char in "([":
            stack.append(char)
        elif not in_text and char in ")]":
            if not stack or {"(": ")", "[": "]"}[stack.pop()] != char:
                return False
    return not stack and not in_text


def test_the_per_file_sql_is_well_formed_with_and_without_the_rescued_keys() -> None:
    from lakeflow_framework.pipeline_metrics import read_files_sql

    for rescued in (True, False):
        for blocking in ([], ["rule_a", "rule_b"]):
            for quarantine in (True, False):
                sql = files_sql("c.s.staged", blocking, quarantine=quarantine, rescued=rescued)
                assert _balanced(sql), sql
                assert ("AS rescued_keys" in sql) and sql.count("(") == sql.count(")")
    row = {"_file_load": True, "batch_id": "b", "target_table": "c.s.t", "_fail_rules": ("x",)}
    row["_quarantine_table"] = "c.s.q"
    for rescued in (True, False):
        assert _balanced(read_files_sql(row, rescued=rescued) or "")


def test_the_two_schema_checks_are_well_formed_sql() -> None:
    from lakeflow_framework.quality import schema_expressions

    for columns in ([], ["a"], ["a", "o'brien"]):
        for expression in schema_expressions(columns):
            assert _balanced(expression), expression
