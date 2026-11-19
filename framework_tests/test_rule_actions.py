"""What happens to a row that fails a rule: quarantine, drop, warn, or fail."""

from __future__ import annotations

from pathlib import Path

import pytest
from test_sources import FILE_SOURCE, _config, _Pipelines, _Spark, _volume_spec

from lakeflow_framework import ConfigValidationError, build_pipeline
from lakeflow_framework.config import config_from_document
from lakeflow_framework.metadata import object_rows
from lakeflow_framework.quality import (
    compile_rules,
    expectations,
    fail_expectations,
    quarantine_where,
    valid_where,
)

RULES = (
    "  rules:\n"
    "    - { name: id_present, expression: \"thing_id IS NOT NULL\" }\n"
    "    - { name: id_format, expression: \"thing_id rlike '^[A-Z]+$'\", on_fail: %s }\n"
)


def spec(table_level: str | None = None, rule_level: str = "warn") -> str:
    head = f"  on_bad_rows: {table_level}\n" if table_level else ""
    return _volume_spec(extra=head + RULES % rule_level)


def build(tmp_path: Path, text: str):
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", text)
    spark = _Spark()
    pipelines = _FailPipelines(spark)
    build_pipeline("files_things", spark=spark, pipelines=pipelines, config=config)
    return config, spark, pipelines


class _FailPipelines(_Pipelines):
    def __init__(self, spark=None):
        super().__init__(spark)
        self.fail_expectations: list[str] = []

    def expect_all_or_fail(self, constraints):
        self.fail_expectations.extend(constraints)
        return lambda func: func


def test_a_spec_that_says_nothing_quarantines_exactly_as_before(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "a.yaml", _volume_spec(extra=RULES % "quarantine"))
    assert config.on_bad_rows == "quarantine"
    assert {rule.action for rule in config.rules} == {"quarantine"}
    rules = compile_rules(config)
    assert valid_where(rules) == "size(_dq_failed_rules) = 0"
    assert quarantine_where(rules) == "size(_dq_failed_rules) > 0"
    assert fail_expectations(rules) == {}
    assert "on_bad_rows" not in config.canonical()["raw_to_bronze"]
    assert all("on_fail" not in r for r in config.canonical()["raw_to_bronze"]["rules"])


def test_the_table_sets_a_default_and_a_rule_can_override_it(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "a.yaml", spec(table_level="drop", rule_level="warn"))
    assert config.on_bad_rows == "drop"
    assert {rule.name: rule.action for rule in config.rules} == {
        "id_present": "drop",  # no on_fail, so the table's setting
        "id_format": "warn",  # its own
    }
    # The framework's own checks follow the table's setting.
    assert {r.name: r.action for r in compile_rules(config) if r.origin != "declared"} == {
        "cast_thing_id": "drop",
        "schema_conformance": "drop",
    }


@pytest.mark.parametrize("action", ["quarantine", "drop", "warn", "fail"])
def test_the_canonical_form_round_trips_every_action(tmp_path: Path, action: str) -> None:
    config = _config(tmp_path, FILE_SOURCE, "a.yaml", spec(table_level=action, rule_level="warn"))
    again = config_from_document(config.canonical())
    assert again.on_bad_rows == action
    assert [(r.name, r.action) for r in again.rules] == [(r.name, r.action) for r in config.rules]
    assert again.content_hash() == config.content_hash()


def test_the_action_is_part_of_the_hash_so_a_change_makes_a_new_version(tmp_path: Path) -> None:
    quarantine = _config(tmp_path, FILE_SOURCE, "a.yaml", spec(rule_level="quarantine"))
    warn = _config(tmp_path, FILE_SOURCE, "b.yaml", spec(rule_level="warn"))
    assert quarantine.content_hash() != warn.content_hash()


def test_an_unknown_action_is_refused_with_the_choices(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="on_bad_rows must be one of"):
        _config(tmp_path, FILE_SOURCE, "a.yaml", spec(table_level="ignore"))
    with pytest.raises(ConfigValidationError, match="rule 'id_format' on_fail must be one of"):
        _config(tmp_path, FILE_SOURCE, "b.yaml", spec(rule_level="skip"))


def test_a_warn_rule_loads_the_row_and_only_counts_it(tmp_path: Path) -> None:
    rules = compile_rules(_config(tmp_path, FILE_SOURCE, "a.yaml", spec(table_level="warn")))
    assert valid_where(rules) == "true"  # nothing blocks a row
    assert quarantine_where(rules) == "false"  # nothing is quarantined
    assert "id_format" in expectations(rules) and fail_expectations(rules) == {}


def test_a_drop_rule_removes_the_row_and_keeps_it_nowhere(tmp_path: Path) -> None:
    rules = compile_rules(_config(tmp_path, FILE_SOURCE, "a.yaml", spec(table_level="drop")))
    assert valid_where(rules).startswith("NOT arrays_overlap(_dq_failed_rules, array(")
    assert "'id_present'" in valid_where(rules)
    assert quarantine_where(rules) == "false"


def test_quarantine_and_drop_and_warn_can_be_mixed_in_one_table(tmp_path: Path) -> None:
    text = _volume_spec(
        extra=(
            "  rules:\n"
            "    - { name: q, expression: \"thing_id IS NOT NULL\" }\n"
            "    - { name: d, expression: \"length(thing_id) > 0\", on_fail: drop }\n"
            "    - { name: w, expression: \"thing_id = upper(thing_id)\", on_fail: warn }\n"
        )
    )
    rules = compile_rules(_config(tmp_path, FILE_SOURCE, "a.yaml", text))
    bronze, quarantine = valid_where(rules), quarantine_where(rules)
    # A row that broke only the warn rule loads. One that broke the quarantine or drop rule does not.
    assert "'q'" in bronze and "'d'" in bronze and "'w'" not in bronze
    assert quarantine.startswith("arrays_overlap") and "'q'" in quarantine and "'d'" not in quarantine


def test_a_fail_rule_on_files_blocks_the_file_and_a_check_fails_the_update(tmp_path: Path) -> None:
    config, spark, pipelines = build(tmp_path, spec(table_level=None, rule_level="fail"))
    # The only run-level failure is the check after the load. A failing flow would leave the file
    # unread, so it would be picked up again.
    assert pipelines.fail_expectations == ["no_file_blocked_by_fail_rule"]
    # The rule itself still counts, like any other, in the event log.
    assert "id_format" in pipelines.expectations and "id_present" in pipelines.expectations
    assert "files_things_blocked_check" in pipelines.names and "files_things_files" in pipelines.names
    assert config.on_bad_rows == "quarantine"


def test_a_table_set_to_fail_blocks_a_file_on_every_rule_and_every_built_in_check(
    tmp_path: Path,
) -> None:
    config, spark, pipelines = build(tmp_path, spec(table_level="fail", rule_level="fail"))
    assert {"id_present", "id_format", "cast_thing_id", "schema_conformance"} <= set(
        pipelines.expectations
    )
    assert pipelines.fail_expectations == ["no_file_blocked_by_fail_rule"]


def test_a_file_that_is_blocked_keeps_its_good_rows_out_and_its_bad_rows_in_quarantine(
    tmp_path: Path,
) -> None:
    config, spark, _pipelines = build(tmp_path, spec(table_level=None, rule_level="fail"))
    rules = compile_rules(config)
    assert valid_where(rules, files=True) == "_dq_result = 'pass'"
    assert quarantine_where(rules, files=True) == "_dq_result IN ('fail', 'quarantine')"
    assert spark.readStream.joins == [(["_src_file_path", "_batch_id"], "left_anti")]


def test_a_database_table_has_no_files_so_fail_still_fails_the_run(tmp_path: Path) -> None:
    rules = compile_rules(
        _config(tmp_path, FILE_SOURCE, "a.yaml", spec(table_level=None, rule_level="fail"))
    )
    assert set(fail_expectations(rules)) == {"id_format"}
    assert "id_format" not in expectations(rules)


def test_the_result_of_a_row_is_decided_once_and_stored_with_it(tmp_path: Path) -> None:
    from lakeflow_framework.quality import result_sql

    text = _volume_spec(
        extra=(
            "  rules:\n"
            "    - { name: f, expression: \"thing_id IS NOT NULL\", on_fail: fail }\n"
            "    - { name: q, expression: \"length(thing_id) > 0\" }\n"
            "    - { name: d, expression: \"thing_id = upper(thing_id)\", on_fail: drop }\n"
            "    - { name: w, expression: \"length(thing_id) < 9\", on_fail: warn }\n"
        )
    )
    sql = result_sql(compile_rules(_config(tmp_path, FILE_SOURCE, "a.yaml", text)))
    # fail outranks quarantine, which outranks drop; a row that broke only warn rules passes.
    assert sql.index("'fail'") < sql.index("'quarantine'") < sql.index("'drop'")
    assert "'w'" not in sql and sql.endswith("ELSE 'pass' END")


def test_the_pipeline_uses_the_routing_for_bronze_and_quarantine(tmp_path: Path) -> None:
    _config_, spark, _pipelines = build(tmp_path, spec(table_level="drop", rule_level="warn"))
    wheres = spark.readStream.wheres
    assert "_dq_result = 'pass'" in wheres  # bronze
    assert "_dq_result IN ('fail', 'quarantine')" in wheres  # quarantine


def test_the_registry_records_each_rules_action(tmp_path: Path) -> None:
    import json

    config = _config(tmp_path, FILE_SOURCE, "a.yaml", spec(table_level="drop", rule_level="warn"))
    (pipeline,) = [r for r in object_rows(config) if r["object_kind"] == "pipeline"]
    rules = {r["name"]: r["action"] for r in json.loads(pipeline["config_json"])["rules"]}
    assert rules["id_present"] == "drop" and rules["id_format"] == "warn"
    assert rules["schema_conformance"] == "drop"
