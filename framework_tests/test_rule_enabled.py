"""A rule with enabled: false stays in the spec and the registry, shown as inactive, and never runs."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakeflow_framework import ConfigValidationError, build_pipeline
from lakeflow_framework.config import config_from_document
from lakeflow_framework.metadata import object_rows
from lakeflow_framework.quality import compile_rules, failed_rules_sql
from lakeflow_framework.rule_check import check_rules
from test_sources import FILE_SOURCE, _config, _Pipelines, _Spark, _volume_spec


def rules_block(enabled: str = "") -> str:
    return (
        "  rules:\n"
        '    - { name: id_present, expression: "thing_id IS NOT NULL" }\n'
        f"    - {{ name: id_format, expression: \"thing_id rlike '^[A-Z]+$'\"{enabled} }}\n"
    )


def config_with(tmp_path: Path, enabled: str = ""):
    return _config(
        tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec(extra=rules_block(enabled))
    )


def registered_rules(config) -> list[dict]:
    (pipeline,) = [r for r in object_rows(config) if r["object_kind"] == "pipeline"]
    return json.loads(pipeline["config_json"])["rules"]


def test_a_rule_is_enabled_unless_the_spec_says_otherwise(tmp_path: Path) -> None:
    config = config_with(tmp_path)
    assert [rule.enabled for rule in config.rules] == [True, True]
    assert "enabled" not in json.dumps(config.canonical())  # an unused option changes no hash


def test_enabled_false_switches_one_rule_off(tmp_path: Path) -> None:
    config = config_with(tmp_path, ", enabled: false")
    assert {rule.name: rule.enabled for rule in config.rules} == {
        "id_present": True,
        "id_format": False,
    }
    assert [rule.name for rule in config.active_rules] == ["id_present"]


def test_a_value_that_is_not_true_or_false_is_refused(tmp_path: Path) -> None:
    with pytest.raises(
        ConfigValidationError, match="rule 'id_format' enabled must be true or false"
    ):
        config_with(tmp_path, ", enabled: no thanks")


def test_a_disabled_rule_is_never_evaluated(tmp_path: Path) -> None:
    config = config_with(tmp_path, ", enabled: false")
    declared = [rule.name for rule in compile_rules(config) if rule.origin == "declared"]
    assert declared == ["id_present"]
    assert "id_format" not in failed_rules_sql(compile_rules(config))
    spark = _Spark()
    pipelines = _Pipelines(spark)
    build_pipeline("files_things", spark=spark, pipelines=pipelines, config=config)
    assert "id_format" not in pipelines.expectations and "id_present" in pipelines.expectations


def test_a_disabled_rule_is_registered_as_inactive_and_the_others_carry_no_flag(
    tmp_path: Path,
) -> None:
    rules = registered_rules(config_with(tmp_path, ", enabled: false"))
    by_name = {rule["name"]: rule for rule in rules}
    assert by_name["id_format"]["enabled"] is False
    assert by_name["id_format"]["expression"] == "thing_id rlike '^[A-Z]+$'"
    assert by_name["id_format"]["origin"] == "declared"
    assert "enabled" not in by_name["id_present"]
    assert all("enabled" not in rule for rule in rules if rule["origin"] != "declared")
    # the declared rules come first, disabled or not, then the framework's own
    origins = [rule["origin"] for rule in rules]
    assert origins == sorted(origins, key=lambda origin: origin != "declared")


def test_a_spec_that_never_uses_the_option_registers_exactly_what_it_did_before(
    tmp_path: Path,
) -> None:
    assert all("enabled" not in rule for rule in registered_rules(config_with(tmp_path)))


def test_switching_a_rule_off_or_on_makes_a_new_version(tmp_path: Path) -> None:
    def hash_of(enabled: str) -> str:
        config = _config(
            tmp_path,
            FILE_SOURCE,
            f"t{abs(hash(enabled))}.yaml",
            _volume_spec(extra=rules_block(enabled)).replace("files_things", "files_things"),
        )
        (pipeline,) = [r for r in object_rows(config) if r["object_kind"] == "pipeline"]
        return pipeline["content_hash"]

    assert hash_of("") != hash_of(", enabled: false")
    assert hash_of(", enabled: true") == hash_of("")


def test_the_option_survives_the_registry_round_trip(tmp_path: Path) -> None:
    config = config_with(tmp_path, ", enabled: false")
    again = config_from_document(config.canonical())
    assert [(r.name, r.enabled) for r in again.rules] == [
        ("id_present", True),
        ("id_format", False),
    ]
    assert again.content_hash() == config.content_hash()


def test_the_rule_check_skips_a_disabled_rule(tmp_path: Path) -> None:
    config = config_with(tmp_path, ", enabled: false")
    sent: list[str] = []

    def run(sql: str) -> list[dict]:
        sent.append(sql)
        return [{}]

    results = check_rules(run, config)
    assert [result.rule for result in results] == ["id_present"]
    assert not any("rlike" in sql for sql in sent)
