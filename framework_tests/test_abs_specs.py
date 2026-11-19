"""The quality rules in the abs specs: every column is covered, and the patterns mean what they say."""

from __future__ import annotations

import re
from pathlib import Path

from lakeflow_framework import validate_configs
from lakeflow_framework.quality import compile_rules, failed_rules_sql

SPECS = Path(__file__).resolve().parents[1] / "framework_ingestion/sources/abs/fdh"


def _gl():
    (config,) = validate_configs([SPECS / "abs_gl.yaml"], catalog="framework_dev")
    return config


def _pattern(config, rule: str) -> re.Pattern:
    expression = next(r.expression for r in config.rules if r.name == rule)
    return re.compile(re.search(r"rlike '(.+)'$", expression).group(1))


def test_abs_gl_has_two_rules_for_every_column() -> None:
    config = _gl()
    names = [rule.name for rule in config.rules]
    assert len(names) == len(set(names)) == 26 == 2 * len(config.columns)
    for column in config.columns:
        mentioned = [r.name for r in config.rules if re.search(rf"\b{column.name}\b", r.expression)]
        assert len(mentioned) >= 2, (column.name, mentioned)
    # Each rule is checked in the pipeline's own failed-rules expression.
    sql = failed_rules_sql(compile_rules(config))
    assert all(f"'rule', '{name}'" in sql for name in names)


def test_a_missing_end_value_is_allowed_but_a_missing_start_value_is_not() -> None:
    by_name = {rule.name: rule.expression for rule in _gl().rules}
    for column in ("end_station_name", "end_station_id", "end_lat", "end_lng"):
        assert all(
            expression.startswith(f"{column} IS NULL OR")
            for name, expression in by_name.items()
            if name.startswith(column)
        )
    # A null rule result counts as a failure, so the start columns must not be null.
    assert "IS NOT NULL" in by_name["start_station_name_present"]
    assert "IS NOT NULL" in by_name["ride_id_present"]


def test_the_coordinate_and_id_patterns_accept_and_reject_the_right_values() -> None:
    config = _gl()
    lat = _pattern(config, "start_lat_in_range")
    for good in ("40.74542", "-73.9", "0", "89.99", "90", "90.0", "-90", "7"):
        assert lat.fullmatch(good), good
    for bad in ("90.5", "91", "100", "abc", "", "4.", ".5", "--1"):
        assert not lat.fullmatch(bad), bad
    lng = _pattern(config, "start_lng_in_range")
    for good in ("-73.99", "180", "180.0", "179.9", "5", "-0.5", "99.1"):
        assert lng.fullmatch(good), good
    for bad in ("181", "-180.5", "200", "x", "", "1.", "1,5"):
        assert not lng.fullmatch(bad), bad
    ride = _pattern(config, "ride_id_format")
    assert ride.fullmatch("811C6F5F6C3C16E7") and ride.fullmatch("45d49aad81c4c76a")
    assert not ride.fullmatch("811C6F5F6C3C16E") and not ride.fullmatch("ZZZC6F5F6C3C16E7")
    number = _pattern(config, "start_lat_is_number")
    assert number.fullmatch("-40.7") and not number.fullmatch("40.") and not number.fullmatch("N/A")
