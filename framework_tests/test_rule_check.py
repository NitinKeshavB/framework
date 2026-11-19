"""check-rules: every quality rule is run on a warehouse so what only an engine knows is found early."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakeflow_framework.cli import main
from lakeflow_framework.config import config_from_document
from lakeflow_framework.quality import assert_safe_rule_expression
from lakeflow_framework.rule_check import (
    RuleCheckError,
    check_rules,
    format_results,
    sample_row,
    sample_value,
)
from test_framework import _billing


def _config(*rules: tuple[str, str]):
    document = _billing()
    document["raw_to_bronze"]["rules"] = [{"name": n, "expression": e} for n, e in rules]
    return config_from_document(document)


class Warehouse:
    """A warehouse that refuses any statement holding a marker word, and counts rows otherwise."""

    def __init__(self, refuse=(), total=1000, failing=None, missing_table=False):
        self.refuse, self.total, self.failing = refuse, total, failing or {}
        self.missing_table = missing_table
        self.statements: list[str] = []

    def __call__(self, statement: str):
        self.statements.append(statement)
        for marker in self.refuse:
            if marker in statement:
                raise RuleCheckError(f"[PARSE_SYNTAX_ERROR] near {marker}")
        if "count(*) AS total" in statement:
            if self.missing_table:
                raise RuleCheckError("[TABLE_OR_VIEW_NOT_FOUND]")
            row = {"total": str(self.total)}
            for i in range(10):
                row[f"f{i}"] = str(self.failing.get(i, 0))
            return [row]
        return [{"r0": "true"}]


def test_good_rules_are_checked_in_one_statement() -> None:
    warehouse = Warehouse()
    results = check_rules(warehouse, _config(("a", "invoice_id IS NOT NULL"), ("b", "amount >= 0")))
    assert [(r.rule, r.error) for r in results] == [("a", None), ("b", None)]
    (statement,) = warehouse.statements
    assert "coalesce((invoice_id IS NOT NULL), false) AS r0" in statement
    assert "coalesce((amount >= 0), false) AS r1" in statement


def test_a_refused_rule_is_named_and_the_others_still_pass() -> None:
    warehouse = Warehouse(refuse=("BROKEN",))
    results = check_rules(
        warehouse,
        _config(("fine", "invoice_id IS NOT NULL"), ("bad", "invoice_id rlike 'BROKEN'")),
    )
    by_name = {r.rule: r for r in results}
    assert by_name["fine"].error is None
    assert "PARSE_SYNTAX_ERROR" in by_name["bad"].error
    lines = format_results(results)
    assert lines[0].startswith("  ok     fine") and lines[1].startswith("  ERROR  bad:")


def test_the_sample_row_has_every_column_a_rule_may_name_with_a_value_of_its_type() -> None:
    sql = sample_row(_config(("a", "invoice_id IS NOT NULL")))
    assert "'x' AS `invoice_id`" in sql
    assert "CAST(1 AS decimal(18,2)) AS `amount`" in sql
    for name in ("_rescued_data", "_src_file_path", "_src_row_hash", "_batch_id"):
        assert f"AS `{name}`" in sql
    assert "true AS `_is_deleted`" in sql
    assert sample_value("date") == "DATE '2026-01-01'"
    assert sample_value("timestamp") == "TIMESTAMP '2026-01-01 00:00:00'"
    assert sample_value("boolean") == "true" and sample_value("string") == "'x'"
    assert sample_value("int") == "CAST(1 AS int)" and sample_value("double") == "CAST(1.5 AS double)"


def test_with_a_table_each_rule_says_how_many_existing_rows_it_would_quarantine() -> None:
    warehouse = Warehouse(total=80688, failing={1: 3})
    results = check_rules(
        warehouse,
        _config(("a", "invoice_id IS NOT NULL"), ("b", "amount >= 0")),
        table="cat.sch.bronze",
    )
    assert [(r.failing, r.total) for r in results] == [(0, 80688), (3, 80688)]
    assert "FROM cat.sch.bronze" in warehouse.statements[-1]
    lines = format_results(results)
    assert "(80,688 rows checked)" in lines[0]
    assert "would quarantine 3 of 80,688 rows" in lines[1]


def test_a_table_that_does_not_exist_yet_is_not_an_error() -> None:
    results = check_rules(
        Warehouse(missing_table=True), _config(("a", "invoice_id IS NOT NULL")), table="c.s.t"
    )
    assert results[0].error is None and results[0].failing is None
    assert format_results(results) == ["  ok     a"]


def test_a_rule_that_failed_to_run_is_not_counted() -> None:
    warehouse = Warehouse(refuse=("BROKEN",), failing={1: 5})
    results = check_rules(
        warehouse,
        _config(("bad", "invoice_id rlike 'BROKEN'"), ("fine", "amount >= 0")),
        table="c.s.t",
    )
    assert results[0].error and results[0].failing is None
    assert results[1].failing == 5 and "AS f0" not in warehouse.statements[-1]


def test_a_spec_without_rules_costs_nothing() -> None:
    warehouse = Warehouse()
    assert check_rules(warehouse, _config()) == []
    assert warehouse.statements == []


def test_the_command_exits_non_zero_when_a_rule_is_broken(monkeypatch, capsys) -> None:
    bundle = Path(__file__).resolve().parents[1] / "framework_ingestion"
    for refuse, expected in (((), 0), (("rlike",), 1)):
        monkeypatch.setattr(
            "lakeflow_framework.rule_check.databricks_cli", lambda *_a, refuse=refuse: Warehouse(refuse)
        )
        code = main(["check-rules", str(bundle), "--warehouse", "w", "--profile", "P"])
        assert code == expected
    out = capsys.readouterr().out
    assert "abs_gl.yaml: 26 rule(s)" in out and "ERROR  ride_id_format" in out


@pytest.mark.parametrize(
    ("expression", "message"),
    [
        ("length(trim(a) > 0", "never closed"),
        ("length(a)) > 0", "no '(' before it"),
        ("a IS NOT", "ends in the middle of a condition"),
        ("a IS NOT NULL AND", "ends in the middle of a condition"),
        ("length(a) >=", "ends in the middle of a condition"),
        ("length(a) > 1 +", "ends in the middle of a condition"),
        ("a IN ('x',", "never closed"),
    ],
)
def test_generate_catches_unclosed_brackets_and_half_written_conditions(expression, message) -> None:
    errors: list[str] = []
    assert_safe_rule_expression(expression, {"a"}, label="s", name="r", errors=errors)
    assert errors and message in errors[0]


def test_a_rule_that_looks_unfinished_but_is_not_is_accepted() -> None:
    for expression in (
        "a IS NOT NULL",
        "CASE WHEN a = 'x' THEN true ELSE false END",
        "a rlike '^(x|y)$'",
        "a IN ('and', 'or', ')')",  # operators and brackets inside text are only text
        "length(a) BETWEEN 1 AND 10",
    ):
        errors: list[str] = []
        assert_safe_rule_expression(expression, {"a"}, label="s", name="r", errors=errors)
        assert errors == [], expression
