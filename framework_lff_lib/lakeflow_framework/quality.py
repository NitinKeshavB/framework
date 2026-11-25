"""Compile config rules into SQL predicates. A NULL predicate is a failure.

Rule expressions are interpolated into the staging SELECT by design. They are not arbitrary SQL:
``assert_safe_rule_expression`` allows only this table's columns, literals, and an allow-listed
set of functions and operators. Subqueries, comments, and DDL/DML keywords are rejected. Even
with that check, expressions still run as the pipeline's ``run_as`` identity — grant that
principal only the source's own volume / foreign catalog / bronze schema.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from lakeflow_framework.config import PipelineConfig
from lakeflow_framework.load import RESCUE_FORMATS

# Each rule is evaluated once, into this column. Pass, fail, and expectations all read it.
DQ_COLUMN = "_dq_failed_rules"
# What the row's failed rules mean for it: fail, quarantine, drop or pass. File sources only.
DQ_RESULT = "_dq_result"
PASSED = f"size({DQ_COLUMN}) = 0"
FAILED = f"size({DQ_COLUMN}) > 0"

_RULE_COMMENT = re.compile(r"(--|/\*|\*/)")
_RULE_DENIED = re.compile(
    r"(?i)\b("
    r"select|from|where|join|union|intersect|except|with|insert|update|delete|merge|"
    r"create|drop|alter|truncate|grant|revoke|execute|call|exists|lateral|explode|"
    r"inline|pivot|unpivot|window|over|partition|qualify|using|table|values|describe|"
    r"show|use|refresh|optimize|vacuum|copy|restore|clone|msck|analyze|cache|uncache|"
    r"set|reset|add|remove|list|get_json_object|from_json|schema_of_json"
    r")\b"
)
_RULE_TOKEN = re.compile(
    r"'([^']|'')*'"  # string literal
    r'|"([^"]|"")*"'  # quoted identifier
    r"|`([^`]+)`"  # backtick identifier
    r"|0[xX][0-9A-Fa-f]+"  # hex
    r"|\d+(\.\d+)?([eE][+-]?\d+)?"  # number
    r"|[A-Za-z_][A-Za-z0-9_]*"  # bare identifier / keyword / function
    r"|<=|>=|<>|!=|[=<>+\-*/%(),.]"  # operators and punctuation
    r"|\s+"
)
_RULE_KEYWORDS = frozenset(
    {
        "and",
        "or",
        "not",
        "is",
        "null",
        "true",
        "false",
        "in",
        "between",
        "like",
        "rlike",
        "regexp",
        "case",
        "when",
        "then",
        "else",
        "end",
        "as",
        "cast",
        "try_cast",
    }
)
# A rule cannot end on one of these: something has to follow.
_RULE_OPEN_ENDS = frozenset(
    {"and", "or", "not", "is", "in", "between", "like", "rlike", "regexp", "case", "when", "then",
     "else", "as", "cast", "try_cast"}
)  # fmt: skip
_RULE_OPERATORS = frozenset({"=", "<", ">", "<=", ">=", "<>", "!=", "+", "-", "*", "/", "%", ","})
_RULE_FUNCTIONS = frozenset(
    {
        "abs",
        "ceil",
        "ceiling",
        "coalesce",
        "concat",
        "contains",
        "current_date",
        "current_timestamp",
        "date",
        "date_add",
        "date_diff",
        "date_format",
        "date_sub",
        "datediff",
        "day",
        "endswith",
        "floor",
        "greatest",
        "hour",
        "if",
        "iff",
        "ifnull",
        "isnull",
        "least",
        "left",
        "length",
        "lower",
        "ltrim",
        "minute",
        "month",
        "nullif",
        "nvl",
        "nvl2",
        "regexp_extract",
        "regexp_replace",
        "replace",
        "right",
        "round",
        "rtrim",
        "second",
        "size",
        "startswith",
        "substring",
        "timestamp",
        "to_date",
        "to_timestamp",
        "trim",
        "trunc",
        "upper",
        "year",
    }
)


@dataclass(frozen=True)
class CompiledRule:
    name: str
    expression: str
    origin: str
    action: str = "quarantine"  # quarantine, drop, warn, or fail

    def holds(self) -> str:
        return f"COALESCE(({self.expression}), false)"


def assert_safe_rule_expression(
    expression: str, columns: set[str], *, label: str, name: str, errors: list[str]
) -> None:
    """Reject rule SQL that is not a boolean expression over this table's columns.

    Does not make expressions trusted: they still execute as the pipeline ``run_as`` principal.
    """

    if not expression or ";" in expression or _RULE_COMMENT.search(expression):
        errors.append(
            f"{label}: rule '{name}' needs one boolean expression "
            "(no comments, no statement separators)"
        )
        return
    if _RULE_DENIED.search(expression):
        errors.append(
            f"{label}: rule '{name}' must not use subqueries, DDL/DML, or denied SQL keywords"
        )
        return
    allowed = {column.lower() for column in columns} | _RULE_KEYWORDS | _RULE_FUNCTIONS
    position = 0
    depth = 0
    last = ""
    while position < len(expression):
        match = _RULE_TOKEN.match(expression, position)
        if match is None:
            errors.append(
                f"{label}: rule '{name}' has invalid token near {expression[position:]!r}"
            )
            return
        token = match.group(0)
        position = match.end()
        if token.isspace():
            continue
        last = token.lower()
        if last == "(":
            depth += 1
        elif last == ")":
            depth -= 1
            if depth < 0:
                errors.append(f"{label}: rule '{name}' has a ')' with no '(' before it")
                return
        if token[:1] in "'\"`0.=<>+-*/%(),." or token[:1].isdigit():
            continue
        if token.lower() not in allowed:
            errors.append(
                f"{label}: rule '{name}' may only reference this table's columns, "
                f"literals, and approved functions (unknown {token!r})"
            )
            return
    if depth:
        errors.append(f"{label}: rule '{name}' has a '(' that is never closed")
    elif last in _RULE_OPEN_ENDS or last in _RULE_OPERATORS:
        errors.append(f"{label}: rule '{name}' ends in the middle of a condition (after {last!r})")


def compile_rules(config: PipelineConfig) -> list[CompiledRule]:
    """The declared rules, then the checks the framework adds. The added ones follow the table's
    ``on_fail``: a table set to fail also fails on a missing key or an unreadable value.

    A table with no quarantine (no rules and no quarantine block) loses no row. A new column, or
    a value that does not fit its column, is kept in ``_rescued_data`` and the row loads, with
    the check shown as a warning. The one exception is a row an Auto CDC load cannot apply: a
    null key or sequence value. That row is dropped and counted in ``rows_dropped``. Once the
    table asks for quarantine, or sets ``on_fail`` itself, every added check follows that.
    """

    action = config.on_fail
    key_action = type_action = schema_action = action
    if action == "quarantine" and not config.has_quarantine:
        type_action = schema_action = "warn"
        key_action = "drop" if config.uses_auto_cdc else "warn"
    rules = [
        CompiledRule(rule.name, rule.expression, "declared", rule.action)
        for rule in config.active_rules
    ]
    for key in config.primary_keys:
        rules.append(CompiledRule(f"not_null_{key}", f"{key} IS NOT NULL", "key", key_action))
    sequence = config.watermark_column
    if config.dedup and sequence and sequence not in config.primary_keys:
        # Auto CDC cannot order a row whose sequence value is null.
        rules.append(
            CompiledRule(f"not_null_{sequence}", f"{sequence} IS NOT NULL", "key", key_action)
        )
    for column in config.columns:
        expression = (
            f"TRY_CAST({column.name} AS {column.type_name}) IS NOT NULL OR {column.name} IS NULL"
        )
        rules.append(CompiledRule(f"cast_{column.name}", expression, "type", type_action))
    if (
        config.load_method in {"autoloader", "volume_files"}
        and config.file_format in RESCUE_FORMATS
    ):
        rules.append(
            CompiledRule("schema_conformance", "_rescued_data IS NULL", "schema", schema_action)
        )
    return rules


def failed_rules_sql(rules: list[CompiledRule]) -> str:
    if not rules:
        return "CAST(array() AS ARRAY<STRING>)"
    structs = ", ".join(
        f"named_struct('rule', '{rule.name}', 'ok', {rule.holds()})" for rule in rules
    )
    return "transform(filter(array(" + structs + "), entry -> NOT entry.ok), entry -> entry.rule)"


def expectations(rules: list[CompiledRule], *, files: bool = False) -> dict[str, str]:
    """One warn expectation per rule that does not fail the update, for the counts in the UI and
    the event log. Read from DQ_COLUMN so the rule is not evaluated again.

    A file source blocks the file instead of failing mid-flow, so its ``fail`` rules count here too.
    """

    return {
        rule.name: f"NOT array_contains({DQ_COLUMN}, '{rule.name}')"
        for rule in rules
        if files or rule.action != "fail"
    }


def fail_expectations(rules: list[CompiledRule], *, files: bool = False) -> dict[str, str]:
    """The rules that fail the whole update when any row breaks them (``expect_all_or_fail``).

    Only for sources that are not files. A file source never uses this: a failed flow would leave
    the file unread, so it would be picked up again. Its ``fail`` rules block the file instead (see
    ``blocking_rules``) and a check after the load fails the update.
    """

    if files:
        return {}
    return {
        rule.name: f"NOT array_contains({DQ_COLUMN}, '{rule.name}')"
        for rule in rules
        if rule.action == "fail"
    }


def blocking_rules(rules: list[CompiledRule]) -> list[str]:
    """Names of the ``fail`` rules: one row breaking one of them blocks its whole file."""

    return [rule.name for rule in rules if rule.action == "fail"]


def result_sql(rules: list[CompiledRule]) -> str:
    """What happens to a row, decided once when it is staged and stored with it: ``fail`` (its file
    is blocked), ``quarantine``, ``drop`` or ``pass`` (a warn-only row passes).

    Stored, not recomputed, so a rule changed later never reclassifies rows already read.
    """

    branches = []
    for action in ("fail", "quarantine", "drop"):
        names = [rule.name for rule in rules if rule.action == action]
        if names:
            branches.append(f"WHEN arrays_overlap({DQ_COLUMN}, {_names(names)}) THEN '{action}'")
    return "CASE " + " ".join(branches) + " ELSE 'pass' END" if branches else "'pass'"


def valid_where(rules: list[CompiledRule], *, files: bool = False) -> str:
    """The rows that go to bronze: those that broke no quarantine or drop rule.

    A row that broke only a warn rule is kept. For a file source a row of a blocked file is kept
    out by the caller; for any other source a ``fail`` rule fails the update before this runs.
    """

    if files:
        return f"{DQ_RESULT} = 'pass'"
    if all(rule.action == "quarantine" for rule in rules):
        return PASSED
    blocking = [rule.name for rule in rules if rule.action in {"quarantine", "drop"}]
    if not blocking:
        return "true"
    return f"NOT arrays_overlap({DQ_COLUMN}, {_names(blocking)})"


def quarantine_where(rules: list[CompiledRule], *, files: bool = False) -> str:
    """The rows that go to quarantine: those that broke a quarantine rule (and, for a file source,
    a ``fail`` rule: the bad rows of a blocked file are kept for inspection)."""

    if files:
        return f"{DQ_RESULT} IN ('fail', 'quarantine')"
    if all(rule.action == "quarantine" for rule in rules):
        return FAILED
    names = [rule.name for rule in rules if rule.action == "quarantine"]
    return f"arrays_overlap({DQ_COLUMN}, {_names(names)})" if names else "false"


def _names(names: list[str]) -> str:
    return "array(" + ", ".join(f"'{name}'" for name in names) + ")"
