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
    while position < len(expression):
        match = _RULE_TOKEN.match(expression, position)
        if match is None:
            errors.append(
                f"{label}: rule '{name}' has invalid token near {expression[position:]!r}"
            )
            return
        token = match.group(0)
        position = match.end()
        if token.isspace() or token[:1] in "'\"`0.=<>+-*/%(),." or token[:1].isdigit():
            continue
        if token.lower() not in allowed:
            errors.append(
                f"{label}: rule '{name}' may only reference this table's columns, "
                f"literals, and approved functions (unknown {token!r})"
            )
            return


def compile_rules(config: PipelineConfig) -> list[CompiledRule]:
    rules = [CompiledRule(rule.name, rule.expression, "declared") for rule in config.rules]
    for key in config.primary_keys:
        rules.append(CompiledRule(f"not_null_{key}", f"{key} IS NOT NULL", "key"))
    sequence = config.watermark_column
    if config.dedup and sequence and sequence not in config.primary_keys:
        # Auto CDC cannot order a row whose sequence value is null.
        rules.append(CompiledRule(f"not_null_{sequence}", f"{sequence} IS NOT NULL", "key"))
    for column in config.columns:
        expression = (
            f"TRY_CAST({column.name} AS {column.type_name}) IS NOT NULL OR {column.name} IS NULL"
        )
        rules.append(CompiledRule(f"cast_{column.name}", expression, "type"))
    if (
        config.load_method in {"autoloader", "volume_files"}
        and config.file_format in RESCUE_FORMATS
    ):
        rules.append(CompiledRule("schema_conformance", "_rescued_data IS NULL", "schema"))
    return rules


def failed_rules_sql(rules: list[CompiledRule]) -> str:
    if not rules:
        return "CAST(array() AS ARRAY<STRING>)"
    structs = ", ".join(
        f"named_struct('rule', '{rule.name}', 'ok', {rule.holds()})" for rule in rules
    )
    return "transform(filter(array(" + structs + "), entry -> NOT entry.ok), entry -> entry.rule)"


def expectations(rules: list[CompiledRule]) -> dict[str, str]:
    """One warn expectation per rule, read from DQ_COLUMN so the rule is not evaluated again."""

    return {rule.name: f"NOT array_contains({DQ_COLUMN}, '{rule.name}')" for rule in rules}
