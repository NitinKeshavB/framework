"""Compile config rules into SQL predicates. A NULL predicate is a failure."""

from __future__ import annotations

from dataclasses import dataclass

from lakeflow_framework.config import PipelineConfig
from lakeflow_framework.load import RESCUE_FORMATS

# Each rule is evaluated once, into this column. Pass, fail, and expectations all read it.
DQ_COLUMN = "_dq_failed_rules"
PASSED = f"size({DQ_COLUMN}) = 0"
FAILED = f"size({DQ_COLUMN}) > 0"


@dataclass(frozen=True)
class CompiledRule:
    name: str
    expression: str
    origin: str

    def holds(self) -> str:
        return f"COALESCE(({self.expression}), false)"


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


def pass_sql(rules: list[CompiledRule]) -> str:
    if not rules:
        return "TRUE"
    return " AND ".join(rule.holds() for rule in rules)


def fail_sql(rules: list[CompiledRule]) -> str:
    if not rules:
        return "FALSE"
    return "NOT (" + pass_sql(rules) + ")"
