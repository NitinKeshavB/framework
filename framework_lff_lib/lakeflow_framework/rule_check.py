"""Run a spec's quality rules on a SQL warehouse, before they are deployed.

``generate`` and ``bundle:validate`` read a rule as text: they catch an unknown column, an
unapproved function, an unclosed bracket, or a condition that stops half way. They cannot catch what
only an engine knows: a regex that does not compile, a function given the wrong number of arguments,
text compared to a number, a rule that is not a condition. Those fail when the pipeline loads, and a
rule that cannot be analysed fails the whole update, not only its own table.

This evaluates every rule against one sample row on a warehouse, so each of those shows up here,
named, before a deploy. Given a table that already holds data, it also counts the rows each rule
would send to quarantine.
"""

from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from lakeflow_framework.config import IS_DELETED, LINEAGE_COLUMNS, PipelineConfig
from lakeflow_framework.sqlnames import quote_name

Runner = Callable[[str], list[dict[str, Any]]]


class RuleCheckError(Exception):
    """The warehouse refused a statement. The message is its first line."""


@dataclass
class RuleResult:
    table: str
    rule: str
    error: str | None = None
    failing: int | None = None  # rows the rule would quarantine, when the table has data
    total: int | None = None


def sample_value(type_name: str) -> str:
    """A SQL literal of a column's type, so a rule is checked against the right kind of value."""

    kind = type_name.strip().lower()
    if kind.startswith(("decimal", "numeric")) or kind in {
        "int",
        "integer",
        "bigint",
        "smallint",
        "tinyint",
        "long",
        "short",
    }:
        return f"CAST(1 AS {type_name})"
    if kind in {"double", "float", "real"}:
        return f"CAST(1.5 AS {type_name})"
    if kind == "boolean":
        return "true"
    if kind == "date":
        return "DATE '2026-01-01'"
    if kind.startswith("timestamp"):
        return "TIMESTAMP '2026-01-01 00:00:00'"
    return "'x'"


def sample_row(config: PipelineConfig) -> str:
    """A one-row query holding every column a rule may name, with a non-empty value of its type."""

    values = {column.name: sample_value(column.type_name) for column in config.columns}
    for name in sorted(LINEAGE_COLUMNS - set(values)):
        values[name] = "true" if name == IS_DELETED else "'x'"
    return "SELECT " + ", ".join(f"{value} AS {quote_name(name)}" for name, value in values.items())


def check_rules(
    run: Runner, config: PipelineConfig, *, table: str | None = None
) -> list[RuleResult]:
    """Evaluate each declared rule of ``config``. With ``table``, also count the failing rows."""

    results = [RuleResult(config.pipeline_id, rule.name) for rule in config.active_rules]
    if not results:
        return results
    row = sample_row(config)
    every = ", ".join(
        f"coalesce(({rule.expression}), false) AS r{i}"
        for i, rule in enumerate(config.active_rules)
    )
    try:
        run(f"SELECT {every} FROM ({row})")
    except RuleCheckError:
        # One rule is wrong. Run them one at a time to say which.
        for result, rule in zip(results, config.active_rules, strict=True):
            try:
                run(f"SELECT coalesce(({rule.expression}), false) AS r FROM ({row})")
            except RuleCheckError as exc:
                result.error = str(exc)
    if table:
        _count(run, config, table, results)
    return results


def _count(run: Runner, config: PipelineConfig, table: str, results: list[RuleResult]) -> None:
    good = [(i, rule) for i, rule in enumerate(config.active_rules) if results[i].error is None]
    if not good:
        return
    counts = ", ".join(
        f"count_if(NOT coalesce(({rule.expression}), false)) AS f{i}" for i, rule in good
    )
    try:
        (found,) = run(f"SELECT count(*) AS total, {counts} FROM {table}")
    except RuleCheckError:
        return  # the table does not exist yet, or holds no readable data
    for i, _rule in good:
        results[i].total = int(found["total"])
        results[i].failing = int(found[f"f{i}"])


def format_results(results: list[RuleResult]) -> list[str]:
    """One line per rule: ok, how many existing rows it would quarantine, or the error."""

    lines = []
    for result in results:
        if result.error:
            lines.append(f"  ERROR  {result.rule}: {result.error}")
        elif result.failing:
            lines.append(
                f"  ok     {result.rule}: would quarantine {result.failing:,} of {result.total:,} rows"
            )
        else:
            seen = f" ({result.total:,} rows checked)" if result.total is not None else ""
            lines.append(f"  ok     {result.rule}{seen}")
    return lines


def databricks_cli(profile: str | None, warehouse: str) -> Runner:
    """Run statements on a warehouse through the Databricks CLI, as the profile's user."""

    def api(method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        command = ["databricks", "api", method, path]
        if profile:
            command += ["--profile", profile]
        if body is not None:
            command += ["--json", json.dumps(body)]
        done = subprocess.run(command, capture_output=True, text=True)
        if done.returncode:
            raise RuleCheckError((done.stderr or done.stdout).strip().splitlines()[0][:300])
        return json.loads(done.stdout)

    def run(statement: str) -> list[dict[str, Any]]:
        reply = api(
            "post",
            "/api/2.0/sql/statements",
            {"warehouse_id": warehouse, "statement": statement, "wait_timeout": "50s"},
        )
        while reply["status"]["state"] in ("PENDING", "RUNNING"):
            time.sleep(2)
            reply = api("get", f"/api/2.0/sql/statements/{reply['statement_id']}")
        if reply["status"]["state"] != "SUCCEEDED":
            message = reply["status"].get("error", {}).get("message", reply["status"]["state"])
            first = next((line for line in message.splitlines() if line.strip()), message)
            raise RuleCheckError(first.strip()[:300])
        columns = [c["name"] for c in reply["manifest"]["schema"].get("columns", [])]
        return [
            dict(zip(columns, row, strict=True))
            for row in reply.get("result", {}).get("data_array", [])
        ]

    return run
