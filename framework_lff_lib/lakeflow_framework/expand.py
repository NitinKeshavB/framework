"""Write out the Python and SQL a generated pipeline builds, for one table spec.

``build_pipelines`` builds a pipeline's datasets when the pipeline runs, so the generated pipeline
file is one line. ``expand`` prints the same datasets from the same functions, so the options,
rule expressions, comments and table properties are the framework's own and not retyped. The
files are for reading and review. They are not deployed, and running them next to the real
pipeline would define every table twice.

Only an append table can be expanded. A table that keeps history by key (Auto CDC) has a named
source view and CDC flows that this does not write out yet.
"""

from __future__ import annotations

import pprint
from pathlib import Path
from typing import Any

from lakeflow_framework.config import PipelineConfig, validate_configs
from lakeflow_framework.load import get_strategy
from lakeflow_framework.pipeline import _table_options, assemble, file_columns, is_file_load
from lakeflow_framework.quality import blocking_rules, compile_rules

INTERNAL = "_dq_failed_rules, _dq_result"


class ExpandError(ValueError):
    """The spec cannot be written out."""


def expand_spec(spec: Path, out: Path, catalog: str) -> list[Path]:
    """Validate one spec and write ``<pipeline_id>_pipeline.py`` and ``.sql`` into ``out``."""

    configs = validate_configs([spec], catalog=catalog)
    (config,) = configs
    python, sql = expand(config, spec.name)
    out.mkdir(parents=True, exist_ok=True)
    written = [
        out / f"{config.pipeline_id}_pipeline.py",
        out / f"{config.pipeline_id}_pipeline.sql",
    ]
    written[0].write_text(python, encoding="utf-8")
    written[1].write_text(sql, encoding="utf-8")
    return written


def expand(config: PipelineConfig, spec_name: str) -> tuple[str, str]:
    """The pipeline as Python and as SQL."""

    graph = assemble(config)
    by_kind = {dataset.kind: dataset for dataset in graph.datasets}
    if "valid" in by_kind or config.uses_auto_cdc:
        raise ExpandError(
            f"{config.pipeline_id} keeps history by key (Auto CDC); expand writes append tables only"
        )
    if "quarantine" not in by_kind:
        raise ExpandError(f"{config.pipeline_id} has no quarantine table; expand needs one")
    staged, bronze, quarantine = by_kind["staged"], by_kind["bronze"], by_kind["quarantine"]
    if config.load_method != "volume_files":
        raise ExpandError(
            f"{config.pipeline_id} loads by {config.load_method}; expand needs volume_files"
        )
    options = get_strategy(config.load_method).options(config)
    return (
        _python(config, spec_name, graph, staged, bronze, quarantine, options),
        _sql(config, spec_name, graph, staged, bronze, quarantine, options),
    )


def _pretty(value: Any) -> str:
    return pprint.pformat(value, width=100, sort_dicts=False)


def _text(value: str) -> str:
    """A SQL string literal."""

    return "'" + value.replace("'", "''") + "'"


def _python(config, spec_name, graph, staged, bronze, quarantine, options) -> str:
    pid = config.pipeline_id
    passing = bronze.detail["where"]
    props = {layer: _table_options(config, layer) for layer in ("staged", "bronze", "quarantine")}
    fail_rules = tuple(blocking_rules(compile_rules(config)))
    return f'''"""{pid}: what the generated pipeline builds, written out. Not used by the framework.

Generated from {spec_name} by `lakeflow-framework expand`. The real pipeline file is the one line
``build_pipelines(["{pid}"])``, which builds these same datasets and also registers the event hook
that writes load_ledger and ledger_detail. This file registers the same hook at the end.
"""

import uuid

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from lakeflow_framework.pipeline_metrics import Dataflow, UpdateRecorder, install

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

PATH = {config.path!r}
BRONZE = {bronze.name!r}
QUARANTINE = {quarantine.name!r}
# One id per update, stamped on every row as _batch_id. The framework makes it when its code loads.
BATCH_ID = str(uuid.uuid4())

# Auto Loader. The pipeline owns the checkpoint and schemaLocation, so neither is set.
OPTIONS = {_pretty(options)}

# Added to every row as it is read.
FILE_COLUMNS = {_pretty(file_columns())}

# The rules a row broke, as an array of rule names. Stored with the row, so a later rule change
# never reclassifies a file already read.
FAILED_RULES = """
{staged.detail["failed_rules"]}
"""

# What happens to the row: fail, quarantine, drop, or pass (a row that broke only warn rules).
RESULT = """
{staged.detail["result"]}
"""

# One warn expectation per rule, so pass and fail counts reach the event log.
EXPECTATIONS = {_pretty(dict(graph.expectations))}


@dp.expect_all(EXPECTATIONS)
@dp.table(
    name={staged.name!r},
    comment={props["staged"]["comment"]!r},
    table_properties={_pretty(props["staged"]["table_properties"])},
    private=True,
    cluster_by={props["staged"]["cluster_by"]!r},
)
def {pid}_staged():
    reader = spark.readStream.format("cloudFiles")
    for key, value in OPTIONS.items():
        reader = reader.option(key, value)
    frame = reader.load(PATH).selectExpr("*", *FILE_COLUMNS)
    frame = frame.selectExpr("*", f"'{{BATCH_ID}}' AS _batch_id", f"{{FAILED_RULES}} AS _dq_failed_rules")
    return frame.selectExpr("*", f"{{RESULT}} AS _dq_result")


@dp.table(
    name=BRONZE,
    comment={props["bronze"]["comment"]!r},
    table_properties={_pretty(props["bronze"]["table_properties"])},
    cluster_by={props["bronze"]["cluster_by"]!r},
)
def bronze():
    rows = spark.readStream.table({staged.name!r}).filter({passing!r})
    return rows.selectExpr("* EXCEPT ({INTERNAL})")


@dp.table(
    name=QUARANTINE,
    comment={props["quarantine"]["comment"]!r},
    table_properties={_pretty(props["quarantine"]["table_properties"])},
    cluster_by={props["quarantine"]["cluster_by"]!r},
)
def quarantine():
    rows = spark.readStream.table({staged.name!r}).filter({quarantine.detail["where"]!r})
    return rows.selectExpr("* EXCEPT ({INTERNAL})", "_dq_failed_rules AS failed_rules")


# ---- Load recording --------------------------------------------------------------------------
# An event hook folds the update's events into one load_ledger row per table and one
# ledger_detail row per file, and writes both when every flow has finished. It reads bronze and
# quarantine by _batch_id for the per-file counts, so the pipeline needs no working table for them.
CATALOG = {config.catalog!r}

recorder = UpdateRecorder(
    {config.job_name!r},  # the Databricks job this pipeline belongs to
    {config.layer!r},
    [
        Dataflow(
            pipeline_id={pid!r},
            owner_kind={config.owner_kind!r},
            owner={config.owner!r},
            staged={staged.name!r},
            target=BRONZE,
            quarantine=QUARANTINE,
            append=True,
            file_load={is_file_load(config)!r},
            expect_file={bool(config.expect_file)!r},
            fail_rules={fail_rules!r},
            path=PATH,
            path_glob={(config.path_glob or "")!r},
            source_kind={config.source_kind!r},
            run_as={config.run_as!r},
        )
    ],
    batch_id=BATCH_ID,
)
install(dp, spark, recorder, CATALOG)
'''


def _sql(config, spec_name, graph, staged, bronze, quarantine, options) -> str:
    pid = config.pipeline_id
    passing = bronze.detail["where"]
    props = {layer: _table_options(config, layer) for layer in ("staged", "bronze", "quarantine")}

    def properties(layer: str) -> str:
        return ",\n    ".join(
            f"{_text(key)} = {_text(value)}"
            for key, value in props[layer]["table_properties"].items()
        )

    def cluster(layer: str) -> str:
        return ", ".join(props[layer]["cluster_by"])

    read_options = ",\n      ".join(
        f"{key.removeprefix('cloudFiles.')} => {_text(value)}"
        for key, value in options.items()
        if key != "cloudFiles.format"
    )
    file_cols = ",\n      ".join(file_columns())
    constraints = ",\n  ".join(
        f"CONSTRAINT {name} EXPECT ({condition})" for name, condition in graph.expectations
    )
    return f"""-- {pid}: the same datasets as SQL. Not used by the framework, and not run.
-- Generated from {spec_name} by `lakeflow-framework expand`.
-- Differences from the Python pipeline: ${{batch_id}} is a pipeline setting (the framework makes one
-- id per update); the event hook that writes load_ledger and ledger_detail has no SQL form.
-- Auto Loader options are passed to read_files() without the cloudFiles. prefix.

CREATE OR REFRESH PRIVATE STREAMING TABLE {staged.name} (
  {constraints}
)
CLUSTER BY ({cluster("staged")})
COMMENT {_text(props["staged"]["comment"])}
TBLPROPERTIES (
    {properties("staged")}
)
AS
SELECT *,
  {staged.detail["result"]} AS _dq_result
FROM (
  SELECT *,
    '${{batch_id}}' AS _batch_id,
    {staged.detail["failed_rules"]} AS _dq_failed_rules
  FROM (
    SELECT *,
      {file_cols}
    FROM STREAM read_files(
      {_text(config.path)},
      format => {_text(options["cloudFiles.format"])},
      {read_options}
    )
  )
);

CREATE OR REFRESH STREAMING TABLE {bronze.name}
CLUSTER BY ({cluster("bronze")})
COMMENT {_text(props["bronze"]["comment"])}
TBLPROPERTIES (
    {properties("bronze")}
)
AS
SELECT * EXCEPT ({INTERNAL})
FROM STREAM({staged.name})
WHERE {passing};

CREATE OR REFRESH STREAMING TABLE {quarantine.name}
CLUSTER BY ({cluster("quarantine")})
COMMENT {_text(props["quarantine"]["comment"])}
TBLPROPERTIES (
    {properties("quarantine")}
)
AS
SELECT * EXCEPT ({INTERNAL}), _dq_failed_rules AS failed_rules
FROM STREAM({staged.name})
WHERE {quarantine.detail["where"]};
"""
