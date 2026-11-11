"""Bronze graph. Spark and the pipelines module are passed in.

Two shapes, chosen by the load method:

- raw table methods (autoloader, copy_into, ctas) land files in a raw table, then publish it to
  bronze and quarantine;
- rawless methods (volume_files, database_table) read the source directly. The raw layer is the
  files already in the raw volume, or the database table itself.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lakeflow_framework.config import IDENTIFIER, PipelineConfig, config_from_document
from lakeflow_framework.load import _projection, get_strategy
from lakeflow_framework.quality import compile_rules, fail_sql, failed_rules_sql, pass_sql

STREAMING_FILES = {"autoloader", "volume_files"}


class MetadataNotFound(Exception):
    """The pipeline id is not in the registry."""


@dataclass
class Dataset:
    kind: str
    name: str
    relation: str
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class Graph:
    pipeline_id: str
    datasets: list[Dataset]
    expectations: list[tuple[str, str]]
    batch_sql: list[str]
    auto_cdc: dict[str, Any] | None


def build_pipeline(
    pipeline_id: str,
    *,
    spark: Any = None,
    pipelines: Any = None,
    metadata: Any = None,
    config: PipelineConfig | dict[str, Any] | None = None,
) -> Graph:
    if spark is None and pipelines is None and metadata is None and config is None:
        # The one-line pipeline file: build_pipeline("<id>"). Use the running session.
        pipelines = _cluster_pipelines()
        spark = _cluster_session()
    parsed = _resolve_config(pipeline_id, spark, metadata, config)
    if parsed.pipeline_id != pipeline_id:
        raise MetadataNotFound(f"{pipeline_id} does not match config {parsed.pipeline_id}")
    graph = assemble(parsed)
    if pipelines is not None:
        _register(graph, pipelines, spark, parsed)
    return graph


def _cluster_session() -> Any:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise MetadataNotFound(
            "build_pipeline needs Spark. Off a cluster, pass config= and pipelines="
        ) from exc
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _cluster_pipelines() -> Any:
    try:
        from pyspark import pipelines
    except ImportError as exc:
        raise MetadataNotFound(
            "build_pipeline needs pyspark.pipelines, which only a Lakeflow pipeline provides"
        ) from exc
    return pipelines


def assemble(config: PipelineConfig) -> Graph:
    strategy = get_strategy(config.load_method)
    rules = compile_rules(config)
    bronze_name = config.table_name("bronze")
    quarantine_name = config.table_name("quarantine")
    valid_name = f"{config.pipeline_id}_valid"
    expectations = [(rule.name, rule.expression) for rule in rules]
    datasets: list[Dataset] = []
    if config.has_raw:
        origin = config.table_name("raw")
        datasets.append(
            Dataset(
                kind="raw",
                name=origin,
                relation=strategy.raw_relation,
                detail={
                    "options": strategy.options(config),
                    "path": config.path,
                    "projection": _projection(config, source="file"),
                    "format": config.file_format,
                },
            )
        )
    elif config.load_method == "database_table":
        origin = config.source_reference()
    else:
        origin = config.path
    relation = "batch" if config.is_batch else "streaming"
    datasets.append(
        Dataset(
            kind="valid",
            name=valid_name,
            relation="temporary_view",
            detail={"where": pass_sql(rules), "source": origin},
        )
    )
    datasets.append(
        Dataset(
            kind="quarantine",
            name=quarantine_name,
            relation=relation,
            detail={
                "where": fail_sql(rules),
                "failed_rules": failed_rules_sql(rules),
                "source": origin,
            },
        )
    )
    auto_cdc = None
    if config.uses_auto_cdc:
        datasets.append(
            Dataset(
                kind="bronze",
                name=bronze_name,
                relation="auto_cdc",
                detail={"keys": list(config.primary_keys), "sequence_by": config.watermark_column},
            )
        )
        auto_cdc = {
            "target": bronze_name,
            "source": valid_name,
            "keys": list(config.primary_keys),
            "sequence_by": config.watermark_column,
            "stored_as_scd_type": 1,
        }
    else:
        datasets.append(
            Dataset(
                kind="bronze",
                name=bronze_name,
                relation=relation,
                detail={"where": pass_sql(rules), "source": origin},
            )
        )
    return Graph(
        pipeline_id=config.pipeline_id,
        datasets=datasets,
        expectations=expectations,
        batch_sql=strategy.batch_sql(config),
        auto_cdc=auto_cdc,
    )


def _resolve_config(
    pipeline_id: str,
    spark: Any,
    metadata: Any,
    config: PipelineConfig | dict[str, Any] | None,
) -> PipelineConfig:
    if not IDENTIFIER.match(pipeline_id):
        raise MetadataNotFound(f"{pipeline_id} is not a pipeline id")
    if isinstance(config, PipelineConfig):
        return config
    if isinstance(config, dict):
        return config_from_document(config)
    reader = metadata
    if reader is None:
        if spark is None:
            raise MetadataNotFound(f"{pipeline_id} has no config and no metadata reader")
        reader = UnityMetadata(spark)
    document = reader.get_pipeline(pipeline_id)
    if document is None:
        raise MetadataNotFound(f"{pipeline_id} is not in the metadata registry")
    return config_from_document(document)


def _filtered(frame: Any, where: str, extra: str | None = None) -> Any:
    """Keep the rows matching ``where``. Test doubles without ``filter`` return a description."""

    if not hasattr(frame, "filter"):
        return {"where": where, "frame": frame}
    result = frame.filter(where)
    return result.selectExpr("*", extra) if extra else result


def _register(graph: Graph, pipelines: Any, spark: Any, config: PipelineConfig) -> None:
    by_kind = {dataset.kind: dataset for dataset in graph.datasets}
    raw = by_kind.get("raw")
    valid, quarantine, bronze = by_kind["valid"], by_kind["quarantine"], by_kind["bronze"]
    pass_where = valid.detail["where"]
    fail_where = quarantine.detail["where"]
    failed_rules = f"{quarantine.detail['failed_rules']} AS failed_rules"
    options = get_strategy(config.load_method).options(config)

    def _read_files() -> Any:
        reader = spark.readStream.format("cloudFiles")
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load(config.path)

    def _read_origin() -> Any:
        """The source as it is: files for Auto Loader, the table for a database load."""

        if config.load_method in STREAMING_FILES:
            return _read_files()
        if config.load_method == "database_table":
            return spark.read.table(config.source_reference())
        if config.is_batch:
            return spark.read.table(raw.name)
        return spark.readStream.table(raw.name)

    landing_name = f"{config.pipeline_id}_landing"

    def _read_stored() -> Any:
        """What quarantine and bronze read: the raw table, or the source itself."""

        if config.load_method == "volume_files":
            return spark.readStream.table(landing_name)
        if raw is None:
            return _read_origin()
        if config.is_batch:
            return spark.read.table(raw.name)
        return spark.readStream.table(raw.name)

    def _lineage(frame: Any) -> Any:
        if raw is not None or not hasattr(frame, "selectExpr"):
            return frame
        if config.load_method == "volume_files":
            return frame.selectExpr(
                "*", "_metadata.file_path AS _source_file", "current_timestamp() AS _ingested_at"
            )
        return frame.selectExpr("*", "current_timestamp() AS _ingested_at")

    if config.load_method == "volume_files":

        @pipelines.temporary_view(name=landing_name)
        def _landing() -> Any:
            return _lineage(_read_files())

    @pipelines.temporary_view(name=valid.name)
    def _valid() -> Any:
        frame = _read_stored() if config.load_method == "volume_files" else _lineage(_read_origin())
        if hasattr(frame, "filter"):
            return frame.filter(pass_where)
        return {"view": valid.name, "where": pass_where, "frame": frame}

    quarantine_kind = pipelines.materialized_view if config.is_batch else pipelines.table

    @quarantine_kind(name=quarantine.name)
    def _quarantine() -> Any:
        frame = _read_stored()
        if config.load_method != "volume_files":
            frame = _lineage(frame)
        return _filtered(frame, fail_where, failed_rules)

    if bronze.relation == "auto_cdc":
        pipelines.create_streaming_table(name=bronze.name)
        if config.load_method == "database_table":
            pipelines.create_auto_cdc_from_snapshot_flow(
                target=bronze.name,
                source=valid.name,
                keys=list(config.primary_keys),
                stored_as_scd_type=1,
            )
        else:
            pipelines.create_auto_cdc_flow(**graph.auto_cdc)
    else:
        bronze_kind = pipelines.materialized_view if config.is_batch else pipelines.table

        @bronze_kind(name=bronze.name)
        def _bronze() -> Any:
            if raw is None:
                reader = spark.read if config.is_batch else spark.readStream
                return reader.table(valid.name)
            return _filtered(_read_stored(), pass_where)

    if config.load_method == "autoloader":

        def _raw() -> Any:
            return _read_files()

        registered = _raw
        for name, expression in reversed(graph.expectations):
            decorator = pipelines.expect(name, expression)
            if callable(decorator):
                registered = decorator(registered)
        pipelines.table(name=raw.name)(registered)


class UnityMetadata:
    """Read the current pipeline document from framework_meta. Inserts happen in the collector."""

    def __init__(self, spark: Any, catalog: str | None = None):
        self.spark = spark
        self.catalog = catalog

    def get_pipeline(self, pipeline_id: str) -> dict[str, Any] | None:
        if not IDENTIFIER.fullmatch(pipeline_id):
            raise MetadataNotFound(f"{pipeline_id} is not a pipeline id")
        frame = self.spark.sql(
            "SELECT config_json FROM framework_meta.v_object_current "
            "WHERE object_kind = 'pipeline' AND full_name = :pipeline_id",
            args={"pipeline_id": pipeline_id},
        )
        rows = frame.collect() if hasattr(frame, "collect") else list(frame)
        if not rows:
            return None
        import json

        payload = rows[0]["config_json"] if isinstance(rows[0], dict) else rows[0].config_json
        return json.loads(payload)["config"]
