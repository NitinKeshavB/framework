"""Raw, bronze, and quarantine graph. Spark and the pipelines module are passed in."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lakeflow_framework.config import IDENTIFIER, PipelineConfig, config_from_document
from lakeflow_framework.load import _projection, get_strategy
from lakeflow_framework.quality import compile_rules, fail_sql, failed_rules_sql, pass_sql


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
    parsed = _resolve_config(pipeline_id, spark, metadata, config)
    if parsed.pipeline_id != pipeline_id:
        raise MetadataNotFound(f"{pipeline_id} does not match config {parsed.pipeline_id}")
    graph = assemble(parsed)
    if pipelines is not None:
        _register(graph, pipelines, spark, parsed)
    return graph


def assemble(config: PipelineConfig) -> Graph:
    strategy = get_strategy(config.load_method)
    rules = compile_rules(config)
    raw_name = config.table_name("raw")
    bronze_name = config.table_name("bronze")
    quarantine_name = config.table_name("quarantine")
    expectations = [(rule.name, rule.expression) for rule in rules]
    datasets = [
        Dataset(
            kind="raw",
            name=raw_name,
            relation=strategy.raw_relation,
            detail={
                "options": strategy.options(config),
                "path": config.path,
                "projection": _projection(config, source="file"),
                "format": config.file_format,
            },
        ),
        Dataset(
            kind="valid",
            name=f"{config.pipeline_id}_valid",
            relation="temporary_view",
            detail={"where": pass_sql(rules), "source": raw_name},
        ),
        Dataset(
            kind="quarantine",
            name=quarantine_name,
            relation=strategy.raw_relation if config.load_method != "ctas" else "batch",
            detail={
                "where": fail_sql(rules),
                "failed_rules": failed_rules_sql(rules),
                "source": raw_name,
            },
        ),
    ]
    auto_cdc = None
    if config.dedup:
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
            "source": f"{config.pipeline_id}_valid",
            "keys": list(config.primary_keys),
            "sequence_by": config.watermark_column,
            "stored_as_scd_type": 1,
        }
    else:
        datasets.append(
            Dataset(
                kind="bronze",
                name=bronze_name,
                relation="batch" if config.load_method == "ctas" else "streaming",
                detail={"where": pass_sql(rules), "source": raw_name},
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


def _register(graph: Graph, pipelines: Any, spark: Any, config: PipelineConfig) -> None:
    raw = next(dataset for dataset in graph.datasets if dataset.kind == "raw")
    valid = next(dataset for dataset in graph.datasets if dataset.kind == "valid")
    quarantine = next(dataset for dataset in graph.datasets if dataset.kind == "quarantine")
    bronze = next(dataset for dataset in graph.datasets if dataset.kind == "bronze")

    def _read_raw() -> Any:
        if config.load_method == "autoloader":
            reader = spark.readStream.format("cloudFiles")
            for key, value in raw.detail["options"].items():
                reader = reader.option(key, value)
            return reader.load(config.path)
        if config.load_method == "ctas":
            return spark.read.table(raw.name)
        return spark.readStream.table(raw.name)

    @pipelines.temporary_view(name=valid.name)
    def _valid() -> Any:
        frame = _read_raw()
        if hasattr(frame, "filter"):
            return frame.filter(valid.detail["where"])
        return {"view": valid.name, "where": valid.detail["where"], "frame": frame}

    if quarantine.relation == "batch":

        @pipelines.materialized_view(name=quarantine.name)
        def _quarantine() -> Any:
            return {"table": quarantine.name, "where": quarantine.detail["where"]}

    else:

        @pipelines.table(name=quarantine.name)
        def _quarantine_stream() -> Any:
            return spark.readStream.table(raw.name)

    if bronze.relation == "auto_cdc":
        pipelines.create_streaming_table(name=bronze.name)
        pipelines.create_auto_cdc_flow(**graph.auto_cdc)
    elif bronze.relation == "batch":

        @pipelines.materialized_view(name=bronze.name)
        def _bronze_batch() -> Any:
            return spark.read.table(raw.name)
    else:

        @pipelines.table(name=bronze.name)
        def _bronze_stream() -> Any:
            return spark.readStream.table(raw.name)

    if config.load_method == "autoloader":

        def _raw() -> Any:
            return _read_raw()

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
        frame = self.spark.sql(
            "SELECT config_json FROM framework_meta.v_object_current "
            f"WHERE object_kind = 'pipeline' AND full_name = '{pipeline_id}'"
        )
        rows = frame.collect() if hasattr(frame, "collect") else list(frame)
        if not rows:
            return None
        import json

        payload = rows[0]["config_json"] if isinstance(rows[0], dict) else rows[0].config_json
        return json.loads(payload)["config"]
