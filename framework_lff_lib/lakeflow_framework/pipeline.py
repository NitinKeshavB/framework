"""Bronze graph. Spark and the pipelines module are passed in.

Two shapes, chosen by the load method:

- raw table methods (autoloader, copy_into, ctas) land files in a raw table, then publish it to
  bronze and quarantine;
- rawless methods (volume_files, database_table) read the source directly. The raw layer is the
  files already in the raw volume, or the database table itself.

``build_pipelines`` isolates per-spec failures: one bad or missing spec is logged and skipped so
the other tables in the delivery still register. The pipeline's event hook (``pipeline_metrics``)
then records that table FAILED, with the error that stopped it.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from lakeflow_framework.config import (
    BATCH_ID,
    CATALOG_SETTING,
    IDENTIFIER,
    INGEST_TIMESTAMP,
    IS_DELETED,
    MANIFEST_SETTING,
    MAX_CLUSTER_KEYS,
    RESCUED_DATA,
    SPECS_SETTING,
    SRC_FILE_NAME,
    SRC_FILE_PATH,
    ConfigValidationError,
    PipelineConfig,
    config_from_document,
    validate_configs,
)
from lakeflow_framework.load import ROW_HASH_EXPR, _projection, get_strategy, projection_exprs
from lakeflow_framework.pipeline_metrics import Dataflow, UpdateRecorder
from lakeflow_framework.pipeline_metrics import install as install_recorder
from lakeflow_framework.quality import (
    DQ_COLUMN,
    DQ_RESULT,
    blocking_rules,
    compile_rules,
    expectations,
    fail_expectations,
    failed_rules_sql,
    quarantine_where,
    result_sql,
    valid_where,
)
from lakeflow_framework.sqlnames import require_catalog, sql_string
from lakeflow_framework.volume_listing import list_volume_files

log = logging.getLogger(__name__)


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
    fail_expectations: list[tuple[str, str]] = field(default_factory=list)


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


def build_pipelines(
    pipeline_ids: list[str],
    *,
    spark: Any = None,
    pipelines: Any = None,
    metadata: Any = None,
) -> list[Graph]:
    """Register every table in a delivery.

    The generated pipeline sets ``lff.manifest`` to its delivery JSON, so the specs are read
    from the bundle. Without it, the registry is queried once for the whole list.

    One table that fails to load or register is skipped so the rest of the delivery still runs.
    The delivery's load recorder (``pipeline_metrics``) is installed as an event hook, and it
    records that table as FAILED with the error that stopped it, in the same update's rows.
    """

    if not pipeline_ids:
        return []
    if spark is None and pipelines is None and metadata is None:
        pipelines = _cluster_pipelines()
        spark = _cluster_session()
    graphs: list[Graph] = []
    if metadata is None:
        problems: dict[str, str] = {}
        delivery: dict[str, str] = {}
        configured = bundle_configs(spark, problems=problems, delivery=delivery)
        if configured is not None:
            for pipeline_id in pipeline_ids:
                config = configured.get(pipeline_id)
                if config is None:
                    problems.setdefault(
                        pipeline_id,
                        f"spec is missing or invalid in {MANIFEST_SETTING}/{SPECS_SETTING}",
                    )
                    log.error(
                        "%s: not loadable from %s/%s (%s); skipping so other tables in this "
                        "delivery still load",
                        pipeline_id,
                        MANIFEST_SETTING,
                        SPECS_SETTING,
                        problems[pipeline_id],
                    )
                    continue
                graph = _try_build(
                    pipeline_id, spark=spark, pipelines=pipelines, config=config, problems=problems
                )
                if graph is not None:
                    graphs.append(graph)
            # Installed before the check below, so a delivery in which every table failed to
            # register still records why.
            _install_recorder(pipelines, spark, list(pipeline_ids), configured, problems, delivery)
            return _require_some(graphs, pipeline_ids)
    reader = metadata
    if reader is None:
        if spark is None:
            raise MetadataNotFound("build_pipelines has no metadata reader")
        reader = UnityMetadata(spark)
    if hasattr(reader, "get_pipelines"):
        documents = reader.get_pipelines(list(pipeline_ids))
    else:
        documents = {pipeline_id: reader.get_pipeline(pipeline_id) for pipeline_id in pipeline_ids}
    for pipeline_id in pipeline_ids:
        document = documents.get(pipeline_id) if isinstance(documents, dict) else None
        if not document:
            log.error(
                "%s: not in the metadata registry; skipping so other tables in this delivery "
                "still load",
                pipeline_id,
            )
            continue
        graph = _try_build(pipeline_id, spark=spark, pipelines=pipelines, config=document)
        if graph is not None:
            graphs.append(graph)
    return _require_some(graphs, pipeline_ids)


def bundle_configs(
    spark: Any,
    *,
    problems: dict[str, str] | None = None,
    delivery: dict[str, str] | None = None,
) -> dict[str, PipelineConfig] | None:
    """Configs from ``lff.manifest`` (preferred) or legacy ``lff.specs``, or None if unset.

    Manifest: one JSON read/parse for the whole delivery. Specs: each path validated alone so
    one bad file does not block the others.

    ``problems`` receives, by pipeline id, why a spec could not be loaded. ``delivery`` receives
    the job name the manifest was generated for, under ``job_name``.
    """

    conf = getattr(spark, "conf", None)
    if conf is None:
        return None
    catalog = conf.get(CATALOG_SETTING, None) or None
    manifest = conf.get(MANIFEST_SETTING, None)
    if manifest:
        return _configs_from_manifest(
            Path(str(manifest).strip()), catalog, problems=problems, delivery=delivery
        )
    specs = conf.get(SPECS_SETTING, None)
    if not specs:
        return None
    paths = [Path(path.strip()) for path in specs.split(",") if path.strip()]
    configured: dict[str, PipelineConfig] = {}
    for path in paths:
        try:
            for config in validate_configs([path], catalog=catalog):
                configured[config.pipeline_id] = config
        except ConfigValidationError as exc:
            log.error("%s: invalid spec; skipping: %s", path, "; ".join(exc.errors))
            if problems is not None:
                problems.setdefault(path.stem, f"invalid spec {path.name}: {'; '.join(exc.errors)}")
        except Exception as exc:
            log.exception("%s: could not read spec; skipping: %s", path, exc)
            if problems is not None:
                problems.setdefault(path.stem, f"could not read spec {path.name}: {exc}")
    return configured


def _with_catalog(value: Any, catalog: str) -> Any:
    """Swap the build-time placeholder catalog for the environment's in a manifest document.

    The manifest is generated once, before the target is known, so volume paths carry
    ``/Volumes/<PLACEHOLDER_CATALOG>/...``. Overriding only the top-level ``catalog`` key would
    leave the raw read pointed at the placeholder catalog.
    """

    from lakeflow_framework.sources import PLACEHOLDER_CATALOG

    prefix = f"/Volumes/{PLACEHOLDER_CATALOG}/"
    if isinstance(value, str):
        return f"/Volumes/{catalog}/{value[len(prefix) :]}" if value.startswith(prefix) else value
    if isinstance(value, dict):
        return {key: _with_catalog(item, catalog) for key, item in value.items()}
    if isinstance(value, list):
        return [_with_catalog(item, catalog) for item in value]
    return value


def _configs_from_manifest(
    path: Path,
    catalog: str | None,
    *,
    problems: dict[str, str] | None = None,
    delivery: dict[str, str] | None = None,
) -> dict[str, PipelineConfig]:
    import json

    configured: dict[str, PipelineConfig] = {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.exception("%s: could not read delivery manifest; skipping all tables: %s", path, exc)
        if problems is not None:
            problems["*"] = f"could not read delivery manifest {path.name}: {exc}"
        return configured
    if delivery is not None and payload.get("delivery"):
        delivery["job_name"] = str(payload["delivery"])
    for item in payload.get("pipelines") or []:
        pipeline_id = str(item.get("pipeline_id") or "")
        document = item.get("config")
        if not pipeline_id or not isinstance(document, dict):
            log.error("%s: malformed pipeline entry; skipping", path)
            continue
        if catalog:
            document = {**_with_catalog(document, catalog), "catalog": catalog}
        try:
            configured[pipeline_id] = config_from_document(document)
        except ConfigValidationError as exc:
            log.error(
                "%s (%s): invalid manifest config; skipping: %s",
                path,
                pipeline_id,
                "; ".join(exc.errors),
            )
            if problems is not None:
                problems[pipeline_id] = f"invalid spec: {'; '.join(exc.errors)}"
        except Exception as exc:
            log.exception("%s (%s): could not load; skipping: %s", path, pipeline_id, exc)
            if problems is not None:
                problems[pipeline_id] = f"could not load spec: {exc}"
    return configured


def _try_build(
    pipeline_id: str,
    *,
    spark: Any,
    pipelines: Any,
    config: PipelineConfig | dict[str, Any],
    problems: dict[str, str] | None = None,
) -> Graph | None:
    try:
        return build_pipeline(pipeline_id, spark=spark, pipelines=pipelines, config=config)
    except Exception as exc:
        log.exception(
            "%s: failed to register; skipping so other tables in this delivery still load: %s",
            pipeline_id,
            exc,
        )
        if problems is not None:
            problems[pipeline_id] = f"failed to register: {type(exc).__name__}: {exc}"
        return None


def _install_recorder(
    pipelines: Any,
    spark: Any,
    pipeline_ids: list[str],
    configured: dict[str, PipelineConfig],
    problems: dict[str, str],
    delivery: dict[str, str],
) -> None:
    """Record this delivery's load counts, status, and errors once per update (event hook).

    Needs a running pipeline: off a cluster there is no ``pipelines`` module to hook.
    """

    if pipelines is None or spark is None:
        return
    known = next(iter(configured.values()), None)
    job_name = (known.job_name if known else None) or delivery.get("job_name")
    catalog = known.catalog if known else _conf(spark, CATALOG_SETTING)
    if not job_name or not catalog:
        log.error("load metrics: no job name or catalog for this delivery; not recording")
        return
    dataflows: list[Dataflow] = []
    for pipeline_id in pipeline_ids:
        config = configured.get(pipeline_id)
        error = problems.get(pipeline_id) or problems.get("*")
        if config is None:
            dataflows.append(
                Dataflow(
                    pipeline_id=pipeline_id,
                    owner_kind="source_system",
                    owner=pipeline_id.split("_", 1)[0],
                    staged=f"{pipeline_id}_staged",
                    target="",
                    quarantine="",
                    build_error=error or "spec could not be loaded",
                )
            )
            continue
        dataflows.append(
            Dataflow(
                pipeline_id=pipeline_id,
                owner_kind=config.owner_kind,
                owner=config.owner,
                staged=f"{pipeline_id}_staged",
                target=config.table_name("bronze"),
                quarantine=config.table_name("quarantine") if config.has_quarantine else "",
                append=not config.uses_auto_cdc,
                build_error=error,
                files=_sibling(config, files_name(config)) if is_file_load(config) else "",
                blocked_check=(
                    _sibling(config, f"{pipeline_id}_blocked_check")
                    if is_file_load(config) and _has_fail_rule(config)
                    else ""
                ),
                arrivals=(
                    _sibling(config, arrivals_name(config))
                    if is_file_load(config) and config.expect_file
                    else ""
                ),
                path=config.path if is_file_load(config) else "",
                source_kind=config.source_kind,
                run_as=config.run_as,
            )
        )
    recorder = UpdateRecorder(
        job_name, (known.layer if known else "bronze"), dataflows, batch_id=batch_id()
    )
    install_recorder(pipelines, spark, recorder, catalog)


# The schema a generated pipeline publishes its own working tables to (staged, files, checks).
# Bronze and quarantine are named explicitly, so only these land here.
PIPELINE_SCHEMA = "framework_meta"


def _sibling(config: PipelineConfig, name: str) -> str:
    """The full name of a working table the pipeline publishes in its own schema."""

    return f"{config.catalog}.{PIPELINE_SCHEMA}.{name}"


def _has_fail_rule(config: PipelineConfig) -> bool:
    return bool(blocking_rules(compile_rules(config)))


def _conf(spark: Any, key: str) -> str | None:
    conf = getattr(spark, "conf", None)
    return (conf.get(key, None) or None) if conf is not None else None


def _require_some(graphs: list[Graph], pipeline_ids: list[str]) -> list[Graph]:
    if graphs:
        return graphs
    raise MetadataNotFound(
        f"every table in this delivery failed to register: {', '.join(pipeline_ids)}"
    )


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


def is_file_load(config: PipelineConfig) -> bool:
    """True when the source is files, where one file is the unit that is read, counted and blocked."""

    return config.load_method in {"autoloader", "volume_files"}


def files_name(config: PipelineConfig) -> str:
    return f"{config.pipeline_id}_files"


def arrivals_name(config: PipelineConfig) -> str:
    return f"{config.pipeline_id}_arrivals"


# A file that landed a moment before the previous run started still belongs to that run.
ARRIVAL_SLACK_MINUTES = 5


def arrivals_sql(config: PipelineConfig, inventory: str, files: str) -> str:
    """Files that arrived since the previous regular run: where they are, and whether this run read
    rows from them (``has_rows``) or an earlier load already recorded them (``recorded_before``).

    A file that arrived, was not read and was never recorded is an empty one: Auto Loader read
    it and found no rows, so it never shows in the per-file view. ``since`` is the start of the
    previous regular run, so a backfill (which also writes to the ledger) does not move it.
    """

    pid = sql_string(config.pipeline_id)
    ledger = f"{PIPELINE_SCHEMA}.load_ledger"
    return (
        f"SELECT i.file_path, regexp_extract(i.file_path, '[^/]+$', 0) AS file_name, "
        "i.file_bytes, i.arrived_at, "
        f"f.{SRC_FILE_PATH} IS NOT NULL AS has_rows, l.src_file_path IS NOT NULL AS recorded_before "
        f"FROM {inventory} i "
        f"LEFT JOIN {files} f ON f.{SRC_FILE_PATH} = i.file_path "
        f"LEFT JOIN (SELECT DISTINCT src_file_path FROM {PIPELINE_SCHEMA}.ledger_detail "
        f"WHERE dataflow = {pid}) l ON l.src_file_path = i.file_path "
        "WHERE i.arrived_at > coalesce(("
        f"SELECT max(started_at) FROM {ledger} WHERE dataflow = {pid} "
        "AND update_id NOT LIKE 'backfill-%'"
        f") - INTERVAL {ARRIVAL_SLACK_MINUTES} MINUTES, TIMESTAMP '1970-01-01')"
    )


def assemble(config: PipelineConfig) -> Graph:
    """The datasets for one table. The source is read once, into a private staging table.

    A pipeline cannot send one read to two tables, and a temporary view runs again for every reader.
    So the staging table computes DQ_COLUMN once per row; bronze takes the passing rows through the
    valid view, and quarantine takes the failing rows.
    """

    strategy = get_strategy(config.load_method)
    rules = compile_rules(config)
    relation = "batch" if config.is_batch else "streaming"
    bronze_name = config.table_name("bronze")
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
    checked = {"source": origin, "failed_rules": failed_rules_sql(rules)}
    files = is_file_load(config)
    if files:
        checked["options"] = strategy.options(config)
        checked["result"] = result_sql(rules)
    staged_name = f"{config.pipeline_id}_staged"
    valid_name = f"{config.pipeline_id}_valid"
    datasets.append(Dataset(kind="staged", name=staged_name, relation=relation, detail=checked))
    datasets.append(
        Dataset(
            kind="valid",
            name=valid_name,
            relation="temporary_view",
            detail={
                "source": staged_name,
                "where": valid_where(rules, files=files),
                "blocked_by": files_name(config) if blocking_rules(rules) and files else None,
            },
        )
    )
    if files:
        # One row per file of this run, with its counts and status. The ledger is written from it,
        # and a file with a ``fail`` rule broken is kept out of bronze through it.
        datasets.append(
            Dataset(
                kind="files",
                name=files_name(config),
                relation="materialized_view",
                detail={"source": staged_name, "blocking": blocking_rules(rules)},
            )
        )
        if config.expect_file:
            # Every file that arrived since the last run, with whether this run read rows from it.
            datasets.append(
                Dataset(
                    kind="arrivals",
                    name=arrivals_name(config),
                    relation="materialized_view",
                    detail={"path": config.path, "files": files_name(config)},
                )
            )
        if blocking_rules(rules):
            datasets.append(
                Dataset(
                    kind="blocked_check",
                    name=f"{config.pipeline_id}_blocked_check",
                    relation="materialized_view",
                    detail={"source": files_name(config)},
                )
            )
    auto_cdc = None
    if config.uses_auto_cdc:
        datasets.append(
            Dataset(
                kind="bronze",
                name=bronze_name,
                relation="auto_cdc",
                detail={
                    "source": valid_name,
                    "keys": list(config.primary_keys),
                    "sequence_by": config.watermark_column,
                },
            )
        )
        auto_cdc = {
            "target": bronze_name,
            "source": valid_name,
            "keys": list(config.primary_keys),
            "sequence_by": config.watermark_column,
            "stored_as_scd_type": config.scd_type,
        }
        if config.scd_type == 2:
            # A new version opens when the data changes, not when the pipeline stamps a new load.
            auto_cdc["track_history_except_column_list"] = list(UNTRACKED_COLUMNS)
    else:
        datasets.append(
            Dataset(
                kind="bronze", name=bronze_name, relation=relation, detail={"source": valid_name}
            )
        )
    if config.has_quarantine:
        datasets.append(
            Dataset(
                kind="quarantine",
                name=config.table_name("quarantine"),
                relation=relation,
                detail={"source": staged_name, "where": quarantine_where(rules, files=files)},
            )
        )
    return Graph(
        pipeline_id=config.pipeline_id,
        datasets=datasets,
        expectations=list(expectations(rules, files=files).items()),
        fail_expectations=list(fail_expectations(rules, files=files).items()),
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


def _filtered(frame: Any, where: str) -> Any:
    """Keep the rows matching ``where``. Requires a DataFrame-like with ``.filter``."""

    if not hasattr(frame, "filter"):
        raise TypeError(
            "pipeline frames must support .filter; inject a DataFrame-like test double, "
            "not a bare dict"
        )
    return frame.filter(where)


def _typed_sequence(frame: Any, config: PipelineConfig) -> Any:
    """Cast the sequence column to its declared type, so Auto CDC does not order strings.

    The valid view keeps only rows whose cast and not-null rules passed, so the cast cannot fail.
    """

    sequence, type_name = config.watermark_column, config.sequence_type
    if not type_name or not hasattr(frame, "columns"):
        return frame
    return frame.selectExpr(
        *(
            f"CAST(`{name}` AS {type_name}) AS `{name}`" if name == sequence else f"`{name}`"
            for name in frame.columns
        )
    )


def quarantine_frame(frame: Any, config: PipelineConfig, internal: str) -> Any:
    """The quarantine shape: bronze's columns and types, plus ``failed_rules``.

    Bronze casts its sequence column (``_typed_sequence``); quarantine does the same with
    TRY_CAST, because its rows are the ones that may not convert. A value that does not convert
    is NULL in the column and kept as text in ``_rescued_data`` under the column's name, merged
    into what Auto Loader already rescued. A table with no rescue column gets one here.
    The pipeline and a backfill both use this, so the two write the same shape.
    """

    if not hasattr(frame, "selectExpr"):
        return frame
    frame = frame.selectExpr(f"* EXCEPT ({internal})", f"{DQ_COLUMN} AS failed_rules")
    sequence, type_name = config.watermark_column, config.sequence_type
    if not sequence or not type_name or not hasattr(frame, "columns"):
        return frame
    return frame.selectExpr(*rescued_cast_exprs(list(frame.columns), sequence, type_name))


def rescued_cast_exprs(columns: list[str], column: str, type_name: str) -> list[str]:
    """Select list that TRY_CASTs ``column`` and keeps an unconvertible value in _rescued_data."""

    lost = f"`{column}` IS NOT NULL AND TRY_CAST(`{column}` AS {type_name}) IS NULL"
    kept = f"map('{column}', CAST(`{column}` AS STRING))"
    if RESCUED_DATA in columns:
        earlier = (
            f"coalesce(from_json({RESCUED_DATA}, 'MAP<STRING, STRING>'), "
            "CAST(map() AS MAP<STRING, STRING>))"
        )
        rescued = (
            f"CASE WHEN {lost} THEN to_json(map_concat({earlier}, {kept})) "
            f"ELSE {RESCUED_DATA} END AS {RESCUED_DATA}"
        )
    else:
        rescued = f"CASE WHEN {lost} THEN to_json({kept}) END AS {RESCUED_DATA}"
    exprs = [
        f"TRY_CAST(`{name}` AS {type_name}) AS `{name}`"
        if name == column
        else (rescued if name == RESCUED_DATA else f"`{name}`")
        for name in columns
    ]
    if RESCUED_DATA not in columns:
        exprs.append(rescued)
    return exprs


# Change history bronze and quarantine keep, so a silver run that was down for days can still
# read exactly what changed. Delta removes change files older than the retention.
CHANGE_FEED_RETENTION = "interval 30 days"
# Columns the pipeline stamps on every load. An SCD2 table must not open a version because of them.
UNTRACKED_COLUMNS = (SRC_FILE_PATH, SRC_FILE_NAME, BATCH_ID, INGEST_TIMESTAMP)
_BATCH_ID: str | None = None
_BATCH_STARTED: datetime | None = None
# The per-file status of a file of this run. FAILED means a ``fail`` rule blocked the file.
FILE_STATUS = "file_status"
FILE_FAILED = "FAILED"
FILE_WARNING = "WARNING"
FILE_PROCESSED = "PROCESSED"
# A clock that runs a little behind the cluster's must not hide rows of this run.
_CLOCK_SLACK = timedelta(minutes=5)


def batch_id() -> str:
    """The id of this pipeline run, written to every row as ``_batch_id``.

    Pipeline code cannot read its own update id, so one id is made when the run's code loads.
    That happens once per update, so every micro-batch of the run shares it. The recorder, in
    the same process, writes it to ``load_ledger.batch_id`` beside the real update id.
    """

    global _BATCH_ID, _BATCH_STARTED
    if _BATCH_ID is None:
        _BATCH_ID = str(uuid.uuid4())
        _BATCH_STARTED = datetime.now(UTC)
    return _BATCH_ID


def files_sql(
    staged: str, blocking: list[str], *, this_run: bool = True, quarantine: bool = True
) -> str:
    """One row per file of this run: counts and status, from the staged rows of this run only.

    The run is found by ``_batch_id`` and, so Delta can skip older files, by the load time the
    staged table is clustered on. A backfill passes ``this_run=False`` for a view that holds
    nothing but the rows it is counting. With ``quarantine=False`` (a table without one) the bad
    rows of a blocked file are kept nowhere, so they count as dropped, not quarantined.
    """

    run = batch_id()
    since = (_BATCH_STARTED or datetime.now(UTC)) - _CLOCK_SLACK
    scope = (
        f"WHERE {BATCH_ID} = {sql_string(run)} "
        f"AND {INGEST_TIMESTAMP} >= TIMESTAMP '{since.strftime('%Y-%m-%d %H:%M:%S')}+00:00' "
        if this_run
        else ""
    )
    names = ", ".join(sql_string(name) for name in blocking)
    blockers = (
        f"array_join(array_intersect(array_distinct(flatten(collect_list({DQ_COLUMN}))), "
        f"array({names})), ', ')"
        if blocking
        else "''"
    )
    kept_bad = "('fail', 'quarantine')" if quarantine else "('quarantine')"
    lost_bad = "('drop')" if quarantine else "('drop', 'fail')"
    return (
        f"SELECT {SRC_FILE_PATH}, {SRC_FILE_NAME}, {BATCH_ID}, "
        "count(*) AS rows_read, "
        f"CASE WHEN count_if({DQ_RESULT} = 'fail') > 0 THEN 0 "
        f"ELSE count_if({DQ_RESULT} = 'pass') END AS rows_loaded, "
        f"count_if({DQ_RESULT} IN {kept_bad}) AS rows_quarantined, "
        f"count_if({DQ_RESULT} IN {lost_bad}) AS rows_dropped, "
        f"CASE WHEN count_if({DQ_RESULT} = 'fail') > 0 "
        f"THEN count_if({DQ_RESULT} = 'pass') ELSE 0 END AS rows_blocked, "
        f"CASE WHEN count_if({DQ_RESULT} = 'fail') > 0 THEN '{FILE_FAILED}' "
        f"WHEN count_if({DQ_RESULT} <> 'pass') > 0 THEN '{FILE_WARNING}' "
        f"ELSE '{FILE_PROCESSED}' END AS {FILE_STATUS}, "
        f"{blockers} AS blocking_rules, "
        f"array_join(array_sort(array_distinct(flatten(collect_list({DQ_COLUMN})))), ', ') "
        "AS failed_rules "
        f"FROM {staged} {scope}"
        f"GROUP BY {SRC_FILE_PATH}, {SRC_FILE_NAME}, {BATCH_ID}"
    )


def _table_options(config: PipelineConfig, layer: str) -> dict[str, Any]:
    """Comment, properties, and clustering for a table the pipeline publishes."""

    tags = config.tags.get(layer) or config.tags.get("bronze")
    classification = tags.classification if tags else ""
    described = {"bronze": config.description, "quarantine": config.quarantine_description}
    options: dict[str, Any] = {
        "comment": described.get(layer)
        or (
            f"{layer.capitalize()} for {config.pipeline_id} from source system "
            f"{config.source_system} ({config.data_domain}). Technical owner "
            f"{config.technical_owner}. Classification {classification}."
            + (" failed_rules lists the rules each row failed." if layer == "quarantine" else "")
        ),
        "table_properties": {
            "quality": layer,
            "lakeflow_framework.pipeline_id": config.pipeline_id,
        },
    }
    if layer == "files":
        options["comment"] = (
            f"One row per file of the latest run of {config.pipeline_id}: counts and status."
        )
        return options
    if layer == "arrivals":
        options["comment"] = (
            f"Files that arrived for {config.pipeline_id} since its previous run, and whether "
            "the run read rows from each. A file with none is empty."
        )
        return options
    if layer == "staged":
        options["private"] = True
        if is_file_load(config):
            # The per-file view reads only the rows of this run, found by load time.
            options["cluster_by"] = [INGEST_TIMESTAMP]
        return options
    if layer in {"bronze", "quarantine"}:
        options["table_properties"].update(
            {
                "delta.enableChangeDataFeed": "true",
                "delta.deletedFileRetentionDuration": CHANGE_FEED_RETENTION,
                "delta.logRetentionDuration": CHANGE_FEED_RETENTION,
            }
        )
    # Cluster on the load time so a reader that wants only what is newer skips the older files.
    keys = list(config.cluster_by) if layer == "bronze" else []
    if layer == "bronze" and not keys and config.uses_auto_cdc:
        keys = list(config.primary_keys)[: MAX_CLUSTER_KEYS - 1]
    if layer in {"bronze", "quarantine"} and INGEST_TIMESTAMP not in keys:
        keys = [INGEST_TIMESTAMP, *keys][:MAX_CLUSTER_KEYS]
    if keys:
        options["cluster_by"] = keys
    else:
        options["cluster_by_auto"] = True
    return options


def _audit_exprs(config: PipelineConfig, batch: str | None = None) -> list[str]:
    """Select expressions for the audit columns added when a row is staged.

    The row hash, file path, file name, and load time come from the read itself, where ``*`` is
    exactly what the source gave (see ``file_columns`` and ``load.projection_exprs``).
    """

    exprs = [f"{sql_string(batch or batch_id())} AS {BATCH_ID}"]
    if config.delete_when:
        exprs.append(f"coalesce(CAST(({config.delete_when}) AS BOOLEAN), false) AS {IS_DELETED}")
    return exprs


def file_columns(loaded_at: datetime | None = None) -> list[str]:
    """What is added to a row as it is read from a file: its hash, where it came from, and when.

    A backfill passes ``loaded_at`` so every evaluation of its micro-batch gives a row the same load
    time; the pipeline lets Spark stamp the moment it reads.
    """

    stamp = (
        f"TIMESTAMP '{loaded_at.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')}+00:00'"
        if loaded_at
        else "current_timestamp()"
    )
    return [
        ROW_HASH_EXPR,
        f"_metadata.file_path AS {SRC_FILE_PATH}",
        f"_metadata.file_name AS {SRC_FILE_NAME}",
        f"{stamp} AS {INGEST_TIMESTAMP}",
    ]


def stage_frame(frame: Any, config: PipelineConfig, batch: str | None = None) -> Any:
    """The staging step: audit columns, the rules each row failed, and what each row's failures
    mean for it. The main pipeline and a backfill both run exactly this, so a row is judged the
    same way whichever loaded it. ``frame`` already has ``file_columns`` added."""

    rules = compile_rules(config)
    staged = frame.selectExpr(
        "*", *_audit_exprs(config, batch), f"{failed_rules_sql(rules)} AS {DQ_COLUMN}"
    )
    if is_file_load(config):
        staged = staged.selectExpr("*", f"{result_sql(rules)} AS {DQ_RESULT}")
    return staged


def _register(graph: Graph, pipelines: Any, spark: Any, config: PipelineConfig) -> None:
    by_kind = {dataset.kind: dataset for dataset in graph.datasets}
    raw, staged, valid = by_kind.get("raw"), by_kind["staged"], by_kind["valid"]
    bronze, quarantine = by_kind["bronze"], by_kind.get("quarantine")
    options = get_strategy(config.load_method).options(config)
    persisted = pipelines.materialized_view if config.is_batch else pipelines.table

    def _reader() -> Any:
        return spark.read if config.is_batch else spark.readStream

    def _read_files() -> Any:
        reader = spark.readStream.format("cloudFiles")
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load(config.path)

    def _read_source() -> Any:
        if config.load_method == "volume_files":
            frame = _read_files()
            extra = file_columns()
        elif config.load_method == "database_table":
            frame = spark.read.table(config.source_reference())
            extra = [ROW_HASH_EXPR, f"current_timestamp() AS {INGEST_TIMESTAMP}"]
        elif raw is not None:
            return _reader().table(raw.name)
        else:
            raise MetadataNotFound(f"{config.pipeline_id} has no source table to read")
        return frame.selectExpr("*", *extra) if hasattr(frame, "selectExpr") else frame

    def _staged() -> Any:
        frame = _read_source()
        if not hasattr(frame, "selectExpr"):
            raise TypeError(
                "pipeline frames must support .selectExpr; inject a DataFrame-like test double"
            )
        return stage_frame(frame, config)

    registered = _staged
    if graph.expectations:
        registered = pipelines.expect_all(dict(graph.expectations))(registered)
    if graph.fail_expectations:
        # A row that breaks one of these fails the update, so nothing of that run reaches bronze.
        registered = pipelines.expect_all_or_fail(dict(graph.fail_expectations))(registered)
    persisted(name=staged.name, **_table_options(config, "staged"))(registered)

    files_dataset = by_kind.get("files")
    internal = f"{DQ_COLUMN}, {DQ_RESULT}" if files_dataset else DQ_COLUMN

    @pipelines.temporary_view(name=valid.name)
    def _valid() -> Any:
        frame = _filtered(_reader().table(staged.name), valid.detail["where"])
        if valid.detail.get("blocked_by"):
            # The files this run blocked. A file is read once, so a blocked file is never retried:
            # its good rows stay out of bronze by this join while its bad rows go to quarantine.
            blocked = (
                spark.read.table(valid.detail["blocked_by"])
                .filter(f"{FILE_STATUS} = '{FILE_FAILED}'")
                .select(SRC_FILE_PATH, BATCH_ID)
            )
            frame = frame.join(blocked, [SRC_FILE_PATH, BATCH_ID], "left_anti")
        if hasattr(frame, "selectExpr"):
            frame = frame.selectExpr(f"* EXCEPT ({internal})")
        return _typed_sequence(frame, config)

    if files_dataset is not None:

        @pipelines.materialized_view(name=files_dataset.name, **_table_options(config, "files"))
        def _files() -> Any:
            return spark.sql(
                files_sql(
                    staged.name,
                    files_dataset.detail["blocking"],
                    quarantine=config.has_quarantine,
                )
            )

        arrivals_dataset = by_kind.get("arrivals")
        if arrivals_dataset is not None:
            inventory = f"{config.pipeline_id}_inventory"

            @pipelines.temporary_view(name=inventory)
            def _inventory() -> Any:
                # Names, sizes and arrival times only: the content of a file is not read, and a
                # file with no bytes is listed too.
                return spark.createDataFrame(
                    [
                        (item["file_path"], item["file_bytes"], item["arrived_at"])
                        for item in list_volume_files(config.path, config.path_glob)
                    ],
                    "file_path STRING, file_bytes BIGINT, arrived_at TIMESTAMP",
                )

            @pipelines.materialized_view(
                name=arrivals_dataset.name, **_table_options(config, "arrivals")
            )
            def _arrivals() -> Any:
                return spark.sql(arrivals_sql(config, inventory, files_dataset.name))

        checked_files = by_kind.get("blocked_check")
        if checked_files is not None:
            # Runs after bronze and quarantine have finished, so the files are already read when
            # it fails the update. Only this run's blocked files are in the files view.
            @pipelines.expect_all_or_fail({"no_file_blocked_by_fail_rule": "1 = 0"})
            @pipelines.materialized_view(name=checked_files.name)
            def _blocked_check() -> Any:
                return spark.sql(
                    f"SELECT {SRC_FILE_NAME}, rows_read, rows_quarantined, rows_blocked, "
                    f"blocking_rules FROM {files_dataset.name} "
                    f"WHERE {FILE_STATUS} = '{FILE_FAILED}'"
                )

    if bronze.relation == "auto_cdc":
        pipelines.create_streaming_table(name=bronze.name, **_table_options(config, "bronze"))
        if config.load_method == "database_table":
            snapshot: dict[str, Any] = {
                "target": bronze.name,
                "source": valid.name,
                "keys": list(config.primary_keys),
                "stored_as_scd_type": config.scd_type,
            }
            if config.scd_type == 2:
                snapshot["track_history_except_column_list"] = list(UNTRACKED_COLUMNS)
            pipelines.create_auto_cdc_from_snapshot_flow(**snapshot)
        else:
            pipelines.create_auto_cdc_flow(**graph.auto_cdc)
    else:

        @persisted(name=bronze.name, **_table_options(config, "bronze"))
        def _bronze() -> Any:
            return _reader().table(valid.name)

    if quarantine is not None:
        where = quarantine.detail["where"]

        @persisted(name=quarantine.name, **_table_options(config, "quarantine"))
        def _quarantine() -> Any:
            return quarantine_frame(
                _filtered(_reader().table(staged.name), where), config, internal
            )

    if config.load_method == "autoloader":

        def _raw() -> Any:
            frame = _read_files()
            if hasattr(frame, "selectExpr"):
                return frame.selectExpr(*projection_exprs(config, source="file"))
            return frame

        pipelines.table(name=raw.name, **_table_options(config, "raw"))(_raw)


class UnityMetadata:
    """Read the current pipeline document from framework_meta. registry_sync writes it at deploy."""

    def __init__(self, spark: Any, catalog: str | None = None):
        self.spark = spark
        self.catalog = catalog

    def get_pipeline(self, pipeline_id: str) -> dict[str, Any] | None:
        return self.get_pipelines([pipeline_id]).get(pipeline_id)

    def get_pipelines(self, pipeline_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Current config for each id. One query, so a delivery does not scan the registry per table."""

        import json

        ids: list[str] = []
        for pipeline_id in pipeline_ids:
            if not IDENTIFIER.fullmatch(pipeline_id):
                raise MetadataNotFound(f"{pipeline_id} is not a pipeline id")
            if pipeline_id not in ids:
                ids.append(pipeline_id)
        if not ids:
            return {}
        # Inside a pipeline, spark.sql is wrapped by Lakeflow and takes only the statement, so a
        # bound parameter (args=) fails. Each id is safe to inline: it matched IDENTIFIER above,
        # so it holds only letters, digits, and underscores.
        quoted = ", ".join(f"'{pipeline_id}'" for pipeline_id in ids)
        # A pipeline's default catalog is the deployment's. A job's is not, so a caller outside a
        # pipeline names the catalog.
        schema = (
            f"{require_catalog(self.catalog)}.framework_meta" if self.catalog else "framework_meta"
        )
        frame = self.spark.sql(
            f"SELECT full_name, config_json FROM {schema}.v_asset_registry_current "
            f"WHERE object_kind = 'pipeline' AND full_name IN ({quoted})"
        )
        rows = frame.collect() if hasattr(frame, "collect") else list(frame)
        found: dict[str, dict[str, Any]] = {}
        for row in rows:
            if isinstance(row, dict):
                name, payload = str(row["full_name"]), row["config_json"]
            else:
                name, payload = str(row.full_name), row.config_json
            found[name] = json.loads(payload)["config"]
        return found
