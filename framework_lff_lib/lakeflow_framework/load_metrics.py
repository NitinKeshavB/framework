"""Append-only load counts for any medallion layer, written after each pipeline refresh.

Central recon for bronze, silver, gold (and product). Runs even when refresh_pipeline failed
(``run_if: ALL_DONE``). Rows are driven from registered dataflows for the job, left-joined to
the latest update's ``flow_progress``: every dataflow gets one ledger row, ``FAILED`` when the
event log is missing or has nothing for that dataflow (including partial delivery failures).
AUTO CDC metrics use ``num_upserted_rows`` / ``num_deleted_rows`` when ``num_output_rows`` is
absent. No MERGE. Retries may append again; ``v_loads`` keeps the latest ``recorded_at``.
"""

from __future__ import annotations

import logging
import re
from typing import Any

log = logging.getLogger(__name__)

EVENT_LOG_SUFFIX = "_event_log"
LEDGER = "load_ledger"
LAYERS = frozenset({"bronze", "silver", "gold", "product"})
LEDGER_COLUMNS = (
    "job_name",
    "layer",
    "job_run_id",
    "pipeline_id",
    "update_pipeline_uuid",
    "pipeline_name",
    "update_id",
    "dataflow",
    "owner_kind",
    "owner",
    "target_table",
    "quarantine_table",
    "started_at",
    "ended_at",
    "duration_seconds",
    "rows_read",
    "rows_written",
    "rows_upserted",
    "rows_deleted",
    "rows_quarantine",
    "rows_processed",
    "target_status",
    "quarantine_status",
    "expectations",
    "failure_reason",
    "recorded_at",
)
METRIC_TABLE = "load_metric"
METRIC_NAMES = (
    "rows_read",
    "rows_written",
    "rows_upserted",
    "rows_deleted",
    "rows_quarantine",
    "rows_processed",
)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_EXPECTATIONS = (
    "ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>"
)
_FAILED_REASON = (
    "no flow_progress for the latest update; pipeline likely failed before metrics — "
    "repair the failed task and rerun from the point of failure"
)
_MISSING_EVENT_LOG_REASON = (
    "event log table not found; pipeline likely failed before publishing metrics — "
    "repair the failed task and rerun from the point of failure"
)
# Prefer append metrics; AUTO CDC exposes upserts/deletes instead of num_output_rows.
_FP = "details:flow_progress"
_METRICS = f"{_FP}.metrics"
_OUTPUT_ROWS = (
    f"coalesce("
    f"{_METRICS}.num_output_rows::BIGINT, "
    f"CASE WHEN {_METRICS}.num_upserted_rows IS NOT NULL "
    f"OR {_METRICS}.num_deleted_rows IS NOT NULL "
    f"THEN coalesce({_METRICS}.num_upserted_rows::BIGINT, 0) "
    f"+ coalesce({_METRICS}.num_deleted_rows::BIGINT, 0) END)"
)
_LOAD_LOG_COLS = (
    "update_id",
    "dataflow",
    "rows_read",
    "rows_written",
    "rows_upserted",
    "rows_deleted",
    "rows_quarantine",
    "rows_processed",
    "target_status",
    "failure_reason",
)


def record_loads(
    spark,
    catalog: str,
    job_name: str,
    layer: str = "bronze",
    job_run_id: str | None = None,
) -> int:
    """Append one load row per registered dataflow for the latest update."""

    if not _IDENTIFIER.fullmatch(catalog or ""):
        raise ValueError("catalog is required")
    if not _IDENTIFIER.fullmatch(job_name or ""):
        raise ValueError("job name is required")
    if job_run_id is not None and not _IDENTIFIER.fullmatch(job_run_id):
        raise ValueError("job_run_id is required")
    _require_layer(layer)
    from lakeflow_framework.metadata import EVENTS_SCHEMA

    meta = f"{catalog}.framework_meta"
    events = f"{catalog}.{EVENTS_SCHEMA}"
    event_log_name = f"{job_name}{EVENT_LOG_SUFFIX}"
    event_log = f"{events}.{event_log_name}"
    if _event_log_exists(spark, events, event_log_name):
        log.info(
            "load metrics: recording latest %s update of %s from %s", layer, job_name, event_log
        )
        loads = latest_loads_sql(event_log, meta, job_name, layer, job_run_id=job_run_id)
    else:
        log.info(
            "load metrics: %s missing; inserting FAILED row(s) for registered dataflows",
            event_log,
        )
        loads = missing_event_log_loads_sql(meta, job_name, layer, job_run_id=job_run_id)
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW _framework_loads AS {loads}")
    counted = _rows(spark.sql("SELECT count(*) AS n FROM _framework_loads"))
    written = int(counted[0]["n"]) if counted else 0
    if not written:
        log.info("load metrics: no dataflows registered for %s; nothing to append", job_name)
        return 0
    cols = ", ".join(LEDGER_COLUMNS[:-1])
    spark.sql(
        f"INSERT INTO {meta}.{LEDGER} SELECT {cols}, "
        f"current_timestamp() AS recorded_at FROM _framework_loads"
    )
    # Dual-write long-shape metrics so new metric names do not require a wide ALTER.
    metric_selects = " UNION ALL ".join(
        f"SELECT job_name, layer, update_id, dataflow, '{name}' AS metric_name, "
        f"CAST({name} AS BIGINT) AS metric_value FROM _framework_loads"
        for name in METRIC_NAMES
    )
    spark.sql(
        f"INSERT INTO {meta}.{METRIC_TABLE} "
        f"SELECT job_name, layer, update_id, dataflow, current_timestamp() AS recorded_at, "
        f"metric_name, metric_value FROM ({metric_selects})"
    )
    log_cols = ", ".join(_LOAD_LOG_COLS)
    for row in _rows(spark.sql(f"SELECT {log_cols} FROM _framework_loads")):
        log.info(
            "load metrics: %s %s update %s dataflow %s: read=%s written=%s upserted=%s deleted=%s "
            "quarantine=%s processed=%s status=%s reason=%s",
            layer,
            job_name,
            *(row.get(col) for col in _LOAD_LOG_COLS),
        )
    log.info("load metrics: appended %d row(s) to %s.%s", written, meta, LEDGER)
    return written


def failed_loads_stub_sql(meta: str, job_name: str, layer: str = "bronze") -> str:
    """Registered dataflows for ``job_name`` (registry driver embedded by ``latest_loads_sql``)."""

    return _registered_dataflows_sql(meta, job_name, layer)


def missing_event_log_loads_sql(
    meta: str,
    job_name: str,
    layer: str = "bronze",
    job_run_id: str | None = None,
) -> str:
    """One FAILED ledger row per registered dataflow when ``{job}_event_log`` does not exist."""

    _require_layer(layer)
    run_id = _job_run_id_sql(job_run_id)
    return f"""
SELECT
  CAST('{job_name}' AS STRING) AS job_name,
  CAST('{layer}' AS STRING) AS layer,
  {run_id} AS job_run_id,
  CAST(reg.pipeline_id AS STRING) AS pipeline_id,
  CAST(NULL AS STRING) AS update_pipeline_uuid,
  CAST(NULL AS STRING) AS pipeline_name,
  CAST(CONCAT('failed-', date_format(current_timestamp(), 'yyyyMMddHHmmss')) AS STRING) AS update_id,
  CAST(reg.dataflow AS STRING) AS dataflow,
  CAST(reg.owner_kind AS STRING) AS owner_kind,
  CAST(reg.owner AS STRING) AS owner,
  CAST(reg.target_table AS STRING) AS target_table,
  CAST(reg.quarantine_table AS STRING) AS quarantine_table,
  CAST(current_timestamp() AS TIMESTAMP) AS started_at,
  CAST(current_timestamp() AS TIMESTAMP) AS ended_at,
  CAST(NULL AS DOUBLE) AS duration_seconds,
  CAST(NULL AS BIGINT) AS rows_read,
  CAST(NULL AS BIGINT) AS rows_written,
  CAST(NULL AS BIGINT) AS rows_upserted,
  CAST(NULL AS BIGINT) AS rows_deleted,
  CAST(NULL AS BIGINT) AS rows_quarantine,
  CAST(NULL AS BIGINT) AS rows_processed,
  CAST('FAILED' AS STRING) AS target_status,
  CAST(NULL AS STRING) AS quarantine_status,
  CAST(NULL AS STRING) AS expectations,
  CAST('{_MISSING_EVENT_LOG_REASON}' AS STRING) AS failure_reason
FROM ({_registered_dataflows_sql(meta, job_name, layer)}) reg
"""


def latest_loads_sql(
    event_log: str,
    meta: str,
    job_name: str,
    layer: str = "bronze",
    job_run_id: str | None = None,
) -> str:
    """One ledger row per registered dataflow, left-joined to the newest update's metrics."""

    _require_layer(layer)
    registered = _registered_dataflows_sql(meta, job_name, layer)
    run_id = _job_run_id_sql(job_run_id)
    target_table = _max_when_layer(layer, "r.table_name")
    quarantine_table = _max_when_layer("quarantine", "r.table_name")
    rows_read = _max_when_layer("staged", "r.rows_written")
    rows_written = _max_when_layer(layer, "r.rows_written")
    rows_upserted = _max_when_layer(layer, "r.rows_upserted")
    rows_deleted = _max_when_layer(layer, "r.rows_deleted")
    rows_quarantine = _max_when_layer("quarantine", "r.rows_written")
    target_status = _max_when_layer(layer, "r.status")
    quarantine_status = _max_when_layer("quarantine", "r.status")
    return f"""
WITH latest AS (
  SELECT origin.update_id AS update_id
  FROM {event_log}
  WHERE event_type = 'update_progress' AND origin.update_id IS NOT NULL
  QUALIFY row_number() OVER (ORDER BY timestamp DESC) = 1
),
progress AS (
  SELECT
    origin.pipeline_id AS pipeline_id,
    origin.pipeline_name AS pipeline_name,
    origin.update_id AS update_id,
    origin.flow_name AS flow_name,
    timestamp AS event_at,
    {_FP}.status::STRING AS status,
    {_OUTPUT_ROWS} AS output_rows,
    {_METRICS}.num_upserted_rows::BIGINT AS upserted_rows,
    {_METRICS}.num_deleted_rows::BIGINT AS deleted_rows,
    {_FP}.data_quality.dropped_records::BIGINT AS dropped_rows,
    from_json({_FP}.data_quality.expectations, '{_EXPECTATIONS}') AS checks
  FROM {event_log} e
  JOIN latest ON latest.update_id = e.origin.update_id
  WHERE e.event_type = 'flow_progress'
),
targets AS (
  SELECT
    origin.pipeline_id AS pipeline_id,
    origin.flow_name AS flow_name,
    max_by(details:flow_definition.output_dataset::STRING, timestamp) AS table_name
  FROM {event_log} e
  JOIN latest ON latest.update_id = e.origin.update_id
  WHERE e.event_type = 'flow_definition'
  GROUP BY origin.pipeline_id, origin.flow_name
),
loads AS (
  SELECT
    pipeline_id,
    update_id,
    flow_name,
    max(pipeline_name) AS pipeline_name,
    min(event_at) AS started_at,
    max(event_at) AS ended_at,
    max_by(status, event_at) FILTER (WHERE status IS NOT NULL) AS status,
    sum(output_rows) AS rows_written,
    sum(upserted_rows) AS rows_upserted,
    sum(deleted_rows) AS rows_deleted
  FROM progress
  GROUP BY pipeline_id, update_id, flow_name
),
flows AS (
  SELECT
    lower(config:table::STRING) AS table_name,
    max(config:layer::STRING) AS layer,
    max(parent_full_name) AS dataflow,
    max(owner_kind) AS owner_kind,
    max(owner) AS owner
  FROM {meta}.asset_registry_current
  WHERE object_kind = 'flow' AND config:table IS NOT NULL
  GROUP BY lower(config:table::STRING)
),
resolved AS (
  SELECT
    l.pipeline_id,
    l.pipeline_name,
    l.update_id,
    l.flow_name,
    l.started_at,
    l.ended_at,
    l.status,
    l.rows_written,
    l.rows_upserted,
    l.rows_deleted,
    coalesce(t.table_name, l.flow_name) AS table_name,
    coalesce(
      f.dataflow,
      nullif(regexp_extract(coalesce(t.table_name, l.flow_name), '([A-Za-z0-9_]+)_staged`?$', 1), '')
    ) AS dataflow,
    coalesce(
      f.layer,
      CASE WHEN coalesce(t.table_name, l.flow_name) RLIKE '_staged`?$' THEN 'staged' END
    ) AS layer,
    f.owner_kind,
    f.owner
  FROM loads l
  LEFT JOIN targets t ON t.pipeline_id = l.pipeline_id AND t.flow_name = l.flow_name
  LEFT JOIN flows f ON f.table_name = lower(replace(coalesce(t.table_name, l.flow_name), '`', ''))
),
staged_expectations AS (
  SELECT
    r.dataflow,
    to_json(sort_array(collect_list(named_struct(
      'name', c.name, 'passed_records', c.passed_records, 'failed_records', c.failed_records
    )))) AS expectations
  FROM resolved r
  JOIN progress p ON p.pipeline_id = r.pipeline_id AND p.update_id = r.update_id
    AND p.flow_name = r.flow_name
  LATERAL VIEW explode(p.checks) exploded AS c
  WHERE r.layer = 'staged' AND r.dataflow IS NOT NULL
  GROUP BY r.dataflow
),
from_events AS (
  SELECT
    CAST(r.pipeline_id AS STRING) AS update_pipeline_uuid,
    CAST(max(r.pipeline_name) AS STRING) AS pipeline_name,
    CAST(r.update_id AS STRING) AS update_id,
    CAST(r.dataflow AS STRING) AS dataflow,
    CAST(max(r.owner_kind) AS STRING) AS owner_kind,
    CAST(max(r.owner) AS STRING) AS owner,
    CAST({target_table} AS STRING) AS target_table,
    CAST({quarantine_table} AS STRING) AS quarantine_table,
    CAST(min(r.started_at) AS TIMESTAMP) AS started_at,
    CAST(max(r.ended_at) AS TIMESTAMP) AS ended_at,
    CAST(
      (unix_millis(max(r.ended_at)) - unix_millis(min(r.started_at))) / 1000.0 AS DOUBLE
    ) AS duration_seconds,
    CAST({rows_read} AS BIGINT) AS rows_read,
    CAST({rows_written} AS BIGINT) AS rows_written,
    CAST({rows_upserted} AS BIGINT) AS rows_upserted,
    CAST({rows_deleted} AS BIGINT) AS rows_deleted,
    CAST({rows_quarantine} AS BIGINT) AS rows_quarantine,
    CAST(
      coalesce({rows_written}, 0) + coalesce({rows_quarantine}, 0) AS BIGINT
    ) AS rows_processed,
    CAST({target_status} AS STRING) AS target_status,
    CAST({quarantine_status} AS STRING) AS quarantine_status,
    CAST(max(e.expectations) AS STRING) AS expectations,
    CAST(
      CASE
        WHEN {target_status} IN ('FAILED', 'CANCELED')
        THEN CONCAT('flow status ', {target_status})
        ELSE NULL
      END AS STRING
    ) AS failure_reason
  FROM resolved r
  LEFT JOIN staged_expectations e ON e.dataflow = r.dataflow
  WHERE r.dataflow IS NOT NULL
    AND r.layer IN ('staged', '{layer}', 'quarantine')
  GROUP BY r.pipeline_id, r.update_id, r.dataflow
  HAVING {target_table} IS NOT NULL
),
registered AS (
  {registered}
)
SELECT
  CAST('{job_name}' AS STRING) AS job_name,
  CAST('{layer}' AS STRING) AS layer,
  {run_id} AS job_run_id,
  CAST(reg.pipeline_id AS STRING) AS pipeline_id,
  CAST(e.update_pipeline_uuid AS STRING) AS update_pipeline_uuid,
  CAST(e.pipeline_name AS STRING) AS pipeline_name,
  CAST(
    coalesce(
      e.update_id,
      latest.update_id,
      CONCAT('failed-', date_format(current_timestamp(), 'yyyyMMddHHmmss'))
    ) AS STRING
  ) AS update_id,
  CAST(reg.dataflow AS STRING) AS dataflow,
  CAST(coalesce(e.owner_kind, reg.owner_kind) AS STRING) AS owner_kind,
  CAST(coalesce(e.owner, reg.owner) AS STRING) AS owner,
  CAST(coalesce(e.target_table, reg.target_table) AS STRING) AS target_table,
  CAST(coalesce(e.quarantine_table, reg.quarantine_table) AS STRING) AS quarantine_table,
  CAST(coalesce(e.started_at, current_timestamp()) AS TIMESTAMP) AS started_at,
  CAST(coalesce(e.ended_at, current_timestamp()) AS TIMESTAMP) AS ended_at,
  CAST(e.duration_seconds AS DOUBLE) AS duration_seconds,
  CAST(e.rows_read AS BIGINT) AS rows_read,
  CAST(e.rows_written AS BIGINT) AS rows_written,
  CAST(e.rows_upserted AS BIGINT) AS rows_upserted,
  CAST(e.rows_deleted AS BIGINT) AS rows_deleted,
  CAST(e.rows_quarantine AS BIGINT) AS rows_quarantine,
  CAST(e.rows_processed AS BIGINT) AS rows_processed,
  CAST(coalesce(e.target_status, 'FAILED') AS STRING) AS target_status,
  CAST(e.quarantine_status AS STRING) AS quarantine_status,
  CAST(e.expectations AS STRING) AS expectations,
  CAST(
    CASE
      WHEN e.dataflow IS NULL THEN '{_FAILED_REASON}'
      ELSE e.failure_reason
    END AS STRING
  ) AS failure_reason
FROM registered reg
LEFT JOIN latest ON true
LEFT JOIN from_events e ON e.dataflow = reg.dataflow
"""


def _registered_dataflows_sql(meta: str, job_name: str, layer: str) -> str:
    """SELECT of this job's dataflows from ``asset_registry_current`` (registry driver)."""

    _require_layer(layer)
    return f"""
SELECT
  CAST(d.full_name AS STRING) AS dataflow,
  CAST(d.owner_kind AS STRING) AS owner_kind,
  CAST(d.owner AS STRING) AS owner,
  CAST(d.config:{layer}::STRING AS STRING) AS target_table,
  CAST(d.config:quarantine::STRING AS STRING) AS quarantine_table,
  CAST(p.full_name AS STRING) AS pipeline_id
FROM {meta}.asset_registry_current d
JOIN {meta}.asset_registry_current p
  ON p.object_kind = 'pipeline' AND p.full_name = d.parent_full_name
WHERE d.object_kind = 'dataflow'
  AND p.parent_full_name = '{job_name}'
"""


def _event_log_exists(spark, schema: str, table_name: str) -> bool:
    """True when ``schema.table_name`` is present (same probe as ``record_pipeline_updates``)."""

    try:
        names = {
            str(row.get("tableName") or row.get("table_name") or "")
            for row in _rows(spark.sql(f"SHOW TABLES IN {schema}"))
        }
    except Exception:
        return False
    return table_name in names


def _job_run_id_sql(job_run_id: str | None) -> str:
    if job_run_id is None:
        return "CAST(NULL AS STRING)"
    if not _IDENTIFIER.fullmatch(job_run_id):
        raise ValueError("job_run_id is required")
    return f"CAST('{job_run_id}' AS STRING)"


def _require_layer(layer: str) -> None:
    if layer not in LAYERS:
        raise ValueError(f"layer must be one of {sorted(LAYERS)}")


def _max_when_layer(layer_name: str, expr: str) -> str:
    return f"max(CASE WHEN r.layer = '{layer_name}' THEN {expr} END)"


def _rows(frame: Any) -> list[dict]:
    from lakeflow_framework.sqlnames import spark_rows

    return spark_rows(frame)
