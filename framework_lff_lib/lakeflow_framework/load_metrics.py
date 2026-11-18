"""Append-only load counts for any medallion layer, written after each pipeline refresh.

Central recon for bronze, silver, gold (and product). Runs even when refresh_pipeline failed
(``run_if: ALL_DONE``): if the event log has no flow_progress, inserts FAILED stub rows so
``v_loads`` always shows the attempt. No MERGE. Retries may append again; ``v_loads`` keeps
the latest ``recorded_at``.
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
    "pipeline_id",
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
    "rows_quarantine",
    "rows_processed",
    "target_status",
    "quarantine_status",
    "expectations",
    "failure_reason",
    "recorded_at",
)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_EXPECTATIONS = (
    "ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>"
)
_FAILED_REASON = (
    "no flow_progress for the latest update; pipeline likely failed before metrics — "
    "repair the failed task and rerun from the point of failure"
)


def record_loads(spark, catalog: str, job_name: str, layer: str = "bronze") -> int:
    """Append load rows for the latest update, or FAILED stubs when there is no progress."""

    if not _IDENTIFIER.fullmatch(catalog or ""):
        raise ValueError("catalog is required")
    if not _IDENTIFIER.fullmatch(job_name or ""):
        raise ValueError("job name is required")
    if layer not in LAYERS:
        raise ValueError(f"layer must be one of {sorted(LAYERS)}")
    meta = f"{catalog}.framework_meta"
    event_log = f"{meta}.{job_name}{EVENT_LOG_SUFFIX}"
    log.info("load metrics: recording latest %s update of %s from %s", layer, job_name, event_log)
    loads = latest_loads_sql(event_log, meta, job_name, layer)
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW _framework_loads AS {loads}")
    counted = _rows(spark.sql("SELECT count(*) AS n FROM _framework_loads"))
    written = int(counted[0]["n"]) if counted else 0
    if not written:
        log.info("load metrics: no flow_progress; inserting FAILED stub row(s)")
        stub = failed_loads_stub_sql(meta, job_name, layer)
        spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW _framework_loads AS {stub}")
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
    for row in _rows(
        spark.sql(
            "SELECT dataflow, update_id, rows_read, rows_written, rows_quarantine, "
            "rows_processed, target_status, failure_reason FROM _framework_loads"
        )
    ):
        log.info(
            "load metrics: %s %s update %s dataflow %s: read=%s written=%s quarantine=%s "
            "processed=%s status=%s reason=%s",
            layer,
            job_name,
            row.get("update_id"),
            row.get("dataflow"),
            row.get("rows_read"),
            row.get("rows_written"),
            row.get("rows_quarantine"),
            row.get("rows_processed"),
            row.get("target_status"),
            row.get("failure_reason"),
        )
    log.info("load metrics: appended %d row(s) to %s.%s", written, meta, LEDGER)
    return written


def record_bronze_loads(spark, catalog: str, job_name: str) -> int:
    """Compatibility wrapper: append bronze-layer rows into the central load ledger."""

    return record_loads(spark, catalog, job_name, layer="bronze")


def failed_loads_stub_sql(meta: str, job_name: str, layer: str = "bronze") -> str:
    """One FAILED ledger row per registered dataflow when the event log has no progress."""

    if layer not in LAYERS:
        raise ValueError(f"layer must be one of {sorted(LAYERS)}")
    target = "bronze" if layer == "bronze" else layer
    return f"""
SELECT
  CAST('{job_name}' AS STRING) AS job_name,
  CAST('{layer}' AS STRING) AS layer,
  CAST(p.full_name AS STRING) AS pipeline_id,
  CAST(NULL AS STRING) AS pipeline_name,
  CAST(CONCAT('failed-', date_format(current_timestamp(), 'yyyyMMddHHmmss')) AS STRING) AS update_id,
  CAST(d.full_name AS STRING) AS dataflow,
  CAST(d.owner_kind AS STRING) AS owner_kind,
  CAST(d.owner AS STRING) AS owner,
  CAST(d.config:{target}::STRING AS STRING) AS target_table,
  CAST(d.config:quarantine::STRING AS STRING) AS quarantine_table,
  CAST(current_timestamp() AS TIMESTAMP) AS started_at,
  CAST(current_timestamp() AS TIMESTAMP) AS ended_at,
  CAST(NULL AS DOUBLE) AS duration_seconds,
  CAST(NULL AS BIGINT) AS rows_read,
  CAST(NULL AS BIGINT) AS rows_written,
  CAST(NULL AS BIGINT) AS rows_quarantine,
  CAST(NULL AS BIGINT) AS rows_processed,
  CAST('FAILED' AS STRING) AS target_status,
  CAST(NULL AS STRING) AS quarantine_status,
  CAST(NULL AS STRING) AS expectations,
  CAST('{_FAILED_REASON}' AS STRING) AS failure_reason
FROM {meta}.object_current d
JOIN {meta}.object_current p
  ON p.object_kind = 'pipeline' AND p.full_name = d.parent_full_name
WHERE d.object_kind = 'dataflow'
  AND p.parent_full_name = '{job_name}'
"""


def latest_loads_sql(event_log: str, meta: str, job_name: str, layer: str = "bronze") -> str:
    """SQL that builds one ledger row per dataflow for the newest update in ``event_log``."""

    if layer not in LAYERS:
        raise ValueError(f"layer must be one of {sorted(LAYERS)}")
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
    details:flow_progress.status::STRING AS status,
    details:flow_progress.metrics.num_output_rows::BIGINT AS output_rows,
    details:flow_progress.data_quality.dropped_records::BIGINT AS dropped_rows,
    from_json(details:flow_progress.data_quality.expectations, '{_EXPECTATIONS}') AS checks
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
    sum(output_rows) AS rows_written
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
  FROM {meta}.object_current
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
)
SELECT
  CAST('{job_name}' AS STRING) AS job_name,
  CAST('{layer}' AS STRING) AS layer,
  CAST(r.pipeline_id AS STRING) AS pipeline_id,
  CAST(max(r.pipeline_name) AS STRING) AS pipeline_name,
  CAST(r.update_id AS STRING) AS update_id,
  CAST(r.dataflow AS STRING) AS dataflow,
  CAST(max(r.owner_kind) AS STRING) AS owner_kind,
  CAST(max(r.owner) AS STRING) AS owner,
  CAST(max(CASE WHEN r.layer = '{layer}' THEN r.table_name END) AS STRING) AS target_table,
  CAST(max(CASE WHEN r.layer = 'quarantine' THEN r.table_name END) AS STRING) AS quarantine_table,
  CAST(min(r.started_at) AS TIMESTAMP) AS started_at,
  CAST(max(r.ended_at) AS TIMESTAMP) AS ended_at,
  CAST(
    (unix_millis(max(r.ended_at)) - unix_millis(min(r.started_at))) / 1000.0 AS DOUBLE
  ) AS duration_seconds,
  CAST(max(CASE WHEN r.layer = 'staged' THEN r.rows_written END) AS BIGINT) AS rows_read,
  CAST(max(CASE WHEN r.layer = '{layer}' THEN r.rows_written END) AS BIGINT) AS rows_written,
  CAST(max(CASE WHEN r.layer = 'quarantine' THEN r.rows_written END) AS BIGINT) AS rows_quarantine,
  CAST(
    coalesce(max(CASE WHEN r.layer = '{layer}' THEN r.rows_written END), 0)
      + coalesce(max(CASE WHEN r.layer = 'quarantine' THEN r.rows_written END), 0)
    AS BIGINT
  ) AS rows_processed,
  CAST(max(CASE WHEN r.layer = '{layer}' THEN r.status END) AS STRING) AS target_status,
  CAST(max(CASE WHEN r.layer = 'quarantine' THEN r.status END) AS STRING) AS quarantine_status,
  CAST(max(e.expectations) AS STRING) AS expectations,
  CAST(
    CASE
      WHEN max(CASE WHEN r.layer = '{layer}' THEN r.status END) IN ('FAILED', 'CANCELED')
      THEN CONCAT('flow status ', max(CASE WHEN r.layer = '{layer}' THEN r.status END))
      ELSE NULL
    END AS STRING
  ) AS failure_reason
FROM resolved r
LEFT JOIN staged_expectations e ON e.dataflow = r.dataflow
WHERE r.dataflow IS NOT NULL
  AND r.layer IN ('staged', '{layer}', 'quarantine')
GROUP BY r.pipeline_id, r.update_id, r.dataflow
HAVING max(CASE WHEN r.layer = '{layer}' THEN r.table_name END) IS NOT NULL
"""


def latest_bronze_loads_sql(event_log: str, meta: str, job_name: str) -> str:
    """Compatibility wrapper for bronze-layer SQL."""

    return latest_loads_sql(event_log, meta, job_name, layer="bronze")


def _rows(frame: Any) -> list[dict]:
    collected = frame.collect() if hasattr(frame, "collect") else list(frame)
    rows = []
    for row in collected:
        rows.append(row if isinstance(row, dict) else row.asDict(recursive=True))
    return rows
