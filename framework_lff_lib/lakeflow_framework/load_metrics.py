"""Append-only bronze load counts, written by each delivery job after its pipeline refresh.

No MERGE: each run inserts a few rows for the update that just finished. Retries may append a
second row for the same update_id; v_bronze_loads keeps the latest recorded_at. Counts come from
that update's event-log flow_progress only, never from scanning bronze or quarantine tables.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

EVENT_LOG_SUFFIX = "_event_log"
LEDGER = "bronze_load_ledger"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_EXPECTATIONS = (
    "ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>"
)


def record_bronze_loads(spark, catalog: str, job_name: str) -> int:
    """Append rows_read / bronze / quarantine counts for the latest pipeline update of ``job_name``."""

    if not _IDENTIFIER.fullmatch(catalog or ""):
        raise ValueError("catalog is required")
    if not _IDENTIFIER.fullmatch(job_name or ""):
        raise ValueError("job name is required")
    meta = f"{catalog}.framework_meta"
    event_log = f"{meta}.{job_name}{EVENT_LOG_SUFFIX}"
    log.info("load metrics: recording latest update of %s from %s", job_name, event_log)
    frame = spark.sql(latest_bronze_loads_sql(event_log, meta, job_name))
    rows = _rows(frame)
    if not rows:
        log.info("load metrics: no flow_progress for the latest update yet")
        return 0
    recorded = datetime.now(timezone.utc).replace(microsecond=0)
    for row in rows:
        row["recorded_at"] = recorded
        if row.get("expectations") is not None and not isinstance(row["expectations"], str):
            row["expectations"] = str(row["expectations"])
    spark.createDataFrame(rows).write.mode("append").saveAsTable(f"{meta}.{LEDGER}")
    for row in rows:
        log.info(
            "load metrics: %s update %s dataflow %s: read=%s bronze=%s quarantine=%s processed=%s",
            job_name,
            row.get("update_id"),
            row.get("dataflow"),
            row.get("rows_read"),
            row.get("rows_bronze"),
            row.get("rows_quarantine"),
            row.get("rows_processed"),
        )
    log.info("load metrics: appended %d row(s) to %s.%s", len(rows), meta, LEDGER)
    return len(rows)


def latest_bronze_loads_sql(event_log: str, meta: str, job_name: str) -> str:
    """SQL that builds one ledger row per dataflow for the newest update in ``event_log``."""

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
    max_by(status, event_at) AS status,
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
  '{job_name}' AS job_name,
  r.pipeline_id,
  max(r.pipeline_name) AS pipeline_name,
  r.update_id,
  r.dataflow,
  max(r.owner_kind) AS owner_kind,
  max(r.owner) AS owner,
  max(CASE WHEN r.layer = 'bronze' THEN r.table_name END) AS bronze_table,
  max(CASE WHEN r.layer = 'quarantine' THEN r.table_name END) AS quarantine_table,
  min(r.started_at) AS started_at,
  max(r.ended_at) AS ended_at,
  (unix_millis(max(r.ended_at)) - unix_millis(min(r.started_at))) / 1000.0 AS duration_seconds,
  max(CASE WHEN r.layer = 'staged' THEN r.rows_written END) AS rows_read,
  max(CASE WHEN r.layer = 'bronze' THEN r.rows_written END) AS rows_bronze,
  max(CASE WHEN r.layer = 'quarantine' THEN r.rows_written END) AS rows_quarantine,
  coalesce(max(CASE WHEN r.layer = 'bronze' THEN r.rows_written END), 0)
    + coalesce(max(CASE WHEN r.layer = 'quarantine' THEN r.rows_written END), 0) AS rows_processed,
  max(CASE WHEN r.layer = 'bronze' THEN r.status END) AS bronze_status,
  max(CASE WHEN r.layer = 'quarantine' THEN r.status END) AS quarantine_status,
  max(e.expectations) AS expectations
FROM resolved r
LEFT JOIN staged_expectations e ON e.dataflow = r.dataflow
WHERE r.dataflow IS NOT NULL
  AND r.layer IN ('staged', 'bronze', 'quarantine')
GROUP BY r.pipeline_id, r.update_id, r.dataflow
"""


def _rows(frame: Any) -> list[dict]:
    collected = frame.collect() if hasattr(frame, "collect") else list(frame)
    rows = []
    for row in collected:
        rows.append(row if isinstance(row, dict) else row.asDict(recursive=True))
    return rows
