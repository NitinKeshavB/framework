"""framework_metrics pipeline: one row per pipeline update, flow, and table in table_load_metrics.

Every ingestion pipeline publishes its event log to framework_meta as <job name>_event_log.
Each update of this pipeline lists those tables and streams their flow_definition and
flow_progress events into flow_events, one append flow per log. table_load_metrics groups
those events by pipeline, update, and flow. bronze_loads pivots staged, bronze, and
quarantine into one row per table load with rows_read and rows_processed. A log added since the last update gets its
flow on the next one.

Quality rules are checked on the private <pipeline_id>_staged table, so expectation counts
are on the staged row; its dataflow column names the spec, as on the bronze row.
"""

from __future__ import annotations

import re

CATALOG_CONF = "lakeflow_framework.catalog"
EVENT_LOG_SUFFIX = "_event_log"
EVENTS = "flow_events"
METRICS = "table_load_metrics"
BRONZE_LOADS = "bronze_loads"
EVENT_SCHEMA = (
    "job_name STRING, pipeline_id STRING, pipeline_name STRING, update_id STRING, "
    "flow_name STRING, event_id STRING, event_type STRING, event_at TIMESTAMP, details STRING"
)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_EXPECTATIONS = (
    "ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>"
)
_KEYS = "job_name, pipeline_id, update_id, flow_name"


def event_logs(table_names: list[str]) -> dict[str, str]:
    """Event log table name to the job it belongs to, for every <job>_event_log name."""

    logs: dict[str, str] = {}
    for name in sorted(table_names):
        job = name[: -len(EVENT_LOG_SUFFIX)] if name.endswith(EVENT_LOG_SUFFIX) else ""
        if job and _IDENTIFIER.match(name):
            logs[name] = job
    return logs


def event_columns(job_name: str) -> list[str]:
    if not _IDENTIFIER.match(job_name):
        raise ValueError(f"not a job name: {job_name}")
    return [
        f"'{job_name}' AS job_name",
        "origin.pipeline_id AS pipeline_id",
        "origin.pipeline_name AS pipeline_name",
        "origin.update_id AS update_id",
        "origin.flow_name AS flow_name",
        "id AS event_id",
        "event_type",
        "timestamp AS event_at",
        "details",
    ]


def no_events() -> str:
    columns = ", ".join(
        f"CAST(NULL AS {kind}) AS {name}"
        for name, kind in (field.split() for field in EVENT_SCHEMA.split(", "))
    )
    return f"(SELECT {columns} WHERE false) no_events"


def table_load_metrics_sql(events: str, meta: str) -> str:
    """Group flow events into one row per pipeline, update, flow, and table."""

    return f"""
WITH progress AS (
  SELECT
    {_KEYS},
    pipeline_name,
    event_at,
    details:flow_progress.status::STRING AS status,
    details:flow_progress.metrics.num_output_rows::BIGINT AS output_rows,
    details:flow_progress.metrics.num_upserted_rows::BIGINT AS upserted_rows,
    details:flow_progress.metrics.num_deleted_rows::BIGINT AS deleted_rows,
    details:flow_progress.metrics.backlog_bytes::DOUBLE AS backlog_bytes,
    details:flow_progress.metrics.backlog_files::DOUBLE AS backlog_files,
    details:flow_progress.data_quality.dropped_records::BIGINT AS dropped_rows,
    from_json(details:flow_progress.data_quality.expectations, '{_EXPECTATIONS}') AS checks
  FROM {events}
  WHERE event_type = 'flow_progress'
),
targets AS (
  SELECT
    pipeline_id,
    flow_name,
    max_by(details:flow_definition.output_dataset::STRING, event_at) AS table_name
  FROM {events}
  WHERE event_type = 'flow_definition'
  GROUP BY pipeline_id, flow_name
),
loads AS (
  SELECT
    {_KEYS},
    max(pipeline_name) AS pipeline_name,
    min(event_at) AS started_at,
    max(event_at) AS ended_at,
    max_by(status, event_at) AS status,
    sum(output_rows) AS rows_written,
    sum(upserted_rows) AS rows_upserted,
    sum(deleted_rows) AS rows_deleted,
    sum(dropped_rows) AS rows_dropped,
    max_by(backlog_bytes, CASE WHEN backlog_bytes IS NOT NULL THEN event_at END) AS backlog_bytes,
    max_by(backlog_files, CASE WHEN backlog_files IS NOT NULL THEN event_at END) AS backlog_files
  FROM progress
  GROUP BY {_KEYS}
),
check_totals AS (
  SELECT
    {_KEYS},
    expectation.name AS name,
    sum(expectation.passed_records) AS passed_records,
    sum(expectation.failed_records) AS failed_records
  FROM progress
  LATERAL VIEW explode(checks) exploded AS expectation
  GROUP BY {_KEYS}, expectation.name
),
expectations AS (
  SELECT
    {_KEYS},
    sort_array(collect_list(named_struct(
      'name', name, 'passed_records', passed_records, 'failed_records', failed_records
    ))) AS expectations
  FROM check_totals
  GROUP BY {_KEYS}
),
flows AS (
  SELECT
    lower(config:table::STRING) AS table_name,
    max(config:layer::STRING) AS layer,
    max(parent_full_name) AS dataflow
  FROM {meta}.object_current
  WHERE object_kind = 'flow' AND config:table IS NOT NULL
  GROUP BY lower(config:table::STRING)
),
resolved AS (
  SELECT
    l.*,
    coalesce(t.table_name, l.flow_name) AS table_name,
    coalesce(
      f.dataflow,
      nullif(regexp_extract(coalesce(t.table_name, l.flow_name), '([A-Za-z0-9_]+)_staged`?$', 1), '')
    ) AS dataflow,
    coalesce(
      f.layer,
      CASE WHEN coalesce(t.table_name, l.flow_name) RLIKE '_staged`?$' THEN 'staged' END
    ) AS layer
  FROM loads l
  LEFT JOIN targets t ON t.pipeline_id = l.pipeline_id AND t.flow_name = l.flow_name
  LEFT JOIN flows f ON f.table_name = lower(replace(coalesce(t.table_name, l.flow_name), '`', ''))
)
SELECT
  r.job_name,
  r.pipeline_id,
  r.pipeline_name,
  r.update_id,
  r.flow_name,
  r.table_name,
  r.layer,
  r.dataflow,
  d.owner_kind,
  d.owner,
  r.status,
  r.started_at,
  r.ended_at,
  (unix_millis(r.ended_at) - unix_millis(r.started_at)) / 1000.0 AS duration_seconds,
  r.rows_written,
  r.rows_upserted,
  r.rows_deleted,
  r.rows_dropped,
  CAST(r.backlog_bytes AS BIGINT) AS backlog_bytes,
  CAST(r.backlog_files AS BIGINT) AS backlog_files,
  e.expectations
FROM resolved r
LEFT JOIN expectations e
  ON e.job_name = r.job_name
 AND e.pipeline_id = r.pipeline_id
 AND e.update_id = r.update_id
 AND e.flow_name = r.flow_name
LEFT JOIN {meta}.object_current d
  ON d.object_kind = 'dataflow' AND d.full_name = r.dataflow
"""


def bronze_loads_sql(metrics: str) -> str:
    """One row per bronze table load: rows read (staged) and rows written to bronze and quarantine."""

    return f"""
SELECT
  job_name,
  pipeline_id,
  max(pipeline_name) AS pipeline_name,
  update_id,
  dataflow,
  max(owner_kind) AS owner_kind,
  max(owner) AS owner,
  max(CASE WHEN layer = 'bronze' THEN table_name END) AS bronze_table,
  max(CASE WHEN layer = 'quarantine' THEN table_name END) AS quarantine_table,
  min(started_at) AS started_at,
  max(ended_at) AS ended_at,
  (unix_millis(max(ended_at)) - unix_millis(min(started_at))) / 1000.0 AS duration_seconds,
  max(CASE WHEN layer = 'staged' THEN rows_written END) AS rows_read,
  max(CASE WHEN layer = 'bronze' THEN rows_written END) AS rows_bronze,
  max(CASE WHEN layer = 'quarantine' THEN rows_written END) AS rows_quarantine,
  coalesce(max(CASE WHEN layer = 'bronze' THEN rows_written END), 0)
    + coalesce(max(CASE WHEN layer = 'quarantine' THEN rows_written END), 0) AS rows_processed,
  max(CASE WHEN layer = 'bronze' THEN status END) AS bronze_status,
  max(CASE WHEN layer = 'quarantine' THEN status END) AS quarantine_status,
  max(CASE WHEN layer = 'staged' THEN expectations END) AS expectations
FROM {metrics}
WHERE dataflow IS NOT NULL
  AND layer IN ('staged', 'bronze', 'quarantine')
GROUP BY job_name, pipeline_id, update_id, dataflow
"""


def register(dp, spark, catalog: str) -> None:
    if not _IDENTIFIER.match(catalog):
        raise ValueError(f"{CATALOG_CONF} must name a catalog")
    meta = f"{catalog}.framework_meta"
    names = [row.tableName for row in spark.sql(f"SHOW TABLES IN {meta}").collect()]
    logs = event_logs(names)
    print(f"framework_metrics: {len(logs)} event log(s) in {meta}: {', '.join(logs) or 'none'}")
    if logs:
        dp.create_streaming_table(
            name=EVENTS,
            comment="flow_definition and flow_progress events from every ingestion event log",
            schema=EVENT_SCHEMA,
            cluster_by_auto=True,
        )
        for log, job in logs.items():
            _append(dp, spark, f"{meta}.{log}", job)
        source = EVENTS
    else:
        # No pipeline has published an event log yet: keep the table, with no rows.
        source = no_events()

    @dp.materialized_view(
        name=METRICS,
        comment="Rows, quality checks, backlog, and duration per pipeline update, flow, and table",
        cluster_by_auto=True,
    )
    def _metrics():
        return spark.sql(table_load_metrics_sql(source, meta))

    @dp.materialized_view(
        name=BRONZE_LOADS,
        comment="Rows read and rows written to bronze and quarantine for each table load",
        cluster_by_auto=True,
    )
    def _bronze_loads():
        return spark.sql(bronze_loads_sql(METRICS))


def _append(dp, spark, log: str, job: str) -> None:
    # Never name the flow <job>_event_log: that table already exists in this schema, and Lakeflow
    # treats a flow with the same name as a self-cycle. The "_flow" suffix is what works on the
    # workspace.
    @dp.append_flow(target=EVENTS, name=f"{job}{EVENT_LOG_SUFFIX}_flow")
    def _events():
        return (
            spark.readStream.table(log)
            .where("event_type IN ('flow_definition', 'flow_progress')")
            .selectExpr(*event_columns(job))
        )


def _running_pipeline():
    try:
        from pyspark import pipelines as dp
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    spark = SparkSession.getActiveSession()
    if spark is None:
        return None
    catalog = spark.conf.get(CATALOG_CONF, None)
    return (dp, spark, catalog) if catalog else None


_running = _running_pipeline()
if _running is not None:
    register(*_running)
