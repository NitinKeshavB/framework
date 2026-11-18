"""Job runs and pipeline updates kept in framework_meta.

``job_runs`` and ``job_task_runs`` are append-only event tables (same pattern as
``load_ledger``). ``record_this_run`` and hourly catch-up INSERT; retries may insert again.
``v_job_runs`` / ``v_job_task_runs`` keep the latest ``recorded_at`` per key — no MERGE, so
1000 concurrent NRT writers do not contend on Delta commits to the same keys.

Hourly catch-up prefers one INSERT SELECT over ``system.lakeflow.job_run_timeline`` /
``job_task_run_timeline`` (needs SELECT on ``system.lakeflow``). Without the grant it falls
back to the Jobs API walk. system.lakeflow freshness is typically up to about an hour;
NRT remains ``record_this_run``.

Pipeline updates come from the ``<job>_event_log`` tables under ``framework_events``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

log = logging.getLogger(__name__)

WORKFLOW_TAG = "lakeflow_framework_workflow"
EVENT_LOG_SUFFIX = "_event_log"
# How far back the first pass reads, and how far behind the latest start later passes reread.
FIRST_LOOKBACK = timedelta(days=30)
REREAD = timedelta(days=2)
JOB_RUN_COLUMNS = (
    "job_name",
    "deployed_name",
    "workspace_id",
    "job_id",
    "run_id",
    "started_at",
    "ended_at",
    "status",
    "life_cycle_state",
    "result_state",
    "termination_code",
    "failure_reason",
    "trigger_type",
    "setup_seconds",
    "queue_seconds",
    "execution_seconds",
    "run_seconds",
    "run_page_url",
    "recorded_at",
)
TASK_RUN_COLUMNS = (
    "job_name",
    "workspace_id",
    "job_id",
    "job_run_id",
    "task_run_id",
    "task_key",
    "started_at",
    "ended_at",
    "status",
    "life_cycle_state",
    "result_state",
    "termination_code",
    "failure_reason",
    "recorded_at",
)
PIPELINE_UPDATE_COLUMNS = (
    "job_name",
    "pipeline_id",
    "pipeline_name",
    "update_id",
    "started_at",
    "ended_at",
    "state",
    "status",
    "recorded_at",
)
_TIMESTAMPS = {"started_at", "ended_at", "recorded_at"}
_SECONDS = {"setup_seconds", "queue_seconds", "execution_seconds", "run_seconds"}
_RUNNING = {
    "PENDING",
    "QUEUED",
    "RUNNING",
    "TERMINATING",
    "BLOCKED",
    "WAITING",
    "WAITING_FOR_RETRY",
}
_SKIPPED_RESULTS = {"EXCLUDED", "UPSTREAM_CANCELED", "UPSTREAM_FAILED"}
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_CATALOG = re.compile(r"^[A-Za-z0-9_-]+$")


def run_status(life_cycle_state: str | None, result_state: str | None) -> str:
    """RUNNING, SUCCEEDED, SKIPPED, or FAILED, from a Jobs API run or task state.

    ``SUCCESS_WITH_FAILURES`` (job finished with a failed task) maps to FAILED.
    """

    if life_cycle_state in _RUNNING:
        return "RUNNING"
    if result_state == "SUCCESS":
        return "SUCCEEDED"
    if life_cycle_state == "SKIPPED" or result_state in _SKIPPED_RESULTS:
        return "SKIPPED"
    return "FAILED"


def job_run_rows(
    run: Mapping[str, Any],
    job_name: str,
    deployed_name: str,
    workspace_id: str,
    recorded_at: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """One job_runs row and its job_task_runs rows, from a Jobs API run as a dict."""

    life, result, code = _state(run)
    job_id = _text(run.get("job_id"))
    run_id = _text(run.get("run_id"))
    row = {
        "job_name": job_name,
        "deployed_name": deployed_name,
        "workspace_id": workspace_id,
        "job_id": job_id,
        "run_id": run_id,
        "started_at": _instant(run.get("start_time")),
        "ended_at": _instant(run.get("end_time")),
        "status": run_status(life, result),
        "life_cycle_state": life,
        "result_state": result,
        "termination_code": code,
        "failure_reason": _failure_reason(run),
        "trigger_type": _text(run.get("trigger")),
        "setup_seconds": _seconds(run.get("setup_duration")),
        "queue_seconds": _seconds(run.get("queue_duration")),
        "execution_seconds": _seconds(run.get("execution_duration")),
        "run_seconds": _seconds(run.get("run_duration")),
        "run_page_url": _text(run.get("run_page_url")),
        "recorded_at": recorded_at,
    }
    tasks = []
    for task in run.get("tasks") or []:
        task_life, task_result, task_code = _state(task)
        tasks.append(
            {
                "job_name": job_name,
                "workspace_id": workspace_id,
                "job_id": job_id,
                "job_run_id": run_id,
                "task_run_id": _text(task.get("run_id")),
                "task_key": _text(task.get("task_key")),
                "started_at": _instant(task.get("start_time")),
                "ended_at": _instant(task.get("end_time")),
                "status": run_status(task_life, task_result),
                "life_cycle_state": task_life,
                "result_state": task_result,
                "termination_code": task_code,
                "failure_reason": _failure_reason(task),
                "recorded_at": recorded_at,
            }
        )
    return row, tasks


def record_this_run(
    spark,
    client,
    catalog: str,
    job_name: str,
    run_id: str,
    now: datetime | None = None,
) -> int:
    """Append one Jobs API run (this delivery job) into job_runs and job_task_runs for NRT.

    Insert only — no MERGE. ``v_job_runs`` / ``v_job_task_runs`` keep the latest row per key.
    """

    if not _IDENTIFIER.fullmatch(job_name or ""):
        raise ValueError("job name is required")
    if not str(run_id or "").isdigit():
        raise ValueError("run_id is required")
    meta = _meta(catalog)
    recorded = (now or datetime.now(UTC)).replace(microsecond=0)
    run = client.jobs.get_run(int(run_id)).as_dict()
    settings = (client.jobs.get(int(run.get("job_id"))).as_dict().get("settings")) or {}
    deployed = _text(settings.get("name")) or job_name
    workspace_id = _text(client.get_workspace_id())
    row, tasks = job_run_rows(run, job_name, deployed, workspace_id, recorded)
    _append(spark, f"{meta}.job_runs", [row], JOB_RUN_COLUMNS)
    _append(spark, f"{meta}.job_task_runs", tasks, TASK_RUN_COLUMNS)
    log.info(
        "this run: appended run %s of %s status=%s reason=%s (%d task(s))",
        run_id,
        job_name,
        row.get("status"),
        row.get("failure_reason"),
        len(tasks),
    )
    return 1


def framework_jobs(jobs: Iterable[Mapping[str, Any]], workflows: set[str]) -> dict[str, tuple]:
    """job_id to (job_name, deployed name) for each listed job tagged as a registered workflow."""

    found: dict[str, tuple] = {}
    for job in jobs:
        settings = job.get("settings") or {}
        name = (settings.get("tags") or {}).get(WORKFLOW_TAG)
        if name in workflows:
            found[_text(job.get("job_id"))] = (name, _text(settings.get("name")))
    return found


def record_job_runs(spark, client, catalog: str, now: datetime | None = None) -> int:
    """Append registered workflow runs into job_runs and job_task_runs (catch-up).

    Prefers ``system.lakeflow`` (one INSERT SELECT). Falls back to the Jobs API walk when that
    schema is unavailable. ``client`` is a ``databricks.sdk.WorkspaceClient`` used only on the
    API path. Returns the number of job-run rows appended.
    """

    meta = _meta(catalog)
    recorded = (now or datetime.now(UTC)).replace(microsecond=0)
    workflows = {
        str(row["job_name"])
        for row in _rows(spark.sql(f"SELECT job_name FROM {meta}.v_workflows"))
        if row["job_name"]
    }
    if not workflows:
        log.info("job runs: v_workflows lists no workflow; nothing to read")
        return 0
    log.info("job runs: %d registered workflow(s)", len(workflows))
    since = _read_from(spark, f"{meta}.job_runs", recorded)
    if system_lakeflow_available(spark):
        log.info(
            "job runs: appending from system.lakeflow since %s (NRT remains record_this_run)",
            since.isoformat(),
        )
        return record_job_runs_from_system(spark, catalog, since, recorded)
    log.warning("job runs: system.lakeflow unavailable; falling back to Jobs API walk")
    return record_job_runs_from_api(spark, client, catalog, workflows, since, recorded)


def system_lakeflow_available(spark) -> bool:
    """True when this identity can SELECT system.lakeflow.job_run_timeline."""

    try:
        spark.sql("SELECT 1 FROM system.lakeflow.job_run_timeline LIMIT 1").collect()
        return True
    except Exception as exc:
        log.info("job runs: system.lakeflow probe failed: %s", exc)
        return False


def job_runs_from_system_sql(meta: str, since: datetime, recorded: datetime) -> str:
    """One MERGE source: registered workflows' job runs from system.lakeflow timelines."""

    cutoff = since.strftime("%Y-%m-%d %H:%M:%S")
    stamped = recorded.strftime("%Y-%m-%d %H:%M:%S")
    tag = WORKFLOW_TAG
    return f"""
WITH workflows AS (
  SELECT job_name FROM {meta}.v_workflows
),
jobs AS (
  SELECT workspace_id, job_id, name AS deployed_name, tags['{tag}'] AS job_name
  FROM (
    SELECT
      workspace_id, job_id, name, tags, delete_time, change_time,
      ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.jobs
  )
  WHERE rn = 1 AND delete_time IS NULL
    AND tags['{tag}'] IN (SELECT job_name FROM workflows)
),
runs AS (
  SELECT
    j.job_name,
    j.deployed_name,
    t.workspace_id,
    t.job_id,
    t.run_id,
    min(t.period_start_time) AS started_at,
    max_by(t.period_end_time, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS ended_at,
    max_by(t.result_state, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS result_state,
    max_by(t.termination_code, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS termination_code,
    max_by(t.trigger_type, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS trigger_type,
    max_by(t.setup_duration_seconds, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS setup_seconds,
    max_by(t.queue_duration_seconds, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS queue_seconds,
    max_by(t.execution_duration_seconds, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS execution_seconds,
    max_by(t.run_duration_seconds, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS run_seconds
  FROM system.lakeflow.job_run_timeline t
  INNER JOIN jobs j
    ON t.workspace_id = j.workspace_id AND t.job_id = j.job_id
  WHERE t.run_type = 'JOB_RUN'
    AND t.period_start_time >= TIMESTAMP '{cutoff}'
  GROUP BY j.job_name, j.deployed_name, t.workspace_id, t.job_id, t.run_id
)
SELECT
  job_name,
  deployed_name,
  workspace_id,
  job_id,
  run_id,
  started_at,
  CASE WHEN result_state IS NULL THEN CAST(NULL AS TIMESTAMP) ELSE ended_at END AS ended_at,
  CASE
    WHEN result_state IS NULL THEN 'RUNNING'
    WHEN result_state = 'SUCCEEDED' THEN 'SUCCEEDED'
    WHEN result_state IN ('SKIPPED', 'BLOCKED') THEN 'SKIPPED'
    ELSE 'FAILED'
  END AS status,
  CASE WHEN result_state IS NULL THEN 'RUNNING' ELSE 'TERMINATED' END AS life_cycle_state,
  result_state,
  termination_code,
  CAST(NULL AS STRING) AS failure_reason,
  trigger_type,
  CAST(setup_seconds AS DOUBLE) AS setup_seconds,
  CAST(queue_seconds AS DOUBLE) AS queue_seconds,
  CAST(execution_seconds AS DOUBLE) AS execution_seconds,
  CAST(run_seconds AS DOUBLE) AS run_seconds,
  CAST(NULL AS STRING) AS run_page_url,
  TIMESTAMP '{stamped}' AS recorded_at
FROM runs
"""


def job_task_runs_from_system_sql(meta: str, since: datetime, recorded: datetime) -> str:
    """Task runs for the same registered workflows and window as job_runs_from_system_sql."""

    cutoff = since.strftime("%Y-%m-%d %H:%M:%S")
    stamped = recorded.strftime("%Y-%m-%d %H:%M:%S")
    tag = WORKFLOW_TAG
    return f"""
WITH workflows AS (
  SELECT job_name FROM {meta}.v_workflows
),
jobs AS (
  SELECT workspace_id, job_id, tags['{tag}'] AS job_name
  FROM (
    SELECT
      workspace_id, job_id, tags, delete_time, change_time,
      ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.jobs
  )
  WHERE rn = 1 AND delete_time IS NULL
    AND tags['{tag}'] IN (SELECT job_name FROM workflows)
),
tasks AS (
  SELECT
    j.job_name,
    t.workspace_id,
    t.job_id,
    t.job_run_id,
    t.run_id AS task_run_id,
    t.task_key,
    min(t.period_start_time) AS started_at,
    max_by(t.period_end_time, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS ended_at,
    max_by(t.result_state, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS result_state,
    max_by(t.termination_code, CASE WHEN t.result_state IS NOT NULL THEN 1 ELSE 0 END)
      AS termination_code
  FROM system.lakeflow.job_task_run_timeline t
  INNER JOIN jobs j
    ON t.workspace_id = j.workspace_id AND t.job_id = j.job_id
  WHERE t.period_start_time >= TIMESTAMP '{cutoff}'
  GROUP BY j.job_name, t.workspace_id, t.job_id, t.job_run_id, t.run_id, t.task_key
)
SELECT
  job_name,
  workspace_id,
  job_id,
  job_run_id,
  task_run_id,
  task_key,
  started_at,
  CASE WHEN result_state IS NULL THEN CAST(NULL AS TIMESTAMP) ELSE ended_at END AS ended_at,
  CASE
    WHEN result_state IS NULL THEN 'RUNNING'
    WHEN result_state = 'SUCCEEDED' THEN 'SUCCEEDED'
    WHEN result_state IN ('SKIPPED', 'BLOCKED') THEN 'SKIPPED'
    ELSE 'FAILED'
  END AS status,
  CASE WHEN result_state IS NULL THEN 'RUNNING' ELSE 'TERMINATED' END AS life_cycle_state,
  result_state,
  termination_code,
  CAST(NULL AS STRING) AS failure_reason,
  TIMESTAMP '{stamped}' AS recorded_at
FROM tasks
"""


def record_job_runs_from_system(spark, catalog: str, since: datetime, recorded: datetime) -> int:
    """Append job and task runs from system.lakeflow. Returns job_runs rows inserted."""

    meta = _meta(catalog)
    runs_view, tasks_view = "_framework_system_job_runs", "_framework_system_job_task_runs"
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW {runs_view} AS {job_runs_from_system_sql(meta, since, recorded)}"
    )
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW {tasks_view} AS "
        f"{job_task_runs_from_system_sql(meta, since, recorded)}"
    )
    cols = ", ".join(JOB_RUN_COLUMNS)
    spark.sql(f"INSERT INTO {meta}.job_runs SELECT {cols} FROM {runs_view}")
    task_cols = ", ".join(TASK_RUN_COLUMNS)
    spark.sql(f"INSERT INTO {meta}.job_task_runs SELECT {task_cols} FROM {tasks_view}")
    count = _rows(spark.sql(f"SELECT count(*) AS n FROM {runs_view}"))
    appended = int(count[0]["n"]) if count else 0
    log.info("job runs: system.lakeflow appended %d run(s)", appended)
    return appended


def record_job_runs_from_api(
    spark,
    client,
    catalog: str,
    workflows: set[str],
    since: datetime,
    recorded: datetime,
) -> int:
    """Jobs API walk: list jobs, list_runs per job, get_run for still-RUNNING rows. Appends."""

    meta = _meta(catalog)
    jobs = framework_jobs((job.as_dict() for job in client.jobs.list()), workflows)
    found = {name for name, _deployed in jobs.values()}
    for missing in sorted(workflows - found):
        log.warning(
            "job runs: no job tagged %s=%s is visible to this identity", WORKFLOW_TAG, missing
        )
    log.info("job runs: reading runs that started since %s", since.isoformat())
    workspace_id = _text(client.get_workspace_id())
    runs: dict[str, dict] = {}
    tasks: list[dict] = []
    for job_id, (job_name, deployed) in sorted(jobs.items()):
        listed = client.jobs.list_runs(
            job_id=int(job_id), start_time_from=_millis(since), expand_tasks=True
        )
        count = 0
        for run in listed:
            row, task_rows = job_run_rows(run.as_dict(), job_name, deployed, workspace_id, recorded)
            runs[row["run_id"]] = row
            tasks += task_rows
            count += 1
        log.info("job runs: %s (job %s, %s): %d run(s)", job_name, job_id, deployed, count)
    # Latest status per run — append table may hold older RUNNING rows after a later SUCCEEDED.
    for item in _rows(
        spark.sql(
            f"""
            SELECT job_id, run_id FROM (
              SELECT job_id, run_id, status,
                row_number() OVER (PARTITION BY job_id, run_id ORDER BY recorded_at DESC) AS rn
              FROM {meta}.job_runs
            ) WHERE rn = 1 AND status = 'RUNNING'
            """
        )
    ):
        run_id = _text(item["run_id"])
        job = jobs.get(_text(item["job_id"]))
        if run_id in runs or job is None:
            continue
        log.info("job runs: rereading run %s of %s, still running last pass", run_id, job[0])
        run = client.jobs.get_run(int(run_id)).as_dict()
        row, task_rows = job_run_rows(run, job[0], job[1], workspace_id, recorded)
        runs[run_id] = row
        tasks += task_rows
    _append(spark, f"{meta}.job_runs", list(runs.values()), JOB_RUN_COLUMNS)
    _append(spark, f"{meta}.job_task_runs", tasks, TASK_RUN_COLUMNS)
    statuses = sorted({str(row["status"]) for row in runs.values()})
    for status in statuses:
        count = sum(1 for row in runs.values() if str(row["status"]) == status)
        log.info("job runs: %d %s", count, status)
    log.info("job runs: appended %d run(s) and %d task run(s)", len(runs), len(tasks))
    return len(runs)


def event_log_jobs(table_names: Iterable[str]) -> dict[str, str]:
    """Event log table name to its job, for every <job>_event_log name."""

    logs: dict[str, str] = {}
    for name in sorted(table_names):
        if name.endswith(EVENT_LOG_SUFFIX) and _IDENTIFIER.match(name):
            logs[name] = name[: -len(EVENT_LOG_SUFFIX)]
    return logs


def pipeline_updates_sql(events_table: str, job_name: str, since: datetime) -> str:
    """One row per pipeline update in a single event log (one scan, no UNION ALL fan-in).

    An update with any event since ``since`` is read whole, so its start is not cut off.
    Call once per log — Catalyst plans stay small at hundreds of pipelines.
    """

    if not _IDENTIFIER.match(job_name or ""):
        raise ValueError("job name is required")
    cutoff = since.strftime("%Y-%m-%d %H:%M:%S")
    return f"""
WITH marked AS (
  SELECT
    CAST('{job_name}' AS STRING) AS job_name,
    origin.pipeline_id AS pipeline_id,
    origin.pipeline_name AS pipeline_name,
    origin.update_id AS update_id,
    timestamp AS event_at,
    details:update_progress.state::STRING AS state,
    max(timestamp) OVER (PARTITION BY origin.update_id) AS latest_at
  FROM {events_table}
  WHERE event_type = 'update_progress' AND origin.update_id IS NOT NULL
)
SELECT
  job_name,
  pipeline_id,
  max(pipeline_name) AS pipeline_name,
  update_id,
  min(event_at) AS started_at,
  CASE WHEN max_by(state, event_at) IN ('COMPLETED', 'FAILED', 'CANCELED')
    THEN max(event_at) END AS ended_at,
  max_by(state, event_at) AS state,
  CASE max_by(state, event_at) WHEN 'COMPLETED' THEN 'SUCCEEDED'
    WHEN 'FAILED' THEN 'FAILED' WHEN 'CANCELED' THEN 'CANCELED' ELSE 'RUNNING' END AS status,
  current_timestamp() AS recorded_at
FROM marked
WHERE latest_at >= TIMESTAMP '{cutoff}'
GROUP BY job_name, pipeline_id, update_id
"""


def record_pipeline_updates(spark, catalog: str, now: datetime | None = None) -> int:
    """Merge pipeline updates from each event log into pipeline_updates.

    Event logs live in ``framework_events`` (not ``framework_meta``). Each log is merged on
    its own so the plan never grows with UNION ALL across hundreds of pipelines.
    """

    from lakeflow_framework.metadata import EVENTS_SCHEMA

    meta = _meta(catalog)
    events = f"{catalog}.{EVENTS_SCHEMA}"
    recorded = (now or datetime.now(UTC)).replace(microsecond=0)
    names = [
        str(row.get("tableName") or row.get("table_name") or "")
        for row in _rows(spark.sql(f"SHOW TABLES IN {events}"))
    ]
    names = [name for name in names if name]
    logs = event_log_jobs(names)
    if not logs:
        log.info("pipeline updates: no <job>_event_log table in %s yet", events)
        return 0
    log.info("pipeline updates: event logs %s", ", ".join(sorted(logs)))
    since = _read_from(spark, f"{meta}.pipeline_updates", recorded)
    log.info("pipeline updates: reading updates since %s", since.isoformat())
    merged = 0
    for table_name, job in sorted(logs.items()):
        source = pipeline_updates_sql(f"{events}.{table_name}", job, since)
        spark.sql(
            _merge_sql(
                f"{meta}.pipeline_updates",
                source,
                ("pipeline_id", "update_id"),
                PIPELINE_UPDATE_COLUMNS,
            )
        )
        merged += 1
    log.info("pipeline updates: merged the updates from %d event log(s)", merged)
    return merged


def _read_from(spark, table: str, now: datetime) -> datetime:
    rows = _rows(spark.sql(f"SELECT max(started_at) AS latest FROM {table}"))
    latest = rows[0]["latest"] if rows else None
    if latest is None:
        return now - FIRST_LOOKBACK
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=UTC)
    return latest - REREAD


def _append(spark, table: str, rows: list[dict], columns: tuple) -> None:
    """Insert rows; duplicates are fine — the matching v_* view keeps the latest recorded_at."""

    if not rows:
        return
    from lakeflow_framework.spark_types import require_writer, types_module

    writer = require_writer(spark)
    t = types_module()

    def kind(column: str):
        if t is None:
            return None
        if column in _TIMESTAMPS:
            return t.TimestampType()
        if column in _SECONDS:
            return t.DoubleType()
        return t.StringType()

    schema = (
        t.StructType([t.StructField(column, kind(column), True) for column in columns])
        if t is not None
        else None
    )
    writer.createDataFrame([[row[c] for c in columns] for row in rows], schema).write.mode(
        "append"
    ).saveAsTable(table)


def _merge_sql(table: str, source: str, keys: tuple, columns: tuple) -> str:
    """MERGE helper kept for pipeline_updates (single writer, still upserted)."""

    from lakeflow_framework.sqlnames import merge_sql

    return merge_sql(table, source, keys, columns, dedupe_source=True)


def _state(run: Mapping[str, Any]) -> tuple[str | None, str | None, str | None]:
    state = run.get("state") or {}
    status = run.get("status") or {}
    details = status.get("termination_details") or {}
    life = _text(state.get("life_cycle_state")) or _text(status.get("state"))
    result = _text(state.get("result_state"))
    code = _text(details.get("code"))
    if result is None and code is not None and life == "TERMINATED":
        result = "SUCCESS" if code == "SUCCESS" else code
    return life, result, code


def _failure_reason(run: Mapping[str, Any]) -> str | None:
    """Human-readable why from Jobs API state / termination details."""

    state = run.get("state") or {}
    status = run.get("status") or {}
    details = status.get("termination_details") or {}
    for value in (
        state.get("state_message"),
        details.get("message"),
        details.get("type"),
    ):
        text = _text(value)
        if text:
            return text[:4000]
    return None


def _meta(catalog: str) -> str:
    if not _CATALOG.match(catalog or ""):
        raise ValueError("catalog is required")
    return f"{catalog}.framework_meta"


def _instant(millis: Any) -> datetime | None:
    if not millis:
        return None
    return datetime.fromtimestamp(int(millis) / 1000, tz=UTC)


def _millis(instant: datetime) -> int:
    return int(instant.timestamp() * 1000)


def _seconds(millis: Any) -> float | None:
    return None if millis is None else int(millis) / 1000.0


def _text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(getattr(value, "value", value))


def _rows(frame: Any) -> list[dict[str, Any]]:
    from lakeflow_framework.sqlnames import spark_rows

    return spark_rows(frame)
