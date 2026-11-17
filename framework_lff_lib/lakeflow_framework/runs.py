"""Job runs from the Jobs API and pipeline updates from the event logs, kept in framework_meta.

Neither needs grants on the system tables. The Jobs API lists the runs of the jobs the caller
can see, so the identity that runs framework_runs needs CAN_VIEW on the delivery jobs, which
the deploying identity has. Pipeline updates come from the <job>_event_log tables the
ingestion pipelines publish.

Each pass reads from a little before the latest start already recorded, so a run that was
still going last time is read again and its row updated. Rows are merged, never duplicated.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

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
_RUNNING = {"PENDING", "QUEUED", "RUNNING", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"}
_SKIPPED_RESULTS = {"EXCLUDED", "UPSTREAM_CANCELED", "UPSTREAM_FAILED"}
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_CATALOG = re.compile(r"^[A-Za-z0-9_-]+$")


def run_status(life_cycle_state: str | None, result_state: str | None) -> str:
    """RUNNING, SUCCEEDED, SKIPPED, or FAILED, from a Jobs API run or task state."""

    if life_cycle_state in _RUNNING:
        return "RUNNING"
    if result_state == "SUCCESS" or result_state == "SUCCESS_WITH_FAILURES":
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
                "recorded_at": recorded_at,
            }
        )
    return row, tasks


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
    """Merge the runs of every registered workflow's job into job_runs and job_task_runs.

    ``client`` is a ``databricks.sdk.WorkspaceClient``. Returns the number of job runs read.
    """

    meta = _meta(catalog)
    recorded = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    workflows = {
        str(row["job_name"])
        for row in _rows(spark.sql(f"SELECT job_name FROM {meta}.v_workflows"))
        if row["job_name"]
    }
    if not workflows:
        log.info("job runs: v_workflows lists no workflow; nothing to read")
        return 0
    log.info("job runs: %d registered workflow(s)", len(workflows))
    jobs = framework_jobs((job.as_dict() for job in client.jobs.list()), workflows)
    found = {name for name, _deployed in jobs.values()}
    for missing in sorted(workflows - found):
        log.warning(
            "job runs: no job tagged %s=%s is visible to this identity", WORKFLOW_TAG, missing
        )
    since = _read_from(spark, f"{meta}.job_runs", recorded)
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
    # A run that started before the reread window and was still going last pass.
    for item in _rows(
        spark.sql(f"SELECT job_id, run_id FROM {meta}.job_runs WHERE status = 'RUNNING'")
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
    _merge(spark, f"{meta}.job_runs", list(runs.values()), JOB_RUN_COLUMNS, ("job_id", "run_id"))
    _merge(
        spark,
        f"{meta}.job_task_runs",
        tasks,
        TASK_RUN_COLUMNS,
        ("job_run_id", "task_run_id"),
    )
    statuses = sorted({str(row["status"]) for row in runs.values()})
    for status in statuses:
        count = sum(1 for row in runs.values() if str(row["status"]) == status)
        log.info("job runs: %d %s", count, status)
    log.info("job runs: merged %d run(s) and %d task run(s)", len(runs), len(tasks))
    return len(runs)


def event_log_jobs(table_names: Iterable[str]) -> dict[str, str]:
    """Event log table name to its job, for every <job>_event_log name."""

    logs: dict[str, str] = {}
    for name in sorted(table_names):
        if name.endswith(EVENT_LOG_SUFFIX) and _IDENTIFIER.match(name):
            logs[name] = name[: -len(EVENT_LOG_SUFFIX)]
    return logs


def pipeline_updates_sql(meta: str, logs: Mapping[str, str], since: datetime) -> str:
    """One row per pipeline update in every event log, from its update_progress events.

    An update with any event since ``since`` is read whole, so its start is not cut off.
    """

    if not logs:
        raise ValueError("no event logs")
    cutoff = since.strftime("%Y-%m-%d %H:%M:%S")
    reads = [
        f"SELECT '{job}' AS job_name, origin.pipeline_id AS pipeline_id, "
        f"origin.pipeline_name AS pipeline_name, origin.update_id AS update_id, "
        f"timestamp AS event_at, details:update_progress.state::STRING AS state "
        f"FROM {meta}.{log} "
        f"WHERE event_type = 'update_progress' AND origin.update_id IN ("
        f"SELECT origin.update_id FROM {meta}.{log} "
        f"WHERE event_type = 'update_progress' AND timestamp >= TIMESTAMP '{cutoff}')"
        for log, job in logs.items()
        if _IDENTIFIER.match(job)
    ]
    events = " UNION ALL ".join(reads)
    return (
        "SELECT job_name, pipeline_id, max(pipeline_name) AS pipeline_name, update_id, "
        "min(event_at) AS started_at, "
        "CASE WHEN max_by(state, event_at) IN ('COMPLETED', 'FAILED', 'CANCELED') "
        "THEN max(event_at) END AS ended_at, "
        "max_by(state, event_at) AS state, "
        "CASE max_by(state, event_at) WHEN 'COMPLETED' THEN 'SUCCEEDED' "
        "WHEN 'FAILED' THEN 'FAILED' WHEN 'CANCELED' THEN 'CANCELED' ELSE 'RUNNING' END "
        "AS status, "
        "current_timestamp() AS recorded_at "
        f"FROM ({events}) events "
        "WHERE update_id IS NOT NULL "
        "GROUP BY job_name, pipeline_id, update_id"
    )


def record_pipeline_updates(spark, catalog: str, now: datetime | None = None) -> int:
    """Merge every pipeline update in the event logs into pipeline_updates. Returns the logs read."""

    meta = _meta(catalog)
    recorded = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    names = [row["tableName"] for row in _rows(spark.sql(f"SHOW TABLES IN {meta}"))]
    logs = event_log_jobs(names)
    if not logs:
        log.info("pipeline updates: no <job>_event_log table in %s yet", meta)
        return 0
    log.info("pipeline updates: event logs %s", ", ".join(sorted(logs)))
    since = _read_from(spark, f"{meta}.pipeline_updates", recorded)
    log.info("pipeline updates: reading updates since %s", since.isoformat())
    source = pipeline_updates_sql(meta, logs, since)
    spark.sql(
        _merge_sql(
            f"{meta}.pipeline_updates",
            source,
            ("pipeline_id", "update_id"),
            PIPELINE_UPDATE_COLUMNS,
        )
    )
    log.info("pipeline updates: merged the updates from %d event log(s)", len(logs))
    return len(logs)


def _read_from(spark, table: str, now: datetime) -> datetime:
    rows = _rows(spark.sql(f"SELECT max(started_at) AS latest FROM {table}"))
    latest = rows[0]["latest"] if rows else None
    if latest is None:
        return now - FIRST_LOOKBACK
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    return latest - REREAD


def _merge(spark, table: str, rows: list[dict], columns: tuple, keys: tuple) -> None:
    if not rows:
        return
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

    def kind(column: str):
        if column in _TIMESTAMPS:
            return TimestampType()
        if column in _SECONDS:
            return DoubleType()
        return StringType()

    schema = StructType([StructField(column, kind(column), True) for column in columns])
    view = "_framework_" + table.rsplit(".", 1)[-1]
    spark.createDataFrame(
        [[row[c] for c in columns] for row in rows], schema
    ).createOrReplaceTempView(view)
    # A run listed twice in one pass would fail the MERGE, so each key is kept once.
    source = (
        f"SELECT * FROM {view} QUALIFY row_number() OVER "
        f"(PARTITION BY {', '.join(keys)} ORDER BY recorded_at DESC) = 1"
    )
    spark.sql(_merge_sql(table, source, keys, columns))


def _merge_sql(table: str, source: str, keys: tuple, columns: tuple) -> str:
    on = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    assignments = ", ".join(f"t.{column} = s.{column}" for column in columns)
    names = ", ".join(columns)
    values = ", ".join(f"s.{column}" for column in columns)
    return (
        f"MERGE INTO {table} t USING ({source}) s ON {on} "
        f"WHEN MATCHED THEN UPDATE SET {assignments} "
        f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
    )


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


def _meta(catalog: str) -> str:
    if not _CATALOG.match(catalog or ""):
        raise ValueError("catalog is required")
    return f"{catalog}.framework_meta"


def _instant(millis: Any) -> datetime | None:
    if not millis:
        return None
    return datetime.fromtimestamp(int(millis) / 1000, tz=timezone.utc)


def _millis(instant: datetime) -> int:
    return int(instant.timestamp() * 1000)


def _seconds(millis: Any) -> float | None:
    return None if millis is None else int(millis) / 1000.0


def _text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(getattr(value, "value", value))


def _rows(frame: Any) -> list[dict[str, Any]]:
    collected = frame.collect() if hasattr(frame, "collect") else list(frame)
    return [row if isinstance(row, dict) else row.asDict() for row in collected]
