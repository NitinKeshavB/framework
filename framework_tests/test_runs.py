"""Job runs (system.lakeflow + Jobs API fallback) and pipeline updates from event logs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from lakeflow_framework.runs import (
    event_log_jobs,
    framework_jobs,
    job_run_rows,
    job_runs_from_system_sql,
    job_task_runs_from_system_sql,
    pipeline_updates_sql,
    record_job_runs,
    record_pipeline_updates,
    run_status,
)

NOW = datetime(2026, 11, 17, 6, 0, tzinfo=UTC)
STARTED = int(datetime(2026, 11, 17, 5, 0, tzinfo=UTC).timestamp() * 1000)


def _run(run_id=11, life="TERMINATED", result="SUCCESS", **extra):
    run = {
        "job_id": 7,
        "run_id": run_id,
        "start_time": STARTED,
        "end_time": STARTED + 90_000,
        "state": {"life_cycle_state": life, "result_state": result},
        "status": {
            "termination_details": {
                "code": "SUCCESS" if result == "SUCCESS" else "RUN_EXECUTION_ERROR"
            }
        },
        "trigger": "PERIODIC",
        "setup_duration": 1000,
        "queue_duration": 0,
        "execution_duration": 85_000,
        "run_duration": 90_000,
        "run_page_url": "https://example/run/11",
        "tasks": [
            {
                "run_id": 12,
                "task_key": "refresh_pipeline",
                "start_time": STARTED,
                "end_time": STARTED + 89_000,
                "state": {"life_cycle_state": life, "result_state": result},
            }
        ],
    }
    run.update(extra)
    return run


@pytest.mark.parametrize(
    ("life", "result", "status"),
    [
        ("RUNNING", None, "RUNNING"),
        ("QUEUED", None, "RUNNING"),
        ("WAITING", None, "RUNNING"),
        ("WAITING_FOR_RETRY", None, "RUNNING"),
        ("TERMINATED", "SUCCESS", "SUCCEEDED"),
        ("TERMINATED", "SUCCESS_WITH_FAILURES", "FAILED"),
        ("TERMINATED", "FAILED", "FAILED"),
        ("TERMINATED", "TIMEDOUT", "FAILED"),
        ("SKIPPED", None, "SKIPPED"),
        ("TERMINATED", "EXCLUDED", "SKIPPED"),
        ("INTERNAL_ERROR", None, "FAILED"),
    ],
)
def test_a_run_state_maps_to_one_status(life, result, status):
    assert run_status(life, result) == status


def test_a_run_becomes_one_row_and_a_row_per_task():
    row, tasks = job_run_rows(_run(), "abs_daily", "[dev me] abs_daily", "123", NOW)
    assert (row["job_id"], row["run_id"], row["status"]) == ("7", "11", "SUCCEEDED")
    assert row["termination_code"] == "SUCCESS"
    assert row["failure_reason"] is None
    assert row["run_seconds"] == 90.0 and row["setup_seconds"] == 1.0
    assert row["started_at"] == datetime(2026, 11, 17, 5, 0, tzinfo=UTC)
    assert row["trigger_type"] == "PERIODIC"
    (task,) = tasks
    assert (task["job_run_id"], task["task_run_id"], task["task_key"]) == (
        "11",
        "12",
        "refresh_pipeline",
    )
    assert task["status"] == "SUCCEEDED"
    assert task["failure_reason"] is None


def test_failure_reason_comes_from_state_message():
    run = _run(result="FAILED")
    run["state"]["state_message"] = "Pipeline update failed: SCHEMA_MISMATCH"
    run["tasks"][0]["state"] = {
        "life_cycle_state": "TERMINATED",
        "result_state": "FAILED",
        "state_message": "boom",
    }
    row, tasks = job_run_rows(run, "abs_daily", "abs_daily", "123", NOW)
    assert row["status"] == "FAILED"
    assert row["failure_reason"] == "Pipeline update failed: SCHEMA_MISMATCH"
    assert tasks[0]["failure_reason"] == "boom"


def test_a_running_run_has_no_end_and_the_newer_status_shape_is_read():
    run = _run(life=None, result=None, end_time=0)
    run["state"] = {}
    run["status"] = {"state": "RUNNING"}
    row, _ = job_run_rows(run, "abs_daily", "abs_daily", "123", NOW)
    assert row["status"] == "RUNNING" and row["ended_at"] is None
    done = _run(result=None)
    done["state"] = {"life_cycle_state": "TERMINATED"}
    done["status"] = {"state": "TERMINATED", "termination_details": {"code": "SUCCESS"}}
    assert job_run_rows(done, "abs_daily", "abs_daily", "123", NOW)[0]["status"] == "SUCCEEDED"


def test_only_jobs_tagged_as_a_registered_workflow_are_read():
    jobs = [
        {
            "job_id": 7,
            "settings": {
                "name": "[dev me] abs_daily",
                "tags": {"lakeflow_framework_workflow": "abs_daily"},
            },
        },
        {"job_id": 8, "settings": {"name": "other", "tags": {"team": "x"}}},
        {"job_id": 9, "settings": {"name": "old", "tags": {"lakeflow_framework_workflow": "gone"}}},
        {"job_id": 10, "settings": {"name": "untagged"}},
    ]
    assert framework_jobs(jobs, {"abs_daily"}) == {"7": ("abs_daily", "[dev me] abs_daily")}


class _Item:
    def __init__(self, data):
        self.data = data

    def as_dict(self):
        return self.data


class _Jobs:
    def __init__(self, runs, running=None):
        self.runs = runs
        self.running = running or {}
        self.listed: list[dict] = []

    def list(self):
        return [
            _Item(
                {
                    "job_id": 7,
                    "settings": {
                        "name": "abs_daily",
                        "tags": {"lakeflow_framework_workflow": "abs_daily"},
                    },
                }
            )
        ]

    def list_runs(self, **kwargs):
        self.listed.append(kwargs)
        return [_Item(run) for run in self.runs]

    def get_run(self, run_id):
        return _Item(self.running[run_id])


class _Client:
    def __init__(self, jobs):
        self.jobs = jobs

    def get_workspace_id(self):
        return 123


class _Writer:
    def __init__(self, frame):
        self._frame = frame

    def mode(self, _mode):
        return self

    def saveAsTable(self, name):
        if self._frame._spark is not None:
            self._frame._spark.appended.append((name, self._frame.rows))


class _Frame:
    def __init__(self, rows=None, spark=None):
        self.rows = rows or []
        self._spark = spark
        self.write = _Writer(self)

    def collect(self):
        return self.rows

    def createOrReplaceTempView(self, _name):
        pass


class _Spark:
    def __init__(self, answers, *, system_ok=False):
        self.answers = answers
        self.system_ok = system_ok
        self.ran: list[str] = []
        self.frames: list[list] = []
        self.appended: list[tuple[str, list]] = []

    def sql(self, statement):
        self.ran.append(statement)
        if "FROM system.lakeflow.job_run_timeline LIMIT 1" in statement:
            if not self.system_ok:
                raise RuntimeError("PERMISSION_DENIED: system.lakeflow")
            return _Frame([{"1": 1}], self)
        for needle, rows in self.answers.items():
            if needle in statement:
                return _Frame(rows, self)
        return _Frame(spark=self)

    def createDataFrame(self, rows, _schema):
        self.frames.append(rows)
        return _Frame(rows, self)


def test_runs_are_read_from_before_the_latest_start_and_appended():
    latest = datetime(2026, 11, 16, 5, 0)
    spark = _Spark(
        {
            "v_workflows": [{"job_name": "abs_daily"}],
            "max(started_at)": [{"latest": latest}],
            "status = 'RUNNING'": [{"job_id": "7", "run_id": "5"}],
        }
    )
    still_going = _run(run_id=5)
    jobs = _Jobs([_run()], running={5: still_going})
    assert record_job_runs(spark, _Client(jobs), "framework_dev", now=NOW) == 2
    (listed,) = jobs.listed
    assert listed["job_id"] == 7 and listed["expand_tasks"] is True
    expected = latest.replace(tzinfo=UTC) - timedelta(days=2)
    assert listed["start_time_from"] == int(expected.timestamp() * 1000)
    assert not [s for s in spark.ran if s.startswith("MERGE INTO")]
    tables = [name for name, _rows in spark.appended]
    assert tables == [
        "framework_dev.framework_meta.job_runs",
        "framework_dev.framework_meta.job_task_runs",
    ]
    assert "PARTITION BY job_id, run_id ORDER BY recorded_at DESC" in "\n".join(spark.ran)


def test_the_first_pass_reads_thirty_days_and_nothing_registered_reads_nothing():
    spark = _Spark(
        {"v_workflows": [{"job_name": "abs_daily"}], "max(started_at)": [{"latest": None}]}
    )
    jobs = _Jobs([])
    assert record_job_runs(spark, _Client(jobs), "framework_dev", now=NOW) == 0
    assert jobs.listed[0]["start_time_from"] == int((NOW - timedelta(days=30)).timestamp() * 1000)
    assert not [s for s in spark.ran if s.startswith("MERGE")]
    assert record_job_runs(_Spark({}), _Client(_Jobs([])), "framework_dev", now=NOW) == 0
    with pytest.raises(ValueError):
        record_job_runs(_Spark({}), _Client(_Jobs([])), "bad catalog")


def test_system_lakeflow_sql_merges_registered_workflows_in_one_plan():
    since = NOW - timedelta(days=2)
    runs = job_runs_from_system_sql("framework_dev.framework_meta", since, NOW)
    tasks = job_task_runs_from_system_sql("framework_dev.framework_meta", since, NOW)
    assert "system.lakeflow.job_run_timeline" in runs
    assert "system.lakeflow.job_task_run_timeline" in tasks
    assert "tags['lakeflow_framework_workflow']" in runs
    assert "FROM framework_dev.framework_meta.v_workflows" in runs
    assert "run_type = 'JOB_RUN'" in runs
    assert "WHEN result_state = 'SUCCEEDED' THEN 'SUCCEEDED'" in runs
    spark = _Spark(
        {
            "v_workflows": [{"job_name": "abs_daily"}],
            "max(started_at)": [{"latest": since}],
            "count(*)": [{"n": 3}],
        },
        system_ok=True,
    )
    jobs = _Jobs([])
    assert record_job_runs(spark, _Client(jobs), "framework_dev", now=NOW) == 3
    assert jobs.listed == []
    assert not [s for s in spark.ran if s.startswith("MERGE INTO")]
    inserts = [s for s in spark.ran if s.startswith("INSERT INTO")]
    assert inserts[0].startswith("INSERT INTO framework_dev.framework_meta.job_runs")
    assert inserts[1].startswith("INSERT INTO framework_dev.framework_meta.job_task_runs")
    assert any("CREATE OR REPLACE TEMP VIEW _framework_system_job_runs" in s for s in spark.ran)


def test_pipeline_updates_come_from_update_progress_events_in_each_event_log():
    assert event_log_jobs(["abs_daily_event_log", "asset_registry_current", "x-y_event_log"]) == {
        "abs_daily_event_log": "abs_daily"
    }
    sql = pipeline_updates_sql("c.framework_events.abs_daily_event_log", "abs_daily", NOW)
    assert "FROM c.framework_events.abs_daily_event_log" in sql
    assert "event_type = 'update_progress'" in sql
    # One scan: window latest_at, no correlated IN and no UNION ALL fan-in.
    assert "max(timestamp) OVER (PARTITION BY origin.update_id)" in sql
    assert "latest_at >= TIMESTAMP '2026-11-17 06:00:00'" in sql
    assert "UNION ALL" not in sql
    assert "WHEN 'COMPLETED' THEN 'SUCCEEDED'" in sql
    with pytest.raises(ValueError):
        pipeline_updates_sql("c.framework_events.abs_daily_event_log", "bad-job", NOW)


def test_pipeline_updates_merge_on_pipeline_and_update():
    spark = _Spark(
        {
            "SHOW TABLES": [{"tableName": "abs_daily_event_log"}, {"tableName": "job_runs"}],
            "max(started_at)": [{"latest": None}],
        }
    )
    assert record_pipeline_updates(spark, "framework_dev", now=NOW) == 1
    (merge,) = [s for s in spark.ran if s.startswith("MERGE INTO")]
    assert merge.startswith("MERGE INTO framework_dev.framework_meta.pipeline_updates t")
    assert "framework_dev.framework_events.abs_daily_event_log" in merge
    assert "ON t.pipeline_id = s.pipeline_id AND t.update_id = s.update_id" in merge
    assert record_pipeline_updates(_Spark({"SHOW TABLES": []}), "framework_dev", now=NOW) == 0
