"""Job runs from the Jobs API and pipeline updates from the event logs, without system tables."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from lakeflow_framework.runs import (
    event_log_jobs,
    framework_jobs,
    job_run_rows,
    pipeline_updates_sql,
    record_job_runs,
    record_pipeline_updates,
    run_status,
)

NOW = datetime(2026, 11, 17, 6, 0, tzinfo=timezone.utc)
STARTED = int(datetime(2026, 11, 17, 5, 0, tzinfo=timezone.utc).timestamp() * 1000)


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
        ("TERMINATED", "SUCCESS", "SUCCEEDED"),
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
    assert row["run_seconds"] == 90.0 and row["setup_seconds"] == 1.0
    assert row["started_at"] == datetime(2026, 11, 17, 5, 0, tzinfo=timezone.utc)
    assert row["trigger_type"] == "PERIODIC"
    (task,) = tasks
    assert (task["job_run_id"], task["task_run_id"], task["task_key"]) == (
        "11",
        "12",
        "refresh_pipeline",
    )
    assert task["status"] == "SUCCEEDED"


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


class _Frame:
    def __init__(self, rows=None):
        self.rows = rows or []

    def collect(self):
        return self.rows

    def createOrReplaceTempView(self, _name):
        pass


class _Spark:
    def __init__(self, answers):
        self.answers = answers
        self.ran: list[str] = []
        self.frames: list[list] = []

    def sql(self, statement):
        self.ran.append(statement)
        for needle, rows in self.answers.items():
            if needle in statement:
                return _Frame(rows)
        return _Frame()

    def createDataFrame(self, rows, _schema):
        self.frames.append(rows)
        return _Frame()


def test_runs_are_read_from_before_the_latest_start_and_merged_on_their_keys():
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
    expected = latest.replace(tzinfo=timezone.utc) - timedelta(days=2)
    assert listed["start_time_from"] == int(expected.timestamp() * 1000)
    merges = [s for s in spark.ran if s.startswith("MERGE INTO")]
    assert merges[0].startswith("MERGE INTO framework_dev.framework_meta.job_runs t")
    assert "ON t.job_id = s.job_id AND t.run_id = s.run_id" in merges[0]
    assert "ON t.job_run_id = s.job_run_id AND t.task_run_id = s.task_run_id" in merges[1]
    assert "QUALIFY row_number()" in merges[0]


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


def test_pipeline_updates_come_from_update_progress_events_in_each_event_log():
    assert event_log_jobs(["abs_daily_event_log", "object_current", "x-y_event_log"]) == {
        "abs_daily_event_log": "abs_daily"
    }
    sql = pipeline_updates_sql("c.framework_meta", {"abs_daily_event_log": "abs_daily"}, NOW)
    assert "FROM c.framework_meta.abs_daily_event_log" in sql
    assert "event_type = 'update_progress'" in sql
    # An update with an event in the window is read whole, so its start is not cut off.
    assert "origin.update_id IN (SELECT origin.update_id" in sql
    assert "TIMESTAMP '2026-11-17 06:00:00'" in sql
    assert "WHEN 'COMPLETED' THEN 'SUCCEEDED'" in sql
    with pytest.raises(ValueError):
        pipeline_updates_sql("c.framework_meta", {}, NOW)


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
    assert "ON t.pipeline_id = s.pipeline_id AND t.update_id = s.update_id" in merge
    assert record_pipeline_updates(_Spark({"SHOW TABLES": []}), "framework_dev", now=NOW) == 0
