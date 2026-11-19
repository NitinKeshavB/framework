"""Log lines the framework jobs write. No Spark."""

from __future__ import annotations

import logging

import pytest

from lakeflow_framework.logs import job_logging, statement_summary, step
from lakeflow_framework.registry import apply_ddl


@pytest.fixture(autouse=True)
def _restore_framework_logger():
    """job_logging() changes the package logger for the whole process. Undo it after each test,
    or caplog (a root-logger handler) stops seeing the package's records."""

    package = logging.getLogger("lakeflow_framework")
    handlers, level, propagate = list(package.handlers), package.level, package.propagate
    yield
    package.handlers[:] = handlers
    package.setLevel(level)
    package.propagate = propagate


def test_apply_ddl_skips_a_column_that_is_already_there_but_not_other_errors() -> None:
    class Spark:
        def __init__(self, failure: str):
            self.failure, self.ran = failure, []

        def sql(self, statement):
            if statement.startswith("SELECT 1 FROM"):
                return []
            self.ran.append(statement)
            if statement.startswith("ALTER"):
                raise RuntimeError(self.failure)
            return []

    text = "ALTER TABLE t ADD COLUMN batch_id STRING AFTER recorded_at;\nCREATE TABLE b (x INT)"
    again = Spark("[FIELD_ALREADY_EXISTS] Cannot add column, because `batch_id` already exists")
    assert apply_ddl(again, "framework_dev", text) == 2
    # The failed ALTER is skipped and the statement after it still runs.
    assert [s for s in again.ran if not s.startswith("INSERT")] == [
        "ALTER TABLE t ADD COLUMN batch_id STRING AFTER recorded_at",
        "CREATE TABLE b (x INT)",
    ]
    with pytest.raises(RuntimeError, match="permission denied"):
        apply_ddl(Spark("permission denied"), "framework_dev", text)


def test_job_logging_adds_one_handler_however_often_it_is_called(capsys) -> None:
    log = job_logging("example")
    job_logging("example")
    log.info("hello")
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    assert lines[0].endswith("INFO lakeflow_framework.jobs.example: hello")


def test_step_logs_the_time_taken_and_the_error_that_ended_it(capsys) -> None:
    log = job_logging("example")
    with step(log, "load"):
        pass
    with pytest.raises(RuntimeError), step(log, "write"):
        raise RuntimeError("no access")
    out = capsys.readouterr().out
    assert "load: started" in out and "load: finished in" in out
    assert "write: failed after" in out and "RuntimeError: no access" in out


def test_a_statement_is_one_line_and_cut_to_the_width() -> None:
    assert statement_summary("SELECT 1\n  FROM t") == "SELECT 1 FROM t"
    summary = statement_summary("SELECT " + "x, " * 100, width=40)
    assert len(summary) == 40 and summary.endswith("...")


def test_apply_ddl_logs_each_statement_and_a_skip(caplog) -> None:
    class Spark:
        def __init__(self, applied: bool):
            self.applied = applied

        def sql(self, statement):
            if statement.startswith("SELECT 1 FROM") and self.applied:
                return [{"1": 1}]
            return []

    with caplog.at_level(logging.INFO, logger="lakeflow_framework"):
        apply_ddl(
            Spark(False), "framework_dev", "CREATE TABLE a (x INT);\nCREATE VIEW b AS SELECT 1"
        )
        apply_ddl(Spark(True), "framework_dev", "CREATE TABLE a (x INT)")
    messages = [record.getMessage() for record in caplog.records]
    assert any(message.startswith("statement 1/2 in ") for message in messages)
    assert any("CREATE VIEW b AS SELECT 1" in message for message in messages)
    assert any("already in ddl_applied" in message for message in messages)
