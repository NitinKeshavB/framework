"""Log lines the framework jobs write. No Spark."""

from __future__ import annotations

import logging

import pytest

from lakeflow_framework.logs import job_logging, statement_summary, step
from lakeflow_framework.registry import apply_ddl


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
