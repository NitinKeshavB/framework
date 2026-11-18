"""Append-only central load metrics. No Spark cluster."""

from __future__ import annotations

import pytest

from lakeflow_framework.load_metrics import (
    failed_loads_stub_sql,
    latest_loads_sql,
    record_loads,
)


def test_latest_loads_sql_reads_only_the_newest_update() -> None:
    sql = latest_loads_sql(
        "c.framework_meta.abs_daily_event_log", "c.framework_meta", "abs_daily", layer="bronze"
    )
    assert "update_progress" in sql
    assert "QUALIFY row_number() OVER (ORDER BY timestamp DESC) = 1" in sql
    assert "rows_read" in sql and "rows_written" in sql and "failure_reason" in sql
    assert "CAST('bronze' AS STRING) AS layer" in sql
    assert "MERGE" not in sql.upper()


def test_failed_loads_stub_sql_marks_failed() -> None:
    sql = failed_loads_stub_sql("c.framework_meta", "abs_daily", layer="bronze")
    assert "FAILED" in sql
    assert "object_kind = 'dataflow'" in sql
    assert "parent_full_name = 'abs_daily'" in sql
    assert "failure_reason" in sql


def test_record_loads_inserts_progress_then_never_merges() -> None:
    statements: list[str] = []

    class Frame:
        def __init__(self, rows):
            self._rows = rows

        def collect(self):
            return self._rows

    class Spark:
        def sql(self, statement: str):
            statements.append(statement)
            if "count(*)" in statement:
                return Frame([{"n": 1}])
            if "FROM _framework_loads" in statement and "INSERT" not in statement.upper():
                return Frame(
                    [
                        {
                            "dataflow": "abs_accounting",
                            "update_id": "u1",
                            "rows_read": 100,
                            "rows_written": 90,
                            "rows_quarantine": 10,
                            "rows_processed": 100,
                            "target_status": "COMPLETED",
                            "failure_reason": None,
                        }
                    ]
                )
            return Frame([])

    assert record_loads(Spark(), "aal_dev_fdh", "abs_daily", layer="bronze") == 1
    insert = next(statement for statement in statements if statement.upper().startswith("INSERT"))
    assert "INSERT INTO aal_dev_fdh.framework_meta.load_ledger" in insert
    assert "MERGE" not in insert.upper()
    assert not any("failed-" in statement for statement in statements)


def test_record_loads_inserts_failed_stub_when_no_progress() -> None:
    statements: list[str] = []
    counts = iter([{"n": 0}, {"n": 1}])

    class Frame:
        def __init__(self, rows):
            self._rows = rows

        def collect(self):
            return self._rows

    class Spark:
        def sql(self, statement: str):
            statements.append(statement)
            if "count(*)" in statement:
                return Frame([next(counts)])
            if "FROM _framework_loads" in statement and "INSERT" not in statement.upper():
                return Frame(
                    [
                        {
                            "dataflow": "abs_accounting",
                            "update_id": "failed-1",
                            "rows_read": None,
                            "rows_written": None,
                            "rows_quarantine": None,
                            "rows_processed": None,
                            "target_status": "FAILED",
                            "failure_reason": "no flow_progress",
                        }
                    ]
                )
            return Frame([])

    assert record_loads(Spark(), "aal_dev_fdh", "abs_daily", layer="bronze") == 1
    assert any("object_kind = 'dataflow'" in statement for statement in statements)
    insert = next(statement for statement in statements if statement.upper().startswith("INSERT"))
    assert "load_ledger" in insert
    with pytest.raises(ValueError):
        record_loads(Spark(), "aal_dev_fdh", "abs_daily", layer="raw")
