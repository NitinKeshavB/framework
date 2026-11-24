"""Append-only central load metrics. No Spark cluster."""

from __future__ import annotations

import pytest

from lakeflow_framework.load_metrics import (
    failed_loads_stub_sql,
    latest_loads_sql,
    missing_event_log_loads_sql,
    record_loads,
)


class _Frame:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


def _load_row(
    dataflow: str,
    *,
    update_id: str = "u1",
    rows_read=None,
    rows_written=None,
    rows_upserted=None,
    rows_deleted=None,
    rows_quarantine=None,
    rows_processed=None,
    target_status: str = "COMPLETED",
    failure_reason=None,
) -> dict:
    return {
        "dataflow": dataflow,
        "update_id": update_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_upserted": rows_upserted,
        "rows_deleted": rows_deleted,
        "rows_quarantine": rows_quarantine,
        "rows_processed": rows_processed,
        "target_status": target_status,
        "failure_reason": failure_reason,
    }


def _recording_spark(
    statements: list[str],
    *,
    count: int,
    load_rows: list[dict] | None = None,
    event_logs: list[str] | None = None,
):
    tables = event_logs if event_logs is not None else ["abs_daily_event_log"]

    class Spark:
        def sql(self, statement: str):
            statements.append(statement)
            upper = statement.upper()
            if "SHOW TABLES" in upper:
                return _Frame([{"tableName": name} for name in tables])
            if "count(*)" in statement:
                return _Frame([{"n": count}])
            if (
                load_rows is not None
                and "FROM _framework_loads" in statement
                and "INSERT" not in upper
            ):
                return _Frame(load_rows)
            return _Frame([])

    return Spark()


def test_latest_loads_sql_reads_only_the_newest_update() -> None:
    sql = latest_loads_sql(
        "c.framework_meta.abs_daily_event_log", "c.framework_meta", "abs_daily", layer="bronze"
    )
    assert "update_progress" in sql
    assert "QUALIFY row_number() OVER (ORDER BY timestamp DESC) = 1" in sql
    assert "rows_read" in sql and "rows_written" in sql and "failure_reason" in sql
    assert "num_upserted_rows" in sql and "num_deleted_rows" in sql
    assert "CAST('bronze' AS STRING) AS layer" in sql
    assert "MERGE" not in sql.upper()


def test_latest_loads_sql_is_driven_by_registered_dataflows() -> None:
    sql = latest_loads_sql(
        "c.framework_meta.abs_daily_event_log",
        "c.framework_meta",
        "abs_daily",
        layer="bronze",
        job_run_id="99",
    )
    assert "registered AS" in sql
    assert "FROM registered reg" in sql
    assert "LEFT JOIN from_events e ON e.dataflow = reg.dataflow" in sql
    assert "object_kind = 'dataflow'" in sql
    assert "parent_full_name = 'abs_daily'" in sql
    assert "coalesce(e.target_status, 'FAILED')" in sql
    assert "no flow_progress for the latest update" in sql
    assert "CAST('99' AS STRING) AS job_run_id" in sql
    assert "rows_upserted" in sql and "rows_deleted" in sql


def test_failed_loads_stub_sql_lists_registered_dataflows() -> None:
    sql = failed_loads_stub_sql("c.framework_meta", "abs_daily", layer="bronze")
    assert "object_kind = 'dataflow'" in sql
    assert "parent_full_name = 'abs_daily'" in sql
    assert "target_table" in sql


def test_missing_event_log_loads_sql_marks_failed() -> None:
    sql = missing_event_log_loads_sql(
        "c.framework_meta", "abs_daily", layer="bronze", job_run_id="7"
    )
    assert "FAILED" in sql
    assert "event log table not found" in sql
    assert "CAST('7' AS STRING) AS job_run_id" in sql


def test_record_loads_inserts_progress_then_never_merges() -> None:
    statements: list[str] = []
    spark = _recording_spark(
        statements,
        count=1,
        load_rows=[
            _load_row(
                "abs_accounting",
                rows_read=100,
                rows_written=90,
                rows_quarantine=10,
                rows_processed=100,
            )
        ],
    )
    assert record_loads(spark, "aal_dev_fdh", "abs_daily", layer="bronze", job_run_id="11") == 1
    insert = next(statement for statement in statements if statement.upper().startswith("INSERT"))
    assert "INSERT INTO aal_dev_fdh.framework_meta.load_ledger" in insert
    assert "MERGE" not in insert.upper()
    assert any("SHOW TABLES" in statement.upper() for statement in statements)
    assert any("registered AS" in statement for statement in statements)
    assert any("CAST('11' AS STRING) AS job_run_id" in statement for statement in statements)
    assert (
        sum(1 for s in statements if "CREATE OR REPLACE TEMPORARY VIEW _framework_loads" in s) == 1
    )


def test_record_loads_marks_missing_dataflows_failed_in_one_pass() -> None:
    """Partial delivery failure: registry rows always appear; no second stub query."""

    statements: list[str] = []
    spark = _recording_spark(
        statements,
        count=2,
        load_rows=[
            _load_row(
                "table_a",
                rows_read=10,
                rows_written=10,
                rows_quarantine=0,
                rows_processed=10,
            ),
            _load_row(
                "table_b",
                target_status="FAILED",
                failure_reason="no flow_progress",
            ),
        ],
    )
    assert record_loads(spark, "aal_dev_fdh", "abs_daily", layer="bronze") == 2
    assert (
        sum(1 for s in statements if "CREATE OR REPLACE TEMPORARY VIEW _framework_loads" in s) == 1
    )
    assert any("LEFT JOIN from_events" in s for s in statements)
    insert = next(statement for statement in statements if statement.upper().startswith("INSERT"))
    assert "load_ledger" in insert
    with pytest.raises(ValueError):
        record_loads(spark, "aal_dev_fdh", "abs_daily", layer="raw")


def test_record_loads_falls_back_when_event_log_missing() -> None:
    statements: list[str] = []
    spark = _recording_spark(
        statements,
        count=1,
        event_logs=[],
        load_rows=[
            _load_row(
                "abs_accounting",
                update_id="failed-1",
                target_status="FAILED",
                failure_reason="event log table not found",
            )
        ],
    )
    assert record_loads(spark, "aal_dev_fdh", "abs_daily", layer="bronze", job_run_id="5") == 1
    assert any("event log table not found" in statement for statement in statements)
    assert not any("LEFT JOIN from_events" in statement for statement in statements)


def test_record_loads_returns_zero_when_no_registered_dataflows() -> None:
    statements: list[str] = []
    spark = _recording_spark(statements, count=0)
    assert record_loads(spark, "aal_dev_fdh", "abs_daily", layer="bronze") == 0
    assert not any(statement.upper().startswith("INSERT") for statement in statements)


def test_registered_dataflows_sql_carries_what_the_file_check_needs() -> None:
    from lakeflow_framework.load_metrics import _registered_dataflows_sql

    sql = _registered_dataflows_sql("c.framework_meta", "abs_daily", "bronze")
    for needle in ("source_path", "path_glob", "expect_file", "rules_json"):
        assert needle in sql


def test_catchup_dataflows_get_expect_file_and_fail_rules_from_the_registry() -> None:
    from lakeflow_framework import load_catchup

    rules = (
        '[{"name": "a", "action": "fail"}, {"name": "b", "action": "fail", "enabled": false}, '
        '{"name": "c", "action": "drop"}]'
    )
    assert load_catchup._fail_rules(rules) == ("a",)
    assert load_catchup._fail_rules(None) == ()
    assert load_catchup._fail_rules("not json") == ()
