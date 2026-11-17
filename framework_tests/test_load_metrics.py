"""Append-only bronze load metrics. No Spark cluster."""

from __future__ import annotations

import pytest

from lakeflow_framework.load_metrics import latest_bronze_loads_sql, record_bronze_loads


def test_latest_bronze_loads_sql_reads_only_the_newest_update() -> None:
    sql = latest_bronze_loads_sql(
        "c.framework_meta.abs_daily_event_log", "c.framework_meta", "abs_daily"
    )
    assert "update_progress" in sql
    assert "QUALIFY row_number() OVER (ORDER BY timestamp DESC) = 1" in sql
    assert "rows_read" in sql and "rows_quarantine" in sql and "rows_processed" in sql
    assert "'abs_daily' AS job_name" in sql or "CAST('abs_daily' AS STRING)" in sql
    assert "WHERE r.dataflow IS NOT NULL" in sql
    assert "GROUP BY r.pipeline_id, r.update_id, r.dataflow" in sql
    assert "CAST(" in sql and "AS BIGINT)" in sql and "AS TIMESTAMP)" in sql
    assert "max_by(status, event_at) FILTER (WHERE status IS NOT NULL)" in sql
    assert "MERGE" not in sql.upper()


def test_record_bronze_loads_inserts_select_and_never_merges() -> None:
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
            if "FROM _framework_bronze_loads" in statement and "INSERT" not in statement.upper():
                return Frame(
                    [
                        {
                            "dataflow": "abs_accounting",
                            "update_id": "u1",
                            "rows_read": 100,
                            "rows_bronze": 90,
                            "rows_quarantine": 10,
                            "rows_processed": 100,
                        }
                    ]
                )
            return Frame([])

    assert record_bronze_loads(Spark(), "aal_dev_fdh", "abs_daily") == 1
    assert any(statement.startswith("CREATE OR REPLACE TEMPORARY VIEW") for statement in statements)
    insert = next(statement for statement in statements if statement.upper().startswith("INSERT"))
    assert "INSERT INTO aal_dev_fdh.framework_meta.bronze_load_ledger" in insert
    assert "current_timestamp() AS recorded_at" in insert
    assert "MERGE" not in insert.upper()
    assert not any("createDataFrame" in statement for statement in statements)
    with pytest.raises(ValueError):
        record_bronze_loads(Spark(), "bad catalog", "abs_daily")
