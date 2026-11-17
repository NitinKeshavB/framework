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
    assert "'abs_daily' AS job_name" in sql
    assert "WHERE r.dataflow IS NOT NULL" in sql
    assert "GROUP BY r.pipeline_id, r.update_id, r.dataflow" in sql
    assert "MERGE" not in sql.upper()


def test_record_bronze_loads_appends_and_never_merges() -> None:
    writes: list[tuple[str, str]] = []

    class Writer:
        def mode(self, mode: str):
            assert mode == "append"
            return self

        def saveAsTable(self, table: str):
            writes.append(("append", table))

    class Frame:
        def collect(self):
            return [
                {
                    "job_name": "abs_daily",
                    "pipeline_id": "p1",
                    "pipeline_name": "abs_daily",
                    "update_id": "u1",
                    "dataflow": "abs_accounting",
                    "owner_kind": "source_system",
                    "owner": "abs",
                    "bronze_table": "aal_dev_fdh.abs_bronze.finance_abs",
                    "quarantine_table": "aal_dev_fdh.abs_bronze.finance_abs_qrntn",
                    "started_at": None,
                    "ended_at": None,
                    "duration_seconds": 1.0,
                    "rows_read": 100,
                    "rows_bronze": 90,
                    "rows_quarantine": 10,
                    "rows_processed": 100,
                    "bronze_status": "COMPLETED",
                    "quarantine_status": "COMPLETED",
                    "expectations": "[]",
                }
            ]

        @property
        def write(self):
            return Writer()

    class Spark:
        def sql(self, _statement):
            return Frame()

        def createDataFrame(self, rows):
            assert rows and rows[0]["rows_quarantine"] == 10
            assert "recorded_at" in rows[0]
            return Frame()

    assert record_bronze_loads(Spark(), "aal_dev_fdh", "abs_daily") == 1
    assert writes == [("append", "aal_dev_fdh.framework_meta.bronze_load_ledger")]
    with pytest.raises(ValueError):
        record_bronze_loads(Spark(), "bad catalog", "abs_daily")
