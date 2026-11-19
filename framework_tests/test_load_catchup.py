"""A load whose pipeline did not finish writing its own ledger row is written from its event log."""

from __future__ import annotations

import json

from lakeflow_framework.load_catchup import catch_up_loads
from lakeflow_framework.load_metrics import LEDGER_COLUMNS
from test_pipeline_metrics import BRONZE, QUARANTINE, loaded

UPDATE_ID = "0f8fad5b-d9cb-469f-a165-70867728950e"
COLUMNS = [*LEDGER_COLUMNS, "batch_id"]


class Frame:
    def __init__(self, spark, data):
        self.spark, self.data = spark, data

    @property
    def write(self):
        return self

    def mode(self, _mode):
        return self

    def saveAsTable(self, name):  # noqa: N802 (Spark's name)
        self.spark.saved[name] = self.data


class Spark:
    def __init__(
        self,
        *,
        missing=(UPDATE_ID,),
        batches=("batch-1",),
        recorded=(),
        registered=True,
        later=False,
    ):
        self.missing, self.batches, self.recorded, self.registered = (
            missing,
            batches,
            recorded,
            registered,
        )
        self.later = later  # a second table registered after the update ran
        self.statements: list[str] = []
        self.saved: dict[str, list] = {}

    def sql(self, statement: str):
        rows = self._answer(statement)
        return type("Result", (), {"collect": lambda _self: rows})()

    def _answer(self, statement: str) -> list[dict]:
        self.statements.append(statement)
        if statement.startswith("SHOW TABLES IN"):
            return [{"tableName": "abs_daily_event_log"}, {"tableName": "something_else"}]
        if "asset_registry_current" in statement:
            row = {
                "dataflow": "abs_accounting",
                "owner_kind": "source_system",
                "owner": "abs",
                "target_table": BRONZE,
                "quarantine_table": QUARANTINE,
                "pipeline_id": "abs_accounting",
            }
            newer = {
                **row,
                "dataflow": "abs_gl",
                "pipeline_id": "abs_gl",
                "target_table": "aal.abs_bronze.gl_abs",
                "quarantine_table": "aal.abs_bronze.gl_abs_qrntn",
            }
            if not self.registered:
                return []
            return [row, newer] if self.later else [row]
        if "NOT EXISTS" in statement:
            return [{"update_id": update_id} for update_id in self.missing]
        if "to_json(named_struct" in statement:
            return [{"event": json.dumps(event)} for event in loaded(100, 90, 10)]
        if "SELECT DISTINCT _batch_id" in statement:
            return [{"batch": batch} for batch in self.batches]
        if "framework_meta.load_ledger WHERE job_name" in statement:
            return [{"dataflow": name} for name in self.recorded]
        raise AssertionError(f"unexpected statement: {statement}")

    def createDataFrame(self, data, _schema):  # noqa: N802 (Spark's name)
        return Frame(self, data)


def _ledger(spark: Spark) -> list[dict]:
    return [dict(zip(COLUMNS, record, strict=True)) for record in spark.saved[LEDGER]]


LEDGER = "aal_dev_fdh.framework_meta.load_ledger"


def test_an_update_with_no_ledger_row_is_replayed_and_written() -> None:
    spark = Spark()
    assert catch_up_loads(spark, "aal_dev_fdh") == 1
    (row,) = _ledger(spark)
    assert (row["job_name"], row["dataflow"], row["update_id"]) == (
        "abs_daily",
        "abs_accounting",
        "u-1",
    )
    # The same numbers the hook would have written for these events.
    assert (row["rows_read"], row["rows_written"], row["rows_quarantine"]) == (100, 90, 10)
    assert row["target_status"] == "COMPLETED" and row["failure_reason"] is None
    assert row["batch_id"] == "batch-1"
    assert len(spark.saved["aal_dev_fdh.framework_meta.load_metric"]) == 6
    # Only the pipeline's own event log was read, and only the last week of it.
    lookup = next(s for s in spark.statements if "NOT EXISTS" in s)
    assert "aal_dev_fdh.framework_events.abs_daily_event_log" in lookup
    assert "INTERVAL 7 DAYS" in lookup and "l.job_name = 'abs_daily'" in lookup


def test_a_table_added_after_an_update_is_not_blamed_for_it() -> None:
    spark = Spark(later=True)
    assert catch_up_loads(spark, "aal_dev_fdh") == 1
    # The registry now has two tables, but this update only ever ran abs_accounting.
    assert [row["dataflow"] for row in _ledger(spark)] == ["abs_accounting"]


def test_nothing_missing_writes_nothing() -> None:
    spark = Spark(missing=())
    assert catch_up_loads(spark, "aal_dev_fdh") == 0
    assert not spark.saved
    assert not any("to_json" in statement for statement in spark.statements)


def test_a_dataflow_already_recorded_is_not_written_again() -> None:
    spark = Spark(recorded=("abs_accounting",))
    assert catch_up_loads(spark, "aal_dev_fdh") == 0
    assert not spark.saved


def test_the_batch_id_is_left_empty_when_it_cannot_be_told_apart() -> None:
    for batches in ((), ("a", "b")):
        spark = Spark(batches=batches)
        assert catch_up_loads(spark, "aal_dev_fdh") == 1
        assert _ledger(spark)[0]["batch_id"] is None


def test_a_job_with_nothing_registered_and_an_unsafe_id_are_skipped() -> None:
    assert catch_up_loads(Spark(registered=False), "aal_dev_fdh") == 0
    spark = Spark(missing=("x'; DROP TABLE y; --",))
    assert catch_up_loads(spark, "aal_dev_fdh") == 0
    assert not any(
        "DROP TABLE" in statement and "to_json" in statement for statement in spark.statements
    )
