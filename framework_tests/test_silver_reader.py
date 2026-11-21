"""How a flow checkpoint reader consumes only what is new from a source."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from lakeflow_framework.silver_reader import choose_mode, read_new

BRONZE = "aal.abs_bronze.gl_abs"
SILVER = "aal.silver.rides"
DATA = ["ride_id", "_src_row_hash", "_batch_id", "_ingest_timestamp"]


class Frame:
    """Records what is done to it. Nothing runs, so each test reads the trace."""

    def __init__(self, spark: Spark, columns: list[str] | None = None) -> None:
        self.spark = spark
        self.columns = columns if columns is not None else list(spark.columns)

    def _next(self, *step: Any) -> Frame:
        self.spark.trace.append(step)
        return Frame(self.spark, self.columns)

    def option(self, key: str, value: Any) -> Frame:
        return self._next("option", key, value)

    def format(self, name: str) -> Frame:
        return self._next("format", name)

    def table(self, name: str) -> Frame:
        return self._next("table", name)

    def where(self, condition: str) -> Frame:
        return self._next("where", condition)

    def select(self, *names: str) -> Frame:
        return self._next("select", *names)

    def join(self, other: Frame, on: list[str], how: str) -> Frame:
        return self._next("join", tuple(on), how)

    def selectExpr(self, *exprs: str) -> Frame:  # noqa: N802 (Spark's name)
        return self._next("selectExpr", *exprs)

    def collect(self) -> list[dict]:
        return [{"newest": self.spark.newest}]


class Spark:
    def __init__(
        self,
        *,
        feed: bool = True,
        version: int = 9,
        progress: dict | None = None,
        columns: list[str] | None = None,
        newest: datetime | None = None,
        operation: str = "STREAMING UPDATE",
    ) -> None:
        self.feed, self.version, self.progress = feed, version, progress
        self.operation = operation
        self.columns = columns if columns is not None else DATA
        self.newest = newest
        self.trace: list[tuple] = []
        self.statements: list[str] = []
        self.inserts: list[str] = []
        self.read = Frame(self)

    def sql(self, statement: str):
        self.statements.append(statement)
        rows: list[dict] = []
        if statement.startswith("SHOW TBLPROPERTIES"):
            value = "true" if self.feed else "false"
            rows = [{"key": "delta.enableChangeDataFeed", "value": value}]
        elif statement.startswith("DESCRIBE HISTORY"):
            rows = [{"version": self.version, "operation": self.operation}]
        elif "FROM aal.framework_meta.v_flow_checkpoint" in statement:
            if self.progress:
                rows = [
                    {
                        "watermark_ts": self.progress.get("last_ingest_timestamp"),
                        "watermark_id": (
                            str(self.progress["last_commit_version"])
                            if self.progress.get("last_commit_version") is not None
                            else None
                        ),
                    }
                ]
        elif statement.startswith("INSERT INTO"):
            self.inserts.append(statement)
        return type("Result", (), {"collect": lambda _self: rows})()

    def steps(self, kind: str) -> list[tuple]:
        return [step for step in self.trace if step[0] == kind]


def test_the_first_read_takes_the_table_as_it_is_and_commit_records_the_version() -> None:
    spark = Spark(feed=True, version=9)
    changes = read_new(spark, BRONZE, SILVER)
    assert changes.mode == "change_feed"
    assert ("option", "versionAsOf", 9) in spark.trace
    assert spark.steps("selectExpr")[-1] == (
        "selectExpr",
        "*",
        "'insert' AS _change_type",
        "9 AS _commit_version",
    )
    assert not spark.inserts
    changes.commit()
    (insert,) = spark.inserts
    assert "aal.framework_meta.flow_checkpoint" in insert
    assert "'SUCCEEDED'" in insert
    assert f"'{SILVER}'" in insert and f"'{BRONZE}'" in insert
    assert "'9'" in insert  # watermark_id


def test_later_reads_ask_the_change_feed_for_only_the_commits_not_yet_read() -> None:
    spark = Spark(
        feed=True, version=9, progress={"last_ingest_timestamp": None, "last_commit_version": 6}
    )
    changes = read_new(spark, BRONZE, SILVER)
    options = {(key, value) for _step, key, value in spark.steps("option")}
    assert {("readChangeFeed", "true"), ("startingVersion", 7), ("endingVersion", 9)} <= options
    assert ("format", "delta") in spark.trace
    assert ("where", "_change_type != 'update_preimage'") in spark.trace
    last = spark.steps("selectExpr")[-1]
    assert last[1] == "* EXCEPT (_change_type)"
    assert "'update_postimage' THEN 'update'" in last[2]
    assert not spark.steps("join")
    changes.commit()
    assert "'9'" in spark.inserts[0]


def test_a_rebuilt_table_stops_the_read_with_what_to_do_instead_of_a_delta_error() -> None:
    progress = {"last_ingest_timestamp": None, "last_commit_version": 4}
    refreshed = Spark(feed=True, version=9, progress=progress, operation="DLT REFRESH")
    with pytest.raises(
        ValueError, match="rebuilt since the reader last consumed it.*DLT REFRESH at version 9"
    ) as error:
        read_new(refreshed, BRONZE, SILVER)
    assert "flow_checkpoint" in str(error.value)
    assert ("DESCRIBE HISTORY aal.abs_bronze.gl_abs LIMIT 5") in refreshed.statements
    fine = Spark(
        feed=True,
        version=9,
        progress={**progress, "last_commit_version": 9},
        operation="DLT REFRESH",
    )
    assert read_new(fine, BRONZE, SILVER).mode == "change_feed"
    assert read_new(Spark(feed=True, progress=progress), BRONZE, SILVER).mode == "change_feed"


def test_an_update_that_changed_nothing_is_left_out_when_keys_are_given() -> None:
    spark = Spark(
        feed=True, version=9, progress={"last_ingest_timestamp": None, "last_commit_version": 6}
    )
    read_new(spark, BRONZE, SILVER, keys=["ride_id"])
    same = ("ride_id", "_commit_version", "_src_row_hash")
    assert ("where", "_change_type = 'update_preimage'") in spark.trace
    assert ("select", *same) in spark.trace
    assert ("join", same, "left_anti") in spark.trace


def test_nothing_new_gives_no_rows_and_does_not_ask_the_feed_for_a_later_version() -> None:
    spark = Spark(
        feed=True, version=9, progress={"last_ingest_timestamp": None, "last_commit_version": 9}
    )
    changes = read_new(spark, BRONZE, SILVER)
    assert ("where", "1 = 0") in spark.trace
    assert not [key for _s, key, _v in spark.steps("option") if key == "readChangeFeed"]
    changes.commit()
    assert "'9'" in spark.inserts[0]


def test_without_a_change_feed_the_first_read_is_everything_and_commit_saves_the_newest_load_time() -> (
    None
):
    newest = datetime(2026, 11, 19, 10, 30, 5, 250000)
    spark = Spark(feed=False, version=4, newest=newest)
    changes = read_new(spark, BRONZE, SILVER)
    assert changes.mode == "ingest_timestamp"
    assert ("option", "versionAsOf", 4) in spark.trace
    assert not spark.steps("where")
    assert spark.steps("selectExpr")[-1] == ("selectExpr", "*", "'upsert' AS _change_type")
    changes.commit()
    (insert,) = spark.inserts
    assert "TIMESTAMP '2026-11-19 10:30:05.250000'" in insert
    assert "CAST(NULL AS STRING)" in insert  # watermark_id


def test_without_a_change_feed_later_reads_take_only_rows_loaded_after_the_last_read() -> None:
    mark = datetime(2026, 11, 19, 9, 0, 0)
    spark = Spark(
        feed=False, version=7, progress={"last_ingest_timestamp": mark, "last_commit_version": None}
    )
    read_new(spark, BRONZE, SILVER)
    assert ("where", "_ingest_timestamp > TIMESTAMP '2026-11-19 09:00:00.000000'") in spark.trace


def test_a_flagged_row_is_a_delete_and_a_run_with_no_new_rows_saves_nothing() -> None:
    spark = Spark(feed=False, columns=[*DATA, "_is_deleted"], newest=None)
    changes = read_new(spark, BRONZE, SILVER)
    assert (
        "CASE WHEN coalesce(_is_deleted, false) THEN 'delete' ELSE 'upsert' END"
        in (spark.steps("selectExpr")[-1][-1])
    )
    changes.commit()
    assert not spark.inserts


def test_an_scd2_table_cannot_be_read_by_load_time_because_closures_leave_no_trace() -> None:
    columns = [*DATA, "__START_AT", "__END_AT"]
    with pytest.raises(ValueError, match="closed versions only the change feed shows"):
        read_new(Spark(feed=False, columns=columns), BRONZE, SILVER)
    assert read_new(Spark(feed=True, columns=columns), BRONZE, SILVER).mode == "change_feed"


def test_a_mode_that_the_table_cannot_give_is_refused() -> None:
    with pytest.raises(ValueError, match="change feed is not on"):
        read_new(Spark(feed=False), BRONZE, SILVER, mode="change_feed")
    with pytest.raises(ValueError, match="no _ingest_timestamp column"):
        read_new(Spark(feed=False, columns=["ride_id"]), BRONZE, SILVER)
    assert read_new(Spark(feed=True), BRONZE, SILVER, mode="ingest_timestamp").mode == (
        "ingest_timestamp"
    )
    with pytest.raises(ValueError, match="mode must be one of"):
        read_new(Spark(), BRONZE, SILVER, mode="whatever")


def test_table_names_are_checked_because_they_go_into_sql() -> None:
    for bad in ("gl_abs", "a.b", "a.b.c.d", "a.b.c; DROP TABLE x", "a.b-c.d"):
        with pytest.raises(ValueError, match="catalog.schema.table"):
            read_new(Spark(), bad, SILVER)
        with pytest.raises(ValueError, match="catalog.schema.table"):
            read_new(Spark(), BRONZE, bad)


def test_choose_mode_follows_the_table() -> None:
    assert choose_mode("auto", True, DATA) == "change_feed"
    assert choose_mode("auto", False, DATA) == "ingest_timestamp"
    assert choose_mode("ingest_timestamp", True, DATA) == "ingest_timestamp"
