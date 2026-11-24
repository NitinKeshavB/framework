"""A run of many files: one ledger row for the table, one row per file, a blocked file fails the load."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lakeflow_framework.load_metrics import LEDGER_COLUMNS
from lakeflow_framework.pipeline_metrics import (
    DETAIL_COLUMNS,
    DETAIL_SCHEMA,
    apply_files,
    file_records,
    ledger_records,
    metric_records,
    write_rows,
)
from test_pipeline_metrics import (
    BRONZE,
    QUARANTINE,
    STAGED,
    _Spark,
    dataflow,
    loaded,
    progress,
    recorder,
    run,
    update_state,
)

AT = datetime(2026, 11, 19, tzinfo=UTC)


def _file(name: str, *, read: int, loaded_rows: int, **extra) -> dict:
    status = extra.pop("status", "PROCESSED")
    dropped = int(extra.pop("dropped", 0))
    return {
        "detail_kind": "file",
        "src_file_path": f"/Volumes/c/s/v/{name}",
        "src_file_name": name,
        "status": status,
        "rows_read": read,
        "rows_inserted": loaded_rows,
        "rows_quarantined": extra.pop("quarantined", 0),
        "rows_dropped": dropped,
        "failed_rules": extra.pop("failed_rules", ""),
        "blocking_rules": extra.pop("blocking_rules", ""),
    }


def hundred_files_three_bad() -> list[dict]:
    files = [_file(f"ok_{n:03}.csv", read=10, loaded_rows=10) for n in range(97)]
    files += [
        _file(
            f"bad_{n}.csv",
            read=10,
            loaded_rows=0,
            quarantined=2,
            dropped=8,
            status="FAILED",
            failed_rules="amount_positive",
            blocking_rules="amount_positive",
        )
        for n in range(3)
    ]
    return files


def row_for(files: list[dict], *, read: int, written: int, quarantine: int) -> dict:
    (row,) = run(recorder(dataflow()), loaded(read, written, quarantine))
    row["batch_id"] = "batch-1"
    return apply_files(row, files)


def test_three_bad_files_in_a_hundred_give_one_failed_load_that_names_them() -> None:
    row = row_for(hundred_files_three_bad(), read=1000, written=970, quarantine=6)
    assert (row["files_read"], row["files_failed"]) == (100, 3)
    assert row["rows_dropped"] == 24  # the good rows of the three blocked files
    assert row["target_status"] == "FAILED"
    assert "3 of 100 file(s) blocked by fail rules" in row["failure_reason"]
    assert "bad_0.csv (amount_positive)" in row["failure_reason"]


def test_every_file_is_written_with_its_own_counts_and_the_batch() -> None:
    row = row_for(hundred_files_three_bad(), read=1000, written=970, quarantine=6)
    records = file_records([row], AT)
    assert len(records) == 100
    first = dict(zip(DETAIL_COLUMNS, records[0], strict=True))
    assert first["src_file_name"] == "ok_000.csv"
    assert (first["rows_read"], first["rows_inserted"], first["rows_dropped"]) == (10, 10, 0)
    bad = dict(zip(DETAIL_COLUMNS, records[-1], strict=True))
    # A blocked file loads nothing: its bad rows are in quarantine and its good rows are dropped.
    assert bad["status"] == "FAILED" and bad["rows_inserted"] == 0 and bad["rows_dropped"] == 8
    assert bad["rows_quarantined"] == 2
    assert bad["error_message"] == "blocked by fail rule(s): amount_positive"
    assert first["error_message"] is None
    assert len(DETAIL_SCHEMA.split(", ")) == len(DETAIL_COLUMNS)


def test_a_run_with_only_clean_files_stays_completed() -> None:
    files = [_file("a.csv", read=5, loaded_rows=5), _file("b.csv", read=5, loaded_rows=5)]
    row = row_for(files, read=10, written=10, quarantine=0)
    assert row["files_failed"] == 0 and row["target_status"] == "COMPLETED"
    assert row["failure_reason"] is None


def test_a_warned_file_is_not_a_failure() -> None:
    files = [_file("a.csv", read=5, loaded_rows=4, quarantined=1, status="WARNING")]
    row = row_for(files, read=5, written=4, quarantine=1)
    assert row["files_failed"] == 0 and row["failure_reason"] is None


def test_the_counts_are_numbers_even_when_nothing_was_read() -> None:
    row = row_for([], read=0, written=0, quarantine=0)
    record = dict(zip(LEDGER_COLUMNS, ledger_records([row], AT)[0], strict=True))
    assert (record["files_read"], record["files_failed"]) == (0, 0)
    assert metric_records([row], AT) == []


def test_write_rows_adds_the_per_file_rows_to_the_ledger_detail() -> None:
    spark = _Spark()
    spark.files = [_file_count_row()]
    (row,) = run(recorder(dataflow(file_load=True)), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    assert write_rows(spark, "aal_dev_fdh", [row]) == 1
    saved = spark.saved["aal_dev_fdh.framework_meta.ledger_detail"]
    assert saved.save_mode == "append" and len(saved.data) == 1
    # Counted from bronze and quarantine themselves: no per-pipeline table is read.
    (query,) = [q for q in spark.queries if "_batch_id = 'batch-1'" in q]
    assert f"FROM {BRONZE} " in query and f"FROM {QUARANTINE} " in query
    assert "_files" not in query


def _file_count_row() -> dict:
    return {
        "_src_file_path": "/Volumes/c/s/v/a.csv",
        "_src_file_name": "a.csv",
        "rows_loaded": 5,
        "rows_quarantined": 0,
        "failed_rules": "",
        "blocking_rules": "",
    }


CHECK = "aal.framework_meta.abs_accounting_blocked_check"
FILES = "aal.framework_meta.abs_accounting_files"


def blocked_run(check_status: str = "FAILED", update: str = "FAILED") -> list[dict]:
    """A good file of 5 rows and a blocked file of 80,000 rows, 3 of them bad.

    The staged flow reads 80,005. Bronze gets the 5 good rows, quarantine the 3 bad ones, and the
    other 79,997 rows of the blocked file are read and kept nowhere.
    """

    return [
        update_state("RUNNING", 0),
        progress(STAGED, "RUNNING", 1, metrics={"num_output_rows": 80005}),
        progress(STAGED, "COMPLETED", 2),
        progress(BRONZE, "RUNNING", 3, metrics={"num_output_rows": 5}),
        progress(BRONZE, "COMPLETED", 4),
        progress(QUARANTINE, "RUNNING", 3, metrics={"num_output_rows": 3}),
        progress(QUARANTINE, "COMPLETED", 4),
        progress(FILES, "COMPLETED", 4),
        progress(CHECK, check_status, 5),
        update_state(update, 9),
    ]


def blocked_dataflow():
    return dataflow(
        file_load=True,
        fail_rules=("amt",),
        blocked_check="abs_accounting_blocked_check",
        files=FILES,
    )


def files_dataset_rows() -> list[dict]:
    """What the table's per-file dataset holds for that run (see ``files_sql``)."""

    common = {"failed_rules": "", "blocking_rules": "", "rows_dropped": 0, "rows_blocked": 0}
    return [
        {
            **common,
            "_src_file_path": "/Volumes/c/s/v/good.csv",
            "_src_file_name": "good.csv",
            "file_status": "PROCESSED",
            "rows_read": 5,
            "rows_loaded": 5,
            "rows_quarantined": 0,
        },
        {
            **common,
            "_src_file_path": "/Volumes/c/s/v/bad.csv",
            "_src_file_name": "bad.csv",
            "file_status": "FAILED",
            "rows_read": 80000,
            "rows_loaded": 0,
            "rows_quarantined": 3,
            "rows_blocked": 79997,
            "failed_rules": "amt",
            "blocking_rules": "amt",
        },
    ]


def test_a_blocked_file_reads_everything_it_held_and_shows_what_was_dropped() -> None:
    spark = _Spark()
    spark.files = files_dataset_rows()
    (row,) = run(recorder(blocked_dataflow()), blocked_run())
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row])
    good, bad = row["_files"]
    assert (good["status"], good["rows_read"], good["rows_inserted"]) == ("PROCESSED", 5, 5)
    # 80,000 read: 3 quarantined, the other 79,997 dropped, none loaded, and the file is FAILED
    assert (bad["status"], bad["rows_read"], bad["rows_inserted"]) == ("FAILED", 80000, 0)
    assert (bad["rows_quarantined"], bad["rows_dropped"]) == (3, 79997)
    assert bad["rows_read"] == bad["rows_inserted"] + bad["rows_quarantined"] + bad["rows_dropped"]
    assert bad["error_message"] == "blocked by fail rule(s): amt"
    # the load row agrees with its files
    assert (row["rows_read"], row["rows_inserted"], row["rows_quarantined"]) == (80005, 5, 3)
    assert row["rows_dropped"] == 79997
    assert row["target_status"] == "FAILED" and (row["files_read"], row["files_failed"]) == (2, 1)
    assert (
        "1 of 2 file(s) blocked by fail rules, none of their rows loaded" in row["failure_reason"]
    )
    detail = spark.saved["aal_dev_fdh.framework_meta.ledger_detail"].data
    by_name = {record[DETAIL_COLUMNS.index("src_file_name")]: record for record in detail}
    assert by_name["bad.csv"][DETAIL_COLUMNS.index("rows_read")] == 80000
    assert by_name["bad.csv"][DETAIL_COLUMNS.index("status")] == "FAILED"
    assert by_name["good.csv"][DETAIL_COLUMNS.index("status")] == "PROCESSED"
    (query,) = [q for q in spark.queries if "abs_accounting_files" in q]
    assert "_batch_id = 'batch-1'" in query


def test_the_per_file_counts_fall_back_to_bronze_and_quarantine_if_the_dataset_cannot_be_read() -> (
    None
):
    class NoFiles(_Spark):
        def sql(self, statement: str):
            if "abs_accounting_files" in statement:
                raise RuntimeError("table not found")
            return super().sql(statement)

    spark = NoFiles()
    spark.files = [
        {
            **_file_count_row(),
            "_src_file_path": "/Volumes/c/s/v/bad.csv",
            "_src_file_name": "bad.csv",
            "rows_loaded": 0,
            "rows_quarantined": 3,
            "failed_rules": "amt",
            "blocking_rules": "amt",
        }
    ]
    (row,) = run(recorder(blocked_dataflow()), blocked_run())
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row])  # the ledger is still written
    (bad,) = row["_files"]
    assert (bad["status"], bad["rows_quarantined"]) == ("FAILED", 3)
    assert row["target_status"] == "FAILED" and row["rows_dropped"] == 79997


class _Earlier(_Spark):
    """A ledger that already holds a load of one of the files, as after a full refresh."""

    def __init__(self, earlier: list[str]) -> None:
        super().__init__()
        self.earlier = earlier

    def sql(self, statement: str):
        if "SELECT DISTINCT src_file_path" in statement:
            self.queries.append(statement)
            found = [{"src_file_path": path} for path in self.earlier]
            return type("R", (), {"collect": lambda _s: found})()
        return super().sql(statement)


def test_a_run_that_reads_a_file_again_is_recorded_as_a_backfill() -> None:
    spark = _Earlier(["/Volumes/c/s/v/a.csv"])
    spark.files = [_file_count_row()]
    (row,) = run(recorder(dataflow(file_load=True)), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row])
    assert row["load_kind"] == "backfill"
    ledger = dict(
        zip(
            LEDGER_COLUMNS,
            spark.saved["aal_dev_fdh.framework_meta.load_ledger"].data[0],
            strict=True,
        )
    )
    assert ledger["load_kind"] == "backfill"
    (detail,) = spark.saved["aal_dev_fdh.framework_meta.ledger_detail"].data
    assert detail[DETAIL_COLUMNS.index("load_kind")] == "backfill"
    (query,) = [q for q in spark.queries if "SELECT DISTINCT src_file_path" in q]
    assert "update_id <> " in query  # an earlier update, not this one


def test_a_run_of_new_files_stays_regular() -> None:
    spark = _Earlier([])
    spark.files = [_file_count_row()]
    (row,) = run(recorder(dataflow(file_load=True)), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row])
    assert row["load_kind"] == "regular"
    (detail,) = spark.saved["aal_dev_fdh.framework_meta.ledger_detail"].data
    assert detail[DETAIL_COLUMNS.index("load_kind")] == "regular"


def test_a_catch_up_replay_keeps_its_own_kind_even_when_the_files_were_seen() -> None:
    spark = _Earlier(["/Volumes/c/s/v/a.csv"])
    spark.files = [_file_count_row()]
    (row,) = run(recorder(dataflow(file_load=True)), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row], replay=True)
    assert row["load_kind"] == "catchup"


def test_a_file_that_lost_rows_to_a_drop_rule_shows_them_in_ledger_detail() -> None:
    """The load row said 3 read, 1 inserted, 2 dropped, and the file row said 1 read, 0 dropped."""

    spark = _Spark()
    spark.files = [
        {
            "_src_file_path": "/Volumes/c/s/v/drops.csv",
            "_src_file_name": "drops.csv",
            "file_status": "WARNING",
            "rows_read": 3,
            "rows_loaded": 1,
            "rows_quarantined": 0,
            "rows_dropped": 2,
            "rows_blocked": 0,
            "failed_rules": "amt",
            "blocking_rules": "",
        }
    ]
    flow = dataflow(file_load=True, files=FILES)  # a table that drops rows: no check, no blocking
    events = [
        update_state("RUNNING", 0),
        progress(STAGED, "RUNNING", 1, metrics={"num_output_rows": 3}),
        progress(STAGED, "COMPLETED", 2),
        progress(BRONZE, "RUNNING", 3, metrics={"num_output_rows": 1}),
        progress(BRONZE, "COMPLETED", 4),
        progress(QUARANTINE, "RUNNING", 3, metrics={"num_output_rows": 0}),
        progress(QUARANTINE, "COMPLETED", 4),
        progress(FILES, "COMPLETED", 4),
        update_state("COMPLETED", 9),
    ]
    (row,) = run(recorder(flow), events)
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row])
    (item,) = row["_files"]
    assert (item["rows_read"], item["rows_inserted"], item["rows_dropped"]) == (3, 1, 2)
    assert item["status"] == "WARNING" and row["target_status"] == "COMPLETED"
    # the file agrees with the load
    assert (row["rows_read"], row["rows_inserted"], row["rows_dropped"]) == (3, 1, 2)
    (detail,) = spark.saved["aal_dev_fdh.framework_meta.ledger_detail"].data
    assert detail[DETAIL_COLUMNS.index("rows_dropped")] == 2
    assert detail[DETAIL_COLUMNS.index("rows_read")] == 3


def test_the_recorder_waits_for_the_blocked_file_check_before_it_decides() -> None:
    rec = recorder(blocked_dataflow())
    events = blocked_run()
    # every load flow and the per-file dataset are done, but the check that fails the update is not
    assert all(rec.handle(event) is None for event in events[:8])
    rows = rec.handle(events[8])  # the check fails, so the update did
    assert rows is not None and "FAILED" in rows[0]["failure_reason"]
    # the flows that finished still count: nothing is read as zero because the update failed
    counts = (rows[0]["rows_read"], rows[0]["rows_inserted"], rows[0]["rows_quarantined"])
    assert counts == (80005, 5, 3)
    assert rows[0]["rows_dropped"] == 79997


def test_a_fail_rule_hit_in_the_event_log_fails_a_table_whose_bad_rows_are_kept_nowhere() -> None:
    events = loaded(10, 8, 0)
    events.insert(
        3,
        progress(
            STAGED,
            "COMPLETED",
            2,
            data_quality={
                "expectations": [{"name": "amt", "passed_records": 8, "failed_records": 2}]
            },
        ),
    )
    (row,) = run(recorder(dataflow(quarantine="", fail_rules=("amt",))), events)
    apply_files(row, [])
    assert row["target_status"] == "FAILED"
    assert "amt (2)" in row["failure_reason"]


def test_ledger_and_detail_are_written_together() -> None:
    spark = _Spark()
    spark.files = [_file_count_row()]
    (row,) = run(recorder(dataflow(file_load=True)), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row])
    assert {name.rsplit(".", 1)[-1] for name in spark.saved} == {
        "ledger_detail",
        "load_ledger",
    }


def test_a_failing_write_is_raised_after_the_others_have_finished() -> None:
    class Broken(_Spark):
        def createDataFrame(self, data, schema):  # noqa: N802 (Spark's name)
            frame = super().createDataFrame(data, schema)
            if "files_failed" in schema:  # the ledger
                frame.saveAsTable = lambda _name: (_ for _ in ()).throw(RuntimeError("no MODIFY"))
            return frame

    spark = Broken()
    spark.files = [_file_count_row()]
    (row,) = run(recorder(dataflow(file_load=True)), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    with pytest.raises(RuntimeError, match="no MODIFY"):
        write_rows(spark, "aal_dev_fdh", [row])
    assert "aal_dev_fdh.framework_meta.ledger_detail" in spark.saved


def test_a_replay_does_not_write_files_the_cut_off_update_already_wrote() -> None:
    class Replay(_Spark):
        def sql(self, statement: str):
            if "FROM aal_dev_fdh.framework_meta.ledger_detail" in statement:

                class _Rows:
                    def collect(self):
                        return [{"src_file_path": "/Volumes/c/s/v/a.csv"}]

                return _Rows()
            return super().sql(statement)

    spark = Replay()
    spark.files = [_file_count_row()]
    (row,) = run(recorder(dataflow(file_load=True)), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row], replay=True)
    assert "aal_dev_fdh.framework_meta.ledger_detail" not in spark.saved
    assert "aal_dev_fdh.framework_meta.load_ledger" in spark.saved
    from lakeflow_framework.load_metrics import LEDGER_COLUMNS

    written = dict(
        zip(
            LEDGER_COLUMNS,
            spark.saved["aal_dev_fdh.framework_meta.load_ledger"].data[0],
            strict=True,
        )
    )
    assert written["load_kind"] == "catchup"


def test_a_replay_without_files_only_writes_the_ledger() -> None:
    spark = _Spark()
    (row,) = run(recorder(dataflow()), loaded(1, 1, 0))
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row], replay=True)
    assert "aal_dev_fdh.framework_meta.ledger_detail" not in spark.saved
