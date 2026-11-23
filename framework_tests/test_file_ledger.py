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
            loaded_rows=8,
            quarantined=2,
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
    assert row["rows_dropped"] == 24  # read - inserted - quarantined, from the table's own flows
    assert row["target_status"] == "FAILED"
    assert "3 of 100 file(s) had rows that broke fail rules" in row["failure_reason"]
    assert "bad_0.csv (amount_positive)" in row["failure_reason"]


def test_every_file_is_written_with_its_own_counts_and_the_batch() -> None:
    row = row_for(hundred_files_three_bad(), read=1000, written=970, quarantine=6)
    records = file_records([row], AT)
    assert len(records) == 100
    first = dict(zip(DETAIL_COLUMNS, records[0], strict=True))
    assert first["src_file_name"] == "ok_000.csv"
    assert (first["rows_read"], first["rows_inserted"], first["rows_dropped"]) == (10, 10, 0)
    bad = dict(zip(DETAIL_COLUMNS, records[-1], strict=True))
    # The good rows of a file with a bad row still load; only the bad rows are kept out.
    assert bad["status"] == "FAILED" and bad["rows_inserted"] == 8 and bad["rows_quarantined"] == 2
    assert bad["error_message"] == "rows broke fail rule(s): amount_positive"
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


def test_a_file_whose_quarantined_rows_broke_a_fail_rule_is_failed_and_its_good_rows_count() -> (
    None
):
    spark = _Spark()
    spark.files = [
        {**_file_count_row(), "rows_loaded": 8, "rows_quarantined": 2, "blocking_rules": "amt"}
    ]
    (row,) = run(recorder(dataflow(file_load=True, fail_rules=("amt",))), loaded(10, 8, 2))
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row])
    (item,) = row["_files"]
    assert item["status"] == "FAILED" and item["rows_read"] == 10 and item["rows_inserted"] == 8
    assert row["target_status"] == "FAILED" and row["files_failed"] == 1
    (query,) = [q for q in spark.queries if "_batch_id = 'batch-1'" in q]
    assert "array('amt')" in query


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
