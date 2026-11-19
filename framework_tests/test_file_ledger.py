"""A run of many files: one ledger row for the table, one row per file, a blocked file fails the load."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lakeflow_framework.pipeline_metrics import (
    FILE_COLUMNS,
    FILE_SCHEMA,
    apply_files,
    file_records,
    ledger_records,
    metric_records,
    read_files,
    write_rows,
)
from test_pipeline_metrics import _Spark, dataflow, loaded, recorder, run

AT = datetime(2026, 11, 19, tzinfo=UTC)


def _file(name: str, *, read: int, loaded_rows: int, **extra) -> dict:
    status = extra.pop("status", "PROCESSED")
    return {
        "src_file_path": f"/Volumes/c/s/v/{name}",
        "src_file_name": name,
        "status": status,
        "rows_read": read,
        "rows_loaded": loaded_rows,
        "rows_quarantined": extra.pop("quarantined", 0),
        "rows_dropped": extra.pop("dropped", 0),
        "rows_blocked": extra.pop("blocked", 0),
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
            blocked=8,
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
    assert (row["rows_blocked"], row["rows_dropped"]) == (24, 0)
    assert row["target_status"] == "FAILED"
    assert "3 of 100 file(s) blocked" in row["failure_reason"]
    assert "bad_0.csv (amount_positive)" in row["failure_reason"]
    # Every row read is accounted for: loaded, quarantined, dropped or blocked.
    assert 970 + 6 + 0 + 24 == row["rows_read"]


def test_every_file_is_written_with_its_own_counts_and_the_batch() -> None:
    row = row_for(hundred_files_three_bad(), read=1000, written=970, quarantine=6)
    records = file_records([row], AT)
    assert len(records) == 100
    first = dict(zip(FILE_COLUMNS, records[0], strict=True))
    assert first["batch_id"] == "batch-1" and first["src_file_name"] == "ok_000.csv"
    assert (first["rows_read"], first["rows_loaded"], first["rows_blocked"]) == (10, 10, 0)
    bad = dict(zip(FILE_COLUMNS, records[-1], strict=True))
    assert bad["status"] == "FAILED" and bad["rows_loaded"] == 0 and bad["rows_blocked"] == 8
    assert bad["error_message"] == "blocked by fail rule(s): amount_positive"
    assert first["error_message"] is None
    assert len(FILE_SCHEMA.split(", ")) == len(FILE_COLUMNS)


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
    record = ledger_records([row], AT)[0]
    assert all(value is not None for value in record[-4:]) and record[-4:] == (0, 0, 0, 0)
    assert all(value is not None for *_, value in metric_records([row], AT))


def test_write_rows_adds_the_per_file_rows_to_the_file_ledger() -> None:
    spark = _Spark()
    spark.files = [
        {
            "_src_file_path": "/Volumes/c/s/v/a.csv",
            "_src_file_name": "a.csv",
            "file_status": "PROCESSED",
            "rows_read": 5,
            "rows_loaded": 5,
            "rows_quarantined": 0,
            "rows_dropped": 0,
            "rows_blocked": 0,
            "failed_rules": "",
            "blocking_rules": "",
        }
    ]
    (row,) = run(recorder(dataflow(files="aal.abs_bronze.abs_accounting_files")), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    assert write_rows(spark, "aal_dev_fdh", [row]) == 1
    saved = spark.saved["aal_dev_fdh.framework_meta.file_ledger"]
    assert saved.save_mode == "append" and len(saved.data) == 1
    assert any("abs_accounting_files WHERE _batch_id = 'batch-1'" in q for q in spark.queries)


def _file_view_row() -> dict:
    return {
        "_src_file_path": "/Volumes/c/s/v/a.csv",
        "_src_file_name": "a.csv",
        "file_status": "PROCESSED",
        "rows_read": 5,
        "rows_loaded": 5,
        "rows_quarantined": 0,
        "rows_dropped": 0,
        "rows_blocked": 0,
        "failed_rules": "",
        "blocking_rules": "",
    }


def test_the_three_tables_are_written_together_so_none_waits_for_the_others() -> None:
    spark = _Spark()
    spark.files = [_file_view_row()]
    (row,) = run(recorder(dataflow(files="aal.abs_bronze.abs_accounting_files")), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row])
    assert {name.rsplit(".", 1)[-1] for name in spark.saved} == {
        "file_ledger",
        "load_metric",
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
    spark.files = [_file_view_row()]
    (row,) = run(recorder(dataflow(files="aal.abs_bronze.abs_accounting_files")), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    with pytest.raises(RuntimeError, match="no MODIFY"):
        write_rows(spark, "aal_dev_fdh", [row])
    assert "aal_dev_fdh.framework_meta.load_metric" in spark.saved


def test_a_replay_does_not_write_files_the_cut_off_update_already_wrote() -> None:
    class Replay(_Spark):
        def sql(self, statement: str):
            if "FROM aal_dev_fdh.framework_meta.file_ledger" in statement:
                return type("R", (), {"collect": lambda _s: [{"src_file_path": "/Volumes/c/s/v/a.csv"}]})()
            return super().sql(statement)

    spark = Replay()
    spark.files = [_file_view_row()]
    (row,) = run(recorder(dataflow(files="aal.abs_bronze.abs_accounting_files")), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row], replay=True)
    assert "aal_dev_fdh.framework_meta.file_ledger" not in spark.saved
    assert "aal_dev_fdh.framework_meta.load_ledger" in spark.saved


def test_a_replay_waits_for_the_blocked_check_only_when_the_update_ran_it() -> None:
    from lakeflow_framework.load_catchup import _with_checks

    flow = dataflow(files="aal.framework_meta.abs_accounting_files")
    check = "aal.framework_meta.abs_accounting_blocked_check"

    def event(name: str) -> dict:
        return {"origin": {"flow_name": name}}

    ran = _with_checks([flow], [event(flow.files), event(check)])
    assert (ran[0].files, ran[0].blocked_check) == (flow.files, check)
    # No fail rule: the files view ran and nothing else, so nothing more is waited for.
    plain = _with_checks([flow], [event(flow.files)])
    assert (plain[0].files, plain[0].blocked_check) == (flow.files, "")
    # An update that never got that far names neither.
    early = _with_checks([flow], [event("aal.abs_bronze.finance_abs")])
    assert (early[0].files, early[0].blocked_check) == ("", "")


def test_a_file_view_that_cannot_be_read_gives_zero_counts_not_a_lost_row() -> None:
    class Broken(_Spark):
        def sql(self, statement: str):
            if "_batch_id =" in statement:
                raise RuntimeError("view not found")
            return super().sql(statement)

    spark = Broken()
    (row,) = run(recorder(dataflow(files="aal.abs_bronze.abs_accounting_files")), loaded(5, 5, 0))
    row["batch_id"] = "batch-1"
    assert write_rows(spark, "aal_dev_fdh", [row]) == 1
    assert row["files_read"] == row["files_failed"] == 0
    assert "aal_dev_fdh.framework_meta.file_ledger" not in spark.saved


def test_a_run_without_a_batch_id_does_not_guess_its_files() -> None:
    (row,) = run(recorder(dataflow(files="aal.abs_bronze.abs_accounting_files")), loaded(5, 5, 0))
    row["batch_id"] = None
    assert read_files(_Spark(), row) == []
