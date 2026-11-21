"""Backfill: what is asked for, what is refused, and what the ledger says afterwards."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakeflow_framework import backfill
from lakeflow_framework.backfill import (
    BackfillError,
    Result,
    Selection,
    check_supported,
    parse_selection,
    reader_filters,
    warning_text,
)
from lakeflow_framework.pipeline_metrics import FILE_COLUMNS, file_records
from test_sources import FILE_SOURCE, _config, _volume_spec


def test_files_mode_takes_names_and_nothing_else() -> None:
    assert parse_selection("files", "a.csv, b.csv").files == ("a.csv", "b.csv")
    for bad in ("dir/a.csv", "a*.csv", "{a,b}.csv", "..\\a.csv"):
        with pytest.raises(BackfillError, match="not a file name"):
            parse_selection("files", bad)
    with pytest.raises(BackfillError, match="not dates"):
        parse_selection("files", "a.csv", date_from="2026-01-01")


def test_dates_mode_needs_a_valid_range() -> None:
    selection = parse_selection("dates", date_from="2026-11-01", date_to="2026-11-02 06:30")
    assert (selection.date_from, selection.date_to) == ("2026-11-01", "2026-11-02 06:30")
    assert parse_selection("dates", date_from="2026-11-01").date_to is None
    with pytest.raises(BackfillError, match="needs date_from or date_to"):
        parse_selection("dates")
    with pytest.raises(BackfillError, match="is not a date"):
        parse_selection("dates", date_from="yesterday")
    with pytest.raises(BackfillError, match="is after"):
        parse_selection("dates", date_from="2026-11-03", date_to="2026-11-01")
    with pytest.raises(BackfillError, match="does not take file names"):
        parse_selection("dates", "a.csv", date_from="2026-11-01")


def test_a_listed_path_matches_the_path_bronze_stores() -> None:
    assert backfill._plain_path("dbfs:/Volumes/c/s/v/a.csv") == "/Volumes/c/s/v/a.csv"
    assert backfill._plain_path("file:/Volumes/c/s/v/a.csv") == "/Volumes/c/s/v/a.csv"
    assert backfill._plain_path("/Volumes/c/s/v/a.csv") == "/Volumes/c/s/v/a.csv"


def test_an_unknown_mode_is_refused_with_the_choices() -> None:
    with pytest.raises(
        BackfillError, match="mode must be one of files, dates, failed, full_refresh"
    ):
        parse_selection("everything")


def test_no_selector_means_every_file() -> None:
    assert parse_selection("files").everything
    assert not parse_selection("files", "a.csv").everything
    assert not parse_selection("failed").everything


def test_the_selection_becomes_file_source_filters() -> None:
    assert reader_filters(parse_selection("files", "a.csv,b.csv")) == {
        "pathGlobFilter": "{a.csv,b.csv}"
    }
    # A date alone is the start of its day, and an end date reaches the end of that day.
    assert reader_filters(
        parse_selection("dates", date_from="2026-11-01", date_to="2026-11-02")
    ) == {
        "modifiedAfter": "2026-11-01T00:00:00",
        "modifiedBefore": "2026-11-03T00:00:00",
    }
    assert reader_filters(
        parse_selection("dates", date_from="2026-11-19 19:00", date_to="2026-11-19T19:10:30")
    ) == {"modifiedAfter": "2026-11-19T19:00:00", "modifiedBefore": "2026-11-19T19:10:30"}
    assert reader_filters(parse_selection("files")) == {}


def test_only_a_file_table_without_history_can_be_reloaded_by_file(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    check_supported(config, parse_selection("files", "a.csv"))
    scd1 = _config(
        tmp_path,
        FILE_SOURCE,
        "files_other.yaml",
        _volume_spec(
            extra="  load_type: scd1\n  primary_keys: [thing_id]\n  watermark_column: thing_id"
        ),
    )
    with pytest.raises(BackfillError, match="only full_refresh"):
        check_supported(scd1, parse_selection("files", "a.csv"))
    check_supported(scd1, parse_selection("full_refresh"))  # a full refresh works for any table


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class _Spark:
    def __init__(self, deleted: int = 3):
        self.statements: list[str] = []
        self.deleted = deleted

    def sql(self, statement: str):
        self.statements.append(statement)
        if statement.startswith("DELETE"):
            return _Rows([(self.deleted,)])
        return _Rows([])


def test_a_files_old_rows_are_removed_from_bronze_and_quarantine_before_it_loads_again() -> None:
    spark = _Spark(deleted=7)
    replaced = backfill._remove(spark, ["/v/a.csv", "/v/it's.csv"], "c.b.t", "c.q.t")
    assert replaced == 7  # counted from bronze only
    assert [s.split(" WHERE")[0] for s in spark.statements] == [
        "DELETE FROM c.b.t",
        "DELETE FROM c.q.t",
    ]
    assert "'/v/it''s.csv'" in spark.statements[0]


def test_many_files_are_deleted_in_chunks() -> None:
    spark = _Spark(deleted=1)
    backfill._remove(spark, [f"/v/{n}.csv" for n in range(1200)], "c.b.t", "c.q.t")
    assert len(spark.statements) == 6  # three chunks of up to 500, for two tables


def _blocked(name: str) -> dict:
    return {
        "src_file_path": f"/v/{name}",
        "src_file_name": name,
        "status": "FAILED",
        "rows_read": 10,
        "rows_loaded": 0,
        "rows_quarantined": 2,
        "rows_dropped": 0,
        "rows_blocked": 8,
        "failed_rules": "amount_positive",
        "blocking_rules": "amount_positive",
        "load_kind": "backfill",
    }


def _clean(name: str) -> dict:
    return {**_blocked(name), "status": "PROCESSED", "rows_loaded": 10, "rows_quarantined": 0,
            "rows_blocked": 0, "failed_rules": "", "blocking_rules": ""}  # fmt: skip


def test_the_ledger_marks_the_table_failed_but_the_backfill_ends_with_a_warning(
    tmp_path: Path, monkeypatch
) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    written: list[dict] = []
    monkeypatch.setattr(
        backfill, "write_rows", lambda _spark, _catalog, rows: written.extend(rows) or 1
    )
    result = Result("files_things", "files")
    result.files = [_clean("a.csv"), _blocked("b.csv"), _clean("c.csv")]
    result.rows_replaced = 20
    backfill._record(
        _Spark(), config, result, started=datetime(2026, 11, 19, tzinfo=UTC), update_id="backfill-1"
    )
    (row,) = written
    # Every file is processed to the end; the load is recorded FAILED with the file named.
    assert (row["files_read"], row["files_failed"]) == (3, 1)
    assert (row["rows_read"], row["rows_inserted"], row["rows_quarantined"]) == (30, 20, 2)
    assert (row["rows_dropped"], row["rows_deleted"]) == (8, 20)
    assert row["target_status"] == "FAILED"
    assert row["failure_reason"].startswith("backfill: 1 of 3 file(s) blocked")
    assert "b.csv (amount_positive)" in row["failure_reason"]
    assert row["update_id"] == "backfill-1" and row["pipeline_name"] == "framework_backfill"
    assert row["expectations"] == "[]"
    # The files were written to ledger_detail by each micro-batch, so the table's write adds none.
    assert row["_files"] == []
    records = file_records([{**row, "_files": result.files}], datetime(2026, 11, 19, tzinfo=UTC))
    kinds = {dict(zip(FILE_COLUMNS, r, strict=True))["load_kind"] for r in records}
    assert kinds == {"backfill"} and len(records) == 3


def test_only_the_names_that_are_not_in_the_directory_are_reported_missing() -> None:
    selection = parse_selection("files", "a.csv,gone.csv")
    assert backfill._not_found(selection, ["/v/a.csv", "/v/b.csv"]) == ["gone.csv"]
    assert backfill._not_found(parse_selection("dates", date_from="2026-11-01"), []) == []
    assert (
        backfill._not_found(parse_selection("files"), []) == []
    )  # no names: nothing was asked for


def _record_with(tmp_path: Path, monkeypatch, result: Result) -> dict:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    written: list[dict] = []
    monkeypatch.setattr(
        backfill, "write_rows", lambda _spark, _catalog, rows: written.extend(rows) or 1
    )
    backfill._record(
        _Spark(), config, result, started=datetime(2026, 11, 19, tzinfo=UTC), update_id="backfill-1"
    )
    return written[0]


def test_a_file_that_is_not_there_is_recorded_failed_while_the_others_still_load(
    tmp_path: Path, monkeypatch
) -> None:
    result = Result("files_things", "files")
    result.files = [_clean("a.csv")]
    result.missing = ["gone.csv"]
    result.warnings = ["1 file(s) not found: gone.csv"]
    row = _record_with(tmp_path, monkeypatch, result)
    assert row["target_status"] == "FAILED"
    assert (row["files_read"], row["files_failed"]) == (2, 1)
    assert row["failure_reason"] == "backfill: 1 file(s) not found: gone.csv"
    (item,) = row["_files"]
    assert (item["src_file_name"], item["status"], item["error_message"]) == (
        "gone.csv",
        "FAILED",
        "file not found",
    )
    assert item["src_file_path"].endswith("/gone.csv") and item["rows_read"] == 0


def test_a_file_that_was_read_with_no_rows_is_recorded_failed(tmp_path: Path, monkeypatch) -> None:
    result = Result("files_things", "files")
    result.files = [_clean("a.csv")]
    result.empty = ["/Volumes/c/s/v/empty.csv"]
    result.warnings = ["1 file(s) had no rows: empty.csv"]
    row = _record_with(tmp_path, monkeypatch, result)
    assert row["target_status"] == "FAILED" and row["files_failed"] == 1
    assert row["_files"][0]["error_message"] == "file has no rows"
    assert row["rows_read"] == 10  # the good file's rows are still counted


def test_a_request_that_finds_no_file_at_all_is_recorded_failed(
    tmp_path: Path, monkeypatch
) -> None:
    result = Result("files_things", "dates", none_found=True)
    row = _record_with(tmp_path, monkeypatch, result)
    assert row["target_status"] == "FAILED"
    assert row["failure_reason"].startswith("backfill: no file found for this request in /Volumes/")
    assert (row["rows_read"], row["files_read"], row["_files"]) == (0, 0, [])


def test_the_dry_run_says_what_it_would_record_failed() -> None:
    result = Result("files_things", "files", dry_run=True, missing=["gone.csv"])
    assert "not found, would be recorded FAILED: gone.csv" in warning_text(result)
    nothing = Result("files_things", "dates", dry_run=True, none_found=True)
    assert "no file found for this request" in warning_text(nothing)


def test_the_warning_names_the_blocked_files_and_the_job_is_not_failed() -> None:
    result = Result("files_things", "files")
    result.files = [_clean("a.csv"), _blocked("b.csv")]
    result.warnings.append("1 file(s) blocked by fail rules: b.csv")
    text = warning_text(result)
    assert (
        "2 file(s) reloaded" in text and "WARNING: 1 file(s) blocked by fail rules: b.csv" in text
    )


def test_a_dry_run_lists_the_files_and_changes_nothing() -> None:
    result = Result(
        "files_things", "files", planned=[f"/v/{n}.csv" for n in range(60)], dry_run=True
    )
    text = warning_text(result)
    assert text.startswith("DRY RUN files_things: 60 file(s) selected")
    assert "... and 10 more" in text


def test_full_refresh_and_a_run_with_no_selector_need_confirmation(
    tmp_path: Path, monkeypatch
) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    monkeypatch.setattr(backfill, "load_config", lambda *_: config)
    spark = _Spark()
    with pytest.raises(BackfillError, match="needs confirm|needs\\s+confirm|confirm"):
        backfill.run_backfill(spark, "c", "files_things", Selection("full_refresh"))
    with pytest.raises(BackfillError, match="reloads every file"):
        backfill.run_backfill(spark, "c", "files_things", Selection("files"))
    # A dry run is always allowed, and says what it would reset.
    dry = backfill.run_backfill(spark, "c", "files_things", Selection("full_refresh"), dry_run=True)
    assert dry.dry_run and any(name.endswith("files_things_staged") for name in dry.planned)
    assert not any(statement.startswith("DELETE") for statement in spark.statements)
