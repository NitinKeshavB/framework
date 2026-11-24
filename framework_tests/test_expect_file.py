"""A scheduled load expects a file with rows each run: none, or an empty one, is FAILED in the ledger."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakeflow_framework import ConfigValidationError, build_pipeline
from lakeflow_framework.config import config_from_document
from lakeflow_framework.pipeline import assemble
from lakeflow_framework.pipeline_metrics import (
    EMPTY_FILE,
    apply_arrivals,
    apply_files,
    read_arrivals,
    write_rows,
)
from test_pipeline_metrics import _Spark, dataflow, loaded, recorder, run
from test_sources import FILE_SOURCE, _config, _Pipelines, _volume_spec
from test_sources import _Spark as _SourceSpark

SCHEDULED = FILE_SOURCE.replace(
    "daily: {trigger: file_arrival}", 'daily: {schedule: "0 0 2 * * ?"}'
)


def config_for(tmp_path: Path, source: str = SCHEDULED, extra: str = ""):
    return _config(
        tmp_path,
        source,
        "files_things.yaml",
        _volume_spec(source=extra or "  raw_path: things/\n  format: csv\n"),
    )


def test_a_table_in_a_scheduled_delivery_expects_a_file_by_default(tmp_path: Path) -> None:
    assert config_for(tmp_path).expect_file is True


def test_a_table_in_a_delivery_fired_by_a_file_does_not(tmp_path: Path) -> None:
    assert config_for(tmp_path, FILE_SOURCE).expect_file is False


def test_a_table_can_turn_it_off_or_on_itself(tmp_path: Path) -> None:
    off = config_for(tmp_path, extra="  raw_path: things/\n  format: csv\n  expect_file: false\n")
    assert off.expect_file is False
    on = config_for(
        tmp_path, FILE_SOURCE, extra="  raw_path: things/\n  format: csv\n  expect_file: true\n"
    )
    assert on.expect_file is True
    with pytest.raises(ConfigValidationError, match="expect_file must be true or false"):
        config_for(tmp_path, extra="  raw_path: things/\n  format: csv\n  expect_file: maybe\n")


def test_the_setting_survives_the_registry_round_trip(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    again = config_from_document(config.canonical())
    assert again.expect_file is True and again.content_hash() == config.content_hash()
    plain = config_for(tmp_path, FILE_SOURCE)
    assert "expect_file" not in plain.canonical()["source_to_raw"]["source"]


def test_no_table_is_added_for_the_check_whether_or_not_a_file_is_expected(tmp_path: Path) -> None:
    for source in (SCHEDULED, FILE_SOURCE):
        kinds = [dataset.kind for dataset in assemble(config_for(tmp_path, source)).datasets]
        assert not {"arrivals", "files", "blocked_check"} & set(kinds)
    spark = _SourceSpark()
    pipelines = _Pipelines(spark)
    build_pipeline("files_things", spark=spark, pipelines=pipelines, config=config_for(tmp_path))
    assert not [n for n in pipelines.names if n.endswith(("_arrivals", "_inventory", "_files"))]


def test_the_check_lists_the_volume_and_reads_only_the_ledger_tables(
    tmp_path: Path, monkeypatch
) -> None:
    from datetime import UTC, datetime

    from lakeflow_framework import pipeline_metrics

    listed = [
        {
            "file_path": "/v/old.csv",
            "file_bytes": 1,
            "arrived_at": datetime(2026, 11, 1, tzinfo=UTC),
        },
        {
            "file_path": "/v/new.csv",
            "file_bytes": 0,
            "arrived_at": datetime(2026, 11, 19, tzinfo=UTC),
        },
    ]
    monkeypatch.setattr(pipeline_metrics, "list_volume_files", lambda _path, _glob=None: listed)

    class Ledger(_Spark):
        def sql(self, statement: str):
            self.queries.append(statement)
            if "max(started_at)" in statement:
                since = datetime(2026, 11, 18, tzinfo=UTC).timestamp()
                return type("R", (), {"collect": lambda _s: [{"since": since}]})()
            return type("R", (), {"collect": lambda _s: []})()

    spark = Ledger()
    (row,) = run(recorder(dataflow(expect_file=True, path="/v/")), loaded(0, 0, 0))
    row["batch_id"] = "batch-1"
    arrivals = read_arrivals(spark, "aal_dev_fdh", row, [])
    # Only what arrived after the previous regular run (less a few minutes) is new.
    assert [item["src_file_name"] for item in arrivals] == ["new.csv"]
    assert arrivals[0]["has_rows"] is False and arrivals[0]["recorded_before"] is False
    ledger_query = next(q for q in spark.queries if "max(started_at)" in q)
    assert "update_id NOT LIKE 'backfill-%'" in ledger_query  # a backfill does not move it
    assert all("framework_meta.load_ledger" in q or "ledger_detail" in q for q in spark.queries)
    assert read_arrivals(spark, "aal_dev_fdh", {**row, "_expect_file": False}, []) is None


def _arrival(name: str, *, has_rows: bool, recorded_before: bool = False) -> dict:
    return {
        "src_file_path": f"/Volumes/c/s/v/{name}",
        "src_file_name": name,
        "has_rows": has_rows,
        "recorded_before": recorded_before,
    }


def make_row(files: list[dict] | None = None) -> dict:
    (row,) = run(recorder(dataflow()), loaded(0, 0, 0))
    row["batch_id"] = "batch-1"
    row["_path"] = "/Volumes/c/s/v/"
    apply_files(row, files or [])
    return row


def test_a_run_where_no_file_arrived_is_failed_but_says_why() -> None:
    row = apply_arrivals(make_row(), [])
    assert row["target_status"] == "FAILED"
    assert row["failure_reason"] == "no file arrived in /Volumes/c/s/v/ since the previous run"
    assert (row["rows_read"], row["rows_inserted"], row["files_read"]) == (0, 0, 0)


def test_files_that_arrived_and_were_already_recorded_do_not_count_as_new() -> None:
    row = apply_arrivals(make_row(), [_arrival("old.csv", has_rows=False, recorded_before=True)])
    assert row["target_status"] == "FAILED" and "no file arrived" in row["failure_reason"]


def test_a_file_with_no_rows_is_recorded_failed_and_the_others_are_left_alone() -> None:
    row = make_row()
    apply_arrivals(
        row, [_arrival("good.csv", has_rows=True), _arrival("empty.csv", has_rows=False)]
    )
    assert row["target_status"] == "FAILED"
    assert row["failure_reason"] == "1 file(s) arrived with no rows: empty.csv"
    assert (row["files_read"], row["files_failed"]) == (1, 1)
    (item,) = row["_files"]
    assert (item["src_file_name"], item["status"], item["error_message"]) == (
        "empty.csv",
        "FAILED",
        EMPTY_FILE,
    )
    assert (
        item["rows_read"],
        item.get("rows_inserted") or item.get("rows_inserted"),
        item.get("rows_dropped"),
    ) == (0, 0, 0)  # never NULL


def test_a_good_file_that_arrived_leaves_the_load_completed() -> None:
    files = [
        {
            "src_file_path": "/Volumes/c/s/v/a.csv",
            "src_file_name": "a.csv",
            "status": "PROCESSED",
            "rows_read": 5,
            "rows_inserted": 5,
            "rows_quarantined": 0,
            "rows_dropped": 0,
        }
    ]
    row = make_row(files)
    apply_arrivals(row, [_arrival("a.csv", has_rows=True)])
    assert row["target_status"] == "COMPLETED" and row["failure_reason"] is None


def test_an_empty_file_beside_a_blocked_one_keeps_both_reasons() -> None:
    blocked = [
        {
            "src_file_path": "/Volumes/c/s/v/b.csv",
            "src_file_name": "b.csv",
            "status": "FAILED",
            "rows_read": 2,
            "rows_inserted": 0,
            "rows_quarantined": 1,
            "rows_dropped": 1,
            "blocking_rules": "amount_numeric",
        }
    ]
    row = make_row(blocked)
    apply_arrivals(row, [_arrival("b.csv", has_rows=True), _arrival("e.csv", has_rows=False)])
    assert "blocked by fail rules" in row["failure_reason"] and "e.csv" in row["failure_reason"]
    assert (row["files_read"], row["files_failed"]) == (2, 2)


def test_a_table_that_expects_no_file_is_not_touched() -> None:
    row = make_row()
    assert apply_arrivals(row, None)["target_status"] == "COMPLETED"


def test_an_empty_file_found_by_the_check_is_written_with_the_ledger(monkeypatch) -> None:
    from datetime import UTC, datetime

    from lakeflow_framework import pipeline_metrics

    monkeypatch.setattr(
        pipeline_metrics,
        "list_volume_files",
        lambda _path, _glob=None: [
            {
                "file_path": "/v/e.csv",
                "file_bytes": 0,
                "arrived_at": datetime(2026, 11, 19, tzinfo=UTC),
            }
        ],
    )
    spark = _Spark()
    (row,) = run(recorder(dataflow(expect_file=True, path="/v/")), loaded(0, 0, 0))
    row["batch_id"] = "batch-1"
    write_rows(spark, "aal_dev_fdh", [row])
    from lakeflow_framework.pipeline_metrics import DETAIL_COLUMNS

    saved = spark.saved["aal_dev_fdh.framework_meta.ledger_detail"].data
    assert len(saved) == 1
    detail = dict(zip(DETAIL_COLUMNS, saved[0], strict=True))
    assert detail["status"] == "FAILED" and detail["src_file_name"] == "e.csv"
    from lakeflow_framework.load_metrics import LEDGER_COLUMNS

    ledger = dict(
        zip(
            LEDGER_COLUMNS,
            spark.saved["aal_dev_fdh.framework_meta.load_ledger"].data[0],
            strict=True,
        )
    )
    assert ledger["target_status"] == "FAILED"
