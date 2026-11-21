"""Load counts, status, and errors recorded by the pipeline itself, once per update. No Spark."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from typing import Any

import pytest

from lakeflow_framework.config import config_from_document
from lakeflow_framework.load_metrics import LEDGER_COLUMNS
from lakeflow_framework.pipeline import _configs_from_manifest, _install_recorder
from lakeflow_framework.pipeline_metrics import (
    LEDGER_SCHEMA,
    Dataflow,
    UpdateRecorder,
    install,
    ledger_records,
    metric_records,
    recorded_dataflows,
    write_rows,
)
from test_manifest import DOCUMENT

UPDATE = "u-1"
STAGED = "aal.framework_meta.abs_accounting_staged"
BRONZE = "aal.abs_bronze.finance_abs"
QUARANTINE = "aal.abs_bronze.finance_abs_qrntn"
COUNTS = ("rows_read", "rows_inserted", "rows_updated", "rows_deleted", "rows_quarantined")


def dataflow(**overrides: Any) -> Dataflow:
    values = {
        "pipeline_id": "abs_accounting",
        "owner_kind": "source_system",
        "owner": "abs",
        "staged": "abs_accounting_staged",
        "target": BRONZE,
        "quarantine": QUARANTINE,
    }
    return Dataflow(**{**values, **overrides})


def recorder(*flows: Dataflow) -> UpdateRecorder:
    return UpdateRecorder("abs_daily", "bronze", list(flows) or [dataflow()])


def progress(flow: str, status: str, at: int, update: str = UPDATE, **extra: Any) -> dict:
    return {
        "event_type": "flow_progress",
        "timestamp": f"2026-11-19T10:00:{at:02d}.000Z",
        "level": "INFO",
        "origin": {
            "update_id": update,
            "flow_name": flow,
            "pipeline_id": "uuid-1",
            "pipeline_name": "[fdh] abs_daily",
        },
        "details": {"flow_progress": {"status": status, **extra}},
    }


def update_state(state: str, at: int, update: str = UPDATE, **extra: Any) -> dict:
    return {
        "event_type": "update_progress",
        "timestamp": f"2026-11-19T10:00:{at:02d}.000Z",
        "level": "ERROR" if state == "FAILED" else "INFO",
        "origin": {
            "update_id": update,
            "pipeline_id": "uuid-1",
            "pipeline_name": "[fdh] abs_daily",
        },
        "details": {"update_progress": {"state": state}},
        **extra,
    }


def run(rec: UpdateRecorder, events: list[dict]) -> list[dict] | None:
    result = None
    for event in events:
        result = rec.handle(event) or result
    return result


def loaded(read: int, written: int, quarantined: int) -> list[dict]:
    return [
        update_state("RUNNING", 0),
        progress(STAGED, "RUNNING", 1, metrics={"num_output_rows": read}),
        progress(STAGED, "COMPLETED", 2),
        progress(BRONZE, "RUNNING", 3, metrics={"num_output_rows": written}),
        progress(BRONZE, "COMPLETED", 4),
        progress(QUARANTINE, "RUNNING", 3, metrics={"num_output_rows": quarantined}),
        progress(QUARANTINE, "COMPLETED", 4),
        update_state("COMPLETED", 9),
    ]


def test_a_load_records_read_written_and_quarantined_once() -> None:
    rec = recorder()
    (row,) = run(rec, loaded(read=100, written=90, quarantined=10))
    assert (row["rows_read"], row["rows_inserted"], row["rows_quarantined"]) == (100, 90, 10)
    assert (row["rows_updated"], row["rows_deleted"]) == (0, 0)
    assert (row["target_status"], row["quarantine_status"]) == ("COMPLETED", "COMPLETED")
    assert row["failure_reason"] is None
    # From the first flow starting (not the update waiting for compute) to the last flow finishing.
    assert row["duration_seconds"] == 3.0
    assert row["started_at"].second == 1 and row["ended_at"].second == 4
    assert row["update_pipeline_uuid"] == "uuid-1" and row["pipeline_name"] == "[fdh] abs_daily"
    assert (row["job_name"], row["layer"], row["dataflow"]) == (
        "abs_daily",
        "bronze",
        "abs_accounting",
    )
    assert (row["target_table"], row["quarantine_table"]) == (BRONZE, QUARANTINE)
    # The same update, replayed or finished twice, produces nothing more.
    assert run(rec, [update_state("COMPLETED", 10)]) is None
    assert run(rec, loaded(100, 90, 10)) is None


def test_the_rows_come_when_the_last_flow_finishes_not_when_the_update_closes() -> None:
    rec = recorder()
    events = loaded(100, 90, 10)
    assert events[-1]["event_type"] == "update_progress"
    for event in events[:-2]:
        assert rec.handle(event) is None  # quarantine is still running
    assert rec.handle(events[-2])  # the last flow completes: recorded now
    assert rec.handle(events[-1]) is None  # the update closing adds nothing


def test_a_delivery_waits_for_every_table_before_it_records() -> None:
    second = dataflow(
        pipeline_id="abs_two",
        staged="abs_two_staged",
        target="aal.abs_bronze.two",
        quarantine="aal.abs_bronze.two_qrntn",
    )
    rec = recorder(dataflow(), second)
    assert run(rec, loaded(1, 1, 0)[:-1]) is None  # abs_two has not started
    rows = run(
        rec,
        [
            progress("aal.framework_meta.abs_two_staged", "COMPLETED", 5),
            progress("aal.abs_bronze.two", "COMPLETED", 6),
            progress("aal.abs_bronze.two_qrntn", "COMPLETED", 6),
        ],
    )
    assert [row["dataflow"] for row in rows] == ["abs_accounting", "abs_two"]


def test_a_failed_flow_is_recorded_once_the_rest_have_finished() -> None:
    events = [
        update_state("RUNNING", 0),
        progress(STAGED, "COMPLETED", 1),
        progress(BRONZE, "FAILED", 2),
        progress(QUARANTINE, "COMPLETED", 3),
    ]
    (row,) = run(recorder(), events)
    assert row["target_status"] == "FAILED" and row["quarantine_status"] == "COMPLETED"
    assert row["failure_reason"]


def test_an_update_with_no_new_files_records_zeros_not_nulls() -> None:
    events = [
        update_state("RUNNING", 0),
        progress(STAGED, "COMPLETED", 1),
        progress(BRONZE, "COMPLETED", 2),
        progress(QUARANTINE, "COMPLETED", 2),
        update_state("COMPLETED", 3),
    ]
    (row,) = run(recorder(), events)
    assert [row[count] for count in COUNTS] == [0, 0, 0, 0, 0]
    assert row["target_status"] == "COMPLETED"
    assert row["failure_reason"] is None


def test_a_flow_total_is_the_largest_seen_not_the_sum_of_the_reports() -> None:
    events = loaded(read=50, written=50, quarantined=0)
    events.insert(2, progress(STAGED, "RUNNING", 2, metrics={"num_output_rows": 30}))
    (row,) = run(recorder(), events)
    assert row["rows_read"] == 50


def test_a_failed_update_keeps_what_finished_zeroes_the_rest_and_says_why() -> None:
    boom = {
        "event_type": "flow_progress",
        "timestamp": "2026-11-19T10:00:05.000Z",
        "level": "ERROR",
        "message": "Flow 'finance_abs' has FAILED",
        "origin": {"update_id": UPDATE, "flow_name": BRONZE},
        "details": {"flow_progress": {"status": "FAILED"}},
        "error": {
            "exceptions": [
                {"class_name": "AnalysisException", "message": "[DELTA_FAILED_TO_MERGE_FIELDS] x"}
            ]
        },
    }
    events = [
        update_state("RUNNING", 0),
        progress(STAGED, "RUNNING", 1, metrics={"num_output_rows": 100}),
        progress(STAGED, "COMPLETED", 2),
        progress(BRONZE, "RUNNING", 3, metrics={"num_output_rows": 40}),
        boom,
        update_state("FAILED", 8, message="Update u-1 has failed."),
    ]
    (row,) = run(recorder(), events)
    assert row["rows_read"] == 100  # the staged flow finished before the failure
    assert (row["rows_inserted"], row["rows_quarantined"]) == (0, 0)
    assert (row["rows_updated"], row["rows_deleted"]) == (0, 0)
    assert row["target_status"] == "FAILED"
    assert "AnalysisException: [DELTA_FAILED_TO_MERGE_FIELDS] x" in row["failure_reason"]
    assert "Flow 'finance_abs' has FAILED" in row["failure_reason"]


def test_a_failure_with_no_error_event_still_says_what_happened() -> None:
    (row,) = run(recorder(), [update_state("RUNNING", 0), update_state("FAILED", 4)])
    assert [row[count] for count in COUNTS] == [0, 0, 0, 0, 0]
    assert row["target_status"] == "FAILED"
    assert "ended FAILED" in row["failure_reason"]


def test_a_canceled_update_is_canceled_with_zero_counts() -> None:
    events = [
        update_state("RUNNING", 0),
        progress(BRONZE, "RUNNING", 1, metrics={"num_output_rows": 9}),
        update_state("CANCELED", 2),
    ]
    (row,) = run(recorder(), events)
    assert (row["target_status"], row["rows_inserted"]) == ("CANCELED", 0)
    assert "ended CANCELED" in row["failure_reason"]


def test_another_tables_error_is_not_blamed_on_this_one() -> None:
    other = dataflow(
        pipeline_id="abs_other",
        staged="abs_other_staged",
        target="aal.abs_bronze.other",
        quarantine="aal.abs_bronze.other_qrntn",
    )
    error = {
        "event_type": "flow_progress",
        "timestamp": "2026-11-19T10:00:05.000Z",
        "level": "ERROR",
        "message": "other table broke",
        "origin": {"update_id": UPDATE, "flow_name": "aal.abs_bronze.other"},
        "details": {"flow_progress": {"status": "FAILED"}},
    }
    rows = run(
        recorder(dataflow(), other),
        [update_state("RUNNING", 0), error, update_state("COMPLETED", 6)],
    )
    mine, theirs = rows
    assert mine["dataflow"] == "abs_accounting" and mine["failure_reason"] is None
    assert theirs["target_status"] == "FAILED" and "other table broke" in theirs["failure_reason"]


def test_auto_cdc_counts_upserts_and_deletes() -> None:
    events = [
        update_state("RUNNING", 0),
        progress(STAGED, "RUNNING", 1, metrics={"num_output_rows": 12}),
        progress(STAGED, "COMPLETED", 2),
        progress(BRONZE, "RUNNING", 3, metrics={"num_upserted_rows": 8, "num_deleted_rows": 3}),
        progress(BRONZE, "COMPLETED", 4),
        progress(QUARANTINE, "COMPLETED", 4),
        update_state("COMPLETED", 5),
    ]
    (row,) = run(recorder(dataflow(append=False)), events)
    assert (row["rows_read"], row["rows_updated"], row["rows_deleted"]) == (12, 8, 3)
    assert row["rows_inserted"] == 0


def test_a_spec_that_could_not_register_is_failed_with_its_error() -> None:
    broken = Dataflow(
        pipeline_id="abs_broken",
        owner_kind="source_system",
        owner="abs",
        staged="abs_broken_staged",
        target="",
        quarantine="",
        build_error="invalid spec: path must be a /Volumes path",
    )
    good, bad = run(recorder(dataflow(), broken), loaded(10, 10, 0))
    assert good["failure_reason"] is None and good["rows_inserted"] == 10
    assert bad["target_status"] == "FAILED"
    assert bad["failure_reason"] == "invalid spec: path must be a /Volumes path"
    assert [bad[count] for count in COUNTS] == [0, 0, 0, 0, 0]
    assert bad["target_table"] is None and bad["quarantine_table"] is None


def test_expectations_are_kept_from_the_staged_flow() -> None:
    checks = json.dumps(
        [{"name": "amount_ok", "dataset": "x", "passed_records": 90, "failed_records": 10}]
    )
    events = loaded(100, 90, 10)
    events.insert(
        2,
        progress(STAGED, "RUNNING", 2, data_quality={"expectations": checks, "dropped_records": 0}),
    )
    (row,) = run(recorder(), events)
    assert json.loads(row["expectations"]) == [
        {"name": "amount_ok", "passed_records": 90, "failed_records": 10}
    ]


def test_a_very_long_error_is_cut() -> None:
    error = {
        "event_type": "update_progress",
        "timestamp": "2026-11-19T10:00:05.000Z",
        "level": "ERROR",
        "message": "x" * 9000,
        "origin": {"update_id": UPDATE},
        "details": {"update_progress": {"state": "FAILED"}},
    }
    (row,) = run(recorder(), [error])
    assert len(row["failure_reason"]) <= 4000 and row["failure_reason"].endswith("...")
    twice = {**error, "message": "y" * 3000, "error": {"exceptions": [{"message": "z" * 3000}]}}
    (row,) = run(recorder(), [{**twice, "origin": {"update_id": "u-2"}}])
    assert len(row["failure_reason"]) <= 4000


class _Frame:
    def __init__(self, spark: _Spark, data: list, schema: str) -> None:
        self.spark, self.data, self.schema, self.save_mode = spark, data, schema, None

    @property
    def write(self) -> _Frame:
        return self

    def mode(self, mode: str) -> _Frame:
        self.save_mode = mode
        return self

    def saveAsTable(self, name: str) -> None:  # noqa: N802 (Spark's name)
        self.spark.saved[name] = self


class _Spark:
    """The few Spark calls a pipeline hook may make: SELECT through sql(), and DataFrame writes."""

    conf = type("Conf", (), {"get": staticmethod(lambda key, default=None: default)})()

    def __init__(self, recorded: tuple[str, ...] = ()) -> None:
        self.recorded = recorded
        self.files: list[dict] = []
        self.queries: list[str] = []
        self.saved: dict[str, _Frame] = {}

    def sql(self, statement: str):
        assert statement.lstrip().upper().startswith("SELECT"), "a pipeline allows only SELECT"
        self.queries.append(statement)
        if "_batch_id =" in statement:  # the per-file view of a run
            return type("Result", (), {"collect": lambda _self: self.files})()
        return type(
            "Result", (), {"collect": lambda _self: [{"dataflow": d} for d in self.recorded]}
        )()

    def createDataFrame(self, data: list, schema: str) -> _Frame:  # noqa: N802 (Spark's name)
        return _Frame(self, data, schema)


def test_ledger_records_match_the_ledger_columns_with_zeros_not_nulls() -> None:
    rows = run(recorder(dataflow(), dataflow(pipeline_id="abs_two")), loaded(5, 5, 0))
    at = datetime(2026, 11, 19, tzinfo=UTC)
    records = ledger_records(rows, at)
    columns = [part.split()[0] for part in LEDGER_SCHEMA.split(", ")]
    # CREATE TABLE column order matches LEDGER_COLUMNS (first-deploy shape; no ADD COLUMN lag).
    assert columns == list(LEDGER_COLUMNS) and len(records) == 2
    first = dict(zip(columns, records[0], strict=True))
    assert (first["rows_read"], first["rows_inserted"], first["rows_quarantined"]) == (5, 5, 0)
    assert (first["rows_updated"], first["rows_deleted"]) == (0, 0)
    assert (first["files_read"], first["files_failed"]) == (0, 0)
    assert first["job_run_id"] is None  # not known inside the pipeline; the job views join on time
    assert first["recorded_at"] == at
    assert "rows_read BIGINT" in LEDGER_SCHEMA and "recorded_at TIMESTAMP" in LEDGER_SCHEMA


def test_metric_records_have_every_metric_for_every_dataflow() -> None:
    (row,) = run(recorder(), loaded(100, 90, 10))
    at = datetime(2026, 11, 19, tzinfo=UTC)
    assert metric_records([row], at) == []


def test_write_rows_appends_with_the_dataframe_writer_not_sql() -> None:
    spark = _Spark()
    assert write_rows(spark, "aal_dev_fdh", []) == 0 and not spark.saved
    (row,) = run(recorder(), loaded(1, 1, 0))
    assert write_rows(spark, "aal_dev_fdh", [row]) == 1
    ledger = spark.saved["aal_dev_fdh.framework_meta.load_ledger"]
    assert ledger.save_mode == "append"
    assert len(ledger.data) == 1
    assert "aal_dev_fdh.framework_meta.load_metric" not in spark.saved
    assert ledger.schema == LEDGER_SCHEMA


def test_a_dataflow_already_recorded_for_the_update_is_not_written_again() -> None:
    second = dataflow(
        pipeline_id="abs_two",
        staged="abs_two_staged",
        target="aal.abs_bronze.two",
        quarantine="aal.abs_bronze.two_qrntn",
    )
    events = loaded(3, 3, 0)[:-1] + [
        progress("aal.framework_meta.abs_two_staged", "COMPLETED", 5),
        progress("aal.abs_bronze.two", "COMPLETED", 6),
        progress("aal.abs_bronze.two_qrntn", "COMPLETED", 6),
    ]
    rows = run(recorder(dataflow(), second), events)
    assert len(rows) == 2
    spark = _Spark(recorded=("abs_accounting",))
    assert write_rows(spark, "aal_dev_fdh", rows) == 1
    (written,) = spark.saved["aal_dev_fdh.framework_meta.load_ledger"].data
    assert written[LEDGER_COLUMNS.index("dataflow")] == "abs_two"
    everything = _Spark(recorded=("abs_accounting", "abs_two"))
    assert write_rows(everything, "aal_dev_fdh", rows) == 0 and not everything.saved


def test_the_existing_rows_query_is_a_quoted_select_on_a_valid_catalog() -> None:
    spark = _Spark()
    row = {"job_name": "abs_daily", "layer": "bronze", "update_id": "it's"}
    recorded_dataflows(spark, "aal_dev_fdh", row)
    assert "update_id = 'it''s'" in spark.queries[0]
    with pytest.raises(ValueError):
        recorded_dataflows(spark, "bad catalog;", row)


class _Hooks:
    def __init__(self) -> None:
        self.callback = None
        self.options: dict | None = None

    def on_event_hook(self, **options):
        self.options = options

        def register(function):
            self.callback = function
            return function

        return register


def test_install_registers_one_hook_that_writes_once_per_update() -> None:
    hooks, written = _Hooks(), []
    rec = recorder()
    assert install(hooks, "spark", rec, "aal_dev_fdh", write=lambda *a: written.append(a) or 1)
    assert hooks.options == {"max_allowable_consecutive_failures": None}
    for event in loaded(7, 7, 0):
        hooks.callback(event)
    assert len(written) == 1
    spark, catalog, rows = written[0]
    assert (spark, catalog, rows[0]["rows_read"]) == ("spark", "aal_dev_fdh", 7)


def test_a_failing_write_is_raised_so_the_event_log_shows_it(caplog) -> None:
    hooks = _Hooks()

    def broken(*_args):
        raise RuntimeError("no MODIFY on load_ledger")

    install(hooks, "spark", recorder(), "aal_dev_fdh", write=broken)
    # Registered with no limit on failures, so Lakeflow records this but never fails the update.
    assert hooks.options == {"max_allowable_consecutive_failures": None}
    events = loaded(1, 1, 0)
    for event in events[:-2]:
        hooks.callback(event)
    with caplog.at_level("ERROR", logger="lakeflow_framework"), pytest.raises(RuntimeError):
        hooks.callback(events[-2])
    assert "could not record abs_daily" in caplog.text


def test_a_runtime_without_hooks_is_reported_not_ignored(monkeypatch, caplog) -> None:
    monkeypatch.setitem(sys.modules, "dlt", None)  # import dlt -> ImportError
    with caplog.at_level("ERROR", logger="lakeflow_framework"):
        assert install(object(), "spark", recorder(), "aal_dev_fdh") is False
    assert "no on_event_hook" in caplog.text


def test_the_pipeline_installs_a_recorder_for_every_table_in_the_delivery() -> None:
    config = config_from_document({**DOCUMENT, "catalog": "aal_dev_fdh"})
    hooks = _Hooks()

    spark = _Spark()
    problems = {"abs_broken": "failed to register: ValueError: bad rule"}
    _install_recorder(
        hooks,
        spark,
        ["abs_accounting", "abs_broken"],
        {"abs_accounting": config},
        problems,
        {"job_name": "abs_daily"},
    )
    assert hooks.callback is not None
    bronze, quarantine = config.table_name("bronze"), config.table_name("quarantine")
    staged = "aal_dev_fdh.framework_meta.abs_accounting_staged"
    for event in [
        update_state("RUNNING", 0),
        progress(staged, "RUNNING", 1, metrics={"num_output_rows": 20}),
        progress(staged, "COMPLETED", 2),
        progress(bronze, "RUNNING", 3, metrics={"num_output_rows": 18}),
        progress(bronze, "COMPLETED", 4),
        progress(quarantine, "RUNNING", 3, metrics={"num_output_rows": 2}),
        progress(quarantine, "COMPLETED", 4),
        update_state("COMPLETED", 9),
    ]:
        hooks.callback(event)
    assert bronze == "aal_dev_fdh.abs_bronze.finance_abs"
    assert set(spark.saved) == {"aal_dev_fdh.framework_meta.load_ledger"}
    # One row for the table that loaded and one for the table whose spec failed to register.
    columns = list(LEDGER_COLUMNS)
    loaded_row, broken_row = (
        dict(zip(columns, record, strict=True))
        for record in spark.saved["aal_dev_fdh.framework_meta.load_ledger"].data
    )
    from lakeflow_framework.pipeline import batch_id

    # The ledger keeps the _batch_id the bronze rows of this run carry, beside the update id.
    assert loaded_row["batch_id"] == broken_row["batch_id"] == batch_id()
    assert (loaded_row["rows_read"], loaded_row["rows_inserted"]) == (20, 18)
    assert (loaded_row["rows_quarantined"], loaded_row["failure_reason"]) == (2, None)
    assert broken_row["dataflow"] == "abs_broken" and broken_row["target_status"] == "FAILED"
    assert broken_row["failure_reason"] == "failed to register: ValueError: bad rule"
    assert broken_row["rows_read"] == broken_row["rows_inserted"] == 0
    assert "aal_dev_fdh.framework_meta.load_metric" not in spark.saved


def test_manifest_problems_name_the_spec_and_the_delivery(tmp_path) -> None:
    broken = {**DOCUMENT, "pipeline_id": "abs_broken", "layer": "nonsense"}
    manifest = tmp_path / "abs_daily.json"
    manifest.write_text(
        json.dumps(
            {
                "delivery": "abs_daily",
                "pipelines": [
                    {"pipeline_id": "abs_accounting", "config": DOCUMENT},
                    {"pipeline_id": "abs_broken", "config": broken},
                ],
            }
        ),
        encoding="utf-8",
    )
    problems: dict[str, str] = {}
    delivery: dict[str, str] = {}
    configured = _configs_from_manifest(
        manifest, "aal_dev_fdh", problems=problems, delivery=delivery
    )
    assert delivery == {"job_name": "abs_daily"}
    assert "abs_accounting" in configured
    if "abs_broken" not in configured:
        assert problems["abs_broken"]
    missing = _configs_from_manifest(tmp_path / "nope.json", None, problems=problems)
    assert missing == {} and "could not read delivery manifest" in problems["*"]
