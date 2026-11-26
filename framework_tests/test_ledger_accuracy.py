"""A ledger row says what happened: a file is never called empty when the run read its rows."""

from __future__ import annotations

from datetime import UTC, datetime

from lakeflow_framework.load_catchup import _batch_of
from lakeflow_framework.pipeline_metrics import apply_arrivals, apply_files

PATH = "/Volumes/c/s/v/t/20261103/a.csv"


class _Spark:
    """Answers the batch lookup: only the quarantine table holds rows of this run."""

    def __init__(self, where: dict[str, list[dict]]) -> None:
        self.where, self.asked = where, []

    def sql(self, query: str):
        table = next((name for name in self.where if f"FROM {name} " in query), None)
        self.asked.append(table)
        rows = self.where.get(table, [])
        return type("Rows", (), {"collect": lambda _s: rows})()


def _run() -> dict:
    return {
        "target_table": "c.b.t",
        "_quarantine_table": "c.q.t_rejects",
        "started_at": datetime(2026, 11, 26, 2, 14, tzinfo=UTC),
        "ended_at": datetime(2026, 11, 26, 2, 15, tzinfo=UTC),
    }


def test_a_run_that_kept_everything_out_of_bronze_finds_its_batch_in_quarantine() -> None:
    spark = _Spark({"c.b.t": [], "c.q.t_rejects": [{"batch": "224dc180"}]})
    assert _batch_of(spark, _run()) == "224dc180"
    assert spark.asked == ["c.b.t", "c.q.t_rejects"]


def test_two_batches_in_the_window_are_not_guessed() -> None:
    spark = _Spark({"c.b.t": [{"batch": "a"}, {"batch": "b"}], "c.q.t_rejects": []})
    assert _batch_of(spark, _run()) is None


def test_a_blocked_file_read_back_from_quarantine_keeps_the_rows_it_held_back() -> None:
    # 9 rows read, none loaded, 2 in quarantine: the other 7 were held back with the blocked file
    row = {"rows_read": 9, "rows_inserted": 0, "rows_quarantined": 2, "rows_dropped": 7}
    file = {
        "src_file_path": PATH,
        "src_file_name": "a.csv",
        "status": "FAILED",
        "rows_read": 2,
        "rows_inserted": 0,
        "rows_quarantined": 2,
        "rows_dropped": 0,
        "blocking_rules": "ride_id_present",
    }
    apply_files(row, [file])
    assert (file["rows_read"], file["rows_quarantined"], file["rows_dropped"]) == (9, 2, 7)
    assert row["rows_dropped"] == 7


def test_the_counts_of_one_file_are_not_stretched_when_they_do_not_add_up() -> None:
    row = {"rows_read": 12, "rows_inserted": 0, "rows_quarantined": 2, "rows_dropped": 7}
    file = {
        "src_file_path": PATH,
        "src_file_name": "a.csv",
        "status": "FAILED",
        "rows_read": 2,
        "rows_inserted": 0,
        "rows_quarantined": 2,
        "rows_dropped": 0,
    }
    apply_files(row, [file])
    assert (file["rows_read"], file["rows_dropped"]) == (2, 0)


def test_a_file_is_not_called_empty_when_the_run_read_rows_but_its_counts_were_not_found() -> None:
    row = {"rows_read": 9, "target_status": "FAILED", "_files": []}
    arrival = {
        "src_file_path": PATH,
        "src_file_name": "a.csv",
        "has_rows": False,
        "recorded_before": False,
    }
    apply_arrivals(row, [arrival])
    assert row["_files"] == [] and "no rows" not in str(row.get("failure_reason"))
    row["_files"] = []
    apply_arrivals(row, [])  # and no file is said to be missing either
    assert "no file arrived" not in str(row.get("failure_reason"))


def test_a_run_that_read_nothing_is_still_judged_as_before() -> None:
    row = {"rows_read": 0, "_files": []}
    apply_arrivals(row, [])
    assert row["target_status"] == "FAILED" and "no file arrived" in row["failure_reason"]
    empty = {"rows_read": 0, "_files": []}
    apply_arrivals(
        empty,
        [
            {
                "src_file_path": PATH,
                "src_file_name": "a.csv",
                "has_rows": False,
                "recorded_before": False,
            }
        ],
    )
    assert empty["_files"][0]["error_message"] == "file has no rows"


# --- the recorder asks for everything at once, so its write starts before the pipeline is gone ----


def test_the_three_lookups_of_the_arrivals_check_are_asked_together(monkeypatch) -> None:
    import threading

    from lakeflow_framework import pipeline_metrics as metrics

    together = threading.Barrier(3, timeout=5)  # fails the test if they are asked one by one

    class Spark:
        def sql(self, _query: str):
            together.wait()
            return type("Rows", (), {"collect": lambda _s: []})()

    def listing(_path: str, _glob: str | None) -> list[dict]:
        together.wait()
        return []

    monkeypatch.setattr(metrics, "list_volume_files", listing)
    row = {
        "_expect_file": True,
        "_path": "/Volumes/c/s/v/t/",
        "dataflow": "abs_tax",
        "layer": "bronze",
        "job_name": "abs_tax_evening",
    }
    assert metrics.arrival_inputs(Spark(), "aal_dev_fdh", row) == ([], set())


def test_the_recorders_lookups_are_asked_together_and_the_row_is_still_written(
    monkeypatch,
) -> None:
    import threading

    from lakeflow_framework import pipeline_metrics as metrics
    from test_pipeline_metrics import _Spark as Warehouse
    from test_pipeline_metrics import loaded, recorder, run

    together = threading.Barrier(3, timeout=5)  # recorded, the files, the arrivals' inputs

    def wait(result):
        def ask(*_args, **_kwargs):
            together.wait()
            return result

        return ask

    monkeypatch.setattr(metrics, "recorded_dataflows", wait(set()))
    monkeypatch.setattr(metrics, "read_files", wait([]))
    monkeypatch.setattr(metrics, "arrival_inputs", wait(None))
    (row,) = run(recorder(), loaded(1, 1, 0))
    spark = Warehouse()
    assert metrics.write_rows(spark, "aal_dev_fdh", [row]) == 1
    assert "aal_dev_fdh.framework_meta.load_ledger" in spark.saved


def test_a_row_the_update_already_recorded_is_not_written_again_even_with_lookups_in_flight(
    monkeypatch,
) -> None:
    from lakeflow_framework import pipeline_metrics as metrics
    from test_pipeline_metrics import _Spark as Warehouse
    from test_pipeline_metrics import loaded, recorder, run

    monkeypatch.setattr(metrics, "recorded_dataflows", lambda *_a: {"abs_accounting"})
    monkeypatch.setattr(metrics, "read_files", lambda *_a: [])
    monkeypatch.setattr(metrics, "arrival_inputs", lambda *_a: None)
    (row,) = run(recorder(), loaded(1, 1, 0))
    spark = Warehouse()
    assert metrics.write_rows(spark, "aal_dev_fdh", [row]) == 0 and not spark.saved


# --- a file an earlier, unrecorded update read is not called empty by the next run ---------------


def _arrived(name: str) -> dict:
    return {
        "file_path": f"/Volumes/c/s/v/t/{name}",
        "arrived_at": datetime(2026, 11, 26, 2, 32, tzinfo=UTC),
    }


class _Tables:
    """Bronze and quarantine hold these file paths."""

    def __init__(self, held: set[str]) -> None:
        self.held, self.queries = held, []

    def sql(self, query: str):
        self.queries.append(query)
        rows = [{"_src_file_path": path} for path in self.held if f"'{path}'" in query]
        return type("Rows", (), {"collect": lambda _s: rows})()


def test_a_file_read_by_an_update_with_no_ledger_row_is_not_called_empty() -> None:
    from lakeflow_framework.pipeline_metrics import read_arrivals

    seven, eight = "Tax_testing_schema_evol_7.csv", "Tax_testing_schema_evol_8.csv"
    row = {
        "target_table": "c.b.t",
        "_quarantine_table": "c.q.t",
        "_expect_file": True,
        "_path": "/x/",
    }
    files = [{"src_file_path": f"/Volumes/c/s/v/t/{eight}"}]
    spark = _Tables({f"/Volumes/c/s/v/t/{seven}"})
    inputs = ([_arrived(seven), _arrived(eight)], set())
    arrivals = read_arrivals(spark, "c", row, files, inputs=inputs)
    by_name = {item["src_file_name"]: item for item in arrivals}
    assert by_name[seven]["read_elsewhere"] and not by_name[eight]["read_elsewhere"]
    assert len(spark.queries) == 2  # bronze and quarantine, for the one file nobody counted
    assert "_ingest_timestamp >=" in spark.queries[0] and eight not in spark.queries[0]

    # the run's own row: one file, counted; the other is no business of this run
    result = {
        "rows_read": 9,
        "_files": [{"src_file_name": eight, "status": "FAILED"}],
        "files_read": 1,
        "files_failed": 1,
    }
    apply_arrivals(result, arrivals)
    assert [item["src_file_name"] for item in result["_files"]] == [eight]
    assert (result["files_read"], result["files_failed"]) == (1, 1)


def test_a_file_nobody_read_is_still_called_empty_and_costs_no_lookup_when_all_are_known() -> None:
    from lakeflow_framework.pipeline_metrics import read_arrivals

    row = {"target_table": "c.b.t", "_expect_file": True, "_path": "/x/"}
    inputs = ([_arrived("zero.csv")], set())
    arrivals = read_arrivals(_Tables(set()), "c", row, [], inputs=inputs)
    assert arrivals[0]["read_elsewhere"] is False
    known = ([_arrived("a.csv")], {"/Volumes/c/s/v/t/a.csv"})
    spark = _Tables(set())
    read_arrivals(spark, "c", row, [], inputs=known)
    assert spark.queries == []  # the ledger already knows it: nothing to look up


def test_an_arrival_another_update_read_does_not_become_no_file_arrived() -> None:
    row = {"rows_read": 0, "_files": []}
    arrival = {
        "src_file_path": PATH,
        "src_file_name": "a.csv",
        "has_rows": False,
        "recorded_before": False,
        "read_elsewhere": True,
    }
    apply_arrivals(row, [arrival])
    assert "target_status" not in row and row["_files"] == []


# --- the new work stays cheap -----------------------------------------------------------------


def test_the_checks_on_rows_a_backfill_just_wrote_only_look_at_recent_files() -> None:
    from lakeflow_framework import backfill

    class Spark:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def sql(self, query: str):
            self.queries.append(query)
            return type("Rows", (), {"collect": lambda _s: [(4,)]})()

    spark = Spark()
    began = datetime(2026, 11, 26, 3, 0, tzinfo=UTC)
    backfill._check_written(spark, "c.b.t", "b1", [PATH], 4, since=began)
    backfill._discard(spark, [PATH], "b1", "c.b.t", since=began)
    # the tables are clustered on the load time: the bound lets Delta skip every older file
    assert all(
        "_ingest_timestamp >= TIMESTAMP '2026-11-26 02:55:00+00:00'" in q for q in spark.queries
    )
    spark.queries.clear()
    backfill._check_written(spark, "c.b.t", "b1", [PATH], 4)
    assert "_ingest_timestamp" not in spark.queries[0]  # without a start, nothing is invented


def test_a_row_with_nothing_rescued_adds_nothing_to_the_rescued_keys_of_its_file() -> None:
    from lakeflow_framework.pipeline import _rescued_keys

    sql = _rescued_keys("_rescued_data")
    # collect_list leaves out a NULL: a 100,000-row file builds a list of its rescued rows only
    assert "collect_list(CASE WHEN _rescued_data IS NOT NULL THEN" in sql and "END)" in sql


def test_a_replay_asks_nothing_about_a_row_the_ledger_already_has(monkeypatch) -> None:
    from lakeflow_framework import pipeline_metrics as metrics
    from test_pipeline_metrics import _Spark as Warehouse
    from test_pipeline_metrics import loaded, recorder, run

    def never(*_args):
        raise AssertionError("asked about a row that is already recorded")

    monkeypatch.setattr(metrics, "recorded_dataflows", lambda *_a: {"abs_accounting"})
    monkeypatch.setattr(metrics, "read_files", never)
    monkeypatch.setattr(metrics, "arrival_inputs", never)
    (row,) = run(recorder(), loaded(1, 1, 0))
    assert metrics.write_rows(Warehouse(), "aal_dev_fdh", [row], replay=True) == 0


# --- rows that broke a rule and were kept are said so --------------------------------------------


def test_rows_kept_because_their_rules_only_warn_are_named_by_rule() -> None:
    from lakeflow_framework.pipeline_metrics import warning_text

    item = {
        "status": "WARNING",
        "rows_warned": 2,
        "failed_rules": "ride_id_format, ride_id_present",
        "rescued_keys": "",
    }
    said = warning_text(item)
    assert said == "2 row(s) loaded although they broke rule(s): ride_id_format, ride_id_present"
    assert "_rescued_data" not in said  # nothing was rescued: do not say it was


def test_rows_that_broke_a_rule_and_carry_a_new_column_say_both() -> None:
    from lakeflow_framework.pipeline_metrics import warning_text

    item = {
        "status": "WARNING",
        "rows_warned": 9,
        "failed_rules": "new_columns, ride_id_present",
        "rescued_keys": "sample_column_3, sample_column_4",
    }
    said = warning_text(item)
    assert said.startswith("9 row(s) loaded although they broke rule(s): ride_id_present")
    assert said.endswith("New column(s) kept in _rescued_data: sample_column_3, sample_column_4")


def test_a_value_that_did_not_fit_is_said_after_the_rules() -> None:
    from lakeflow_framework.pipeline_metrics import warning_text

    item = {
        "status": "WARNING",
        "rows_warned": 1,
        "failed_rules": "ride_id_present, schema_conformance",
    }
    assert warning_text(item).endswith(
        "A value that did not fit its column is kept in _rescued_data"
    )


def test_a_reload_with_on_fail_warn_names_the_files_whose_bad_rows_went_into_the_table() -> None:
    from lakeflow_framework import backfill
    from lakeflow_framework.backfill import Result

    result = Result("abs_tax", "rescue", on_fail="warn")
    result.files = [
        {"src_file_name": "evol_10.csv", "failed_rules": "ride_id_format, ride_id_present"},
        {
            "src_file_name": "evol_11.csv",
            "failed_rules": "new_columns",
        },  # a new column is no bad row
        {"src_file_name": "evol_12.csv", "failed_rules": ""},
    ]
    backfill._say_what_was_kept(result)
    (said,) = result.warnings
    assert "because on_fail was warn for this run" in said
    assert "evol_10.csv (ride_id_format, ride_id_present)" in said
    assert "evol_11" not in said and "evol_12" not in said

    quiet = Result("abs_tax", "rescue", on_fail="quarantine")
    quiet.files = result.files
    backfill._say_what_was_kept(quiet)
    assert quiet.warnings == []  # only the warn override keeps bad rows


# --- a file that lands while a run is going belongs to the next run ------------------------------


def test_a_file_that_landed_after_the_run_began_is_not_claimed_missing_by_it() -> None:
    from lakeflow_framework import pipeline_metrics as metrics

    began = datetime(2026, 11, 25, 8, 36, 15, tzinfo=UTC)
    row = {
        "_expect_file": True,
        "_path": "/x/",
        "started_at": began,
        "ended_at": datetime(2026, 11, 25, 8, 36, 42, tzinfo=UTC),
        "dataflow": "abs_accounting",
        "layer": "bronze",
        "job_name": "abs_evening",
    }

    class Spark:
        def sql(self, _query: str):
            return type("Rows", (), {"collect": lambda _s: []})()

    listing = [
        {"file_path": "/x/before.csv", "arrived_at": datetime(2026, 11, 25, 8, 36, 5, tzinfo=UTC)},
        # lands 12 s into the run, after Auto Loader has listed the folder
        {"file_path": "/x/during.csv", "arrived_at": datetime(2026, 11, 25, 8, 36, 27, tzinfo=UTC)},
    ]
    original = metrics.list_volume_files
    metrics.list_volume_files = lambda *_a, **_k: listing
    try:
        arrived, _ = metrics.arrival_inputs(Spark(), "c", row)
    finally:
        metrics.list_volume_files = original
    assert [item["file_path"] for item in arrived] == ["/x/before.csv"]


# --- a row kept in spite of a rule is flagged in the ledger, in the pipeline's own run ------------


def test_a_table_with_a_warn_rule_gets_the_per_file_dataset_so_its_files_can_show_warning(
    tmp_path,
) -> None:
    from dataclasses import replace

    from lakeflow_framework.config import QualityRule
    from lakeflow_framework.pipeline import has_file_counts, has_warning
    from test_sources import FILE_SOURCE, _config, _volume_spec

    config = _config(tmp_path, FILE_SOURCE, "a.yaml", _volume_spec())
    assert not has_warning(config) and not has_file_counts(config)  # no rule at all: no dataset
    warned = replace(
        config,
        rules=[QualityRule(name="id_present", expression="thing_id IS NOT NULL", action="warn")],
    )
    assert has_warning(warned) and has_file_counts(warned)
    quarantining = replace(
        config,
        rules=[
            QualityRule(name="id_present", expression="thing_id IS NOT NULL", action="quarantine")
        ],
    )
    assert not has_warning(quarantining)  # quarantined rows are in quarantine: bronze can say


def test_a_new_column_alone_does_not_need_the_per_file_dataset(tmp_path) -> None:
    from lakeflow_framework.pipeline import has_file_counts
    from test_sources import FILE_SOURCE, _config, _volume_spec

    config = _config(tmp_path, FILE_SOURCE, "a.yaml", _volume_spec())
    # new_columns warns on every csv table, and bronze keeps what it rescued
    assert not has_file_counts(config)


# --- a file refused for new columns (schema_evolution fail) --------------------------------------

DRIFT_PATH = (
    "/Volumes/aal_dev_fdh/abs_raw/tax_source_extract/20261101/Tax_testing_schema_evol_3.csv"
)
# As the event log of update 24653941 carries it (shortened): the newlines are literal "\n".
STREAM_TEXT = (
    "Flow 'aal_dev_fdh.framework_meta.abs_tax_staged' has FAILED due to a non-retryable exception; "
    "org.apache.spark.sql.streaming.StreamingQueryException: [STREAM_FAILED] Query [id = f96a] "
    "terminated with exception: UnknownFieldException: [UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE] "
    "Encountered unknown fields during parsing: [sample_column_3, sample_column_4], which can be "
    "fixed by an automatic retry: false\\n Unknown fields inside file path: "
    + DRIFT_PATH
    + "\\nRescued file schema: sample_column_3 STRING,sample_column_4 STRING SQLSTATE: KD003 "
    "SQLSTATE: XXKST\\n=== Streaming Query ==="
)


def test_the_new_columns_their_types_and_the_file_are_read_from_the_stream_error() -> None:
    from lakeflow_framework.pipeline_metrics import drift_file, drift_types, new_fields

    assert new_fields(STREAM_TEXT) == ["sample_column_3", "sample_column_4"]
    assert drift_file(STREAM_TEXT) == DRIFT_PATH
    assert drift_types(STREAM_TEXT) == {"sample_column_3": "string", "sample_column_4": "string"}
    # the same text with real newlines, as the recorder's event carries it
    plain = STREAM_TEXT.replace("\\n", "\n")
    assert drift_file(plain) == DRIFT_PATH and drift_types(plain)["sample_column_4"] == "string"
    assert (
        new_fields("some other error") == [] and drift_types("x") == {} and drift_file("x") is None
    )


def test_the_load_row_says_schema_drift_the_file_the_columns_with_types_and_what_to_do() -> None:
    from lakeflow_framework.pipeline_metrics import drift_types, new_fields_reason

    said = new_fields_reason(
        ["sample_column_3", "sample_column_4"], DRIFT_PATH, drift_types(STREAM_TEXT)
    )
    assert said.startswith(
        "schema drift: Tax_testing_schema_evol_3.csv has new column(s) the table does not have: "
        "sample_column_3 (string), sample_column_4 (string)."
    )
    assert "add the column(s) to the spec under raw_to_bronze.columns" in said
    assert (
        "- { name: sample_column_3, type: string } - { name: sample_column_4, type: string }"
        in said
    )
    assert "deploy" in said and "framework_backfill with mode failed" in said
    assert "moves the file to _failed so it is not read again" in said


def test_a_type_the_error_does_not_give_is_left_for_the_user_to_choose() -> None:
    from lakeflow_framework.pipeline_metrics import new_fields_reason

    said = new_fields_reason(["c1"], None, {})
    assert "a file has new column(s)" in said and "- { name: c1, type: <type> }" in said


def test_only_the_file_the_error_names_gets_a_row_and_it_says_how_to_fix_it() -> None:
    from lakeflow_framework.pipeline_metrics import drift_types

    row = {
        "rows_read": 0,
        "_files": [],
        "target_status": "FAILED",
        "failure_reason": "x",
        "_read_failed": True,
        "_new_fields": ["sample_column_3", "sample_column_4"],
        "_drift_file": DRIFT_PATH,
        "_drift_types": drift_types(STREAM_TEXT),
    }
    arrivals = [
        {
            "src_file_path": DRIFT_PATH,
            "src_file_name": "Tax_testing_schema_evol_3.csv",
            "has_rows": False,
            "recorded_before": False,
        },
        # another file of the same batch: not read, read by the next run, so nothing is said of it
        {
            "src_file_path": PATH,
            "src_file_name": "a.csv",
            "has_rows": False,
            "recorded_before": False,
        },
    ]
    apply_arrivals(row, arrivals)
    (item,) = row["_files"]
    assert item["src_file_path"] == DRIFT_PATH and item["status"] == "FAILED"
    assert item["error_message"].startswith(
        "schema drift: new column(s) the table does not have: sample_column_3 (string), "
        "sample_column_4 (string) (schema_evolution fail). row count unknown; moved to _failed."
    )
    assert "- { name: sample_column_3, type: string }" in item["error_message"]
    assert "no rows" not in row["failure_reason"] and (row["files_read"], row["files_failed"]) == (
        1,
        1,
    )


def test_a_read_that_failed_for_another_reason_claims_nothing_about_any_file() -> None:
    row = {
        "rows_read": 0,
        "_files": [],
        "_read_failed": True,
        "_new_fields": [],
        "_drift_file": None,
    }
    arrivals = [
        {
            "src_file_path": PATH,
            "src_file_name": "a.csv",
            "has_rows": False,
            "recorded_before": False,
        },
    ]
    apply_arrivals(row, arrivals)
    assert row["_files"] == [] and "no rows" not in str(row.get("failure_reason"))


def test_a_drifted_file_is_parked_by_the_job_like_a_blocked_one() -> None:
    from lakeflow_framework.failed_files import DRIFT_MESSAGE as PARKED
    from lakeflow_framework.failed_files import blocked_paths_sql
    from lakeflow_framework.pipeline_metrics import DRIFT_MESSAGE as WRITTEN

    assert PARKED == WRITTEN  # the parking reads exactly what the ledger writes
    sql = blocked_paths_sql("aal_dev_fdh", "abs_tax")
    assert "error_message LIKE 'blocked by fail rule%'" in sql
    assert "OR error_message LIKE 'schema drift%'" in sql


def test_a_table_that_stops_on_drift_gets_the_park_task(tmp_path) -> None:
    from dataclasses import replace

    from lakeflow_framework.pipeline import stops_on_drift
    from test_sources import FILE_SOURCE, _config, _volume_spec

    config = _config(tmp_path, FILE_SOURCE, "a.yaml", _volume_spec())
    assert not stops_on_drift(config)
    assert stops_on_drift(replace(config, schema_evolution="fail"))
    assert not stops_on_drift(replace(config, schema_evolution="fail", file_format="parquet"))


def test_the_recorder_gives_the_drift_reason_and_the_file_row_for_a_run_stopped_by_new_columns() -> (
    None
):
    from test_pipeline_metrics import dataflow, progress, recorder, update_state

    rec = recorder(dataflow())
    events = [
        update_state("RUNNING", 0),
        progress("abs_accounting_staged", "RUNNING", 1),
        {
            "event_type": "flow_progress",
            "timestamp": "2026-11-19T10:00:02.000Z",
            "level": "ERROR",
            "origin": {"flow_name": "abs_accounting_staged", "pipeline_id": "p"},
            "message": STREAM_TEXT.replace("\\n", "\n"),
            "details": {"flow_progress": {"status": "FAILED"}},
        },
        update_state("FAILED", 3),
    ]
    rows = None
    for event in events:
        event.setdefault("origin", {})["update_id"] = "u-1"
        rows = rec.handle(event) or rows
    (row,) = rows
    assert row["failure_reason"].startswith(
        "schema drift: Tax_testing_schema_evol_3.csv has new column"
    )
    assert row["_read_failed"] is True and row["_drift_file"] == DRIFT_PATH
    assert row["_drift_types"] == {"sample_column_3": "string", "sample_column_4": "string"}


# --- a drifted file: FAILED, its rows counted and held back, recorded by the job ------------------


def test_a_table_skipped_because_the_update_failed_is_failed_not_skipped() -> None:
    from lakeflow_framework.pipeline_metrics import _Flow, _status

    skipped = _Flow()
    skipped.status = "SKIPPED"
    assert _status(skipped, "FAILED", None) == "FAILED"
    assert _status(skipped, "CANCELED", None) == "CANCELED"
    assert _status(skipped, "COMPLETED", None) == "SKIPPED"  # a healthy update that skipped it


def test_the_drifted_files_rows_are_read_and_held_back_and_the_message_says_how_many() -> None:
    from lakeflow_framework.pipeline_metrics import add_drift_file, drift_types

    row = {
        "rows_read": 0,
        "rows_dropped": 0,
        "_files": [],
        "_new_fields": ["sample_column_3", "sample_column_4"],
        "_drift_file": DRIFT_PATH,
        "_drift_types": drift_types(STREAM_TEXT),
        "_drift_rows": 9,
    }
    add_drift_file(row)
    (item,) = row["_files"]
    assert (
        item["rows_read"],
        item["rows_inserted"],
        item["rows_quarantined"],
        item["rows_dropped"],
    ) == (9, 0, 0, 9)
    assert "9 row(s) in the file, none loaded; moved to _failed" in item["error_message"]
    assert (row["rows_read"], row["rows_dropped"], row["files_read"], row["files_failed"]) == (
        9,
        9,
        1,
        1,
    )


def test_the_pipeline_hook_leaves_a_drift_row_to_the_job_which_can_count_the_file() -> None:
    from lakeflow_framework.pipeline_metrics import install

    written: list = []

    class Pipelines:
        def on_event_hook(self, **_kwargs):
            def register(function):
                self.function = function
                return function

            return register

    class Recorder:
        job_name = "abs_tax_evening"

        def __init__(self, rows):
            self.rows = rows

        def handle(self, _event):
            return self.rows

    pipelines = Pipelines()
    install(
        pipelines,
        None,
        Recorder([{"update_id": "u", "_drift_file": DRIFT_PATH}]),
        "c",
        write=lambda *_a: written.append(1) or 1,
    )
    pipelines.function({})
    assert written == []  # left for park_failed_files
    pipelines = Pipelines()
    install(
        pipelines,
        None,
        Recorder([{"update_id": "u"}]),
        "c",
        write=lambda *_a: written.append(1) or 1,
    )
    pipelines.function({})
    assert written == [1]  # any other run is still written by the hook


def test_the_job_recording_its_own_update_keeps_it_regular(monkeypatch) -> None:
    from lakeflow_framework import pipeline_metrics as metrics
    from test_pipeline_metrics import _Spark as Warehouse
    from test_pipeline_metrics import loaded, recorder, run

    monkeypatch.setattr(metrics, "recorded_dataflows", lambda *_a: set())
    monkeypatch.setattr(metrics, "read_files", lambda *_a: [])
    monkeypatch.setattr(metrics, "arrival_inputs", lambda *_a: None)
    monkeypatch.setattr(metrics, "_unrecorded_files", lambda _s, _m, _r, files: files)
    (row,) = run(recorder(), loaded(1, 1, 0))
    metrics.write_rows(Warehouse(), "aal_dev_fdh", [row], replay=True, keep_kind=True)
    assert row["load_kind"] == "regular"
    (again,) = run(recorder(), loaded(1, 1, 0))
    metrics.write_rows(Warehouse(), "aal_dev_fdh", [again], replay=True)
    assert again["load_kind"] == "catchup"  # the hourly catch-up still says so


def test_the_load_rows_reason_says_how_many_rows_schema_drift_kept_out() -> None:
    from lakeflow_framework.pipeline_metrics import add_drift_file, drift_types, new_fields_reason

    def row(count):
        return {
            "target_status": "SKIPPED",
            "failure_reason": new_fields_reason(
                ["sample_column_3"], DRIFT_PATH, drift_types(STREAM_TEXT)
            ),
            "_files": [],
            "_new_fields": ["sample_column_3"],
            "_drift_file": DRIFT_PATH,
            "_drift_rows": count,
        }

    counted = add_drift_file(row(9))
    assert counted["target_status"] == "FAILED"
    assert "none of the file's 9 row(s) were read into the table" in counted["failure_reason"]
    unknown = add_drift_file(row(None))
    assert (
        "no rows were read into the table (the file's row count is unknown)"
        in unknown["failure_reason"]
    )


# --- a listed file that was moved before it was read ---------------------------------------------

GONE = "/Volumes/aal_dev_fdh/abs_raw/tax_source_extract/20261101/Tax_testing_schema_evol_3.csv"
NOT_FOUND = (
    "Flow 'aal_dev_fdh.framework_meta.abs_tax_staged' has FAILED more than 2 times and will not be "
    "restarted.; org.apache.spark.sql.streaming.StreamingQueryException: [STREAM_FAILED] Query "
    "terminated with exception: [FAILED_READ_FILE.DBR_FILE_NOT_EXIST] Error while reading file "
    f"{GONE}. [CLOUD_FILE_SOURCE_FILE_NOT_FOUND] A file notification was received for file: {GONE} "
    "but it does not exist anymore. Please ensure that files are not deleted before they are processed."
)


def test_a_file_moved_after_auto_loader_listed_it_is_named_in_the_reason() -> None:
    from lakeflow_framework.pipeline_metrics import missing_file, missing_file_reason

    assert missing_file(NOT_FOUND) == GONE
    assert missing_file("other error") is None
    said = missing_file_reason(GONE)
    assert said.startswith(
        "file not found: Tax_testing_schema_evol_3.csv was listed by Auto Loader"
    )
    assert "moved or deleted before it was read" in said and "_failed" in said


def test_auto_loader_skips_a_listed_file_that_is_no_longer_there(tmp_path) -> None:
    from lakeflow_framework.load import get_strategy
    from test_sources import FILE_SOURCE, _config, _volume_spec

    config = _config(tmp_path, FILE_SOURCE, "a.yaml", _volume_spec())
    assert get_strategy("volume_files").options(config)["ignoreMissingFiles"] == "true"
