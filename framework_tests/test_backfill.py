"""Backfill: what is asked for, what is refused, and what the ledger says afterwards."""

from __future__ import annotations

import dataclasses
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
from lakeflow_framework.pipeline_metrics import DETAIL_COLUMNS, FILE_COLUMNS, file_records
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
        BackfillError, match="mode must be one of files, dates, failed, rescue, full_refresh"
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


JOB = (
    Path(__file__).resolve().parents[1] / "framework_bundle" / "resources" / "framework_ops.job.yml"
)


def job_defaults() -> dict[str, str]:
    """The backfill job's parameter defaults, with the bundle's catalog filled in."""

    import yaml

    job = yaml.safe_load(JOB.read_text(encoding="utf-8"))["resources"]["jobs"]["framework_backfill"]
    return {
        item["name"]: str(item["default"]).replace("${var.catalog}", "aal_dev_fdh")
        for item in job["parameters"]
    }


def test_the_job_shows_patterns_for_the_user_to_edit() -> None:
    defaults = job_defaults()
    assert defaults["table"] == "aal_dev_fdh.<bronze_schema>.<table>"
    assert defaults["files"] == "/Volumes/aal_dev_fdh/<raw_schema>/<volume>/<folder>/<file>.csv"
    # the mode lists every choice, so it must be picked: the default is refused as it stands
    assert defaults["mode"] == "<files|dates|failed|rescue|full_refresh>"
    with pytest.raises(BackfillError, match="mode still holds the pattern.*choose one of"):
        parse_selection(defaults["mode"])
    # nothing else changes: a run is a dry run until someone says otherwise
    assert (defaults["dry_run"], defaults["confirm"]) == ("true", "false")
    assert defaults["date_from"] == "" and defaults["date_to"] == ""


def test_the_default_files_pattern_is_refused_in_mode_files_and_ignored_elsewhere() -> None:
    pattern = job_defaults()["files"]
    with pytest.raises(BackfillError, match="files still holds the pattern.*replace the parts"):
        parse_selection("files", pattern)
    # another mode leaves the pattern alone, so editing only table and mode is enough
    assert parse_selection("failed", pattern).files == ()
    assert parse_selection("full_refresh", pattern).files == ()
    assert parse_selection("dates", pattern, "2026-11-01").files == ()
    assert backfill.is_pattern(pattern) and not backfill.is_pattern(f"{FOLDER}a.csv")


def test_a_pattern_beside_a_real_path_is_still_refused_in_mode_files() -> None:
    with pytest.raises(BackfillError, match="still holds the pattern"):
        parse_selection("files", f"{FOLDER}a.csv,{job_defaults()['files']}")


def test_the_edited_pattern_is_a_valid_path() -> None:
    edited = (
        job_defaults()["files"]
        .replace("<raw_schema>", "abs_raw")
        .replace("<volume>", "gl_source_extract")
        .replace("<folder>", "2026")
        .replace("<file>", "a")
    )
    assert edited == "/Volumes/aal_dev_fdh/abs_raw/gl_source_extract/2026/a.csv"
    assert parse_selection("files", edited).paths == (edited,)


def test_the_default_table_pattern_is_refused_with_what_to_put_there(monkeypatch) -> None:
    with pytest.raises(BackfillError, match="table still holds the pattern.*full name"):
        backfill.load_config(None, "aal_dev_fdh", job_defaults()["table"])


def test_clearing_files_still_means_every_file_and_needs_confirm() -> None:
    assert parse_selection("files", "").everything


def _fail_config(tmp_path: Path):
    """A table whose rule fails the load when a row breaks it."""

    rules = (
        "  rules:\n"
        '    - { name: id_present, expression: "thing_id IS NOT NULL" }\n'
        "    - { name: id_format, expression: \"thing_id rlike '^[A-Z]+$'\", on_fail: fail }\n"
    )
    return _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec(extra=rules))


def test_the_job_has_an_on_fail_parameter_that_is_empty_by_default_and_passed_to_the_task() -> None:
    import yaml

    job = yaml.safe_load(JOB.read_text(encoding="utf-8"))["resources"]["jobs"]["framework_backfill"]
    assert {item["name"]: item["default"] for item in job["parameters"]}["on_fail"] == ""
    task_parameters = job["tasks"][0]["spark_python_task"]["parameters"]
    assert task_parameters[-1] == "{{job.parameters.on_fail}}" and len(task_parameters) == 9
    assert "rescued_columns" not in {item["name"] for item in job["parameters"]}


def test_on_fail_gives_every_rule_and_every_added_check_one_action_for_the_run(
    tmp_path: Path,
) -> None:
    from lakeflow_framework.quality import blocking_rules, compile_rules

    config = _fail_config(tmp_path)
    assert blocking_rules(compile_rules(config)) == ["id_format"]  # the table's own: fail
    changed = backfill.with_on_fail(config, "quarantine")
    assert blocking_rules(compile_rules(changed)) == []  # nothing blocks a file now
    assert {r.name: r.action for r in compile_rules(changed) if r.name == "new_columns"} == {
        "new_columns": "warn"  # a new column is rescued and warned, whatever on_fail says
    }
    assert {r.action for r in compile_rules(changed) if r.name != "new_columns"} == {"quarantine"}
    assert [rule.action for rule in config.rules] == [
        "quarantine",
        "fail",
    ]  # the table is unchanged
    assert backfill.with_on_fail(config, "") is config  # empty keeps the table's own


def test_a_full_refresh_resets_the_per_file_dataset_of_a_table_that_drops_rows(
    tmp_path: Path,
) -> None:
    from lakeflow_framework.backfill import Result, _full_refresh

    rules = (
        "  on_fail: drop\n"
        "  rules:\n"
        '    - { name: id_present, expression: "thing_id IS NOT NULL" }\n'
    )
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec(extra=rules))
    result = _full_refresh(
        None, config, Result("files_things", "full_refresh", dry_run=True), confirm=True
    )
    assert any(item.endswith(".framework_meta.files_things_files") for item in result.planned)
    # nothing blocks a file here, so there is no check to reset
    assert not any(item.endswith("_blocked_check") for item in result.planned)


def test_a_value_that_is_not_an_action_is_refused() -> None:
    with pytest.raises(BackfillError, match="on_fail must be one of quarantine, drop, warn, fail"):
        backfill.with_on_fail(None, "ignore")


def test_the_reload_is_judged_by_the_override_not_the_table(tmp_path: Path, monkeypatch) -> None:
    from lakeflow_framework.quality import blocking_rules, compile_rules

    config = _fail_config(tmp_path)
    judged: list = []
    monkeypatch.setattr(backfill, "load_config", lambda *_args: config)
    monkeypatch.setattr(
        backfill, "list_volume_files", lambda path, glob=None: _listed(f"{FOLDER}a.csv")
    )
    monkeypatch.setattr(backfill, "list_parked", lambda root: [])
    monkeypatch.setattr(backfill, "_count_existing", lambda *a: 0)
    monkeypatch.setattr(
        backfill,
        "_load",
        lambda spark, cfg, *a, **k: judged.append(blocking_rules(compile_rules(cfg))),
    )
    monkeypatch.setattr(backfill, "_recorded_files", lambda *a: [])
    monkeypatch.setattr(backfill, "_record", lambda *a, **k: None)
    selection = parse_selection("files", f"{FOLDER}a.csv")
    backfill.run_backfill(None, "framework_dev", "files_things", selection)
    result = backfill.run_backfill(
        None, "framework_dev", "files_things", selection, on_fail="quarantine"
    )
    assert judged == [["id_format"], []]  # blocked by the table's own rule, then not blocked
    assert result.on_fail == "quarantine"
    assert "on_fail was quarantine for this run, not the table's own" in backfill.warning_text(
        result
    )


def test_the_dry_run_says_which_action_it_would_use(tmp_path: Path, monkeypatch) -> None:
    config = _fail_config(tmp_path)
    monkeypatch.setattr(backfill, "load_config", lambda *_args: config)
    monkeypatch.setattr(
        backfill, "list_volume_files", lambda path, glob=None: _listed(f"{FOLDER}a.csv")
    )
    monkeypatch.setattr(backfill, "list_parked", lambda root: [])
    result = backfill.run_backfill(
        None,
        "framework_dev",
        "files_things",
        parse_selection("files", f"{FOLDER}a.csv"),
        dry_run=True,
        on_fail="drop",
    )
    assert "on_fail would be drop for this run" in backfill.warning_text(result)


def test_a_full_refresh_refuses_on_fail_because_it_runs_the_tables_own_pipeline(
    tmp_path: Path, monkeypatch
) -> None:
    config = _fail_config(tmp_path)
    monkeypatch.setattr(backfill, "load_config", lambda *_args: config)
    with pytest.raises(BackfillError, match="full_refresh runs the table's own pipeline"):
        backfill.run_backfill(
            None,
            "framework_dev",
            "files_things",
            parse_selection("full_refresh"),
            dry_run=True,
            on_fail="quarantine",
        )


FOLDER = "/Volumes/framework_dev/files_raw/landing/things/"
VOLUME = "/Volumes/framework_dev/files_raw/landing"


def _listed(*paths: str) -> list[dict]:
    return [
        {"file_path": path, "file_bytes": 1, "arrived_at": datetime(2026, 11, 20, tzinfo=UTC)}
        for path in paths
    ]


def _plan_files(tmp_path: Path, monkeypatch, files: str, *, listed: list[str], parked=()):
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    monkeypatch.setattr(backfill, "list_volume_files", lambda path, glob=None: _listed(*listed))
    monkeypatch.setattr(backfill, "list_parked", lambda root: list(parked))
    selection = parse_selection("files", files)
    return (
        config,
        selection,
        [item["path"] for item in backfill.plan_files(None, config, selection)],
    )


def test_a_file_can_be_given_by_its_full_volume_path() -> None:
    selection = parse_selection("files", f"{FOLDER}a.csv, b.csv")
    assert selection.files == (f"{FOLDER}a.csv", "b.csv")
    assert selection.paths == (f"{FOLDER}a.csv",) and selection.names == ("b.csv",)


def test_a_path_that_is_not_a_full_volume_path_or_a_pattern_is_refused() -> None:
    for bad in ("sub/a.csv", "/tmp/a.csv", f"{FOLDER}*.csv", f"{VOLUME}/../x/a.csv", f"{FOLDER}"):
        with pytest.raises(BackfillError, match="file name or a full volume path|full volume path"):
            parse_selection("files", bad)


def test_the_source_filters_on_the_names_inside_the_paths() -> None:
    selection = parse_selection("files", f"{FOLDER}a.csv,{FOLDER}sub/a.csv,b.csv")
    assert backfill.reader_filters(selection)["pathGlobFilter"] == "{a.csv,b.csv}"


def test_a_full_path_loads_exactly_that_file_not_a_file_of_the_same_name(
    tmp_path: Path, monkeypatch
) -> None:
    listed = [f"{FOLDER}a.csv", f"{FOLDER}old/a.csv"]
    _c, _s, planned = _plan_files(tmp_path, monkeypatch, f"{FOLDER}old/a.csv", listed=listed)
    assert planned == [f"{FOLDER}old/a.csv"]


def test_a_bare_name_still_matches_every_file_of_that_name(tmp_path: Path, monkeypatch) -> None:
    listed = [f"{FOLDER}a.csv", f"{FOLDER}old/a.csv", f"{FOLDER}b.csv"]
    _c, _s, planned = _plan_files(tmp_path, monkeypatch, "a.csv", listed=listed)
    assert planned == [f"{FOLDER}a.csv", f"{FOLDER}old/a.csv"]


def test_the_path_of_a_parked_file_works_as_given_or_as_it_was_before(
    tmp_path: Path, monkeypatch
) -> None:
    original = f"{FOLDER}sub/bad.csv"
    in_failed = f"{VOLUME}/_failed/things/sub/bad.csv"
    for given in (original, in_failed):
        _c, selection, planned = _plan_files(
            tmp_path, monkeypatch, given, listed=[f"{FOLDER}good.csv"], parked=[original]
        )
        assert planned == [original]  # planned from where it was, so it is moved back and loaded
        assert backfill._not_found(selection, planned) == []


def test_a_path_that_is_nowhere_is_reported_as_it_was_given(tmp_path: Path, monkeypatch) -> None:
    missing = f"{FOLDER}nothing.csv"
    _c, selection, planned = _plan_files(tmp_path, monkeypatch, missing, listed=[f"{FOLDER}a.csv"])
    assert planned == [] and backfill._not_found(selection, planned) == [missing]


def test_another_tables_file_is_refused_even_by_its_full_path(tmp_path: Path, monkeypatch) -> None:
    config, _s, _p = _plan_files(tmp_path, monkeypatch, "a.csv", listed=[])
    for other in (f"{VOLUME}/credits/a.csv", f"{VOLUME}/_failed/credits/a.csv"):
        with pytest.raises(BackfillError, match="not in this table's folder"):
            backfill.check_files_belong(config, parse_selection("files", other))
    backfill.check_files_belong(config, parse_selection("files", f"{FOLDER}a.csv"))
    backfill.check_files_belong(config, parse_selection("files", f"{VOLUME}/_failed/things/a.csv"))


class _Registry:
    """Stands in for the registry: the table specs it holds, and which tables map to which."""

    def __init__(self, specs: dict[str, dict], tables: list[tuple[str, str]]) -> None:
        self.specs, self.tables, self.queries = specs, tables, []

    def sql(self, statement: str):
        self.queries.append(statement)
        rows = [{"full_name": name, "pipeline_id": pid} for name, pid in self.tables]
        return type("R", (), {"collect": lambda _self: rows})()


def _load_as(tmp_path: Path, monkeypatch, name: str, registry: _Registry):
    from lakeflow_framework import pipeline

    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    document = config.canonical()

    class Reader:
        def __init__(self, spark, catalog=None):
            pass

        def get_pipeline(self, pipeline_id):
            return document if pipeline_id in registry.specs else None

    monkeypatch.setattr(pipeline, "UnityMetadata", Reader)
    return backfill.load_config(registry, "framework_dev", name)


BRONZE = "framework_dev.files_bronze.things"
QUARANTINE = "framework_dev.files_quarantine.things_qrntn"


def test_the_pipeline_id_works_and_asks_the_registry_nothing_else(
    tmp_path: Path, monkeypatch
) -> None:
    registry = _Registry({"files_things": {}}, [(BRONZE, "files_things")])
    config = _load_as(tmp_path, monkeypatch, "files_things", registry)
    assert config.pipeline_id == "files_things" and registry.queries == []


def test_the_bronze_table_name_finds_its_pipeline_bare_or_full(tmp_path: Path, monkeypatch) -> None:
    registry = _Registry(
        {"files_things": {}}, [(BRONZE, "files_things"), (QUARANTINE, "files_things")]
    )
    for name in ("things", "files_bronze.things", BRONZE):
        assert _load_as(tmp_path, monkeypatch, name, registry).pipeline_id == "files_things"
    # the quarantine table leads to the same pipeline
    assert _load_as(tmp_path, monkeypatch, "things_qrntn", registry).pipeline_id == "files_things"


def test_a_name_that_only_looks_like_a_table_is_not_matched(tmp_path: Path, monkeypatch) -> None:
    # LIKE treats _ as a wildcard: the registry returns thingsX, and it must not count as things_
    registry = _Registry(
        {"files_things": {}}, [("framework_dev.files_bronze.thingsX", "files_things")]
    )
    with pytest.raises(BackfillError, match="things_ is not in the registry"):
        _load_as(tmp_path, monkeypatch, "things_", registry)


def test_a_name_two_pipelines_share_must_be_given_as_the_pipeline_id(
    tmp_path: Path, monkeypatch
) -> None:
    registry = _Registry(
        {"one": {}, "two": {}},
        [("c.s1.orders", "one"), ("c.s2.orders", "two")],
    )
    with pytest.raises(
        BackfillError, match="orders matches more than one table.*give the table id"
    ):
        _load_as(tmp_path, monkeypatch, "orders", registry)


def test_an_unknown_name_says_what_to_look_for(tmp_path: Path, monkeypatch) -> None:
    registry = _Registry({}, [])
    with pytest.raises(BackfillError, match="not in the registry as a table id or a table name"):
        _load_as(tmp_path, monkeypatch, "nothing", registry)


def test_a_name_with_anything_unsafe_in_it_is_refused_before_a_query(
    tmp_path: Path, monkeypatch
) -> None:
    registry = _Registry({}, [])
    with pytest.raises(BackfillError, match="is not a pipeline id or a table name"):
        _load_as(tmp_path, monkeypatch, "x'; DROP TABLE y; --", registry)
    assert registry.queries == []


class _Spark:
    def __init__(self, deleted: int = 3):
        self.statements: list[str] = []
        self.deleted = deleted

    def sql(self, statement: str):
        self.statements.append(statement)
        if statement.startswith("DELETE"):
            return _Rows([(self.deleted,)])
        return _Rows([])


class _Ledger(_Spark):
    """Answers the lookup of the table's pipeline from the ledger."""

    def __init__(self, pipeline: str | None) -> None:
        super().__init__()
        self.pipeline = pipeline

    def sql(self, statement: str):
        if "update_pipeline_uuid FROM" in statement:
            return _Rows([(self.pipeline,)] if self.pipeline else [])
        return super().sql(statement)


def _recorded_row(tmp_path: Path, monkeypatch, spark) -> dict:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    written: list[dict] = []
    monkeypatch.setattr(
        backfill, "write_rows", lambda _spark, _catalog, rows: written.extend(rows) or 1
    )
    result = Result("files_things", "files")
    result.files = [_detail_row("a.csv", rows_read=5, rows_inserted=5)]
    backfill._record(
        spark, config, result, started=datetime(2026, 11, 24, tzinfo=UTC), update_id="backfill-1"
    )
    return written[0]


def test_a_backfill_row_carries_the_tables_pipeline_so_v_loads_can_name_it(
    tmp_path: Path, monkeypatch
) -> None:
    row = _recorded_row(tmp_path, monkeypatch, _Ledger("4b334ee2-3245-4b93-9f29-388e5990870c"))
    assert row["update_pipeline_uuid"] == "4b334ee2-3245-4b93-9f29-388e5990870c"


def test_a_table_that_never_loaded_is_still_recorded_without_a_pipeline(
    tmp_path: Path, monkeypatch
) -> None:
    row = _recorded_row(tmp_path, monkeypatch, _Ledger(None))
    assert row["update_pipeline_uuid"] is None and row["load_kind"] == "backfill"


def test_v_loads_names_the_pipeline_of_a_load_that_was_not_a_pipeline_update() -> None:
    ddl = (JOB.parents[1] / "sql" / "framework_meta.sql").read_text(encoding="utf-8")
    view = ddl.split("framework_meta.v_loads AS", 1)[1].split("CREATE OR REPLACE VIEW", 1)[0]
    # the update's own name first, then the pipeline's name by id, then by job
    assert "coalesce(pu.pipeline_name, by_id.pipeline_name, by_job.pipeline_name)" in view
    # one row per key in each, so no load is repeated
    assert "GROUP BY pipeline_id" in view and "GROUP BY job_name" in view


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
    # None of the blocked file's rows loaded: its bad rows are in quarantine, its good rows dropped.
    assert (row["rows_read"], row["rows_inserted"], row["rows_quarantined"]) == (30, 20, 2)
    assert (row["rows_dropped"], row["rows_deleted"]) == (8, 20)
    assert row["target_status"] == "FAILED"
    assert row["failure_reason"].startswith("backfill: 1 of 3 file(s) blocked")
    assert "b.csv (amount_positive)" in row["failure_reason"]
    assert row["update_id"] == "backfill-1" and row["load_kind"] == "backfill"
    # The files were written to ledger_detail by each micro-batch, so the table's write adds none.
    assert row["_files"] == []
    records = file_records([{**row, "_files": result.files}], datetime(2026, 11, 19, tzinfo=UTC))
    kinds = {dict(zip(FILE_COLUMNS, r, strict=True))["load_kind"] for r in records}
    assert kinds == {"backfill"} and len(records) == 3


def _detail_row(name: str, **values) -> dict:
    """A file as ledger_detail holds it, which is how a backfill reads its own files back."""

    row = dict.fromkeys(DETAIL_COLUMNS)
    row.update(
        update_id="backfill-1",
        dataflow="files_things",
        detail_kind="file",
        status="PROCESSED",
        src_file_path=f"/Volumes/framework_dev/files_raw/landing/things/{name}",
        src_file_name=name,
        rows_read=0,
        rows_inserted=0,
        rows_quarantined=0,
        rows_dropped=0,
        failed_rules="",
        blocking_rules="",
        load_kind="backfill",
    )
    row.update(values)
    return row


def test_the_final_record_reads_the_files_as_ledger_detail_holds_them(
    tmp_path: Path, monkeypatch
) -> None:
    # ledger_detail has rows_inserted, and no rows_loaded or rows_blocked: the end of a backfill once
    # failed with KeyError: 'rows_loaded' because it asked for a column that only files_sql has
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    written: list[dict] = []
    monkeypatch.setattr(
        backfill, "write_rows", lambda _spark, _catalog, rows: written.extend(rows) or 1
    )
    result = Result("files_things", "files")
    result.files = [
        _detail_row("a.csv", rows_read=5, rows_inserted=5),
        _detail_row(
            "b.csv",
            status="FAILED",
            rows_read=80000,
            rows_quarantined=3,
            rows_dropped=79997,
            failed_rules="amt",
            blocking_rules="amt",
            error_message="blocked by fail rule(s): amt",
        ),
    ]
    result.rows_replaced = 5
    backfill._record(
        _Spark(), config, result, started=datetime(2026, 11, 24, tzinfo=UTC), update_id="backfill-1"
    )
    (row,) = written
    assert (row["rows_read"], row["rows_inserted"], row["rows_quarantined"]) == (80005, 5, 3)
    assert row["rows_dropped"] == 79997 and row["rows_deleted"] == 5
    assert (row["files_read"], row["files_failed"]) == (2, 1)
    assert row["target_status"] == "FAILED" and row["load_kind"] == "backfill"


def test_only_the_names_that_are_not_in_the_directory_are_reported_missing() -> None:
    selection = parse_selection("files", "a.csv,gone.csv")
    assert backfill._not_found(selection, ["/v/a.csv", "/v/b.csv"]) == ["gone.csv"]
    assert backfill._not_found(parse_selection("dates", date_from="2026-11-01"), []) == []
    assert (
        backfill._not_found(parse_selection("files"), []) == []
    )  # no names: nothing was asked for


def _record_with(tmp_path: Path, monkeypatch, result: Result, expect_file: bool = False) -> dict:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    config = dataclasses.replace(config, expect_file=expect_file)
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
    row = _record_with(tmp_path, monkeypatch, result, expect_file=True)
    assert row["target_status"] == "FAILED" and row["files_failed"] == 1
    assert row["_files"][0]["error_message"] == "file has no rows"
    assert row["rows_read"] == 10  # the good file's rows are still counted


def test_an_empty_file_is_only_a_warning_for_a_table_that_does_not_expect_one(
    tmp_path: Path, monkeypatch
) -> None:
    result = Result("files_things", "files")
    result.files = [_clean("a.csv")]
    result.empty = ["/Volumes/c/s/v/empty.csv"]
    row = _record_with(tmp_path, monkeypatch, result, expect_file=False)
    assert row["target_status"] == "COMPLETED" and row["files_failed"] == 0


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


# --- mode rescue: read named files again, and keep what the table lacks in _rescued_data -------


class _Collected:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def collect(self) -> list[dict]:
        return self.rows


class _RescueSpark:
    """bronze has these columns. ``left`` answers the check made after a reload."""

    def __init__(self, columns: list[str], left: list[dict] | None = None) -> None:
        self.columns, self.left, self.queries = columns, left or [], []

    def table(self, _name: str):
        return type("Frame", (), {"columns": self.columns})()

    def sql(self, query: str) -> _Collected:
        self.queries.append(query)
        return _Collected(self.left)


BRONZE_COLUMNS = ["thing_id", "_rescued_data", "_src_file_path"]
PATH = "/Volumes/c/s/v/things/a.csv"


def test_mode_rescued_takes_file_names_and_nothing_else() -> None:
    named = parse_selection("rescue", "a.csv,/Volumes/c/s/v/things/b.csv")
    assert named.mode == "rescue" and named.files == ("a.csv", "/Volumes/c/s/v/things/b.csv")
    with pytest.raises(BackfillError, match="needs the files to read again"):
        parse_selection("rescue")
    with pytest.raises(BackfillError, match="takes file names, not dates"):
        parse_selection("rescue", "a.csv", date_from="2026-11-01")
    with pytest.raises(TypeError):
        parse_selection("rescue", "a.csv", rescued_columns="a")  # type: ignore[call-arg]


def test_the_files_pattern_the_job_defaults_to_must_be_edited_in_mode_rescued() -> None:
    pattern = "/Volumes/c/<raw_schema>/<volume>/<folder>/<file>.csv"
    with pytest.raises(BackfillError, match="still holds the pattern"):
        parse_selection("rescue", pattern)


class _Frame:
    def __init__(self) -> None:
        self.exprs: tuple[str, ...] = ()

    def selectExpr(self, *exprs: str):
        self.exprs = exprs
        return self


def test_columns_the_table_lacks_go_back_into_rescued_data_and_leave_the_row() -> None:
    frame = _Frame()
    backfill.fold_into_rescued(frame, ["sample_column_1", "odd`name"])
    except_list, rescued = frame.exprs
    assert except_list == "* EXCEPT (`sample_column_1`, `odd``name`, _rescued_data)"
    assert "map('sample_column_1', CAST(`sample_column_1` AS STRING), 'odd`name'" in rescued
    assert "map_filter(" in rescued and "value IS NOT NULL" in rescued
    assert "from_json(_rescued_data, 'map<string,string>')" in rescued  # merged, not replaced
    assert rescued.startswith("CASE WHEN size(") and rescued.endswith("AS _rescued_data")


def test_a_column_the_table_still_lacks_is_a_warning_that_says_to_run_it_again(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    result = Result("files_things", "rescue", planned=[PATH], not_in_table=["sample_column_1"])
    backfill._say_what_is_rescued(
        _RescueSpark(BRONZE_COLUMNS, left=[{"n": 9, "keys": ["sample_column_1"]}]), config, result
    )
    (warning,) = result.warnings
    assert "1 column(s) in the file(s) are not in the table yet: sample_column_1" in warning
    assert "kept in _rescued_data" in warning and "then run this again" in warning


def test_a_value_that_does_not_fit_its_type_is_reported_apart_from_a_missing_column(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    result = Result("files_things", "rescue", planned=[PATH], not_in_table=["new_col"])
    backfill._say_what_is_rescued(
        _RescueSpark(BRONZE_COLUMNS, left=[{"n": 3, "keys": ["new_col", "started_at"]}]),
        config,
        result,
    )
    assert len(result.warnings) == 2
    assert "rows still hold rescued values for: started_at" in result.warnings[1]
    assert "new_col" not in result.warnings[1]


def test_nothing_is_said_when_the_reload_left_nothing_rescued(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    result = Result("files_things", "rescue", planned=[PATH])
    backfill._say_what_is_rescued(_RescueSpark(BRONZE_COLUMNS), config, result)
    assert result.warnings == []


def _dry_run(tmp_path: Path, monkeypatch, files: str, present=(PATH,)):
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    monkeypatch.setattr(backfill, "load_config", lambda *_args: config)
    monkeypatch.setattr(
        backfill,
        "list_volume_files",
        lambda *_args: [
            {"file_path": path, "arrived_at": None, "file_bytes": 1} for path in present
        ],
    )
    monkeypatch.setattr(backfill, "list_parked", lambda _root: [])
    monkeypatch.setattr(backfill, "list_aside", lambda _root: [])
    return backfill.run_backfill(
        _RescueSpark(BRONZE_COLUMNS),
        "c",
        "files_things",
        parse_selection("rescue", files),
        dry_run=True,
    )


def test_the_dry_run_lists_the_named_files_and_reads_nothing_from_the_table(
    tmp_path: Path, monkeypatch
) -> None:
    result = _dry_run(tmp_path, monkeypatch, "a.csv")
    assert result.planned == [PATH]
    assert "1 file(s) to read again" in warning_text(result)


def test_a_named_file_that_is_not_there_is_recorded_as_missing(tmp_path: Path, monkeypatch) -> None:
    result = _dry_run(tmp_path, monkeypatch, "gone.csv", present=())
    assert result.missing == ["gone.csv"]
    assert "not found, would be recorded FAILED: gone.csv" in warning_text(result)


# --- a backfill never says it loaded what it did not ------------------------------------------


class _Field:
    def __init__(self, name: str, kind: str) -> None:
        self.name = name
        self.dataType = type("Type", (), {"simpleString": lambda _self: kind})()


class _TypedSpark:
    """A table with these (name, type) columns. ``count`` answers a count query."""

    def __init__(self, fields: list[tuple[str, str]], count: int = 0) -> None:
        self.fields, self.count, self.queries = [_Field(*item) for item in fields], count, []

    def table(self, _name: str):
        fields = self.fields
        return type(
            "Table",
            (),
            {
                "schema": type("Schema", (), {"fields": fields})(),
                "columns": [f.name for f in fields],
            },
        )()

    def sql(self, query: str) -> _Collected:
        self.queries.append(query)
        return _Collected([(self.count,)])


def test_a_table_that_holds_fewer_rows_than_were_counted_is_not_recorded_as_loaded() -> None:
    spark = _TypedSpark([("a", "string")], count=7)
    backfill._check_written(spark, "c.s.t", "b1", [PATH], 7)  # as counted: fine
    with pytest.raises(BackfillError, match="holds 7 of the 9 row"):
        backfill._check_written(spark, "c.s.t", "b1", [PATH], 9)
    assert "_batch_id = 'b1'" in spark.queries[0] and "_src_file_path IN" in spark.queries[0]


def test_only_rows_an_earlier_load_wrote_are_deleted_after_the_new_ones_are_in() -> None:
    spark = _TypedSpark([("a", "string")])
    backfill._remove(spark, [PATH], "c.s.bronze", keep_batch="b1")
    assert "_batch_id IS NULL OR _batch_id <> 'b1'" in spark.queries[0]
    backfill._remove(_s := _TypedSpark([("a", "string")]), [PATH], "c.s.bronze")
    assert "_batch_id" not in _s.queries[0]


def test_a_failed_load_takes_its_own_new_rows_out_and_touches_nothing_older() -> None:
    spark = _TypedSpark([("a", "string")])
    backfill._discard(spark, [PATH], "b1", "c.s.bronze", "c.s.quarantine")
    assert [q.split(" WHERE ")[0] for q in spark.queries] == [
        "DELETE FROM c.s.bronze",
        "DELETE FROM c.s.quarantine",
    ]
    assert all("_batch_id = 'b1'" in query for query in spark.queries)


def test_the_new_rows_are_written_and_checked_before_the_old_ones_are_deleted() -> None:
    import inspect

    source = inspect.getsource(backfill._load)
    insert, check = source.index("_insert(session, good"), source.index("_check_written(")
    remove, record = source.index("_remove(session"), source.index("file_records(")
    assert insert < check < remove < record  # nothing is deleted or recorded before it is proven
    assert "_discard(session" in source


def test_a_column_the_table_has_and_the_file_lacks_is_written_as_null() -> None:
    spark = _TypedSpark([("ride_id", "string"), ("col1", "double"), ("sample_column_1", "string")])
    seen: dict = {}

    class Frame:
        columns = ["ride_id"]

        def selectExpr(self, *exprs: str):
            seen["exprs"] = exprs
            return self

        def createOrReplaceTempView(self, name: str) -> None:
            seen["view"] = name

    backfill._insert(spark, Frame(), "c.s.bronze")
    assert seen["exprs"] == (
        "*",
        "CAST(NULL AS double) AS `col1`",
        "CAST(NULL AS string) AS `sample_column_1`",
    )
    assert spark.queries[-1].startswith("INSERT INTO c.s.bronze BY NAME")


def test_a_reload_reads_with_the_tables_own_types_so_a_value_that_does_not_fit_is_rescued(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    undeclared = dataclasses.replace(config, columns=[])
    spark = _TypedSpark(
        [
            ("ride_id", "string"),
            ("started_at", "timestamp"),
            ("_rescued_data", "string"),
            ("_src_file_path", "string"),
            ("_batch_id", "string"),
        ]
    )
    assert backfill._bronze_hints(spark, undeclared) == "`ride_id` string, `started_at` timestamp"
    assert backfill._bronze_hints(spark, config) == ""  # declared columns already are the hints


def test_a_stopped_load_is_recorded_failed_with_the_cause_not_the_pages_around_it(
    tmp_path: Path, monkeypatch
) -> None:
    written: list[dict] = []
    monkeypatch.setattr(backfill, "write_rows", lambda _s, _c, rows: written.extend(rows) or 1)
    monkeypatch.setattr(backfill, "_recorded_files", lambda *_a: [])
    monkeypatch.setattr(backfill, "_known_pipeline", lambda *_a: None)
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    result = Result("files_things", "files", planned=[PATH])
    error = RuntimeError(
        "[STREAM_FAILED] Query terminated with exception: [PYTHON_EXCEPTION] Traceback ...\n"
        "pyspark.errors.exceptions.connect.DateTimeException: [CAST_INVALID_INPUT] The value "
        "'43:33.0' of the type STRING cannot be cast to TIMESTAMP\nJVM stacktrace:"
    )
    backfill._record_failure(
        _RescueSpark(BRONZE_COLUMNS),
        config,
        result,
        error,
        started=datetime(2026, 11, 25, tzinfo=UTC),
        update_id="backfill-1",
        batch="b1",
    )
    (row,) = written
    assert row["target_status"] == "FAILED"
    assert row["failure_reason"].startswith(
        "backfill stopped: DateTimeException: [CAST_INVALID_INPUT]"
    )
    assert "STREAM_FAILED" not in row["failure_reason"]


def test_the_backfill_task_is_never_retried_on_its_own() -> None:
    import yaml

    job = yaml.safe_load(JOB.read_text(encoding="utf-8"))["resources"]["jobs"]["framework_backfill"]
    assert job["tasks"][0]["max_retries"] == 0


def test_a_file_that_lacks_columns_the_table_has_says_so(tmp_path: Path) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    result = Result("files_things", "rescue", planned=[PATH], not_in_file=["col1", "col2"])
    backfill._say_what_is_rescued(_RescueSpark(BRONZE_COLUMNS), config, result)
    assert "2 column(s) the table has are not in the file(s): col1, col2" in result.warnings[0]
    assert "NULL for these rows" in result.warnings[0]


# --- mode failed with file names: retry only those ---------------------------------------------

FAILED_A = "/Volumes/framework_dev/files_raw/landing/things/a.csv"
FAILED_B = "/Volumes/framework_dev/files_raw/landing/things/b.csv"


def _failed_run(tmp_path: Path, monkeypatch, files: str, dry_run: bool = True):
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    monkeypatch.setattr(backfill, "load_config", lambda *_args: config)
    monkeypatch.setattr(backfill, "list_parked", lambda _root: [])
    monkeypatch.setattr(backfill, "list_aside", lambda _root: [])
    spark = _RescueSpark(BRONZE_COLUMNS, left=[{"path": FAILED_A}, {"path": FAILED_B}])
    return backfill.run_backfill(
        spark, "c", "files_things", parse_selection("failed", files), dry_run=dry_run
    )


def test_mode_failed_takes_file_names_but_still_needs_none() -> None:
    assert parse_selection("failed").files == ()
    assert parse_selection("failed", "a.csv,/Volumes/c/s/v/things/b.csv").files == (
        "a.csv",
        "/Volumes/c/s/v/things/b.csv",
    )
    with pytest.raises(BackfillError, match="takes file names, not dates"):
        parse_selection("failed", "a.csv", date_from="2026-11-01")


def test_naming_files_in_mode_failed_retries_only_those(tmp_path: Path, monkeypatch) -> None:
    everything = _failed_run(tmp_path, monkeypatch, "")
    assert everything.planned == [FAILED_A, FAILED_B]
    by_name = _failed_run(tmp_path, monkeypatch, "b.csv")
    assert by_name.planned == [FAILED_B] and by_name.not_failed == []
    by_path = _failed_run(tmp_path, monkeypatch, FAILED_A)
    assert by_path.planned == [FAILED_A]


def test_a_named_file_that_did_not_fail_is_said_in_the_dry_run(tmp_path: Path, monkeypatch) -> None:
    result = _failed_run(tmp_path, monkeypatch, "a.csv,fine.csv")
    assert result.planned == [FAILED_A] and result.not_failed == ["fine.csv"]
    assert result.missing == [] and not result.none_found  # no ledger row for a file never failed
    text = warning_text(result)
    assert "this would be refused: 1 named file(s) did not fail: fine.csv" in text
    assert "Use mode files to reload a file that did not fail" in text


def test_a_real_run_naming_a_file_that_did_not_fail_is_refused_before_anything_changes(
    tmp_path: Path, monkeypatch
) -> None:
    with pytest.raises(BackfillError, match="1 named file.s. did not fail: fine.csv"):
        _failed_run(tmp_path, monkeypatch, "a.csv,fine.csv", dry_run=False)
    # nothing was moved, deleted or recorded: the only query was the ledger lookup
    # (the fake counts every query it is given)


def test_the_files_pattern_the_job_defaults_to_is_ignored_in_mode_failed() -> None:
    assert parse_selection("failed", job_defaults()["files"]).files == ()
