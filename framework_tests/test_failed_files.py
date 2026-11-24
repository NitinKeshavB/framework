"""A file a fail rule blocked is moved to _failed at the root of its volume, and nothing reads it."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakeflow_framework import ConfigValidationError, failed_files
from lakeflow_framework.config import _failed_folder_problem
from lakeflow_framework.failed_files import (
    blocked_paths_sql,
    move_failed,
    park_failed_files,
    parked_path,
    restore_failed,
)
from lakeflow_framework.volume_listing import list_volume_files
from test_sources import FILE_SOURCE, _config, _volume_spec


@pytest.fixture
def volume(tmp_path: Path, monkeypatch) -> Path:
    """A folder standing in for /Volumes/<catalog>/<schema>/<volume>."""

    root = tmp_path / "gl_source_extract"
    root.mkdir()

    def volume_root(path: str) -> Path | None:
        return root if Path(failed_files.plain_path(path)).is_relative_to(root) else None

    monkeypatch.setattr(failed_files, "volume_root", volume_root)
    return root


def make(root: Path, relative: str, text: str = "a,b\n1,2\n") -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_the_folder_is_at_the_root_of_the_volume_keeping_the_path_below_it(volume: Path) -> None:
    # a table that reads the whole volume
    assert parked_path(str(volume), str(volume / "a.csv")) == volume / "_failed" / "a.csv"
    # a table that reads a folder in the volume: the folder is still under the volume's root
    table = str(volume / "invoices") + "/"
    assert parked_path(table, str(volume / "invoices" / "2026" / "a.csv")) == (
        volume / "_failed" / "invoices" / "2026" / "a.csv"
    )
    assert parked_path(str(volume), "dbfs:" + str(volume / "a.csv")) == volume / "_failed" / "a.csv"


def test_two_tables_in_one_volume_share_the_folder_and_never_collide(volume: Path) -> None:
    one = parked_path(str(volume / "invoices"), str(volume / "invoices" / "x.csv"))
    two = parked_path(str(volume / "credits"), str(volume / "credits" / "x.csv"))
    assert one == volume / "_failed" / "invoices" / "x.csv"
    assert two == volume / "_failed" / "credits" / "x.csv" and one != two


def test_a_table_never_moves_another_tables_file_or_a_parked_one(volume: Path) -> None:
    assert parked_path(str(volume / "invoices"), str(volume / "credits" / "a.csv")) is None
    assert parked_path(str(volume), str(volume / "_failed" / "a.csv")) is None


def test_a_path_that_is_not_in_a_volume_is_left_alone(tmp_path: Path) -> None:
    # no volume fixture: this is not a /Volumes path
    assert parked_path(str(tmp_path), str(tmp_path / "a.csv")) is None
    assert failed_files.volume_root("/Volumes/aal_dev_fdh/abs_raw/gl_source_extract/a.csv") == (
        Path("/Volumes/aal_dev_fdh/abs_raw/gl_source_extract")
    )


def test_the_file_moves_and_is_no_longer_among_the_good_files(volume: Path) -> None:
    bad = make(volume, "invoices/sub/bad.csv")
    make(volume, "invoices/good.csv")
    target = move_failed(str(volume / "invoices"), str(bad))
    assert target == volume / "_failed" / "invoices" / "sub" / "bad.csv"
    assert target.read_text() == "a,b\n1,2\n" and not bad.exists()
    assert (volume / "invoices" / "good.csv").exists()


def test_the_listing_never_sees_the_failed_folder(volume: Path) -> None:
    bad = make(volume, "bad.csv")
    make(volume, "good.csv")
    move_failed(str(volume), str(bad))
    # a table that reads the volume root skips the underscore folder, as Auto Loader does, so a parked
    # file is never read again and never counts as a new arrival
    assert [Path(item["file_path"]).name for item in list_volume_files(str(volume))] == ["good.csv"]
    assert (volume / "_failed" / "bad.csv").exists()


def test_a_table_below_the_volume_root_cannot_reach_the_folder_at_all(volume: Path) -> None:
    make(volume, "invoices/good.csv")
    move_failed(str(volume / "invoices"), str(make(volume, "invoices/bad.csv")))
    names = [Path(item["file_path"]).name for item in list_volume_files(str(volume / "invoices"))]
    assert names == ["good.csv"]
    assert (volume / "_failed" / "invoices" / "bad.csv").exists()


def test_a_file_that_is_gone_or_already_parked_is_not_an_error(volume: Path) -> None:
    assert move_failed(str(volume), str(volume / "missing.csv")) is None
    parked = move_failed(str(volume), str(make(volume, "bad.csv")))
    assert move_failed(str(volume), str(parked)) is None


def test_a_second_file_with_the_same_name_does_not_overwrite_the_first(volume: Path) -> None:
    first = move_failed(str(volume), str(make(volume, "bad.csv", "first")))
    second = move_failed(str(volume), str(make(volume, "bad.csv", "second")))
    assert first.read_text() == "first" and second.read_text() == "second"
    assert second != first and second.name.startswith("bad.csv.")


def test_restore_moves_a_parked_file_back_for_a_backfill(volume: Path) -> None:
    bad = make(volume, "invoices/sub/bad.csv")
    move_failed(str(volume / "invoices"), str(bad))
    assert restore_failed(str(volume / "invoices"), [str(bad)]) == [bad]
    assert bad.exists() and not (volume / "_failed" / "invoices" / "sub" / "bad.csv").exists()
    assert restore_failed(str(volume / "invoices"), [str(bad)]) == []  # already back


def test_restore_takes_the_newest_when_a_name_was_parked_twice(volume: Path) -> None:
    move_failed(str(volume), str(make(volume, "bad.csv", "first")))
    newer = move_failed(str(volume), str(make(volume, "bad.csv", "second")))
    os.utime(newer, (time.time() + 60, time.time() + 60))
    original = volume / "bad.csv"
    restore_failed(str(volume), [str(original)])
    # the newest copy is the one reloaded, the older one stays parked
    assert original.read_text() == "second" and not newer.exists()
    assert (volume / "_failed" / "bad.csv").read_text() == "first"


def test_the_ledger_query_names_only_files_a_fail_rule_blocked() -> None:
    sql = blocked_paths_sql("aal_dev_fdh", "abs_gl")
    assert "aal_dev_fdh.framework_meta.ledger_detail" in sql
    assert "dataflow = 'abs_gl'" in sql and "detail_kind = 'file'" in sql
    # the latest status per file, so a file reloaded OK is not parked, and only a blocked one
    assert "row_number() OVER (PARTITION BY src_file_path ORDER BY recorded_at DESC) = 1" in sql
    assert "status = 'FAILED'" in sql and "error_message LIKE 'blocked by fail rule%'" in sql
    assert "INTERVAL 30 DAYS" in sql


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def collect(self) -> list[dict]:
        return self.rows


class _Spark:
    def __init__(self, paths: list[str]) -> None:
        self.paths = paths

    def sql(self, statement: str) -> _Result:
        return _Result([{"src_file_path": path} for path in self.paths])


def test_park_failed_files_moves_what_the_ledger_says_and_a_second_run_moves_nothing(
    volume: Path, monkeypatch
) -> None:
    bad = make(volume, "invoices/bad.csv")
    good = make(volume, "invoices/good.csv")

    class Config:
        path = str(volume / "invoices") + "/"

    monkeypatch.setattr("lakeflow_framework.backfill.load_config", lambda *_args: Config())
    spark = _Spark([str(bad), str(volume / "invoices" / "gone.csv")])
    moved = park_failed_files(spark, "aal_dev_fdh", ["abs_gl"])
    assert moved == {"abs_gl": [str(volume / "_failed" / "invoices" / "bad.csv")]}
    assert good.exists() and not bad.exists()
    assert park_failed_files(spark, "aal_dev_fdh", ["abs_gl"]) == {"abs_gl": []}


def test_list_parked_gives_the_original_paths_and_ignores_other_tables(volume: Path) -> None:
    move_failed(str(volume / "invoices"), str(make(volume, "invoices/sub/a.csv")))
    move_failed(str(volume / "invoices"), str(make(volume, "invoices/b.csv", "first")))
    move_failed(str(volume / "invoices"), str(make(volume, "invoices/b.csv", "second")))
    move_failed(str(volume / "credits"), str(make(volume, "credits/c.csv")))
    # b.csv was parked twice (the second has a timestamp suffix) and is listed once
    assert failed_files.list_parked(str(volume / "invoices") + "/") == [
        str(volume / "invoices" / "b.csv"),
        str(volume / "invoices" / "sub" / "a.csv"),
    ]
    assert failed_files.list_parked(str(volume / "nothing")) == []
    # a table that reads the whole volume sees everything parked in it
    assert len(failed_files.list_parked(str(volume))) == 3


def test_list_parked_is_empty_outside_a_volume(tmp_path: Path) -> None:
    assert failed_files.list_parked(str(tmp_path)) == []


# --- backfilling and the _failed folder -----------------------------------------------------------


class _Updates:
    """The pipelines API of the Databricks client: starts an update and reports its state."""

    def __init__(self, states: list[str], log: list[str]) -> None:
        self.states, self.log, self.polls = states, log, 0

    def start_update(self, **kwargs):
        self.log.append("start")
        self.started = kwargs
        return type("Started", (), {"update_id": "u-1"})()

    def get_update(self, **kwargs):
        state = self.states[min(self.polls, len(self.states) - 1)]
        self.polls += 1
        return type("Got", (), {"update": type("Update", (), {"state": state})()})()


def _refresh(tmp_path: Path, monkeypatch, states: list[str], *, dry_run: bool = False, aside=()):
    from lakeflow_framework import backfill
    from lakeflow_framework.backfill import Result

    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    log: list[str] = []
    monkeypatch.setattr(backfill, "restore_failed", lambda root, paths: log.append("restore"))
    monkeypatch.setattr(backfill, "list_aside", lambda root: list(aside))
    monkeypatch.setattr(backfill, "restore_aside", lambda root, paths: log.append("restore_aside"))
    monkeypatch.setattr(
        backfill,
        "park_failed_files",
        lambda spark, catalog, ids: log.append("park") or {ids[0]: ["/v/_failed/bad.csv"]},
    )
    updates = _Updates(states, log)
    client = type("Client", (), {"pipelines": updates})()
    sleeps: list[float] = []
    spark = type(
        "Spark",
        (),
        {"sql": lambda self, _s: type("R", (), {"collect": lambda _x: [("pipeline-uuid",)]})()},
    )()
    result = Result("files_things", "full_refresh", dry_run=dry_run)
    result = backfill._full_refresh(
        spark,
        config,
        result,
        confirm=True,
        client=client,
        sleep=sleeps.append,
        wait_seconds=60,
    )
    return result, log, sleeps, updates


def test_a_full_refresh_leaves_failed_alone_waits_for_the_update_then_parks(
    tmp_path: Path, monkeypatch
) -> None:
    result, log, sleeps, updates = _refresh(tmp_path, monkeypatch, ["RUNNING", "RUNNING", "FAILED"])
    # nothing is moved back: the pipeline reads the files that are in the folder and skips _failed.
    # The blocked file fails the update, so the job waits for it and parks what it blocked.
    assert log == ["start", "park"]
    assert result.restored == []
    assert result.update_state == "FAILED" and result.update_id == "u-1"
    assert result.parked == ["/v/_failed/bad.csv"]
    assert updates.started["full_refresh_selection"] == result.planned
    assert sleeps[-1] == 30  # a pause for the pipeline to write the ledger before it is read


def test_a_full_refresh_that_never_ends_in_time_parks_nothing_and_says_so(
    tmp_path: Path, monkeypatch
) -> None:
    result, log, _sleeps, _updates = _refresh(tmp_path, monkeypatch, ["RUNNING"])
    assert "park" not in log and result.parked == []
    assert result.update_state == "RUNNING"
    assert "next job run" in result.warnings[0]


def test_a_dry_run_of_a_full_refresh_starts_and_moves_nothing(tmp_path: Path, monkeypatch) -> None:
    result, log, _sleeps, _updates = _refresh(tmp_path, monkeypatch, ["COMPLETED"], dry_run=True)
    assert log == [] and result.update_id == "" and result.restored == []


def test_the_summary_of_a_full_refresh_says_what_was_parked(tmp_path: Path, monkeypatch) -> None:
    from lakeflow_framework.backfill import warning_text

    result, *_ = _refresh(tmp_path, monkeypatch, ["COMPLETED"])
    text = warning_text(result)
    assert "update u-1 completed" in text and "1 blocked file(s) moved to _failed" in text
    assert "moved back" not in text


BAD = "/Volumes/framework_dev/files_raw/landing/things/bad.csv"
GOOD = "/Volumes/framework_dev/files_raw/landing/things/good.csv"


def _item(path: str) -> dict:
    return {"file_path": path, "file_bytes": 1, "arrived_at": datetime.now(UTC)}


def _plan(tmp_path: Path, monkeypatch, selection, *, parked: list[str], listed: list[str]):
    from lakeflow_framework import backfill

    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    monkeypatch.setattr(
        backfill, "list_volume_files", lambda path, glob=None: [_item(p) for p in listed]
    )
    monkeypatch.setattr(backfill, "list_parked", lambda root: list(parked))
    return [item["path"] for item in backfill.plan_files(None, config, selection)]


def test_a_name_the_user_gives_is_found_in_failed_when_it_is_not_in_the_folder(
    tmp_path: Path, monkeypatch
) -> None:
    from lakeflow_framework.backfill import parse_selection

    planned = _plan(
        tmp_path, monkeypatch, parse_selection("files", "bad.csv"), parked=[BAD], listed=[GOOD]
    )
    assert planned == [BAD]  # planned from where it was, so it is moved back and loaded


def test_a_name_that_is_in_the_folder_is_loaded_from_there(tmp_path: Path, monkeypatch) -> None:
    from lakeflow_framework.backfill import parse_selection

    planned = _plan(
        tmp_path, monkeypatch, parse_selection("files", "good.csv"), parked=[BAD], listed=[GOOD]
    )
    assert planned == [GOOD]  # the parked file was not asked for, so it is left alone


def test_dates_and_no_selection_never_look_in_failed(tmp_path: Path, monkeypatch) -> None:
    from lakeflow_framework.backfill import parse_selection

    for selection in (parse_selection("dates", date_from="2026-01-01"), parse_selection("files")):
        planned = _plan(tmp_path, monkeypatch, selection, parked=[BAD], listed=[GOOD])
        assert planned == [GOOD]


def _run(tmp_path: Path, monkeypatch, selection, *, parked: list[str], listed: list[str], **kw):
    """run_backfill with the load itself stubbed out; returns the result and what was called."""

    from lakeflow_framework import backfill

    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    log: list = []
    monkeypatch.setattr(backfill, "load_config", lambda *_args: config)
    monkeypatch.setattr(
        backfill, "list_volume_files", lambda path, glob=None: [_item(p) for p in listed]
    )
    aside = list(kw.get("aside", []))
    monkeypatch.setattr(backfill, "list_parked", lambda root: list(parked))
    monkeypatch.setattr(backfill, "list_aside", lambda root: list(aside))
    monkeypatch.setattr(
        backfill,
        "restore_failed",
        lambda root, paths: set(paths) & set(parked) and log.append(("restore", paths)),
    )
    monkeypatch.setattr(
        backfill,
        "restore_aside",
        lambda root, paths: set(paths) & set(aside) and log.append(("restore_aside", paths)),
    )
    monkeypatch.setattr(backfill, "move_failed", lambda root, path: log.append(("park", path)))
    monkeypatch.setattr(
        backfill, "move_aside", lambda root, path: log.append(("aside", path)) or Path(path)
    )
    monkeypatch.setattr(backfill, "read_by_pipeline", lambda *a: set(kw.get("read", ())))
    monkeypatch.setattr(backfill, "_count_existing", lambda *a: 0)
    monkeypatch.setattr(backfill, "_load", lambda *a, **k: log.append(("load",)))
    monkeypatch.setattr(backfill, "_recorded_files", lambda *a: kw.get("recorded", []))
    monkeypatch.setattr(backfill, "_record", lambda *a, **k: None)
    monkeypatch.setattr(
        backfill, "_failed_files", lambda *a: [{"path": p} for p in kw.get("ledger", [])]
    )
    result = backfill.run_backfill(
        None, "framework_dev", "files_things", selection, **kw.get("opts", {})
    )
    return result, log


def test_naming_a_parked_file_moves_it_back_then_loads_it(tmp_path: Path, monkeypatch) -> None:
    from lakeflow_framework.backfill import parse_selection

    result, log = _run(
        tmp_path, monkeypatch, parse_selection("files", "bad.csv"), parked=[BAD], listed=[GOOD]
    )
    assert [entry[0] for entry in log] == ["restore", "load"]
    assert log[0] == ("restore", [BAD]) and result.restored == [BAD]


def test_a_still_blocked_file_is_parked_again_after_the_load(tmp_path: Path, monkeypatch) -> None:
    from lakeflow_framework.backfill import parse_selection

    blocked = {"src_file_path": BAD, "src_file_name": "bad.csv", "status": "FAILED"}
    result, log = _run(
        tmp_path,
        monkeypatch,
        parse_selection("files", "bad.csv"),
        parked=[BAD],
        listed=[],
        recorded=[blocked],
    )
    assert [entry[0] for entry in log] == ["restore", "load", "park"] and log[-1] == ("park", BAD)


def test_a_dates_backfill_loads_the_folder_and_moves_nothing_back(
    tmp_path: Path, monkeypatch
) -> None:
    from lakeflow_framework.backfill import parse_selection

    result, log = _run(
        tmp_path,
        monkeypatch,
        parse_selection("dates", date_from="2026-01-01"),
        parked=[BAD],
        listed=[GOOD],
    )
    assert [entry[0] for entry in log] == ["load"] and result.restored == []


def test_a_new_failure_in_a_backfill_is_parked_like_a_regular_run(
    tmp_path: Path, monkeypatch
) -> None:
    from lakeflow_framework.backfill import parse_selection

    blocked = {"src_file_path": GOOD, "src_file_name": "good.csv", "status": "FAILED"}
    _result, log = _run(
        tmp_path,
        monkeypatch,
        parse_selection("dates", date_from="2026-01-01"),
        parked=[],
        listed=[GOOD],
        recorded=[blocked],
    )
    assert [entry[0] for entry in log] == ["load", "park"] and log[-1] == ("park", GOOD)


def test_a_dry_run_says_which_files_would_come_back_and_moves_none(
    tmp_path: Path, monkeypatch
) -> None:
    from lakeflow_framework.backfill import parse_selection, warning_text

    result, log = _run(
        tmp_path,
        monkeypatch,
        parse_selection("files", "bad.csv"),
        parked=[BAD],
        listed=[],
        opts={"dry_run": True},
    )
    assert log == [] and result.restored == [BAD]
    assert "1 file(s) would be moved back out of _failed or _backfilled" in warning_text(result)


def test_the_failed_mode_moves_back_only_the_files_the_ledger_names(
    tmp_path: Path, monkeypatch
) -> None:
    from lakeflow_framework.backfill import parse_selection

    other = BAD.replace("bad.csv", "other.csv")
    _result, log = _run(
        tmp_path,
        monkeypatch,
        parse_selection("failed"),
        parked=[BAD, other],
        listed=[],
        ledger=[BAD],
    )
    assert log[0] == ("restore", [BAD])  # the other parked file is not asked for, so it stays


# --- a file a backfill loaded is kept away from the regular job --------------------------------


def test_a_file_the_pipeline_has_not_read_is_set_aside_after_the_backfill_loads_it(
    tmp_path: Path, monkeypatch
) -> None:
    from lakeflow_framework.backfill import parse_selection, warning_text

    loaded = {"src_file_path": GOOD, "src_file_name": "good.csv", "status": "PROCESSED"}
    result, log = _run(
        tmp_path,
        monkeypatch,
        parse_selection("files", GOOD),
        parked=[],
        listed=[GOOD],
        recorded=[loaded],
        read=set(),
    )
    # the pipeline's checkpoint does not know the file, so its next run would load it again
    assert [entry[0] for entry in log] == ["load", "aside"] and log[-1] == ("aside", GOOD)
    assert result.set_aside == [GOOD]
    assert "moved to _backfilled so the regular job does not read them again" in warning_text(
        result
    )


def test_a_file_the_pipeline_already_read_stays_where_it_is(tmp_path: Path, monkeypatch) -> None:
    from lakeflow_framework.backfill import parse_selection

    loaded = {"src_file_path": GOOD, "src_file_name": "good.csv", "status": "PROCESSED"}
    result, log = _run(
        tmp_path,
        monkeypatch,
        parse_selection("files", GOOD),
        parked=[],
        listed=[GOOD],
        recorded=[loaded],
        read={GOOD},  # a pipeline update recorded it: its checkpoint keeps it from being read again
    )
    assert [entry[0] for entry in log] == ["load"] and result.set_aside == []


def test_a_blocked_file_is_parked_in_failed_and_not_set_aside(tmp_path: Path, monkeypatch) -> None:
    from lakeflow_framework.backfill import parse_selection

    blocked = {"src_file_path": GOOD, "src_file_name": "good.csv", "status": "FAILED"}
    result, log = _run(
        tmp_path,
        monkeypatch,
        parse_selection("files", GOOD),
        parked=[],
        listed=[GOOD],
        recorded=[blocked],
        read=set(),
    )
    assert [entry[0] for entry in log] == ["load", "park"] and result.set_aside == []


def test_naming_a_file_that_was_set_aside_brings_it_back_to_load_it(
    tmp_path: Path, monkeypatch
) -> None:
    from lakeflow_framework.backfill import parse_selection

    result, log = _run(
        tmp_path, monkeypatch, parse_selection("files", BAD), parked=[], listed=[], aside=[BAD]
    )
    assert [entry[0] for entry in log] == ["restore_aside", "load"] and result.restored == [BAD]


def test_a_full_refresh_brings_back_what_a_backfill_set_aside_before_it_starts(
    tmp_path: Path, monkeypatch
) -> None:
    result, log, _sleeps, _updates = _refresh(tmp_path, monkeypatch, ["COMPLETED"], aside=[BAD])
    # the refresh rebuilds the table from the folder, so those files must be in it
    assert log[:2] == ["restore_aside", "start"] and result.restored == [BAD]
    dry, log, _sleeps, _updates = _refresh(
        tmp_path, monkeypatch, ["COMPLETED"], aside=[BAD], dry_run=True
    )
    assert log == [] and dry.restored == [BAD]


def test_both_folders_work_the_same_way_and_a_moved_file_is_not_moved_again(volume: Path) -> None:
    from lakeflow_framework.failed_files import list_aside, move_aside, restore_aside, unparked_path

    table = str(volume / "invoices")
    a = make(volume, "invoices/a.csv")
    target = move_aside(table, str(a))
    assert target == volume / "_backfilled" / "invoices" / "a.csv" and not a.exists()
    assert list_aside(table) == [str(a)] and failed_files.list_parked(table) == []
    # in one folder it is never put into the other
    assert move_failed(table, str(target)) is None and move_aside(table, str(target)) is None
    # either spelling of its path means the file it was
    assert unparked_path(str(target)) == str(a)
    assert unparked_path(str(volume / "_failed" / "invoices" / "a.csv")) == str(a)
    assert restore_aside(table, [str(a)]) == [a] and a.exists() and list_aside(table) == []


def test_the_pipeline_has_read_a_file_when_a_pipeline_update_recorded_it() -> None:
    from lakeflow_framework.failed_files import read_by_pipeline

    queries: list[str] = []

    class Spark:
        def sql(self, statement: str):
            queries.append(statement)
            rows = [{"src_file_path": "dbfs:" + GOOD}]  # the file source can add a scheme
            return type("R", (), {"collect": lambda _self: rows})()

    read = read_by_pipeline(Spark(), "framework_dev", "files_things", [GOOD, BAD])
    assert read == {GOOD}  # only what was asked about, and without the scheme
    (query,) = queries
    # a backfill's own rows (update id backfill-...) do not count: it does not move the checkpoint
    assert "update_id NOT LIKE 'backfill-%'" in query and "dataflow = 'files_things'" in query


def test_backfilled_is_reserved_like_failed() -> None:
    root = "/Volumes/aal_dev_fdh/abs_raw/gl_source_extract/"
    assert "reserved" in _failed_folder_problem(root + "_backfilled/x/", "volume_files")
    assert "_backfilled" in _failed_folder_problem(root, "copy_into")
    assert _failed_folder_problem(root + "invoices/", "copy_into") is None


# --- nothing is pointed at the folder ---------------------------------------------------------


def test_a_table_cannot_read_the_failed_folder(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="_failed is reserved"):
        _config(
            tmp_path,
            FILE_SOURCE,
            "files_things.yaml",
            _volume_spec(source="  raw_path: _failed/things/\n  format: csv\n"),
        )


def test_copy_into_and_ctas_must_read_a_folder_below_the_volume_root() -> None:
    root = "/Volumes/aal_dev_fdh/abs_raw/gl_source_extract/"
    for method in ("copy_into", "ctas", "autoloader"):
        assert "below the volume root" in _failed_folder_problem(root, method)
        assert _failed_folder_problem(root + "invoices/", method) is None
    # Auto Loader on a volume (volume_files) skips the underscore folder itself, so the root is fine
    assert _failed_folder_problem(root, "volume_files") is None
    assert "reserved" in _failed_folder_problem(root + "_failed/x/", "volume_files")
