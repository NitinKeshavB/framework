"""The listing that sees a file with no bytes, which Spark's binaryFile reader leaves out."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

from lakeflow_framework import backfill
from lakeflow_framework.backfill import parse_selection
from lakeflow_framework.volume_listing import list_volume_files
from test_sources import FILE_SOURCE, _config, _volume_spec


def put(folder: Path, name: str, text: str = "id\n1\n", mtime: datetime | None = None) -> Path:
    path = folder / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    if mtime:
        os.utime(path, (mtime.timestamp(), mtime.timestamp()))
    return path


def test_a_file_with_no_bytes_is_listed(tmp_path: Path) -> None:
    put(tmp_path, "a.csv")
    put(tmp_path, "empty.csv", "")
    found = {
        Path(item["file_path"]).name: item["file_bytes"]
        for item in list_volume_files(str(tmp_path))
    }
    assert found == {"a.csv": 5, "empty.csv": 0}


def test_hidden_and_underscore_names_and_the_glob_are_honoured(tmp_path: Path) -> None:
    for name in ("a.csv", "b.txt", ".hidden.csv", "_tmp.csv"):
        put(tmp_path, name)
    put(tmp_path / "_state", "x.csv")
    names = [Path(item["file_path"]).name for item in list_volume_files(str(tmp_path), "*.csv")]
    assert names == ["a.csv"]


def test_subfolders_are_listed_and_the_arrival_time_is_utc(tmp_path: Path) -> None:
    when = datetime(2026, 11, 19, 8, 1, 4, tzinfo=UTC)
    put(tmp_path / "month" / "week", "a.csv", mtime=when)
    (item,) = list_volume_files(str(tmp_path) + "/")
    assert item["file_path"].endswith("month/week/a.csv") and item["arrived_at"] == when


def test_a_backfill_plans_by_name_and_by_arrival_over_the_same_listing(
    tmp_path: Path, monkeypatch
) -> None:
    config = _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())
    volume = tmp_path / "vol"
    put(volume, "old.csv", mtime=datetime(2026, 11, 1, 12, tzinfo=UTC))
    put(volume, "new.csv", mtime=datetime(2026, 11, 19, 8, 1, tzinfo=UTC))
    put(volume, "empty.csv", "", mtime=datetime(2026, 11, 19, 8, 2, tzinfo=UTC))
    config.path = str(volume) + "/"
    config.path_glob = "*.csv"

    def names(selection) -> list[str]:
        return [Path(item["path"]).name for item in backfill.plan_files(None, config, selection)]

    assert names(parse_selection("files", "new.csv,missing.csv")) == ["new.csv"]
    assert names(
        parse_selection("dates", date_from="2026-11-19 08:00", date_to="2026-11-19 08:10")
    ) == [
        "empty.csv",
        "new.csv",
    ]
    assert names(parse_selection("dates", date_to="2026-11-02")) == ["old.csv"]  # that whole day
    assert names(parse_selection("files")) == ["empty.csv", "new.csv", "old.csv"]
