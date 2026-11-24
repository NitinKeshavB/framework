"""expand writes out the Python and SQL a table's pipeline builds, for reading and review."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from lakeflow_framework.cli import main
from lakeflow_framework.expand import ExpandError, expand
from test_sources import DATABASE_SOURCE, FILE_SOURCE, _config, _database_spec, _volume_spec


def volume_config(tmp_path: Path):
    return _config(tmp_path, FILE_SOURCE, "files_things.yaml", _volume_spec())


def test_the_python_parses_and_has_the_datasets_and_the_hook(tmp_path: Path) -> None:
    python, _sql = expand(volume_config(tmp_path), "files_things.yaml")
    tree = ast.parse(python)
    functions = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
    assert functions == {"files_things_staged", "bronze", "quarantine"}
    assert "install(dp, spark, recorder, CATALOG)" in python
    assert "UpdateRecorder(" in python and "file_load=True" in python


def test_bronze_reads_staged_directly_and_there_is_no_valid_view(tmp_path: Path) -> None:
    python, sql = expand(volume_config(tmp_path), "files_things.yaml")
    assert "temporary_view" not in python and "TEMPORARY VIEW" not in sql
    assert "_valid" not in python and "_valid" not in sql
    assert "spark.readStream.table('files_things_staged').filter(\"_dq_result = 'pass'\")" in python


def test_the_values_are_the_frameworks_own(tmp_path: Path) -> None:
    config = volume_config(tmp_path)
    python, sql = expand(config, "files_things.yaml")
    assert config.path in python and config.path in sql
    assert "cloudFiles.schemaEvolutionMode" in python and "schemaEvolutionMode =>" in sql
    assert "cloudFiles.format" in python and "format => 'csv'" in sql
    for rule in ("cast_thing_id", "schema_conformance"):
        assert f"CONSTRAINT {rule} EXPECT" in sql and f"'{rule}'" in python
    assert sql.count("CREATE OR REFRESH") == 3  # staged, bronze, quarantine


def test_a_table_that_keeps_history_by_key_is_refused(tmp_path: Path) -> None:
    spec = _database_spec(mode="merge").replace(
        "  columns:", "  primary_keys: [thing_id]\n  columns:"
    )
    config = _config(tmp_path, DATABASE_SOURCE, "db_things.yaml", spec)
    with pytest.raises(ExpandError, match="Auto CDC"):
        expand(config, "db_things.yaml")


def test_a_table_not_read_from_a_volume_is_refused(tmp_path: Path) -> None:
    config = _config(tmp_path, DATABASE_SOURCE, "db_things.yaml", _database_spec())
    with pytest.raises(ExpandError, match="volume_files"):
        expand(config, "db_things.yaml")


def test_the_command_writes_both_files(tmp_path: Path, capsys) -> None:
    config = volume_config(tmp_path)
    spec = next(tmp_path.rglob("files_things.yaml"))
    out = tmp_path / "out"
    assert main(["expand", str(spec), "--catalog", "framework_dev", "--out", str(out)]) == 0
    assert sorted(path.name for path in out.iterdir()) == [
        "files_things_pipeline.py",
        "files_things_pipeline.sql",
    ]
    ast.parse((out / "files_things_pipeline.py").read_text(encoding="utf-8"))
    assert config.pipeline_id in capsys.readouterr().out


def test_the_command_says_why_it_cannot_expand(tmp_path: Path, capsys) -> None:
    spec = tmp_path / "finance" / "db"
    _config(tmp_path, DATABASE_SOURCE, "db_things.yaml", _database_spec())
    path = next(tmp_path.rglob("db_things.yaml"))
    assert main(["expand", str(path), "--catalog", "framework_dev", "--out", str(spec)]) == 1
    assert "volume_files" in capsys.readouterr().err
