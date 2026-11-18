"""What a table spec inherits from source.yaml and its folder, and what stays in the spec.

source.yaml holds facts about the source system: domain, owner, deliveries, and the raw schema.
Everything about a file or its sensitivity is in the table spec, because one source system can
deliver files of different kinds.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from lakeflow_framework.config import ConfigValidationError
from lakeflow_framework.ingestion import BUILD_DIR, generate, load_sources, new_source, new_table

SOURCE = """\
source_system: files
domain: finance
owner: files.owner@jll.com
kind: sftp_files
raw_volume: {schema: files_raw}
deliveries:
  daily: {trigger: file_arrival}
"""

SPEC = """\
pipeline_id: files_things
data_domain: things
data: {classification: confidential, pii: [pii_finance]}
notify: {on_failure: [alerts@jll.com]}
source: {volume: landing, raw_path: things/, format: csv, delimiter: ",", header: false}
raw_to_bronze:
  bronze: {table: things}
  columns:
    - {name: thing_id, type: string}
"""


def _bundle(tmp_path: Path, source: str = SOURCE, spec: str = SPEC, folder: str = "reporting"):
    root = tmp_path / "sources" / "files"
    (root / folder).mkdir(parents=True, exist_ok=True)
    (root / "source.yaml").write_text(source, encoding="utf-8")
    (root / folder / "files_things.yaml").write_text(spec, encoding="utf-8")
    return tmp_path


def _add_spec(bundle: Path, name: str, spec: str, folder: str = "reporting") -> None:
    (bundle / "sources" / "files" / folder / f"files_{name}.yaml").write_text(
        spec, encoding="utf-8"
    )


def _other(name: str, **changes: str) -> str:
    text = SPEC.replace("files_things", f"files_{name}").replace("things", name)
    for old, new in changes.items():
        text = text.replace(old.replace("__", " "), new)
    return text


def _only_table(bundle: Path):
    (system,) = load_sources(bundle)
    (table,) = system.tables
    return table


def test_a_spec_inherits_the_source_system_and_its_folder(tmp_path: Path) -> None:
    table = _only_table(_bundle(tmp_path))
    config = table.config
    assert (config.domain, config.layer) == ("finance", "bronze")
    assert config.technical_owner == "files.owner@jll.com"
    assert (config.requested_by, config.team) == ("reporting", "reporting")
    assert (config.delivery, config.job_name) == ("daily", "files_daily")
    assert table.delivery == "daily" and table.requested_by == "reporting"
    assert config.path == "/Volumes/catalog/files_raw/landing/things/"


def test_a_spec_next_to_source_yaml_has_no_requester(tmp_path: Path) -> None:
    root = tmp_path / "sources" / "files"
    root.mkdir(parents=True)
    (root / "source.yaml").write_text(SOURCE, encoding="utf-8")
    (root / "files_things.yaml").write_text(SPEC, encoding="utf-8")
    config = _only_table(tmp_path).config
    assert config.requested_by == ""
    assert config.team == "files"


def test_a_spec_value_wins_over_the_inherited_one(tmp_path: Path) -> None:
    spec = SPEC + "technical_owner: table.owner@jll.com\nteam: own_team\n"
    config = _only_table(_bundle(tmp_path, spec=spec)).config
    assert config.technical_owner == "table.owner@jll.com"
    assert config.team == "own_team"


def test_sensitivity_and_notify_are_the_tables_own(tmp_path: Path) -> None:
    config = _only_table(_bundle(tmp_path)).config
    assert config.tags["bronze"].classification == "confidential"
    assert config.tags["bronze"].pii == ["pii_finance"]
    assert config.notify_failure == ["alerts@jll.com"]
    without = SPEC.replace("data: {classification: confidential, pii: [pii_finance]}\n", "")
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(_bundle(tmp_path, spec=without))
    assert "classification is required" in str(caught.value)


def test_tables_in_one_source_system_can_read_different_formats(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    json_spec = _other(
        "events", **{'format:__csv,__delimiter:__",",__header:__false': "format: json"}
    )
    _add_spec(bundle, "events", json_spec)
    (system,) = load_sources(bundle)
    formats = {table.config.pipeline_id: table.config.file_format for table in system.tables}
    assert formats == {"files_things": "csv", "files_events": "json"}


def test_the_volume_is_named_in_the_spec(tmp_path: Path) -> None:
    spec = SPEC.replace("volume: landing", "volume: month_end")
    config = _only_table(_bundle(tmp_path, spec=spec)).config
    assert config.path == "/Volumes/catalog/files_raw/month_end/things/"


def test_a_volume_in_source_yaml_is_only_a_default(tmp_path: Path) -> None:
    source = SOURCE.replace("{schema: files_raw}", "{schema: files_raw, volume: shared}")
    no_volume = SPEC.replace("volume: landing, ", "")
    assert _only_table(_bundle(tmp_path, source, no_volume)).config.path.endswith(
        "/files_raw/shared/things/"
    )
    named = _only_table(_bundle(tmp_path, source, SPEC)).config
    assert named.path.endswith("/files_raw/landing/things/")


def test_a_table_without_a_volume_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(_bundle(tmp_path, spec=SPEC.replace("volume: landing, ", "")))
    assert "source.volume is required" in str(caught.value)


def test_source_yaml_needs_the_raw_schema(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(_bundle(tmp_path, source=SOURCE.replace("{schema: files_raw}", "{}")))
    assert "needs raw_volume with a schema" in str(caught.value)


def test_the_trigger_watches_the_volume_the_tables_read(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, spec=SPEC.replace("volume: landing", "volume: month_end"))
    generate(bundle)
    job = yaml.safe_load(
        (bundle / BUILD_DIR / "resources" / "files_daily.job.yml").read_text("utf-8")
    )
    trigger = job["resources"]["jobs"]["files_daily"]["trigger"]
    assert trigger == {"file_arrival": {"url": "/Volumes/${var.catalog}/files_raw/month_end/"}}
    assert generate(bundle, check=True) == []


def test_one_delivery_cannot_fire_on_two_volumes(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    _add_spec(bundle, "events", _other("events", **{"volume:__landing": "volume: elsewhere"}))
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(bundle)
    text = str(caught.value)
    assert "delivery daily fires on one volume" in text
    assert "files_raw.elsewhere" in text and "files_raw.landing" in text


def test_a_scheduled_delivery_may_read_several_volumes(tmp_path: Path) -> None:
    source = SOURCE.replace("{trigger: file_arrival}", '{schedule: "0 0 2 * * ?"}')
    bundle = _bundle(tmp_path, source=source)
    _add_spec(bundle, "events", _other("events", **{"volume:__landing": "volume: elsewhere"}))
    (system,) = load_sources(bundle)
    assert len(system.tables) == 2


def test_two_deliveries_can_watch_two_volumes(tmp_path: Path) -> None:
    source = SOURCE.replace(
        "  daily: {trigger: file_arrival}\n",
        "  daily: {trigger: file_arrival}\n  other: {trigger: file_arrival}\n",
    )
    spec = SPEC + "delivery: daily\n"
    bundle = _bundle(tmp_path, source=source, spec=spec)
    events = _other("events", **{"volume:__landing": "volume: elsewhere"}) + "delivery: other\n"
    _add_spec(bundle, "events", events)
    (system,) = load_sources(bundle)
    assert {table.delivery for table in system.tables} == {"daily", "other"}


def test_several_deliveries_need_the_table_to_choose(tmp_path: Path) -> None:
    source = SOURCE.replace(
        "  daily: {trigger: file_arrival}\n",
        '  daily: {trigger: file_arrival}\n  weekly: {schedule: "0 0 2 ? * MON"}\n',
    )
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(_bundle(tmp_path, source=source))
    assert "delivery must be one of daily, weekly" in str(caught.value)
    table = _only_table(_bundle(tmp_path, source=source, spec=SPEC + "delivery: weekly\n"))
    assert (table.delivery, table.config.job_name) == ("weekly", "files_weekly")


def test_defaults_in_source_yaml_are_refused(tmp_path: Path) -> None:
    source = SOURCE + "defaults:\n  data: {classification: internal}\n"
    with pytest.raises(ConfigValidationError) as caught:
        load_sources(_bundle(tmp_path, source=source))
    assert "defaults is not supported" in str(caught.value)


def test_quarantine_sits_in_the_bronze_schema_unless_the_spec_names_another(
    tmp_path: Path,
) -> None:
    config = _only_table(_bundle(tmp_path)).config
    assert config.table_name("quarantine") == "catalog.files_bronze.things_qrntn"
    named = SPEC.replace("{table: things}", "{schema: lake_bronze, table: things}")
    config = _only_table(_bundle(tmp_path, spec=named)).config
    assert config.table_name("quarantine") == "catalog.lake_bronze.things_qrntn"
    spec = SPEC + "quarantine: {schema: files_rejects, table: bad_things}\n"
    config = _only_table(_bundle(tmp_path, spec=spec)).config
    assert config.table_name("quarantine") == "catalog.files_rejects.bad_things"


def test_new_source_names_only_the_schema_and_new_table_names_the_volume(tmp_path: Path) -> None:
    new_source(
        tmp_path,
        "finance",
        "files",
        owner="files.owner@jll.com",
        kind="sftp_files",
        delivery="daily",
    )
    source_text = (tmp_path / "sources" / "files" / "source.yaml").read_text(encoding="utf-8")
    assert "schema: files_raw" in source_text
    assert "volume:" not in source_text.replace("# ", "").split("raw_volume:")[1].split("\n\n")[0]
    path = new_table(
        tmp_path, "finance", "files", "things", requested_by="reporting", delivery=None
    )
    text = path.read_text(encoding="utf-8")
    for inherited in (
        "domain:",
        "team:",
        "technical_owner:",
        "layer:",
        "job_name:",
        "requested_by:",
    ):
        assert not any(line.startswith(inherited) for line in text.splitlines()), inherited
    assert "pipeline_id: files_things" in text
    assert "  volume: TODO_volume" in text
    assert "\ndata:\n  classification: internal\n" in text
    assert path.parent.name == "reporting"
