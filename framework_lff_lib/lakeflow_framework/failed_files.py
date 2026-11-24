"""Park the files a ``fail`` rule blocked in a ``_failed`` folder at the root of their volume.

A blocked file is already read: none of its rows loaded, its bad rows are in quarantine, and the
ledger says FAILED. Leaving it among the good files makes it look pending, so it is moved to
``/Volumes/<catalog>/<schema>/<volume>/_failed/``, keeping its path below the volume (a file at
``<volume>/invoices/a.csv`` is parked as ``<volume>/_failed/invoices/a.csv``). Several tables can
share a volume and one ``_failed`` folder, and their files never collide.

Nothing reads that folder:
- Auto Loader and the volume listing skip a folder whose name starts with ``_``.
- A table cannot be pointed at it, or, for COPY INTO and CTAS, at the volume root that contains it
  (``config._failed_folder_problem`` refuses both when the spec is validated).

A backfill of failed files moves them back first (``restore_failed``), loads them through the table's
rules, and parks again any that are still blocked.
"""

from __future__ import annotations

import logging
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lakeflow_framework.config import BACKFILLED_FOLDER, FAILED_FOLDER
from lakeflow_framework.sqlnames import require_catalog, sql_string
from lakeflow_framework.sqlnames import spark_rows as _rows

log = logging.getLogger(__name__)

# Both folders sit at the root of a volume and are skipped by Auto Loader, because their names
# start with an underscore.
PARKING_FOLDERS = (FAILED_FOLDER, BACKFILLED_FOLDER)
_VOLUME = re.compile(r"^(/Volumes/[\w\-]+/[\w\-]+/[\w\-]+)(/.*)?$")
# The error_message ledger_detail gives a file blocked by a fail rule (see apply_files).
BLOCKED_MESSAGE = "blocked by fail rule"
LOOKBACK_DAYS = 30


def plain_path(path: str) -> str:
    """A file's path without the scheme the file source puts in front of it."""

    for scheme in ("dbfs:", "file:"):
        if path.startswith(scheme):
            return path[len(scheme) :]
    return path


def volume_root(path: str) -> Path | None:
    """The ``/Volumes/<catalog>/<schema>/<volume>`` a path is in, or None when it is not in one."""

    found = _VOLUME.match(plain_path(path))
    return Path(found.group(1)) if found else None


def parked_path(root: str, path: str, folder: str = FAILED_FOLDER) -> Path | None:
    """Where a file goes: ``<volume>/<folder>/<its path below the volume>``.

    ``root`` is the table's folder. None for a path that is not below it, is not in a volume, or is
    already parked, so one table never moves another table's file.
    """

    base, file = Path(root.rstrip("/")), Path(plain_path(path))
    volume = volume_root(str(file))
    if volume is None:
        return None
    try:
        file.relative_to(base)
        relative = file.relative_to(volume)
    except ValueError:
        return None
    if relative.parts and relative.parts[0] in PARKING_FOLDERS:
        return None
    return volume / folder / relative


def unparked_path(path: str) -> str:
    """The path a file had before it was moved: ``<volume>/_failed/x/a.csv`` is ``<volume>/x/a.csv``,
    and so is ``<volume>/_backfilled/x/a.csv``.

    A path that is not in a ``_failed`` folder comes back unchanged (without any scheme), so a user
    can give either the place a blocked file used to be or the place it sits now.
    """

    plain = plain_path(path)
    volume = volume_root(plain)
    if volume is None:
        return plain
    try:
        relative = Path(plain).relative_to(volume)
    except ValueError:
        return plain
    if not relative.parts or relative.parts[0] not in PARKING_FOLDERS or len(relative.parts) < 2:
        return plain
    return str(volume / _SUFFIX.sub("", str(Path(*relative.parts[1:]))))


def move_failed(root: str, path: str) -> Path | None:
    """Move one blocked file into ``_failed``."""

    return _move(root, path, FAILED_FOLDER)


def move_aside(root: str, path: str) -> Path | None:
    """Move a file a backfill loaded into ``_backfilled``, so the pipeline does not read it."""

    return _move(root, path, BACKFILLED_FOLDER)


def _move(root: str, path: str, folder: str) -> Path | None:
    """Move one file into ``folder``. Returns where it went, or None when there was
    nothing to do: the path is outside the root, already moved, or no longer there.

    A file already parked under the same name is kept: the new one gets a timestamp suffix.
    """

    target = parked_path(root, path, folder)
    source = Path(plain_path(path))
    if target is None or not source.is_file():
        return None
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target = target.with_name(f"{target.name}.{datetime.now(UTC):%Y%m%dT%H%M%S}")
    shutil.move(str(source), str(target))
    log.info("moved %s to %s", source, target)
    return target


def restore_failed(root: str, paths: list[str]) -> list[Path]:
    """Move files in ``_failed`` back to where they were, so they can be loaded again."""

    return _restore(root, paths, FAILED_FOLDER)


def restore_aside(root: str, paths: list[str]) -> list[Path]:
    """Move files in ``_backfilled`` back to where they were."""

    return _restore(root, paths, BACKFILLED_FOLDER)


def _restore(root: str, paths: list[str], folder: str) -> list[Path]:
    """Move files in ``folder`` back to where they were, so they can be loaded again.

    A path that is not parked is left alone. When a name was parked more than once, the newest wins.
    """

    restored: list[Path] = []
    for path in paths:
        original = Path(plain_path(path))
        parked = parked_path(root, path, folder)
        if parked is None or original.exists():
            continue
        candidates = [parked] if parked.is_file() else []
        candidates += sorted(
            parked.parent.glob(f"{parked.name}.*"), key=lambda item: item.stat().st_mtime
        )[-1:]
        if not candidates:
            continue
        original.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(candidates[-1]), str(original))
        restored.append(original)
    return restored


_SUFFIX = re.compile(r"\.\d{8}T\d{6}$")


def list_parked(root: str) -> list[str]:
    """The blocked files parked in ``_failed`` from this table's folder, by their original paths."""

    return _list(root, FAILED_FOLDER)


def list_aside(root: str) -> list[str]:
    """The files set aside in ``_backfilled`` from this table's folder, by their original paths."""

    return _list(root, BACKFILLED_FOLDER)


def _list(root: str, parking: str) -> list[str]:
    """The files moved into ``parking`` from this table's folder, as the paths they had before.

    A name parked more than once carries a timestamp suffix, which is dropped here, so each file
    is listed once. Empty when the folder is not in a volume or nothing was parked.
    """

    volume = volume_root(root)
    if volume is None:
        return []
    try:
        below = Path(root.rstrip("/")).relative_to(volume)
    except ValueError:
        return []
    folder = volume / parking
    start = folder / below
    if not start.is_dir():
        return []
    return sorted(
        {
            str(volume / _SUFFIX.sub("", str(path.relative_to(folder))))
            for path in start.rglob("*")
            if path.is_file()
        }
    )


def blocked_paths_sql(catalog: str, pipeline_id: str) -> str:
    """The files whose latest ledger status is FAILED because a fail rule blocked them."""

    meta = f"{require_catalog(catalog)}.framework_meta"
    return (
        f"SELECT src_file_path FROM {meta}.ledger_detail "
        f"WHERE dataflow = {sql_string(pipeline_id)} AND detail_kind = 'file' "
        f"AND src_file_path IS NOT NULL AND recorded_at >= current_timestamp() - INTERVAL "
        f"{LOOKBACK_DAYS} DAYS "
        "QUALIFY row_number() OVER (PARTITION BY src_file_path ORDER BY recorded_at DESC) = 1 "
        f"AND status = 'FAILED' AND error_message LIKE {sql_string(BLOCKED_MESSAGE + '%')}"
    )


def park_failed_files(spark: Any, catalog: str, pipeline_ids: list[str]) -> dict[str, list[str]]:
    """Move every blocked file of these tables into its ``_failed`` folder. Returns what moved.

    The ledger is the list: a file moves when its latest status is FAILED for a blocking rule and
    it is still among the good files. Running it again moves nothing more.
    """

    from lakeflow_framework.backfill import load_config

    moved: dict[str, list[str]] = {}
    for pipeline_id in pipeline_ids:
        config = load_config(spark, catalog, pipeline_id)
        found = _rows(spark.sql(blocked_paths_sql(catalog, pipeline_id)))
        moved[pipeline_id] = [
            str(target)
            for row in found
            if (target := move_failed(config.path, str(row["src_file_path"]))) is not None
        ]
    return moved


def read_by_pipeline(spark: Any, catalog: str, pipeline_id: str, paths: list[str]) -> set[str]:
    """The files among ``paths`` that the table's pipeline has already read.

    The ledger says so: a pipeline update recorded the file in ``ledger_detail``. A backfill's own
    rows do not count (its update id starts with ``backfill-``), because a backfill does not move the
    pipeline's checkpoint. A file the pipeline has not read is one its next run would read.
    """

    meta = f"{require_catalog(catalog)}.framework_meta"
    wanted = {plain_path(path) for path in paths}
    found: set[str] = set()
    ordered = sorted(wanted)
    for start in range(0, len(ordered), 500):
        listed = ", ".join(sql_string(path) for path in ordered[start : start + 500])
        rows = _rows(
            spark.sql(
                f"SELECT DISTINCT src_file_path FROM {meta}.ledger_detail "
                f"WHERE dataflow = {sql_string(pipeline_id)} AND update_id NOT LIKE 'backfill-%' "
                f"AND src_file_path IN ({listed})"
            )
        )
        found.update(plain_path(str(row["src_file_path"])) for row in rows)
    return found & wanted
