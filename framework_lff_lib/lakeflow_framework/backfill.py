"""Reload chosen files of one table, or refresh the whole table, through the table's own rules.

One job serves every table: it takes the table's id, loads that table's spec from the registry,
and runs the same staging step as the pipeline (``pipeline.stage_frame``), so the same read options,
rules, rule actions and audit columns apply. Nothing of the pipeline is copied.

Modes:

- ``files``: the named files.
- ``dates``: files that arrived (were last modified) between ``date_from`` and ``date_to``.
- ``failed``: files whose latest ``ledger_detail`` status is FAILED, to try them again.
- ``rescue``: the named files, read again once the table has gained the column they rescued. A column
  the table still lacks goes back into ``_rescued_data`` and the file is recorded with a warning, so the
  same files can be run again later. A value that does not fit its column's type is rescued again.
- ``full_refresh``: every file again, by refreshing the table's own pipeline with a full refresh.

With none of ``files``, ``dates`` or ``failed``, the selector is empty, which means every file in the
table's directory. That and ``full_refresh`` need ``confirm``.

A file's earlier rows are removed from bronze and quarantine before it is loaded again, so a
reload never leaves a duplicate. A file blocked by a ``fail`` rule is recorded FAILED, its bad rows go
to quarantine, none of its good rows load, and the backfill goes on with the next file. The run still
ends successfully, with a warning, because one bad file must not stop a thousand good ones.
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from lakeflow_framework.config import (
    BACKFILLED_FOLDER,
    BATCH_ID,
    FAILED_FOLDER,
    RESCUED_DATA,
    RULE_ACTIONS,
    SRC_FILE_NAME,
    SRC_FILE_PATH,
    PipelineConfig,
)
from lakeflow_framework.failed_files import (
    list_aside,
    list_parked,
    move_aside,
    move_failed,
    park_failed_files,
    plain_path,
    read_by_pipeline,
    restore_aside,
    restore_failed,
    unparked_path,
)
from lakeflow_framework.load import get_strategy
from lakeflow_framework.pipeline import (
    FILE_FAILED,
    FILE_STATUS,
    PIPELINE_SCHEMA,
    blocked_check_name,
    file_columns,
    files_name,
    files_sql,
    has_blocking,
    has_file_counts,
    is_file_load,
    quarantine_frame,
    stage_frame,
)
from lakeflow_framework.pipeline_metrics import (
    EMPTY_FILE,
    FILE_LEDGER,
    FILE_SCHEMA,
    apply_files,
    file_records,
    mark_failed,
    write_rows,
)
from lakeflow_framework.quality import DQ_COLUMN, DQ_RESULT, blocking_rules, compile_rules
from lakeflow_framework.sqlnames import IDENTIFIER, require_catalog, sql_string
from lakeflow_framework.volume_listing import list_volume_files

log = logging.getLogger(__name__)

MODES = ("files", "dates", "failed", "rescue", "full_refresh")
STATE_VOLUME = "backfill_state"
LAYER = "bronze"
_FILE_NAME = re.compile(r"^[^/\\{}*?\[\]]+$")
# A full path to a file in a volume: /Volumes/<catalog>/<schema>/<volume>/<folders>/<file>.
_FILE_PATH = re.compile(r"^/Volumes/[\w\-]+/[\w\-]+/[\w\-]+(/[^/\\{}*?\[\]]+)+$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$")
MAX_LISTED = 50
# A full refresh is the pipeline's update. The job waits for it, so it can park what it blocked.
UPDATE_DONE = frozenset({"COMPLETED", "FAILED", "CANCELED"})
POLL_SECONDS = 20
SETTLE_SECONDS = 30  # the pipeline writes the ledger as the update ends
FULL_REFRESH_WAIT_SECONDS = 6 * 3600


class BackfillError(Exception):
    """The request cannot be run as asked. Raised before anything is changed."""


@dataclass(frozen=True)
class Selection:
    mode: str
    files: tuple[str, ...] = ()
    date_from: str | None = None
    date_to: str | None = None

    @property
    def everything(self) -> bool:
        """No selector at all: every file in the table's directory."""

        return self.mode == "files" and not self.files

    @property
    def paths(self) -> tuple[str, ...]:
        """The files given as a full volume path, as they were before any move to ``_failed``."""

        return tuple(unparked_path(item) for item in self.files if item.startswith("/"))

    @property
    def names(self) -> tuple[str, ...]:
        """The files given as a bare name, which can match a file in any folder of the table."""

        return tuple(item for item in self.files if not item.startswith("/"))


@dataclass
class Result:
    table: str
    mode: str
    planned: list[str] = field(default_factory=list)
    files: list[dict[str, Any]] = field(default_factory=list)
    rows_replaced: int = 0
    dry_run: bool = False
    update_id: str = ""
    warnings: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)  # requested file names that are not there
    empty: list[str] = field(default_factory=list)  # files that were read and held no rows
    none_found: bool = False  # the dates or the directory held no file at all
    on_fail: str = ""  # the action every rule had for this run instead of the table's own
    update_state: str = ""  # how a full refresh's pipeline update ended
    restored: list[str] = field(default_factory=list)  # files moved back out of _failed to reload
    parked: list[str] = field(default_factory=list)  # files still blocked, moved into _failed again
    set_aside: list[str] = field(default_factory=list)  # loaded here, not read by the pipeline yet
    not_in_table: list[str] = field(default_factory=list)  # mode rescue: kept in _rescued_data
    not_in_file: list[str] = field(
        default_factory=list
    )  # mode rescue: the table has, the file lacks
    error: str = ""  # why the load stopped, when it did


def is_pattern(value: str) -> bool:
    """True for a value still holding a ``<placeholder>`` the job's default put there."""

    return bool(re.search(r"<[^<>\s]+>", value or ""))


def parse_selection(
    mode: str, files: str = "", date_from: str = "", date_to: str = ""
) -> Selection:
    """Check what was asked for.

    ``files`` is a comma-separated list. Each item is a full volume path
    (``/Volumes/<catalog>/<schema>/<volume>/<folder>/a.csv``), which is exactly one file, or a bare
    file name, which matches a file of that name in any folder of the table. Patterns are refused.
    """

    mode = (mode or "").strip().lower()
    if is_pattern(mode):
        raise BackfillError(
            f"mode still holds the pattern {mode!r}: choose one of {', '.join(MODES)}"
        )
    if mode not in MODES:
        raise BackfillError(f"mode must be one of {', '.join(MODES)}, not {mode!r}")
    names = tuple(name.strip() for name in (files or "").split(",") if name.strip())
    # The job's default for ``files`` is a pattern (<raw_schema>, <volume> ...) for the user to edit.
    # In mode files it must have been edited. In any other mode it is not an answer, so it is ignored.
    unedited = [name for name in names if is_pattern(name)]
    if unedited and mode in {"files", "rescue"}:
        raise BackfillError(
            f"files still holds the pattern {unedited[0]!r}: replace the parts in <angle brackets> "
            "with a real volume path"
            + (", or clear files to reload every file (with confirm)" if mode == "files" else "")
        )
    if unedited:
        names = tuple(name for name in names if not is_pattern(name))
    start, end = (date_from or "").strip() or None, (date_to or "").strip() or None
    if mode in {"files", "rescue"}:
        if start or end:
            raise BackfillError(f"mode {mode} takes file names, not dates; use mode dates")
        for name in names:
            if name.startswith("/"):
                if not _FILE_PATH.match(name) or "/../" in name or "/./" in name:
                    raise BackfillError(
                        f"{name!r} is not a full volume path: use "
                        "/Volumes/<catalog>/<schema>/<volume>/<folder>/<file>"
                    )
            elif not _FILE_NAME.match(name):
                raise BackfillError(
                    f"{name!r} is not a file name or a full volume path: no patterns or folders"
                )
        if mode == "rescue" and not names:
            raise BackfillError(
                "mode rescue needs the files to read again: put their full volume paths in files"
            )
        return Selection(mode, files=names)
    if names:
        raise BackfillError(f"mode {mode} does not take file names")
    if mode == "dates":
        if not (start or end):
            raise BackfillError("mode dates needs date_from or date_to")
        for value in (start, end):
            if value and not _DATE.match(value):
                raise BackfillError(f"{value!r} is not a date: use YYYY-MM-DD or YYYY-MM-DD HH:MM")
        if start and end and start > end:
            raise BackfillError(f"date_from {start} is after date_to {end}")
        return Selection(mode, date_from=start, date_to=end)
    if start or end:
        raise BackfillError(f"mode {mode} does not take dates")
    return Selection(mode)


def reader_filters(selection: Selection) -> dict[str, str]:
    """File-source options that pick the files. Dates are arrival (modification) times."""

    options: dict[str, str] = {}
    if selection.mode in {"files", "rescue"} and selection.files:
        # The source filters on a file's name. A file given by full path is then picked out exactly
        # in ``_load``, because another folder can hold a file with the same name.
        names = dict.fromkeys(item.rsplit("/", 1)[-1] for item in selection.files)
        options["pathGlobFilter"] = "{" + ",".join(names) + "}"
    if selection.date_from:
        options["modifiedAfter"] = _stamp(selection.date_from)
    if selection.date_to:
        options["modifiedBefore"] = _stamp(selection.date_to, end=True)
    return options


def _stamp(value: str, *, end: bool = False) -> str:
    """A date as the file source wants it: YYYY-MM-DDTHH:MM:SS, in UTC.

    ``date_from`` is the start of its day when it has no time. ``date_to`` without a time reaches
    the end of that day, so 2026-11-01 to 2026-11-03 includes everything that arrived on the 3rd.
    """

    when = datetime.fromisoformat(value.replace(" ", "T"))
    if end and "T" not in value and " " not in value:
        when += timedelta(days=1)
    return when.strftime("%Y-%m-%dT%H:%M:%S")


_TABLE_NAME = re.compile(r"^[A-Za-z0-9_\-]+(\.[A-Za-z0-9_\-]+){0,2}$")
_TABLE_KINDS = ("table", "streaming_table", "materialized_view")


def load_config(spark: Any, catalog: str, name: str) -> PipelineConfig:
    """The table's spec from the registry, with this environment's catalog.

    ``name`` is the table's pipeline id (``abs_gl``). The name of its bronze or quarantine table
    works too, bare (``gl_abs``) or full (``aal_dev_fdh.abs_bronze.gl_abs``), as long as exactly one
    table in the registry has it.
    """

    from lakeflow_framework.config import config_from_document
    from lakeflow_framework.pipeline import UnityMetadata, _with_catalog

    if is_pattern(name):
        raise BackfillError(
            f"table still holds the pattern {name!r}: replace the parts in <angle brackets> "
            "with the bronze table's full name, e.g. aal_dev_fdh.abs_bronze.gl_abs"
        )
    if not _TABLE_NAME.match(name or ""):
        raise BackfillError(f"{name!r} is not a pipeline id or a table name")
    registry = UnityMetadata(spark, catalog)
    document = registry.get_pipeline(name) if IDENTIFIER.match(name) else None
    if not document:
        pipeline_id = _pipeline_of_table(spark, catalog, name)
        document = registry.get_pipeline(pipeline_id)
        if not document:
            raise BackfillError(f"{pipeline_id} is not in the registry; deploy it first")
    return config_from_document({**_with_catalog(document, catalog), "catalog": catalog})


def _pipeline_of_table(spark: Any, catalog: str, name: str) -> str:
    """The pipeline id that loads the table called ``name``, from the registry.

    Raises when no table has that name, or when more than one pipeline does.
    """

    meta = f"{require_catalog(catalog)}.framework_meta"
    last = name.rsplit(".", 1)[-1]
    kinds = ", ".join(sql_string(kind) for kind in _TABLE_KINDS)
    found = spark.sql(
        f"SELECT full_name, config_json:pipeline_id AS pipeline_id FROM {meta}.asset_registry_current "
        f"WHERE object_kind IN ({kinds}) AND full_name LIKE {sql_string('%' + last)}"
    ).collect()
    # LIKE treats _ as a wildcard, so the match is checked again here, exactly.
    matches = {
        str(row["pipeline_id"]): str(row["full_name"])
        for row in (item.asDict() if hasattr(item, "asDict") else dict(item) for item in found)
        if row["pipeline_id"]
        and (str(row["full_name"]) == name or str(row["full_name"]).endswith("." + name))
    }
    if len(matches) == 1:
        return next(iter(matches))
    if matches:
        listed = ", ".join(f"{table} (table id {pid})" for pid, table in sorted(matches.items()))
        raise BackfillError(f"{name} matches more than one table: {listed}; give the table id")
    raise BackfillError(
        f"{name} is not in the registry as a table id or a table name; deploy it first "
        "(the table id is the pipeline_id at the top of its spec)"
    )


def with_on_fail(config: PipelineConfig, on_fail: str) -> PipelineConfig:
    """The table's spec with one action for every rule, for this run only.

    ``on_fail`` replaces the action of each rule the spec declares and of the checks the framework
    adds, as ``raw_to_bronze.on_fail`` does. Empty keeps the table's own. The registry is not
    changed, so the next regular load judges its files by the table's own setting again.
    """

    action = (on_fail or "").strip().lower()
    if not action:
        return config
    if action not in RULE_ACTIONS:
        raise BackfillError(f"on_fail must be one of {', '.join(RULE_ACTIONS)}, not {on_fail!r}")
    return replace(
        config, on_fail=action, rules=[replace(rule, action=action) for rule in config.rules]
    )


def check_supported(config: PipelineConfig, selection: Selection) -> None:
    if selection.mode == "full_refresh":
        return
    if config.load_method != "volume_files" or not is_file_load(config):
        raise BackfillError(
            f"{config.pipeline_id} loads by {config.load_method}: only full_refresh can reload it"
        )
    if config.uses_auto_cdc:
        raise BackfillError(
            f"{config.pipeline_id} keeps history by key (load_type {config.load_type}): "
            "only full_refresh can reload it"
        )


def plan_files(spark: Any, config: PipelineConfig, selection: Selection) -> list[dict[str, Any]]:
    """The files the selection picks, by path, arrival time and size. Reads no file content."""

    if selection.mode == "failed":
        return _failed_files(spark, config)
    after = (
        datetime.fromisoformat(_stamp(selection.date_from)).replace(tzinfo=UTC)
        if selection.date_from
        else None
    )
    before = (
        datetime.fromisoformat(_stamp(selection.date_to, end=True)).replace(tzinfo=UTC)
        if selection.date_to
        else None
    )
    # No names means every file, not none.
    chosen = selection.mode in {"files", "rescue"} and bool(selection.files)
    names, paths = set(selection.names), set(selection.paths)

    def wanted(path: str) -> bool:
        return not chosen or path.rsplit("/", 1)[-1] in names or plain_path(path) in paths

    planned = [
        {"path": item["file_path"], "arrived": item["arrived_at"], "bytes": item["file_bytes"]}
        for item in list_volume_files(config.path, config.path_glob)
        if wanted(item["file_path"])
        and (after is None or item["arrived_at"] > after)
        and (before is None or item["arrived_at"] < before)
    ]
    if chosen:
        # Something the user named that is not among the files may be a file a fail rule blocked
        # and parked. Naming it is how it is loaded again, so it is planned from where it was.
        found = {plain_path(item["path"]) for item in planned}
        found_names = {path.rsplit("/", 1)[-1] for path in found}
        planned += [
            {"path": path, "arrived": None, "bytes": None}
            for path in [*list_parked(config.path), *list_aside(config.path)]
            if path not in found
            and (path in paths or path.rsplit("/", 1)[-1] in names - found_names)
        ]
    return planned


def check_files_belong(config: PipelineConfig, selection: Selection) -> None:
    """Refuse a file given by full path that is not in this table's folder.

    The path may be where the file is, or where a blocked file sits in ``_failed``. Another table's
    file is never reloaded through this one, whatever the path says.
    """

    for path in selection.paths:
        if not plain_path(path).startswith(config.path):
            raise BackfillError(
                f"{path} is not in this table's folder {config.path}: "
                "give the table that reads it, or a path inside that folder"
            )


def _plain_path(path: str) -> str:
    """A file's path as bronze stores it in ``_src_file_path``: no ``dbfs:`` or ``file:`` prefix."""

    for scheme in ("dbfs:", "file:"):
        if path.startswith(scheme):
            return path[len(scheme) :]
    return path


def _failed_files(spark: Any, config: PipelineConfig) -> list[dict[str, Any]]:
    meta = f"{require_catalog(config.catalog)}.framework_meta"
    rows = spark.sql(
        f"SELECT src_file_path AS path FROM {meta}.ledger_detail "
        f"WHERE dataflow = {sql_string(config.pipeline_id)} "
        "QUALIFY row_number() OVER (PARTITION BY src_file_path ORDER BY recorded_at DESC) = 1 "
        "AND status = 'FAILED' ORDER BY src_file_path"
    ).collect()
    return [{"path": str(row["path"]), "arrived": None, "bytes": None} for row in rows]


def _data_columns(spark: Any, config: PipelineConfig) -> list[str]:
    """The table's own columns: not the ones the framework adds, nor ``_rescued_data``."""

    from lakeflow_framework.metadata import metadata_column_names

    skip = {name.lower() for name in metadata_column_names()} | {RESCUED_DATA}
    return [name for name in _bronze_columns(spark, config) if name.lower() not in skip]


def _bronze_hints(spark: Any, config: PipelineConfig) -> str:
    """The table's column types, as Auto Loader hints, for a table that declares none.

    The pipeline reads a value that does not fit its column into ``_rescued_data``. A reload that
    inferred its own types would read the same value as text and fail to write it, so it reads
    with the table's types as the pipeline does.
    """

    from lakeflow_framework.load import HINT_FORMATS
    from lakeflow_framework.metadata import metadata_column_names

    if config.columns or config.fixed_columns or config.file_format not in HINT_FORMATS:
        return ""
    skip = {name.lower() for name in metadata_column_names()} | {RESCUED_DATA}
    try:
        fields = spark.table(config.table_name("bronze")).schema.fields
    except Exception:
        log.exception("could not read the table's types: the reload infers its own")
        return ""
    return ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        for field in fields
        if field.name.lower() not in skip
    )


def _bronze_columns(spark: Any, config: PipelineConfig) -> list[str]:
    return [str(name) for name in spark.table(config.table_name("bronze")).columns]


def _still_rescued(spark: Any, config: PipelineConfig, paths: list[str]) -> tuple[int, list[str]]:
    """After a reload: how many of these files' rows still hold rescued values, and for which columns."""

    rows, names = 0, set()
    keys = f"filter(map_keys(from_json({RESCUED_DATA}, 'map<string,string>')), key -> key <> '_file_path')"
    for start in range(0, len(paths), 500):
        listed = ", ".join(sql_string(path) for path in paths[start : start + 500])
        found = spark.sql(
            f"SELECT count(*) AS n, array_distinct(flatten(collect_list(coalesce({keys}, "
            "CAST(array() AS ARRAY<STRING>))))) AS keys "
            f"FROM {config.table_name('bronze')} WHERE {RESCUED_DATA} IS NOT NULL "
            f"AND {SRC_FILE_PATH} IN ({listed})"
        ).collect()
        if found:
            rows += int(found[0]["n"] or 0)
            names |= {str(key) for key in (found[0]["keys"] or [])}
    return rows, sorted(names)


def _say_what_is_rescued(spark: Any, config: PipelineConfig, result: Result) -> None:
    """After a reload in mode rescue: say which columns are still kept in ``_rescued_data``."""

    if result.not_in_table:
        result.warnings.append(
            f"{len(result.not_in_table)} column(s) in the file(s) are not in the table yet: "
            f"{', '.join(result.not_in_table)}. Their values are kept in {RESCUED_DATA}. Add them "
            "to the table (declare them in the spec or set schema_evolution: add_new_columns, "
            "deploy, run the regular job once), then run this again"
        )
    if result.not_in_file:
        result.warnings.append(
            f"{len(result.not_in_file)} column(s) the table has are not in the file(s): "
            f"{', '.join(result.not_in_file)}. They are NULL for these rows"
        )
    left, names = _still_rescued(spark, config, result.planned)
    known = {name.lower() for name in result.not_in_table}
    other = [name for name in names if name.lower() not in known]
    if left and other:
        result.warnings.append(
            f"rows still hold rescued values for: {', '.join(other)}. A value that does not fit "
            "its column's type stays rescued: keep the column a string, or change the type and "
            "run a full_refresh"
        )


def fold_into_rescued(frame: Any, extras: list[str]) -> Any:
    """Put columns the table does not have back into ``_rescued_data`` and drop them.

    A column that is there now but not in the table cannot be inserted, so its values go where the
    pipeline would have put them, merged with what Auto Loader already rescued. An empty value is
    left out, and a row with nothing to rescue keeps ``_rescued_data`` null.
    """

    def ident(name: str) -> str:
        return "`" + name.replace("`", "``") + "`"

    pairs = ", ".join(f"{sql_string(name)}, CAST({ident(name)} AS STRING)" for name in extras)
    merged = (
        f"map_concat(coalesce(from_json({RESCUED_DATA}, 'map<string,string>'), "
        "CAST(map() AS MAP<STRING, STRING>)), "
        f"map_filter(map({pairs}), (key, value) -> value IS NOT NULL))"
    )
    dropped = ", ".join([ident(name) for name in extras] + [RESCUED_DATA])
    return frame.selectExpr(
        f"* EXCEPT ({dropped})",
        f"CASE WHEN size({merged}) = 0 THEN NULL ELSE to_json({merged}) END AS {RESCUED_DATA}",
    )


def run_backfill(
    spark: Any,
    catalog: str,
    pipeline_id: str,
    selection: Selection,
    *,
    dry_run: bool = False,
    confirm: bool = False,
    job_run_id: str | None = None,
    on_fail: str = "",
) -> Result:
    """Reload the selected files of one table. Returns what was done (or would be).

    ``on_fail`` judges the reloaded files by that action instead of the table's own, for this run.
    """

    config = load_config(spark, catalog, pipeline_id)
    check_supported(config, selection)
    check_files_belong(config, selection)
    if (on_fail or "").strip() and selection.mode == "full_refresh":
        raise BackfillError(
            "on_fail applies when files are reloaded here. A full_refresh runs the table's own "
            "pipeline with the table's own rules; change on_fail in the spec for that"
        )
    config = with_on_fail(config, on_fail)
    # ``pipeline_id`` may have been a table name: from here on the table is its pipeline id.
    result = Result(
        config.pipeline_id, selection.mode, dry_run=dry_run, on_fail=(on_fail or "").strip().lower()
    )
    if selection.mode == "full_refresh":
        return _full_refresh(spark, config, result, confirm=confirm)
    if selection.everything and not confirm and not dry_run:
        raise BackfillError(
            "no files or dates were given, which reloads every file in "
            f"{config.path}. Run with dry_run first, or confirm to go ahead"
        )
    planned = plan_files(spark, config, selection)
    result.planned = [item["path"] for item in planned]
    result.missing = _not_found(selection, result.planned)
    # A request for files that are not there is a failure to record, not nothing to do. Retrying
    # the failed files (mode failed) with none failed is fine: there is nothing to retry.
    result.none_found = not planned and not result.missing and selection.mode != "failed"
    if selection.mode in {"files", "failed", "rescue"} and planned:
        # Files named by the user (or by the ledger, in mode failed) may be parked in _failed. Only
        # these are brought back. Dates, no selection and a full refresh read the files that are in
        # the folder and leave _failed alone.
        in_failed = set(list_parked(config.path))
        in_aside = set(list_aside(config.path))
        result.restored = [path for path in result.planned if path in in_failed | in_aside]
    if dry_run:
        return result
    if not planned and not (result.missing or result.none_found):
        return result
    update_id = f"backfill-{job_run_id or uuid.uuid4()}"
    result.update_id = update_id
    started = datetime.now(UTC)
    batch = str(uuid.uuid4())
    if result.restored:
        # _failed is skipped by Auto Loader, so a named file is moved back to be read through the
        # table's rules. If it is still blocked it is parked again after the load.
        restore_failed(config.path, result.restored)
        restore_aside(config.path, result.restored)
    if planned:
        # What is replaced is counted before anything is deleted: the load runs in worker
        # processes, which cannot hand numbers back through memory.
        result.rows_replaced = _count_existing(spark, result.planned, config.table_name("bronze"))
        try:
            _load(spark, config, selection, result, batch=batch, update_id=update_id)
        except Exception as exc:
            _record_failure(
                spark, config, result, exc, started=started, update_id=update_id, batch=batch
            )
            raise
        # Each micro-batch wrote its files to ledger_detail as it went.
        result.files = _recorded_files(spark, config, update_id)
        if selection.mode == "rescue":
            _say_what_is_rescued(spark, config, result)
        recorded = {item["src_file_path"] for item in result.files}
        result.empty = [path for path in result.planned if path not in recorded]
    blocked = blocked_files(result)
    if blocked:
        # Still blocked: park them again, so they are not left among the good files.
        result.parked = [
            str(target)
            for item in blocked
            if (target := move_failed(config.path, item["src_file_path"])) is not None
        ]
        result.warnings.append(
            f"{len(blocked)} file(s) blocked by fail rules: "
            + ", ".join(item["src_file_name"] for item in blocked[:MAX_LISTED])
        )
    result.set_aside = _set_aside(spark, config, result)
    if result.set_aside:
        result.warnings.append(
            f"{len(result.set_aside)} file(s) moved to {BACKFILLED_FOLDER} so the regular "
            "job does not read them again: "
            + ", ".join(path.rsplit("/", 1)[-1] for path in result.set_aside[:MAX_LISTED])
        )
    if result.empty:
        result.warnings.append(
            f"{len(result.empty)} file(s) had no rows: "
            + ", ".join(path.rsplit("/", 1)[-1] for path in result.empty[:MAX_LISTED])
        )
    if result.missing:
        result.warnings.append(
            f"{len(result.missing)} file(s) not found: " + ", ".join(result.missing)
        )
    if result.none_found:
        result.warnings.append(f"no file found for this request in {config.path}")
    _record(spark, config, result, started=started, update_id=update_id, batch=batch)
    return result


def _not_found(selection: Selection, planned: list[str]) -> list[str]:
    """What was asked for, as it was given, that is not among the planned files."""

    if selection.mode not in {"files", "rescue"} or not selection.files:
        return []
    found = {plain_path(path) for path in planned}
    found_names = {path.rsplit("/", 1)[-1] for path in found}
    return [
        item
        for item in selection.files
        if (unparked_path(item) not in found if item.startswith("/") else item not in found_names)
    ]


def _load(
    spark: Any,
    config: PipelineConfig,
    selection: Selection,
    result: Result,
    *,
    batch: str,
    update_id: str,
) -> None:
    strategy = get_strategy(config.load_method)
    rules = compile_rules(config)
    blocking = blocking_rules(rules)
    state = f"/Volumes/{config.catalog}/{PIPELINE_SCHEMA}/{STATE_VOLUME}/{config.pipeline_id}/{update_id}"
    options = {
        **strategy.options(config),
        **reader_filters(selection),
        "cloudFiles.schemaLocation": f"{state}/schema",
    }
    hints = _bronze_hints(spark, config)
    if hints:
        options["cloudFiles.schemaHints"] = hints
    if selection.mode in {"failed", "rescue"}:
        names = sorted({path.rsplit("/", 1)[-1] for path in result.planned})
        options["pathGlobFilter"] = "{" + ",".join(names) + "}"
    # The source filters on a file's name only. A file given by full path, or taken from the ledger
    # in mode failed, is then picked out exactly, so a file of the same name in another folder is not
    # reloaded with it.
    exact = None
    if selection.paths or selection.mode in {"failed", "rescue"}:
        listed = ", ".join(sql_string(plain_path(path)) for path in result.planned)
        exact = f"regexp_replace({SRC_FILE_PATH}, '^(dbfs|file):', '') IN ({listed})"
    reader = spark.readStream.format("cloudFiles")
    for key, value in options.items():
        reader = reader.option(key, value)
    read = reader.load(config.path)
    bronze = config.table_name("bronze")
    if selection.mode == "rescue":
        # What the files have that the table has not yet goes back into _rescued_data, and the file is
        # recorded with a warning. Once the table has the column, a later run reads it as a column.
        have = {name.lower() for name in _bronze_columns(spark, config)}
        if RESCUED_DATA.lower() not in have:
            raise BackfillError(
                f"{bronze} has no {RESCUED_DATA} column: its format carries its own columns"
            )
        in_file = {name.lower() for name in read.columns}
        result.not_in_file = [
            name for name in _data_columns(spark, config) if name.lower() not in in_file
        ]
        extras = [name for name in read.columns if name.lower() not in have]
        if extras:
            result.not_in_table = extras
            read = fold_into_rescued(read, extras)
    staged = stage_frame(read.selectExpr("*", *file_columns(datetime.now(UTC))), config, batch)
    # A table without rules or a quarantine block has no quarantine table: its bad rows are dropped.
    quarantine = config.table_name("quarantine") if config.has_quarantine else None
    targets = [bronze, quarantine] if quarantine else [bronze]
    stub = {
        "job_name": config.job_name,
        "layer": config.layer or LAYER,
        "update_id": update_id,
        "batch_id": batch,
        "dataflow": config.pipeline_id,
        "target_table": bronze,
        "load_kind": "backfill",
        "started_at": datetime.now(UTC),
    }
    ledger = f"{require_catalog(config.catalog)}.framework_meta.{FILE_LEDGER}"

    # This runs in a worker process: it may use only what it is given, and it hands nothing back
    # but through the tables it writes.
    def process(frame: Any, _batch_number: int) -> None:
        # A micro-batch has its own session, and a temp view lives in the session that made it.
        session = frame.sparkSession
        if exact:
            frame = frame.filter(exact)
        view = f"_backfill_{uuid.uuid4().hex}"
        frame.createOrReplaceTempView(view)
        counted = session.sql(
            files_sql(view, blocking, this_run=False, quarantine=quarantine is not None)
        ).collect()
        files = [_as_file(row) for row in counted]
        paths = [item["src_file_path"] for item in files]
        blocked = [item["src_file_path"] for item in files if item["status"] == FILE_FAILED]
        good = frame.filter(f"{DQ_RESULT} = 'pass'")
        if blocked:
            # A blocked file loads nothing: its good rows stay out too. Its bad rows go to quarantine.
            keep = session.createDataFrame([(path,) for path in blocked], f"{SRC_FILE_PATH} STRING")
            good = good.join(keep, SRC_FILE_PATH, "left_anti")
        internal = f"{DQ_COLUMN}, {DQ_RESULT}"
        # The new rows are written and checked before anything old is deleted: a load that fails
        # leaves the table as it was (what was written is taken out again). Only then are the old
        # rows deleted and the file recorded as loaded.
        try:
            _insert(session, good.selectExpr(f"* EXCEPT ({internal})"), bronze)
            if quarantine:
                # Same shape as the pipeline's quarantine flow (bronze's types plus failed_rules).
                bad = quarantine_frame(
                    frame.filter(f"{DQ_RESULT} IN ('fail', 'quarantine')"), config, internal
                )
                _insert(session, bad, quarantine)
            _check_written(
                session, bronze, batch, paths, sum(int(i["rows_inserted"] or 0) for i in files)
            )
            if quarantine:
                _check_written(
                    session,
                    quarantine,
                    batch,
                    paths,
                    sum(int(i["rows_quarantined"] or 0) for i in files),
                )
        except Exception:
            _discard(session, paths, batch, *targets)
            raise
        _remove(session, paths, *targets, keep_batch=batch)
        marked = apply_files(dict(stub), files)["_files"]
        session.createDataFrame(
            file_records([{**stub, "_files": marked}], _now()), FILE_SCHEMA
        ).write.mode("append").saveAsTable(ledger)

    query = (
        staged.writeStream.foreachBatch(process)
        .option("checkpointLocation", f"{state}/checkpoint")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


def _now() -> datetime:
    return datetime.now(UTC)


def _count_existing(spark: Any, paths: list[str], table: str) -> int:
    """How many rows of these files bronze holds now: what a reload replaces."""

    total = 0
    for start in range(0, len(paths), 500):
        listed = ", ".join(sql_string(path) for path in paths[start : start + 500])
        found = spark.sql(
            f"SELECT count(*) AS n FROM {table} WHERE {SRC_FILE_PATH} IN ({listed})"
        ).collect()
        total += int(found[0][0] or 0) if found else 0
    return total


def _recorded_files(spark: Any, config: PipelineConfig, update_id: str) -> list[dict[str, Any]]:
    """The files this backfill wrote to ledger_detail, in the shape ``apply_files`` takes."""

    meta = f"{require_catalog(config.catalog)}.framework_meta"
    rows = spark.sql(
        f"SELECT * FROM {meta}.{FILE_LEDGER} WHERE update_id = {sql_string(update_id)} "
        f"AND dataflow = {sql_string(config.pipeline_id)} ORDER BY src_file_path"
    ).collect()
    files = []
    for row in rows:
        data = row.asDict() if hasattr(row, "asDict") else dict(row)
        files.append({**data, "load_kind": "backfill"})
    return files


def _set_aside(spark: Any, config: PipelineConfig, result: Result) -> list[str]:
    """Move the files this backfill loaded, and the pipeline has not read, into ``_backfilled``.

    A backfill does not touch the pipeline's checkpoint, so the pipeline's next run would read a file
    it has not read before and load it a second time. A file in a folder whose name starts with an
    underscore is never listed, so it is never read. A file the pipeline already read stays where
    it is: its checkpoint already keeps it from being read again. A blocked file was parked in
    ``_failed`` already.
    """

    loaded = [
        item["src_file_path"]
        for item in result.files
        if item.get("status") != FILE_FAILED and item.get("src_file_path")
    ]
    if not loaded:
        return []
    already_read = read_by_pipeline(spark, config.catalog, config.pipeline_id, loaded)
    return [
        str(target)
        for path in loaded
        if plain_path(path) not in already_read
        and (target := move_aside(config.path, path)) is not None
    ]


def blocked_files(result: Result) -> list[dict[str, Any]]:
    return [item for item in result.files if item["status"] == FILE_FAILED]


def _as_file(row: Any) -> dict[str, Any]:
    data = row.asDict() if hasattr(row, "asDict") else dict(row)
    dropped = int(data.get("rows_dropped") or 0) + int(data.get("rows_blocked") or 0)
    return {
        "detail_kind": "file",
        "src_file_path": data[SRC_FILE_PATH],
        "src_file_name": data[SRC_FILE_NAME],
        "status": data[FILE_STATUS],
        "rows_read": data["rows_read"],
        "rows_inserted": data.get("rows_loaded") or data.get("rows_inserted") or 0,
        "rows_quarantined": data["rows_quarantined"],
        "rows_dropped": dropped,
        "failed_rules": data["failed_rules"],
        "blocking_rules": data["blocking_rules"],
        "load_kind": "backfill",
    }


def _remove(spark: Any, paths: list[str], *tables: str, keep_batch: str | None = None) -> int:
    """Delete these files' earlier rows. Returns how many bronze rows were replaced.

    With ``keep_batch`` the rows that batch has just written stay: only earlier ones go.
    """

    replaced = 0
    older = (
        f" AND ({BATCH_ID} IS NULL OR {BATCH_ID} <> {sql_string(keep_batch)})" if keep_batch else ""
    )
    for start in range(0, len(paths), 500):
        listed = ", ".join(sql_string(path) for path in paths[start : start + 500])
        for number, table in enumerate(tables):
            done = spark.sql(
                f"DELETE FROM {table} WHERE {SRC_FILE_PATH} IN ({listed}){older}"
            ).collect()
            if number == 0 and done:
                replaced += int(done[0][0] or 0)
    return replaced


def _discard(spark: Any, paths: list[str], batch: str, *tables: str) -> None:
    """Take out the rows this batch wrote for these files, after a load that did not finish.

    The earlier rows were not touched, so this leaves the table as it was. It never hides the
    failure that brought it here: if it cannot run, it says so and the failure goes on.
    """

    for table in tables:
        for start in range(0, len(paths), 500):
            listed = ", ".join(sql_string(path) for path in paths[start : start + 500])
            try:
                spark.sql(
                    f"DELETE FROM {table} WHERE {BATCH_ID} = {sql_string(batch)} "
                    f"AND {SRC_FILE_PATH} IN ({listed})"
                ).collect()
            except Exception:
                log.exception("could not take this load's rows out of %s", table)


def _check_written(spark: Any, table: str, batch: str, paths: list[str], expected: int) -> None:
    """Stop, before anything is deleted or recorded, when the table does not hold the rows counted."""

    held = 0
    for start in range(0, len(paths), 500):
        listed = ", ".join(sql_string(path) for path in paths[start : start + 500])
        found = spark.sql(
            f"SELECT count(*) AS n FROM {table} WHERE {BATCH_ID} = {sql_string(batch)} "
            f"AND {SRC_FILE_PATH} IN ({listed})"
        ).collect()
        held += int(found[0][0] or 0) if found else 0
    if held != expected:
        raise BackfillError(
            f"{table} holds {held} of the {expected} row(s) this load counted, so it is not "
            "recorded as loaded"
        )


def _insert(spark: Any, frame: Any, table: str) -> None:
    # A column the table has and the file lacks is NULL for these rows, written out.
    have = {name.lower() for name in frame.columns}
    absent = [field for field in spark.table(table).schema.fields if field.name.lower() not in have]
    if absent:
        frame = frame.selectExpr(
            "*",
            *[
                f"CAST(NULL AS {field.dataType.simpleString()}) AS `{field.name}`"
                for field in absent
            ],
        )
    view = f"_backfill_rows_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(view)
    spark.sql(f"INSERT INTO {table} BY NAME SELECT * FROM {view}")


def _root_message(exc: BaseException) -> str:
    """The cause of a failure in a line: a streaming query wraps it in pages of its own text."""

    found = re.findall(r"(\w+(?:Exception|Error)): (.+)", str(exc))
    if found:
        kind, text = found[-1]
        return f"{kind}: {text}"[:400]
    return (str(exc) or type(exc).__name__)[:400]


def _record_failure(
    spark: Any,
    config: PipelineConfig,
    result: Result,
    exc: BaseException,
    *,
    started: datetime,
    update_id: str,
    batch: str,
) -> None:
    """Record a load that stopped as FAILED, with the files it did finish. The job then fails."""

    result.error = _root_message(exc)
    try:
        result.files = _recorded_files(spark, config, update_id)
        _record(spark, config, result, started=started, update_id=update_id, batch=batch)
    except Exception:
        log.exception("could not record the failed backfill %s", update_id)


def _record(
    spark: Any,
    config: PipelineConfig,
    result: Result,
    *,
    started: datetime,
    update_id: str,
    batch: str | None = None,
) -> None:
    """One ledger row for the table and one per file, like a load of the pipeline."""

    ended = datetime.now(UTC)
    files = result.files
    read = sum(int(item["rows_read"] or 0) for item in files)
    # The files come back from ledger_detail, whose column is rows_inserted. rows_loaded is what
    # files_sql calls it while a micro-batch is being counted.
    written = sum(int(item.get("rows_inserted") or item.get("rows_loaded") or 0) for item in files)
    quarantined = sum(int(item["rows_quarantined"] or 0) for item in files)
    row: dict[str, Any] = {
        "layer": config.layer or LAYER,
        "job_name": config.job_name,
        "started_at": started,
        "update_id": update_id,
        "dataflow": config.pipeline_id,
        "load_kind": "backfill",
        "target_status": "COMPLETED",
        "ended_at": ended,
        "batch_id": batch,
        "job_run_id": None,
        # Not a pipeline update, but the table's pipeline is known, and v_loads names it from this.
        "update_pipeline_uuid": _known_pipeline(spark, config),
        "source_kind": config.source_kind or "",
        "run_as": config.run_as or "",
        "target_table": config.table_name("bronze"),
        "rows_read": read,
        "rows_inserted": written,
        "rows_updated": 0,
        "rows_deleted": result.rows_replaced,
        "rows_quarantined": quarantined,
        "rows_dropped": 0,
        "files_read": 0,
        "files_failed": 0,
        "failure_reason": None,
        "_append": True,
        "_quarantine_table": (config.table_name("quarantine") if config.has_quarantine else None),
    }
    apply_files(row, files)
    if row["files_failed"]:
        row["failure_reason"] = "backfill: " + str(row["failure_reason"])
    # The loaded files are already in ledger_detail, written as each micro-batch finished. What is
    # written here is the files that left no row of their own: asked for and not there, or read and
    # empty. The job still succeeds; the ledger says it failed.
    row["_files"] = _problem_files(config, result)
    if result.error:
        mark_failed(row, f"backfill stopped: {result.error}")
    if row["_files"]:
        row["files_read"] += len(row["_files"])
        row["files_failed"] += len(row["_files"])
        parts = [warning for warning in result.warnings if "blocked" not in warning]
        mark_failed(row, "backfill: " + "; ".join(parts))
    elif result.none_found:
        mark_failed(row, f"backfill: no file found for this request in {config.path}")
    write_rows(spark, config.catalog, [row])


def _problem_files(config: PipelineConfig, result: Result) -> list[dict[str, Any]]:
    """File rows for the files the load left no row for: not found, or read with no rows."""

    def failed(path: str, name: str, why: str) -> dict[str, Any]:
        return {
            "detail_kind": "file",
            "src_file_path": path,
            "src_file_name": name,
            "status": FILE_FAILED,
            "rows_read": 0,
            "rows_inserted": 0,
            "rows_quarantined": 0,
            "rows_dropped": 0,
            "failed_rules": "",
            "blocking_rules": "",
            "error_message": why,
            "load_kind": "backfill",
        }

    return [
        *(failed(f"{config.path}{name}", name, "file not found") for name in result.missing),
        # An empty file is a failure for a table that expects a file each run, which is the one
        # rule for every way a load is recorded. For any other table it is only a warning.
        *(
            failed(path, path.rsplit("/", 1)[-1], EMPTY_FILE)
            for path in (result.empty if config.expect_file else [])
        ),
    ]


def _full_refresh(
    spark: Any,
    config: PipelineConfig,
    result: Result,
    *,
    confirm: bool,
    client: Any = None,
    sleep: Callable[[float], None] = time.sleep,
    wait_seconds: int = FULL_REFRESH_WAIT_SECONDS,
) -> Result:
    """Refresh this table in its own pipeline, from scratch. Every file is read again.

    The job waits for the update to end, then moves the files it blocked into ``_failed``, as a
    regular run does. ``client`` is the Databricks client (injected in tests).
    """

    if not (confirm or result.dry_run):
        raise BackfillError(
            "full_refresh deletes the table's data and reads every file again, so it needs "
            "confirm; use dry_run to see what it would reset"
        )
    result.planned = [
        config.table_name("bronze"),
        *([config.table_name("quarantine")] if config.has_quarantine else []),
        f"{config.catalog}.{PIPELINE_SCHEMA}.{config.pipeline_id}_staged",
    ]
    if has_file_counts(config):
        # The per-file dataset is refreshed with the table, and so is the check that fails the
        # update when the table has a fail rule.
        result.planned.append(f"{config.catalog}.{PIPELINE_SCHEMA}.{files_name(config)}")
    if has_blocking(config):
        result.planned.append(f"{config.catalog}.{PIPELINE_SCHEMA}.{blocked_check_name(config)}")
    # Files a fail rule blocked sit in _failed, which the pipeline never reads, so a full refresh
    # leaves them there. Naming one in a files backfill is how it is loaded again.
    # Files a backfill loaded sit in _backfilled. They are good data, and the refresh rebuilds the
    # table from the folder, so they come back first and the pipeline reads them this time.
    result.restored = list_aside(config.path)
    if result.dry_run:
        return result
    pipeline_uuid = _pipeline_uuid(spark, config)
    if result.restored:
        restore_aside(config.path, result.restored)
    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
    update = client.pipelines.start_update(
        pipeline_id=pipeline_uuid, full_refresh_selection=result.planned
    )
    result.update_id = str(update.update_id)
    # The update is the pipeline's, not this job's, so wait for it: the files it blocked can only be
    # parked once it has finished and the ledger holds what it found.
    result.update_state = _wait_for_update(
        client, pipeline_uuid, result.update_id, sleep=sleep, timeout=wait_seconds
    )
    if result.update_state not in UPDATE_DONE:
        result.warnings.append(
            f"update {result.update_id} was still {result.update_state} after {wait_seconds}s: "
            "blocked files are moved to _failed by the table's next job run"
        )
        return result
    sleep(SETTLE_SECONDS)  # the pipeline writes the ledger as it ends
    moved = park_failed_files(spark, config.catalog, [config.pipeline_id])
    result.parked = moved.get(config.pipeline_id, [])
    return result


def _wait_for_update(
    client: Any,
    pipeline_uuid: str,
    update_id: str,
    *,
    sleep: Callable[[float], None],
    timeout: int,
) -> str:
    """Poll a pipeline update until it ends or ``timeout`` seconds pass. Returns its last state."""

    waited = 0
    while True:
        found = client.pipelines.get_update(pipeline_id=pipeline_uuid, update_id=update_id)
        state = getattr(found.update.state, "value", found.update.state)
        name = str(state or "").upper()
        if name in UPDATE_DONE or waited >= timeout:
            return name
        sleep(POLL_SECONDS)
        waited += POLL_SECONDS


def _known_pipeline(spark: Any, config: PipelineConfig) -> str | None:
    """The id of the table's pipeline, or None when it has never loaded (a backfill still records)."""

    try:
        return _pipeline_uuid(spark, config)
    except Exception:
        log.warning(
            "the pipeline of %s is not known yet, so its backfill row has none", config.pipeline_id
        )
        return None


def _pipeline_uuid(spark: Any, config: PipelineConfig) -> str:
    """The Databricks pipeline that runs this table, from the table's latest ledger row."""

    meta = f"{require_catalog(config.catalog)}.framework_meta"
    rows = spark.sql(
        f"SELECT update_pipeline_uuid FROM {meta}.load_ledger "
        f"WHERE dataflow = {sql_string(config.pipeline_id)} AND update_pipeline_uuid IS NOT NULL "
        "ORDER BY recorded_at DESC LIMIT 1"
    ).collect()
    if not rows:
        raise BackfillError(
            f"{config.pipeline_id} has never loaded, so its pipeline is not known; "
            "run its job once first"
        )
    return str(rows[0][0])


def warning_text(result: Result) -> str:
    """One line for the job's output."""

    if result.dry_run:
        shown = result.planned[:MAX_LISTED]
        more = len(result.planned) - len(shown)
        noun = (
            "table(s) would be reset"
            if result.mode == "full_refresh"
            else "file(s) to read again"
            if result.mode == "rescue"
            else "file(s) selected"
        )
        text = (
            f"DRY RUN {result.table}: {len(result.planned)} {noun}"
            + (":\n  " + "\n  ".join(shown) if shown else "")
            + (f"\n  ... and {more} more" if more > 0 else "")
        )
        if result.restored:
            text += (
                f"\n{len(result.restored)} file(s) would be moved back out of "
                f"{FAILED_FOLDER} or {BACKFILLED_FOLDER}"
            )
        if result.on_fail:
            text += (
                f"\nWARNING: on_fail would be {result.on_fail} for this run, not the table's own"
            )
        if result.missing:
            text += f"\nWARNING: not found, would be recorded FAILED: {', '.join(result.missing)}"
        if result.none_found:
            text += "\nWARNING: no file found for this request, would be recorded FAILED"
        return text + "".join(f"\nWARNING: {warning}" for warning in result.warnings)
    if result.mode == "full_refresh":
        text = (
            f"{result.table}: full refresh update {result.update_id} {result.update_state.lower()}"
            if result.update_state
            else f"{result.table}: full refresh started, update {result.update_id}"
        )
        if result.restored:
            text += (
                f"; {len(result.restored)} file(s) moved back out of {BACKFILLED_FOLDER} to be read"
            )
        if result.parked:
            text += f"; {len(result.parked)} blocked file(s) moved to _failed"
        return text + "".join(f"\nWARNING: {warning}" for warning in result.warnings)
    text = (
        f"{result.table}: {len(result.files)} file(s) reloaded, "
        f"{result.rows_replaced} earlier row(s) replaced"
    )
    if result.restored:
        text += f"; {len(result.restored)} file(s) moved back to be read"
    if result.parked:
        text += f"; {len(result.parked)} blocked file(s) moved to _failed"
    if result.on_fail:
        text += f"\nWARNING: on_fail was {result.on_fail} for this run, not the table's own"
    return text + "".join(f"\nWARNING: {warning}" for warning in result.warnings)
