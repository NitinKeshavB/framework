"""Reload chosen files of one table, or refresh the whole table, through the table's own rules.

One job serves every table: it takes the table's id, loads that table's spec from the registry,
and runs the same staging step as the pipeline (``pipeline.stage_frame``), so the same read options,
rules, rule actions, audit columns and file blocking apply. Nothing of the pipeline is copied.

Modes:

- ``files``: the named files.
- ``dates``: files that arrived (were last modified) between ``date_from`` and ``date_to``.
- ``failed``: files whose latest ``ledger_detail`` status is FAILED, to try them again.
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
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from lakeflow_framework.config import SRC_FILE_NAME, SRC_FILE_PATH, PipelineConfig
from lakeflow_framework.load import get_strategy
from lakeflow_framework.pipeline import (
    FILE_FAILED,
    FILE_STATUS,
    PIPELINE_SCHEMA,
    file_columns,
    files_name,
    files_sql,
    is_file_load,
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

MODES = ("files", "dates", "failed", "full_refresh")
STATE_VOLUME = "backfill_state"
LAYER = "bronze"
_FILE_NAME = re.compile(r"^[^/\\{}*?\[\]]+$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$")
MAX_LISTED = 50


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


def parse_selection(
    mode: str, files: str = "", date_from: str = "", date_to: str = ""
) -> Selection:
    """Check what was asked for. ``files`` is a comma-separated list of file names."""

    mode = (mode or "").strip().lower()
    if mode not in MODES:
        raise BackfillError(f"mode must be one of {', '.join(MODES)}, not {mode!r}")
    names = tuple(name.strip() for name in (files or "").split(",") if name.strip())
    start, end = (date_from or "").strip() or None, (date_to or "").strip() or None
    if mode == "files":
        if start or end:
            raise BackfillError("mode files takes file names, not dates; use mode dates")
        for name in names:
            if not _FILE_NAME.match(name):
                raise BackfillError(
                    f"{name!r} is not a file name: give names only, not paths or patterns"
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
    if selection.mode == "files" and selection.files:
        options["pathGlobFilter"] = "{" + ",".join(selection.files) + "}"
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


def load_config(spark: Any, catalog: str, pipeline_id: str) -> PipelineConfig:
    """The table's spec from the registry, with this environment's catalog."""

    from lakeflow_framework.config import config_from_document
    from lakeflow_framework.pipeline import UnityMetadata, _with_catalog

    if not IDENTIFIER.match(pipeline_id or ""):
        raise BackfillError(f"{pipeline_id!r} is not a table id")
    document = UnityMetadata(spark, catalog).get_pipeline(pipeline_id)
    if not document:
        raise BackfillError(f"{pipeline_id} is not in the registry; deploy it first")
    return config_from_document({**_with_catalog(document, catalog), "catalog": catalog})


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
    wanted = set(selection.files) if selection.mode == "files" and selection.files else None
    return [
        {"path": item["file_path"], "arrived": item["arrived_at"], "bytes": item["file_bytes"]}
        for item in list_volume_files(config.path, config.path_glob)
        if (wanted is None or item["file_path"].rsplit("/", 1)[-1] in wanted)
        and (after is None or item["arrived_at"] > after)
        and (before is None or item["arrived_at"] < before)
    ]


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


def run_backfill(
    spark: Any,
    catalog: str,
    pipeline_id: str,
    selection: Selection,
    *,
    dry_run: bool = False,
    confirm: bool = False,
    job_run_id: str | None = None,
) -> Result:
    """Reload the selected files of one table. Returns what was done (or would be)."""

    config = load_config(spark, catalog, pipeline_id)
    check_supported(config, selection)
    result = Result(pipeline_id, selection.mode, dry_run=dry_run)
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
    if dry_run:
        return result
    if not planned and not (result.missing or result.none_found):
        return result
    update_id = f"backfill-{job_run_id or uuid.uuid4()}"
    result.update_id = update_id
    started = datetime.now(UTC)
    batch = str(uuid.uuid4())
    if planned:
        # What is replaced is counted before anything is deleted: the load runs in worker
        # processes, which cannot hand numbers back through memory.
        result.rows_replaced = _count_existing(spark, result.planned, config.table_name("bronze"))
        _load(spark, config, selection, result, batch=batch, update_id=update_id)
        # Each micro-batch wrote its files to ledger_detail as it went.
        result.files = _recorded_files(spark, config, update_id)
        recorded = {item["src_file_path"] for item in result.files}
        result.empty = [path for path in result.planned if path not in recorded]
    blocked = blocked_files(result)
    if blocked:
        result.warnings.append(
            f"{len(blocked)} file(s) blocked by fail rules: "
            + ", ".join(item["src_file_name"] for item in blocked[:MAX_LISTED])
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
    """The file names that were asked for and are not in the directory."""

    if selection.mode != "files" or not selection.files:
        return []
    found = {path.rsplit("/", 1)[-1] for path in planned}
    return [name for name in selection.files if name not in found]


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
    if selection.mode == "failed":
        names = sorted({path.rsplit("/", 1)[-1] for path in result.planned})
        options["pathGlobFilter"] = "{" + ",".join(names) + "}"
    reader = spark.readStream.format("cloudFiles")
    for key, value in options.items():
        reader = reader.option(key, value)
    staged = stage_frame(
        reader.load(config.path).selectExpr("*", *file_columns(datetime.now(UTC))), config, batch
    )
    bronze, quarantine = config.table_name("bronze"), config.table_name("quarantine")
    stub = {
        "job_name": config.job_name,
        "layer": config.layer or LAYER,
        "update_id": update_id,
        "batch_id": batch,
        "dataflow": config.pipeline_id,
        "target_table": bronze,
        "load_kind": "backfill",
    }
    ledger = f"{require_catalog(config.catalog)}.framework_meta.{FILE_LEDGER}"

    # This runs in a worker process: it may use only what it is given, and it hands nothing back
    # but through the tables it writes.
    def process(frame: Any, _batch_number: int) -> None:
        # A micro-batch has its own session, and a temp view lives in the session that made it.
        session = frame.sparkSession
        view = f"_backfill_{uuid.uuid4().hex}"
        frame.createOrReplaceTempView(view)
        counted = session.sql(files_sql(view, blocking, this_run=False)).collect()
        files = [_as_file(row) for row in counted]
        paths = [item["src_file_path"] for item in files]
        _remove(session, paths, bronze, quarantine)
        blocked = [item["src_file_path"] for item in files if item["status"] == FILE_FAILED]
        marked = apply_files(dict(stub), files)["_files"]
        session.createDataFrame(
            file_records([{**stub, "_files": marked}], _now()), FILE_SCHEMA
        ).write.mode("append").saveAsTable(ledger)
        good = frame.filter(f"{DQ_RESULT} = 'pass'")
        if blocked:
            keep = session.createDataFrame([(path,) for path in blocked], f"{SRC_FILE_PATH} STRING")
            good = good.join(keep, SRC_FILE_PATH, "left_anti")
        internal = f"{DQ_COLUMN}, {DQ_RESULT}"
        _insert(session, good.selectExpr(f"* EXCEPT ({internal})"), bronze)
        bad = frame.filter(f"{DQ_RESULT} IN ('fail', 'quarantine')").selectExpr(
            f"* EXCEPT ({internal})", f"{DQ_COLUMN} AS failed_rules"
        )
        _insert(session, bad, quarantine)

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


def _remove(spark: Any, paths: list[str], *tables: str) -> int:
    """Delete these files' earlier rows. Returns how many bronze rows were replaced."""

    replaced = 0
    for start in range(0, len(paths), 500):
        listed = ", ".join(sql_string(path) for path in paths[start : start + 500])
        for number, table in enumerate(tables):
            done = spark.sql(f"DELETE FROM {table} WHERE {SRC_FILE_PATH} IN ({listed})").collect()
            if number == 0 and done:
                replaced += int(done[0][0] or 0)
    return replaced


def _insert(spark: Any, frame: Any, table: str) -> None:
    view = f"_backfill_rows_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(view)
    spark.sql(f"INSERT INTO {table} BY NAME SELECT * FROM {view}")


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
    written = sum(int(item["rows_loaded"] or 0) for item in files)
    quarantined = sum(int(item["rows_quarantined"] or 0) for item in files)
    row: dict[str, Any] = {
        "job_name": config.job_name,
        "layer": config.layer or LAYER,
        "job_run_id": None,
        "pipeline_id": config.pipeline_id,
        "update_pipeline_uuid": None,
        "pipeline_name": "framework_backfill",
        "update_id": update_id,
        "dataflow": config.pipeline_id,
        "owner_kind": config.owner_kind,
        "owner": config.owner,
        "source_kind": config.source_kind or "",
        "run_as": config.run_as or "",
        "load_kind": "backfill",
        "target_table": config.table_name("bronze"),
        "quarantine_table": config.table_name("quarantine"),
        "started_at": started,
        "ended_at": ended,
        "duration_seconds": round(max((ended - started).total_seconds(), 0.0), 3),
        "rows_read": read,
        "rows_inserted": written,
        "rows_updated": 0,
        "rows_deleted": result.rows_replaced,
        "rows_quarantined": quarantined,
        "rows_dropped": 0,
        "target_status": "COMPLETED",
        "quarantine_status": "COMPLETED",
        "expectations": "[]",
        "failure_reason": None,
        "batch_id": batch,
        "files_read": 0,
        "files_failed": 0,
        "_append": True,
    }
    apply_files(row, files)
    if row["files_failed"]:
        row["failure_reason"] = "backfill: " + str(row["failure_reason"])
    # The loaded files are already in ledger_detail, written as each micro-batch finished. What is
    # written here is the files that left no row of their own: asked for and not there, or read and
    # empty. The job still succeeds; the ledger says it failed.
    row["_files"] = _problem_files(config, result)
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
        *(failed(path, path.rsplit("/", 1)[-1], EMPTY_FILE) for path in result.empty),
    ]


def _full_refresh(spark: Any, config: PipelineConfig, result: Result, *, confirm: bool) -> Result:
    """Refresh this table in its own pipeline, from scratch. Every file is read again."""

    if not (confirm or result.dry_run):
        raise BackfillError(
            "full_refresh deletes the table's data and reads every file again, so it needs "
            "confirm; use dry_run to see what it would reset"
        )
    result.planned = [
        config.table_name("bronze"),
        config.table_name("quarantine"),
        f"{config.catalog}.{PIPELINE_SCHEMA}.{config.pipeline_id}_staged",
        f"{config.catalog}.{PIPELINE_SCHEMA}.{files_name(config)}",
    ]
    if blocking_rules(compile_rules(config)):
        # The check is what fails the update when a file is blocked, so it is refreshed too.
        result.planned.append(
            f"{config.catalog}.{PIPELINE_SCHEMA}.{config.pipeline_id}_blocked_check"
        )
    if result.dry_run:
        return result
    pipeline_uuid = _pipeline_uuid(spark, config)
    from databricks.sdk import WorkspaceClient

    update = WorkspaceClient().pipelines.start_update(
        pipeline_id=pipeline_uuid, full_refresh_selection=result.planned
    )
    result.update_id = str(update.update_id)
    return result


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
        noun = "table(s) would be reset" if result.mode == "full_refresh" else "file(s) selected"
        text = (
            f"DRY RUN {result.table}: {len(result.planned)} {noun}"
            + (":\n  " + "\n  ".join(shown) if shown else "")
            + (f"\n  ... and {more} more" if more > 0 else "")
        )
        if result.missing:
            text += f"\nWARNING: not found, would be recorded FAILED: {', '.join(result.missing)}"
        if result.none_found:
            text += "\nWARNING: no file found for this request, would be recorded FAILED"
        return text
    if result.mode == "full_refresh":
        return f"{result.table}: full refresh started, update {result.update_id}"
    text = (
        f"{result.table}: {len(result.files)} file(s) reloaded, "
        f"{result.rows_replaced} earlier row(s) replaced"
    )
    return text + "".join(f"\nWARNING: {warning}" for warning in result.warnings)
