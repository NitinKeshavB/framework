"""Load recon recorded by the pipeline itself, once per pipeline update.

A Lakeflow event hook sees every event of the update as it happens. ``UpdateRecorder`` folds
those events into counters in memory, one pass, and on the update's terminal event
(COMPLETED, FAILED, or CANCELED) it produces one ``load_ledger`` row per dataflow. The rows are
written in a single statement, so nothing re-reads the event log and nothing is computed twice.

Counts, per dataflow:

- ``rows_read``: rows the staged flow took from the source files (or source table);
- ``rows_inserted`` / ``rows_updated`` / ``rows_deleted``: what the target flow changed
  (append → inserted; CDC upsert metric → updated until the platform splits inserts);
- ``rows_quarantined``: rows the quarantine flow wrote;
- ``rows_dropped``: rows rejected by drop or fail-blocked rules (single bucket).

Every count is a number. A flow that reported nothing (an update with no new files) is 0, and
so is a flow that did not complete in a failed or canceled update. ``failure_reason`` carries the
error text the update reported, or the error that stopped a spec from registering.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from lakeflow_framework.config import BATCH_ID, INGEST_TIMESTAMP, SRC_FILE_NAME, SRC_FILE_PATH
from lakeflow_framework.load_metrics import LEDGER, LEDGER_COLUMNS
from lakeflow_framework.sqlnames import require_catalog, sql_string
from lakeflow_framework.volume_listing import list_volume_files

log = logging.getLogger(__name__)

TERMINAL_UPDATE = frozenset({"COMPLETED", "FAILED", "CANCELED"})
TERMINAL_FLOW = frozenset({"COMPLETED", "FAILED", "STOPPED", "SKIPPED", "EXCLUDED", "CANCELED"})
FLOW_DONE = frozenset({"COMPLETED", "IDLE"})
MAX_ERROR_CHARS = 4000
MAX_PART_CHARS = 1500

_COLUMN_TYPES = {
    "started_at": "TIMESTAMP",
    "ended_at": "TIMESTAMP",
    "load_date": "DATE",
    "rows_read": "BIGINT",
    "rows_inserted": "BIGINT",
    "rows_updated": "BIGINT",
    "rows_deleted": "BIGINT",
    "rows_quarantined": "BIGINT",
    "rows_dropped": "BIGINT",
    "files_read": "BIGINT",
    "files_failed": "BIGINT",
    "recorded_at": "TIMESTAMP",
}
_ROW_COLUMNS = tuple(
    column for column in LEDGER_COLUMNS if column not in {"recorded_at", "load_date"}
)


@dataclass(frozen=True)
class Dataflow:
    """One table of the delivery, as the recorder needs to find its flows in the event stream."""

    pipeline_id: str
    owner_kind: str
    owner: str
    staged: str  # the private staging table, bare name
    target: str  # catalog.schema.table
    quarantine: str  # catalog.schema.table
    append: bool = True  # False for AUTO CDC, where duplicate keys collapse so counts differ
    build_error: str | None = None  # why the spec could not be registered, when it could not
    file_load: bool = False  # a file source: per-file rows are counted from bronze and quarantine
    expect_file: bool = False  # a file must arrive every run; checked against the volume listing
    fail_rules: tuple[str, ...] = ()  # rules whose broken rows block their file and fail the load
    blocked_check: str = ""  # the dataset whose failure fails the update when a file was blocked
    files: str = ""  # catalog.schema.table of the per-file counts, for a table with a fail rule
    path: str = ""  # where the files land, for the message when none came
    path_glob: str = ""  # which files in ``path`` count
    source_kind: str = ""  # sftp_files / sharepoint_files / database / …
    run_as: str = ""  # configured service principal, empty when default


@dataclass
class _Flow:
    status: str | None = None
    metrics: dict[str, int] = field(default_factory=dict)
    expectations: dict[str, dict[str, int]] = field(default_factory=dict)


@dataclass
class _Update:
    pipeline_uuid: str | None = None
    pipeline_name: str | None = None
    started: datetime | None = None  # the first flow event
    first_seen: datetime | None = None  # the first event of any kind
    flows: dict[str, _Flow] = field(default_factory=dict)
    errors: list[tuple[str | None, str]] = field(default_factory=list)


class UpdateRecorder:
    """Fold a pipeline update's events into ledger rows. Pure Python: no Spark, no I/O."""

    def __init__(
        self,
        job_name: str,
        layer: str,
        dataflows: list[Dataflow],
        batch_id: str | None = None,
        *,
        load_kind: str = "regular",
    ):
        self.job_name = job_name
        self.layer = layer
        self.dataflows = dataflows
        # The id every bronze row of this run carries (_batch_id). The ledger keeps it beside the
        # real update id, because pipeline code cannot read the update id to stamp rows with it.
        self.batch_id = batch_id
        self.load_kind = load_kind
        self._updates: dict[str, _Update] = {}
        self._done: set[str] = set()

    def handle(self, event: dict[str, Any]) -> list[dict[str, Any]] | None:
        """Take one event. Returns the update's ledger rows once, on its terminal event."""

        origin = event.get("origin") or {}
        update_id = origin.get("update_id")
        if not update_id or update_id in self._done:
            return None
        update = self._updates.setdefault(update_id, _Update())
        update.pipeline_uuid = update.pipeline_uuid or origin.get("pipeline_id")
        update.pipeline_name = update.pipeline_name or origin.get("pipeline_name")
        moment = _moment(event.get("timestamp"))
        if moment and (update.first_seen is None or moment < update.first_seen):
            update.first_seen = moment
        # The load starts with its first flow, not with the update waiting for serverless compute.
        if (
            moment
            and event.get("event_type") == "flow_progress"
            and (update.started is None or moment < update.started)
        ):
            update.started = moment
        if str(event.get("level") or "").upper() == "ERROR":
            text = _error_text(event)
            if text:
                update.errors.append((_text(origin.get("flow_name")), text))
        kind = event.get("event_type")
        details = _mapping(event.get("details"))
        state = None
        if kind == "flow_progress":
            self._flow_progress(update, origin, _mapping(details.get("flow_progress")), event)
            # The update's own terminal event arrives as the pipeline shuts down, and a hook that
            # writes then can be cut off. Every flow finishing is the same fact, a moment earlier.
            state = self._all_flows_finished(update)
        elif kind == "update_progress":
            state = str(_mapping(details.get("update_progress")).get("state") or "").upper()
        if state in TERMINAL_UPDATE:
            self._done.add(update_id)
            del self._updates[update_id]
            return self._rows(update_id, update, str(state), moment or _now())
        return None

    def _expected_tables(self) -> list[str]:
        return [
            table
            for flow in self.dataflows
            if flow.build_error is None
            for table in (flow.staged, flow.target, flow.quarantine, flow.files, flow.blocked_check)
            if table
        ]

    def _all_flows_finished(self, update: _Update) -> str | None:
        """The update's outcome once every flow this delivery runs has a terminal status."""

        tables = self._expected_tables()
        if not tables:
            return None
        statuses = []
        for table in tables:
            flow = self._flow(update, table)
            if flow is None or flow.status not in TERMINAL_FLOW:
                return None
            statuses.append(flow.status)
        if "FAILED" in statuses:
            return "FAILED"
        if "CANCELED" in statuses or "STOPPED" in statuses:
            return "CANCELED"
        return "COMPLETED"

    def _flow_progress(
        self, update: _Update, origin: dict[str, Any], progress: dict[str, Any], event: dict
    ) -> None:
        name = _key(origin.get("flow_name"))
        if not name:
            return
        flow = update.flows.setdefault(name, _Flow())
        status = progress.get("status")
        if status:
            flow.status = str(status).upper()
        # Metrics are totals for the flow in this update, so the largest seen is the count.
        for metric, value in _mapping(progress.get("metrics")).items():
            number = _number(value)
            if number is not None:
                flow.metrics[metric] = max(flow.metrics.get(metric, 0), number)
        for check in _expectations(_mapping(progress.get("data_quality")).get("expectations")):
            flow.expectations[str(check.get("name"))] = {
                "passed_records": _number(check.get("passed_records")) or 0,
                "failed_records": _number(check.get("failed_records")) or 0,
            }

    def _rows(
        self, update_id: str, update: _Update, state: str, ended: datetime
    ) -> list[dict[str, Any]]:
        return [self._row(update_id, update, state, ended, flow) for flow in self.dataflows]

    def _row(
        self, update_id: str, update: _Update, state: str, ended: datetime, flow: Dataflow
    ) -> dict[str, Any]:
        healthy = state == "COMPLETED" and flow.build_error is None
        staged = self._flow(update, flow.staged)
        target = self._flow(update, flow.target)
        quarantine = self._flow(update, flow.quarantine)
        counted = {
            "staged": _usable(staged, healthy),
            "target": _usable(target, healthy),
            "quarantine": _usable(quarantine, healthy),
        }
        read = _count(staged, "num_output_rows") if counted["staged"] else 0
        upserted = _count(target, "num_upserted_rows") if counted["target"] else 0
        deleted = _count(target, "num_deleted_rows") if counted["target"] else 0
        written = 0
        if counted["target"] and target is not None:
            if "num_output_rows" in target.metrics:
                written = target.metrics["num_output_rows"]
            else:
                written = upserted + deleted
        quarantined = _count(quarantine, "num_output_rows") if counted["quarantine"] else 0
        # Append → rows_inserted; CDC upsert metric → rows_updated (platform does not split inserts).
        if flow.append and "num_output_rows" in (target.metrics if target else {}):
            inserted, updated = written, 0
        elif upserted:
            inserted, updated = 0, upserted
        else:
            inserted, updated = written, 0
        target_status = _status(target, state, flow.build_error)
        quarantine_status = _status(quarantine, state, flow.build_error)
        failed = (
            flow.build_error is not None
            or target_status in {"FAILED", "CANCELED"}
            or quarantine_status in {"FAILED", "CANCELED"}
            or (state != "COMPLETED")
        )
        started = update.started or update.first_seen or ended
        reason = self._reason(update_id, update, state, flow) if failed else None
        if (
            failed
            and not reason
            and quarantine_status in {"FAILED", "CANCELED"}
            and target_status not in {"FAILED", "CANCELED"}
        ):
            reason = f"quarantine flow status {quarantine_status}"
        row: dict[str, Any] = {
            "layer": self.layer,
            "job_name": self.job_name,
            "started_at": started,
            "update_id": update_id,
            "dataflow": flow.pipeline_id,
            "load_kind": self.load_kind,
            "target_status": target_status,
            "ended_at": ended,
            "batch_id": self.batch_id,
            "job_run_id": None,
            "update_pipeline_uuid": update.pipeline_uuid,
            "source_kind": flow.source_kind or "",
            "run_as": flow.run_as or "",
            "target_table": flow.target or None,
            "rows_read": read,
            "rows_inserted": inserted,
            "rows_updated": updated,
            "rows_deleted": deleted,
            "rows_quarantined": quarantined,
            "rows_dropped": 0,
            "files_read": 0,
            "files_failed": 0,
            "failure_reason": reason,
            # Not ledger columns: what the per-file rows and the arrival check are built from.
            "_file_load": flow.file_load,
            "_files_table": flow.files,
            "_expect_file": flow.expect_file,
            "_fail_rules": flow.fail_rules,
            "_path": flow.path,
            "_path_glob": flow.path_glob,
            "_append": flow.append,
            "_quarantine_table": flow.quarantine or None,
        }
        flows_counted = (
            counted["staged"]
            and counted["target"]
            and (counted["quarantine"] or not flow.quarantine)
        )
        if flow.append and flows_counted:
            # What was read and kept nowhere is dropped: a row a drop rule removed, and the good
            # rows of a blocked file. Per-file rows cannot say. This holds when the update failed
            # because a file was blocked, as long as these flows finished.
            row["rows_dropped"] = max(read - inserted - quarantined, 0)
        if flow.fail_rules and counted["staged"] and staged is not None:
            # Rows that broke a ``fail`` rule, from the staged flow's own expectations. A table
            # with no quarantine keeps those rows nowhere, so the files cannot show them.
            hits = {
                name: staged.expectations[name]["failed_records"]
                for name in flow.fail_rules
                if staged.expectations.get(name, {}).get("failed_records")
            }
            if hits:
                row["_fail_hits"] = hits
        return row

    def _flow(self, update: _Update, table: str) -> _Flow | None:
        for name, flow in update.flows.items():
            if _same_table(name, table):
                return flow
        return None

    def _reason(self, update_id: str, update: _Update, state: str, flow: Dataflow) -> str:
        if flow.build_error:
            return _clip(flow.build_error)
        # Empty names left out: "" is "in" every key, so a table without quarantine would claim
        # every other table's error in the same delivery.
        own = tuple(
            name
            for name in (
                _key(flow.staged),
                _key(flow.target),
                _key(flow.quarantine),
                flow.pipeline_id.lower(),
            )
            if name
        )
        parts: list[str] = []
        for name, text in update.errors:
            key = _key(name)
            if key and not any(_same_table(key, table) or table in key for table in own):
                continue  # another table's error in the same delivery
            if text not in parts:
                parts.append(text)
        if not parts:
            parts.append(f"pipeline update {update_id} ended {state} and reported no error message")
        return _clip("; ".join(parts))


LEDGER_SCHEMA = ", ".join(
    f"{column} {_COLUMN_TYPES.get(column, 'STRING')}" for column in LEDGER_COLUMNS
)
# Retired dual-write extras; kept so older tests importing these names still collect.
EXTRA_LEDGER_COLUMNS: tuple[str, ...] = ()
PIPELINE_METRIC_NAMES: tuple[str, ...] = ()


def metric_records(rows: list[dict[str, Any]], recorded_at: datetime) -> list[tuple]:
    """Retired: load_metric dual-write is gone. Returns an empty list."""

    return []


LEDGER_DETAIL = "ledger_detail"
FILE_LEDGER = LEDGER_DETAIL  # alias for older call sites / tests mid-refactor
DETAIL_COLUMNS = (
    "update_id",
    "dataflow",
    "recorded_at",
    "detail_kind",
    "status",
    "load_date",
    "job_name",
    "layer",
    "load_kind",
    "target_table",
    "src_object",
    "src_file_path",
    "src_file_name",
    "rows_read",
    "rows_inserted",
    "rows_quarantined",
    "rows_dropped",
    "failed_rules",
    "blocking_rules",
    "error_message",
)
_DETAIL_NUMBERS = frozenset({"rows_read", "rows_inserted", "rows_quarantined", "rows_dropped"})


def _detail_type(column: str) -> str:
    if column in _DETAIL_NUMBERS:
        return "BIGINT"
    if column == "recorded_at":
        return "TIMESTAMP"
    if column == "load_date":
        return "DATE"
    return "STRING"


DETAIL_SCHEMA = ", ".join(f"{column} {_detail_type(column)}" for column in DETAIL_COLUMNS)
FILE_COLUMNS = DETAIL_COLUMNS  # alias
FILE_SCHEMA = DETAIL_SCHEMA
MAX_FILES_IN_REASON = 5


def _load_date(started_at: Any) -> Any:
    """Calendar day of the load start (UTC date), for filters and archive predicates."""

    if started_at is None:
        return None
    if hasattr(started_at, "date"):
        return started_at.date()
    return started_at


def ledger_records(rows: list[dict[str, Any]], recorded_at: datetime) -> list[tuple]:
    """The ledger rows as tuples in ``LEDGER_SCHEMA`` order. A count is never NULL."""

    out: list[tuple] = []
    for row in rows:
        values: list[Any] = []
        for column in LEDGER_COLUMNS:
            if column == "recorded_at":
                values.append(recorded_at)
            elif column == "load_date":
                values.append(_load_date(row.get("started_at") or recorded_at))
            elif column in {
                "rows_read",
                "rows_inserted",
                "rows_updated",
                "rows_deleted",
                "rows_quarantined",
                "rows_dropped",
                "files_read",
                "files_failed",
            }:
                values.append(int(row.get(column) or 0))
            else:
                values.append(row.get(column))
        out.append(tuple(values))
    return out


def file_records(rows: list[dict[str, Any]], recorded_at: datetime) -> list[tuple]:
    """One tuple per detail row (file or input) of each ledger row, in ``DETAIL_SCHEMA`` order."""

    return [
        (
            row["update_id"],
            row["dataflow"],
            recorded_at,
            item.get("detail_kind") or "file",
            item["status"],
            _load_date(row.get("started_at") or recorded_at),
            row["job_name"],
            row["layer"],
            item.get("load_kind") or row.get("load_kind") or "regular",
            row["target_table"],
            item.get("src_object"),
            item.get("src_file_path"),
            item.get("src_file_name"),
            int(item.get("rows_read") or 0),
            int(item.get("rows_inserted") or item.get("rows_loaded") or 0),
            int(item.get("rows_quarantined") or 0),
            int(item.get("rows_dropped") or 0) + int(item.get("rows_blocked") or 0),
            item.get("failed_rules") or "",
            item.get("blocking_rules") or "",
            item.get("error_message"),
        )
        for row in rows
        for item in row.get("_files") or []
    ]


def read_files_sql(row: dict[str, Any], rescued: bool = False) -> str | None:
    """One row per file of this run, counted from what the run wrote to bronze and quarantine.

    No working table: every row of the run carries ``_batch_id``, and bronze and quarantine are
    clustered on the load time, so the time bound lets Delta skip older files. A file's quarantine
    rows hold the rules they broke, which is how a ``fail`` rule marks its file. A row dropped by a
    drop rule is kept nowhere, so it is not in any file's count (the table's ``rows_dropped``
    has it). For a table that keeps history by key, bronze counts the rows the run left there.
    With ``rescued`` (bronze has a ``_rescued_data`` column) the rows that hold rescued values
    are counted too, so a file that loaded with a new column is flagged.
    """

    batch, started, target = row.get("batch_id"), row.get("started_at"), row.get("target_table")
    if not row.get("_file_load") or not batch or not target:
        return None
    scope = f"WHERE {BATCH_ID} = {sql_string(str(batch))}"
    if isinstance(started, datetime):
        since = (started.astimezone(UTC) - LOAD_TIME_SLACK).strftime("%Y-%m-%d %H:%M:%S")
        scope += f" AND {INGEST_TIMESTAMP} >= TIMESTAMP '{since}+00:00'"
    none = "CAST(array() AS ARRAY<STRING>)"
    flag = "CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END" if rescued else "0"
    parts = [
        f"SELECT {SRC_FILE_PATH}, {SRC_FILE_NAME}, 1 AS loaded, 0 AS quarantined, "
        f"{none} AS failed, {flag} AS warned FROM {target} {scope}"
    ]
    if row.get("_quarantine_table"):
        parts.append(
            f"SELECT {SRC_FILE_PATH}, {SRC_FILE_NAME}, 0, 1, failed_rules, 0 "
            f"FROM {row['_quarantine_table']} {scope}"
        )
    fail_rules = ", ".join(sql_string(name) for name in row.get("_fail_rules") or ())
    blockers = (
        f"array_join(array_sort(array_intersect(array_distinct(flatten(collect_list(failed))), "
        f"array({fail_rules}))), ', ')"
        if fail_rules
        else "''"
    )
    return (
        f"SELECT {SRC_FILE_PATH}, {SRC_FILE_NAME}, sum(loaded) AS rows_loaded, "
        f"sum(quarantined) AS rows_quarantined, sum(warned) AS rows_warned, "
        f"{blockers} AS blocking_rules, "
        "array_join(array_sort(array_distinct(flatten(collect_list(failed)))), ', ') "
        "AS failed_rules "
        f"FROM ({' UNION ALL '.join(parts)}) GROUP BY {SRC_FILE_PATH}, {SRC_FILE_NAME} "
        f"ORDER BY {SRC_FILE_PATH}"
    )


def read_files_table(spark: Any, row: dict[str, Any]) -> list[dict[str, Any]]:
    """The exact per-file counts of this run, from the table's own per-file dataset.

    A table that has a ``fail`` rule has one, because a blocked file's good rows are read and then
    held back: they are in neither bronze nor quarantine, so only this dataset knows how many rows
    the file held. A file blocked with 80,000 rows, 3 of them bad, reads 80,000: 3 quarantined and
    the rest dropped. Every other file of the run is counted here too.
    """

    table, batch = row.get("_files_table"), row.get("batch_id")
    found = spark.sql(
        f"SELECT * FROM {table} WHERE {BATCH_ID} = {sql_string(str(batch))} ORDER BY {SRC_FILE_PATH}"
    ).collect()
    files = []
    for item in found:
        data = item.asDict() if hasattr(item, "asDict") else dict(item)
        files.append(
            {
                "src_file_path": data.get(SRC_FILE_PATH),
                "src_file_name": data.get(SRC_FILE_NAME),
                "status": data.get("file_status"),
                "detail_kind": "file",
                "rows_read": int(data.get("rows_read") or 0),
                "rows_inserted": int(data.get("rows_loaded") or 0),
                "rows_quarantined": int(data.get("rows_quarantined") or 0),
                # a blocked file's good rows were read and kept nowhere: dropped
                "rows_dropped": int(data.get("rows_dropped") or 0)
                + int(data.get("rows_blocked") or 0),
                "failed_rules": data.get("failed_rules") or "",
                "blocking_rules": data.get("blocking_rules") or "",
                "rows_warned": int(data.get("rows_warned") or 0),
            }
        )
    return files


def _has_rescued_column(spark: Any, row: dict[str, Any]) -> bool:
    """Whether bronze has the column Auto Loader rescues into. Parquet and avro do not."""

    try:
        return "_rescued_data" in spark.table(str(row.get("target_table"))).columns
    except Exception:
        return False


def read_files(spark: Any, row: dict[str, Any]) -> list[dict[str, Any]]:
    """The per-file counts of this run, for ``ledger_detail``. Empty for a table that is not files."""

    if row.get("_files_table") and row.get("batch_id"):
        try:
            return read_files_table(spark, row)
        except Exception:
            # Count from bronze and quarantine instead: a blocked file then shows only its bad
            # rows, but the ledger row is still written.
            log.exception("load metrics: could not read the file counts of %s", row["dataflow"])
    sql = read_files_sql(row, _has_rescued_column(spark, row))
    if sql is None:
        return []
    files = []
    for item in spark.sql(sql).collect():
        data = item.asDict() if hasattr(item, "asDict") else dict(item)
        loaded = int(data.get("rows_loaded") or 0)
        quarantined = int(data.get("rows_quarantined") or 0)
        blocking = data.get("blocking_rules") or ""
        warned = int(data.get("rows_warned") or 0)
        files.append(
            {
                "src_file_path": data.get(SRC_FILE_PATH),
                "src_file_name": data.get(SRC_FILE_NAME),
                "status": "FAILED"
                if blocking
                else "WARNING"
                if quarantined or warned
                else "PROCESSED",
                "detail_kind": "file",
                "rows_read": loaded + quarantined,
                "rows_inserted": loaded,
                "rows_quarantined": quarantined,
                "rows_dropped": 0,
                "failed_rules": data.get("failed_rules") or "",
                "blocking_rules": blocking,
                "rows_warned": warned,
            }
        )
    return files


EMPTY_FILE = "file has no rows"
# A file that landed a moment before the previous run started still belongs to that run.
ARRIVAL_SLACK = timedelta(minutes=5)
# A clock that runs a little behind the cluster's must not hide rows of this run.
LOAD_TIME_SLACK = timedelta(minutes=5)


def read_arrivals(
    spark: Any, catalog: str, row: dict[str, Any], files: list[dict[str, Any]]
) -> list[dict[str, Any]] | None:
    """The files that arrived since the previous regular run, or None when no file is expected.

    The volume is listed for names, sizes and arrival times only. A file is new when it arrived
    after the previous regular run started (a backfill, which also writes to the ledger, does not
    move that). ``has_rows`` is true when this run counted rows from it (``files``);
    ``recorded_before`` when an earlier load already wrote it to ``ledger_detail``. Nothing here is
    a pipeline table: it is the listing, ``load_ledger`` and ``ledger_detail``.
    """

    if not row.get("_expect_file") or not row.get("_path"):
        return None
    meta = f"{require_catalog(catalog)}.framework_meta"
    pid = sql_string(row["dataflow"])
    # layer and job_name are the ledger's clustering keys (with started_at), so naming them lets
    # Delta skip files instead of reading every row ever recorded.
    # The run before this one. A replay of an older run must not count later runs, so only rows
    # started before this run are looked at (the hook's own run is not in the ledger yet).
    this_run = row.get("started_at")
    before = (
        f" AND started_at < TIMESTAMP '{this_run.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f}+00:00'"
        if isinstance(this_run, datetime)
        else ""
    )
    previous = spark.sql(
        f"SELECT unix_timestamp(max(started_at)) AS since FROM {meta}.{LEDGER} "
        f"WHERE layer = {sql_string(row['layer'])} AND job_name = {sql_string(row['job_name'])} "
        f"AND dataflow = {pid} AND update_id NOT LIKE 'backfill-%'{before}"
    ).collect()
    since = previous[0]["since"] if previous else None
    cutoff = float(since) - ARRIVAL_SLACK.total_seconds() if since is not None else None
    recorded = {
        str(item["src_file_path"])
        for item in spark.sql(
            f"SELECT DISTINCT src_file_path FROM {meta}.{LEDGER_DETAIL} "
            f"WHERE dataflow = {pid} AND src_file_path IS NOT NULL"
        ).collect()
    }
    read = {str(item["src_file_path"]) for item in files}
    # Nothing that arrived after this run ended belongs to it. In the hook that is every file,
    # since it runs as the update ends. A replay runs later, so a file that came after is left
    # for the run it belongs to.
    ended = row.get("ended_at")
    until = (
        ended.timestamp() + ARRIVAL_SLACK.total_seconds() if isinstance(ended, datetime) else None
    )
    return [
        {
            "src_file_path": item["file_path"],
            "src_file_name": item["file_path"].rsplit("/", 1)[-1],
            "has_rows": item["file_path"] in read,
            "recorded_before": item["file_path"] in recorded,
        }
        for item in list_volume_files(row["_path"], row.get("_path_glob") or None)
        if (cutoff is None or item["arrived_at"].timestamp() > cutoff)
        and (until is None or item["arrived_at"].timestamp() <= until)
    ]


def apply_arrivals(row: dict[str, Any], arrivals: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Mark the load FAILED when a file was expected and none came, or one came with no rows.

    The job itself still succeeds: this is a fact about the data, and it lands in the ledger.
    ``arrivals`` is None for a table that expects no file, which is left alone.
    """

    if arrivals is None:
        return row
    files = row.setdefault("_files", [])
    # A file an earlier load already recorded is not new, even when it falls in the slack window.
    new = [item for item in arrivals if item["has_rows"] or not item["recorded_before"]]
    if not new and not files:
        mark_failed(
            row, f"no file arrived in {row.get('_path') or 'the source'} since the previous run"
        )
        return row
    empty = [item for item in new if not item["has_rows"]]
    for item in empty:
        files.append(
            {
                "detail_kind": "file",
                "src_file_path": item["src_file_path"],
                "src_file_name": item["src_file_name"],
                "status": "FAILED",
                "rows_read": 0,
                "rows_inserted": 0,
                "rows_quarantined": 0,
                "rows_dropped": 0,
                "failed_rules": "",
                "blocking_rules": "",
                "error_message": EMPTY_FILE,
            }
        )
    if empty:
        row["files_read"] = int(row.get("files_read") or 0) + len(empty)
        row["files_failed"] = int(row.get("files_failed") or 0) + len(empty)
        names = ", ".join(str(item["src_file_name"]) for item in empty[:MAX_FILES_IN_REASON])
        more = len(empty) - MAX_FILES_IN_REASON
        mark_failed(
            row,
            f"{len(empty)} file(s) arrived with no rows: {names}"
            + (f" and {more} more" if more > 0 else ""),
        )
    return row


def mark_failed(row: dict[str, Any], reason: str) -> None:
    """Record the load as FAILED, keeping a reason already there (a blocked file, an error)."""

    row["target_status"] = "FAILED"
    existing = row.get("failure_reason")
    row["failure_reason"] = _clip(f"{existing}; {reason}" if existing else reason)


def warning_text(item: dict[str, Any]) -> str | None:
    """What a loaded file's WARNING row says: rows that loaded with a warning, such as a new
    column kept in ``_rescued_data``."""

    warned = int(item.get("rows_warned") or 0)
    if not warned or item["status"] == "FAILED":
        return None
    names = item.get("failed_rules") or ""
    return (
        f"{warned} row(s) loaded with a warning"
        + (f": {names}" if names else "")
        + " (values kept in _rescued_data)"
    )


def apply_files(row: dict[str, Any], files: list[dict[str, Any]]) -> dict[str, Any]:
    """Fold the per-file counts into the table's ledger row.

    A file with a row that broke a ``fail`` rule is blocked: none of its rows load, its bad rows
    are in quarantine, and the file is FAILED. That makes the table's load FAILED (and the update
    fails after the other files have loaded). The counts stay real, and the reason names the files
    and the rules. ``_fail_hits`` (from the event log) says the same for a table whose bad rows are
    kept nowhere, so no file shows them.
    """

    row["_files"] = files
    failed = [item for item in files if item["status"] == "FAILED"]
    for item in files:
        item["error_message"] = (
            f"blocked by fail rule(s): {item.get('blocking_rules')}"
            if item["status"] == "FAILED"
            else warning_text(item)
        )
    if files:
        row["files_read"] = len(files)
        row["files_failed"] = len(failed)
        dropped = sum(
            int(item.get("rows_dropped") or 0) + int(item.get("rows_blocked") or 0)
            for item in files
        )
        if dropped:  # a backfill counts them per file; the pipeline run has them for the table
            row["rows_dropped"] = dropped
    if failed:
        shown = ", ".join(
            f"{item['src_file_name']} ({item.get('blocking_rules')})"
            for item in failed[:MAX_FILES_IN_REASON]
        )
        more = len(failed) - MAX_FILES_IN_REASON
        row["target_status"] = "FAILED"
        row["failure_reason"] = _clip(
            f"{len(failed)} of {len(files)} file(s) blocked by fail rules, none of their rows "
            f"loaded to {row.get('target_table')}: {shown}"
            + (f" and {more} more" if more > 0 else "")
        )
    elif row.get("_fail_hits"):
        broken = ", ".join(f"{name} ({count})" for name, count in row["_fail_hits"].items())
        mark_failed(row, f"rows broke fail rule(s); their files did not load: {broken}")
    read = int(row.get("rows_read") or 0)
    accounted = sum(
        int(row.get(column) or 0)
        for column in ("rows_inserted", "rows_quarantined", "rows_dropped")
    )
    if row.get("_append") and read != accounted:
        log.warning(
            "%s update %s: read %d rows but %d are accounted for (inserted %d, quarantined %d, "
            "dropped %d)",
            row.get("dataflow"),
            row.get("update_id"),
            read,
            accounted,
            row.get("rows_inserted", 0),
            row.get("rows_quarantined", 0),
            row.get("rows_dropped", 0),
        )
    return row


def recorded_dataflows(spark: Any, catalog: str, row: dict[str, Any]) -> set[str]:
    """The dataflows already in the ledger for this update, so a repeat writes nothing again."""

    meta = f"{require_catalog(catalog)}.framework_meta"
    found = spark.sql(
        f"SELECT dataflow FROM {meta}.{LEDGER} WHERE job_name = {sql_string(row['job_name'])} "
        f"AND layer = {sql_string(row['layer'])} AND update_id = {sql_string(row['update_id'])}"
    ).collect()
    return {str(item["dataflow"]) for item in found}


def write_rows(
    spark: Any, catalog: str, rows: list[dict[str, Any]], *, replay: bool = False
) -> int:
    """Append an update's rows to load_ledger and ledger_detail. Returns how many ledger rows.

    A pipeline's ``spark.sql`` accepts only SELECT, SHOW, DESCRIBE, and USE, so the rows are
    written with the DataFrame writer. Rows already recorded for the update are left out.
    ``replay=True`` (hourly catch-up) stamps ``load_kind=catchup`` when not already backfill.
    """

    if not rows:
        return 0
    meta = f"{require_catalog(catalog)}.framework_meta"
    done = recorded_dataflows(spark, catalog, rows[0])
    fresh = [row for row in rows if row["dataflow"] not in done]
    if not fresh:
        return 0
    for row in fresh:
        if replay and row.get("load_kind") != "backfill":
            row["load_kind"] = "catchup"
        if "_files" not in row:
            try:
                apply_files(row, read_files(spark, row))
            except Exception:
                # A count that cannot be read is 0, not a lost ledger row. The table's own
                # flows still say what happened.
                log.exception("load metrics: could not read the file counts of %s", row["dataflow"])
                apply_files(row, [])
            if row.get("load_kind", "regular") == "regular":
                try:
                    if reloaded_files(spark, meta, row):
                        row["load_kind"] = "backfill"
                except Exception:
                    log.exception(
                        "load metrics: could not tell if %s reloaded files", row["dataflow"]
                    )
            try:
                apply_arrivals(row, read_arrivals(spark, catalog, row, row.get("_files") or []))
            except Exception:
                log.exception("load metrics: could not read the arrivals of %s", row["dataflow"])
    at = _now()
    files = file_records(fresh, at)
    if files and replay:
        files = _unrecorded_files(spark, meta, fresh, files)

    def append(records: list[tuple], schema: str, table: str) -> None:
        spark.createDataFrame(records, schema).write.mode("append").saveAsTable(f"{meta}.{table}")

    writes = [(ledger_records(fresh, at), LEDGER_SCHEMA, LEDGER)]
    if files:
        writes.append((files, DETAIL_SCHEMA, LEDGER_DETAIL))
    # One write per table, at the same time. When an update fails the pipeline shuts down within
    # seconds and cancels a write still running, so the writes must not queue behind each other.
    # A cut-off update has no ledger row and the hourly catch-up writes it from the event log.
    with ThreadPoolExecutor(max_workers=len(writes)) as pool:
        futures = [pool.submit(append, *write) for write in writes]
        for future in futures:
            future.result()
    return len(fresh)


def reloaded_files(spark: Any, meta: str, row: dict[str, Any]) -> set[str]:
    """The files of this run that an earlier load already recorded in ``ledger_detail``.

    A regular run never reads a file twice, because the checkpoint marks it read. So a run that
    does is a reload: a full refresh, which the backfill job starts as a normal pipeline update and
    the hook would otherwise record as ``regular``. Such a run is recorded as ``backfill``.
    """

    paths = sorted(
        {
            str(item["src_file_path"])
            for item in row.get("_files") or []
            if item.get("src_file_path")
        }
    )
    found: set[str] = set()
    for start in range(0, len(paths), 500):
        listed = ", ".join(sql_string(path) for path in paths[start : start + 500])
        rows = spark.sql(
            f"SELECT DISTINCT src_file_path FROM {meta}.{LEDGER_DETAIL} "
            f"WHERE dataflow = {sql_string(row['dataflow'])} "
            f"AND update_id <> {sql_string(row['update_id'])} AND src_file_path IN ({listed})"
        ).collect()
        found.update(str(item["src_file_path"]) for item in rows)
    return found


def _unrecorded_files(
    spark: Any, meta: str, rows: list[dict[str, Any]], files: list[tuple]
) -> list[tuple]:
    """The file rows an update has not already written, for a catch-up of one that was cut off."""

    update_id = rows[0]["update_id"]
    found = spark.sql(
        f"SELECT src_file_path FROM {meta}.{LEDGER_DETAIL} "
        f"WHERE update_id = {sql_string(update_id)} AND src_file_path IS NOT NULL"
    ).collect()
    done = {str(item["src_file_path"]) for item in found}
    path = DETAIL_COLUMNS.index("src_file_path")
    return [
        record for record in files if record[path] is not None and str(record[path]) not in done
    ]


def install(
    pipelines: Any,
    spark: Any,
    recorder: UpdateRecorder,
    catalog: str,
    *,
    write: Callable[[Any, str, list[dict[str, Any]]], int] = write_rows,
) -> bool:
    """Register the recorder as the pipeline's event hook. False when the runtime has no hooks.

    A failed write is logged and raised again, so the pipeline's event log shows it as a failed
    hook (``hook_progress``). The hook is registered with no limit on consecutive failures, so it
    never fails the update: losing a ledger row must not fail a load, and must not go unseen.
    """

    decorator = _hook_decorator(pipelines)
    if decorator is None:
        log.error(
            "this runtime has no on_event_hook; load metrics for %s will not be recorded",
            recorder.job_name,
        )
        return False

    def record_load_metrics(event: dict[str, Any]) -> None:
        try:
            rows = recorder.handle(event)
            if rows:
                written = write(spark, catalog, rows)
                log.info(
                    "load metrics: recorded %d dataflow row(s) of %s update %s",
                    written,
                    recorder.job_name,
                    rows[0]["update_id"],
                )
        except Exception:
            log.exception("load metrics: could not record %s", recorder.job_name)
            raise

    try:
        try:
            decorator(max_allowable_consecutive_failures=None)(record_load_metrics)
        except TypeError:
            decorator(record_load_metrics)
    except Exception:
        log.exception("could not register the load recorder for %s", recorder.job_name)
        return False
    return True


def _hook_decorator(pipelines: Any) -> Callable[..., Any] | None:
    hook = getattr(pipelines, "on_event_hook", None)
    if hook is not None:
        return hook
    try:
        import dlt  # type: ignore[import-not-found]
    except ImportError:
        return None
    return getattr(dlt, "on_event_hook", None)


def _usable(flow: _Flow | None, healthy: bool) -> bool:
    """Counts count when the update is healthy, or when that flow finished before it failed."""

    return flow is not None and (healthy or flow.status in FLOW_DONE)


def _count(flow: _Flow | None, metric: str) -> int:
    return int(flow.metrics.get(metric, 0)) if flow else 0


def _status(flow: _Flow | None, state: str, build_error: str | None) -> str:
    if build_error:
        return "FAILED"
    if flow is not None and flow.status in TERMINAL_FLOW:
        return flow.status
    if flow is not None and flow.status == "IDLE":
        return "COMPLETED"
    return state


def _expectations_json(flow: _Flow | None) -> str | None:
    if flow is None or not flow.expectations:
        return None
    checks = [{"name": name, **flow.expectations[name]} for name in sorted(flow.expectations)]
    return json.dumps(checks, separators=(",", ":"))


def _expectations(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return []
    return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []


def _error_text(event: dict[str, Any]) -> str:
    parts: list[str] = []
    message = _text(event.get("message"))
    if message:
        parts.append(message)
    error = _mapping(event.get("error"))
    for exception in error.get("exceptions") or []:
        item = _mapping(exception)
        detail = _text(item.get("message"))
        cls = _text(item.get("class_name"))
        text = f"{cls}: {detail}" if cls and detail else detail or cls
        if text and text not in parts and not any(text in part for part in parts):
            parts.append(text)
    return "; ".join(_clip(part, MAX_PART_CHARS) for part in parts)


def _clip(text: str, limit: int = MAX_ERROR_CHARS) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _same_table(name: str | None, table: str) -> bool:
    """A flow name matches a table by full name, or by bare name when the event gave one."""

    flow, wanted = _key(name), _key(table)
    if not flow or not wanted:
        return False
    return flow == wanted or ("." not in wanted and flow.endswith("." + wanted))


def _key(name: Any) -> str:
    return str(name or "").replace("`", "").lower()


def _text(value: Any) -> str | None:
    return str(value) if value not in (None, "") else None


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def _number(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _moment(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _now() -> datetime:
    return datetime.now(UTC)
