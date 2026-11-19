"""Load recon recorded by the pipeline itself, once per pipeline update.

A Lakeflow event hook sees every event of the update as it happens. ``UpdateRecorder`` folds
those events into counters in memory, one pass, and on the update's terminal event
(COMPLETED, FAILED, or CANCELED) it produces one ``load_ledger`` row per dataflow. The rows are
written in a single statement, so nothing re-reads the event log and nothing is computed twice.

Counts, per dataflow:

- ``rows_read``: rows the staged flow took from the source files (or source table);
- ``rows_written`` / ``rows_upserted`` / ``rows_deleted``: what the bronze flow changed;
- ``rows_quarantine``: rows the quarantine flow wrote.

Every count is a number. A flow that reported nothing (an update with no new files) is 0, and
so is a flow that did not complete in a failed or canceled update. ``failure_reason`` carries the
error text the update reported, or the error that stopped a spec from registering.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from lakeflow_framework.load_metrics import LEDGER, LEDGER_COLUMNS, METRIC_NAMES, METRIC_TABLE
from lakeflow_framework.sqlnames import require_catalog, sql_string

log = logging.getLogger(__name__)

TERMINAL_UPDATE = frozenset({"COMPLETED", "FAILED", "CANCELED"})
TERMINAL_FLOW = frozenset({"COMPLETED", "FAILED", "STOPPED", "SKIPPED", "EXCLUDED", "CANCELED"})
FLOW_DONE = frozenset({"COMPLETED", "IDLE"})
MAX_ERROR_CHARS = 4000
MAX_PART_CHARS = 1500

_COLUMN_TYPES = {
    "started_at": "TIMESTAMP",
    "ended_at": "TIMESTAMP",
    "duration_seconds": "DOUBLE",
    "rows_read": "BIGINT",
    "rows_written": "BIGINT",
    "rows_upserted": "BIGINT",
    "rows_deleted": "BIGINT",
    "rows_quarantine": "BIGINT",
    "rows_processed": "BIGINT",
    "recorded_at": "TIMESTAMP",
}
_ROW_COLUMNS = tuple(column for column in LEDGER_COLUMNS if column != "recorded_at")


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
        self, job_name: str, layer: str, dataflows: list[Dataflow], batch_id: str | None = None
    ):
        self.job_name = job_name
        self.layer = layer
        self.dataflows = dataflows
        # The id every bronze row of this run carries (_batch_id). The ledger keeps it beside the
        # real update id, because pipeline code cannot read the update id to stamp rows with it.
        self.batch_id = batch_id
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
            for table in (flow.staged, flow.target, flow.quarantine)
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
        target_status = _status(target, state, flow.build_error)
        quarantine_status = _status(quarantine, state, flow.build_error)
        failed = (
            flow.build_error is not None
            or target_status in {"FAILED", "CANCELED"}
            or (state != "COMPLETED")
        )
        started = update.started or update.first_seen or ended
        row: dict[str, Any] = {
            "job_name": self.job_name,
            "layer": self.layer,
            "job_run_id": None,
            "pipeline_id": flow.pipeline_id,
            "update_pipeline_uuid": update.pipeline_uuid,
            "pipeline_name": update.pipeline_name,
            "update_id": update_id,
            "dataflow": flow.pipeline_id,
            "owner_kind": flow.owner_kind,
            "owner": flow.owner,
            "target_table": flow.target or None,
            "quarantine_table": flow.quarantine or None,
            "started_at": started,
            "ended_at": ended,
            "duration_seconds": round(max((ended - started).total_seconds(), 0.0), 3),
            "rows_read": read,
            "rows_written": written,
            "rows_upserted": upserted,
            "rows_deleted": deleted,
            "rows_quarantine": quarantined,
            "rows_processed": written + quarantined,
            "target_status": target_status,
            "quarantine_status": quarantine_status,
            "expectations": _expectations_json(staged) if counted["staged"] else None,
            "failure_reason": self._reason(update_id, update, state, flow) if failed else None,
            "batch_id": self.batch_id,
        }
        if healthy and flow.append and read != written + quarantined:
            # An append load puts every row it read into bronze or quarantine.
            log.warning(
                "%s update %s: read %d rows but wrote %d and quarantined %d",
                flow.pipeline_id,
                update_id,
                read,
                written,
                quarantined,
            )
        return row

    def _flow(self, update: _Update, table: str) -> _Flow | None:
        for name, flow in update.flows.items():
            if _same_table(name, table):
                return flow
        return None

    def _reason(self, update_id: str, update: _Update, state: str, flow: Dataflow) -> str:
        if flow.build_error:
            return _clip(flow.build_error)
        own = (
            _key(flow.staged),
            _key(flow.target),
            _key(flow.quarantine),
            flow.pipeline_id.lower(),
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


# The table's physical order: the columns of LEDGER_COLUMNS, then batch_id (added after them).
LEDGER_SCHEMA = (
    ", ".join(f"{column} {_COLUMN_TYPES.get(column, 'STRING')}" for column in LEDGER_COLUMNS)
    + ", batch_id STRING"
)
METRIC_SCHEMA = (
    "job_name STRING, layer STRING, update_id STRING, dataflow STRING, "
    "recorded_at TIMESTAMP, metric_name STRING, metric_value BIGINT"
)


def ledger_records(rows: list[dict[str, Any]], recorded_at: datetime) -> list[tuple]:
    """The ledger rows as tuples in ``LEDGER_SCHEMA`` order."""

    return [
        (*(row[column] for column in _ROW_COLUMNS), recorded_at, row["batch_id"]) for row in rows
    ]


def metric_records(rows: list[dict[str, Any]], recorded_at: datetime) -> list[tuple]:
    """The same counts in the long shape: one tuple per dataflow and metric."""

    return [
        (
            row["job_name"],
            row["layer"],
            row["update_id"],
            row["dataflow"],
            recorded_at,
            name,
            int(row[name]),
        )
        for row in rows
        for name in METRIC_NAMES
    ]


def recorded_dataflows(spark: Any, catalog: str, row: dict[str, Any]) -> set[str]:
    """The dataflows already in the ledger for this update, so a repeat writes nothing again."""

    meta = f"{require_catalog(catalog)}.framework_meta"
    found = spark.sql(
        f"SELECT dataflow FROM {meta}.{LEDGER} WHERE job_name = {sql_string(row['job_name'])} "
        f"AND layer = {sql_string(row['layer'])} AND update_id = {sql_string(row['update_id'])}"
    ).collect()
    return {str(item["dataflow"]) for item in found}


def write_rows(spark: Any, catalog: str, rows: list[dict[str, Any]]) -> int:
    """Append an update's rows to load_ledger and load_metric. Returns how many ledger rows.

    A pipeline's ``spark.sql`` accepts only SELECT, SHOW, DESCRIBE, and USE, so the rows are
    written with the DataFrame writer. Rows already recorded for the update are left out.
    """

    if not rows:
        return 0
    meta = f"{require_catalog(catalog)}.framework_meta"
    done = recorded_dataflows(spark, catalog, rows[0])
    fresh = [row for row in rows if row["dataflow"] not in done]
    if not fresh:
        return 0
    at = _now()
    spark.createDataFrame(ledger_records(fresh, at), LEDGER_SCHEMA).write.mode(
        "append"
    ).saveAsTable(f"{meta}.{LEDGER}")
    spark.createDataFrame(metric_records(fresh, at), METRIC_SCHEMA).write.mode(
        "append"
    ).saveAsTable(f"{meta}.{METRIC_TABLE}")
    return len(fresh)


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
