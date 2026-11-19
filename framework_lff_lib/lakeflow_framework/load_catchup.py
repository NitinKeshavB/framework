"""Write the ledger rows of a pipeline update whose own hook did not finish.

The pipeline writes its load rows from an event hook, a few seconds after the last table
finishes. When serverless shuts the pipeline down first, that write is lost and nothing is raised,
so the update has no ``load_ledger`` row. This replays such an update's events from the pipeline's
event log through the same ``UpdateRecorder`` and writes the rows, so the result is the one the
hook would have written. ``framework_runs`` calls it hourly. An update that already has rows is
left alone, and ``write_rows`` skips a dataflow already recorded, so running it twice is safe.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import replace
from datetime import datetime
from typing import Any

from lakeflow_framework.load_metrics import EVENT_LOG_SUFFIX, _registered_dataflows_sql
from lakeflow_framework.metadata import EVENTS_SCHEMA
from lakeflow_framework.pipeline_metrics import (
    Dataflow,
    UpdateRecorder,
    _same_table,
    write_rows,
)
from lakeflow_framework.sqlnames import IDENTIFIER, require_catalog
from lakeflow_framework.sqlnames import spark_rows as _rows

log = logging.getLogger(__name__)

LOOKBACK_DAYS = 7
LAYER = "bronze"
_UPDATE_ID = re.compile(r"^[0-9A-Za-z-]{8,64}$")
_EVENTS = (
    "to_json(named_struct('timestamp', cast(timestamp as string), 'event_type', event_type, "
    "'level', level, 'message', message, 'origin', origin, 'details', details, 'error', error))"
)


def catch_up_loads(spark: Any, catalog: str, *, days: int = LOOKBACK_DAYS) -> int:
    """Record every recent update that has no ledger row. Returns the ledger rows written."""

    meta = f"{require_catalog(catalog)}.framework_meta"
    events = f"{catalog}.{EVENTS_SCHEMA}"
    written = 0
    for name in _event_logs(spark, events):
        job = name[: -len(EVENT_LOG_SUFFIX)]
        if not IDENTIFIER.match(job):
            continue
        written += _catch_up_job(spark, catalog, meta, f"{events}.{name}", job, days)
    log.info("load catch-up: wrote %d ledger row(s)", written)
    return written


def _event_logs(spark: Any, events: str) -> list[str]:
    try:
        shown = _rows(spark.sql(f"SHOW TABLES IN {events}"))
    except Exception:
        return []
    names = (str(row.get("tableName") or row.get("table_name") or "") for row in shown)
    return sorted(name for name in names if name.endswith(EVENT_LOG_SUFFIX))


def _catch_up_job(spark: Any, catalog: str, meta: str, event_log: str, job: str, days: int) -> int:
    dataflows = _dataflows(spark, meta, job)
    if not dataflows:
        return 0
    missing = _rows(
        spark.sql(
            f"SELECT DISTINCT origin.update_id AS update_id FROM {event_log} e "
            "WHERE e.event_type = 'update_progress' AND e.origin.update_id IS NOT NULL "
            "AND e.details:update_progress.state IN ('COMPLETED', 'FAILED', 'CANCELED') "
            f"AND e.timestamp > current_timestamp() - INTERVAL {int(days)} DAYS "
            f"AND NOT EXISTS (SELECT 1 FROM {meta}.load_ledger l WHERE l.job_name = '{job}' "
            f"AND l.layer = '{LAYER}' AND l.update_id = e.origin.update_id)"
        )
    )
    written = 0
    for item in missing:
        update_id = str(item["update_id"])
        if not _UPDATE_ID.match(update_id):
            continue
        rows = _replay(spark, event_log, job, dataflows, update_id)
        if not rows:
            log.warning("load catch-up: %s update %s gave no rows to record", job, update_id)
            continue
        for row in rows:
            row["batch_id"] = _batch_of(spark, row)
        written += write_rows(spark, catalog, rows, replay=True)
        log.info("load catch-up: recorded %s update %s", job, update_id)
    return written


def _dataflows(spark: Any, meta: str, job: str) -> list[Dataflow]:
    found = _rows(spark.sql(_registered_dataflows_sql(meta, job, LAYER)))
    return [
        Dataflow(
            pipeline_id=str(row["pipeline_id"]),
            owner_kind=str(row["owner_kind"]),
            owner=str(row["owner"]),
            staged=f"{row['pipeline_id']}_staged",
            target=str(row.get("target_table") or ""),
            quarantine=str(row.get("quarantine_table") or ""),
            files=_files_view(row),
        )
        for row in found
    ]


def _files_view(row: dict[str, Any]) -> str:
    """Where the per-file counts live: in the pipeline's own schema, named after the table.
    A table that has no such view (a database source) makes the read fail, which counts as none."""

    target = str(row.get("target_table") or "")
    catalog = target.split(".", 1)[0] if "." in target else ""
    return f"{catalog}.framework_meta.{row['pipeline_id']}_files" if catalog else ""


def _replay(
    spark: Any, event_log: str, job: str, dataflows: list[Dataflow], update_id: str
) -> list[dict[str, Any]] | None:
    events = [
        json.loads(item["event"])
        for item in _rows(
            spark.sql(
                f"SELECT {_EVENTS} AS event FROM {event_log} WHERE origin.update_id = '{update_id}' "
                "AND event_type IN ('flow_definition', 'flow_progress', 'update_progress') "
                "ORDER BY timestamp"
            )
        )
    ]
    recorder = UpdateRecorder(job, LAYER, _with_checks(_in_this_update(dataflows, events), events))
    for event in events:
        rows = recorder.handle(event)
        if rows:
            return rows
    return None


def _in_this_update(dataflows: list[Dataflow], events: list[dict[str, Any]]) -> list[Dataflow]:
    """The tables this update defined. A table added later was not part of an older update.

    An update that failed before it defined anything names no flow, so every table is kept, as
    the pipeline's own hook does for a failure it cannot place.
    """

    names = [(event.get("origin") or {}).get("flow_name") for event in events]
    mine = [
        flow
        for flow in dataflows
        if any(
            _same_table(name, table)
            for name in names
            for table in (flow.staged, flow.target, flow.quarantine)
        )
    ]
    return mine or dataflows


def _with_checks(dataflows: list[Dataflow], events: list[dict[str, Any]]) -> list[Dataflow]:
    """Wait for the per-file view and the blocked-file check only when this update ran them.

    The pipeline's own hook knows from the spec. A replay knows from the events: a table with
    no ``fail`` rule has no check, and an update that failed before it ran has no flow for it.
    The check is what fails the update, so without it a failed update would be read as complete.
    """

    names = [(event.get("origin") or {}).get("flow_name") for event in events]
    result = []
    for flow in dataflows:
        stem = flow.files.rsplit(".", 1)[0] if flow.files else ""
        check = f"{stem}.{flow.pipeline_id}_blocked_check" if stem else ""
        arrivals = f"{stem}.{flow.pipeline_id}_arrivals" if stem else ""
        ran_files = bool(flow.files) and any(_same_table(name, flow.files) for name in names)
        ran_check = bool(check) and any(_same_table(name, check) for name in names)
        ran_arrivals = bool(arrivals) and any(_same_table(name, arrivals) for name in names)
        result.append(
            replace(
                flow,
                files=flow.files if ran_files else "",
                blocked_check=check if ran_check else "",
                arrivals=arrivals if ran_arrivals else "",
            )
        )
    return result


def _batch_of(spark: Any, row: dict[str, Any]) -> str | None:
    """The ``_batch_id`` this run stamped on its rows, read back from the table it wrote.

    The hook knew it in memory. A replay finds it from the rows loaded during the run, in bronze
    or, when everything was kept out of bronze, in quarantine. None when the run loaded nothing,
    or the tables cannot be read.
    """

    start, end = row.get("started_at"), row.get("ended_at")
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return None
    for table in (row.get("target_table"), row.get("quarantine_table")):
        if not table:
            continue
        try:
            found = _rows(
                spark.sql(
                    f"SELECT DISTINCT _batch_id AS batch FROM {table} "
                    f"WHERE _ingest_timestamp >= {_utc(start)} - INTERVAL 1 MINUTE "
                    f"AND _ingest_timestamp <= {_utc(end)} + INTERVAL 1 MINUTE LIMIT 2"
                )
            )
        except Exception:
            log.warning("load catch-up: could not read the batch of a run from %s", table)
            continue
        if len(found) == 1 and found[0]["batch"]:
            return str(found[0]["batch"])
    return None


def _utc(value: datetime) -> str:
    return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}+00:00'"
