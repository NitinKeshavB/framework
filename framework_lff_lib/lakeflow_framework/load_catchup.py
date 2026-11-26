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


def catch_up_loads(
    spark: Any,
    catalog: str,
    *,
    days: int = LOOKBACK_DAYS,
    pipeline_ids: list[str] | None = None,
    keep_kind: bool = False,
) -> int:
    """Record every recent update that has no ledger row. Returns the ledger rows written.

    With ``pipeline_ids``, only the jobs that run one of those tables are looked at, so a job can
    record its own update as soon as it ends; ``keep_kind`` keeps such a row ``regular``.
    """

    meta = f"{require_catalog(catalog)}.framework_meta"
    events = f"{catalog}.{EVENTS_SCHEMA}"
    written = 0
    for name in _event_logs(spark, events):
        job = name[: -len(EVENT_LOG_SUFFIX)]
        if not IDENTIFIER.match(job):
            continue
        written += _catch_up_job(
            spark, catalog, meta, f"{events}.{name}", job, days, pipeline_ids, keep_kind
        )
    log.info("load catch-up: wrote %d ledger row(s)", written)
    return written


def _event_logs(spark: Any, events: str) -> list[str]:
    try:
        shown = _rows(spark.sql(f"SHOW TABLES IN {events}"))
    except Exception:
        return []
    names = (str(row.get("tableName") or row.get("table_name") or "") for row in shown)
    return sorted(name for name in names if name.endswith(EVENT_LOG_SUFFIX))


def _catch_up_job(
    spark: Any,
    catalog: str,
    meta: str,
    event_log: str,
    job: str,
    days: int,
    pipeline_ids: list[str] | None = None,
    keep_kind: bool = False,
) -> int:
    dataflows = _dataflows(spark, meta, job)
    if not dataflows:
        return 0
    if pipeline_ids is not None and not any(flow.pipeline_id in pipeline_ids for flow in dataflows):
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
        for row in rows:
            if row.get("_drift_file"):
                row["_drift_rows"] = count_file_rows(spark, catalog, row)
        written += write_rows(spark, catalog, rows, replay=True, keep_kind=keep_kind)
        log.info("load catch-up: recorded %s update %s", job, update_id)
    return written


def count_file_rows(spark: Any, catalog: str, row: dict[str, Any]) -> int | None:
    """How many rows the file that stopped the update holds, read with the table's own options.

    Wherever the file is now: still in its folder, or moved to ``_failed`` already. None when it
    cannot be read, and the ledger then says the count is unknown.
    """

    from lakeflow_framework.backfill import load_config
    from lakeflow_framework.failed_files import parked_path
    from lakeflow_framework.load import SPARK_FORMAT, get_strategy

    path = str(row["_drift_file"])
    try:
        config = load_config(spark, catalog, str(row["dataflow"]))
        options = {
            key: value
            for key, value in get_strategy(config.load_method).options(config).items()
            if not key.startswith("cloudFiles.")
            and key not in {"pathGlobFilter", "rescuedDataColumn"}
        }
        reader = spark.read.format(SPARK_FORMAT[config.file_format]).options(**options)
    except Exception:
        log.warning("load catch-up: could not prepare to count the rows of %s", path)
        return None
    candidates = [path]
    try:
        candidates.append(str(parked_path(config.path, path)))
    except Exception:
        log.debug("load catch-up: no parked path for %s", path)
    for candidate in candidates:
        try:
            return int(reader.load(candidate).count())
        except Exception:
            log.debug("load catch-up: %s is not readable at %s", path, candidate)
    log.warning("load catch-up: could not count the rows of %s", path)
    return None


def _fail_rules(rules_json: Any) -> tuple[str, ...]:
    """The names of the rules that block a file, from the registry's list of rules."""

    try:
        rules = json.loads(str(rules_json or "[]"))
    except ValueError:
        return ()
    return tuple(
        str(rule["name"])
        for rule in rules
        if isinstance(rule, dict)
        and rule.get("action") == "fail"
        and rule.get("enabled") is not False
    )


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
            # What the pipeline's own hook knows from the spec, so a replay judges a run with
            # no file, or an empty one, the same way it does.
            expect_file=str(row.get("expect_file") or "").lower() == "true",
            fail_rules=_fail_rules(row.get("rules_json")),
            path=str(row.get("source_path") or ""),
            path_glob=str(row.get("path_glob") or ""),
        )
        for row in found
    ]


def _loads_files(spark: Any, table: str) -> bool:
    """True when the table has a ``_src_file_path`` column, which only a file source stamps."""

    if not table:
        return False
    try:
        columns = _rows(spark.sql(f"DESCRIBE TABLE {table}"))
    except Exception:
        return False
    return any(str(column.get("col_name")) == "_src_file_path" for column in columns)


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
    recorder = UpdateRecorder(
        job, LAYER, _with_file_load(spark, _in_this_update(dataflows, events), events)
    )
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


def _with_file_load(
    spark: Any, dataflows: list[Dataflow], events: list[dict[str, Any]]
) -> list[Dataflow]:
    """Mark the tables whose per-file rows can be counted from bronze and quarantine, and wait for
    a table's blocked-file check only when this update ran one.

    A replay knows from the events: a table with no ``fail`` rule has no check, and an update that
    failed before it ran has no flow for it. The check is what fails the update, so without it a
    failed update would be read as complete. Which rules fail a load and whether a file is
    expected come from the table's own spec, which the registry carries, so a replay names a
    blocked file's rule and runs the same missing-file check as the pipeline's hook.
    """

    names = [(event.get("origin") or {}).get("flow_name") for event in events]
    result = []
    for flow in dataflows:
        loads_files = _loads_files(spark, flow.target)
        check = f"{flow.pipeline_id}_blocked_check"
        files = f"{flow.pipeline_id}_files"
        catalog = flow.target.split(".", 1)[0] if "." in flow.target else ""
        ran_check = any(_same_table(name, check) for name in names)
        ran_files = bool(catalog) and any(_same_table(name, files) for name in names)
        result.append(
            replace(
                flow,
                file_load=loads_files,
                # only a table read from files has a file to expect
                expect_file=flow.expect_file and loads_files,
                blocked_check=check if ran_check else "",
                # the exact per-file counts, when this update built them
                files=f"{catalog}.framework_meta.{files}" if ran_files else "",
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
    # The row is the recorder's, whose quarantine table is the private ``_quarantine_table``.
    quarantine = row.get("_quarantine_table") or row.get("quarantine_table")
    for table in (row.get("target_table"), quarantine):
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
