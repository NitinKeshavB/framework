"""Registry rows and step files. Jobs write files. The collector inserts rows."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from lakeflow_framework.config import PipelineConfig
from lakeflow_framework.quality import compile_rules


def ddl_text() -> str:
    """Registry DDL. It lives in the framework bundle, next to the job that applies it."""

    path = Path(__file__).resolve().parents[2] / "framework_bundle" / "sql" / "framework_meta.sql"
    return path.read_text(encoding="utf-8")


def render_ddl(catalog: str, text: str | None = None) -> str:
    if not catalog or catalog.startswith("${"):
        raise ValueError("catalog is required")
    return (ddl_text() if text is None else text).replace("${catalog}", catalog)


def ddl_statements(catalog: str, text: str | None = None) -> list[str]:
    statements: list[str] = []
    for part in render_ddl(catalog, text).split(";"):
        lines = [line for line in part.splitlines() if not line.strip().startswith("--")]
        statement = "\n".join(lines).strip()
        if statement:
            statements.append(statement)
    return statements


def object_rows(config: PipelineConfig, recorded_at: str | None = None) -> list[dict[str, Any]]:
    recorded = recorded_at or _now()
    pipeline_name = config.pipeline_id
    scope = _scope(config)

    def row(kind: str, name: str, parent: str | None, relation: str | None, payload: Any):
        return _framework_object(kind, name, scope, parent, relation, payload, recorded)

    rules = [
        {"name": rule.name, "expression": rule.expression, "origin": rule.origin}
        for rule in compile_rules(config)
    ]
    rows = [
        row(
            "pipeline",
            pipeline_name,
            config.job_name,
            "owns" if config.job_name else None,
            {"config": config.canonical(), "rules": rules},
        ),
        row("dataflow", pipeline_name, pipeline_name, "contains", _dataflow_record(config)),
    ]
    if config.load_method == "volume_files":
        rows.append(
            row(
                "flow",
                f"{pipeline_name}.raw",
                pipeline_name,
                "reads",
                {"layer": "raw", "path": config.path},
            )
        )
    for layer, relation in (("raw", "writes"), ("bronze", "writes"), ("quarantine", "rejects_to")):
        if layer not in config.layers:
            continue
        full_name = config.table_name(layer)
        flow_name = f"{pipeline_name}.{layer}"
        rows += [
            row("flow", flow_name, pipeline_name, relation, {"table": full_name, "layer": layer}),
            row(
                _securable_kind(config, layer),
                full_name,
                flow_name,
                relation,
                _table_record(config, layer),
            ),
            row(
                "link",
                f"{pipeline_name}->{full_name}",
                pipeline_name,
                relation,
                {"child": full_name},
            ),
        ]
    rows.extend(_column_rows(config, recorded))
    return rows


def workflow_rows(
    configs: list[PipelineConfig], recorded_at: str | None = None
) -> list[dict[str, Any]]:
    """One workflow per job, listing every pipeline in that delivery."""

    recorded = recorded_at or _now()
    grouped: dict[str, list[PipelineConfig]] = {}
    for config in configs:
        if config.job_name:
            grouped.setdefault(config.job_name, []).append(config)
    rows: list[dict[str, Any]] = []
    for job_name, group in sorted(grouped.items()):
        requesters = sorted({item.requested_by for item in group if item.requested_by})
        payload = {
            "pipeline_ids": sorted({item.pipeline_id for item in group}),
            "requested_by": requesters,
        }
        scope = dict(_scope(group[0]))
        if len(requesters) != 1:
            scope["requested_by"] = None
        rows.append(_framework_object("workflow", job_name, scope, None, None, payload, recorded))
    return rows


def definition_rows(configs: list[PipelineConfig]) -> list[dict[str, Any]]:
    """Every definition row for these configs, each version once."""

    seen: set[tuple[str, str, str]] = set()
    rows: list[dict[str, Any]] = []
    for config in configs:
        rows.extend(new_object_rows(object_rows(config), seen))
    rows.extend(new_object_rows(workflow_rows(configs), seen))
    return rows


def _dataflow_record(config: PipelineConfig) -> dict[str, Any]:
    if config.load_method == "volume_files":
        raw: str | None = config.path
    elif config.has_raw:
        raw = config.table_name("raw")
    else:
        raw = None
    return {
        "raw": raw,
        "bronze": config.table_name("bronze"),
        "quarantine": config.table_name("quarantine"),
    }


def _column_rows(config: PipelineConfig, recorded_at: str) -> list[dict[str, Any]]:
    """One version per column of each table the pipeline writes."""

    scope = _scope(config)
    rows: list[dict[str, Any]] = []
    for layer in config.layers:
        table_name = config.table_name(layer)
        tags = config.tags[layer]
        fields = [
            (column.name, column.type_name, column.description, column.classification, column.pii)
            for column in config.columns
        ]
        if not fields and layer == "raw":
            fields = [
                (column.name, "string", "", tags.classification, [])
                for column in config.fixed_columns
            ]
        seen_names = {name for name, *_rest in fields}
        extras: list[tuple[str, str, str, str, list[str]]] = []
        if layer == "raw":
            extras += [
                ("_source_file", "string", "Source file path", tags.classification, []),
                ("_ingested_at", "timestamp", "Time the row was read", tags.classification, []),
            ]
        elif not config.has_raw:
            extras += _lineage_fields(config, tags.classification)
        if layer == "quarantine":
            extras.append(
                (
                    "failed_rules",
                    "array<string>",
                    "Rules that failed for this row",
                    tags.classification,
                    [],
                )
            )
        fields += [extra for extra in extras if extra[0] not in seen_names]
        for ordinal, (name, type_name, description, classification, pii) in enumerate(
            fields, start=1
        ):
            record = {
                "name": name,
                "ordinal": ordinal,
                "data_type": type_name,
                "description": description,
                "classification": classification,
                "pii": ", ".join(pii),
            }
            rows.append(
                _object(
                    "column",
                    f"{table_name}.{name}",
                    scope,
                    table_name,
                    "column_of",
                    json.dumps(record, sort_keys=True),
                    _column_hash(record),
                    "framework",
                    recorded_at,
                )
            )
    return rows


def _lineage_fields(
    config: PipelineConfig, classification: str
) -> list[tuple[str, str, str, str, list[str]]]:
    """Columns the pipeline adds when there is no raw table to carry them."""

    ingested = ("_ingested_at", "timestamp", "Time the row was read", classification, [])
    if config.load_method == "volume_files":
        return [("_source_file", "string", "Source file path", classification, []), ingested]
    return [ingested]


def _digest(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _column_hash(record: Mapping[str, Any]) -> str:
    """Identity of one column version. The column name is the registry key, not the hash.

    A new data type, description, sensitivity tag, or position opens a version.
    Other pipeline edits do not. Returning to an older value opens a version too.
    """

    versioned = {
        "ordinal": record["ordinal"],
        "data_type": record["data_type"],
        "description": record["description"],
        "classification": record["classification"],
        "pii": record["pii"],
    }
    return _digest(json.dumps(versioned, sort_keys=True, separators=(",", ":")))


def _table_record(config: PipelineConfig, layer: str) -> dict[str, Any]:
    tags = config.tags[layer]
    return {
        "layer": layer,
        "domain": config.domain,
        "data_domain": config.data_domain,
        "team": config.team,
        "technical_owner": config.technical_owner,
        "classification": tags.classification,
        "pii": ", ".join(tags.pii),
    }


def _securable_kind(config: PipelineConfig, layer: str) -> str:
    if layer == "raw":
        return "streaming_table" if config.load_method == "autoloader" else "table"
    if layer == "quarantine":
        return "materialized_view" if config.is_batch else "streaming_table"
    if config.uses_auto_cdc or not config.is_batch:
        return "streaming_table"
    return "materialized_view"


def _scope(config: PipelineConfig) -> dict[str, str | None]:
    """Who owns a definition. Bronze is owned by its source system, never by the team that
    asked for it. ``requested_by`` and ``delivery`` ride along for the maps."""

    return {
        "owner_kind": config.owner_kind,
        "owner": config.owner,
        "requested_by": config.requested_by,
        "delivery": config.delivery or None,
    }


def _framework_object(
    kind: str,
    full_name: str,
    scope: Mapping[str, str | None],
    parent: str | None,
    relation: str | None,
    payload: Any,
    recorded_at: str,
) -> dict[str, Any]:
    config_json = json.dumps(payload, sort_keys=True)
    return _object(
        kind,
        full_name,
        scope,
        parent,
        relation,
        config_json,
        _digest(config_json),
        "framework",
        recorded_at,
    )


def _object(
    kind: str,
    full_name: str,
    scope: Mapping[str, str | None],
    parent: str | None,
    relation: str | None,
    config_json: str,
    content_hash: str,
    source: str,
    recorded_at: str,
) -> dict[str, Any]:
    return {
        "object_kind": kind,
        "full_name": full_name,
        "owner_kind": scope["owner_kind"],
        "owner": scope["owner"],
        "requested_by": scope["requested_by"],
        "delivery": scope["delivery"],
        "parent_full_name": parent,
        "relation": relation,
        "config_json": config_json,
        "content_hash": content_hash,
        "source": source,
        "recorded_at": recorded_at,
    }


OBJECT_COLUMNS = (
    "object_kind",
    "full_name",
    "owner_kind",
    "owner",
    "requested_by",
    "delivery",
    "parent_full_name",
    "relation",
    "config_json",
    "content_hash",
    "source",
    "recorded_at",
)

LEDGER_COLUMNS = (
    "run_id",
    "parent_run_id",
    "object_kind",
    "full_name",
    "owner_kind",
    "owner",
    "step_no",
    "step_name",
    "status",
    "started_at",
    "ended_at",
    "failure_reason",
    "recon_json",
    "recorded_at",
    "event_date",
)

OWNER_KINDS = ("source_system", "project")
# Definitions the collector refuses to hand to a second owner. Flows, links, workflows, and
# columns follow their pipeline or table.
OWNED_KINDS = ("pipeline", "table", "streaming_table", "materialized_view")

_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")
_SAFE_HASH = re.compile(r"^[0-9a-f]{64}$")
_SAFE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SAFE_INSTANT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def write_definition_files(directory: Path, rows: list[dict[str, Any]]) -> list[Path]:
    """One JSON-lines file per owner.

    A sync used to write one file per column, flow, and link. At thousands of jobs that listing
    is the collector's bottleneck. One file per owner is one Auto Loader file; each line is one
    definition. The name is the hash of the body, so an unchanged sync overwrites the same file
    and is not collected again.
    """

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((str(row["owner_kind"]), str(row["owner"])), []).append(row)
    written: list[Path] = []
    for (owner_kind, owner), group in sorted(grouped.items()):
        lines = [
            json.dumps({"kind": "definition", **row}, sort_keys=True, separators=(",", ":"))
            for row in group
        ]
        body = "\n".join(lines) + "\n"
        digest = _digest(body)[:16]
        target = directory / owner_kind / owner / "definitions" / f"{digest}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
        written.append(target)
    return written


def split_payloads(
    payloads: list[Mapping[str, Any]],
) -> tuple[list[Mapping[str, Any]], list[dict[str, Any]]]:
    steps: list[Mapping[str, Any]] = []
    definitions: list[dict[str, Any]] = []
    for payload in payloads:
        if payload.get("kind") == "step":
            steps.append(payload)
        elif payload.get("kind") == "definition":
            definitions.append({column: payload.get(column) for column in OBJECT_COLUMNS})
    return steps, definitions


def new_object_rows(
    rows: list[dict[str, Any]], seen_hashes: set[tuple[str, str, str]]
) -> list[dict[str, Any]]:
    """Keep a row when its hash differs from the current hash of that object.

    The same payload collected again is skipped. A return to an older payload is kept,
    because that is a new period in which the older value was active.
    """

    current = {(kind, name): digest for kind, name, digest in seen_hashes}
    fresh: list[dict[str, Any]] = []
    for row in rows:
        key = (row["object_kind"], row["full_name"])
        digest = row["content_hash"]
        if current.get(key) == digest:
            continue
        current[key] = digest
        fresh.append(row)
    seen_hashes.clear()
    seen_hashes.update((kind, name, digest) for (kind, name), digest in current.items())
    return fresh


def step_payload(
    *,
    owner: str,
    full_name: str,
    run_id: str,
    owner_kind: str = "source_system",
    step_no: int,
    step_name: str,
    status: str,
    object_kind: str = "pipeline",
    parent_run_id: str | None = None,
    failure_reason: str | None = None,
    recon: Mapping[str, Any] | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
) -> dict[str, Any]:
    if status not in {"RUNNING", "SUCCEEDED", "FAILED"}:
        raise ValueError("status must be RUNNING, SUCCEEDED, or FAILED")
    if owner_kind not in OWNER_KINDS:
        raise ValueError(f"owner_kind must be one of {', '.join(OWNER_KINDS)}")
    return {
        "kind": "step",
        "owner_kind": owner_kind,
        "owner": owner,
        "full_name": full_name,
        "object_kind": object_kind,
        "run_id": run_id,
        "parent_run_id": parent_run_id,
        "step_no": step_no,
        "step_name": step_name,
        "status": status,
        "started_at": started_at,
        "ended_at": ended_at,
        "failure_reason": failure_reason,
        "recon": dict(recon) if recon else None,
        "recon_json": json.dumps(recon, sort_keys=True) if recon else None,
        "recorded_at": _now(),
    }


def step_file_name(payload: Mapping[str, Any]) -> str:
    return (
        f"{payload['owner_kind']}/{payload['owner']}/{payload['full_name']}/"
        f"{payload['run_id']}_{payload['step_no']}_{payload['status']}.json"
    )


def write_step_file(directory: Path, payload: Mapping[str, Any]) -> Path:
    target = directory / step_file_name(payload)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return target


def ledger_row(payload: Mapping[str, Any]) -> dict[str, Any]:
    recorded = str(payload.get("recorded_at") or _now())
    return {
        "run_id": payload["run_id"],
        "parent_run_id": payload.get("parent_run_id"),
        "object_kind": payload.get("object_kind", "pipeline"),
        "full_name": payload["full_name"],
        "owner_kind": payload["owner_kind"],
        "owner": payload["owner"],
        "step_no": int(payload["step_no"]),
        "step_name": payload["step_name"],
        "status": payload["status"],
        "started_at": payload.get("started_at"),
        "ended_at": payload.get("ended_at"),
        "failure_reason": payload.get("failure_reason"),
        "recon_json": json.dumps(payload["recon"], sort_keys=True)
        if payload.get("recon")
        else payload.get("recon_json"),
        "recorded_at": recorded,
        "event_date": recorded[:10],
    }


def step_key(payload: Mapping[str, Any]) -> tuple[str, int, str]:
    return (str(payload["run_id"]), int(payload["step_no"]), str(payload["status"]))


def ingest_steps(
    payloads: list[Mapping[str, Any]],
    seen: set[tuple[str, int, str]] | None = None,
) -> list[dict[str, Any]]:
    seen_keys = seen if seen is not None else set()
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        key = step_key(payload)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        rows.append(ledger_row(payload))
    return rows


def build_recon(
    *,
    source_files: list[dict[str, Any]] | None = None,
    source_count: int | None = None,
    inserted: int | None = None,
    updated: int | None = None,
    deleted: int | None = None,
    output_rows: int | None = None,
    rejected: int | None = None,
    target_records: dict[str, int] | None = None,
) -> dict[str, Any]:
    arrived = source_count
    if arrived is None and source_files:
        arrived = sum(int(item.get("record_count") or 0) for item in source_files)
    if inserted is None and updated is None:
        landed = (output_rows or 0) + (rejected or 0)
        comparable = output_rows is not None or rejected is not None
    else:
        landed = (inserted or 0) + (updated or 0) + (rejected or 0)
        comparable = True
    if arrived is None or not comparable:
        result = "unknown"
        detail = "source count or target metrics are missing"
    elif arrived == landed:
        result = "matched"
        detail = f"source {arrived} equals landed {landed}"
    else:
        result = "mismatch"
        detail = f"source {arrived} does not equal landed {landed}"
    return {
        "source_files": source_files or [],
        "source_count": arrived,
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "output_rows": output_rows,
        "rejected": rejected,
        "target_records": target_records or {},
        "recon_result": result,
        "detail": detail,
    }


def registered_sql(pipeline_ids: list[str], catalog: str) -> str:
    _require_catalog(catalog)
    if not pipeline_ids or any(not _SAFE_TOKEN.match(pipeline_id) for pipeline_id in pipeline_ids):
        raise ValueError("pipeline id is required")
    return (
        f"SELECT full_name, content_hash FROM {catalog}.framework_meta.v_object_current "
        f"WHERE object_kind = 'pipeline' AND full_name IN ({_sql_in(pipeline_ids)})"
    )


def current_objects_sql(catalog: str, rows: list[Mapping[str, Any]]) -> str:
    """Current hash for each object. object_current has one row, so this is a point lookup."""

    _require_catalog(catalog)
    names, kinds, owners = _object_keys(rows)
    owner_kinds = sorted({str(row.get("owner_kind") or "") for row in rows})
    if any(not _SAFE_TOKEN.match(value) for value in owner_kinds):
        raise ValueError("owner is required")
    return (
        "SELECT object_kind, full_name, content_hash "
        f"FROM {catalog}.framework_meta.object_current "
        f"WHERE owner_kind IN ({_sql_in(owner_kinds)}) AND owner IN ({_sql_in(owners)}) "
        f"AND object_kind IN ({_sql_in(kinds)}) "
        f"AND full_name IN ({_sql_in(names)})"
    )


def recorded_versions_sql(catalog: str, rows: list[Mapping[str, Any]]) -> str:
    """History rows for these exact versions. A retry matches hash and recorded_at."""

    _require_catalog(catalog)
    names, kinds, owners = _object_keys(rows)
    hashes = sorted({str(row["content_hash"]) for row in rows})
    instants = sorted({str(row["recorded_at"]) for row in rows})
    if any(not _SAFE_HASH.match(value) for value in hashes):
        raise ValueError("content hash is required")
    if any(not _SAFE_INSTANT.match(value) for value in instants):
        raise ValueError("recorded_at is required")
    quoted_times = ", ".join(
        f"to_timestamp('{instant}', 'yyyy-MM-dd''T''HH:mm:ss''Z''')" for instant in instants
    )
    return (
        "SELECT object_kind, full_name, content_hash, "
        "date_format(recorded_at, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS recorded_at "
        f"FROM {catalog}.framework_meta.object_registry "
        f"WHERE owner IN ({_sql_in(owners)}) "
        f"AND object_kind IN ({_sql_in(kinds)}) AND full_name IN ({_sql_in(names)}) "
        f"AND content_hash IN ({_sql_in(hashes)}) AND recorded_at IN ({quoted_times})"
    )


def object_keys_sql(catalog: str, content_hashes: list[str]) -> str:
    _require_catalog(catalog)
    hashes = [value for value in content_hashes if _SAFE_HASH.match(value)]
    if not hashes:
        raise ValueError("content hash is required")
    return (
        f"SELECT object_kind, full_name, content_hash FROM {catalog}.framework_meta.object_registry "
        f"WHERE content_hash IN ({_sql_in(hashes)})"
    )


def owners_sql(catalog: str, full_names: list[str]) -> str:
    """Current owner of each named table or pipeline, from the registry."""

    _require_catalog(catalog)
    names = [value for value in full_names if _SAFE_NAME.match(value)]
    if not names:
        raise ValueError("full name is required")
    return (
        f"SELECT object_kind, full_name, owner_kind, owner "
        f"FROM {catalog}.framework_meta.v_object_current "
        f"WHERE object_kind IN ({_sql_in(OWNED_KINDS)}) "
        f"AND full_name IN ({_sql_in(names)})"
    )


def _require_catalog(catalog: str) -> None:
    if not catalog or not _SAFE_TOKEN.match(catalog):
        raise ValueError("catalog is required")


def _sql_in(values: Iterable[str]) -> str:
    """Quote values already checked against a safe pattern for an IN list."""

    return ", ".join(f"'{value}'" for value in values)


def _object_keys(rows: list[Mapping[str, Any]]) -> tuple[list[str], list[str], list[str]]:
    """Sorted, checked names, kinds, and owners of definition rows."""

    names = sorted({str(row["full_name"]) for row in rows})
    kinds = sorted({str(row["object_kind"]) for row in rows})
    owners = sorted({str(row.get("owner") or "") for row in rows})
    if not names or any(not _SAFE_NAME.match(name) for name in names):
        raise ValueError("object name is required")
    if any(not _SAFE_TOKEN.match(kind) for kind in kinds):
        raise ValueError("object kind is required")
    if any(not _SAFE_TOKEN.match(owner) for owner in owners):
        raise ValueError("owner is required")
    return names, kinds, owners


def ownership_conflicts(
    rows: list[dict[str, Any]], current: Mapping[tuple[str, str], tuple[str, str]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split definition rows into those that may be inserted and those another owner holds.

    A name that has no current owner is free. A name held by the same owner is an update.
    """

    accepted: list[dict[str, Any]] = []
    refused: list[dict[str, Any]] = []
    for row in rows:
        holder = current.get((str(row["object_kind"]), str(row["full_name"])))
        mine = (str(row["owner_kind"]), str(row["owner"]))
        if row["object_kind"] in OWNED_KINDS and holder is not None and holder != mine:
            refused.append(row)
        else:
            accepted.append(row)
    return accepted, refused


def step_keys_sql(
    catalog: str,
    run_ids: list[str],
    owners: list[str] | None = None,
    event_dates: list[str] | None = None,
) -> str:
    """Steps already stored for these runs.

    ``owner`` and ``event_date`` are the leading cluster columns. They are added only when every
    value is safe, so a bad value widens the lookup instead of hiding a row that should dedupe.
    """

    _require_catalog(catalog)
    safe_ids = [value for value in run_ids if _SAFE_TOKEN.match(value)]
    if not safe_ids:
        raise ValueError("run id is required")
    filters = [f"run_id IN ({_sql_in(dict.fromkeys(safe_ids))})"]
    if owners:
        unique_owners = list(dict.fromkeys(owners))
        if all(_SAFE_TOKEN.match(value) for value in unique_owners):
            filters.append(f"owner IN ({_sql_in(sorted(unique_owners))})")
    if event_dates:
        unique_dates = list(dict.fromkeys(event_dates))
        if all(_SAFE_DATE.match(value) for value in unique_dates):
            quoted_dates = ", ".join(f"DATE '{value}'" for value in sorted(unique_dates))
            filters.append(f"event_date IN ({quoted_dates})")
    return (
        f"SELECT run_id, step_no, status FROM {catalog}.framework_meta.run_ledger "
        f"WHERE {' AND '.join(filters)}"
    )


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
