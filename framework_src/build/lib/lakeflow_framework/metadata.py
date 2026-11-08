"""Registry rows and step files. Jobs write files. The collector inserts rows."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from lakeflow_framework.config import PipelineConfig
from lakeflow_framework.quality import compile_rules


def ddl_text() -> str:
    path = Path(__file__).resolve().parents[2] / "framework_sql" / "framework_meta.sql"
    return path.read_text(encoding="utf-8")


def render_ddl(catalog: str) -> str:
    if not catalog or catalog.startswith("${"):
        raise ValueError("catalog is required")
    return ddl_text().replace("${catalog}", catalog)


def ddl_statements(catalog: str) -> list[str]:
    statements: list[str] = []
    for part in render_ddl(catalog).split(";"):
        lines = [line for line in part.splitlines() if not line.strip().startswith("--")]
        statement = "\n".join(lines).strip()
        if statement:
            statements.append(statement)
    return statements


def object_rows(config: PipelineConfig, recorded_at: str | None = None) -> list[dict[str, Any]]:
    recorded = recorded_at or _now()
    config_json = json.dumps(
        {
            "config": config.canonical(),
            "rules": [
                {"name": rule.name, "expression": rule.expression, "origin": rule.origin}
                for rule in compile_rules(config)
            ],
        },
        sort_keys=True,
    )
    content_hash = config.content_hash()
    pipeline_name = config.pipeline_id
    rows = [
        _object(
            "pipeline",
            pipeline_name,
            config.project,
            config.job_name,
            "owns" if config.job_name else None,
            config_json,
            content_hash,
            "framework",
            recorded,
        )
    ]
    for layer, relation in (("raw", "writes"), ("bronze", "writes"), ("quarantine", "rejects_to")):
        full_name = config.table_name(layer)
        rows.append(
            _object(
                "flow",
                f"{pipeline_name}.{layer}",
                config.project,
                pipeline_name,
                relation,
                json.dumps({"table": full_name, "layer": layer}, sort_keys=True),
                content_hash,
                "framework",
                recorded,
            )
        )
        rows.append(
            _object(
                _securable_kind(config, layer),
                full_name,
                config.project,
                f"{pipeline_name}.{layer}",
                relation,
                json.dumps({"layer": layer}, sort_keys=True),
                content_hash,
                "framework",
                recorded,
            )
        )
        rows.append(
            _object(
                "link",
                f"{pipeline_name}->{full_name}",
                config.project,
                pipeline_name,
                relation,
                json.dumps({"child": full_name}, sort_keys=True),
                content_hash,
                "framework",
                recorded,
            )
        )
    if config.job_name:
        rows.append(
            _object(
                "workflow",
                config.job_name,
                config.project,
                None,
                None,
                json.dumps({"pipeline_id": pipeline_name}, sort_keys=True),
                content_hash,
                "framework",
                recorded,
            )
        )
    return rows


def _securable_kind(config: PipelineConfig, layer: str) -> str:
    if layer == "raw":
        return "streaming_table" if config.load_method == "autoloader" else "table"
    if layer == "quarantine":
        return "materialized_view" if config.load_method == "ctas" else "streaming_table"
    if config.dedup or config.load_method != "ctas":
        return "streaming_table"
    return "materialized_view"


def _object(
    kind: str,
    full_name: str,
    project: str,
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
        "project": project,
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
    "project",
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
    "project",
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

_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
_SAFE_HASH = re.compile(r"^[0-9a-f]{64}$")


def definition_file_name(row: Mapping[str, Any]) -> str:
    safe_name = re.sub(r"[^A-Za-z0-9_]+", "_", str(row["full_name"])).strip("_")
    return (
        f"{row['project']}/definitions/"
        f"{row['object_kind']}_{safe_name}_{str(row['content_hash'])[:16]}.json"
    )


def write_definition_files(directory: Path, rows: list[dict[str, Any]]) -> list[Path]:
    written: list[Path] = []
    for row in rows:
        payload = {"kind": "definition", **row}
        target = directory / definition_file_name(row)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
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
    fresh: list[dict[str, Any]] = []
    for row in rows:
        key = (row["object_kind"], row["full_name"], row["content_hash"])
        if key in seen_hashes:
            continue
        seen_hashes.add(key)
        fresh.append(row)
    return fresh


def step_payload(
    *,
    project: str,
    full_name: str,
    run_id: str,
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
    return {
        "kind": "step",
        "project": project,
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
        "recorded_at": _now(),
    }


def step_file_name(payload: Mapping[str, Any]) -> str:
    return (
        f"{payload['project']}/{payload['full_name']}/"
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
        "project": payload["project"],
        "step_no": int(payload["step_no"]),
        "step_name": payload["step_name"],
        "status": payload["status"],
        "started_at": payload.get("started_at"),
        "ended_at": payload.get("ended_at"),
        "failure_reason": payload.get("failure_reason"),
        "recon_json": json.dumps(payload["recon"], sort_keys=True)
        if payload.get("recon")
        else None,
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
    if not catalog or not _SAFE_TOKEN.match(catalog):
        raise ValueError("catalog is required")
    if not pipeline_ids or any(not _SAFE_TOKEN.match(pipeline_id) for pipeline_id in pipeline_ids):
        raise ValueError("pipeline id is required")
    quoted = ", ".join(f"'{pipeline_id}'" for pipeline_id in pipeline_ids)
    return (
        f"SELECT full_name FROM {catalog}.framework_meta.v_object_current "
        f"WHERE object_kind = 'pipeline' AND full_name IN ({quoted})"
    )


def object_keys_sql(catalog: str, content_hashes: list[str]) -> str:
    if not catalog or not _SAFE_TOKEN.match(catalog):
        raise ValueError("catalog is required")
    hashes = [value for value in content_hashes if _SAFE_HASH.match(value)]
    if not hashes:
        raise ValueError("content hash is required")
    quoted = ", ".join(f"'{value}'" for value in hashes)
    return (
        f"SELECT object_kind, full_name, content_hash FROM {catalog}.framework_meta.object_registry "
        f"WHERE content_hash IN ({quoted})"
    )


def step_keys_sql(catalog: str, run_ids: list[str]) -> str:
    if not catalog or not _SAFE_TOKEN.match(catalog):
        raise ValueError("catalog is required")
    safe_ids = [value for value in run_ids if _SAFE_TOKEN.match(value)]
    if not safe_ids:
        raise ValueError("run id is required")
    quoted = ", ".join(f"'{value}'" for value in safe_ids)
    return (
        f"SELECT run_id, step_no, status FROM {catalog}.framework_meta.run_ledger "
        f"WHERE run_id IN ({quoted})"
    )


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
