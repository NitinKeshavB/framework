"""Registry rows and step files. Jobs write files. The collector inserts rows."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from lakeflow_framework.config import (
    AUDIT_COLUMN_DESCRIPTIONS,
    BATCH_ID,
    INGEST_TIMESTAMP,
    IS_DELETED,
    RESCUED_DATA,
    SRC_FILE_NAME,
    SRC_FILE_PATH,
    SRC_ROW_HASH,
    PipelineConfig,
)
from lakeflow_framework.load import RESCUE_FORMATS
from lakeflow_framework.quality import compile_rules


def ddl_text() -> str:
    """Registry DDL shipped as package data (works from a wheel, not only a repo checkout)."""

    from importlib import resources

    return (
        resources.files("lakeflow_framework")
        .joinpath("sql/framework_meta.sql")
        .read_text(encoding="utf-8")
    )


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
    root = pipeline_stable_key(config)

    def row(
        kind: str,
        name: str,
        parent: str | None,
        relation: str | None,
        payload: Any,
        *,
        suffix: str = "",
    ):
        return framework_object(
            kind,
            name,
            scope,
            parent,
            relation,
            payload,
            recorded,
            stable_key=f"{root}|{suffix}" if suffix else root,
        )

    rules = [
        {
            "name": rule.name,
            "expression": rule.expression,
            "origin": rule.origin,
            "action": rule.action,
        }
        for rule in compile_rules(config)
    ]
    rows = [
        row(
            "pipeline",
            pipeline_name,
            config.job_name,
            "owns" if config.job_name else None,
            {"config": config.canonical(), "rules": rules},
            suffix="pipeline",
        ),
        row(
            "dataflow",
            pipeline_name,
            pipeline_name,
            "contains",
            _dataflow_record(config),
            suffix="dataflow",
        ),
    ]
    if config.load_method == "volume_files":
        rows.append(
            row(
                "flow",
                f"{pipeline_name}.raw",
                pipeline_name,
                "reads",
                {"layer": "raw", "path": config.path},
                suffix="flow|raw|reads",
            )
        )
    for layer, relation in (("raw", "writes"), ("bronze", "writes"), ("quarantine", "rejects_to")):
        if layer not in config.layers:
            continue
        full_name = config.table_name(layer)
        flow_name = f"{pipeline_name}.{layer}"
        rows += [
            row(
                "flow",
                flow_name,
                pipeline_name,
                relation,
                {"table": full_name, "layer": layer},
                suffix=f"flow|{layer}|{relation}",
            ),
            row(
                _securable_kind(config, layer),
                full_name,
                flow_name,
                relation,
                _table_record(config, layer),
                suffix=f"table|{layer}",
            ),
            row(
                "link",
                f"{pipeline_name}->{full_name}",
                pipeline_name,
                relation,
                {"child": full_name},
                suffix=f"link|{layer}|{relation}",
            ),
        ]
    rows.extend(_column_rows(config, recorded, root))
    return rows


def workflow_rows(
    configs: list[PipelineConfig],
    recorded_at: str | None = None,
    details: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """One workflow per job, listing every pipeline in that delivery.

    ``details`` adds what the generator knows about each job: its trigger or schedule and the
    source it belongs to.
    """

    recorded = recorded_at or _now()
    grouped: dict[str, list[PipelineConfig]] = {}
    for config in configs:
        if config.job_name:
            grouped.setdefault(config.job_name, []).append(config)
    rows: list[dict[str, Any]] = []
    for job_name, group in sorted(grouped.items()):
        requesters = sorted({item.requested_by for item in group if item.requested_by})
        payload: dict[str, Any] = {
            "pipeline_ids": sorted({item.pipeline_id for item in group}),
            "requested_by": requesters,
        }
        if details and job_name in details:
            payload.update(details[job_name])
        scope = dict(_scope(group[0]))
        if len(requesters) != 1:
            scope["requested_by"] = None
        rows.append(
            framework_object(
                "workflow",
                job_name,
                scope,
                None,
                None,
                payload,
                recorded,
                stable_key=f"workflow|{job_name}",
            )
        )
    return rows


def definition_rows(
    configs: list[PipelineConfig], workflow_details: Mapping[str, Mapping[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Every definition row for these configs, each version once."""

    seen: set[tuple[str, str, str]] = set()
    rows: list[dict[str, Any]] = []
    for config in configs:
        rows.extend(new_object_rows(object_rows(config), seen))
    rows.extend(new_object_rows(workflow_rows(configs, details=workflow_details), seen))
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
        "quarantine": config.table_name("quarantine") if config.has_quarantine else None,
    }


def _column_rows(
    config: PipelineConfig, recorded_at: str, root_stable_key: str
) -> list[dict[str, Any]]:
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
        extras = audit_columns(config, layer, tags.classification)
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
                    stable_key=f"{root_stable_key}|column|{layer}|{name}",
                )
            )
    return rows


INFERRED_SOURCE = "inferred"
QUARANTINE_ONLY_COLUMNS = {"failed_rules": "Rules that failed for this row"}


def metadata_column_names() -> frozenset[str]:
    """The columns the framework adds itself. A table whose registered columns are all these has
    no columns of its own in the registry."""

    return frozenset(AUDIT_COLUMN_DESCRIPTIONS) | frozenset(QUARANTINE_ONLY_COLUMNS)


def observed_column_rows(
    config: PipelineConfig,
    observed: Mapping[str, list[tuple[str, str]]],
    recorded_at: str | None = None,
) -> list[dict[str, Any]]:
    """One version per column of tables that already exist, from what each table really has.

    ``observed`` maps a layer to its (name, type) pairs in table order. The rows are marked
    ``source = inferred``, so a later deploy keeps them (``infer_columns.keep_inferred_columns``)
    and a spec that starts declaring columns replaces them. A column the framework adds keeps its
    description; one that came from the files has none, because nobody described it.
    """

    recorded = recorded_at or _now()
    scope = _scope(config)
    root = pipeline_stable_key(config)
    rows: list[dict[str, Any]] = []
    for layer, found in observed.items():
        table_name = config.table_name(layer)
        classification = config.tags[layer].classification
        described = {
            name: description
            for name, _type, description, _cls, _pii in audit_columns(config, layer, classification)
        }
        if layer == "quarantine":
            described.update(QUARANTINE_ONLY_COLUMNS)
        for ordinal, (name, type_name) in enumerate(found, start=1):
            record = {
                "name": name,
                "ordinal": ordinal,
                "data_type": type_name.lower(),
                "description": described.get(name, ""),
                "classification": classification,
                "pii": "",
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
                    INFERRED_SOURCE,
                    recorded,
                    stable_key=f"{root}|column|{layer}|{name}",
                )
            )
    return rows


def audit_columns(
    config: PipelineConfig, layer: str, classification: str
) -> list[tuple[str, str, str, str, list[str]]]:
    """The audit columns a table carries, as (name, type, description, classification, pii).

    The same set on every bronze and quarantine table of a load type, so a reader can rely on it.
    The raw table of a frozen method carries only what the file read gives it.
    """

    def column(name: str, type_name: str) -> tuple[str, str, str, str, list[str]]:
        return (name, type_name, AUDIT_COLUMN_DESCRIPTIONS[name], classification, [])

    from_files = config.load_method != "database_table"
    found: list[tuple[str, str, str, str, list[str]]] = []
    if from_files:
        found += [column(SRC_FILE_PATH, "string"), column(SRC_FILE_NAME, "string")]
    if layer == "raw":
        return [*found, column(SRC_ROW_HASH, "string"), column(INGEST_TIMESTAMP, "timestamp")]
    found += [
        column(SRC_ROW_HASH, "string"),
        column(BATCH_ID, "string"),
        column(INGEST_TIMESTAMP, "timestamp"),
    ]
    if config.delete_when:
        found.append(column(IS_DELETED, "boolean"))
    if from_files and config.file_format in RESCUE_FORMATS:
        found.append(column(RESCUED_DATA, "string"))
    return found


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
        "pipeline_id": config.pipeline_id,
        "job_name": config.job_name,
        "load_method": config.load_method,
        "load_type": config.load_type,
        "description": {
            "bronze": config.description,
            "quarantine": config.quarantine_description,
        }.get(layer, ""),
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


def framework_object(
    kind: str,
    full_name: str,
    scope: Mapping[str, str | None],
    parent: str | None,
    relation: str | None,
    payload: Any,
    recorded_at: str,
    *,
    stable_key: str | None = None,
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
        stable_key=stable_key,
    )


def asset_id(kind: str, stable_key: str) -> str:
    """Stable surrogate for an asset. Derived from coordinates, not the display name."""

    return _digest(f"{kind}|{stable_key}")


def pipeline_stable_key(config: PipelineConfig) -> str:
    """Source coordinates that survive a pipeline_id / table rename."""

    if config.load_method == "database_table":
        return (
            f"db|{config.source_system}|{config.source_table}|"
            f"{config.bronze_schema}.{config.bronze_table}"
        )
    path = (config.path or "").strip("/")
    parts = path.split("/")
    if len(parts) >= 4 and parts[0] == "Volumes":
        # Drop the environment catalog: Volumes/<catalog>/<schema>/<volume>/...
        without_catalog = "/".join(parts[2:])
        return (
            f"vol|{config.source_system}|{without_catalog}|"
            f"{config.bronze_schema}.{config.bronze_table}"
        )
    return f"path|{config.source_system}|{path}|{config.bronze_schema}.{config.bronze_table}"


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
    bundle_id: str | None = None,
    *,
    stable_key: str | None = None,
) -> dict[str, Any]:
    key = stable_key if stable_key is not None else f"{kind}|{full_name}"
    return {
        "asset_id": asset_id(kind, key),
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
        "bundle_id": bundle_id,
        "recorded_at": recorded_at,
    }


OBJECT_COLUMNS = (
    "asset_id",
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
    "bundle_id",
    "recorded_at",
)
# Staged Spark views for point lookups (avoids megabyte IN lists on large deploys).
LOOKUP_KEYS_VIEW = "_framework_lookup_keys"
OWNER_NAMES_VIEW = "_framework_owner_names"
# Pipeline event logs live here so framework_meta.SHOW TABLES stays small.
EVENTS_SCHEMA = "framework_events"

# Ownership refusals only (replaces the vestigial run_ledger / run_current step tables).
REFUSAL_COLUMNS = (
    "object_kind",
    "full_name",
    "refused_owner_kind",
    "refused_owner",
    "holder_owner_kind",
    "holder_owner",
    "failure_reason",
    "content_hash",
    "recorded_at",
)
# Back-compat alias for imports that still say LEDGER_COLUMNS.
LEDGER_COLUMNS = REFUSAL_COLUMNS

OWNER_KINDS = ("source_system", "project")
# Definitions registry_sync refuses to hand to a second owner. Flows, links, workflows, and
# columns follow their pipeline or table.
OWNED_KINDS = (
    "source_system",
    "schema",
    "pipeline",
    "table",
    "streaming_table",
    "materialized_view",
)
# Validity columns asset_registry stores for each version. registry_sync sets them.
SCD_COLUMNS = ("valid_from", "valid_to", "is_current", "is_deleted")

_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
# ">" is for link names, pipeline->table. Nothing here can close a quoted SQL string.
_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.>-]+$")
_SAFE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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


def ownership_refusal_row(
    *,
    object_kind: str,
    full_name: str,
    refused_owner_kind: str,
    refused_owner: str,
    holder_owner_kind: str,
    holder_owner: str,
    failure_reason: str,
    content_hash: str,
    recorded_at: str | None = None,
) -> dict[str, Any]:
    if refused_owner_kind not in OWNER_KINDS:
        raise ValueError(f"owner_kind must be one of {', '.join(OWNER_KINDS)}")
    if holder_owner_kind and holder_owner_kind not in OWNER_KINDS:
        raise ValueError(f"owner_kind must be one of {', '.join(OWNER_KINDS)}")
    return {
        "object_kind": object_kind,
        "full_name": full_name,
        "refused_owner_kind": refused_owner_kind,
        "refused_owner": refused_owner,
        "holder_owner_kind": holder_owner_kind,
        "holder_owner": holder_owner,
        "failure_reason": failure_reason,
        "content_hash": content_hash,
        "recorded_at": recorded_at or _now(),
    }


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
    started_at: str | None = None,
    ended_at: str | None = None,
    content_hash: str | None = None,
    holder_owner_kind: str | None = None,
    holder_owner: str | None = None,
) -> dict[str, Any]:
    """Build an ownership-refusal payload. Kept name for call-site compatibility."""

    if status not in {"RUNNING", "SUCCEEDED", "FAILED"}:
        raise ValueError("status must be RUNNING, SUCCEEDED, or FAILED")
    if owner_kind not in OWNER_KINDS:
        raise ValueError(f"owner_kind must be one of {', '.join(OWNER_KINDS)}")
    return {
        "kind": "ownership_refusal",
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
        "content_hash": content_hash or "",
        "holder_owner_kind": holder_owner_kind or "",
        "holder_owner": holder_owner or "",
        "recorded_at": _now(),
    }


def ledger_row(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Map a refusal payload to an ``ownership_refusals`` row."""

    return ownership_refusal_row(
        object_kind=str(payload.get("object_kind") or "pipeline"),
        full_name=str(payload["full_name"]),
        refused_owner_kind=str(payload["owner_kind"]),
        refused_owner=str(payload["owner"]),
        holder_owner_kind=str(payload.get("holder_owner_kind") or ""),
        holder_owner=str(payload.get("holder_owner") or ""),
        failure_reason=str(payload.get("failure_reason") or ""),
        content_hash=str(payload.get("content_hash") or ""),
        recorded_at=str(payload.get("recorded_at") or _now()),
    )


def step_key(payload: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(payload["object_kind"]),
        str(payload["full_name"]),
        str(payload.get("content_hash") or payload.get("run_id") or ""),
    )


def ingest_steps(
    payloads: list[Mapping[str, Any]],
    seen: set[tuple[str, str, str]] | None = None,
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


def current_objects_sql(catalog: str, staged: str = LOOKUP_KEYS_VIEW) -> str:
    """Current hash for staged keys. Join a temp view — never an ``IN`` list of every row."""

    _require_catalog(catalog)
    _require_staged_view(staged)
    return (
        "SELECT c.object_kind, c.full_name, c.content_hash "
        f"FROM {catalog}.framework_meta.asset_registry_current c "
        f"INNER JOIN {staged} s "
        "ON c.object_kind = s.object_kind AND c.full_name = s.full_name"
    )


def owners_sql(catalog: str, staged: str = OWNER_NAMES_VIEW) -> str:
    """Current owner of each staged full_name, from the registry.

    Callers must validate and stage names first (``require_safe_names``). Joining a temp
    view avoids megabyte ``IN`` lists and never silently drops an unsafe name.
    """

    _require_catalog(catalog)
    _require_staged_view(staged)
    return (
        f"SELECT c.object_kind, c.full_name, c.owner_kind, c.owner "
        f"FROM {catalog}.framework_meta.v_asset_registry_current c "
        f"INNER JOIN {staged} s ON s.full_name = c.full_name "
        f"WHERE c.object_kind IN ({_sql_in(OWNED_KINDS)})"
    )


def require_safe_names(full_names: list[str]) -> list[str]:
    """Every name must be safe; an unsafe name raises (do not filter it out)."""

    names = [str(name) for name in full_names]
    if not names or any(not _SAFE_NAME.match(name) for name in names):
        raise ValueError("full name is required")
    return names


def _require_catalog(catalog: str) -> None:
    from lakeflow_framework.sqlnames import require_catalog

    require_catalog(catalog)


def _require_staged_view(staged: str) -> None:
    if not re.fullmatch(r"_?[A-Za-z0-9_]+", staged or ""):
        raise ValueError("staged view is required")


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
    content_hashes: list[str],
    full_names: list[str] | None = None,
    **_ignored: Any,
) -> str:
    """Ownership refusals already stored for these content hashes (dedupe key)."""

    _require_catalog(catalog)
    safe_hashes = [value for value in content_hashes if value and len(value) >= 8]
    if not safe_hashes:
        raise ValueError("content hash is required")
    # Hashes are hex; quote safely.
    quoted = ", ".join(f"'{value}'" for value in dict.fromkeys(safe_hashes))
    filters = [f"content_hash IN ({quoted})"]
    if full_names:
        names = require_safe_names(list(full_names))
        filters.append(f"full_name IN ({_sql_in(names)})")
    return (
        f"SELECT object_kind, full_name, content_hash "
        f"FROM {catalog}.framework_meta.ownership_refusals "
        f"WHERE {' AND '.join(filters)}"
    )


def utc_now() -> str:
    return _now()


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
