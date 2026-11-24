"""Pipeline configuration. Validation finishes before any cluster is used."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from lakeflow_framework.sqlnames import IDENTIFIER, TYPE_NAME

# autoloader, copy_into, and ctas land files in a raw table first. volume_files reads files that
# are already in a raw Unity Catalog volume, and database_table reads a database table through a
# foreign catalog. Both go straight to bronze, so they have no raw table.
LOAD_METHODS = ("autoloader", "copy_into", "ctas", "volume_files", "database_table")
RAWLESS_METHODS = ("volume_files", "database_table")
# Kept and tested, but a spec cannot use them until they are run end to end on serverless.
FROZEN_METHODS = ("autoloader", "copy_into", "ctas")
FILE_EVENT_METHODS = ("autoloader", "volume_files")
# Liquid clustering takes up to four keys, and none of these types.
MAX_CLUSTER_KEYS = 4
# How bronze takes its rows. The audit columns a table carries depend on this.
LOAD_TYPES = ("insert_only", "scd1", "scd2", "full_refresh")
MAX_DESCRIPTION_CHARS = 1024

# Audit columns the pipeline adds to bronze and quarantine. They are not data, so none of them
# goes into the row hash.
SRC_FILE_PATH = "_src_file_path"
SRC_FILE_NAME = "_src_file_name"
SRC_ROW_HASH = "_src_row_hash"
BATCH_ID = "_batch_id"
INGEST_TIMESTAMP = "_ingest_timestamp"
IS_DELETED = "_is_deleted"
RESCUED_DATA = "_rescued_data"
LINEAGE_COLUMNS = frozenset(
    {
        SRC_FILE_PATH,
        SRC_FILE_NAME,
        SRC_ROW_HASH,
        BATCH_ID,
        INGEST_TIMESTAMP,
        IS_DELETED,
        RESCUED_DATA,
    }
)
AUDIT_COLUMN_DESCRIPTIONS = {
    SRC_FILE_PATH: "Full path of the source file this row was read from",
    SRC_FILE_NAME: "Name of the source file this row was read from",
    SRC_ROW_HASH: "SHA-256 of the row's data columns, to tell whether a row's content changed",
    BATCH_ID: "Identifier of the pipeline run that wrote this row; joins load_ledger.batch_id",
    INGEST_TIMESTAMP: "When the pipeline wrote this row; silver reads rows newer than its last run",
    IS_DELETED: "True when the source no longer has this row. The row is kept, not removed",
    RESCUED_DATA: "Values that did not fit the declared columns, as JSON",
}
# Pipeline configuration keys. Prefer one compiled manifest (lff.manifest); lff.specs remains
# as a comma-joined fallback for older deploys. Catalog is the environment catalog.
SPECS_SETTING = "lff.specs"
MANIFEST_SETTING = "lff.manifest"
CATALOG_SETTING = "lff.catalog"
# Spec files declare this so validation can evolve across thousands of tables.
SPEC_VERSION = 1
SUPPORTED_SPEC_VERSIONS = frozenset({1})
UNCLUSTERABLE = ("boolean", "array", "map", "struct", "binary")
FILE_KINDS = ("sftp_files", "sharepoint_files")
DATABASE_KIND = "database"
DATABASE_MODES = ("snapshot", "merge")
VOLUME_PATH = re.compile(r"^/Volumes/[\w\-]+/[\w\-]+/[\w\-]+(/.*)?$")
SOURCE_TABLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
FORMATS = (
    "csv",
    "delimited",
    "text",
    "fixed_width",
    "json",
    "parquet",
    "avro",
    "excel",
    "yaml",
)
INFER_SCHEMA_MODES = ("typed", "strings")
# The text formats, where Auto Loader chooses the column types. Parquet and Avro carry their own.
INFER_SCHEMA_FORMATS = frozenset({"csv", "delimited", "json"})
SECRET_PARTS = ("account_key", "access_key", "secret", "password", "token")
UNRESOLVED = re.compile(r"\$\{[^}]+\}")
CLOUD_PATH = re.compile(r"^(abfss|s3)://", re.IGNORECASE)
EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
QUARANTINE_SUFFIX = "_qrntn"
CLASSIFICATIONS = ("internal", "confidential", "highly_confidential")
PII_LABELS = ("pii_health", "pii_basic", "pii_finance", "pii_biometric", "pii_personal")


class ConfigValidationError(Exception):
    """Every problem found in one validation pass."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("\n".join(errors))


@dataclass
class FixedColumn:
    name: str
    start: int
    length: int


@dataclass
class Column:
    """One bronze column. Sensitivity tags fall back to the bronze table when the column omits them."""

    name: str
    type_name: str
    description: str = ""
    classification: str = ""
    pii: list[str] = field(default_factory=list)


# What happens to a row that fails a rule. ``on_fail`` is set in one place only: on
# ``raw_to_bronze`` for all of a table's rules (and the framework's own checks), or on the rules
# themselves. A spec that sets both is refused.
#   quarantine  the row goes to the quarantine table; the good rows load (the default)
#   drop        the row is discarded and kept nowhere
#   warn        the row loads as usual; the failure is only counted in the metrics
#   fail        the update fails, so no row of that run reaches bronze
RULE_ACTIONS = ("quarantine", "drop", "warn", "fail")


@dataclass
class QualityRule:
    name: str
    expression: str
    action: str = "quarantine"


@dataclass
class TableTags:
    """Labels a written table carries. The same labels apply to raw, bronze, and quarantine unless a table overrides them."""

    classification: str
    pii: list[str]


@dataclass
class FileSourceOptions:
    """Typed options for file / volume load strategies. Meaningless for database loads."""

    file_format: str
    path: str
    external_location: str | None = None
    delimiter: str | None = None
    header: bool = True
    header_rows: int | None = None
    data_address: str | None = None
    max_files_per_trigger: int | None = None
    path_glob: str | None = None
    json_multiline: bool = False
    file_events: bool = False
    expect_file: bool = False
    infer_schema: str | None = None
    fixed_columns: list[FixedColumn] = field(default_factory=list)


@dataclass
class DatabaseSourceOptions:
    """Typed options for ``database_table`` loads. Meaningless for file loads."""

    source_catalog: str
    source_table: str
    source_mode: str = "snapshot"


SourceOptions = FileSourceOptions | DatabaseSourceOptions


@dataclass
class PipelineConfig:
    """A bronze load: identity, ownership, target, and quality in the core; source options
    are a discriminated union on ``load_method`` (see ``source_options``).

    Flat file/database fields remain for call sites and the load strategies; they mirror
    ``source_options`` and are empty when that kind does not apply.
    """

    pipeline_id: str
    requested_by: str
    source_system: str
    table: str
    load_method: str
    file_format: str
    path: str
    catalog: str
    layer: str = "bronze"
    raw_schema: str = ""
    bronze_schema: str = ""
    quarantine_schema: str = ""
    raw_table: str = ""
    bronze_table: str = ""
    quarantine_table: str = ""
    external_location: str | None = None
    dedup: bool = False
    primary_keys: list[str] = field(default_factory=list)
    watermark_column: str | None = None
    columns: list[Column] = field(default_factory=list)
    rules: list[QualityRule] = field(default_factory=list)
    on_fail: str = "quarantine"  # raw_to_bronze.on_fail: every rule's action and the added checks'
    fixed_columns: list[FixedColumn] = field(default_factory=list)
    delimiter: str | None = None
    header: bool = True
    header_rows: int | None = None
    data_address: str | None = None
    max_files_per_trigger: int | None = None
    path_glob: str | None = None
    json_multiline: bool = False
    file_events: bool = False
    # Every run is expected to bring a file with rows. A run with no new file, or a file with no
    # rows, is recorded FAILED in the ledger (the job still succeeds). On by default for a table
    # in a scheduled delivery.
    expect_file: bool = False
    # How Auto Loader types the columns of a spec that declares none: "typed" infers real types,
    # "strings" keeps every column a string. None leaves it as before (typed). Meaningless once
    # ``columns`` are declared, and refused then.
    infer_schema: str | None = None
    cluster_by: list[str] = field(default_factory=list)
    load_type: str = "insert_only"
    delete_when: str | None = None
    description: str = ""
    quarantine_description: str = ""
    # The spec has a ``quarantine:`` block (``rules`` and ``on_fail`` count through ``rules``).
    quarantine_requested: bool = False
    job_name: str | None = None
    domain: str = ""
    data_domain: str = ""
    team: str = ""
    technical_owner: str = ""
    notify_failure: list[str] = field(default_factory=list)
    notify_success: list[str] = field(default_factory=list)
    tags: dict[str, TableTags] = field(default_factory=dict)
    document: dict[str, Any] = field(default_factory=dict)
    source_catalog: str = ""
    source_table: str = ""
    source_mode: str = ""
    delivery: str = ""
    source_kind: str = ""  # sftp_files / sharepoint_files / database / …
    run_as: str = ""  # service principal from source.yaml; empty = deployer default
    source_options: SourceOptions | None = None

    @property
    def owner_kind(self) -> str:
        """A bronze load is owned by its source system. Silver, gold, and data products are owned by a project."""

        return "source_system"

    @property
    def owner(self) -> str:
        return self.source_system

    @property
    def has_raw(self) -> bool:
        return self.load_method not in RAWLESS_METHODS

    @property
    def is_batch(self) -> bool:
        return self.load_method in {"ctas", "database_table"}

    @property
    def uses_auto_cdc(self) -> bool:
        return (
            self.dedup
            or self.load_type in {"scd1", "scd2"}
            or (self.load_method == "database_table" and self.source_mode == "merge")
            or self.soft_deletes_rows
        )

    @property
    def soft_deletes_rows(self) -> bool:
        """A database snapshot with keys keeps a key that left the source, closed, not deleted.

        Without keys there is nothing to follow a row by, so the table is replaced each run.
        """

        return (
            self.load_method == "database_table"
            and self.load_type == "full_refresh"
            and bool(self.primary_keys)
        )

    @property
    def scd_type(self) -> int:
        """1 keeps the latest row per key. 2 keeps every version, and closes a removed key."""

        return 2 if self.load_type == "scd2" or self.soft_deletes_rows else 1

    @property
    def sequence_type(self) -> str | None:
        """The declared type of the column a dedup load sequences on. Files land it as a string."""

        if not self.dedup:
            return None
        for column in self.columns:
            if column.name == self.watermark_column:
                return column.type_name
        return None

    def source_reference(self) -> str:
        """The database table a ``database_table`` load reads, as catalog.schema.table."""

        return f"{self.source_catalog}.{self.source_table}"

    @property
    def has_quarantine(self) -> bool:
        """A quarantine table exists when the spec has a quarantine block, rules, or on_fail
        (which needs rules, so it is covered by them).

        With neither, the framework's own checks (casts, keys, schema conformance) drop a bad row
        and count it in ``rows_dropped``: there is no quarantine logic for the table at all.
        """

        return bool(self.rules) or self.quarantine_requested

    @property
    def layers(self) -> tuple[str, ...]:
        written = ("raw", "bronze") if self.has_raw else ("bronze",)
        return (*written, "quarantine") if self.has_quarantine else written

    def layer_schema(self, layer: str) -> str:
        return {
            "raw": self.raw_schema,
            "bronze": self.bronze_schema,
            "quarantine": self.quarantine_schema,
        }[layer]

    def table_name(self, layer: str) -> str:
        if layer == "raw" and not self.has_raw:
            raise ValueError(f"{self.pipeline_id} has no raw table; its raw layer is the source")
        table = {
            "raw": self.raw_table,
            "bronze": self.bronze_table,
            "quarantine": self.quarantine_table,
        }[layer]
        return f"{self.catalog}.{self.layer_schema(layer)}.{table}"

    def canonical(self) -> dict[str, Any]:
        if self.load_method == "database_table":
            source: dict[str, Any] = {
                "system": self.source_system,
                "catalog": self.source_catalog,
                "table": self.source_table,
                "mode": self.source_mode,
            }
        else:
            source = {
                "system": self.source_system,
                "format": self.file_format,
                "path": self.path,
                "header": self.header,
            }
            _put(source, "external_location", self.external_location)
            _put(source, "delimiter", self.delimiter)
            _put(source, "header_rows", self.header_rows)
            _put(source, "data_address", self.data_address)
            _put(source, "max_files_per_trigger", self.max_files_per_trigger)
            _put(source, "path_glob", self.path_glob)
            _put(source, "multiline", True if self.json_multiline else None)
            _put(source, "file_events", True if self.file_events else None)
            _put(source, "expect_file", True if self.expect_file else None)
            _put(source, "infer_schema", self.infer_schema)
            if self.fixed_columns:
                source["fixed_columns"] = [
                    {"name": column.name, "start": column.start, "length": column.length}
                    for column in self.fixed_columns
                ]
        source_to_raw: dict[str, Any] = {"method": self.load_method, "source": source}
        if self.has_raw:
            source_to_raw["raw"] = _table_document(
                self.raw_schema, self.raw_table, self.tags["raw"]
            )
        document = {
            "spec_version": SPEC_VERSION,
            "pipeline_id": self.pipeline_id,
            "requested_by": self.requested_by,
            "domain": self.domain,
            "data_domain": self.data_domain,
            "team": self.team,
            "technical_owner": self.technical_owner,
            "layer": self.layer,
            "catalog": self.catalog,
            "notify": {
                "on_failure": list(self.notify_failure),
                "on_success": list(self.notify_success),
            },
            "source_to_raw": source_to_raw,
            "raw_to_bronze": {
                "bronze": _table_document(
                    self.bronze_schema, self.bronze_table, self.tags["bronze"]
                ),
                "dedup": self.dedup,
                "primary_keys": list(self.primary_keys),
                "columns": [_column_document(column) for column in self.columns],
                "rules": [
                    {
                        "name": rule.name,
                        "expression": rule.expression,
                        # Only when it differs from the table's setting. A rule differs only
                        # when the table sets nothing (the two are never both set), so this
                        # document is always valid to read back.
                        **({"on_fail": rule.action} if rule.action != self.on_fail else {}),
                    }
                    for rule in self.rules
                ],
            },
        }
        if self.has_quarantine:
            # Written only when the table exists, so reading this document back (the manifest)
            # does not turn a spec without rules into one that asked for quarantine.
            document["quarantine"] = _table_document(
                self.quarantine_schema, self.quarantine_table, self.tags["quarantine"]
            )
            _put(document["quarantine"], "description", self.quarantine_description or None)
        _put(document, "job_name", self.job_name)
        _put(document, "delivery", self.delivery or None)
        _put(document["raw_to_bronze"], "watermark_column", self.watermark_column)
        if self.cluster_by:
            document["raw_to_bronze"]["bronze"]["cluster_by"] = list(self.cluster_by)
        document["raw_to_bronze"]["load_type"] = self.load_type
        if self.on_fail != "quarantine":
            document["raw_to_bronze"]["on_fail"] = self.on_fail
        _put(document["raw_to_bronze"], "delete_when", self.delete_when)
        _put(document, "description", self.description or None)
        return document

    def content_hash(self) -> str:
        payload = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def read_mapping(path: Path, errors: list[str]) -> dict[str, Any] | None:
    """The YAML mapping in ``path``, or None with the problem appended to ``errors``."""

    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        errors.append(f"{path}: {exc}")
        return None
    if not isinstance(document, dict):
        errors.append(f"{path}: the document must be a mapping")
        return None
    return document


def validate_configs(
    paths: list[Path], catalog: str | None = None, *, allow_frozen: bool = False
) -> list[PipelineConfig]:
    """Validate spec files. ``catalog`` sets the environment's catalog for every spec, so a
    spec does not hard-code dev or prod. Without it each spec must name its own catalog.

    Precedence when composing: table entry beats ``extends`` profile beats ``source.yaml`` /
    folder beats framework default. A file with a ``tables:`` list expands to one config per
    entry. ``allow_frozen`` admits the FROZEN_METHODS (tests / consumer reference only).
    """

    errors: list[str] = []
    configs: list[PipelineConfig] = []
    for path in paths:
        document = read_mapping(path, errors)
        if document is None:
            continue
        documents, expand_errors = expand_spec_documents(document, path)
        errors.extend(expand_errors)
        for document in documents:
            if catalog:
                document = {**document, "catalog": catalog}
            document, profile_errors = apply_extends(document, path)
            errors.extend(profile_errors)
            if profile_errors:
                continue
            if "source" in document and "source_to_raw" not in document:
                source_file = path.parent / "source.yaml"
                if not source_file.is_file():
                    source_file = path.parent.parent / "source.yaml"
                document, composed = compose_document(document, source_file, str(path), path)
                if composed:
                    errors.extend(composed)
                    continue
            found, parsed = _validate_one(document, str(path))
            errors.extend(found)
            if parsed is not None and parsed.load_method in FROZEN_METHODS and not allow_frozen:
                errors.append(
                    f"{path}: {parsed.load_method} is not in the MVP; "
                    "land files in a volume (volume_files) or read a database (database_table)"
                )
            elif parsed is not None:
                configs.append(parsed)
    errors.extend(_target_conflicts(configs))
    if errors:
        raise ConfigValidationError(errors)
    return configs


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    """Merge mappings; override wins. Nested dicts merge; lists and scalars replace."""

    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, Mapping):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def apply_extends(document: dict[str, Any], spec_path: Path) -> tuple[dict[str, Any], list[str]]:
    """Apply ``extends: profiles/...`` before source.yaml inheritance. Spec keys win."""

    extends = document.get("extends")
    if extends is None:
        return document, []
    if not isinstance(extends, str) or not extends.strip():
        return document, [f"{spec_path}: extends must be a profile path like profiles/name"]
    # Resolve from the ingestion bundle root (folder that holds sources/ and profiles/).
    cursor = spec_path.resolve().parent
    bundle_root = None
    for _ in range(6):
        if (cursor / "sources").is_dir() or (cursor / "profiles").is_dir():
            bundle_root = cursor
            break
        if cursor.parent == cursor:
            break
        cursor = cursor.parent
    if bundle_root is None:
        return document, [f"{spec_path}: could not find bundle root for extends {extends}"]
    relative = extends if extends.endswith((".yaml", ".yml")) else f"{extends}.yaml"
    profile_path = bundle_root / relative
    if not profile_path.is_file():
        return document, [f"{spec_path}: extends profile not found: {extends}"]
    profile_errors: list[str] = []
    profile = read_mapping(profile_path, profile_errors)
    if profile_errors:
        return document, profile_errors
    if profile is None:
        return document, [f"{spec_path}: extends profile is not a mapping: {extends}"]
    without = {key: value for key, value in document.items() if key != "extends"}
    # Profile under, table on top — table entry beats profile.
    return deep_merge(profile, without), []


_TABLE_SHORTCUTS = frozenset(
    {"name", "pipeline_id", "primary_keys", "columns", "mode", "pii", "classification"}
)


def expand_spec_documents(
    document: dict[str, Any], path: Path
) -> tuple[list[dict[str, Any]], list[str]]:
    """Expand a ``tables:`` group file into one document per table entry.

    Shortcuts on each entry (``primary_keys``, ``mode``, ``pii``, …) map into the usual
    nested sections; any other key deep-merges onto the shared document. Table entry wins.
    """

    tables = document.get("tables")
    if tables is None:
        return [document], []
    if not isinstance(tables, list) or not tables:
        return [], [f"{path}: tables must be a non-empty list"]
    shared = {key: value for key, value in document.items() if key != "tables"}
    expanded: list[dict[str, Any]] = []
    errors: list[str] = []
    for index, entry in enumerate(tables):
        if not isinstance(entry, dict):
            errors.append(f"{path}: tables[{index}] must be a mapping")
            continue
        name = str(entry.get("name") or entry.get("pipeline_id") or "")
        if not name:
            errors.append(f"{path}: tables[{index}] needs name or pipeline_id")
            continue
        override = {key: value for key, value in entry.items() if key not in _TABLE_SHORTCUTS}
        if "pipeline_id" in entry:
            override["pipeline_id"] = entry["pipeline_id"]
        item = deep_merge(shared, override)
        if "pipeline_id" not in item:
            if (path.parent / "source.yaml").is_file():
                source_system = path.parent.name
            elif (path.parent.parent / "source.yaml").is_file():
                source_system = path.parent.parent.name
            else:
                source_system = path.parent.name
            item["pipeline_id"] = (
                name if name.startswith(f"{source_system}_") else f"{source_system}_{name}"
            )
        # Default bronze (and quarantine) table name to the short ``name`` when omitted.
        short_name = str(entry.get("name") or "")
        if short_name and IDENTIFIER.match(short_name):
            bronze_block = dict((item.get("raw_to_bronze") or {}).get("bronze") or {})
            if "table" not in bronze_block:
                bronze_block["table"] = short_name
                raw_to_bronze = dict(item.get("raw_to_bronze") or {})
                raw_to_bronze["bronze"] = bronze_block
                item["raw_to_bronze"] = raw_to_bronze
        if any(key in entry for key in ("primary_keys", "columns")):
            bronze = dict(item.get("raw_to_bronze") or {})
            if "primary_keys" in entry:
                bronze["primary_keys"] = entry["primary_keys"]
            if "columns" in entry:
                bronze["columns"] = entry["columns"]
            item["raw_to_bronze"] = bronze
        if "mode" in entry:
            source = dict(item.get("source") or {})
            source["mode"] = entry["mode"]
            item["source"] = source
        if "pii" in entry or "classification" in entry:
            data = dict(item.get("data") or {})
            if "pii" in entry:
                data["pii"] = entry["pii"]
            if "classification" in entry:
                data["classification"] = entry["classification"]
            item["data"] = data
        expanded.append(item)
    return expanded, errors


def inherit_from_source(
    spec: dict[str, Any], source: Mapping[str, Any], source_file: Path, spec_path: Path | None
) -> dict[str, Any]:
    """Fill in what a table spec would only repeat from its source system or its folder.

    From the source system: domain, owner as technical_owner, the only delivery and the job it
    makes. From the folder: requested_by, and team as that requester. Nothing about a file or its
    sensitivity is shared, because one source system can deliver files of different kinds. A spec
    that states a value keeps it.
    """

    merged = dict(spec)
    system = str(source.get("source_system") or "")

    def fill(key: str, value: Any) -> None:
        if value and not merged.get(key):
            merged[key] = value

    fill("domain", source.get("domain"))
    fill("layer", "bronze")
    fill("technical_owner", source.get("owner"))
    if (
        spec_path is not None
        and not merged.get("project")
        and spec_path.resolve().parent.parent == source_file.resolve().parent
    ):
        fill("requested_by", spec_path.resolve().parent.name)
    fill("team", merged.get("requested_by") or system)
    deliveries = source.get("deliveries")
    if isinstance(deliveries, dict) and len(deliveries) == 1:
        fill("delivery", next(iter(deliveries)))
    if merged.get("delivery") and system:
        fill("job_name", f"{system}_{merged['delivery']}")
    return merged


def compose_document(
    spec: dict[str, Any], source_file: Path, label: str, spec_path: Path | None = None
) -> tuple[dict[str, Any], list[str]]:
    """Join a table spec with its source system's ``source.yaml`` into one pipeline document.

    The spec holds what is true of the table: its files (``source.volume``, ``raw_path``, format)
    or its database table. The kind, the raw volume schema, and the foreign catalog are facts of
    the source system, so they live once in ``source.yaml``. A source.yaml may also name a default
    volume, which a spec uses when it does not name its own.
    """

    if not source_file.exists():
        return spec, [f"{label}: {source_file} is missing; a spec with a source section needs it"]
    try:
        source = yaml.safe_load(source_file.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        return spec, [f"{label}: {source_file}: {exc}"]
    authored = spec.get("source")
    if not isinstance(authored, dict) or not isinstance(source, dict):
        return spec, [f"{label}: source must be a mapping"]
    kind = str(source.get("kind") or "")
    system = str(source.get("source_system") or "")
    catalog = str(spec.get("catalog") or "")
    spec = inherit_from_source(spec, source, source_file, spec_path)
    document = {key: value for key, value in spec.items() if key != "source"}
    authored = dict(spec["source"])
    if kind in FILE_KINDS:
        volume = source.get("raw_volume") if isinstance(source.get("raw_volume"), dict) else {}
        schema = str(volume.get("schema") or "")
        # The volume sits next to the table's other file settings; source.yaml only names the schema
        # (and optionally a default volume for tables that do not name their own).
        name = str(authored.pop("volume", "") or volume.get("volume") or "")
        raw_path = str(authored.pop("raw_path", "") or "").strip("/")
        problems = []
        if not IDENTIFIER.match(schema):
            problems.append(f"{label}: {source_file} needs raw_volume with a schema")
        if not IDENTIFIER.match(name):
            problems.append(
                f"{label}: source.volume is required (the volume in {schema or 'the raw schema'})"
            )
        if raw_path.lower().startswith("volumes/") or "//" in raw_path:
            problems.append(
                f"{label}: source.raw_path is a folder inside the volume, not a full path; "
                "leave it out when the files are in the volume itself"
            )
        if not catalog:
            problems.append(f"{label}: catalog is required")
        elif not IDENTIFIER.match(catalog):
            problems.append(f"{label}: catalog must be an identifier")
        if problems:
            return spec, problems
        path = f"/Volumes/{catalog}/{schema}/{name}/" + (f"{raw_path}/" if raw_path else "")
        # A scheduled delivery runs whether or not a file came, so it expects one each run unless
        # the table says otherwise. A delivery fired by a file arriving always has one.
        deliveries = source.get("deliveries") if isinstance(source.get("deliveries"), dict) else {}
        detail = deliveries.get(str(spec.get("delivery") or "")) or {}
        if isinstance(detail, dict) and detail.get("schedule") and "expect_file" not in authored:
            authored["expect_file"] = True
        source_section = {"system": system, "path": path, **authored}
        document["source_to_raw"] = {"method": "volume_files", "source": source_section}
    elif kind == DATABASE_KIND:
        document["source_to_raw"] = {
            "method": "database_table",
            "source": {
                "system": system,
                "catalog": source.get("foreign_catalog"),
                "table": authored.get("table"),
                "mode": authored.get("mode", "snapshot"),
            },
        }
    else:
        kinds = ", ".join([*FILE_KINDS, DATABASE_KIND])
        return spec, [f"{label}: {source_file} kind must be one of {kinds} for a source section"]
    document["source_kind"] = kind
    document["run_as"] = str(source.get("run_as") or "").strip()
    return document, []


def email_notifications(configs: list[PipelineConfig]) -> dict[str, list[str]]:
    """Addresses a Databricks job should mail. One domain can hold many projects."""

    failure: list[str] = []
    success: list[str] = []
    for config in configs:
        for address in config.notify_failure:
            if address not in failure:
                failure.append(address)
        for address in config.notify_success:
            if address not in success:
                success.append(address)
    notes: dict[str, list[str]] = {}
    if failure:
        notes["on_failure"] = failure
    if success:
        notes["on_success"] = success
    return notes


def config_from_document(document: Mapping[str, Any]) -> PipelineConfig:
    errors, parsed = _validate_one(dict(document), document.get("pipeline_id", "document"))
    if errors or parsed is None:
        raise ConfigValidationError(errors or ["document is not a valid pipeline"])
    return parsed


def _validate_one(document: dict[str, Any], label: str) -> tuple[list[str], PipelineConfig | None]:
    errors: list[str] = []
    _reject_secrets(document, label, errors)
    _reject_unresolved(document, label, errors)

    pipeline_id = _identifier(document, "pipeline_id", label, errors)
    version = document.get("spec_version", SPEC_VERSION)
    try:
        version = int(version)
    except (TypeError, ValueError):
        errors.append(f"{label}: spec_version must be an integer")
        version = None
    if version is not None and version not in SUPPORTED_SPEC_VERSIONS:
        errors.append(
            f"{label}: spec_version {version} is not supported "
            f"(supported: {sorted(SUPPORTED_SPEC_VERSIONS)})"
        )
    # requested_by is the project that brought the table in. Older specs call it project.
    # Omitted when the spec sits directly under the source system.
    requested_by = _optional_text(document.get("requested_by")) or _optional_text(
        document.get("project")
    )
    if requested_by and not IDENTIFIER.match(requested_by):
        errors.append(f"{label}: requested_by must be an identifier")
        requested_by = None
    elif not requested_by:
        requested_by = ""
    catalog = str(document.get("catalog") or "")
    if not catalog:
        errors.append(f"{label}: catalog is required")
    elif not IDENTIFIER.match(catalog):
        errors.append(f"{label}: catalog must be an identifier")
    layer = document.get("layer")
    if layer != "bronze":
        if layer in {"silver", "gold", "product"}:
            errors.append(f"{label}: {layer} loads are not implemented; layer must be bronze")
        else:
            errors.append(f"{label}: layer must be bronze")

    source_to_raw = _section(document, "source_to_raw", label, errors)
    raw_to_bronze = _section(document, "raw_to_bronze", label, errors)
    # Everything in quarantine has a default now, so the block may be left out.
    quarantine = (
        {}
        if document.get("quarantine") is None
        else _section(document, "quarantine", label, errors)
    )
    source = _section(source_to_raw, "source", f"{label}: source_to_raw", errors)
    load_method = str(source_to_raw.get("method") or "autoloader")
    has_raw = load_method not in RAWLESS_METHODS
    raw_target = (
        _section(source_to_raw, "raw", f"{label}: source_to_raw", errors) if has_raw else {}
    )
    bronze_target = _section(raw_to_bronze, "bronze", f"{label}: raw_to_bronze", errors)

    if source_to_raw and load_method not in LOAD_METHODS:
        errors.append(f"{label}: source_to_raw.method must be one of {', '.join(LOAD_METHODS)}")
    is_database = load_method == "database_table"
    source_system = (
        _identifier(source, "system", f"{label}: source_to_raw.source", errors) if source else None
    )

    file_options: FileSourceOptions | None = None
    database_options: DatabaseSourceOptions | None = None
    if source and is_database:
        database_options = _parse_database_source(source, label, errors)
        _reject_file_only_keys(source, label, errors)
    elif source:
        file_options = _parse_file_source(source, load_method, label, errors)
        _reject_database_only_keys(source, label, errors)

    file_format = file_options.file_format if file_options else ""
    path = file_options.path if file_options else ""
    source_catalog = database_options.source_catalog if database_options else ""
    source_table = database_options.source_table if database_options else ""
    source_mode = database_options.source_mode if database_options else ""

    raw_table = _target_table(raw_target, "raw", pipeline_id, label, errors) if has_raw else ""
    bronze_table = _target_table(bronze_target, "bronze", raw_table or pipeline_id, label, errors)
    quarantine_table = _quarantine_table(quarantine, bronze_table, label, errors)
    raw_schema = _target_schema(raw_target, "raw", source_system, label, errors) if has_raw else ""
    bronze_schema = _target_schema(bronze_target, "bronze", source_system, label, errors)
    # Quarantine sits beside bronze unless the spec names another schema.
    quarantine_schema = _target_schema(
        quarantine, "quarantine", source_system, label, errors, default=bronze_schema
    )

    dedup = bool(raw_to_bronze.get("dedup", False))
    load_type = _load_type(
        raw_to_bronze.get("load_type"), dedup, is_database, source_mode, label, errors
    )
    # scd1 and scd2 file loads are the dedup path: they upsert on primary_keys by watermark_column.
    dedup = dedup or (not is_database and load_type in {"scd1", "scd2"})
    primary_keys = _name_list(raw_to_bronze.get("primary_keys"), "primary_keys", label, errors)
    watermark = raw_to_bronze.get("watermark_column")
    if watermark is not None and not IDENTIFIER.match(str(watermark)):
        errors.append(f"{label}: watermark_column must be an identifier")
    if dedup and (not primary_keys or not watermark):
        errors.append(f"{label}: dedup requires primary_keys and watermark_column")
    if is_database and dedup:
        errors.append(f"{label}: a database table uses source.mode merge, not dedup")
    if is_database and source_mode == "merge" and not primary_keys:
        errors.append(f"{label}: source.mode merge requires primary_keys")

    domain, data_domain, team, technical_owner = _ownership(document, label, errors)
    notify_failure, notify_success = _notify(document, technical_owner, label, errors)
    tags = _tags(document, raw_target, bronze_target, quarantine, has_raw, label, errors)
    columns = _columns(raw_to_bronze.get("columns"), tags.get("bronze"), label, errors)
    if dedup and watermark and str(watermark) not in {column.name for column in columns}:
        errors.append(
            f"{label}: dedup sequences on watermark_column '{watermark}', "
            "so it must be in columns with its type"
        )
    fixed_columns = file_options.fixed_columns if file_options else []
    if file_options and file_options.infer_schema and (columns or fixed_columns):
        errors.append(
            f"{label}: infer_schema is for a spec that declares no columns; "
            "remove it, or remove the columns"
        )
    rule_columns = (
        {column.name for column in columns}
        | {column.name for column in fixed_columns}
        | set(primary_keys)
        | ({str(watermark)} if watermark else set())
        | LINEAGE_COLUMNS
    )
    on_fail = _table_on_fail(raw_to_bronze, label, errors)
    rules = _rules(
        raw_to_bronze.get("rules"),
        label,
        errors,
        columns=rule_columns,
        default_action=on_fail,
    )
    cluster_by = _cluster_by(bronze_target.get("cluster_by"), columns, label, errors)
    delete_when = _delete_when(
        raw_to_bronze.get("delete_when"), load_type, rule_columns, label, errors
    )
    description = _description(document.get("description"), "description", label, errors)
    quarantine_description = _description(
        quarantine.get("description"), "quarantine.description", label, errors
    )

    if (
        errors
        or not pipeline_id
        or requested_by is None
        or not source_system
        or (has_raw and not (raw_table and raw_schema))
        or not bronze_table
        or not quarantine_table
        or not bronze_schema
        or not quarantine_schema
        or not domain
        or not data_domain
        or not team
        or not technical_owner
        or any(name not in tags for name in (["raw"] if has_raw else []) + ["bronze", "quarantine"])
        or (is_database and database_options is None)
        or (not is_database and file_options is None)
    ):
        return errors, None

    source_options: SourceOptions = (
        database_options if is_database else file_options  # type: ignore[assignment]
    )
    parsed = PipelineConfig(
        pipeline_id=pipeline_id,
        requested_by=requested_by,
        source_system=source_system,
        source_kind=str(document.get("source_kind") or ""),
        run_as=str(document.get("run_as") or "").strip(),
        table=bronze_table,
        load_method=load_method,
        file_format=file_format,
        path=path,
        catalog=catalog,
        layer="bronze",
        raw_schema=raw_schema,
        bronze_schema=bronze_schema,
        quarantine_schema=quarantine_schema,
        raw_table=raw_table,
        bronze_table=bronze_table,
        quarantine_table=quarantine_table,
        external_location=file_options.external_location if file_options else None,
        dedup=dedup,
        primary_keys=primary_keys,
        watermark_column=str(watermark) if watermark else None,
        columns=columns,
        rules=rules,
        fixed_columns=fixed_columns,
        delimiter=file_options.delimiter if file_options else None,
        header=file_options.header if file_options else True,
        header_rows=file_options.header_rows if file_options else None,
        data_address=file_options.data_address if file_options else None,
        max_files_per_trigger=(file_options.max_files_per_trigger if file_options else None),
        path_glob=file_options.path_glob if file_options else None,
        json_multiline=file_options.json_multiline if file_options else False,
        file_events=file_options.file_events if file_options else False,
        expect_file=file_options.expect_file if file_options else False,
        infer_schema=file_options.infer_schema if file_options else None,
        cluster_by=cluster_by,
        on_fail=on_fail,
        load_type=load_type,
        delete_when=delete_when,
        description=description,
        quarantine_description=quarantine_description,
        # on_fail, on the table or a rule, always comes with rules, which already ask for one.
        quarantine_requested=document.get("quarantine") is not None,
        job_name=_optional_text(document.get("job_name")),
        domain=domain,
        data_domain=data_domain,
        team=team,
        technical_owner=technical_owner,
        notify_failure=notify_failure,
        notify_success=notify_success,
        tags=tags,
        document=document,
        source_catalog=source_catalog,
        source_table=source_table,
        source_mode=source_mode,
        delivery=_optional_text(document.get("delivery")) or "",
        source_options=source_options,
    )
    return errors, parsed


_FILE_ONLY_KEYS = frozenset(
    {
        "format",
        "path",
        "delimiter",
        "header",
        "header_rows",
        "data_address",
        "path_glob",
        "multiline",
        "file_events",
        "expect_file",
        "infer_schema",
        "max_files_per_trigger",
        "fixed_columns",
        "external_location",
    }
)
_DATABASE_ONLY_KEYS = frozenset({"catalog", "table", "mode"})


def _reject_file_only_keys(source: Mapping[str, Any], label: str, errors: list[str]) -> None:
    found = sorted(
        key for key in _FILE_ONLY_KEYS if key in source and source[key] not in (None, "")
    )
    if found:
        errors.append(f"{label}: database source must not set file options {', '.join(found)}")


def _reject_database_only_keys(source: Mapping[str, Any], label: str, errors: list[str]) -> None:
    # ``catalog`` / ``table`` / ``mode`` are database-only in composed source_to_raw.
    found = sorted(
        key for key in _DATABASE_ONLY_KEYS if key in source and source[key] not in (None, "")
    )
    if found:
        errors.append(f"{label}: file source must not set database options {', '.join(found)}")


def _parse_database_source(
    source: Mapping[str, Any], label: str, errors: list[str]
) -> DatabaseSourceOptions | None:
    source_catalog = str(source.get("catalog") or "")
    source_table = str(source.get("table") or "")
    source_mode = str(source.get("mode") or "snapshot")
    ok = True
    if not IDENTIFIER.match(source_catalog):
        errors.append(f"{label}: the source system needs a foreign_catalog identifier")
        ok = False
    if not SOURCE_TABLE.match(source_table):
        errors.append(f"{label}: source.table must be <schema>.<table> in the database")
        ok = False
    if source_mode not in DATABASE_MODES:
        errors.append(f"{label}: source.mode must be one of {', '.join(DATABASE_MODES)}")
        ok = False
    if not ok:
        return None
    return DatabaseSourceOptions(source_catalog, source_table, source_mode)


def _parse_file_source(
    source: Mapping[str, Any], load_method: str, label: str, errors: list[str]
) -> FileSourceOptions | None:
    file_format = str(source.get("format") or "")
    path = str(source.get("path") or "")
    ok = True
    if file_format not in FORMATS:
        errors.append(f"{label}: format must be one of {', '.join(FORMATS)}")
        ok = False
    if file_format == "excel" and load_method == "copy_into":
        errors.append(f"{label}: excel cannot use copy_into")
        ok = False
    if load_method == "volume_files" and file_format in {"fixed_width", "yaml"}:
        errors.append(f"{label}: {file_format} is not supported from a raw volume yet")
        ok = False
    if not path:
        errors.append(f"{label}: path is required")
        ok = False
    elif load_method == "volume_files" and not VOLUME_PATH.match(path):
        errors.append(f"{label}: path must be a /Volumes/<catalog>/<schema>/<volume> path")
        ok = False
    elif load_method != "volume_files" and not _storage_path(path):
        errors.append(f"{label}: path must start with abfss:// or s3://, or be a /Volumes path")
        ok = False

    fixed_columns = _fixed_columns(source.get("fixed_columns"), label, errors)
    if file_format == "fixed_width" and not fixed_columns:
        errors.append(f"{label}: fixed_width requires fixed_columns")
        ok = False

    delimiter = source.get("delimiter")
    if file_format in {"csv", "delimited"} and not delimiter:
        errors.append(f"{label}: delimiter is required for {file_format}")
        ok = False
    elif delimiter and ("'" in str(delimiter) or len(str(delimiter)) > 3):
        errors.append(f"{label}: delimiter must be a short literal")
        ok = False

    header_rows = source.get("header_rows")
    data_address = source.get("data_address")
    if file_format == "excel":
        if header_rows not in (0, 1):
            errors.append(f"{label}: header_rows must be 0 or 1 for excel")
            ok = False
        if not data_address:
            errors.append(f"{label}: data_address is required for excel")
            ok = False

    multiline = source.get("multiline", False)
    if not isinstance(multiline, bool):
        errors.append(f"{label}: multiline must be true or false")
        multiline = False
        ok = False

    file_events = source.get("file_events", False)
    if not isinstance(file_events, bool):
        errors.append(f"{label}: file_events must be true or false")
        file_events = False
        ok = False
    elif file_events and load_method not in FILE_EVENT_METHODS:
        errors.append(f"{label}: file_events applies only to files read by Auto Loader")
        ok = False

    expect_file = source.get("expect_file", False)
    if not isinstance(expect_file, bool):
        errors.append(f"{label}: expect_file must be true or false")
        expect_file = False
        ok = False

    infer_schema = source.get("infer_schema")
    if infer_schema is not None:
        if infer_schema not in INFER_SCHEMA_MODES:
            errors.append(f"{label}: infer_schema must be one of {', '.join(INFER_SCHEMA_MODES)}")
            ok = False
        elif file_format not in INFER_SCHEMA_FORMATS:
            errors.append(
                f"{label}: infer_schema applies to {', '.join(sorted(INFER_SCHEMA_FORMATS))}; "
                f"{file_format} carries its own column types"
            )
            ok = False

    max_files = source.get("max_files_per_trigger")
    max_files_value: int | None = None
    if max_files is not None:
        try:
            max_files_value = int(max_files)
            if max_files_value <= 0:
                errors.append(f"{label}: max_files_per_trigger must be greater than 0")
                ok = False
                max_files_value = None
        except (TypeError, ValueError):
            errors.append(f"{label}: max_files_per_trigger must be an integer")
            ok = False

    if not ok:
        return None
    return FileSourceOptions(
        file_format=file_format,
        path=path,
        external_location=_optional_text(source.get("external_location")),
        delimiter=str(delimiter) if delimiter else None,
        header=bool(source.get("header", True)),
        header_rows=int(header_rows) if header_rows in (0, 1) else None,
        data_address=str(data_address) if data_address else None,
        max_files_per_trigger=max_files_value,
        path_glob=_optional_text(source.get("path_glob")),
        json_multiline=multiline,
        file_events=file_events,
        expect_file=expect_file,
        infer_schema=str(infer_schema) if infer_schema else None,
        fixed_columns=fixed_columns,
    )


def _table_document(schema: str, table: str, tags: TableTags) -> dict[str, Any]:
    return {
        "schema": schema,
        "table": table,
        "classification": tags.classification,
        "pii": list(tags.pii),
    }


def _column_document(column: Column) -> dict[str, Any]:
    return {
        "name": column.name,
        "type": column.type_name,
        "description": column.description,
        "classification": column.classification,
        "pii": list(column.pii),
    }


def _ownership(
    document: Mapping[str, Any], label: str, errors: list[str]
) -> tuple[str | None, str | None, str | None, str | None]:
    return (
        _identifier(document, "domain", label, errors),
        _identifier(document, "data_domain", label, errors),
        _identifier(document, "team", label, errors),
        _email(document.get("technical_owner"), "technical_owner", label, errors),
    )


def _notify(
    document: Mapping[str, Any],
    technical_owner: str | None,
    label: str,
    errors: list[str],
) -> tuple[list[str], list[str]]:
    raw = document.get("notify", {})
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        errors.append(f"{label}: notify must be a mapping")
        return [], []
    if "on_failure" in raw:
        failure = _email_list(raw.get("on_failure"), "notify.on_failure", label, errors, True)
    else:
        failure = [technical_owner] if technical_owner else []
    if "on_success" in raw:
        success = _email_list(raw.get("on_success"), "notify.on_success", label, errors, False)
    else:
        success = []
    return failure, success


def _tags(
    document: Mapping[str, Any],
    raw_target: Mapping[str, Any],
    bronze_target: Mapping[str, Any],
    quarantine: Mapping[str, Any],
    has_raw: bool,
    label: str,
    errors: list[str],
) -> dict[str, TableTags]:
    shared = document.get("data", {})
    if not isinstance(shared, dict):
        errors.append(f"{label}: data must be a mapping")
        shared = {}
    found: dict[str, TableTags] = {}
    layers = [("bronze", bronze_target), ("quarantine", quarantine)]
    if has_raw:
        layers.insert(0, ("raw", raw_target))
    for layer, section in layers:
        if layer == "quarantine" and not section and "classification" not in shared:
            # No quarantine block and no shared labels (a manifest copy of a table without
            # quarantine): it has bronze's labels, as an omitted block always meant.
            if "bronze" in found:
                found[layer] = found["bronze"]
            continue
        tags = _layer_tags(shared, section, f"{label}: {layer}", errors)
        if tags is not None:
            found[layer] = tags
    return found


def _layer_tags(
    shared: Mapping[str, Any],
    section: Mapping[str, Any],
    label: str,
    errors: list[str],
) -> TableTags | None:
    classification = section.get("classification", shared.get("classification"))
    pii_value = section["pii"] if "pii" in section else shared.get("pii")
    kind = _classification(classification, label, errors)
    pii = _pii(pii_value, label, errors)
    if kind is None:
        return None
    return TableTags(kind, pii)


def _classification(value: Any, label: str, errors: list[str]) -> str | None:
    if value is None or str(value).strip() == "":
        errors.append(
            f"{label}: classification is required and must be one of {', '.join(CLASSIFICATIONS)}"
        )
        return None
    text = str(value)
    if text not in CLASSIFICATIONS:
        errors.append(f"{label}: classification must be one of {', '.join(CLASSIFICATIONS)}")
        return None
    return text


def _pii(value: Any, label: str, errors: list[str]) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{label}: pii must be a list")
        return []
    names: list[str] = []
    for item in value:
        text = str(item)
        if text not in PII_LABELS:
            errors.append(f"{label}: pii value '{text}' must be one of {', '.join(PII_LABELS)}")
        else:
            names.append(text)
    return names


def _email(value: Any, key: str, label: str, errors: list[str]) -> str | None:
    if value is None or str(value).strip() == "":
        errors.append(f"{label}: {key} is required")
        return None
    text = str(value).strip()
    if not EMAIL.match(text):
        errors.append(f"{label}: {key} must be an email address")
        return None
    return text


def _email_list(value: Any, key: str, label: str, errors: list[str], required: bool) -> list[str]:
    if value is None:
        value = []
    if not isinstance(value, list):
        errors.append(f"{label}: {key} must be a list of emails")
        return []
    if required and not value:
        errors.append(f"{label}: {key} needs at least one email")
        return []
    emails: list[str] = []
    for item in value:
        text = str(item).strip()
        if not EMAIL.match(text):
            errors.append(f"{label}: {key} entry '{item}' must be an email address")
        else:
            emails.append(text)
    return emails


def _storage_path(path: str) -> bool:
    """Cloud URI or a Unity Catalog volume. Auto Loader, COPY INTO, and read_files accept both."""

    return bool(CLOUD_PATH.match(path) or VOLUME_PATH.match(path))


def _put(mapping: dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        mapping[key] = value


def _section(
    document: Mapping[str, Any], key: str, label: str, errors: list[str]
) -> dict[str, Any]:
    value = document.get(key)
    if not isinstance(value, dict):
        errors.append(f"{label}: {key} is required")
        return {}
    return value


def _target_table(
    section: Mapping[str, Any],
    layer: str,
    default: str | None,
    label: str,
    errors: list[str],
) -> str | None:
    table = section.get("table") or default
    if not table:
        errors.append(f"{label}: {layer} table is required")
        return None
    if not IDENTIFIER.match(str(table)):
        errors.append(f"{label}: {layer} table must be an identifier")
        return None
    return str(table)


def _quarantine_table(
    section: Mapping[str, Any], bronze_table: str | None, label: str, errors: list[str]
) -> str | None:
    """The bronze table name plus the suffix, unless the spec names its own quarantine table."""

    if not bronze_table:
        return None
    return _target_table(section, "quarantine", f"{bronze_table}{QUARANTINE_SUFFIX}", label, errors)


def _target_schema(
    section: Mapping[str, Any],
    layer: str,
    source_system: str | None,
    label: str,
    errors: list[str],
    default: str | None = None,
) -> str | None:
    """The schema the spec names, else ``default``, else ``<source system>_<layer>``."""

    schema = (
        section.get("schema") or default or (f"{source_system}_{layer}" if source_system else None)
    )
    if not schema:
        errors.append(f"{label}: {layer} schema is required")
        return None
    if not IDENTIFIER.match(str(schema)):
        errors.append(f"{label}: {layer} schema must be an identifier")
        return None
    return str(schema)


def target_conflicts(configs: list[PipelineConfig]) -> list[str]:
    """Reject two pipelines that write the same catalog.schema.table at any layer."""

    seen: dict[str, str] = {}
    errors: list[str] = []
    for config in configs:
        for layer in config.layers:
            target = config.table_name(layer)
            owner = seen.get(target)
            if owner and owner != config.pipeline_id:
                errors.append(f"{target} is written by {owner} and {config.pipeline_id}")
            seen[target] = config.pipeline_id
    return errors


def _target_conflicts(configs: list[PipelineConfig]) -> list[str]:
    return target_conflicts(configs)


def _reject_secrets(value: Any, label: str, errors: list[str]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = str(key).lower()
            if any(part in lowered for part in SECRET_PARTS):
                errors.append(
                    f"{label}: {key} is not allowed; use a secret scope or a Unity Catalog connection"
                )
            _reject_secrets(item, label, errors)
    elif isinstance(value, list):
        for item in value:
            _reject_secrets(item, label, errors)


def _reject_unresolved(value: Any, label: str, errors: list[str]) -> None:
    if isinstance(value, str) and UNRESOLVED.search(value):
        errors.append(f"{label}: unresolved value '{value}'")
    elif isinstance(value, dict):
        for item in value.values():
            _reject_unresolved(item, label, errors)
    elif isinstance(value, list):
        for item in value:
            _reject_unresolved(item, label, errors)


def _required_text(
    document: Mapping[str, Any], key: str, label: str, errors: list[str]
) -> str | None:
    value = document.get(key)
    if value is None or str(value).strip() == "":
        errors.append(f"{label}: {key} is required")
        return None
    return str(value)


def _identifier(document: Mapping[str, Any], key: str, label: str, errors: list[str]) -> str | None:
    value = _required_text(document, key, label, errors)
    if value and not IDENTIFIER.match(value):
        errors.append(f"{label}: {key} must be an identifier")
        return None
    return value


def _optional_text(value: Any) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    return str(value)


def _name_list(value: Any, key: str, label: str, errors: list[str]) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{label}: {key} must be a list")
        return []
    names: list[str] = []
    for item in value:
        if not IDENTIFIER.match(str(item)):
            errors.append(f"{label}: {key} entries must be identifiers")
        else:
            names.append(str(item))
    return names


def _columns(value: Any, defaults: TableTags | None, label: str, errors: list[str]) -> list[Column]:
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{label}: columns must be a list")
        return []
    columns: list[Column] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, dict):
            errors.append(f"{label}: each column must be a mapping")
            continue
        name = str(item.get("name") or "")
        type_name = str(item.get("type") or "string")
        if not IDENTIFIER.match(name):
            errors.append(f"{label}: column name '{name}' must be an identifier")
            continue
        if name in seen:
            errors.append(f"{label}: column '{name}' is duplicated")
            continue
        seen.add(name)
        if not TYPE_NAME.match(type_name):
            errors.append(f"{label}: column type '{type_name}' is not allowed")
            continue
        description = _column_description(item, name, label, errors)
        classification, pii = _column_tags(item, name, defaults, label, errors)
        columns.append(Column(name, type_name, description, classification, pii))
    return columns


def _load_type(
    value: Any, dedup: bool, is_database: bool, source_mode: str, label: str, errors: list[str]
) -> str:
    """How bronze takes its rows. Derived from the older ``dedup`` and ``source.mode`` settings
    when ``raw_to_bronze.load_type`` is not given, so existing specs keep their meaning."""

    if is_database:
        derived = "full_refresh" if source_mode == "snapshot" else "scd1"
    else:
        derived = "scd1" if dedup else "insert_only"
    if value is None:
        return derived
    name = str(value)
    if name not in LOAD_TYPES:
        errors.append(f"{label}: raw_to_bronze.load_type must be one of {', '.join(LOAD_TYPES)}")
    elif is_database and name != derived:
        errors.append(
            f"{label}: a database table's load_type follows source.mode "
            f"(snapshot: full_refresh, merge: scd1); this one is {derived}"
        )
    elif not is_database and name == "full_refresh":
        errors.append(f"{label}: full_refresh is only for database tables")
    elif not is_database and dedup and name == "insert_only":
        errors.append(f"{label}: load_type insert_only conflicts with dedup: true")
    else:
        return name
    return derived


def _delete_when(
    value: Any, load_type: str, known: set[str], label: str, errors: list[str]
) -> str | None:
    """SQL condition marking a source row as deleted. The row stays and is flagged, not removed."""

    from lakeflow_framework.quality import assert_safe_rule_expression

    expression = _optional_text(value)
    if expression is None:
        return None
    if load_type != "scd1":
        errors.append(f"{label}: delete_when is only for load_type scd1")
        return None
    before = len(errors)
    assert_safe_rule_expression(expression, known, label=label, name="delete_when", errors=errors)
    return None if len(errors) > before else expression


def _description(value: Any, key: str, label: str, errors: list[str]) -> str:
    """Free text that describes a table. Stored in the registry and used as the table comment."""

    if value is None:
        return ""
    if not isinstance(value, str):
        errors.append(f"{label}: {key} must be text")
        return ""
    text = value.strip()
    if len(text) > MAX_DESCRIPTION_CHARS:
        errors.append(f"{label}: {key} is longer than {MAX_DESCRIPTION_CHARS} characters")
        return ""
    return text


def _cluster_by(value: Any, columns: list[Column], label: str, errors: list[str]) -> list[str]:
    if value is None:
        return []
    names = _name_list(value, "bronze.cluster_by", label, errors)
    if len(names) > MAX_CLUSTER_KEYS:
        errors.append(f"{label}: bronze.cluster_by takes at most {MAX_CLUSTER_KEYS} columns")
    types = {column.name: column.type_name.lower() for column in columns}
    for name in names:
        if types.get(name, "").startswith(UNCLUSTERABLE):
            errors.append(f"{label}: bronze.cluster_by cannot use {types[name]} column '{name}'")
    return names


def _column_description(item: Mapping[str, Any], name: str, label: str, errors: list[str]) -> str:
    if "description" not in item:
        return ""
    raw = item.get("description")
    if raw is None:
        return ""
    if not isinstance(raw, str):
        errors.append(f"{label}: column '{name}' description must be text")
        return ""
    return raw.strip()


def _column_tags(
    item: Mapping[str, Any],
    name: str,
    defaults: TableTags | None,
    label: str,
    errors: list[str],
) -> tuple[str, list[str]]:
    column_label = f"{label}: column '{name}'"
    if "classification" in item:
        classification = _classification(item.get("classification"), column_label, errors) or ""
    elif defaults is not None:
        classification = defaults.classification
    else:
        classification = ""
    if "pii" in item:
        pii = _pii(item.get("pii"), column_label, errors)
    elif defaults is not None:
        pii = list(defaults.pii)
    else:
        pii = []
    return classification, pii


def _table_on_fail(raw_to_bronze: Mapping[str, Any], label: str, errors: list[str]) -> str:
    """``raw_to_bronze.on_fail``: the action of every rule, set here or on the rules, not both."""

    if "on_bad_rows" in raw_to_bronze:
        errors.append(f"{label}: raw_to_bronze.on_bad_rows is now raw_to_bronze.on_fail")
    value = raw_to_bronze.get("on_fail")
    if value is None:
        return "quarantine"
    rules = raw_to_bronze.get("rules")
    if not rules:
        # on_fail says what happens to a row that fails a rule, so it means nothing on its own.
        errors.append(
            f"{label}: raw_to_bronze.on_fail needs rules: it sets what happens to a row that "
            "fails a rule. Add rules, or remove on_fail (a table with neither has no quarantine)"
        )
    own = [
        str(item.get("name") or "?")
        for item in (rules if isinstance(rules, list) else [])
        if isinstance(item, Mapping) and item.get("on_fail") is not None
    ]
    if own:
        errors.append(
            f"{label}: set on_fail in one place only: on raw_to_bronze for every rule, or on "
            f"each rule, not both (rule(s) {', '.join(own)} set their own)"
        )
    return _rule_action(value, "on_fail", label, errors)


def _rule_action(value: Any, key: str, label: str, errors: list[str]) -> str:
    """What to do with a row that fails a rule: one of ``RULE_ACTIONS``. Omitted: quarantine."""

    if value is None:
        return "quarantine"
    action = str(value)
    if action not in RULE_ACTIONS:
        errors.append(f"{label}: {key} must be one of {', '.join(RULE_ACTIONS)}; got '{action}'")
        return "quarantine"
    return action


def _rules(
    value: Any,
    label: str,
    errors: list[str],
    *,
    columns: set[str] | None = None,
    default_action: str = "quarantine",
) -> list[QualityRule]:
    from lakeflow_framework.quality import assert_safe_rule_expression

    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{label}: rules must be a list")
        return []
    rules: list[QualityRule] = []
    seen: set[str] = set()
    known = columns or set()
    for item in value:
        if not isinstance(item, dict):
            errors.append(f"{label}: each rule must be a mapping")
            continue
        name = str(item.get("name") or "")
        expression = str(item.get("expression") or "")
        if not IDENTIFIER.match(name):
            errors.append(f"{label}: rule name '{name}' must be an identifier")
            continue
        if name in seen:
            errors.append(f"{label}: rule name '{name}' is duplicated")
        seen.add(name)
        before = len(errors)
        assert_safe_rule_expression(expression, known, label=label, name=name, errors=errors)
        action = (
            _rule_action(item["on_fail"], f"rule '{name}' on_fail", label, errors)
            if item.get("on_fail") is not None
            else default_action
        )
        if len(errors) > before:
            continue
        rules.append(QualityRule(name, expression, action))
    return rules


def _fixed_columns(value: Any, label: str, errors: list[str]) -> list[FixedColumn]:
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{label}: fixed_columns must be a list")
        return []
    columns: list[FixedColumn] = []
    for item in value:
        if not isinstance(item, dict):
            errors.append(f"{label}: each fixed column must be a mapping")
            continue
        name = str(item.get("name") or "")
        if not IDENTIFIER.match(name):
            errors.append(f"{label}: fixed column '{name}' must be an identifier")
            continue
        try:
            start = int(item["start"])
            length = int(item["length"])
        except (KeyError, TypeError, ValueError):
            errors.append(f"{label}: fixed column '{name}' needs start and length")
            continue
        if start < 1 or length < 1:
            errors.append(f"{label}: fixed column '{name}' start and length must be positive")
            continue
        columns.append(FixedColumn(name, start, length))
    return columns
