"""Pipeline configuration. Validation finishes before any cluster is used."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

LOAD_METHODS = ("autoloader", "copy_into", "ctas")
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
SECRET_PARTS = ("account_key", "access_key", "secret", "password", "token")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TYPE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_(), ]*$")
UNRESOLVED = re.compile(r"\$\{[^}]+\}")
CLOUD_PATH = re.compile(r"^(abfss|s3)://", re.IGNORECASE)


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
    name: str
    type_name: str


@dataclass
class QualityRule:
    name: str
    expression: str


@dataclass
class PipelineConfig:
    """A bronze load: files land in raw, then raw is published to bronze."""

    pipeline_id: str
    project: str
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
    fixed_columns: list[FixedColumn] = field(default_factory=list)
    delimiter: str | None = None
    header: bool = True
    header_rows: int | None = None
    data_address: str | None = None
    max_files_per_trigger: int | None = None
    path_glob: str | None = None
    job_name: str | None = None
    document: dict[str, Any] = field(default_factory=dict)

    def layer_schema(self, layer: str) -> str:
        return {
            "raw": self.raw_schema,
            "bronze": self.bronze_schema,
            "quarantine": self.quarantine_schema,
        }[layer]

    def table_name(self, layer: str) -> str:
        table = {
            "raw": self.raw_table,
            "bronze": self.bronze_table,
            "quarantine": self.quarantine_table,
        }[layer]
        return f"{self.catalog}.{self.layer_schema(layer)}.{table}"

    def canonical(self) -> dict[str, Any]:
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
        if self.fixed_columns:
            source["fixed_columns"] = [
                {"name": column.name, "start": column.start, "length": column.length}
                for column in self.fixed_columns
            ]
        document = {
            "pipeline_id": self.pipeline_id,
            "project": self.project,
            "layer": self.layer,
            "catalog": self.catalog,
            "source_to_raw": {
                "method": self.load_method,
                "source": source,
                "raw": {"schema": self.raw_schema, "table": self.raw_table},
            },
            "raw_to_bronze": {
                "bronze": {"schema": self.bronze_schema, "table": self.bronze_table},
                "dedup": self.dedup,
                "primary_keys": list(self.primary_keys),
                "columns": [
                    {"name": column.name, "type": column.type_name} for column in self.columns
                ],
                "rules": [
                    {"name": rule.name, "expression": rule.expression} for rule in self.rules
                ],
            },
            "quarantine": {"schema": self.quarantine_schema, "table": self.quarantine_table},
        }
        _put(document, "job_name", self.job_name)
        _put(document["raw_to_bronze"], "watermark_column", self.watermark_column)
        return document

    def content_hash(self) -> str:
        payload = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_yaml(path: Path) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ConfigValidationError([f"{path}: the document must be a mapping"])
    return document


def validate_configs(paths: list[Path]) -> list[PipelineConfig]:
    errors: list[str] = []
    configs: list[PipelineConfig] = []
    for path in paths:
        try:
            document = load_yaml(path)
        except ConfigValidationError as exc:
            errors.extend(exc.errors)
            continue
        except yaml.YAMLError as exc:
            errors.append(f"{path}: {exc}")
            continue
        found, parsed = _validate_one(document, str(path))
        errors.extend(found)
        if parsed is not None:
            configs.append(parsed)
    errors.extend(_target_conflicts(configs))
    if errors:
        raise ConfigValidationError(errors)
    return configs


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
    project = _required_text(document, "project", label, errors)
    catalog = str(document.get("catalog") or "")
    if not catalog:
        errors.append(f"{label}: catalog is required")
    layer = document.get("layer")
    if layer != "bronze":
        if layer in {"silver", "gold"}:
            errors.append(f"{label}: {layer} loads are not implemented; layer must be bronze")
        else:
            errors.append(f"{label}: layer must be bronze")

    source_to_raw = _section(document, "source_to_raw", label, errors)
    raw_to_bronze = _section(document, "raw_to_bronze", label, errors)
    quarantine = _section(document, "quarantine", label, errors)
    source = _section(source_to_raw, "source", f"{label}: source_to_raw", errors)
    raw_target = _section(source_to_raw, "raw", f"{label}: source_to_raw", errors)
    bronze_target = _section(raw_to_bronze, "bronze", f"{label}: raw_to_bronze", errors)

    load_method = str(source_to_raw.get("method") or "autoloader")
    if source_to_raw and load_method not in LOAD_METHODS:
        errors.append(f"{label}: source_to_raw.method must be one of {', '.join(LOAD_METHODS)}")
    file_format = str(source.get("format") or "")
    if source and file_format not in FORMATS:
        errors.append(f"{label}: format must be one of {', '.join(FORMATS)}")
    if file_format == "excel" and load_method == "copy_into":
        errors.append(f"{label}: excel cannot use copy_into")

    source_system = (
        _identifier(source, "system", f"{label}: source_to_raw.source", errors) if source else None
    )
    path = str(source.get("path") or "")
    if source and not path:
        errors.append(f"{label}: path is required")
    elif path and not CLOUD_PATH.match(path):
        errors.append(f"{label}: path must start with abfss:// or s3://")

    raw_table = _target_table(raw_target, "raw", pipeline_id, label, errors)
    bronze_table = _target_table(bronze_target, "bronze", raw_table or pipeline_id, label, errors)
    quarantine_table = _target_table(
        quarantine, "quarantine", bronze_table or pipeline_id, label, errors
    )
    raw_schema = _target_schema(raw_target, "raw", source_system, label, errors)
    bronze_schema = _target_schema(bronze_target, "bronze", source_system, label, errors)
    quarantine_schema = _target_schema(quarantine, "quarantine", source_system, label, errors)

    dedup = bool(raw_to_bronze.get("dedup", False))
    primary_keys = _name_list(raw_to_bronze.get("primary_keys"), "primary_keys", label, errors)
    watermark = raw_to_bronze.get("watermark_column")
    if watermark is not None and not IDENTIFIER.match(str(watermark)):
        errors.append(f"{label}: watermark_column must be an identifier")
    if dedup and (not primary_keys or not watermark):
        errors.append(f"{label}: dedup requires primary_keys and watermark_column")

    columns = _columns(raw_to_bronze.get("columns"), label, errors)
    rules = _rules(raw_to_bronze.get("rules"), label, errors)
    fixed_columns = _fixed_columns(source.get("fixed_columns"), label, errors)
    if file_format == "fixed_width" and not fixed_columns:
        errors.append(f"{label}: fixed_width requires fixed_columns")

    delimiter = source.get("delimiter")
    if file_format in {"csv", "delimited"} and not delimiter:
        errors.append(f"{label}: delimiter is required for {file_format}")
    elif delimiter and ("'" in str(delimiter) or len(str(delimiter)) > 3):
        errors.append(f"{label}: delimiter must be a short literal")

    header_rows = source.get("header_rows")
    data_address = source.get("data_address")
    if file_format == "excel":
        if header_rows not in (0, 1):
            errors.append(f"{label}: header_rows must be 0 or 1 for excel")
        if not data_address:
            errors.append(f"{label}: data_address is required for excel")

    max_files = source.get("max_files_per_trigger")
    if max_files is not None:
        try:
            if int(max_files) <= 0:
                errors.append(f"{label}: max_files_per_trigger must be greater than 0")
        except (TypeError, ValueError):
            errors.append(f"{label}: max_files_per_trigger must be an integer")
            max_files = None

    if (
        errors
        or not pipeline_id
        or not project
        or not source_system
        or not raw_table
        or not bronze_table
        or not quarantine_table
        or not raw_schema
        or not bronze_schema
        or not quarantine_schema
    ):
        return errors, None

    parsed = PipelineConfig(
        pipeline_id=pipeline_id,
        project=project,
        source_system=source_system,
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
        external_location=_optional_text(source.get("external_location")),
        dedup=dedup,
        primary_keys=primary_keys,
        watermark_column=str(watermark) if watermark else None,
        columns=columns,
        rules=rules,
        fixed_columns=fixed_columns,
        delimiter=str(delimiter) if delimiter else None,
        header=bool(source.get("header", True)),
        header_rows=int(header_rows) if header_rows in (0, 1) else None,
        data_address=str(data_address) if data_address else None,
        max_files_per_trigger=int(max_files) if max_files is not None else None,
        path_glob=_optional_text(source.get("path_glob")),
        job_name=_optional_text(document.get("job_name")),
        document=document,
    )
    return errors, parsed


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


def _target_schema(
    section: Mapping[str, Any],
    layer: str,
    source_system: str | None,
    label: str,
    errors: list[str],
) -> str | None:
    schema = section.get("schema") or (f"{source_system}_{layer}" if source_system else None)
    if not schema:
        errors.append(f"{label}: {layer} schema is required")
        return None
    if not IDENTIFIER.match(str(schema)):
        errors.append(f"{label}: {layer} schema must be an identifier")
        return None
    return str(schema)


def _target_conflicts(configs: list[PipelineConfig]) -> list[str]:
    seen: dict[str, str] = {}
    errors: list[str] = []
    for config in configs:
        for layer in ("raw", "bronze", "quarantine"):
            target = config.table_name(layer)
            owner = seen.get(target)
            if owner and owner != config.pipeline_id:
                errors.append(f"{target} is written by {owner} and {config.pipeline_id}")
            seen[target] = config.pipeline_id
    return errors


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


def _columns(value: Any, label: str, errors: list[str]) -> list[Column]:
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{label}: columns must be a list")
        return []
    columns: list[Column] = []
    for item in value:
        if not isinstance(item, dict):
            errors.append(f"{label}: each column must be a mapping")
            continue
        name = str(item.get("name") or "")
        type_name = str(item.get("type") or "string")
        if not IDENTIFIER.match(name):
            errors.append(f"{label}: column name '{name}' must be an identifier")
            continue
        if not TYPE_NAME.match(type_name):
            errors.append(f"{label}: column type '{type_name}' is not allowed")
            continue
        columns.append(Column(name, type_name))
    return columns


def _rules(value: Any, label: str, errors: list[str]) -> list[QualityRule]:
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{label}: rules must be a list")
        return []
    rules: list[QualityRule] = []
    seen: set[str] = set()
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
        if not expression or ";" in expression or "--" in expression:
            errors.append(f"{label}: rule '{name}' needs one boolean expression")
            continue
        rules.append(QualityRule(name, expression))
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
