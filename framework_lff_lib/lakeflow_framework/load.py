"""Load methods. Each strategy answers only the raw landing step."""

from __future__ import annotations

from dataclasses import dataclass

from lakeflow_framework.config import FORMATS, PipelineConfig

# A trigger with no cap will list an entire backlog in one micro-batch.
DEFAULT_MAX_FILES_PER_TRIGGER = 1000
SPARK_FORMAT = {
    "csv": "csv",
    "delimited": "csv",
    "text": "text",
    "fixed_width": "text",
    "yaml": "text",
    "json": "json",
    "parquet": "parquet",
    "avro": "avro",
    "excel": "excel",
}
RESCUE_FORMATS = {"csv", "delimited", "json", "text"}
# Formats whose columns land as strings or inferred types. Parquet and Avro carry their own types.
HINT_FORMATS = {"csv", "delimited", "json"}
COPY_FORMAT = {
    "csv": "CSV",
    "delimited": "CSV",
    "text": "TEXT",
    "fixed_width": "TEXT",
    "yaml": "TEXT",
    "json": "JSON",
    "parquet": "PARQUET",
    "avro": "AVRO",
}


@dataclass(frozen=True)
class LoadStrategy:
    name: str
    supported_formats: frozenset[str]
    raw_relation: str

    def options(self, config: PipelineConfig) -> dict[str, str]:
        return {}

    def batch_sql(self, config: PipelineConfig) -> list[str]:
        return []


class AutoLoader(LoadStrategy):
    def __init__(self) -> None:
        super().__init__("autoloader", frozenset(FORMATS), "streaming")

    def options(self, config: PipelineConfig) -> dict[str, str]:
        options = {
            "cloudFiles.format": SPARK_FORMAT[config.file_format],
            "cloudFiles.includeExistingFiles": "true",
            "cloudFiles.inferColumnTypes": "false"
            if config.columns or config.fixed_columns
            else "true",
            "cloudFiles.allowOverwrites": "false",
        }
        # Without file events Auto Loader lists the directory. Turn them on only once the external
        # location behind the path has file events enabled.
        if config.file_events:
            options["cloudFiles.useManagedFileEvents"] = "true"
        else:
            options["cloudFiles.useNotifications"] = "false"
        hints = schema_hints(config)
        if hints:
            options["cloudFiles.schemaHints"] = hints
        options["cloudFiles.maxFilesPerTrigger"] = str(
            config.max_files_per_trigger or DEFAULT_MAX_FILES_PER_TRIGGER
        )
        if config.file_format in RESCUE_FORMATS:
            options["cloudFiles.schemaEvolutionMode"] = "rescue"
            options["rescuedDataColumn"] = "_rescued_data"
        if config.file_format in {"csv", "delimited"}:
            options["header"] = str(config.header).lower()
            options["delimiter"] = config.delimiter or ","
            options["mode"] = "PERMISSIVE"
        if config.file_format == "json":
            # multiLine reads each file as one document. Leave it off unless the spec asks.
            if config.json_multiline:
                options["multiLine"] = "true"
            options["mode"] = "PERMISSIVE"
        if config.file_format == "excel":
            options["cloudFiles.schemaEvolutionMode"] = "none"
            options["headerRows"] = str(config.header_rows if config.header_rows is not None else 1)
            options["dataAddress"] = config.data_address or ""
        if config.path_glob:
            options["pathGlobFilter"] = config.path_glob
        return options


class VolumeFiles(AutoLoader):
    """Auto Loader straight from the raw volume into bronze. There is no raw table."""

    def __init__(self) -> None:
        LoadStrategy.__init__(self, "volume_files", frozenset(FORMATS), "none")


class DatabaseTable(LoadStrategy):
    """A table read through a foreign catalog straight into bronze. There is no raw table."""

    def __init__(self) -> None:
        super().__init__("database_table", frozenset(), "none")


class CopyInto(LoadStrategy):
    def __init__(self) -> None:
        super().__init__("copy_into", frozenset(FORMATS) - {"excel"}, "streaming")

    def batch_sql(self, config: PipelineConfig) -> list[str]:
        target = config.table_name("raw")
        file_format = COPY_FORMAT[config.file_format]
        pairs = ["'mergeSchema' = 'false'"]
        if config.file_format in {"csv", "delimited"}:
            header = str(config.header).lower()
            pairs.append(f"'header' = '{header}'")
            pairs.append(f"'delimiter' = {_sql_string(config.delimiter)}")
        format_options = ", ".join(pairs)
        return [
            " ".join(
                [
                    f"COPY INTO {target}",
                    f"FROM {_sql_string(config.path)}",
                    f"FILEFORMAT = {file_format}",
                    f"FORMAT_OPTIONS ({format_options})",
                    "COPY_OPTIONS ('force' = 'false')",
                ]
            )
        ]


class Ctas(LoadStrategy):
    def __init__(self) -> None:
        super().__init__("ctas", frozenset(FORMATS), "batch")

    def batch_sql(self, config: PipelineConfig) -> list[str]:
        target = config.table_name("raw")
        reader = _read_files(config)
        projection = _projection(config, source="file")
        return [f"CREATE OR REPLACE TABLE {target} AS SELECT {projection} FROM {reader}"]


def schema_hints(config: PipelineConfig) -> str:
    """Declared column types for Auto Loader. A value that does not parse goes to _rescued_data."""

    if config.file_format not in HINT_FORMATS:
        return ""
    return ", ".join(f"`{column.name}` {column.type_name}" for column in config.columns)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _read_files(config: PipelineConfig) -> str:
    spark_format = SPARK_FORMAT[config.file_format]
    options = [f"format => {_sql_string(spark_format)}"]
    if config.file_format in {"csv", "delimited"}:
        options.append(f"header => {str(config.header).lower()}")
        options.append(f"delimiter => {_sql_string(config.delimiter)}")
    if config.file_format == "excel":
        options.append(f"headerRows => {config.header_rows}")
        options.append(f"dataAddress => {_sql_string(str(config.data_address))}")
    return f"read_files({_sql_string(config.path)}, {', '.join(options)})"


def projection_exprs(config: PipelineConfig, source: str) -> list[str]:
    """Select list for one file read. Lineage columns are part of the raw table, not a second pass."""

    metadata = ["_metadata.file_path AS _source_file", "current_timestamp() AS _ingested_at"]
    if config.file_format == "fixed_width":
        fields = [
            f"substring(value, {column.start}, {column.length}) AS {column.name}"
            for column in config.fixed_columns
        ]
        return fields + metadata
    if config.file_format == "yaml":
        column = "value" if source == "file" else "content"
        return [f"CAST({column} AS STRING) AS content", *metadata]
    return ["*", *metadata]


def _projection(config: PipelineConfig, source: str) -> str:
    return ", ".join(projection_exprs(config, source))


_STRATEGIES = {
    "autoloader": AutoLoader(),
    "copy_into": CopyInto(),
    "ctas": Ctas(),
    "volume_files": VolumeFiles(),
    "database_table": DatabaseTable(),
}


def get_strategy(name: str) -> LoadStrategy:
    try:
        return _STRATEGIES[name]
    except KeyError as exc:
        raise KeyError(f"unknown load method {name}") from exc
