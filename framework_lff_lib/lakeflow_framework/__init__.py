"""Public facade. Consumers import these names and nothing else."""

from lakeflow_framework.config import ConfigValidationError, PipelineConfig, validate_configs
from lakeflow_framework.load import get_strategy
from lakeflow_framework.pipeline import MetadataNotFound, build_pipeline, build_pipelines

__all__ = [
    "ConfigValidationError",
    "LoadMethod",
    "MetadataNotFound",
    "PipelineConfig",
    "build_pipeline",
    "build_pipelines",
    "validate_configs",
]


class LoadMethod:
    """Names of the raw-landing strategies.

    MVP methods that a spec may use: ``VOLUME_FILES``, ``DATABASE_TABLE``.
    ``AUTOLOADER`` / ``COPY_INTO`` / ``CTAS`` are frozen (kept in code, rejected by validation
    unless ``allow_frozen=True``).
    """

    VOLUME_FILES = "volume_files"
    DATABASE_TABLE = "database_table"
    # Frozen — not admitted by generate unless allow_frozen=True.
    AUTOLOADER = "autoloader"
    COPY_INTO = "copy_into"
    CTAS = "ctas"

    @staticmethod
    def get(name: str):
        return get_strategy(name)
