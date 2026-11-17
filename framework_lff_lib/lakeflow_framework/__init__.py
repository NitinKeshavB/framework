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
    """Names of the raw-landing strategies."""

    AUTOLOADER = "autoloader"
    COPY_INTO = "copy_into"
    CTAS = "ctas"

    @staticmethod
    def get(name: str):
        return get_strategy(name)
