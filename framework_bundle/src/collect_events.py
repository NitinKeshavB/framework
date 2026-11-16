"""Single writer that lands event files into the registry.

Auto Loader checkpoint for this collector lives under the events volume.
File-ingestion pipelines do not set their own checkpoint or schema location.
A batch stays in Spark: the driver does not collect it.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from lakeflow_framework.collector import land_events

# A definition file and a step file do not share columns. An explicit string schema keeps
# both, and a later batch cannot change the types. Extra fields land in _rescued.
_MAX_FILES_PER_TRIGGER = "5000"
_EVENT_FIELDS = (
    "kind",
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
    "run_id",
    "parent_run_id",
    "step_no",
    "step_name",
    "status",
    "started_at",
    "ended_at",
    "failure_reason",
    "recon_json",
)


def main() -> None:
    catalog = sys.argv[1]
    volume = f"/Volumes/{catalog}/framework_meta/framework_events"
    spark = SparkSession.builder.getOrCreate()
    schema = StructType([StructField(name, StringType(), True) for name in _EVENT_FIELDS])

    def consume(batch, _epoch) -> None:
        if batch.isEmpty():
            return
        batch.createOrReplaceTempView("_framework_events")
        land_events(spark, catalog)

    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", _MAX_FILES_PER_TRIGGER)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.rescuedDataColumn", "_rescued")
        # Checkpoint files live under this volume. Only event JSON is a registry payload.
        .option("pathGlobFilter", "*.json")
        .schema(schema)
        .load(volume)
        .writeStream.foreachBatch(consume)
        .option("checkpointLocation", volume + "/_checkpoints/collector")
        .trigger(processingTime="30 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
