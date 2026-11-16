"""Single writer that lands step files into run_ledger.

Auto Loader checkpoint for this collector lives under the events volume.
File-ingestion pipelines do not set their own checkpoint or schema location.
"""

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.collector import append_events

# One micro-batch stays small enough to collect on the driver. The seen sets are a cache;
# the ledger and registry remain the source of truth, so clearing them only costs a lookup.
_MAX_FILES_PER_TRIGGER = "1000"
_MAX_SEEN = 50_000


def main() -> None:
    catalog = sys.argv[1]
    volume = f"/Volumes/{catalog}/framework_meta/framework_events"
    spark = SparkSession.builder.getOrCreate()
    seen_steps: set = set()
    seen_objects: set = set()

    def consume(batch, _epoch) -> None:
        if len(seen_steps) > _MAX_SEEN:
            seen_steps.clear()
        if len(seen_objects) > _MAX_SEEN:
            seen_objects.clear()
        payloads = [row.asDict(recursive=True) for row in batch.collect()]
        append_events(spark, payloads, seen_steps, seen_objects, catalog)

    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", _MAX_FILES_PER_TRIGGER)
        # Checkpoint files live under this volume. Only event JSON is a registry payload.
        .option("pathGlobFilter", "*.json")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(volume)
        .writeStream.foreachBatch(consume)
        .option("checkpointLocation", volume + "/_checkpoints/collector")
        .trigger(processingTime="30 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
