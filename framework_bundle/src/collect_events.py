"""Single writer that lands step files into run_ledger.

Auto Loader checkpoint for this collector lives under the events volume.
File-ingestion pipelines do not set their own checkpoint or schema location.
"""

import sys
import time

from pyspark.sql import SparkSession

from lakeflow_framework.collector import append_events, decode_events

# Serverless allows only an availableNow trigger, so each pass drains the new files and ends.
# Starting a new job run for every pass costs minutes of compute start-up, and a pipeline waits
# for its definition to reach the registry. So one run loops: drain, pause briefly, drain again,
# on the same session and checkpoint. The run ends after MAX_RUN_SECONDS and the continuous job
# starts a fresh one, which keeps a long-lived session from growing without limit.
IDLE_SECONDS = 10
MAX_RUN_SECONDS = 4 * 3600


def main() -> None:
    catalog = sys.argv[1]
    volume = f"/Volumes/{catalog}/framework_meta/framework_events"
    spark = SparkSession.builder.getOrCreate()
    seen_steps: set = set()
    seen_objects: set = set()

    def consume(batch, _epoch) -> None:
        payloads = decode_events([row["value"] for row in batch.collect()])
        append_events(spark, payloads, seen_steps, seen_objects, catalog)

    started = time.monotonic()
    while time.monotonic() - started < MAX_RUN_SECONDS:
        (
            # Each file is one row of text, parsed in decode_events. The JSON format would need
            # cloudFiles.schemaLocation, and an inferred schema stops the stream when a new field
            # appears. Files under _checkpoints are ignored: Spark skips names starting with "_".
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("wholetext", "true")
            .option("pathGlobFilter", "*.json")
            .load(volume)
            .writeStream.foreachBatch(consume)
            .option("checkpointLocation", volume + "/_checkpoints/collector")
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
        )
        time.sleep(IDLE_SECONDS)


if __name__ == "__main__":
    main()
