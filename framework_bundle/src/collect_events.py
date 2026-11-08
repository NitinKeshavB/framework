"""Single writer that lands step files into run_ledger.

Auto Loader checkpoint for this collector lives under the events volume.
File-ingestion pipelines do not set their own checkpoint or schema location.
"""

import sys

from pyspark.sql import SparkSession

from lakeflow_framework.collector import append_events


def main() -> None:
    catalog = sys.argv[1]
    volume = f"/Volumes/{catalog}/framework_meta/framework_events"
    spark = SparkSession.builder.getOrCreate()
    seen_steps: set = set()
    seen_objects: set = set()

    def consume(batch, _epoch) -> None:
        payloads = [row.asDict(recursive=True) for row in batch.collect()]
        append_events(spark, payloads, seen_steps, seen_objects, catalog)

    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume)
        .writeStream.foreachBatch(consume)
        .option("checkpointLocation", volume + "/_checkpoints/collector")
        .trigger(processingTime="30 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
