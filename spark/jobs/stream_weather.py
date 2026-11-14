"""Streaming job: Kafka -> dedupe + watermark -> partitioned Parquet on HDFS.

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    /opt/agri/spark/jobs/stream_weather.py --timeout-seconds 300
"""

import argparse
import json
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import HDFS, spark_session, write_metrics  # noqa: E402

TOPIC = "agri.weather.hourly"
OUTPUT = HDFS + "/data/agri/processed/stream_weather"
CHECKPOINT = HDFS + "/data/agri/checkpoints/stream_weather"
METRICS = "/opt/agri/artifacts/metrics/stream_job.json"

SCHEMA = StructType(
    [
        StructField("location_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("t2m_c", DoubleType()),
        StructField("rh2m_pct", DoubleType()),
        StructField("precip_mm_h", DoubleType()),
        StructField("ws2m_ms", DoubleType()),
        StructField("soil_wetness_root", DoubleType()),
        StructField("soil_wetness_top", DoubleType()),
        StructField("produced_at", LongType()),
        StructField("batch_id", StringType()),
    ]
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--trigger", default="1 second")
    parser.add_argument("--topic", default=TOPIC)
    args = parser.parse_args()

    spark = spark_session("agri_stream_weather")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )

    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), SCHEMA).alias("r")
        )
        .select("r.*")
        .withColumn(
            "event_time", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss")
        )
        .where("event_time IS NOT NULL AND location_id IS NOT NULL")
        # One day, not ten minutes: this is a historical replay, so partitions
        # can be a few hours apart while still being "on time".
        .withWatermark("event_time", "1 day")
        .dropDuplicates(["location_id", "timestamp"])
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn(
            "latency_ms",
            F.unix_timestamp("ingest_time") * 1000 - F.col("produced_at"),
        )
        .withColumn("year", F.year("event_time").cast("int"))
        .withColumn("month", F.month("event_time").cast("int"))
    )

    query = (
        parsed.writeStream.format("parquet")
        .option("path", OUTPUT)
        .option("checkpointLocation", CHECKPOINT)
        .partitionBy("year", "month")
        .outputMode("append")
        .trigger(processingTime=args.trigger)
        .start()
    )

    write_metrics(
        METRICS,
        {
            "status": "started",
            "query_id": query.id,
            "run_id": query.runId,
            "output": OUTPUT,
        },
    )
    print("streaming started:", query.id, flush=True)

    try:
        query.awaitTermination(args.timeout_seconds)
    finally:
        if query.isActive:
            query.stop()
        write_metrics(
            METRICS,
            {
                "status": "stopped",
                "query_id": query.id,
                "output": OUTPUT,
                "note": "checkpoint kept for restart test",
            },
        )
        print(json.dumps({"status": "stopped", "query_id": query.id}))


if __name__ == "__main__":
    main()
