"""01 - Clean hourly weather, deduplicate, and write partitioned Parquet + Hive table."""

import json
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import RAW, hdfs_path, spark_session, table_count, write_metrics, write_table  # noqa: E402

INPUT = RAW + "/hourly"
OUTPUT = hdfs_path("fact_weather_hourly")
METRICS = "/opt/agri/artifacts/metrics/01_clean_weather.json"


def main():
    spark = spark_session("agri_01_clean_weather")
    raw = (
        spark.read.option("header", True).option("inferSchema", False).csv(INPUT)
    )
    cleaned = (
        raw.select(
            F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss").alias("event_time"),
            F.trim("location_id").alias("location_id"),
            F.col("t2m_c").cast("double").alias("t2m_c"),
            F.col("rh2m_pct").cast("double").alias("rh2m_pct"),
            F.col("precip_mm_h").cast("double").alias("precip_mm_h"),
            F.col("ws2m_ms").cast("double").alias("ws2m_ms"),
            F.col("soil_wetness_root").cast("double").alias("soil_wetness_root"),
            F.col("soil_wetness_top").cast("double").alias("soil_wetness_top"),
        )
        .where("event_time IS NOT NULL AND location_id IS NOT NULL")
        .dropDuplicates(["location_id", "event_time"])
        .withColumn("year", F.year("event_time").cast("int"))
        .withColumn("month", F.month("event_time").cast("int"))
    )

    write_table(
        spark,
        cleaned,
        "fact_weather_hourly",
        OUTPUT,
        partition_by=("year", "month"),
    )

    count = table_count(spark, "agri_dwh.fact_weather_hourly")
    partitions = spark.sql("SHOW PARTITIONS agri_dwh.fact_weather_hourly").count()
    payload = {
        "rows": count,
        "partitions": partitions,
        "duplicate_keys": count
        - cleaned.select("location_id", "event_time").distinct().count(),
        "locations": cleaned.select("location_id").distinct().count(),
        "output": OUTPUT,
    }
    write_metrics(METRICS, payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
