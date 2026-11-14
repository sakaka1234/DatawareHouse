"""02 - Load the extended 2010-2024 daily weather table."""

import json
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import RAW, hdfs_path, spark_session, table_count, write_metrics, write_table  # noqa: E402

INPUT = RAW + "/daily"
OUTPUT = hdfs_path("fact_weather_daily")
METRICS = "/opt/agri/artifacts/metrics/02_build_daily.json"


def main():
    spark = spark_session("agri_02_build_daily")
    raw = spark.read.option("header", True).option("inferSchema", False).csv(INPUT)
    daily = raw.select(
        F.to_date("date").alias("date_key"),
        F.trim("location_id").alias("location_id"),
        F.col("t2m_c").cast("double").alias("t2m_c"),
        F.col("t2m_max_c").cast("double").alias("t2m_max_c"),
        F.col("t2m_min_c").cast("double").alias("t2m_min_c"),
        F.col("precip_mm").cast("double").alias("precip_mm"),
        F.col("soil_wetness_root").cast("double").alias("soil_wetness_root"),
        F.col("soil_wetness_top").cast("double").alias("soil_wetness_top"),
        F.col("rh2m_pct").cast("double").alias("rh2m_pct"),
    ).where("date_key IS NOT NULL")

    write_table(spark, daily, "fact_weather_daily", OUTPUT)
    count = table_count(spark, "agri_dwh.fact_weather_daily")
    payload = {
        "rows": count,
        "locations": daily.select("location_id").distinct().count(),
        "min_date": str(daily.agg(F.min("date_key")).collect()[0][0]),
        "max_date": str(daily.agg(F.max("date_key")).collect()[0][0]),
    }
    write_metrics(METRICS, payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
