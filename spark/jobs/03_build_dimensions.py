"""03 - Build dim_location (32 locations) and dim_date (2010-2024)."""

import json
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import RAW, hdfs_path, spark_session, table_count, write_metrics, write_table  # noqa: E402

LOCATION_OUT = hdfs_path("dim_location")
DATE_OUT = hdfs_path("dim_date")
METRICS = "/opt/agri/artifacts/metrics/03_build_dimensions.json"


def main():
    spark = spark_session("agri_03_build_dimensions")

    locations = spark.read.option("header", True).option("inferSchema", False).csv(
        RAW + "/location"
    ).select(
        F.trim("location_id").alias("location_id"),
        F.col("name").alias("name"),
        F.trim("country_iso3").alias("country_iso3"),
        F.col("country_name").alias("country_name"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("crop_system").alias("crop_system"),
    )
    write_table(spark, locations, "dim_location", LOCATION_OUT)

    days = spark.sql(
        "SELECT explode(sequence(to_date('2010-01-01'), to_date('2024-12-31'), interval 1 day)) AS date_key"
    ).select(
        F.col("date_key"),
        F.year("date_key").cast("int").alias("year"),
        F.month("date_key").cast("int").alias("month"),
        F.dayofmonth("date_key").cast("int").alias("day"),
        F.quarter("date_key").cast("int").alias("quarter"),
        F.date_format("date_key", "EEEE").alias("weekday"),
    )
    write_table(spark, days, "dim_date", DATE_OUT)

    payload = {
        "locations": table_count(spark, "agri_dwh.dim_location"),
        "days": table_count(spark, "agri_dwh.dim_date"),
    }
    write_metrics(METRICS, payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
