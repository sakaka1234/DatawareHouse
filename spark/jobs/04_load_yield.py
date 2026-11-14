"""04 - Load World Bank cereal yield (30 countries x 15 years)."""

import json
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import RAW, hdfs_path, spark_session, table_count, write_metrics, write_table  # noqa: E402

OUTPUT = hdfs_path("fact_cereal_annual")
METRICS = "/opt/agri/artifacts/metrics/04_load_yield.json"


def main():
    spark = spark_session("agri_04_load_yield")
    yield_df = spark.read.option("header", True).option("inferSchema", False).csv(
        RAW + "/yield"
    ).select(
        F.trim("country_iso3").alias("country_iso3"),
        F.col("country_name").alias("country_name"),
        F.col("year").cast("int").alias("year"),
        F.col("cereal_area_ha").cast("double").alias("cereal_area_ha"),
        F.col("cereal_production_mt").cast("double").alias("cereal_production_mt"),
        F.col("cereal_yield_kg_per_ha").cast("double").alias("cereal_yield_kg_per_ha"),
    ).where("cereal_yield_kg_per_ha IS NOT NULL")

    write_table(spark, yield_df, "fact_cereal_annual", OUTPUT)
    payload = {
        "rows": table_count(spark, "agri_dwh.fact_cereal_annual"),
        "countries": yield_df.select("country_iso3").distinct().count(),
        "years": yield_df.select("year").distinct().count(),
    }
    write_metrics(METRICS, payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
