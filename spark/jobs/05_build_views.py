"""05 - Build the benchmark views used by Hive, Spark SQL and Superset."""

import sys

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import spark_session  # noqa: E402


def main():
    spark = spark_session("agri_05_build_views")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql("DROP VIEW IF EXISTS agri_dwh.v_benchmark_like_hourly")
    spark.sql(
        """
        CREATE VIEW agri_dwh.v_benchmark_like_hourly AS
        SELECT h.event_time AS ts,
               h.location_id AS location,
               h.soil_wetness_root AS soil_moisture,
               h.t2m_c AS temperature,
               y.cereal_yield_kg_per_ha AS yield_estimate,
               'annual_country' AS yield_grain
        FROM agri_dwh.fact_weather_hourly h
        JOIN agri_dwh.dim_location l ON l.location_id = h.location_id
        JOIN agri_dwh.fact_cereal_annual y
          ON y.country_iso3 = l.country_iso3
         AND y.year = h.year
        """
    )

    spark.sql("DROP VIEW IF EXISTS agri_dwh.v_benchmark_like_daily")
    spark.sql(
        """
        CREATE VIEW agri_dwh.v_benchmark_like_daily AS
        SELECT d.date_key AS ts,
               d.location_id AS location,
               d.soil_wetness_root AS soil_moisture,
               d.t2m_c AS temperature,
               y.cereal_yield_kg_per_ha AS yield_estimate,
               'annual_country' AS yield_grain
        FROM agri_dwh.fact_weather_daily d
        JOIN agri_dwh.dim_location l ON l.location_id = d.location_id
        JOIN agri_dwh.fact_cereal_annual y
          ON y.country_iso3 = l.country_iso3
         AND y.year = year(d.date_key)
        """
    )

    print("Views created")


if __name__ == "__main__":
    main()
