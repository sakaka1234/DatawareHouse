"""06 - Build the shared location-year / country-year feature table for ML."""

import json
import sys

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import hdfs_path, spark_session, table_count, write_metrics, write_table  # noqa: E402

OUTPUT = hdfs_path("ml_yield_features")
METRICS = "/opt/agri/artifacts/metrics/06_build_features.json"

SQL = """
WITH loc_year AS (
  SELECT location_id,
         year(date_key) AS year,
         SUM(GREATEST(t2m_c - 10, 0)) AS gdd,
         SUM(CASE WHEN t2m_max_c > 35 THEN 1 ELSE 0 END) AS heat_days_35,
         SUM(CASE WHEN t2m_min_c < 0 THEN 1 ELSE 0 END) AS frost_days,
         SUM(precip_mm) AS precip_total,
         SUM(CASE WHEN precip_mm < 1 THEN 1 ELSE 0 END) AS dry_days,
         AVG(soil_wetness_root) AS soil_mean,
         MIN(soil_wetness_root) AS soil_min,
         percentile_approx(soil_wetness_root, 0.95) AS soil_p95,
         AVG(rh2m_pct) AS rh_mean
  FROM agri_dwh.fact_weather_daily
  GROUP BY location_id, year(date_key)
),
flags AS (
  SELECT location_id, year(date_key) AS year, date_key,
         CASE WHEN precip_mm < 1 THEN 1 ELSE 0 END AS dry
  FROM agri_dwh.fact_weather_daily
),
grouped AS (
  SELECT location_id, year, date_key, dry,
         date_sub(date_key, CAST(SUM(dry) OVER (
           PARTITION BY location_id, year ORDER BY date_key
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS INT)) AS spell_group
  FROM flags
),
spells AS (
  SELECT location_id, year, spell_group, COUNT(*) AS spell_length
  FROM grouped WHERE dry = 1
  GROUP BY location_id, year, spell_group
),
max_spell AS (
  SELECT location_id, year, MAX(spell_length) AS max_dry_spell
  FROM spells GROUP BY location_id, year
),
country_year AS (
  SELECT l.country_iso3,
         ly.year,
         AVG(ly.gdd) AS gdd,
         AVG(ly.heat_days_35) AS heat_days_35,
         AVG(ly.frost_days) AS frost_days,
         AVG(ly.precip_total) AS precip_total,
         AVG(ly.dry_days) AS dry_days,
         AVG(COALESCE(m.max_dry_spell, 0)) AS max_dry_spell,
         AVG(ly.soil_mean) AS soil_mean,
         AVG(ly.soil_min) AS soil_min,
         AVG(ly.soil_p95) AS soil_p95,
         AVG(ly.rh_mean) AS rh_mean,
         AVG(l.latitude) AS latitude,
         AVG(l.longitude) AS longitude
  FROM loc_year ly
  JOIN agri_dwh.dim_location l ON l.location_id = ly.location_id
  LEFT JOIN max_spell m ON m.location_id = ly.location_id AND m.year = ly.year
  GROUP BY l.country_iso3, ly.year
)
SELECT cy.country_iso3, cy.year, cy.gdd, cy.heat_days_35, cy.frost_days,
       cy.precip_total, cy.dry_days, cy.max_dry_spell,
       cy.soil_mean, cy.soil_min, cy.soil_p95, cy.rh_mean,
       cy.latitude, cy.longitude,
       y.cereal_yield_kg_per_ha AS yield_kg_ha
FROM country_year cy
JOIN agri_dwh.fact_cereal_annual y
  ON y.country_iso3 = cy.country_iso3 AND y.year = cy.year
"""


def main():
    spark = spark_session("agri_06_build_features")
    features = spark.sql(SQL)
    write_table(spark, features, "ml_yield_features", OUTPUT)

    years = features.select("year").distinct().count()
    countries = features.select("country_iso3").distinct().count()
    payload = {
        "rows": table_count(spark, "agri_dwh.ml_yield_features"),
        "countries": countries,
        "years": years,
    }
    write_metrics(METRICS, payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
