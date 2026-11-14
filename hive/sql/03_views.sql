-- Views are created by spark/jobs/05_build_views.py. This copy is the canonical
-- definition for Hive/reference after a restart.
DROP VIEW IF EXISTS agri_dwh.v_benchmark_like_hourly;
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
 AND y.year = h.year;

DROP VIEW IF EXISTS agri_dwh.v_benchmark_like_daily;
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
 AND y.year = year(d.date_key);
