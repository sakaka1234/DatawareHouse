-- Warehouse tables. Spark writes the Parquet files first, then re-registers these
-- external tables (see spark/jobs/common.py). This file documents the contract
-- and lets Hive recover the tables after a cluster restart.
CREATE EXTERNAL TABLE IF NOT EXISTS agri_dwh.fact_weather_hourly (
  event_time TIMESTAMP,
  location_id STRING,
  t2m_c DOUBLE,
  rh2m_pct DOUBLE,
  precip_mm_h DOUBLE,
  ws2m_ms DOUBLE,
  soil_wetness_root DOUBLE,
  soil_wetness_top DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/agri/processed/fact_weather_hourly';

CREATE EXTERNAL TABLE IF NOT EXISTS agri_dwh.fact_weather_daily (
  date_key DATE,
  location_id STRING,
  t2m_c DOUBLE,
  t2m_max_c DOUBLE,
  t2m_min_c DOUBLE,
  precip_mm DOUBLE,
  soil_wetness_root DOUBLE,
  soil_wetness_top DOUBLE,
  rh2m_pct DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/agri/processed/fact_weather_daily';

CREATE EXTERNAL TABLE IF NOT EXISTS agri_dwh.dim_location (
  location_id STRING,
  `name` STRING,
  country_iso3 STRING,
  country_name STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  crop_system STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/agri/processed/dim_location';

CREATE EXTERNAL TABLE IF NOT EXISTS agri_dwh.dim_date (
  date_key DATE,
  `year` INT,
  `month` INT,
  `day` INT,
  quarter INT,
  weekday STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/agri/processed/dim_date';

CREATE EXTERNAL TABLE IF NOT EXISTS agri_dwh.fact_cereal_annual (
  country_iso3 STRING,
  country_name STRING,
  `year` INT,
  cereal_area_ha DOUBLE,
  cereal_production_mt DOUBLE,
  cereal_yield_kg_per_ha DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/agri/processed/fact_cereal_annual';
