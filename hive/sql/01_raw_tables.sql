-- Raw landing tables over the CSV files uploaded to HDFS.
CREATE EXTERNAL TABLE IF NOT EXISTS agri_raw.weather_hourly_csv (
  `timestamp` STRING,
  location_id STRING,
  t2m_c DOUBLE,
  rh2m_pct DOUBLE,
  precip_mm_h DOUBLE,
  ws2m_ms DOUBLE,
  soil_wetness_root DOUBLE,
  soil_wetness_top DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/agri/raw/hourly'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS agri_raw.weather_daily_csv (
  `date` STRING,
  location_id STRING,
  t2m_c DOUBLE,
  t2m_max_c DOUBLE,
  t2m_min_c DOUBLE,
  precip_mm DOUBLE,
  soil_wetness_root DOUBLE,
  soil_wetness_top DOUBLE,
  rh2m_pct DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/agri/raw/daily'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS agri_raw.cereal_annual_csv (
  country_iso3 STRING,
  country_name STRING,
  `year` INT,
  cereal_area_ha DOUBLE,
  cereal_production_mt DOUBLE,
  cereal_yield_kg_per_ha DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/agri/raw/yield'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS agri_raw.location_csv (
  location_id STRING,
  `name` STRING,
  country_iso3 STRING,
  country_name STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  crop_system STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/agri/raw/location'
TBLPROPERTIES ('skip.header.line.count' = '1');
