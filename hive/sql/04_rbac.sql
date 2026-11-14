-- Role based access control, enforced by HiveServer2 SQL standard authorization.
-- Run as the admin user (hive): beeline -u jdbc:hive2://localhost:10000 -n hive -f 04_rbac.sql
SET ROLE ADMIN;

GRANT SELECT ON TABLE agri_dwh.v_benchmark_like_hourly TO ROLE analyst;
GRANT SELECT ON TABLE agri_dwh.v_benchmark_like_daily TO ROLE analyst;

GRANT SELECT ON TABLE agri_dwh.fact_weather_hourly TO ROLE ml_engineer;
GRANT SELECT ON TABLE agri_dwh.fact_weather_daily TO ROLE ml_engineer;
GRANT SELECT ON TABLE agri_dwh.fact_cereal_annual TO ROLE ml_engineer;
GRANT SELECT ON TABLE agri_dwh.dim_location TO ROLE ml_engineer;
GRANT SELECT ON TABLE agri_dwh.v_benchmark_like_hourly TO ROLE ml_engineer;

GRANT ROLE analyst TO USER analyst;
GRANT ROLE ml_engineer TO USER ml_engineer;
