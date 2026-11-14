-- Databases. Run with: beeline -u jdbc:hive2://localhost:10000 -n hive -f 00_databases.sql
CREATE DATABASE IF NOT EXISTS agri_raw
  LOCATION 'hdfs://namenode:9000/user/hive/warehouse/agri_raw.db';
CREATE DATABASE IF NOT EXISTS agri_dwh
  LOCATION 'hdfs://namenode:9000/user/hive/warehouse/agri_dwh.db';
