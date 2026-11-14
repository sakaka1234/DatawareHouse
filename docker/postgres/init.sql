-- One Postgres instance, three isolated databases (metastore, airflow, superset).
-- Hive 2.3 ships a JDBC driver that cannot speak SCRAM, so store md5 hashes.
SET password_encryption = 'md5';

CREATE USER hive WITH PASSWORD 'hive';
CREATE DATABASE metastore OWNER hive;

CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER superset WITH PASSWORD 'superset';
CREATE DATABASE superset OWNER superset;
