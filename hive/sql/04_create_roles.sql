-- Run once. Re-running is harmless: Hive rejects an existing role.
SET ROLE ADMIN;
CREATE ROLE analyst;
CREATE ROLE ml_engineer;
