"""Warehouse build, Parquet lake, quality checks and benchmark helpers."""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import pandas as pd

from config import (
    CSV_DAILY,
    CSV_HOURLY,
    CSV_LOCATION,
    CSV_YIELD,
    DB_PATH,
    PARQUET_DIR,
    WAREHOUSE_DIR,
    duckdb_connection,
    ensure_dirs,
)


def connect(read_only: bool = False):
    return duckdb_connection(read_only=read_only)


def _p(path: Path) -> str:
    return path.as_posix()


def create_base_tables(con) -> None:
    """Load the real CSVs and build the core warehouse tables."""
    ensure_dirs()

    con.execute(
        f"""
        CREATE OR REPLACE TABLE dim_location AS
        SELECT location_id, name, country_iso3, country_name,
               CAST(latitude AS DOUBLE) AS latitude,
               CAST(longitude AS DOUBLE) AS longitude,
               crop_system
        FROM read_csv_auto('{_p(CSV_LOCATION)}', header=true)
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE fact_cereal_annual AS
        SELECT country_iso3, country_name, CAST(year AS INTEGER) AS year,
               CAST(cereal_area_ha AS DOUBLE) AS cereal_area_ha,
               CAST(cereal_production_mt AS DOUBLE) AS cereal_production_mt,
               CAST(cereal_yield_kg_per_ha AS DOUBLE) AS cereal_yield_kg_per_ha
        FROM read_csv_auto('{_p(CSV_YIELD)}', header=true)
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE fact_weather_daily AS
        SELECT CAST(date AS DATE) AS date_key, location_id,
               CAST(t2m_c AS DOUBLE) AS t2m_c,
               CAST(t2m_max_c AS DOUBLE) AS t2m_max_c,
               CAST(t2m_min_c AS DOUBLE) AS t2m_min_c,
               CAST(precip_mm AS DOUBLE) AS precip_mm,
               CAST(soil_wetness_root AS DOUBLE) AS soil_wetness_root,
               CAST(soil_wetness_top AS DOUBLE) AS soil_wetness_top,
               CAST(rh2m_pct AS DOUBLE) AS rh2m_pct
        FROM read_csv_auto('{_p(CSV_DAILY)}', header=true)
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE fact_weather_hourly AS
        WITH raw AS (
            SELECT CAST(timestamp AS TIMESTAMP) AS event_time,
                   location_id,
                   CAST(t2m_c AS DOUBLE) AS t2m_c,
                   CAST(rh2m_pct AS DOUBLE) AS rh2m_pct,
                   CAST(precip_mm_h AS DOUBLE) AS precip_mm_h,
                   CAST(ws2m_ms AS DOUBLE) AS ws2m_ms,
                   CAST(soil_wetness_root AS DOUBLE) AS soil_wetness_root,
                   CAST(soil_wetness_top AS DOUBLE) AS soil_wetness_top
            FROM read_csv_auto('{_p(CSV_HOURLY)}', header=true)
        ),
        ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY location_id, event_time ORDER BY event_time
            ) AS rn
            FROM raw
        )
        SELECT event_time, location_id, t2m_c, rh2m_pct, precip_mm_h,
               ws2m_ms, soil_wetness_root, soil_wetness_top,
               CAST(year(event_time) AS INTEGER) AS year,
               CAST(month(event_time) AS INTEGER) AS month
        FROM ranked
        WHERE rn = 1
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE dim_date AS
        WITH days AS (
            SELECT unnest(generate_series(
                DATE '2015-01-01', DATE '2024-12-31', INTERVAL 1 DAY
            )) AS date_key
        )
        SELECT date_key,
               CAST(year(date_key) AS INTEGER) AS year,
               CAST(month(date_key) AS INTEGER) AS month,
               CAST(day(date_key) AS INTEGER) AS day,
               CAST(quarter(date_key) AS INTEGER) AS quarter,
               strftime(date_key, '%A') AS weekday
        FROM days
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW v_benchmark_like_hourly AS
        SELECT h.event_time AS ts,
               h.location_id AS location,
               h.soil_wetness_root AS soil_moisture,
               h.t2m_c AS temperature,
               y.cereal_yield_kg_per_ha AS yield_estimate,
               'annual_country' AS yield_grain
        FROM fact_weather_hourly h
        JOIN dim_location l ON l.location_id = h.location_id
        JOIN fact_cereal_annual y
          ON y.country_iso3 = l.country_iso3
         AND y.year = h.year
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW v_benchmark_like_daily AS
        SELECT d.date_key AS ts,
               d.location_id AS location,
               d.soil_wetness_root AS soil_moisture,
               d.t2m_c AS temperature,
               y.cereal_yield_kg_per_ha AS yield_estimate,
               'annual_country' AS yield_grain
        FROM fact_weather_daily d
        JOIN dim_location l ON l.location_id = d.location_id
        JOIN fact_cereal_annual y
          ON y.country_iso3 = l.country_iso3
         AND y.year = year(d.date_key)
        """
    )


def write_parquet_lake(con, overwrite: bool = True) -> dict:
    """Write Hive-style partitioned Parquet and register a lake view."""
    target = PARQUET_DIR / "fact_weather_hourly"
    if overwrite and target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)

    con.execute(
        f"""
        COPY (
            SELECT event_time, location_id, t2m_c, rh2m_pct, precip_mm_h,
                   ws2m_ms, soil_wetness_root, soil_wetness_top, year, month
            FROM fact_weather_hourly
        ) TO '{_p(target)}'
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE)
        """
    )

    for table in ("dim_location", "fact_weather_daily", "fact_cereal_annual"):
        con.execute(
            f"COPY (SELECT * FROM {table}) TO '{_p(PARQUET_DIR / (table + '.parquet'))}' "
            f"(FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)"
        )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW fact_weather_hourly_lake AS
        SELECT * FROM read_parquet(
            '{_p(target)}/year=*/month=*/*.parquet',
            hive_partitioning = true
        )
        """
    )

    files = list(target.rglob("*.parquet"))
    return {
        "parquet_dir": str(target),
        "partition_files": len(files),
        "lake_rows": con.execute(
            "SELECT COUNT(*) FROM fact_weather_hourly_lake"
        ).fetchone()[0],
    }


def data_quality_report(con) -> pd.DataFrame:
    checks = []
    total = con.execute("SELECT COUNT(*) FROM fact_weather_hourly").fetchone()[0]
    checks.append(("Số dòng hourly", f"{total:,}", "PASS" if total == 1_052_064 else "FAIL"))

    dup = con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT location_id, event_time, COUNT(*) AS c
            FROM fact_weather_hourly
            GROUP BY 1, 2 HAVING c > 1
        )
        """
    ).fetchone()[0]
    checks.append(("Duplicate (location_id, event_time)", f"{dup:,}", "PASS" if dup == 0 else "FAIL"))

    nulls = con.execute(
        """
        SELECT SUM(CASE WHEN t2m_c IS NULL THEN 1 ELSE 0 END) +
               SUM(CASE WHEN soil_wetness_root IS NULL THEN 1 ELSE 0 END) +
               SUM(CASE WHEN precip_mm_h IS NULL THEN 1 ELSE 0 END)
        FROM fact_weather_hourly
        """
    ).fetchone()[0]
    checks.append(("Null ở 3 biến chính", f"{nulls:,}", "PASS" if nulls == 0 else "FAIL"))

    lo, hi = con.execute("SELECT MIN(t2m_c), MAX(t2m_c) FROM fact_weather_hourly").fetchone()
    checks.append(("Nhiệt độ hourly (°C)", f"{lo:.1f} … {hi:.1f}", "PASS" if -40 <= lo and hi <= 55 else "FAIL"))

    slo, shi = con.execute(
        "SELECT MIN(soil_wetness_root), MAX(soil_wetness_root) FROM fact_weather_hourly"
    ).fetchone()
    checks.append(("Độ ẩm đất (0–1)", f"{slo:.2f} … {shi:.2f}", "PASS" if slo >= 0 and shi <= 1 else "FAIL"))

    locs = con.execute("SELECT COUNT(DISTINCT location_id) FROM fact_weather_hourly").fetchone()[0]
    checks.append(("Số địa điểm", f"{locs}", "PASS" if locs == 12 else "FAIL"))

    months = con.execute(
        "SELECT COUNT(DISTINCT (year * 100 + month)) FROM fact_weather_hourly"
    ).fetchone()[0]
    checks.append(("Số partition year-month", f"{months}", "PASS" if months == 120 else "FAIL"))

    joined = con.execute("SELECT COUNT(*) FROM v_benchmark_like_hourly").fetchone()[0]
    checks.append(("View benchmark join được", f"{joined:,}", "PASS" if joined == total else "FAIL"))

    return pd.DataFrame(checks, columns=["Kiểm tra", "Giá trị", "Kết quả"])


BENCHMARK_QUERIES = [
    ("Đếm toàn bộ dòng", "SELECT COUNT(*) FROM bench"),
    ("Năng suất TB theo khu", "SELECT location, AVG(yield_estimate) FROM bench GROUP BY location"),
    (
        "Độ ẩm TB khi nhiệt > 25",
        "SELECT location, AVG(soil_moisture) FROM bench WHERE temperature > 25 GROUP BY location",
    ),
    ("Top 10 theo năng suất", "SELECT * FROM bench ORDER BY yield_estimate DESC LIMIT 10"),
]


def run_benchmark(con, scales=(100_000, 1_052_064), repeats: int = 3) -> pd.DataFrame:
    rows = []
    for scale in scales:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE bench AS SELECT * FROM v_benchmark_like_hourly LIMIT {int(scale)}"
        )
        for name, sql in BENCHMARK_QUERIES:
            result = con.execute(sql).fetchall()
            con.execute(sql).fetchall()  # warm-up
            times = []
            for _ in range(repeats):
                start = time.perf_counter()
                result = con.execute(sql).fetchall()
                times.append(time.perf_counter() - start)
            rows.append(
                {
                    "scale": scale,
                    "query": name,
                    "seconds_median": round(sorted(times)[len(times) // 2], 6),
                    "result_rows": len(result),
                }
            )
    return pd.DataFrame(rows)


def table_summary(con) -> pd.DataFrame:
    rows = []
    for table in (
        "dim_location",
        "fact_weather_hourly",
        "fact_weather_daily",
        "fact_cereal_annual",
        "dim_date",
    ):
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        rows.append({"table": table, "rows": count})
    return pd.DataFrame(rows)


def reset_database_file() -> None:
    ensure_dirs()
    if DB_PATH.exists():
        DB_PATH.unlink()
