"""Data-quality gate used by Airflow and by the bad-batch demonstration.

Exit code 0 = every check passed. Exit code 1 = the gate blocks the load.
"""

import argparse
import json
import os
import sys

from common import artifacts_path, connect, write_json

CHECKS = [
    ("hourly_rows", "SELECT COUNT(*) FROM agri_dwh.fact_weather_hourly", lambda v: v == 1052064),
    (
        "duplicate_keys",
        "SELECT COUNT(*) FROM (SELECT location_id, event_time, COUNT(*) c "
        "FROM agri_dwh.fact_weather_hourly GROUP BY location_id, event_time "
        "HAVING c > 1) t",
        lambda v: v == 0,
    ),
    (
        "null_core_fields",
        "SELECT SUM(CASE WHEN t2m_c IS NULL THEN 1 ELSE 0 END) + "
        "SUM(CASE WHEN soil_wetness_root IS NULL THEN 1 ELSE 0 END) + "
        "SUM(CASE WHEN precip_mm_h IS NULL THEN 1 ELSE 0 END) "
        "FROM agri_dwh.fact_weather_hourly",
        lambda v: v == 0,
    ),
    (
        "temperature_range",
        "SELECT COUNT(*) FROM agri_dwh.fact_weather_hourly WHERE t2m_c < -40 OR t2m_c > 55",
        lambda v: v == 0,
    ),
    (
        "soil_range",
        "SELECT COUNT(*) FROM agri_dwh.fact_weather_hourly "
        "WHERE soil_wetness_root < 0 OR soil_wetness_root > 1",
        lambda v: v == 0,
    ),
    (
        "hourly_locations",
        "SELECT COUNT(DISTINCT location_id) FROM agri_dwh.fact_weather_hourly",
        lambda v: v == 12,
    ),
    (
        "partitions",
        "SELECT COUNT(*) FROM (SELECT DISTINCT year, month FROM agri_dwh.fact_weather_hourly) t",
        lambda v: v == 120,
    ),
    (
        "view_join",
        "SELECT COUNT(*) FROM agri_dwh.v_benchmark_like_hourly",
        lambda v: v == 1052064,
    ),
    ("daily_rows", "SELECT COUNT(*) FROM agri_dwh.fact_weather_daily", lambda v: v == 175328),
    ("locations", "SELECT COUNT(*) FROM agri_dwh.dim_location", lambda v: v == 32),
    ("yield_rows", "SELECT COUNT(*) FROM agri_dwh.fact_cereal_annual", lambda v: v == 450),
    ("feature_rows", "SELECT COUNT(*) FROM agri_dwh.ml_yield_features", lambda v: v == 450),
]

BAD_CHECKS = [
    (
        "bad_temperature",
        "SELECT COUNT(*) FROM agri_raw.bad_weather_hourly WHERE t2m_c < -40 OR t2m_c > 55",
    ),
    (
        "bad_precip",
        "SELECT COUNT(*) FROM agri_raw.bad_weather_hourly WHERE precip_mm_h < 0",
    ),
    (
        "bad_location",
        "SELECT COUNT(*) FROM agri_raw.bad_weather_hourly "
        "WHERE location_id IS NULL OR location_id = ''",
    ),
]


def run_check(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()[0][0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", default=None)
    parser.add_argument("--bad", action="store_true")
    args = parser.parse_args()

    connection = connect()
    results = []
    try:
        cursor = connection.cursor()
        if args.bad:
            for name, sql in BAD_CHECKS:
                try:
                    value = run_check(cursor, sql)
                    results.append({"check": name, "value": int(value), "passed": int(value) == 0})
                except Exception as exc:
                    results.append({"check": name, "value": None, "passed": False, "error": str(exc)})
        else:
            for name, sql, predicate in CHECKS:
                try:
                    value = run_check(cursor, sql)
                    results.append(
                        {"check": name, "value": int(value), "passed": bool(predicate(value))}
                    )
                except Exception as exc:
                    results.append({"check": name, "value": None, "passed": False, "error": str(exc)})
    finally:
        connection.close()

    report = {
        "mode": "bad_batch" if args.bad else "warehouse",
        "checks": results,
        "failed": [item["check"] for item in results if not item["passed"]],
        "passed": all(item["passed"] for item in results),
    }
    output = args.report or artifacts_path("quality", "bad_batch_report.json" if args.bad else "quality_report.json")
    os.makedirs(os.path.dirname(output), exist_ok=True)
    write_json(output, report)
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
