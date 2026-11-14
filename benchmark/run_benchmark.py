"""Hive vs Spark SQL benchmark on the real 1M-row table."""

import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import time

from common import artifacts_path, connect, spark_submit, write_json

QUERIES = [
    ("count_all", "SELECT COUNT(*) FROM bench"),
    (
        "avg_yield_by_location",
        "SELECT location, AVG(yield_estimate) AS avg_yield FROM bench GROUP BY location",
    ),
    (
        "avg_soil_when_hot",
        "SELECT location, AVG(soil_moisture) AS avg_soil FROM bench "
        "WHERE temperature > 25 GROUP BY location",
    ),
    ("top10_yield", "SELECT * FROM bench ORDER BY yield_estimate DESC LIMIT 10"),
]

SOURCES = ["csv", "parquet_partitioned", "parquet_unpartitioned"]
SCALES = [100000, 1052064]


def source_sql_hive(source, scale):
    if source == "csv":
        base = """
        SELECT h.ts, h.location, h.soil_moisture, h.temperature,
               y.cereal_yield_kg_per_ha AS yield_estimate
        FROM (
          SELECT to_timestamp(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss') AS ts,
                 location_id AS location, soil_wetness_root AS soil_moisture,
                 t2m_c AS temperature,
                 year(to_timestamp(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss')) AS year
          FROM agri_raw.weather_hourly_csv
        ) h
        JOIN agri_dwh.dim_location l ON l.location_id = h.location
        JOIN agri_dwh.fact_cereal_annual y
          ON y.country_iso3 = l.country_iso3 AND y.year = h.year
        """
    elif source == "parquet_partitioned":
        base = "SELECT ts, location, soil_moisture, temperature, yield_estimate FROM agri_dwh.v_benchmark_like_hourly"
    else:
        base = """
        SELECT h.ts, h.location, h.soil_moisture, h.temperature,
               y.cereal_yield_kg_per_ha AS yield_estimate
        FROM (
          SELECT event_time AS ts, location_id AS location,
                 soil_wetness_root AS soil_moisture, t2m_c AS temperature,
                 year(event_time) AS year
          FROM agri_dwh.bench_parquet_unpartitioned
        ) h
        JOIN agri_dwh.dim_location l ON l.location_id = h.location
        JOIN agri_dwh.fact_cereal_annual y
          ON y.country_iso3 = l.country_iso3 AND y.year = h.year
        """
    return "SELECT * FROM (%s) q LIMIT %d" % (base, scale)


def hive_benchmark(source, scale):
    results = []
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("SET hive.exec.mode.local.auto=true")
        source_sql = source_sql_hive(source, scale)
        for name, sql in QUERIES:
            started = time.perf_counter()
            cursor.execute(sql.replace("FROM bench", "FROM (%s) bench" % source_sql))
            rows = cursor.fetchall()
            elapsed = time.perf_counter() - started
            results.append(
                {
                    "engine": "hive",
                    "source": source,
                    "scale": scale,
                    "query": name,
                    "seconds_median": round(elapsed, 3),
                    "result_rows": len(rows),
                }
            )
    finally:
        connection.close()
    return results


def spark_benchmark(source, scale):
    output = artifacts_path("bench", "spark_%s_%d.json" % (source, scale))
    os.makedirs(os.path.dirname(output), exist_ok=True)
    completed = spark_submit(
        "10_benchmark.py",
        ["--source", source, "--scale", str(scale), "--output", output],
        timeout=1800,
    )
    if completed.returncode != 0:
        print("spark benchmark failed for", source, scale)
        print((completed.stdout or "")[-1500:])
        print((completed.stderr or "")[-1500:])
        return []
    with open(output) as handle:
        return json.load(handle)["results"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=["hive", "spark", "all"], default="all")
    parser.add_argument("--scales", default="100000,1052064")
    parser.add_argument("--hive-sources", default="parquet_partitioned")
    parser.add_argument("--spark-sources", default="csv,parquet_partitioned,parquet_unpartitioned")
    args = parser.parse_args()
    scales = [int(value) for value in args.scales.split(",")]

    rows = []
    if args.only in ("hive", "all"):
        for source in args.hive_sources.split(","):
            for scale in scales:
                print("hive", source, scale, flush=True)
                rows.extend(hive_benchmark(source, scale))
    if args.only in ("spark", "all"):
        for source in args.spark_sources.split(","):
            for scale in scales:
                print("spark", source, scale, flush=True)
                rows.extend(spark_benchmark(source, scale))

    reports = artifacts_path("bench")
    os.makedirs(reports, exist_ok=True)
    output = os.path.join(reports, "fullstack_benchmark.csv")
    if os.path.exists(output):
        import pandas as pd

        previous = pd.read_csv(output).to_dict("records")
        merged = {(r["engine"], r["source"], r["scale"], r["query"]): r for r in previous}
        for row in rows:
            merged[(row["engine"], row["source"], row["scale"], row["query"])] = row
        rows = list(merged.values())
    with open(output, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    write_json(os.path.join(reports, "fullstack_benchmark.json"), {"results": rows})
    print("wrote", output, len(rows), "rows")


if __name__ == "__main__":
    main()
