"""10 - Run the four benchmark queries with Spark SQL and emit timings."""

import argparse
import json
import statistics
import sys
import time

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import spark_session  # noqa: E402

CSV_JOIN = """
SELECT h.ts, h.location, h.soil_moisture, h.temperature,
       y.cereal_yield_kg_per_ha AS yield_estimate
FROM (
  SELECT CAST(`timestamp` AS TIMESTAMP) AS ts,
         location_id AS location,
         soil_wetness_root AS soil_moisture,
         t2m_c AS temperature,
         year(CAST(`timestamp` AS TIMESTAMP)) AS year
  FROM agri_raw.weather_hourly_csv
) h
JOIN agri_dwh.dim_location l ON l.location_id = h.location
JOIN agri_dwh.fact_cereal_annual y
  ON y.country_iso3 = l.country_iso3 AND y.year = h.year
"""

UNPARTITIONED_JOIN = """
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
    (
        "top10_yield",
        "SELECT * FROM bench ORDER BY yield_estimate DESC LIMIT 10",
    ),
]


def source_sql(source, scale):
    if source == "csv":
        base = CSV_JOIN
    elif source == "parquet_partitioned":
        base = "SELECT ts, location, soil_moisture, temperature, yield_estimate FROM agri_dwh.v_benchmark_like_hourly"
    elif source == "parquet_unpartitioned":
        base = UNPARTITIONED_JOIN
    else:
        raise SystemExit("unknown source " + source)
    return "SELECT * FROM (%s) q LIMIT %d" % (base, int(scale))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--scale", type=int, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = spark_session("agri_10_benchmark")
    spark.sql("CREATE OR REPLACE TEMP VIEW bench AS " + source_sql(args.source, args.scale))
    rows = spark.sql("SELECT COUNT(*) AS c FROM bench").collect()[0]["c"]

    results = []
    for name, sql in QUERIES:
        spark.sql(sql).collect()  # warm up
        samples = []
        result_rows = 0
        for _ in range(2):
            started = time.perf_counter()
            result = spark.sql(sql).collect()
            samples.append(time.perf_counter() - started)
            result_rows = len(result)
        results.append(
            {
                "engine": "spark_sql",
                "source": args.source,
                "scale": args.scale,
                "query": name,
                "seconds_median": round(statistics.median(samples), 3),
                "result_rows": result_rows,
            }
        )

    with open(args.output, "w") as handle:
        json.dump({"bench_rows": rows, "results": results}, handle, indent=2)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
