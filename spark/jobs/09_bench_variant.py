"""09 - Create an unpartitioned Parquet copy of the hourly fact for benchmarking."""

import json
import sys

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import hdfs_path, spark_session, table_count, write_metrics, write_table  # noqa: E402

OUTPUT = hdfs_path("bench_parquet_unpartitioned")
METRICS = "/opt/agri/artifacts/metrics/09_bench_variant.json"


def main():
    spark = spark_session("agri_09_bench_variant")
    df = spark.sql(
        """
        SELECT event_time, location_id, t2m_c, rh2m_pct, precip_mm_h,
               ws2m_ms, soil_wetness_root, soil_wetness_top
        FROM agri_dwh.fact_weather_hourly
        """
    )
    write_table(spark, df, "bench_parquet_unpartitioned", OUTPUT)
    payload = {
        "rows": table_count(spark, "agri_dwh.bench_parquet_unpartitioned"),
        "output": OUTPUT,
    }
    write_metrics(METRICS, payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
