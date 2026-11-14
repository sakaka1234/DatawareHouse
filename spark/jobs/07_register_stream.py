"""07 - Register the streaming Parquet sink as a Hive table and compute metrics."""

import json
import sys

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import HDFS, spark_session, table_count, write_metrics  # noqa: E402

OUTPUT = HDFS + "/data/agri/processed/stream_weather"
METRICS = "/opt/agri/artifacts/metrics/stream_sink.json"


def main():
    spark = spark_session("agri_07_register_stream")
    spark.sql("DROP TABLE IF EXISTS agri_dwh.fact_weather_stream")
    spark.sql(
        """
        CREATE EXTERNAL TABLE agri_dwh.fact_weather_stream (
          event_time TIMESTAMP, location_id STRING, t2m_c DOUBLE, rh2m_pct DOUBLE,
          precip_mm_h DOUBLE, ws2m_ms DOUBLE, soil_wetness_root DOUBLE,
          soil_wetness_top DOUBLE, produced_at BIGINT, batch_id STRING,
          ingest_time TIMESTAMP, latency_ms BIGINT
        )
        PARTITIONED BY (year INT, month INT)
        STORED AS PARQUET
        LOCATION '%s'
        """
        % OUTPUT
    )
    spark.sql("MSCK REPAIR TABLE agri_dwh.fact_weather_stream")

    rows = table_count(spark, "agri_dwh.fact_weather_stream")
    distinct = (
        spark.sql(
            "SELECT COUNT(*) AS c FROM (SELECT DISTINCT location_id, event_time FROM agri_dwh.fact_weather_stream) t"
        ).collect()[0]["c"]
        if rows
        else 0
    )
    percentile = 0.0
    max_latency = 0
    if rows:
        latency = spark.sql(
            "SELECT percentile_approx(latency_ms, 0.95) AS p95, MAX(latency_ms) AS mx FROM agri_dwh.fact_weather_stream"
        ).collect()[0]
        percentile = latency["p95"] or 0
        max_latency = latency["mx"] or 0

    payload = {
        "rows": rows,
        "distinct_location_events": distinct,
        "duplicate_rows": rows - distinct,
        "latency_p95_ms": percentile,
        "latency_max_ms": max_latency,
    }
    write_metrics(METRICS, payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
