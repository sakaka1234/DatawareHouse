"""Prometheus exporter for HDFS, Spark and pipeline metrics."""

import json
import os
import time

import requests
from prometheus_client import Gauge, start_http_server

HDFS = os.environ.get("HDFS_WEBHDFS", "http://namenode:9870")
SPARK = os.environ.get("SPARK_MASTER_REST", "http://spark-master:8080")
ARTIFACTS = os.environ.get("ARTIFACTS_DIR", "/opt/agri/artifacts")

capacity = Gauge("agri_hdfs_capacity_bytes", "HDFS capacity")
used = Gauge("agri_hdfs_used_bytes", "HDFS used bytes")
data_bytes = Gauge("agri_hdfs_data_bytes", "Bytes under /data/agri")
spark_apps = Gauge("agri_spark_active_apps", "Active Spark applications")
producer_rows = Gauge("agri_producer_rows_total", "Rows sent by the last producer run")
producer_rate = Gauge("agri_producer_rate", "Messages per second of the last producer run")
stream_rows = Gauge("agri_stream_rows_total", "Rows in the streaming sink")
stream_p95 = Gauge("agri_stream_latency_p95_ms", "p95 ingest latency")
warehouse_rows = Gauge("agri_warehouse_hourly_rows", "Hourly warehouse row count")


def read_json(name):
    try:
        with open(os.path.join(ARTIFACTS, "metrics", name)) as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return {}


def update():
    try:
        response = requests.get(
            HDFS + "/jmx", params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}, timeout=5
        )
        bean = response.json()["beans"][0]
        capacity.set(float(bean.get("CapacityTotal", 0)))
        used.set(float(bean.get("CapacityUsed", 0)))
    except Exception:
        pass

    try:
        response = requests.get(
            HDFS + "/webhdfs/v1/data/agri", params={"op": "GETCONTENTSUMMARY"}, timeout=5
        )
        data_bytes.set(float(response.json()["ContentSummary"]["length"]))
    except Exception:
        pass

    try:
        response = requests.get(SPARK + "/json/", timeout=5)
        spark_apps.set(float(len(response.json().get("activeapps", []))))
    except Exception:
        pass

    metrics = read_json("producer.json")
    if metrics:
        producer_rows.set(float(metrics.get("rows_sent", 0)))
        producer_rate.set(float(metrics.get("messages_per_second", 0)))

    sink = read_json("stream_sink.json")
    if sink:
        stream_rows.set(float(sink.get("rows", 0)))
        stream_p95.set(float(sink.get("latency_p95_ms", 0)))

    warehouse = read_json("01_clean_weather.json")
    if warehouse:
        warehouse_rows.set(float(warehouse.get("rows", 0)))


def main():
    start_http_server(9310)
    while True:
        update()
        time.sleep(10)


if __name__ == "__main__":
    main()
