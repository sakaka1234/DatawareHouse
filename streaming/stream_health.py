"""Stream health check: topic growth, approximate lag, sink row count."""

import json
import os
import time

from kafka import KafkaConsumer, TopicPartition

from common import artifacts_path, query_rows, write_json

TOPIC = "agri.weather.hourly"
MAX_LAG = 200000


def main():
    consumer = KafkaConsumer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        group_id=None,
        enable_auto_commit=False,
    )
    try:
        partitions = [
            TopicPartition(TOPIC, p)
            for p in (consumer.partitions_for_topic(TOPIC) or [])
        ]
        if not partitions:
            payload = {"ok": False, "reason": "topic missing"}
            write_json(artifacts_path("metrics", "stream_health.json"), payload)
            print(json.dumps(payload))
            return 1
        end = consumer.end_offsets(partitions)
        total = int(sum(end.values()))
    finally:
        consumer.close()

    sink_rows = 0
    try:
        for rows in query_rows("SELECT COUNT(*) FROM agri_dwh.fact_weather_stream"):
            sink_rows = int(rows[0][0])
    except Exception as exc:
        sink_rows = 0
        print("sink not registered yet:", exc)

    lag = max(0, total - sink_rows)
    payload = {
        "ok": lag <= MAX_LAG,
        "topic": TOPIC,
        "topic_messages": total,
        "approx_consumer_lag": int(lag),
        "sink_rows": sink_rows,
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    write_json(artifacts_path("metrics", "stream_health.json"), payload)
    print(json.dumps(payload, indent=2))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
