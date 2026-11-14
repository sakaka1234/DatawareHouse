"""Replay historical hourly weather into Kafka.

The producer reads the real CSV, orders rows by event time, and sends a window
of rows as JSON. It can deliberately duplicate a fraction of messages and can
inject one very old row to prove that the watermark drops late data.
"""

import argparse
import json
import os
import random
import time

import pandas as pd
from kafka import KafkaProducer

from common import artifacts_path, write_json

CSV = os.environ.get("HOURLY_CSV", "/data/agri/real_data/fact_weather_hourly.csv")
TOPIC = "agri.weather.hourly"
METRICS = artifacts_path("metrics", "producer.json")


def build_producer():
    return KafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks=1,
        linger_ms=0,
        retries=3,
        request_timeout_ms=15000,
    )


def message(row, batch_id):
    return {
        "location_id": row.location_id,
        "timestamp": row.timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
        "t2m_c": float(row.t2m_c),
        "rh2m_pct": float(row.rh2m_pct),
        "precip_mm_h": float(row.precip_mm_h),
        "ws2m_ms": float(row.ws2m_ms),
        "soil_wetness_root": float(row.soil_wetness_root),
        "soil_wetness_top": float(row.soil_wetness_top),
        "produced_at": int(time.time() * 1000),
        "batch_id": batch_id,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--end-offset", type=int, default=0,
                        help="rows to leave out at the end of the sorted frame")
    parser.add_argument("--rate", type=int, default=8000, help="target messages/second")
    parser.add_argument("--duplicate-rate", type=float, default=0.0)
    parser.add_argument("--inject-late", action="store_true")
    parser.add_argument("--late-timestamp", default="2015-06-01T00:00:00")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--topic", default=TOPIC)
    args = parser.parse_args()

    batch_id = args.batch_id or time.strftime("batch-%Y%m%d-%H%M%S")
    frame = pd.read_csv(CSV, parse_dates=["timestamp"])
    frame = frame.sort_values(["timestamp", "location_id"], kind="mergesort")
    end = len(frame) - args.end_offset
    start = max(0, end - args.rows)
    window = frame.iloc[start:end]
    if window.empty:
        raise SystemExit("no rows to send for this offset")

    producer = build_producer()

    sent = 0
    duplicates = 0
    started = time.time()
    for row in window.itertuples(index=False):
        payload = message(row, batch_id)
        producer.send(args.topic, key=payload["location_id"], value=payload)
        sent += 1
        if args.duplicate_rate and random.random() < args.duplicate_rate:
            producer.send(args.topic, key=payload["location_id"], value=payload)
            duplicates += 1
        if args.rate and sent % 500 == 0:
            target = started + sent / float(args.rate)
            delay = target - time.time()
            if delay > 0:
                time.sleep(delay)

    if args.inject_late:
        late = frame[
            (frame["timestamp"] == pd.Timestamp(args.late_timestamp))
            & (frame["location_id"] == "VN-CT")
        ]
        if not late.empty:
            payload = message(next(late.itertuples(index=False)), batch_id + "-late")
            payload["produced_at"] = int(time.time() * 1000)
            producer.send(args.topic, key="VN-CT", value=payload)
            sent += 1

    producer.flush()
    producer.close()
    elapsed = time.time() - started
    payload = {
        "batch_id": batch_id,
        "rows_sent": sent,
        "duplicates_sent": duplicates,
        "seconds": round(elapsed, 2),
        "messages_per_second": round(sent / elapsed, 1),
        "late_injected": bool(args.inject_late),
    }
    write_json(METRICS, payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
