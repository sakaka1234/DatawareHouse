"""A small file-based streaming simulation with watermark and deduplication."""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

import pandas as pd


STREAM_COLUMNS = [
    "event_time",
    "location_id",
    "t2m_c",
    "rh2m_pct",
    "precip_mm_h",
    "ws2m_ms",
    "soil_wetness_root",
    "soil_wetness_top",
]


def init_stream_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_weather_stream (
            event_time TIMESTAMP,
            location_id VARCHAR,
            t2m_c DOUBLE,
            rh2m_pct DOUBLE,
            precip_mm_h DOUBLE,
            ws2m_ms DOUBLE,
            soil_wetness_root DOUBLE,
            soil_wetness_top DOUBLE,
            is_late BOOLEAN,
            ingested_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_ingest_log (
            file_name VARCHAR,
            rows INTEGER,
            duplicates INTEGER,
            late_rows INTEGER,
            max_event_time TIMESTAMP,
            seconds DOUBLE,
            ingested_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_processed_files (
            file_name VARCHAR PRIMARY KEY,
            processed_at TIMESTAMP
        )
        """
    )


def produce_batches(
    csv_path: Path,
    out_dir: Path,
    batch_size: int = 5_000,
    max_rows: int = 100_000,
    clean: bool = True,
) -> list[Path]:
    if clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_columns = ["timestamp"] + [c for c in STREAM_COLUMNS if c != "event_time"]
    df = pd.read_csv(csv_path, usecols=csv_columns)
    df = df.rename(columns={"timestamp": "event_time"})
    df["event_time"] = pd.to_datetime(df["event_time"])
    if max_rows and len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=42)
    df = df.sort_values("event_time").reset_index(drop=True)

    files = []
    for index, start in enumerate(range(0, len(df), batch_size)):
        chunk = df.iloc[start : start + batch_size].copy()
        path = out_dir / f"batch_{index:05d}.jsonl"
        chunk.to_json(path, orient="records", lines=True, date_format="iso")
        files.append(path)
    return files



def make_time_batch(
    csv_path: Path,
    out_dir: Path,
    start: str,
    end: str,
    n: int = 5_000,
    name: str = "current_batch.jsonl",
) -> Path:
    """Write a batch of rows inside a chosen time window (for watermark demos)."""
    csv_columns = ["timestamp"] + [c for c in STREAM_COLUMNS if c != "event_time"]
    df = pd.read_csv(csv_path, usecols=csv_columns)
    df = df.rename(columns={"timestamp": "event_time"})
    df["event_time"] = pd.to_datetime(df["event_time"])
    mask = (df["event_time"] >= pd.Timestamp(start)) & (df["event_time"] < pd.Timestamp(end))
    df = df.loc[mask].head(n)
    path = out_dir / name
    df.to_json(path, orient="records", lines=True, date_format="iso")
    return path


def make_late_batch(csv_path: Path, out_dir: Path, n: int = 200) -> Path:
    """Write a deliberately old batch to demonstrate watermark behaviour."""
    csv_columns = ["timestamp"] + [c for c in STREAM_COLUMNS if c != "event_time"]
    df = pd.read_csv(csv_path, usecols=csv_columns, nrows=n)
    df = df.rename(columns={"timestamp": "event_time"})
    df["event_time"] = pd.to_datetime(df["event_time"])
    df = df[df["event_time"] < "2015-02-01"].head(n)
    path = out_dir / "late_batch.jsonl"
    df.to_json(path, orient="records", lines=True, date_format="iso")
    return path


def _watermark(con) -> pd.Timestamp | None:
    value = con.execute("SELECT MAX(event_time) FROM fact_weather_stream").fetchall()[0][0]
    return pd.Timestamp(value) if value is not None else None


def ingest_file(con, path: Path, watermark_minutes: int = 10) -> dict:
    start = time.perf_counter()
    df = pd.read_json(path, lines=True)
    df["event_time"] = pd.to_datetime(df["event_time"])
    current_max = _watermark(con)
    cutoff = current_max - pd.Timedelta(minutes=watermark_minutes) if current_max is not None else None
    total_before = con.execute("SELECT COUNT(*) FROM fact_weather_stream").fetchall()[0][0]

    con.register("incoming", df)
    if cutoff is not None:
        con.execute(
            """
            INSERT INTO fact_weather_stream
            SELECT event_time, location_id, t2m_c, rh2m_pct, precip_mm_h,
                   ws2m_ms, soil_wetness_root, soil_wetness_top,
                   event_time < ? AS is_late, current_timestamp
            FROM incoming i
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_weather_stream s
                WHERE s.location_id = i.location_id AND s.event_time = i.event_time
            )
            """,
            [cutoff],
        )
    else:
        con.execute(
            """
            INSERT INTO fact_weather_stream
            SELECT event_time, location_id, t2m_c, rh2m_pct, precip_mm_h,
                   ws2m_ms, soil_wetness_root, soil_wetness_top,
                   FALSE AS is_late, current_timestamp
            FROM incoming i
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_weather_stream s
                WHERE s.location_id = i.location_id AND s.event_time = i.event_time
            )
            """
        )
    con.unregister("incoming")

    total = con.execute("SELECT COUNT(*) FROM fact_weather_stream").fetchall()[0][0]
    inserted = int(total - total_before)
    elapsed = time.perf_counter() - start
    late = int(df["event_time"].lt(cutoff).sum()) if cutoff is not None else 0
    duplicates = int(len(df) - inserted)

    con.execute(
        """
        INSERT INTO stream_ingest_log
        VALUES (?, ?, ?, ?, ?, ?, current_timestamp)
        """,
        [path.name, int(len(df)), duplicates, late, current_max, elapsed],
    )
    con.execute(
        "INSERT OR IGNORE INTO stream_processed_files VALUES (?, current_timestamp)",
        [path.name],
    )
    return {
        "file": path.name,
        "rows": int(len(df)),
        "table_rows": int(total),
        "late": late,
        "seconds": round(elapsed, 4),
    }


def process_new_files(con, landing_dir: Path, delay: float = 0.05) -> pd.DataFrame:
    rows = []
    for path in sorted(landing_dir.glob("*.jsonl")):
        already = con.execute(
            "SELECT 1 FROM stream_processed_files WHERE file_name = ?", [path.name]
        ).fetchall()
        if already:
            continue
        rows.append(ingest_file(con, path))
        if delay:
            time.sleep(delay)
    return pd.DataFrame(rows)


def stream_summary(con) -> pd.DataFrame:
    return con.execute(
        """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT location_id) AS locations,
               MIN(event_time) AS min_time,
               MAX(event_time) AS max_time,
               SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_rows,
               COUNT(*) - COUNT(DISTINCT (location_id, event_time)) AS duplicate_rows
        FROM fact_weather_stream
        """
    ).df()
