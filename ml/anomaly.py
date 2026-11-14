"""Score every hourly row with Isolation Forest and a per-location robust z-score."""

import json
import os

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from common import artifacts_path, query_rows, write_json

COLUMNS = [
    "event_time",
    "location_id",
    "t2m_c",
    "rh2m_pct",
    "precip_mm_h",
    "ws2m_ms",
    "soil_wetness_root",
]
NUMERIC = COLUMNS[2:]
EVENT_DAY = "2020-06-14"
SAMPLE_EVERY = 5
CONTAMINATION = 0.005
SQL = (
    "SELECT CAST(event_time AS STRING), location_id, t2m_c, rh2m_pct, "
    "precip_mm_h, ws2m_ms, soil_wetness_root FROM agri_dwh.fact_weather_hourly"
)


def main():
    sample_values = []
    sample_labels = []
    total_rows = 0

    # Pass 1: deterministic 20% sample for training.
    for rows in query_rows(SQL):
        frame = pd.DataFrame(rows, columns=COLUMNS)
        if frame.empty:
            continue
        values = frame[NUMERIC].astype(float).to_numpy()
        sample_values.append(values[::SAMPLE_EVERY].copy())
        sample_labels.append(frame["location_id"].to_numpy()[::SAMPLE_EVERY].copy())
        total_rows += len(frame)
        print("scanned", total_rows, flush=True)

    sample = np.vstack(sample_values)
    labels = np.concatenate(sample_labels)
    if len(sample) > 220000:
        keep = np.linspace(0, len(sample) - 1, 220000).astype(int)
        sample = sample[keep]
        labels = labels[keep]
    print("training sample", sample.shape, flush=True)

    model = IsolationForest(
        n_estimators=200,
        contamination=CONTAMINATION,
        max_samples=min(len(sample), 100000),
        random_state=42,
        n_jobs=2,
    )
    model.fit(sample)

    centre = {}
    scale = {}
    for location in np.unique(labels):
        values = sample[labels == location, 2]
        centre[location] = float(np.median(values))
        iqr = float(np.percentile(values, 75) - np.percentile(values, 25))
        upper = float(np.percentile(values, 99) - centre[location])
        scale[location] = max(iqr, upper, 1e-3)

    # Pass 2: score all rows; only flagged rows are retained.
    flagged = []
    z_flagged = 0
    event_rows = []
    event_flag_any = False
    event_score = None
    lower_count = 0

    for rows in query_rows(SQL):
        frame = pd.DataFrame(rows, columns=COLUMNS)
        if frame.empty:
            continue
        values = frame[NUMERIC].astype(float).to_numpy()
        scores = model.decision_function(values)
        predictions = model.predict(values)

        med = frame["location_id"].map(centre).to_numpy(dtype=float)
        scl = frame["location_id"].map(scale).to_numpy(dtype=float)
        z = np.abs((values[:, 2] - med) / scl)
        z_flagged += int((z > 8).sum())

        mask = predictions == -1
        if mask.any():
            subset = frame.loc[mask].copy()
            subset["score"] = scores[mask]
            subset["z_precip"] = z[mask]
            flagged.append(subset)

        event_mask = (
            frame["location_id"].eq("VN-CT")
            & frame["event_time"].str.startswith(EVENT_DAY)
        ).to_numpy()
        if event_mask.any():
            day = frame.loc[event_mask].copy()
            day_scores = scores[event_mask]
            day["score"] = day_scores
            day["z_precip"] = z[event_mask]
            event_rows.append(day)
            event_flag_any = bool((predictions[event_mask] == -1).any())
            event_score = float(day_scores.min())
        if event_score is not None:
            lower_count += int((scores < event_score).sum())

    anomalies = (
        pd.concat(flagged, ignore_index=True)
        if flagged
        else pd.DataFrame(columns=list(COLUMNS) + ["score", "z_precip"])
    )
    anomalies = anomalies.sort_values("score").reset_index(drop=True)
    anomalies["rank"] = np.arange(1, len(anomalies) + 1)

    artifacts = artifacts_path("ml")
    os.makedirs(artifacts, exist_ok=True)
    output_columns = list(COLUMNS) + ["score", "z_precip", "rank"]
    anomalies[output_columns].head(6000).to_csv(
        os.path.join(artifacts, "ml_anomalies.csv"), index=False
    )

    event_frame = pd.concat(event_rows, ignore_index=True) if event_rows else pd.DataFrame()
    if not event_frame.empty:
        event_frame.sort_values("score").to_csv(
            os.path.join(artifacts, "anomaly_event_rows.csv"), index=False
        )

    by_location = anomalies.groupby("location_id").size().sort_values(ascending=False).head(10)
    payload = {
        "rows_scored": total_rows,
        "sample_rows": int(len(sample)),
        "contamination": CONTAMINATION,
        "flagged": int(len(anomalies)),
        "flagged_pct": round(100.0 * len(anomalies) / max(total_rows, 1), 4),
        "zscore_flagged": z_flagged,
        "event_rows": int(len(event_frame)),
        "event_caught": bool(event_flag_any),
        "event_rank": int(lower_count + 1) if event_score is not None else None,
        "event_top_pct": round(100.0 * (lower_count + 1) / max(total_rows, 1), 4)
        if event_score is not None
        else None,
        "top_locations": {str(k): int(v) for k, v in by_location.items()},
        "top_anomaly": anomalies.head(1)[output_columns].to_dict("records"),
    }
    write_json(os.path.join(artifacts, "anomaly_metrics.json"), payload)
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()
