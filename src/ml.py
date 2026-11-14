"""Models for yield regression, anomaly detection and soil-moisture forecasting."""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import GradientBoostingRegressor, IsolationForest, RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from config import ARTIFACTS_DIR, REPORTS_DIR, ensure_dirs
from features import ANOMALY_FEATURES, make_lag_frame

YIELD_FEATURES = [
    "gdd",
    "heat_days_35",
    "frost_days",
    "precip_total",
    "dry_days",
    "max_dry_spell",
    "soil_mean",
    "soil_min",
    "soil_p95",
    "rh_mean",
    "latitude",
    "longitude",
]


def _metrics(y_true, y_pred) -> dict:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    mae = mean_absolute_error(y_true, y_pred)
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2 = r2_score(y_true, y_pred) if len(y_true) > 2 else float("nan")
    mape = float(np.mean(np.abs((y_true - y_pred) / np.where(y_true == 0, np.nan, y_true))) * 100)
    return {"MAE": round(mae, 2), "RMSE": round(rmse, 2), "R2": round(r2, 4), "MAPE_pct": round(mape, 2)}


def _fresh_models() -> dict:
    return {
        "Baseline mean": DummyRegressor(strategy="mean"),
        "Ridge": make_pipeline(StandardScaler(), Ridge(alpha=1.0)),
        "RandomForest": RandomForestRegressor(
            n_estimators=300,
            max_depth=4,
            min_samples_leaf=3,
            random_state=42,
            n_jobs=-1,
        ),
        "GradientBoosting": GradientBoostingRegressor(
            n_estimators=200,
            learning_rate=0.05,
            max_depth=2,
            random_state=42,
        ),
    }


def train_yield_models(features: pd.DataFrame) -> tuple[pd.DataFrame, dict, pd.DataFrame, pd.DataFrame]:
    """Time-based split: train <=2021, validation 2022, test 2023-2024."""
    data = features.dropna(subset=YIELD_FEATURES + ["yield_kg_ha"]).copy()
    train = data[data["year"] <= 2021]
    valid = data[data["year"] == 2022]
    test = data[data["year"] >= 2023]

    metrics_rows = []
    fitted = {}
    valid_scores = {}
    for name, model in _fresh_models().items():
        model.fit(train[YIELD_FEATURES], train["yield_kg_ha"])
        pred_valid = model.predict(valid[YIELD_FEATURES])
        scores = _metrics(valid["yield_kg_ha"], pred_valid)
        valid_scores[name] = scores["MAE"]
        metrics_rows.append({"model": name, "split": "validation 2022", **scores})
        fitted[name] = model

    best_name = min(valid_scores, key=valid_scores.get)
    best = _fresh_models()[best_name]
    best.fit(pd.concat([train, valid])[YIELD_FEATURES], pd.concat([train, valid])["yield_kg_ha"])
    pred_test = best.predict(test[YIELD_FEATURES])
    metrics_rows.append({"model": best_name + " (refit)", "split": "test 2023-2024", **_metrics(test["yield_kg_ha"], pred_test)})

    for name in fitted:
        pred = fitted[name].predict(test[YIELD_FEATURES])
        metrics_rows.append({"model": name, "split": "test 2023-2024", **_metrics(test["yield_kg_ha"], pred)})

    metrics = pd.DataFrame(metrics_rows)
    pred_frame = test[["location_id", "country_iso3", "year", "yield_kg_ha"]].copy()
    pred_frame["prediction"] = pred_test
    pred_frame["model"] = best_name

    importance = pd.DataFrame()
    if best_name == "RandomForest":
        importance = pd.DataFrame(
            {"feature": YIELD_FEATURES, "importance": best.feature_importances_}
        ).sort_values("importance", ascending=False)
    elif best_name == "Ridge":
        ridge = best.named_steps["ridge"]
        importance = pd.DataFrame(
            {"feature": YIELD_FEATURES, "importance": np.abs(ridge.coef_)}
        ).sort_values("importance", ascending=False)
    elif best_name == "GradientBoosting":
        importance = pd.DataFrame(
            {"feature": YIELD_FEATURES, "importance": best.feature_importances_}
        ).sort_values("importance", ascending=False)

    ensure_dirs()
    joblib.dump(best, ARTIFACTS_DIR / "yield_model.joblib")
    with open(REPORTS_DIR / "yield_metrics.json", "w", encoding="utf-8") as handle:
        json.dump({"best_model": best_name, "metrics": metrics.to_dict(orient="records")}, handle, ensure_ascii=False, indent=2)
    return metrics, {"best_name": best_name, "model": best}, pred_frame, importance


def fit_anomaly_model(df: pd.DataFrame, sample_size: int = 200_000) -> tuple[IsolationForest, pd.DataFrame]:
    data = df.dropna(subset=ANOMALY_FEATURES).copy()
    sample = data.sample(n=min(sample_size, len(data)), random_state=42)
    model = IsolationForest(
        n_estimators=200,
        contamination=0.005,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(sample[ANOMALY_FEATURES])
    data["anomaly_score"] = -model.decision_function(data[ANOMALY_FEATURES])
    data["is_anomaly"] = model.predict(data[ANOMALY_FEATURES]) == -1
    ensure_dirs()
    joblib.dump(model, ARTIFACTS_DIR / "anomaly_model.joblib")
    return model, data


def score_injected_anomalies(model: IsolationForest, df: pd.DataFrame, n: int = 5000) -> dict:
    base = df[df["is_anomaly"] == False].sample(n=min(n, int((df["is_anomaly"] == False).sum())), random_state=7).copy()
    base["t2m_c"] = 50.0
    base["t2m_roll_mean_24"] = 45.0
    base["precip_mm_h"] = 150.0
    base["precip_roll_sum_24"] = 500.0
    base["soil_wetness_root"] = 1.0
    base["soil_wetness_top"] = 1.0
    base["soil_roll_mean_24"] = 1.0
    pred = model.predict(base[ANOMALY_FEATURES]) == -1
    return {
        "injected": int(len(base)),
        "flagged": int(pred.sum()),
        "recall": round(float(pred.mean()), 3),
    }


def known_event_check(scored: pd.DataFrame) -> pd.DataFrame:
    event = scored[
        (scored["location_id"] == "VN-CT")
        & (scored["event_time"].dt.date.astype(str) == "2020-06-14")
    ].copy()
    if event.empty:
        return event
    threshold = scored["anomaly_score"].quantile(0.99)
    event["above_p99"] = event["anomaly_score"] >= threshold
    return event.sort_values("anomaly_score", ascending=False)


def top_anomalies(scored: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    cols = [
        "event_time",
        "location_id",
        "t2m_c",
        "precip_mm_h",
        "soil_wetness_root",
        "anomaly_score",
        "is_anomaly",
    ]
    return scored.nlargest(n, "anomaly_score")[cols].reset_index(drop=True)


def _feature_row(series: pd.Series, date: pd.Timestamp) -> pd.DataFrame | None:
    """Build one lag feature row using values strictly before `date`."""
    history = series.loc[: date - pd.Timedelta(days=1)]
    if len(history) < 31:
        return None
    row = {
        "lag_1": history.iloc[-1],
        "lag_2": history.iloc[-2],
        "lag_3": history.iloc[-3],
        "lag_7": history.iloc[-7],
        "lag_14": history.iloc[-14],
        "lag_30": history.iloc[-30],
        "roll_7": history.iloc[-7:].mean(),
        "roll_30": history.iloc[-30:].mean(),
        "doy_sin": np.sin(2 * np.pi * date.dayofyear / 365.25),
        "doy_cos": np.cos(2 * np.pi * date.dayofyear / 365.25),
    }
    return pd.DataFrame([row])


def forecast_soil(series: pd.Series, test_start: str = "2024-01-01", horizon: int = 7) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Recursive 7-day Ridge forecast, evaluated against persistence."""
    test_start_ts = pd.Timestamp(test_start)
    train = series.loc[: test_start_ts - pd.Timedelta(days=1)]
    frame = make_lag_frame(train)
    features = [c for c in frame.columns if c != "y"]
    model = make_pipeline(StandardScaler(), Ridge(alpha=1.0))
    model.fit(frame[features], frame["y"])

    origins = pd.date_range(test_start_ts, series.index.max(), freq="MS")
    rows = []
    for origin in origins:
        if origin not in series.index:
            continue
        history = series.loc[:origin].copy()
        last_actual = history.iloc[-1]
        for h in range(1, horizon + 1):
            target_date = origin + pd.Timedelta(days=h)
            if target_date > series.index.max():
                break
            row = _feature_row(history, target_date)
            if row is None:
                continue
            pred = float(model.predict(row[features])[0])
            actual = float(series.loc[target_date])
            rows.append(
                {
                    "origin": origin,
                    "target_date": target_date,
                    "horizon": h,
                    "actual": actual,
                    "prediction": pred,
                    "persistence": last_actual,
                }
            )
            history.loc[target_date] = pred

    forecast = pd.DataFrame(rows)
    if forecast.empty:
        return forecast, pd.DataFrame()

    metrics_rows = []
    for label, column in (("Ridge AR", "prediction"), ("Persistence", "persistence")):
        mae = mean_absolute_error(forecast["actual"], forecast[column])
        rmse = float(np.sqrt(mean_squared_error(forecast["actual"], forecast[column])))
        metrics_rows.append({"model": label, "MAE": round(mae, 4), "RMSE": round(rmse, 4)})
    return forecast, pd.DataFrame(metrics_rows)
