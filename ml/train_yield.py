"""Train the cereal yield regressors on the Hive feature table."""

import json
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from common import artifacts_path, query_df, write_json

FEATURES = [
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
TARGET = "yield_kg_ha"


def metrics(actual, predicted):
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    mask = actual != 0
    return {
        "mae": round(float(mean_absolute_error(actual, predicted)), 3),
        "rmse": round(float(np.sqrt(mean_squared_error(actual, predicted))), 3),
        "r2": round(float(r2_score(actual, predicted)), 4),
        "mape_pct": round(float(np.mean(np.abs((actual[mask] - predicted[mask]) / actual[mask])) * 100), 3),
        "n": int(len(actual)),
    }


def main():
    frame = query_df("SELECT * FROM agri_dwh.ml_yield_features")
    frame = frame.dropna(subset=FEATURES + [TARGET])
    print("features:", frame.shape)

    train = frame[frame["year"] <= 2021]
    valid = frame[frame["year"] == 2022]
    test = frame[frame["year"] >= 2023]
    if min(len(train), len(valid), len(test)) == 0:
        raise SystemExit("time split produced an empty partition")

    candidates = {
        "baseline_mean": DummyRegressor(strategy="mean"),
        "ridge": make_pipeline(StandardScaler(), Ridge(alpha=1.0)),
        "random_forest": RandomForestRegressor(
            n_estimators=400, min_samples_leaf=2, random_state=42, n_jobs=2
        ),
        "gradient_boosting": GradientBoostingRegressor(
            n_estimators=300, learning_rate=0.05, max_depth=3, random_state=42
        ),
    }

    comparison = {}
    best_name, best_mae, best_model = None, float("inf"), None
    for name, model in candidates.items():
        model.fit(train[FEATURES], train[TARGET])
        valid_pred = model.predict(valid[FEATURES])
        valid_metrics = metrics(valid[TARGET], valid_pred)
        comparison[name] = {"validation": valid_metrics}
        if valid_metrics["mae"] < best_mae:
            best_name, best_mae, best_model = name, valid_metrics["mae"], model

    # Refit the winner on train + validation, then report the untouched test years.
    best_model.fit(pd.concat([train, valid])[FEATURES], pd.concat([train, valid])[TARGET])
    test_pred = best_model.predict(test[FEATURES])
    test_metrics = metrics(test[TARGET], test_pred)
    baseline = DummyRegressor(strategy="mean")
    baseline.fit(pd.concat([train, valid])[FEATURES], pd.concat([train, valid])[TARGET])
    baseline_metrics = metrics(test[TARGET], baseline.predict(test[FEATURES]))

    importance = {}
    estimator = best_model
    if hasattr(best_model, "named_steps"):
        estimator = best_model.named_steps["ridge"]
    if hasattr(estimator, "feature_importances_"):
        importance = dict(
            sorted(
                zip(FEATURES, [round(float(v), 4) for v in estimator.feature_importances_]),
                key=lambda item: item[1],
                reverse=True,
            )
        )

    predictions = []
    for split, part, predicted in (
        ("train", train, best_model.predict(train[FEATURES])),
        ("validation", valid, best_model.predict(valid[FEATURES])),
        ("test", test, test_pred),
    ):
        for (_, row), value in zip(part.iterrows(), predicted):
            predictions.append(
                {
                    "country_iso3": row["country_iso3"],
                    "year": int(row["year"]),
                    "split": split,
                    "actual_kg_ha": round(float(row[TARGET]), 2),
                    "predicted_kg_ha": round(float(value), 2),
                }
            )

    artifacts = artifacts_path("ml")
    os.makedirs(artifacts, exist_ok=True)
    pd.DataFrame(predictions).to_csv(
        os.path.join(artifacts, "yield_predictions.csv"), index=False
    )
    joblib.dump(best_model, os.path.join(artifacts, "yield_model.joblib"))

    payload = {
        "best_model": best_name,
        "features": FEATURES,
        "train_rows": int(len(train)),
        "validation_rows": int(len(valid)),
        "test_rows": int(len(test)),
        "comparison": comparison,
        "test": test_metrics,
        "baseline_test": baseline_metrics,
        "mae_improvement_pct": round(
            (1 - test_metrics["mae"] / baseline_metrics["mae"]) * 100, 2
        ),
        "feature_importance": importance,
        "countries": int(frame["country_iso3"].nunique()),
        "years": [int(frame["year"].min()), int(frame["year"].max())],
    }
    write_json(os.path.join(artifacts, "yield_metrics.json"), payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
