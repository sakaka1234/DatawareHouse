"""Seven-day soil-moisture forecasts with Ridge, SARIMAX and persistence."""

import json
import os
import warnings

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from statsmodels.tsa.statespace.sarimax import SARIMAX

from common import artifacts_path, query_df, write_json

LOCATIONS = ["VN-CT", "US-IA", "FR-CVL"]
HORIZON = 7
LOOKBACK = 365
warnings.filterwarnings("ignore")


def lag_features(history, day_of_year):
    lags = [history[-i] for i in range(1, HORIZON + 1)]
    angle = 2.0 * np.pi * day_of_year / 365.25
    return lags + [np.sin(angle), np.cos(angle)]


def main():
    frame = query_df(
        "SELECT CAST(date_key AS STRING) AS date_key, location_id, soil_wetness_root "
        "FROM agri_dwh.fact_weather_daily WHERE location_id IN ('%s')"
        % "','".join(LOCATIONS)
    )
    frame["date_key"] = pd.to_datetime(frame["date_key"])

    records = []
    summary = {}
    for location in LOCATIONS:
        series = (
            frame[frame["location_id"] == location]
            .sort_values("date_key")
            .set_index("date_key")["soil_wetness_root"]
            .astype(float)
        )
        series = series.asfreq("D").interpolate(limit_direction="both")
        origins = [
            day
            for day in series.index
            if day.day >= 24
            and day + pd.Timedelta(days=HORIZON) <= series.index.max()
            and day.month in (3, 6, 9, 12)
        ]

        errors = {"ridge": [], "sarimax": [], "persistence": []}
        for origin in origins:
            history = series.loc[:origin].tail(LOOKBACK)
            actual = series.loc[
                origin + pd.Timedelta(days=1): origin + pd.Timedelta(days=HORIZON)
            ]
            if len(actual) < HORIZON:
                continue

            train_x, train_y = [], []
            values = history.to_numpy()
            for index in range(HORIZON, len(values)):
                train_x.append(lag_features(values[:index], history.index[index].dayofyear))
                train_y.append(values[index])
            ridge = Ridge(alpha=1.0).fit(np.asarray(train_x), np.asarray(train_y))

            working = list(values)
            ridge_pred = []
            for step in range(HORIZON):
                target_day = origin + pd.Timedelta(days=step + 1)
                features = lag_features(working, target_day.dayofyear)
                value = float(ridge.predict(np.asarray([features]))[0])
                ridge_pred.append(value)
                working.append(value)

            sarimax_pred = []
            try:
                model = SARIMAX(
                    values,
                    order=(1, 0, 1),
                    seasonal_order=(0, 0, 0, 0),
                    enforce_stationarity=False,
                    enforce_invertibility=False,
                ).fit(disp=False)
                sarimax_pred = list(model.forecast(HORIZON))
            except Exception:
                sarimax_pred = [float(values[-1])] * HORIZON

            persistence = [float(values[-1])] * HORIZON
            for name, predicted in (
                ("ridge", ridge_pred),
                ("sarimax", sarimax_pred),
                ("persistence", persistence),
            ):
                errors[name].extend(np.abs(np.asarray(predicted) - actual.to_numpy()))
                for step, (target, observed, value) in enumerate(
                    zip(actual.index, actual.to_numpy(), predicted), start=1
                ):
                    records.append(
                        {
                            "location_id": location,
                            "origin": origin.strftime("%Y-%m-%d"),
                            "target_date": target.strftime("%Y-%m-%d"),
                            "horizon": step,
                            "actual": round(float(observed), 4),
                            name: round(float(value), 4),
                        }
                    )

        summary[location] = {
            name: {
                "mae": round(float(np.mean(values)), 5),
                "n": int(len(values)),
            }
            for name, values in errors.items()
        }

    artifacts = artifacts_path("ml")
    os.makedirs(artifacts, exist_ok=True)
    output = pd.DataFrame(records)
    output.to_csv(os.path.join(artifacts, "forecast_predictions.csv"), index=False)

    winners = {
        location: min(
            ("ridge", "sarimax", "persistence"),
            key=lambda name: summary[location][name]["mae"],
        )
        for location in summary
    }
    payload = {
        "locations": summary,
        "horizon_days": HORIZON,
        "origins_per_location": {
            location: int(output[output["location_id"] == location]["origin"].nunique())
            for location in summary
        },
        "best_by_location": winners,
        "ridge_beats_persistence": [
            location
            for location in summary
            if summary[location]["ridge"]["mae"] < summary[location]["persistence"]["mae"]
        ],
        "sarimax_beats_persistence": [
            location
            for location in summary
            if summary[location]["sarimax"]["mae"] < summary[location]["persistence"]["mae"]
        ],
    }
    write_json(os.path.join(artifacts, "forecast_metrics.json"), payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
