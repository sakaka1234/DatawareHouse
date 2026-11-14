"""Feature engineering for the yield, anomaly and forecast models."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _max_dry_spell(values: pd.Series, threshold: float = 1.0) -> int:
    best = current = 0
    for value in values:
        if value < threshold:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def build_yield_features(con) -> pd.DataFrame:
    """One row per location-year with weather features and the annual yield label."""
    daily = con.execute(
        """
        SELECT location_id, CAST(date_key AS DATE) AS date_key, precip_mm
        FROM fact_weather_daily
        ORDER BY location_id, date_key
        """
    ).df()
    daily["year"] = pd.to_datetime(daily["date_key"]).dt.year

    dry_spells = (
        daily.groupby(["location_id", "year"], sort=False)["precip_mm"]
        .apply(_max_dry_spell)
        .rename("max_dry_spell")
        .reset_index()
    )

    agg = con.execute(
        """
        SELECT location_id, CAST(year(date_key) AS INTEGER) AS year,
               SUM(GREATEST(t2m_c - 10, 0)) AS gdd,
               SUM(CASE WHEN t2m_max_c > 35 THEN 1 ELSE 0 END) AS heat_days_35,
               SUM(CASE WHEN t2m_min_c < 0 THEN 1 ELSE 0 END) AS frost_days,
               SUM(precip_mm) AS precip_total,
               SUM(CASE WHEN precip_mm < 1 THEN 1 ELSE 0 END) AS dry_days,
               AVG(soil_wetness_root) AS soil_mean,
               MIN(soil_wetness_root) AS soil_min,
               quantile_cont(soil_wetness_root, 0.95) AS soil_p95,
               AVG(rh2m_pct) AS rh_mean
        FROM fact_weather_daily
        GROUP BY 1, 2
        """
    ).df()

    features = agg.merge(dry_spells, on=["location_id", "year"], how="left")

    # Join the dimensions and label with DuckDB after registering the frame.
    con.register("features_df", features)
    features = con.execute(
        """
        SELECT f.*, l.country_iso3, l.country_name, l.latitude, l.longitude,
               l.crop_system, y.cereal_yield_kg_per_ha AS yield_kg_ha
        FROM features_df f
        JOIN dim_location l ON l.location_id = f.location_id
        JOIN fact_cereal_annual y
          ON y.country_iso3 = l.country_iso3
         AND y.year = f.year
        ORDER BY f.location_id, f.year
        """
    ).df()
    con.unregister("features_df")

    con.register("features_df", features)
    con.execute("CREATE OR REPLACE TABLE ml_yield_features AS SELECT * FROM features_df")
    con.unregister("features_df")
    return features


def build_anomaly_features(con) -> pd.DataFrame:
    """Hourly features with rolling context for unsupervised anomaly detection."""
    df = con.execute(
        """
        SELECT event_time, location_id, t2m_c, rh2m_pct, precip_mm_h,
               ws2m_ms, soil_wetness_root, soil_wetness_top
        FROM fact_weather_hourly
        ORDER BY location_id, event_time
        """
    ).df()
    df["event_time"] = pd.to_datetime(df["event_time"])
    df = df.sort_values(["location_id", "event_time"]).reset_index(drop=True)

    grouped = df.groupby("location_id", sort=False)
    df["t2m_roll_mean_24"] = grouped["t2m_c"].transform(
        lambda s: s.rolling(24, min_periods=6).mean()
    )
    df["t2m_roll_std_24"] = grouped["t2m_c"].transform(
        lambda s: s.rolling(24, min_periods=6).std()
    )
    df["precip_roll_sum_24"] = grouped["precip_mm_h"].transform(
        lambda s: s.rolling(24, min_periods=6).sum()
    )
    df["soil_roll_mean_24"] = grouped["soil_wetness_root"].transform(
        lambda s: s.rolling(24, min_periods=6).mean()
    )
    df["t2m_delta"] = grouped["t2m_c"].diff()
    df["soil_delta"] = grouped["soil_wetness_root"].diff()
    df["hour"] = df["event_time"].dt.hour
    df["month"] = df["event_time"].dt.month
    return df


ANOMALY_FEATURES = [
    "t2m_c",
    "rh2m_pct",
    "precip_mm_h",
    "ws2m_ms",
    "soil_wetness_root",
    "soil_wetness_top",
    "t2m_roll_mean_24",
    "t2m_roll_std_24",
    "precip_roll_sum_24",
    "soil_roll_mean_24",
    "t2m_delta",
    "soil_delta",
    "hour",
    "month",
]


def daily_series(con, location_id: str, column: str = "soil_wetness_root") -> pd.Series:
    df = con.execute(
        f"""
        SELECT CAST(date_key AS DATE) AS date_key, AVG({column}) AS value
        FROM fact_weather_daily
        WHERE location_id = ?
        GROUP BY 1 ORDER BY 1
        """,
        [location_id],
    ).df()
    series = pd.Series(
        df["value"].to_numpy(),
        index=pd.to_datetime(df["date_key"]),
        name=column,
    )
    return series.asfreq("D").interpolate()


def make_lag_frame(series: pd.Series, lags=(1, 2, 3, 7, 14, 30)) -> pd.DataFrame:
    frame = pd.DataFrame({"y": series})
    for lag in lags:
        frame[f"lag_{lag}"] = series.shift(lag)
    frame["roll_7"] = series.shift(1).rolling(7).mean()
    frame["roll_30"] = series.shift(1).rolling(30).mean()
    frame["doy_sin"] = np.sin(2 * np.pi * frame.index.dayofyear / 365.25)
    frame["doy_cos"] = np.cos(2 * np.pi * frame.index.dayofyear / 365.25)
    return frame.dropna()
