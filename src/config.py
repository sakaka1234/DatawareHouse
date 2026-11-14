"""Shared paths and plotting defaults for the agri-dwh notebooks."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "real_data"
WAREHOUSE_DIR = ROOT / "warehouse"
PARQUET_DIR = WAREHOUSE_DIR / "parquet"
REPORTS_DIR = ROOT / "reports"
ARTIFACTS_DIR = ROOT / "artifacts"
STREAM_DIR = ROOT / "stream"
DB_PATH = WAREHOUSE_DIR / "agri_dwh.duckdb"

CSV_HOURLY = DATA_DIR / "fact_weather_hourly.csv"
CSV_DAILY = DATA_DIR / "fact_weather_daily.csv"
CSV_YIELD = DATA_DIR / "fact_cereal_annual.csv"
CSV_LOCATION = DATA_DIR / "dim_location.csv"
CSV_BENCHMARK = DATA_DIR / "benchmark_like_hourly.csv"

PALETTE = {
    "green": "#1F6B45",
    "green_light": "#7BA98C",
    "teal": "#2B6E7A",
    "gold": "#8F5E1A",
    "ink": "#14251B",
    "ink_2": "#41544A",
    "muted": "#5F7064",
    "line": "#D7E2D0",
    "surface": "#FCFDF9",
    "danger": "#A9452E",
    "bg": "#F3F7EF",
}

# Chart colors validated for the light surface in the explainer page.
CHART_COLORS = ["#1F6B45", "#8F5E1A", "#2B6E7A", "#A9452E"]


def ensure_dirs() -> None:
    for path in (WAREHOUSE_DIR, PARQUET_DIR, REPORTS_DIR, ARTIFACTS_DIR, STREAM_DIR):
        path.mkdir(parents=True, exist_ok=True)


def setup_matplotlib() -> None:
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "figure.dpi": 120,
            "savefig.dpi": 160,
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.labelsize": 10,
            "axes.edgecolor": PALETTE["line"],
            "axes.labelcolor": PALETTE["ink_2"],
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "grid.color": PALETTE["line"],
            "grid.alpha": 0.55,
            "grid.linewidth": 0.7,
            "xtick.color": PALETTE["muted"],
            "ytick.color": PALETTE["muted"],
            "text.color": PALETTE["ink_2"],
            "legend.frameon": False,
            "figure.facecolor": PALETTE["surface"],
            "axes.facecolor": PALETTE["surface"],
        }
    )


def save_figure(fig, name: str) -> Path:
    ensure_dirs()
    path = REPORTS_DIR / name
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    return path


def duckdb_connection(read_only: bool = False):
    import duckdb

    ensure_dirs()
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def env_note() -> dict:
    """Small environment summary shown at the top of notebooks."""
    import duckdb
    import pandas as pd
    import sklearn
    import sys

    return {
        "python": sys.version.split()[0],
        "pandas": pd.__version__,
        "duckdb": duckdb.__version__,
        "scikit_learn": sklearn.__version__,
        "database": str(DB_PATH),
        "cwd": os.getcwd(),
    }
