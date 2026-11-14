"""Create the Hive connection, datasets, charts and dashboard in Superset."""

import json
import os
import sys
import time

import requests

BASE = os.environ.get("SUPERSET_URL", "http://superset:8088")
USER = os.environ.get("SUPERSET_USER", "admin")
PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")
SESSION = requests.Session()


def wait_ready(timeout=300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(BASE + "/health", timeout=5).status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(5)
    return False


def login():
    # Cookie session auth is used because this Superset build rejects its own
    # JWT on /api/v1/me. The rest of the API works normally with the session.
    response = SESSION.post(
        BASE + "/login/", data={"username": USER, "password": PASSWORD}, timeout=30
    )
    response.raise_for_status()
    return True


def api(token, method, path, **kwargs):
    response = SESSION.request(method, BASE + path, timeout=120, **kwargs)
    if response.status_code >= 400:
        print("API error", method, path, response.status_code, response.text[:800])
    return response


def ensure_database(token):
    payload = {
        "database_name": "Agri Hive",
        "sqlalchemy_uri": "hive://analyst@hiveserver2:10000/agri_dwh",
        "extra": json.dumps(
            {"engine_params": {"connect_args": {"auth": "NOSASL"}}}
        ),
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_dml": False,
    }
    response = api(token, "POST", "/api/v1/database/", json=payload)
    if response.status_code in (200, 201):
        return response.json()["id"]
    listing = api(token, "GET", "/api/v1/database/").json()
    for item in listing.get("result", []):
        if item["database_name"] == "Agri Hive":
            api(
                token,
                "PUT",
                "/api/v1/database/%s" % item["id"],
                json={"sqlalchemy_uri": payload["sqlalchemy_uri"], "extra": payload["extra"]},
            )
            return item["id"]
    raise RuntimeError("could not create or find the Hive database")


def ensure_dataset(token, database_id, table):
    payload = {"database": database_id, "schema": "agri_dwh", "table_name": table}
    response = api(token, "POST", "/api/v1/dataset/", json=payload)
    if response.status_code in (200, 201):
        return response.json()["id"]
    listing = api(token, "GET", "/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,value:%s)))" % table)
    result = listing.json().get("result", [])
    if result:
        return result[0]["id"]
    raise RuntimeError("dataset not available: " + table)


def ensure_chart(token, name, dataset_id, viz_type, params, dashboard_id=None):
    payload = {
        "slice_name": name,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "viz_type": viz_type,
        "params": json.dumps(params),
    }
    if dashboard_id:
        payload["dashboards"] = [dashboard_id]
    response = api(token, "POST", "/api/v1/chart/", json=payload)
    if response.status_code in (200, 201):
        return response.json()["id"]
    listing = api(token, "GET", "/api/v1/chart/?q=(filters:!((col:slice_name,opr:eq,value:'%s')))" % name)
    result = listing.json().get("result", [])
    return result[0]["id"] if result else None


def main():
    if not wait_ready():
        raise SystemExit("Superset did not become ready")
    token = login()
    database_id = ensure_database(token)
    print("database id", database_id)

    listing = api(token, "GET", "/api/v1/dashboard/?q=(page_size:100)").json()
    existing = [
        item["id"]
        for item in listing.get("result", [])
        if item["dashboard_title"] == "Agri DWH - Full Stack"
    ]
    if existing:
        dashboard_id = existing[0]
    else:
        dashboard = api(
            token,
            "POST",
            "/api/v1/dashboard/",
            json={"dashboard_title": "Agri DWH - Full Stack", "published": True},
        )
        dashboard_id = dashboard.json()["id"]
    print("dashboard id", dashboard_id)

    hourly = ensure_dataset(token, database_id, "v_benchmark_like_hourly")
    daily = ensure_dataset(token, database_id, "v_benchmark_like_daily")
    anomalies = ensure_dataset(token, database_id, "ml_anomalies")

    charts = {}
    charts["rows"] = ensure_chart(
        token,
        "Hourly rows",
        hourly,
        "big_number_total",
        {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "rows",
            },
            "adhoc_filters": [],
            "time_range": "No filter",
            "subheader": "fact_weather_hourly",
        },
        dashboard_id,
    )
    charts["yield"] = ensure_chart(
        token,
        "Average cereal yield by location",
        hourly,
        "bar",
        {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "yield_estimate"},
                    "aggregate": "AVG",
                    "label": "avg_yield",
                }
            ],
            "groupby": ["location"],
            "adhoc_filters": [],
            "time_range": "No filter",
            "row_limit": 20,
            "order_desc": True,
        },
        dashboard_id,
    )
    charts["soil"] = ensure_chart(
        token,
        "Soil moisture when temperature above 25C",
        hourly,
        "bar",
        {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "soil_moisture"},
                    "aggregate": "AVG",
                    "label": "avg_soil",
                }
            ],
            "groupby": ["location"],
            "adhoc_filters": [
                {
                    "expressionType": "SIMPLE",
                    "subject": "temperature",
                    "operator": ">",
                    "comparator": "25",
                    "clause": "WHERE",
                }
            ],
            "time_range": "No filter",
            "row_limit": 20,
        },
        dashboard_id,
    )
    charts["monthly"] = ensure_chart(
        token,
        "Daily temperature trend",
        daily,
        "line",
        {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "temperature"},
                    "aggregate": "AVG",
                    "label": "avg_temperature",
                }
            ],
            "groupby": [],
            "granularity_sqla": "ts",
            "time_grain_sqla": "P1M",
            "time_range": "2015-01-01 : 2024-12-31",
            "adhoc_filters": [],
        },
        dashboard_id,
    )
    charts["anomalies"] = ensure_chart(
        token,
        "Flagged anomalies by location",
        anomalies,
        "bar",
        {
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "anomalies",
                }
            ],
            "groupby": ["location_id"],
            "adhoc_filters": [],
            "time_range": "No filter",
            "row_limit": 20,
        },
        dashboard_id,
    )

    # Layout: one row per chart, each chart six grid columns wide.
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
    }
    row = 1
    for key, chart_id in charts.items():
        if not chart_id:
            continue
        row_id = "ROW-" + str(row)
        chart_key = "CHART-" + str(chart_id)
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [chart_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        position[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"chartId": chart_id, "width": 12, "height": 50},
        }
        row += 1

    api(
        token,
        "PUT",
        "/api/v1/dashboard/" + str(dashboard_id),
        json={"position_json": json.dumps(position)},
    )

    print(
        json.dumps(
            {
                "database_id": database_id,
                "dashboard_id": dashboard_id,
                "datasets": {"hourly": hourly, "daily": daily, "anomalies": anomalies},
                "charts": charts,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
