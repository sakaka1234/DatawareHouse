"""Airflow DAGs for the agri data warehouse.

Airflow drives the Spark cluster through the Docker socket, which mirrors how
an orchestrator drives external compute in production. Every task is a real
job, not a sleep.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SPARK = (
    "docker exec spark-master /spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.driver.memory=600m "
    "--conf spark.executor.memory=800m "
    "--conf spark.executor.cores=1 "
    "--conf spark.cores.max=2 "
    "--conf spark.sql.shuffle.partitions=8 "
    "--conf spark.driver.host=spark-master "
)

UPLOAD = (
    'docker exec namenode bash -c "'
    "hdfs dfs -mkdir -p /data/agri/raw/hourly /data/agri/raw/daily "
    "/data/agri/raw/yield /data/agri/raw/location /data/agri/processed /data/agri/checkpoints && "
    "hdfs dfs -put -f /data/agri/real_data/fact_weather_hourly.csv /data/agri/raw/hourly/ && "
    "hdfs dfs -put -f /data/agri/real_data/extended/fact_weather_daily.csv /data/agri/raw/daily/ && "
    "hdfs dfs -put -f /data/agri/real_data/extended/fact_cereal_annual.csv /data/agri/raw/yield/ && "
    "hdfs dfs -put -f /data/agri/real_data/extended/dim_location.csv /data/agri/raw/location/"
    '"'
)

DEFAULTS = {
    "owner": "agri",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "email_on_failure": False,
}

with DAG(
    "agri_batch_dwh",
    description="CSV -> HDFS -> Spark -> Hive, with a quality gate",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["agri", "batch"],
) as batch_dag:
    upload = BashOperator(task_id="upload_to_hdfs", bash_command=UPLOAD)
    clean = BashOperator(
        task_id="clean_hourly_weather", bash_command=SPARK + "/opt/agri/spark/jobs/01_clean_weather.py"
    )
    daily = BashOperator(
        task_id="load_daily_weather", bash_command=SPARK + "/opt/agri/spark/jobs/02_build_daily.py"
    )
    dims = BashOperator(
        task_id="build_dimensions", bash_command=SPARK + "/opt/agri/spark/jobs/03_build_dimensions.py"
    )
    yields = BashOperator(
        task_id="load_cereal_yield", bash_command=SPARK + "/opt/agri/spark/jobs/04_load_yield.py"
    )
    views = BashOperator(
        task_id="build_views", bash_command=SPARK + "/opt/agri/spark/jobs/05_build_views.py"
    )
    roles = BashOperator(
        task_id="create_roles",
        bash_command=(
            "docker exec hiveserver2 bash -c \"beeline -u "
            "'jdbc:hive2://localhost:10000/;auth=noSasl' -n hive "
            "--force=true -f /opt/agri/hive/sql/04_create_roles.sql || true\""
        ),
    )
    rbac = BashOperator(
        task_id="apply_rbac",
        bash_command=(
            "docker exec hiveserver2 bash -c \"beeline -u "
            "'jdbc:hive2://localhost:10000/;auth=noSasl' -n hive "
            "--force=true -f /opt/agri/hive/sql/04_rbac.sql || true\""
        ),
    )
    gate = BashOperator(
        task_id="quality_gate",
        bash_command="docker exec agri-tools python /opt/agri/tests/quality_gate.py",
    )
    upload >> clean >> daily >> dims >> yields >> views >> roles >> rbac >> gate


with DAG(
    "agri_ml_pipeline",
    description="Feature table, three models, predictions written back to Hive",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULTS, "retries": 0},
    tags=["agri", "ml"],
) as ml_dag:
    features = BashOperator(
        task_id="build_feature_table", bash_command=SPARK + "/opt/agri/spark/jobs/06_build_features.py"
    )
    train = BashOperator(
        task_id="train_yield_model",
        bash_command="docker exec agri-tools python /opt/agri/ml/train_yield.py",
    )
    anomaly = BashOperator(
        task_id="score_anomalies",
        bash_command="docker exec agri-tools python /opt/agri/ml/anomaly.py",
    )
    forecast = BashOperator(
        task_id="forecast_soil",
        bash_command="docker exec agri-tools python /opt/agri/ml/forecast.py",
    )
    load_yield = BashOperator(
        task_id="load_yield_predictions",
        bash_command=SPARK
        + "/opt/agri/spark/jobs/08_load_predictions.py --csv /opt/agri/artifacts/ml/yield_predictions.csv --table ml_yield_predictions",
    )
    load_anomaly = BashOperator(
        task_id="load_anomalies",
        bash_command=SPARK
        + "/opt/agri/spark/jobs/08_load_predictions.py --csv /opt/agri/artifacts/ml/ml_anomalies.csv --table ml_anomalies",
    )
    features >> train >> load_yield
    features >> anomaly >> load_anomaly
    features >> forecast


with DAG(
    "agri_stream_health",
    description="Kafka lag and sink freshness",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULTS, "retries": 2},
    tags=["agri", "stream"],
) as stream_dag:
    health = BashOperator(
        task_id="check_stream_health",
        bash_command="docker exec agri-tools python /opt/agri/streaming/stream_health.py",
    )
    register = BashOperator(
        task_id="refresh_stream_table",
        bash_command=SPARK + "/opt/agri/spark/jobs/07_register_stream.py",
    )
    health >> register


def flaky_task(**context):
    marker = "/opt/agri/artifacts/retry_demo_marker"
    if not os.path.exists(marker):
        with open(marker, "w") as handle:
            handle.write("first attempt")
        raise RuntimeError("intentional first-attempt failure: retry must recover")
    return "recovered on retry"


with DAG(
    "agri_retry_demo",
    description="Proves that retry policy recovers a failed task",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={**DEFAULTS, "retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["agri", "ops"],
) as retry_dag:
    PythonOperator(task_id="flaky_but_retried", python_callable=flaky_task)
