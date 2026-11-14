"""Operate the full Docker stack from the host.

Examples:
  python scripts/stack.py up
  python scripts/stack.py smoke
  python scripts/stack.py bootstrap
  python scripts/stack.py stream
  python scripts/stack.py ml
  python scripts/stack.py all
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCKER_DIR = ROOT / "docker"


def run(command, check=True, timeout=None, capture=False):
    print("+", " ".join(command) if isinstance(command, list) else command)
    return subprocess.run(
        command,
        cwd=str(DOCKER_DIR),
        check=check,
        timeout=timeout,
        text=True,
        capture_output=capture,
        shell=isinstance(command, str),
    )


def compose(*args, **kwargs):
    return run(["docker", "compose"] + list(args), **kwargs)


def docker(*args, **kwargs):
    return run(["docker"] + list(args), **kwargs)


def http_ok(url, timeout=3):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return 200 <= response.status < 500
    except Exception:
        return False


def wait_http(label, url, timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if http_ok(url):
            print("  ok:", label, url)
            return True
        time.sleep(4)
    print("  TIMEOUT:", label, url)
    return False


def upload_raw():
    script = (
        "hdfs dfs -mkdir -p /data/agri/raw/hourly /data/agri/raw/daily "
        "/data/agri/raw/yield /data/agri/raw/location /data/agri/processed "
        "/data/agri/checkpoints /user/hive/warehouse && "
        "hdfs dfs -put -f /data/agri/real_data/fact_weather_hourly.csv /data/agri/raw/hourly/ && "
        "hdfs dfs -put -f /data/agri/real_data/extended/fact_weather_daily.csv /data/agri/raw/daily/ && "
        "hdfs dfs -put -f /data/agri/real_data/extended/fact_cereal_annual.csv /data/agri/raw/yield/ && "
        "hdfs dfs -put -f /data/agri/real_data/extended/dim_location.csv /data/agri/raw/location/ && "
        "hdfs dfs -ls -R /data/agri/raw"
    )
    docker("exec", "namenode", "bash", "-c", script, timeout=600)


SPARK_ARGS = [
    "/spark/bin/spark-submit",
    "--master", "spark://spark-master:7077",
    "--conf", "spark.driver.memory=600m",
    "--conf", "spark.executor.memory=800m",
    "--conf", "spark.executor.cores=1",
    "--conf", "spark.cores.max=2",
    "--conf", "spark.sql.shuffle.partitions=8",
    "--conf", "spark.driver.host=spark-master",
]


def spark(job, extra=None, timeout=1800):
    docker("exec", "spark-master", *(SPARK_ARGS + ["/opt/agri/spark/jobs/" + job] + (extra or [])), timeout=timeout)


def rbac():
    docker(
        "exec", "hiveserver2", "bash", "-c",
        "beeline -u 'jdbc:hive2://localhost:10000/;auth=noSasl' -n hive "
        "--force=true -f /opt/agri/hive/sql/04_create_roles.sql || true",
        timeout=300,
    )
    docker(
        "exec", "hiveserver2", "bash", "-c",
        "beeline -u 'jdbc:hive2://localhost:10000/;auth=noSasl' -n hive "
        "--force=true -f /opt/agri/hive/sql/04_rbac.sql || true",
        timeout=300,
    )


def bootstrap():
    upload_raw()
    spark("01_clean_weather.py")
    spark("02_build_daily.py")
    spark("03_build_dimensions.py")
    spark("04_load_yield.py")
    spark("05_build_views.py")
    rbac()


def features():
    spark("06_build_features.py")


def ml():
    docker("exec", "agri-tools", "python", "/opt/agri/ml/train_yield.py", timeout=1800)
    docker("exec", "agri-tools", "python", "/opt/agri/ml/anomaly.py", timeout=3600)
    docker("exec", "agri-tools", "python", "/opt/agri/ml/forecast.py", timeout=1800)


def load_predictions():
    spark(
        "08_load_predictions.py",
        ["--csv", "/opt/agri/artifacts/ml/yield_predictions.csv", "--table", "ml_yield_predictions"],
    )
    spark(
        "08_load_predictions.py",
        ["--csv", "/opt/agri/artifacts/ml/ml_anomalies.csv", "--table", "ml_anomalies"],
    )


def produce(rows=100000, end_offset=0, extra=None):
    args = ["python", "/opt/agri/streaming/producer.py", "--rows", str(rows), "--end-offset", str(end_offset)]
    docker("exec", "agri-tools", *(args + (extra or [])), timeout=1800)


def stream_start(timeout_seconds=300):
    command = SPARK_ARGS + [
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "/opt/agri/spark/jobs/stream_weather.py",
        "--timeout-seconds", str(timeout_seconds),
    ]
    docker("exec", "-d", "spark-master", *command)


def stream_wait(timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = docker(
            "exec", "spark-master", "bash", "-lc",
            "hdfs dfs -ls -R /data/agri/processed/stream_weather 2>/dev/null | grep -c parquet || true",
            capture=True, check=False,
        )
        count = (result.stdout or "0").strip().splitlines()[-1] if result.stdout else "0"
        if count.isdigit() and int(count) > 0:
            print("  stream files:", count)
            return True
        time.sleep(5)
    return False


def smoke():
    checks = []
    checks.append(("HDFS", http_ok("http://localhost:9870")))
    checks.append(("Spark", http_ok("http://localhost:8080")))
    checks.append(("Airflow", http_ok("http://localhost:8085/health")))
    checks.append(("Superset", http_ok("http://localhost:8088/health")))
    checks.append(("Grafana", http_ok("http://localhost:3000/api/health")))
    checks.append(("Kafka", http_ok("http://localhost:9308/metrics")))
    checks.append(("Prometheus", http_ok("http://localhost:9090/-/healthy")))
    try:
        compose("ps", check=True)
        docker("exec", "namenode", "hdfs", "dfs", "-ls", "/", timeout=60)
        docker(
            "exec", "hiveserver2", "beeline", "-u", "jdbc:hive2://localhost:10000/;auth=noSasl", "-n", "hive",
            "-e", "SHOW DATABASES;", timeout=180,
        )
    except subprocess.CalledProcessError as exc:
        checks.append(("CLI", False))
        print(exc)
    print("\nSmoke result")
    for label, ok in checks:
        print("  %-12s %s" % (label, "OK" if ok else "FAIL"))
    return all(ok for _, ok in checks)


def status():
    compose("ps")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=["up", "down", "restart", "smoke", "ingest", "bootstrap", "features", "ml",
                 "load-predictions", "stream", "benchmark", "bi", "status", "all"],
    )
    args = parser.parse_args()

    if args.command == "up":
        compose("up", "-d", "--build", timeout=1800)
        wait_http("HDFS", "http://localhost:9870")
        wait_http("Spark", "http://localhost:8080")
        wait_http("Airflow", "http://localhost:8085/health", timeout=300)
        wait_http("Superset", "http://localhost:8088/health", timeout=300)
        wait_http("Grafana", "http://localhost:3000/api/health")
    elif args.command == "down":
        compose("down")
    elif args.command == "restart":
        compose("restart")
    elif args.command == "smoke":
        sys.exit(0 if smoke() else 1)
    elif args.command == "ingest":
        upload_raw()
    elif args.command == "bootstrap":
        bootstrap()
    elif args.command == "features":
        features()
    elif args.command == "ml":
        ml()
    elif args.command == "load-predictions":
        load_predictions()
    elif args.command == "stream":
        produce(100000)
        stream_start()
        stream_wait()
        spark("07_register_stream.py")
    elif args.command == "benchmark":
        docker("exec", "agri-tools", "python", "/opt/agri/benchmark/run_benchmark.py",
               timeout=7200)
    elif args.command == "bi":
        docker("exec", "agri-tools", "python", "/opt/agri/bi/create_superset.py", timeout=900)
    elif args.command == "status":
        status()
    elif args.command == "all":
        bootstrap()
        features()
        ml()
        load_predictions()
        produce(100000)
        stream_start()
        stream_wait()
        spark("07_register_stream.py")
        docker("exec", "agri-tools", "python", "/opt/agri/benchmark/run_benchmark.py", timeout=7200)
        docker("exec", "agri-tools", "python", "/opt/agri/tests/bad_batch_demo.py", timeout=600)


if __name__ == "__main__":
    main()
