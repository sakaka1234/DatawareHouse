"""Five-minute demo: verify the stack, replay a small stream, show the results."""

import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(label, command, timeout=600):
    print("\n== %s ==" % label, flush=True)
    started = time.time()
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    output = (result.stdout + result.stderr).strip().splitlines()
    for line in output[-12:]:
        print("  " + line)
    print("  (%.1fs, exit %d)" % (time.time() - started, result.returncode))
    return result


def show(path, keys):
    path = ROOT / path
    if not path.exists():
        print("  missing:", path)
        return
    payload = json.loads(path.read_text())
    print("  " + ", ".join("%s=%s" % (key, payload.get(key)) for key in keys if key in payload))


def main():
    print("Agri DWH demo")
    run("stack status", ["python", str(ROOT / "scripts" / "stack.py"), "status"], timeout=120)
    run("smoke", ["python", str(ROOT / "scripts" / "stack.py"), "smoke"], timeout=300)
    run(
        "warehouse sizes",
        ["docker", "exec", "hiveserver2", "beeline", "-u",
         "jdbc:hive2://localhost:10000/;auth=noSasl", "-n", "hive", "-e",
         "SET ROLE ADMIN; "
         "SELECT COUNT(*) FROM agri_dwh.fact_weather_hourly; "
         "SELECT COUNT(*) FROM agri_dwh.fact_weather_stream;"],
        timeout=300,
    )
    print("\n== metrics ==")
    show("artifacts/metrics/01_clean_weather.json", ["rows", "partitions"])
    show("artifacts/metrics/stream_sink.json", ["rows", "duplicate_rows", "latency_p95_ms"])
    show("artifacts/ml/yield_metrics.json", ["best_model", "mae_improvement_pct"])
    show("artifacts/ml/anomaly_metrics.json", ["flagged", "event_caught", "event_rank"])
    show("artifacts/quality/quality_report.json", ["passed", "failed"])
    print("\nSuperset:  http://localhost:8088/superset/dashboard/1/")
    print("Airflow:   http://localhost:8085")
    print("Grafana:   http://localhost:3000/d/agri-dwh-overview")
    return 0


if __name__ == "__main__":
    sys.exit(main())
