"""Fault tests for the full stack. Run from the host, not inside a container."""

import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "fault_test_fullstack.json"
BEELINE = [
    "docker", "exec", "hiveserver2", "beeline",
    "-u", "jdbc:hive2://localhost:10000/;auth=noSasl", "-n", "hive",
    "-e", "SELECT 1;",
]


def run(command, timeout=180):
    try:
        return subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def wait_for(predicate, timeout=240, interval=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def hive_ok():
    result = run(BEELINE, timeout=90)
    return bool(result and result.returncode == 0)


def hdfs_ok():
    result = run(
        ["docker", "exec", "namenode", "bash", "-c",
         "hdfs dfs -cat /data/agri/raw/location/dim_location.csv | head -2"],
        timeout=90,
    )
    return bool(result and result.returncode == 0 and "location_id" in result.stdout)


def producer_ok():
    result = run(
        ["docker", "exec", "agri-tools", "python", "/opt/agri/streaming/producer.py",
         "--rows", "10", "--rate", "1000", "--topic", "agri.weather.final2"],
        timeout=120,
    )
    return bool(result and result.returncode == 0)


def record(results, name, before, during, after, recovered):
    results.append(
        {
            "scenario": name,
            "before": before,
            "during_outage": during,
            "after_restart": after,
            "recovered": recovered,
        }
    )
    print(name, before, during, after, recovered, flush=True)


def main():
    results = []
    if not hive_ok():
        raise SystemExit("Hive is not healthy before the test")

    print("HiveServer2", flush=True)
    before = hive_ok()
    run(["docker", "stop", "hiveserver2"], timeout=120)
    during = hive_ok()
    run(["docker", "start", "hiveserver2"], timeout=120)
    after = wait_for(hive_ok, timeout=300)
    record(results, "hiveserver2 stopped and restarted", before, during, after, after)

    print("DataNode", flush=True)
    before = hdfs_ok()
    run(["docker", "stop", "datanode"], timeout=120)
    during = hdfs_ok()
    run(["docker", "start", "datanode"], timeout=120)
    after = wait_for(hdfs_ok, timeout=300)
    record(results, "datanode stopped and restarted", before, during, after, after)

    print("Kafka", flush=True)
    before = producer_ok()
    run(["docker", "stop", "kafka"], timeout=120)
    during = producer_ok()
    run(["docker", "start", "kafka"], timeout=120)
    after = wait_for(producer_ok, timeout=300)
    record(results, "kafka broker stopped and restarted", before, during, after, after)

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps({"results": results}, indent=2))
    print("wrote", REPORT)
    return 0 if all(item["recovered"] for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
