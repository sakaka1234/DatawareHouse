"""Bad-batch demo: a corrupt CSV is rejected before it can reach the warehouse."""

import csv
import json
import os
import subprocess

from common import artifacts_path, connect, docker_exec, execute, write_json

BAD_DIR = "/opt/agri/artifacts/bad_batch"
HDFS_DIR = "/data/agri/raw/bad"


def main():
    os.makedirs(BAD_DIR, exist_ok=True)
    path = os.path.join(BAD_DIR, "bad_weather.csv")
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["timestamp", "location_id", "t2m_c", "rh2m_pct", "precip_mm_h", "ws2m_ms", "soil_wetness_root", "soil_wetness_top"]
        )
        for index in range(200):
            writer.writerow(
                [
                    "2024-06-19T%02d:00:00" % (index % 24),
                    "" if index % 10 == 0 else "VN-CT",
                    "200.0" if index % 2 == 0 else "31.2",
                    "72.0",
                    "-5.0" if index % 3 == 0 else "0.0",
                    "2.4",
                    "0.71",
                    "0.69",
                ]
            )

    docker_exec("namenode", "hdfs dfs -mkdir -p " + HDFS_DIR)
    docker_exec("namenode", "hdfs dfs -put -f /opt/agri/artifacts/bad_batch/bad_weather.csv " + HDFS_DIR + "/")

    execute("DROP TABLE IF EXISTS agri_raw.bad_weather_hourly")
    execute(
        """
        CREATE EXTERNAL TABLE agri_raw.bad_weather_hourly (
          `timestamp` STRING, location_id STRING, t2m_c DOUBLE, rh2m_pct DOUBLE,
          precip_mm_h DOUBLE, ws2m_ms DOUBLE, soil_wetness_root DOUBLE,
          soil_wetness_top DOUBLE
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 'hdfs://namenode:9000%s'
        TBLPROPERTIES ('skip.header.line.count' = '1')
        """
        % HDFS_DIR
    )

    gate = subprocess.run(
        ["python", "/opt/agri/tests/quality_gate.py", "--bad",
         "--report", "/opt/agri/artifacts/quality/bad_batch_report.json"],
        text=True,
        capture_output=True,
    )
    warehouse_count = execute("SELECT COUNT(*) FROM agri_dwh.fact_weather_hourly")[0][0]
    report = {
        "gate_exit_code": gate.returncode,
        "blocked": gate.returncode != 0,
        "warehouse_rows_after": int(warehouse_count),
        "stdout": gate.stdout[-2000:],
        "stderr": gate.stderr[-2000:],
    }
    write_json(artifacts_path("quality", "bad_batch_demo.json"), report)
    print(json.dumps(report, indent=2))
    return 0 if report["blocked"] and warehouse_count == 1052064 else 1


if __name__ == "__main__":
    raise SystemExit(main())
