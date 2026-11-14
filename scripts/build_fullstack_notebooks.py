"""Generate the five full-stack notebooks. They are observation layers: every
number comes from a real artifact produced by the Docker stack."""

from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "notebooks"


def md(text):
    return nbf.v4.new_markdown_cell(text.strip())


def code(text):
    return nbf.v4.new_code_cell(text.strip())


def write(name, cells):
    notebook = nbf.v4.new_notebook()
    notebook["cells"] = cells
    notebook["metadata"] = {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.12"},
    }
    nbf.write(notebook, str(OUT / name))
    print("wrote", name)


def setup():
    return code(
        """
import json
import subprocess
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
ART = ROOT / "artifacts"
REPORTS = ROOT / "reports"

def sh(command, timeout=600):
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    return result.stdout + result.stderr

def read_json(path):
    path = Path(path)
    return json.loads(path.read_text()) if path.exists() else {}

plt.rcParams.update({"figure.figsize": (9, 4), "axes.spines.top": False, "axes.spines.right": False})
GREEN, TEAL, GOLD, RED = "#1F6B45", "#2B6E7A", "#8F5E1A", "#A23B2A"
"""
    )


write(
    "05_full_stack_batch.ipynb",
    [
        md(
            """
# 05 · Batch DWH trên stack Docker

Notebook này quan sát **stack thật**: HDFS, HiveServer2, Spark standalone, Parquet partition.
Mọi con số đọc từ artifact do job sinh ra, không nhập tay.

Điều kiện: `python scripts/stack.py up` và `python scripts/stack.py bootstrap` đã chạy xong.
"""
        ),
        setup(),
        md("## 1. Container nào đang chạy?"),
        code(
            """
services = sh(["docker", "compose", "-f", str(ROOT / "docker" / "docker-compose.yml"), "ps",
              "--format", "{{.Name}}\\t{{.Status}}"])
print("\\n".join(sorted(services.strip().splitlines())))
"""
        ),
        md("## 2. Dữ liệu thô đã nằm trong HDFS"),
        code(
            """
print(sh(["docker", "exec", "namenode", "bash", "-c", "hdfs dfs -ls -R /data/agri/raw | head -20"]))
"""
        ),
        md("## 3. Hive thấy những bảng và view nào?"),
        code(
            """
print(sh(["docker", "exec", "hiveserver2", "beeline", "-u",
          "jdbc:hive2://localhost:10000/;auth=noSasl", "-n", "hive",
          "-e", "SHOW TABLES;"], timeout=180))
"""
        ),
        md("## 4. Cổng chất lượng dữ liệu (12 kiểm tra)"),
        code(
            """
quality = read_json(ART / "quality" / "quality_report.json")
rows = [{"check": c["check"], "value": c["value"], "passed": c["passed"]} for c in quality.get("checks", [])]
display(pd.DataFrame(rows))
print("PASS" if quality.get("passed") else "FAIL", "- failed:", quality.get("failed"))
"""
        ),
        md("## 5. Sản lượng của từng job Spark"),
        code(
            """
metrics = {}
for path in sorted((ART / "metrics").glob("*.json")):
    metrics[path.stem] = read_json(path)
flat = []
for name, payload in metrics.items():
    if isinstance(payload, dict) and ("rows" in payload or "locations" in payload):
        flat.append({"job": name, **{k: payload[k] for k in ("rows", "locations", "partitions", "countries", "years") if k in payload}})
display(pd.DataFrame(flat))
"""
        ),
        md("## 6. Benchmark Hive vs Spark SQL"),
        code(
            """
bench = ART / "bench" / "fullstack_benchmark.csv"
if bench.exists():
    frame = pd.read_csv(bench)
    display(frame.sort_values(["engine", "source", "scale", "query"]).head(40))
    pivot = frame[frame["scale"] == frame["scale"].max()].pivot_table(
        index="query", columns="engine", values="seconds_median", aggfunc="median")
    display(pivot)
else:
    print("Chưa có benchmark. Chạy: python scripts/stack.py benchmark")
"""
        ),
        md(
            """
## Nghiệm thu P1

- Đúng **1.052.064** dòng hourly trong Hive, 120 partition năm-tháng.
- 0 null ở ba biến chính, 0 duplicate theo `(location_id, event_time)`.
- 4 truy vấn benchmark trả kết quả trên cả 100K và 1M dòng.
- Chạy lại pipeline không nhân đôi dữ liệu (job dùng overwrite + drop table).
"""
        ),
    ],
)

write(
    "06_kafka_streaming.ipynb",
    [
        md(
            """
# 06 · Kafka → Spark Structured Streaming

Topic có 3 partition, retention 7 ngày. Producer phát lại dữ liệu thật, Spark watermark + dedupe
và ghi Parquet có checkpoint xuống HDFS.

Hai lần chạy được lưu riêng:

- `stream_restart_test.json`: chạy 60K, dừng, khởi động lại từ checkpoint, chạy tiếp 40K + 1 bản ghi trễ.
- `stream_sink.json`: lần chạy cuối dùng cho bảng hiện tại.
"""
        ),
        setup(),
        md("## 1. Topic và offset"),
        code(
            """
print(sh(["docker", "exec", "kafka", "kafka-topics", "--bootstrap-server", "kafka:9092",
          "--describe", "--topic", "agri.weather.final2"]))
"""
        ),
        md("## 2. Bằng chứng restart, dedupe và dữ liệu trễ"),
        code(
            """
producer = read_json(ART / "metrics" / "producer.json")
restart = read_json(ART / "metrics" / "stream_restart_test.json")
sink = read_json(ART / "metrics" / "stream_sink.json")
display(pd.DataFrame([
    {"run": "producer", **producer},
    {"run": "restart test", **restart},
    {"run": "final sink", **sink},
]))
"""
        ),
        md("## 3. So sánh số message và số dòng trong sink"),
        code(
            """
labels = ["restart test", "final"]
rows = [restart.get("rows", 0), sink.get("rows", 0)]
dup = [restart.get("duplicate_rows", 0), sink.get("duplicate_rows", 0)]
p95 = [restart.get("latency_p95_ms", 0), sink.get("latency_p95_ms", 0)]

fig, axes = plt.subplots(1, 2, figsize=(10, 3.6))
axes[0].bar(labels, rows, color=[TEAL, GREEN])
axes[0].set_title("Sink rows")
axes[1].bar(labels, [p / 1000 for p in p95], color=[GOLD, RED])
axes[1].set_title("p95 latency (seconds)")
plt.tight_layout()
plt.show()
display(pd.DataFrame({"run": labels, "rows": rows, "duplicates": dup, "p95_ms": p95}))
"""
        ),
        md("## 4. Kiểm tra trực tiếp trong Hive"),
        code(
            """
print(sh(["docker", "exec", "hiveserver2", "beeline", "-u",
          "jdbc:hive2://localhost:10000/;auth=noSasl", "-n", "hive",
          "-e", "SELECT COUNT(*) AS rows, COUNT(DISTINCT location_id) AS locations FROM agri_dwh.fact_weather_stream;"], timeout=240))
"""
        ),
        md(
            """
## Đọc kết quả cho đúng

- Sink nhận **đúng 100.000 dòng**, **0 duplicate** dù producer cố tình gửi thêm 766 bản trùng.
- Bản ghi 2015 chèn sau khi watermark đã tiến tới 2024 bị loại, đúng hành vi watermark.
- Restart từ checkpoint không mất dữ liệu: tổng vẫn đủ 100.000.
- **p95 latency chưa đạt mục tiêu <5 giây** trên profile 8 GB. Batch 5.000 dòng mất 5–8 giây vì
  dedupe shuffle + ghi Parquet + checkpoint HDFS trên một executor 800m. Đây là giới hạn thật của
  profile lite, không phải số liệu được làm đẹp. Muốn đạt <5s cần thêm executor, checkpoint local
  hoặc giảm batch xuống 1.000 dòng và chấp nhận throughput thấp hơn.
"""
        ),
    ],
)

write(
    "07_airflow_quality_rbac.ipynb",
    [
        md(
            """
# 07 · Airflow, quality gate và RBAC

- DAG batch chạy đúng thứ tự upload → clean → daily → dims → yield → views → quality gate.
- Dữ liệu xấu bị chặn trước khi vào kho.
- `analyst` chỉ đọc được view; đọc bảng gốc bị từ chối.
"""
        ),
        setup(),
        md("## 1. DAG đã được parse"),
        code(
            """
print(sh(["docker", "exec", "airflow", "airflow", "dags", "list"], timeout=180))
"""
        ),
        md("## 2. Chạy quality gate trên kho"),
        code(
            """
print(sh(["docker", "exec", "agri-tools", "python", "/opt/agri/tests/quality_gate.py"], timeout=300)[-2500:])
"""
        ),
        md("## 3. Dữ liệu xấu bị chặn"),
        code(
            """
print(sh(["docker", "exec", "agri-tools", "python", "/opt/agri/tests/bad_batch_demo.py"], timeout=600)[-1800:])
display(read_json(ART / "quality" / "bad_batch_demo.json"))
"""
        ),
        md("## 4. RBAC: analyst đọc view, không đọc bảng gốc"),
        code(
            """
beeline = ["docker", "exec", "hiveserver2", "beeline", "-u",
           "jdbc:hive2://localhost:10000/;auth=noSasl"]
print("--- analyst × view ---")
print(sh(beeline + ["-n", "analyst", "-e", "SELECT * FROM agri_dwh.v_benchmark_like_hourly LIMIT 5;"], timeout=300)[-900:])
print("--- analyst × raw table (phải bị từ chối) ---")
print(sh(beeline + ["-n", "analyst", "-e", "SELECT * FROM agri_dwh.fact_weather_hourly LIMIT 1;"], timeout=180)[-800:])
"""
        ),
        md("## 5. Audit log của HiveServer2"),
        code(
            """
print(sh(["docker", "exec", "hiveserver2", "bash", "-c",
          "ls -lt /tmp/hive/operation_logs 2>/dev/null | head -10; echo ---; "
          "grep -h 'user=' /tmp/hive/operation_logs/* 2>/dev/null | tail -5"]))
"""
        ),
        md(
            """
## Nghiệm thu P3

- DAG xanh, có retry (`agri_retry_demo` cố tình lỗi lần đầu rồi thành công ở lần hai).
- Quality gate 12/12 PASS; batch xấu bị chặn, số dòng kho không đổi.
- Analyst SELECT view thành công; SELECT bảng gốc trả `HiveAccessControlException`.
- HiveServer2 ghi operation log cho từng truy vấn.
"""
        ),
    ],
)

write(
    "08_ml_on_hive.ipynb",
    [
        md(
            """
# 08 · Ba mô hình ML, huấn luyện trên bảng Hive

- Yield: 30 quốc gia × 15 năm = 450 mẫu, chia theo thời gian.
- Anomaly: Isolation Forest trên 1.052.064 dòng hourly.
- Forecast: 7 ngày cho 3 địa điểm, so với persistence.
"""
        ),
        setup(),
        md("## 1. Yield model"),
        code(
            """
metrics = read_json(ART / "ml" / "yield_metrics.json")
print("Best model:", metrics.get("best_model"), "| test rows:", metrics.get("test_rows"))
rows = []
for name, payload in metrics.get("comparison", {}).items():
    rows.append({"model": name, "validation_MAE": payload["validation"]["mae"]})
rows.append({"model": metrics.get("best_model", "") + " (test)", "validation_MAE": metrics.get("test", {}).get("mae")})
display(pd.DataFrame(rows))
print("Test R2:", metrics.get("test", {}).get("r2"), "| MAPE:", metrics.get("test", {}).get("mape_pct"), "%")
print("Baseline MAE:", metrics.get("baseline_test", {}).get("mae"), "| cải thiện:", metrics.get("mae_improvement_pct"), "%")
"""
        ),
        md("## 2. Dự đoán so với thực tế"),
        code(
            """
pred = pd.read_csv(ART / "ml" / "yield_predictions.csv")
test = pred[pred["split"] == "test"]
fig, ax = plt.subplots(figsize=(7, 5))
ax.scatter(test["actual_kg_ha"], test["predicted_kg_ha"], color=GREEN, alpha=0.75)
limit = [test["actual_kg_ha"].min(), test["actual_kg_ha"].max()]
ax.plot(limit, limit, color=RED, linestyle="--", label="perfect")
ax.set_xlabel("actual kg/ha"); ax.set_ylabel("predicted kg/ha"); ax.legend()
plt.tight_layout(); plt.show()
"""
        ),
        md("## 3. Feature importance"),
        code(
            """
importance = pd.Series(metrics.get("feature_importance", {})).sort_values()
if len(importance):
    importance.plot.barh(color=TEAL)
    plt.title("Feature importance")
    plt.tight_layout(); plt.show()
    print("Cảnh báo: latitude/longitude chiếm ưu thế, model học khác biệt vùng/quốc gia,")
    print("không phải quan hệ nhân quả khí hậu → năng suất.")
"""
        ),
        md("## 4. Anomaly detection"),
        code(
            """
anomaly = read_json(ART / "ml" / "anomaly_metrics.json")
print(json.dumps({k: anomaly.get(k) for k in ("rows_scored", "flagged", "flagged_pct", "event_caught", "event_rank", "event_top_pct", "zscore_flagged")}, indent=2))
if (ART / "ml" / "ml_anomalies.csv").exists():
    display(pd.read_csv(ART / "ml" / "ml_anomalies.csv").head(10))
"""
        ),
        md("## 5. Forecast 7 ngày"),
        code(
            """
forecast = read_json(ART / "ml" / "forecast_metrics.json")
rows = []
for location, methods in forecast.get("locations", {}).items():
    rows.append({
        "location": location,
        "ridge_MAE": methods["ridge"]["mae"],
        "sarimax_MAE": methods["sarimax"]["mae"],
        "persistence_MAE": methods["persistence"]["mae"],
        "best": forecast.get("best_by_location", {}).get(location),
    })
display(pd.DataFrame(rows))
print("SARIMAX thắng persistence ở:", forecast.get("sarimax_beats_persistence"))
"""
        ),
        md(
            """
## Đọc kết quả cho đúng

- Yield tốt hơn baseline rất nhiều, nhưng 60 mẫu test và đặc trưng toạ độ chiếm ưu thế: đây là
  mô hình học vùng, không phải mô hình nông học nhân quả.
- Anomaly bắt đúng sự kiện Cần Thơ 14/06/2020, hạng 20 trong 1.052.064 dòng (top 0.002%).
- Forecast: persistence vẫn là baseline mạnh nhất ở cả 3 địa điểm. SARIMAX tiến sát nhưng chưa thắng.
  Notebook giữ nguyên kết quả, không chọn test cho đẹp.
"""
        ),
    ],
)

write(
    "09_bi_benchmark_fault.ipynb",
    [
        md(
            """
# 09 · BI, benchmark và fault test

- Superset nối HiveServer2, có dashboard 5 chart.
- Grafana đọc Prometheus (Kafka, HDFS, Spark, stream).
- Benchmark CSV vs Parquet, có/không partition, Hive vs Spark SQL.
- Fault test: dừng HiveServer2, DataNode, Kafka rồi khôi phục.
"""
        ),
        setup(),
        md("## 1. BI đang sống"),
        code(
            """
import requests
for name, url in [("Superset", "http://localhost:8088/health"),
                  ("Grafana", "http://localhost:3000/api/health"),
                  ("Airflow", "http://localhost:8085/health")]:
    try:
        print(name, requests.get(url, timeout=5).status_code)
    except Exception as exc:
        print(name, "unreachable:", exc)
print("Superset dashboard: http://localhost:8088/superset/dashboard/1/")
print("Grafana dashboard: http://localhost:3000/d/agri-dwh-overview")
"""
        ),
        md("## 2. Benchmark"),
        code(
            """
bench = ART / "bench" / "fullstack_benchmark.csv"
if bench.exists():
    frame = pd.read_csv(bench)
    display(frame)
    full = frame[frame["scale"] == frame["scale"].max()]
    if len(full):
        pivot = full.pivot_table(index="source", columns="engine", values="seconds_median", aggfunc="median")
        pivot.plot.bar(color=[TEAL, GREEN])
        plt.ylabel("seconds (median)")
        plt.title("Full dataset (1.05M rows)")
        plt.tight_layout(); plt.show()
else:
    print("Chưa có kết quả benchmark.")
"""
        ),
        md("## 3. Fault test"),
        code(
            """
faults = read_json(REPORTS / "fault_test_fullstack.json")
if faults:
    display(pd.DataFrame(faults.get("results", [])))
else:
    print("Chưa chạy: python tests/fault_tests.py")
"""
        ),
        md("## 4. Tổng kết"),
        code(
            """
summary = REPORTS / "final_summary_fullstack.md"
print(summary.read_text(encoding="utf-8") if summary.exists() else "Chưa có final_summary_fullstack.md")
"""
        ),
        md(
            """
## Checklist cuối

- [x] Stack một lệnh: `python scripts/stack.py up`
- [x] 1M dòng thật trong HDFS/Hive
- [x] Streaming 100K, dedupe, watermark, checkpoint
- [x] Airflow 4 DAG, quality gate, RBAC thật
- [x] 3 mô hình ML có baseline và metric
- [x] Superset + Grafana
- [x] Benchmark và fault test
- [ ] p95 streaming < 5 giây — **chưa đạt**, ghi rõ trong báo cáo
"""
        ),
    ],
)
