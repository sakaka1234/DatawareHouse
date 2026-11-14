# Agri DWH + ML — mô phỏng bài báo bằng dữ liệu thật

Mô phỏng *Design and Implementation of a Scalable Data Warehouse for Agricultural Big Data*
(Sustainability 2025, 17, 3727) bằng **dữ liệu thật** từ NASA POWER và World Bank.

Có hai profile:

| Profile | Engine | Dùng khi |
|---|---|---|
| **Full stack (chính)** | Docker: HDFS + Hive + Spark + Kafka + Airflow + Superset + Grafana + Prometheus | Có Docker Desktop ≥ 8 GB RAM |
| **Lite** | DuckDB + Parquet + Plotly trên Windows | Không có Docker; vẫn giữ đúng schema, grain, ML |

Notebook là lớp quan sát chính. Code dùng chung nằm trong `src/` (lite) và các thư mục job
(`spark/`, `ml/`, `streaming/`, `benchmark/`, `tests/`, `tooling/`).

## Chạy full stack

```bash
cd D:\projectDW\agri-dwh

python scripts/stack.py up          # build + start, khoảng 2-3 phút
python scripts/stack.py smoke
python scripts/stack.py bootstrap   # CSV -> HDFS -> Spark -> Hive
python scripts/stack.py features    # bảng feature cho ML
python scripts/stack.py ml          # yield + anomaly + forecast
python scripts/stack.py load-predictions
python scripts/stack.py stream      # replay 100K qua Kafka
python scripts/stack.py benchmark
python scripts/stack.py bi          # Superset database/dataset/chart/dashboard
```

Sau đó mở Jupyter:

```bash
python -m jupyter lab
```

và chạy lần lượt `notebooks/05_*.ipynb` → `09_*.ipynb`.

Yêu cầu: Docker Desktop bật, **8 GB RAM** cho VM, ổ đĩa còn ≥ 20 GB. Chi tiết port, lỗi thường gặp
và cách khôi phục: [`docs/runbook.md`](docs/runbook.md).

## Cổng truy cập

| UI | URL | Tài khoản |
|---|---|---|
| HDFS NameNode | <http://localhost:9870> | — |
| Spark Master | <http://localhost:8080> | — |
| Airflow | <http://localhost:8085> | admin / admin |
| Superset | <http://localhost:8088> | admin / admin |
| Grafana | <http://localhost:3000> | admin / admin (viewer ẩn danh) |
| Prometheus | <http://localhost:9090> | — |

## Dữ liệu

| Bảng / file | Số dòng | Nguồn |
|---|---:|---|
| `fact_weather_hourly` | 1.052.064 | NASA POWER hourly / MERRA-2, 12 địa điểm, 2015–2024 |
| `fact_weather_daily` | 175.328 | NASA POWER daily, 32 địa điểm, 2010–2024 |
| `fact_cereal_annual` | 450 | World Bank WDI, 30 quốc gia × 15 năm |
| `dim_location` | 32 | 30 quốc gia, 32 địa điểm đại diện |
| `dim_date` | 5.479 | 2010-01-01 → 2024-12-31 |
| `ml_yield_features` | 450 | tổng hợp daily → country-year |
| `fact_weather_stream` | 100.000 | sink của Spark Structured Streaming |

- NASA POWER là **dữ liệu tái phân tích** trên lưới 0,5° × 0,625°, không phải cảm biến tại ruộng.
- Năng suất World Bank là **số liệu quốc gia theo năm**, không phải từng thửa.
- `timestamp` giữ LST như API trả về. Mưa hourly là mm/giờ, daily là mm/ngày.

## Kết quả đã đo

### Batch DWH

- 1.052.064 dòng hourly, **120 partition** năm-tháng, 12 địa điểm.
- 0 null ở 3 biến chính, 0 duplicate theo `(location_id, event_time)`.
- Quality gate **12/12 PASS**; chạy lại pipeline không nhân đôi dữ liệu.
- Hive DDL, view benchmark và RBAC thật (analyst chỉ đọc view).

### Streaming

| Chỉ số | Kết quả |
|---|---|
| Message đã gửi | 100.000 (+ 766 bản trùng cố ý + 1 bản ghi 2015 chèn muộn) |
| Sink rows | **100.000** |
| Duplicate trong sink | **0** |
| Restart từ checkpoint | không mất dữ liệu |
| Bản ghi trễ 2015 | bị watermark loại |
| Throughput producer (đo riêng) | **6.397 msg/s** |
| p95 latency end-to-end | **25–32 giây — chưa đạt mục tiêu < 5 giây** |

p95 cao vì mỗi batch 5.000 dòng mất 5–8 giây (dedupe shuffle + ghi Parquet + checkpoint HDFS trên
một executor 800m). Đây là giới hạn thật của profile 8 GB; notebook 06 ghi rõ, không chỉnh số.

### ML

**Yield — RandomForest, chia theo thời gian:**

| | MAE (kg/ha) | R² | MAPE |
|---|---:|---:|---:|
| RandomForest (test 2023–2024) | **416,8** | **0,909** | **10,65%** |
| Baseline mean | 1.432,4 | −0,02 | 41,63% |

Cải thiện MAE **70,9%** so với baseline. Cảnh báo: 60 mẫu test; `latitude`/`longitude` là feature
quan trọng nhất, nên model học khác biệt vùng/quốc gia rất mạnh, không phải quan hệ nhân quả.

**Anomaly — Isolation Forest + robust z-score:**

- 5.441 giờ bị đánh dấu trên 1.052.064 dòng (0,52%).
- Sự kiện Cần Thơ 14/06/2020 được bắt, **hạng 20** (top 0,002%).
- Top anomaly tập trung ở US-KS, US-IA, IN-PB, MA-MEK.

**Forecast 7 ngày (443 origin × 3 địa điểm):**

| Địa điểm | Ridge MAE | SARIMAX MAE | Persistence MAE | Tốt nhất |
|---|---:|---:|---:|---|
| FR-CVL | 0,0185 | 0,0102 | **0,00999** | persistence |
| US-IA | 0,0227 | 0,0120 | **0,01164** | persistence |
| VN-CT | 0,0197 | 0,0145 | **0,01447** | persistence |

Persistence thắng ở cả ba. SARIMAX tiến sát nhưng chưa vượt; kết quả được giữ trung thực.

### Vận hành

- 4 DAG Airflow: batch, ML, stream health, retry demo; retry thật hoạt động.
- Dữ liệu xấu bị quality gate chặn trước khi vào kho (kho vẫn đủ 1.052.064 dòng).
- RBAC: `analyst` SELECT view thành công; SELECT bảng gốc trả `HiveAccessControlException`.
- Fault test: dừng/khôi phục HiveServer2, DataNode, Kafka — xem `reports/fault_test_fullstack.json`.
- Superset có dashboard 5 chart; Grafana có 6 panel (Kafka, HDFS, Spark, stream).

## Notebook

| Notebook | Nội dung |
|---|---|
| `00_setup_and_profile.ipynb` | Kiểm kê dữ liệu, profiling, biểu đồ khí hậu (lite) |
| `01_batch_dwh.ipynb` | DuckDB, partition lake, benchmark (lite) |
| `02_streaming_simulation.ipynb` | Replay file-based, dedupe, watermark (lite) |
| `03_ml_models.ipynb` | Yield, anomaly, forecast trên 1M dòng (lite) |
| `04_dashboard_and_fault_tests.ipynb` | Plotly dashboard, fault test partition (lite) |
| `05_full_stack_batch.ipynb` | Container, HDFS, Hive, quality gate, benchmark **full stack** |
| `06_kafka_streaming.ipynb` | Kafka offsets, restart, dedupe, latency **full stack** |
| `07_airflow_quality_rbac.ipynb` | DAG, bad batch, RBAC, audit log **full stack** |
| `08_ml_on_hive.ipynb` | Ba mô hình đọc từ bảng Hive **full stack** |
| `09_bi_benchmark_fault.ipynb` | Superset/Grafana, benchmark, fault test **full stack** |

## Cấu trúc

```text
agri-dwh/
├── docker/                 # compose, hive-site, postgres init, Superset/Airflow/Tools images
├── spark/jobs/             # 01..10 PySpark jobs + streaming
├── hive/sql/               # DDL, views, RBAC
├── airflow/dags/           # 4 DAG
├── ml/                     # yield, anomaly, forecast
├── streaming/              # producer, health check
├── benchmark/              # Hive vs Spark matrix
├── tests/                  # quality gate, bad batch, fault tests
├── tooling/                # PyHive/Docker helpers, Prometheus exporter
├── bi/                     # tạo Superset assets qua REST API
├── notebooks/              # 00-09
├── src/                    # bản lite DuckDB
├── scripts/                # stack.py, fetch data, build notebooks
├── data/real_data/         # dữ liệu thật + extended/
├── docs/                   # runbook, benchmark
├── artifacts/              # metric JSON, model, predictions (sinh ra khi chạy)
├── reports/                # hình, dashboard, fault report (sinh ra khi chạy)
└── Makefile                # wrapper của scripts/stack.py
```

## Khác biệt so với bài báo

| Bài báo | Bản này |
|---|---|
| HDFS + Hive + Spark + Kafka + Airflow + Superset + Grafana | **Đã dựng đủ**, chạy bằng Docker Compose |
| 10 triệu dòng tổng hợp | 1.052.064 dòng hourly + 175K daily thật |
| Spark MLlib / ARIMA / LSTM | scikit-learn RandomForest + IsolationForest + SARIMAX |
| RBAC + AES-256 + TLS | RBAC thật; **chưa có TLS/Kerberos/AES** — đây là khoảng trống production |
| Cluster nhiều node | 1 NameNode, 1 DataNode, 1 worker, replication 1 |
| Dashboard tương tác đầy đủ | Superset 5 chart + Grafana 6 panel; layout đơn giản |

## Bản lite (không Docker)

Xem phần cuối của repo: `python run_all.py` chạy 5 notebook 00–04 bằng DuckDB, không cần Docker.
Kết quả lite trước đó: yield MAE 359,8; anomaly 5.440 giờ; forecast Ridge chỉ thắng persistence ở
FR-CVL. Bản full stack dùng dữ liệu mở rộng 30 quốc gia nên số yield khác (416,8) — không phải lỗi.

## Tái lập

```bash
python -m pip install -r requirements.txt
python scripts/stack.py up
python scripts/stack.py all     # bootstrap + ML + stream + benchmark
```

## Nguồn

- NASA POWER: <https://power.larc.nasa.gov/>
- World Bank WDI: <https://data.worldbank.org/indicator/AG.YLD.CREL.KG>
- Bài báo: Theofilou et al., *Sustainability* 2025, 17, 3727. <https://doi.org/10.3390/su17083727>
