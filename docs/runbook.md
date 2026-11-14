# Runbook · Agri DWH full stack

## 1. Thành phần và cổng

| Thành phần | Container | Cổng host | Ghi chú |
|---|---|---:|---|
| HDFS NameNode UI | `namenode` | 9870 | WebHDFS bật |
| HDFS DataNode UI | `datanode` | 9864 | replication 1 |
| HiveServer2 | `hiveserver2` | 10000 | `auth=noSasl` |
| Hive Metastore | `hive-metastore` | 9083 (nội bộ) | Postgres backend |
| Spark Master | `spark-master` | 8080 / 7077 | standalone |
| Spark Worker | `spark-worker` | 8081 | 2 core, 1 GB |
| Kafka | `kafka` | 9092 (nội bộ) | KRaft, 1 broker |
| Airflow | `airflow` | 8085 | admin/admin |
| Superset | `superset` | 8088 | admin/admin |
| Grafana | `grafana` | 3000 | admin/admin, anonymous viewer |
| Prometheus | `prometheus` | 9090 | scrape 10s |
| Kafka exporter | `kafka-exporter` | 9308 | |
| Agri exporter | `agri-exporter` | 9310 | HDFS/Spark/pipeline |
| Postgres | `agri-postgres` | nội bộ | 3 database |

Tổng RAM đặt limit khoảng **6,6 GB**, phù hợp Docker Desktop 8 GB.

## 2. Lệnh thường dùng

```bash
cd D:\projectDW\agri-dwh

python scripts/stack.py up          # build + start toàn bộ
python scripts/stack.py smoke       # kiểm tra UI, HDFS, Hive, Spark
python scripts/stack.py ingest      # CSV -> HDFS
python scripts/stack.py bootstrap   # Spark jobs 01-05 (batch DWH)
python scripts/stack.py features    # job 06
python scripts/stack.py ml          # yield + anomaly + forecast
python scripts/stack.py load-predictions
python scripts/stack.py stream      # producer + streaming + register
python scripts/stack.py benchmark
python scripts/stack.py bi          # tạo database/dataset/chart/dashboard
python scripts/stack.py down
```

`make <target>` là wrapper mỏng của các lệnh trên.

## 3. Thứ tự pipeline

```text
CSV (data/real_data)
  │  stack.py ingest
  ▼
HDFS /data/agri/raw/{hourly,daily,yield,location}
  │  spark 01..05
  ▼
HDFS /data/agri/processed  +  Hive agri_dwh.*
  │  spark 06
  ▼
agri_dwh.ml_yield_features (450 dòng)
  │  ml/*.py
  ▼
artifacts/ml/*.csv  +  job 08
  ▼
agri_dwh.ml_yield_predictions, agri_dwh.ml_anomalies
```

Streaming là nhánh riêng:

```text
CSV -> Kafka agri.weather.hourly -> Spark Structured Streaming
    -> HDFS /data/agri/processed/stream_weather (checkpoint cùng chỗ)
    -> agri_dwh.fact_weather_stream
```

## 4. Kiểm tra sức khoẻ

```bash
python scripts/stack.py smoke
```

Kỳ vọng: HDFS, Spark, Airflow, Superset, Grafana, Kafka exporter, Prometheus đều OK;
`SHOW DATABASES` trả `agri_dwh` và `agri_raw`.

Kiểm tra nhanh bằng SQL:

```bash
docker exec hiveserver2 beeline -u "jdbc:hive2://localhost:10000/;auth=noSasl" -n hive \
  -e "SELECT COUNT(*) FROM agri_dwh.fact_weather_hourly;"
# 1052064
```

## 5. RBAC

- `hive` là admin; phải `SET ROLE ADMIN` trước khi grant.
- `analyst` chỉ có SELECT trên `v_benchmark_like_hourly`, `v_benchmark_like_daily`, `ml_anomalies`.
- `ml_engineer` có SELECT trên các bảng fact/dim cần cho feature.
- SQL standard authorization được bật trong `docker/hive/hive-site.xml`; doAs bật, HDFS proxy user `root` được phép impersonate.
- Kiểm tra:

```bash
docker exec hiveserver2 beeline -u "jdbc:hive2://localhost:10000/;auth=noSasl" -n analyst \
  -e "SELECT * FROM agri_dwh.v_benchmark_like_hourly LIMIT 5;"   # OK
docker exec hiveserver2 beeline -u "jdbc:hive2://localhost:10000/;auth=noSasl" -n analyst \
  -e "SELECT * FROM agri_dwh.fact_weather_hourly LIMIT 1;"      # Permission denied
```

## 6. Airflow

- UI: <http://localhost:8085>, admin/admin.
- DAG: `agri_batch_dwh` (02:00), `agri_ml_pipeline` (03:00), `agri_stream_health` (15 phút), `agri_retry_demo`.
- Trigger tay:

```bash
docker exec airflow airflow dags trigger agri_batch_dwh
docker exec airflow airflow dags list-runs -d agri_batch_dwh
```

- Task chạy lệnh qua Docker socket, tức là Airflow điều khiển Spark/Hive giống scheduler thật.

## 7. Sự cố thường gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `hive-metastore` restart 127 | entrypoint bde2020 tách chuỗi lệnh có dấu cách | dùng `start-metastore.sh` (đã có) |
| `authentication type 10 is not supported` | JDBC 9.4 không hỗ trợ SCRAM | Postgres dùng md5 (`POSTGRES_HOST_AUTH_METHOD=md5`) |
| HiveServer2 `Unrecognized option -Djava...` | `HIVE_OPTS` bị chèn sau class | không đặt `HIVE_OPTS` trong `hive-env.sh` |
| `User root is not allowed to impersonate analyst` | thiếu proxy user trên NameNode | `hadoop.proxyuser.root.*` trong core-site của namenode |
| `Permission denied ... v_benchmark_like_hourly` | user `hive` chưa `SET ROLE ADMIN` | helper `tooling/common.py` tự chạy |
| `No result set` khi chạy SET/DDL qua PyHive | cursor không có result | không gọi `fetchall()` sau DDL |
| Spark không thấy `spark-submit` | PATH image không có `/spark/bin` | dùng đường dẫn tuyệt đối |
| Spark đọc CSV local thành HDFS path | `fs.defaultFS` là HDFS | thêm tiền tố `file://` |
| Kafka producer `kafka.vendor.six.moves` | kafka-python 2.0.2 không hợp Python 3.12 | pin 2.1.5 |
| Superset API 403/500 | schema metadata chưa upgrade, JWT bị từ chối | `superset db upgrade && superset init`; script dùng cookie session |
| Docker Desktop lỗi `mkfs.ext4` | ổ C: hết chỗ | chuyển `docker_data.vhdx` sang D: bằng junction |

## 8. Dừng và khôi phục

```bash
python scripts/stack.py down          # giữ volume
python scripts/stack.py down -v       # xoá cả volume (không dùng khi cần dữ liệu)
python scripts/stack.py up
```

Sau khi `down` không mất dữ liệu vì HDFS/Postgres/Kafka đều dùng named volume.
Pipeline idempotent: chạy lại `bootstrap` ghi đè bảng, không nhân đôi dòng.

## 9. Giới hạn đã biết

- Profile 8 GB: streaming p95 khoảng 25–32 giây khi replay 100K, chưa đạt mục tiêu 5 giây.
- Không có TLS/Kerberos; NOSASL phù hợp demo nội bộ, không phù hợp production.
- Một DataNode, replication 1: fault test chỉ chứng minh phục hồi, không chứng minh chịu mất đĩa.
- Superset dashboard tạo bằng REST API, layout đơn giản một cột.
