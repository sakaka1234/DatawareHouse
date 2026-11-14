# Benchmark · Hive vs Spark SQL

## Cách đo

- Nguồn: 100.000 dòng và toàn bộ 1.052.064 dòng.
- Định dạng: CSV thô (TextFile), Parquet partition `year/month` (view có join dim/yield),
  Parquet không partition (join dim/yield tại chỗ).
- Engine: HiveServer2 (local MapReduce) và Spark SQL (standalone, 1 executor 800m).
- 4 truy vấn: COUNT, AVG yield theo location, AVG soil khi nhiệt > 25, Top 10 yield.
- Mỗi truy vấn chạy khởi động một lần, sau đó lấy trung vị 2 lần đo.
- Hive chỉ chạy trên Parquet partition; Spark chạy đủ 3 nguồn × 2 quy mô.

Kết quả thô: `artifacts/bench/fullstack_benchmark.csv` và `.json`.

## Kết quả (giây, trung vị)

| Engine | Nguồn | 100K | 1.052.064 |
|---|---|---:|---:|
| Hive | Parquet partition | 19,5–20,6 | 17,7–31,8 |
| Spark SQL | CSV | 0,91–1,25 | 2,16–2,59 |
| Spark SQL | Parquet partition (view) | 3,32–4,49 | 5,31–8,92 |
| Spark SQL | Parquet không partition | 0,78–1,16 | 1,93–2,52 |

## Cách đọc

- Đây là benchmark **tương đối** trên một máy 8 GB, không so sánh trực tiếp với cluster trong bài báo.
- Hive chậm hơn khoảng 10–20× vì chạy MapReduce local trong cùng container 768 MB; đây là lựa chọn
  có chủ đích để không phải dựng YARN/Tez trong profile lite.
- **Parquet partition chậm hơn Parquet không partition** trong phép đo này vì nguồn partition là
  một view có join với `dim_location` và `fact_cereal_annual`, còn bảng không partition được join
  theo cùng cách nhưng Spark chọn plan khác. Đây không phải phép so sánh thuần định dạng; nó đo
  đúng thứ người dùng sẽ chạy (view benchmark).
- CSV nhanh bất ngờ vì file chỉ 61 MB và Spark đọc local HDFS với 1 executor; ở quy mô lớn hơn,
  parse CSV sẽ đắt hơn Parquet.
- Partition pruning có tác dụng khi truy vấn lọc theo năm/tháng; 4 truy vấn chuẩn không lọc thời
  gian nên không thể hiện lợi thế này. Notebook 01 (lite) có phần `EXPLAIN` cho thấy pruning.

## Tái lập

```bash
python scripts/stack.py benchmark
# hoặc chạy từng phần
docker exec agri-tools python /opt/agri/benchmark/run_benchmark.py --only spark
docker exec agri-tools python /opt/agri/benchmark/run_benchmark.py --only hive --scales 100000
```
