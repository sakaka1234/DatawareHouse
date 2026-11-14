# Bộ dữ liệu thật cho mô phỏng kho dữ liệu nông nghiệp

Đây là bộ dữ liệu **lấy từ nguồn công khai**, không phải dữ liệu tổng hợp của bài báo gốc. Mục đích là thay ba file CSV ngẫu nhiên trong bài báo bằng dữ liệu thật, cùng cấu trúc đủ để dựng một kho dữ liệu nhỏ và chạy các truy vấn tương tự.

Tải lúc: **2026-11-11** (UTC).

## 1. Nội dung

| File | Số dòng | Dung lượng | Nội dung |
|---|---:|---:|---|
| `dim_location.csv` | 12 | 0,7 KB | 12 địa điểm nông nghiệp đại diện, kèm toạ độ thật |
| `fact_weather_daily.csv` | 43.836 | 2,46 MB | Thời tiết và độ ẩm đất theo ngày, 2015-01-01 → 2024-12-31 |
| `fact_weather_hourly.csv` | 1.052.064 | 61,27 MB | Thời tiết và độ ẩm đất theo giờ, cùng khoảng thời gian |
| `fact_cereal_annual.csv` | 100 | 4,9 KB | Diện tích, sản lượng, năng suất ngũ cốc theo quốc gia và năm |
| `benchmark_like_daily.csv` | 43.836 | 2,24 MB | Bảng phẳng giống schema bài báo, bản ngày |
| `benchmark_like_hourly.csv` | 1.052.064 | 63,26 MB | Bảng phẳng giống schema bài báo, bản giờ |
| `metadata.json` | — | 4,7 KB | URL gọi API, thời điểm tải, số dòng, số giá trị thiếu |
| `metadata_hourly.json` | — | 3,6 KB | Metadata riêng cho bản hourly |

Tổng dung lượng hiện tại khoảng **129 MB**. Nếu đưa `fact_weather_hourly.csv` lên HDFS với replication 3, dung lượng thực tế khoảng **184 MB** (61,27 MB × 3).

43.836 dòng = 12 địa điểm × 3.653 ngày (2015–2024, có năm nhuận). Lần tải này **không có giá trị thiếu** ở bảy biến thời tiết.

### `dim_location.csv`

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `location_id` | string | Mã địa điểm, ví dụ `VN-CT` |
| `name` | string | Tên địa danh |
| `country_iso3` | string | Mã quốc gia ISO-3166 alpha-3 |
| `country_name` | string | Tên quốc gia |
| `latitude` | double | Vĩ độ |
| `longitude` | double | Kinh độ |
| `crop_system` | string | Hệ cây trồng đại diện (rice, maize, wheat…) |

Toạ độ là **điểm đại diện cho vùng nông nghiệp**, không phải ranh giới thửa ruộng thật. NASA POWER trả dữ liệu cho ô lưới chứa điểm đó.

### `fact_weather_daily.csv`

| Cột | Kiểu | Đơn vị | Nguồn |
|---|---|---|---|
| `date` | date | ISO-8601 | NASA POWER |
| `location_id` | string | — | khoá ngoại |
| `t2m_c` | double | °C | T2M, nhiệt độ trung bình 2 m |
| `t2m_max_c` | double | °C | T2M_MAX |
| `t2m_min_c` | double | °C | T2M_MIN |
| `precip_mm` | double | mm/ngày | PRECTOTCORR |
| `soil_wetness_root` | double | 0–1 | GWETROOT, độ ẩm vùng rễ |
| `soil_wetness_top` | double | 0–1 | GWETTOP, độ ẩm lớp mặt |
| `rh2m_pct` | double | % | RH2M, độ ẩm tương đối |

### `fact_cereal_annual.csv`

| Cột | Kiểu | Đơn vị | Nguồn |
|---|---|---|---|
| `country_iso3` | string | — | World Bank |
| `country_name` | string | — | World Bank |
| `year` | int | năm | World Bank |
| `cereal_area_ha` | double | ha | AG.LND.CREL.HA |
| `cereal_production_mt` | double | metric ton | AG.PRD.CREL.MT |
| `cereal_yield_kg_per_ha` | double | kg/ha | AG.YLD.CREL.KG |

### `benchmark_like_daily.csv`

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `ts` | date | ngày |
| `location` | string | mã địa điểm |
| `soil_moisture` | double | độ ẩm vùng rễ, 0–1 |
| `temperature` | double | nhiệt độ trung bình ngày, °C |
| `yield_estimate` | double | năng suất ngũ cốc của quốc gia trong năm, kg/ha |
| `yield_grain` | string | luôn là `annual_country`, nhắc rằng năng suất là số liệu năm |

Đây là bảng tiện dụng để chạy ngay bốn loại truy vấn của bài báo mà không cần Hive. Cột `yield_estimate` **lặp lại theo từng ngày** trong năm; không phải năng suất đo hằng ngày.

### `fact_weather_hourly.csv`

| Cột | Kiểu | Đơn vị | Nguồn |
|---|---|---|---|
| `timestamp` | string | ISO-8601, giờ LST | NASA POWER hourly |
| `location_id` | string | — | khoá ngoại |
| `t2m_c` | double | °C | T2M |
| `rh2m_pct` | double | % | RH2M |
| `precip_mm_h` | double | **mm/giờ** | PRECTOTCORR |
| `ws2m_ms` | double | m/s | WS2M |
| `soil_wetness_root` | double | 0–1 | GWETROOT |
| `soil_wetness_top` | double | 0–1 | GWETTOP |

1.052.064 dòng = 12 địa điểm × 87.672 giờ (2015–2024). Dữ liệu được ghi theo từng địa điểm, không sắp xếp lại toàn bộ theo thời gian. `timestamp` dùng **giờ địa phương mặt trời (LST)** do NASA POWER trả về, không phải UTC. Cột mưa là **mm/giờ**, khác với `precip_mm` (mm/ngày) của bảng daily.

### `benchmark_like_hourly.csv`

Cùng schema với bản daily nhưng ở mức giờ:

| Cột | Ý nghĩa |
|---|---|
| `ts` | timestamp giờ LST |
| `location` | mã địa điểm |
| `soil_moisture` | `soil_wetness_root`, 0–1 |
| `temperature` | `t2m_c`, °C |
| `yield_estimate` | năng suất ngũ cốc quốc gia của năm, kg/ha |
| `yield_grain` | luôn là `annual_country` |

Đây là bảng **1.052.064 dòng** để chạy benchmark cỡ 1 triệu dòng của bài báo. Năng suất vẫn là số liệu năm, lặp lại cho từng giờ; không phải quan trắc năng suất theo giờ.

## 2. Nguồn và giấy phép

### NASA POWER

- Cổng: <https://power.larc.nasa.gov/>
- Tài liệu API ngày: <https://power.larc.nasa.gov/docs/services/api/temporal/daily/>
- Dữ liệu: **MERRA-2 reanalysis**, cộng đồng `AG` (agroclimatology), lưới khí tượng gốc 0,5° × 0,625°.
- Truy cập: không cần đăng ký, không cần API key.
- Điều kiện: dữ liệu NASA mở, được dùng và công bố với trích dẫn. Khi dùng, ghi nguồn NASA POWER và phiên bản MERRA-2.
- Đây là **dữ liệu tái phân tích**: quan trắc vệ tinh và mặt đất được đồng hoá vào mô hình khí quyển, **không phải cảm biến đặt tại ruộng**.

### World Bank — World Development Indicators

- Chỉ số năng suất ngũ cốc: <https://data.worldbank.org/indicator/AG.YLD.CREL.KG>
- API: <https://api.worldbank.org/v2/>
- Giấy phép: **CC BY 4.0**.
- Năng suất là **số liệu quốc gia theo năm**, không phải năng suất từng thửa. Các giá trị 2023–2024 có thể còn tạm tính.

## 3. Ghép với schema của bài báo

Bài báo dùng một bảng phẳng gồm `ts`, `location`, `soil_moisture`, `temperature`, `yield_estimate`. Bộ dữ liệu này tách thành mô hình hình sao:

| Bài báo | Bộ dữ liệu này | Ghi chú |
|---|---|---|
| `ts` | `fact_weather_daily.date` | theo ngày, không theo giây |
| `location` | `dim_location.location_id` | 12 địa điểm thật |
| `soil_moisture` | `fact_weather_daily.soil_wetness_root` | 0–1, không phải 10–40 như dữ liệu bài báo |
| `temperature` | `fact_weather_daily.t2m_c` | °C |
| `yield_estimate` | `fact_cereal_annual.cereal_yield_kg_per_ha` | theo năm và quốc gia; cần join |

Nếu cần một bảng phẳng giống bài báo, dùng view trong `scripts/hive_schema.sql`:

```sql
SELECT w.date AS ts,
       w.location_id AS location,
       w.soil_wetness_root AS soil_moisture,
       w.t2m_c AS temperature,
       y.cereal_yield_kg_per_ha AS yield_estimate
FROM fact_weather_daily w
JOIN dim_location l ON l.location_id = w.location_id
JOIN fact_cereal_annual y
  ON y.country_iso3 = l.country_iso3
 AND y.year = YEAR(w.date);
```

Năng suất năm sẽ lặp lại cho mọi ngày trong năm. Đó là join hợp lệ về mặt mô hình, nhưng không nên diễn giải là năng suất đo hằng ngày.

## 4. Chất lượng dữ liệu và điều cần lưu ý

- **Không có giá trị thiếu** trong lần tải này: 43.836 dòng daily (bảy biến thời tiết) và 1.052.064 dòng hourly (sáu biến). API có thể thay đổi trong lần tải sau.
- **Outlier mưa:** `2020-06-14`, Cần Thơ, `529,06 mm/ngày`; bản hourly cho thấy bốn giờ liên tiếp mưa rất lớn, đỉnh `137,93 mm/giờ` lúc 21:00 LST. Giá trị này đến từ MERRA-2 và rất cực đoan; nên đối chiếu với trạm đo hoặc xử lý như outlier trước khi tính trung bình. Đây cũng là ví dụ tốt cho bước anomaly detection của bài báo.
- **Nhiệt độ:** -26,37 °C (Iowa, 2019-01-30) đến 40,92 °C (Punjab, 2024-06-19) ở bản daily; bản hourly có cực trị -32,88 °C và 48,46 °C do lấy từng giờ.
- **Độ ẩm đất** là độ ẩm tương đối 0–1 của MERRA-2, không phải m³/m³ đo tại chỗ. Giá trị hourly thường gần như không đổi trong ngày vì MERRA-2 cập nhật theo bước thời gian thô hơn.
- **Năng suất** là số liệu quốc gia; không dùng để kết luận về từng cánh đồng.
- **Thời gian hourly** là LST, không phải UTC. Nếu ghép với dữ liệu khác, cần chuyển múi giờ hoặc ghi rõ quy ước.

### Nếu cần dữ liệu cảm biến thật tại chỗ

- **ISMN** (International Soil Moisture Network): ~3.000 trạm, tải cần đăng ký miễn phí, chỉ dùng cho mục đích khoa học và phải trích dẫn mạng lưới thành phần. <https://ismn.earth/data/data-view/>
- **USDA SCAN**: độ ẩm đất và nhiệt độ đất tại các trạm ở Mỹ, công khai. <https://www.wcc.nrcs.usda.gov/scan/>
- **USDA NASS Quick Stats**: năng suất cấp quận/hạt ở Mỹ, cần API key miễn phí. <https://quickstats.nass.usda.gov/api>

### Nếu cần năng suất cấp vùng/thửa

- **USDA NASS** cho Mỹ (county/state).
- **Eurostat** cho các nước EU (NUTS).
- **MapSPAM** cho lưới sản lượng cây trồng toàn cầu (10 km).
- **FAOSTAT** cho quốc gia (cùng mức với World Bank nhưng chi tiết theo cây).

## 5. Mở rộng quy mô

Bộ hiện tại đã có hai mức: **43.836 dòng daily** và **1.052.064 dòng hourly**. Có thể tăng tiếp bằng dữ liệu thật:

| Mục tiêu | Cách làm |
|---|---|
| ~1.000.000 dòng | **đã có**: 12 điểm × 87.672 giờ, 2015–2024 |
| ~2.000.000 dòng | thêm khoảng 12 địa điểm nữa, giữ nguyên 10 năm hourly |
| Hàng chục triệu dòng | dùng API **regional** để lấy lưới ô (mỗi request chỉ một tham số), hoặc mở rộng nhiều vùng và nhiều năm |

Tải lại bản daily:

```bash
python -I scripts/fetch_real_data.py --out "D:/projectDW/real_data"
```

Tải lại bản hourly (đọc danh sách địa điểm từ `dim_location.csv`):

```bash
python -I scripts/fetch_hourly_data.py \
  --locations "D:/projectDW/real_data/dim_location.csv" \
  --out "D:/projectDW/real_data"
```

Thời gian tải thực tế: khoảng 4–6 giây cho mỗi địa điểm 10 năm hourly; toàn bộ 12 địa điểm dưới 2 phút.

## 6. Trích dẫn gợi ý

- NASA POWER. *Prediction Of Worldwide Energy Resources (POWER)*. NASA Langley Research Center. <https://power.larc.nasa.gov/>
- World Bank. *World Development Indicators* (cereal area, production and yield). <https://databank.worldbank.org/source/world-development-indicators>
- Bài báo gốc: Theofilou, A.; Nastis, S.A.; Tsagris, M.; Rodriguez-Perez, S.; Mattas, K. Design and Implementation of a Scalable Data Warehouse for Agricultural Big Data. *Sustainability* 2025, 17, 3727. <https://doi.org/10.3390/su17083727>

> Bộ dữ liệu này **không phải** dữ liệu được công bố kèm bài báo. Bài báo ghi “Data sharing is not applicable” và dùng dữ liệu tổng hợp. Đây là bộ dữ liệu thật do người dùng tự thu thập từ hai nguồn mở ở trên để thay thế cho mục đích mô phỏng.
