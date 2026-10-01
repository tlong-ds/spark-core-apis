# EDA cho Nhiệm vụ 2: Airline Delays

## Mục tiêu và phạm vi

EDA này profile bộ Kaggle `flights.csv` (592,406,591 bytes) để chọn tham số benchmark hợp lý cho Task 2 — không phải phân tích sâu dữ liệu hàng không. Không di chuyển/sửa file nguồn. Profile mô tả trên máy local, không phải production benchmark.

## Cách profile và môi trường

- Spark 3.5.9, `local[2]`, Python 3.11.9.
- `spark.sql.adaptive.enabled=true`, `spark.sql.shuffle.partitions=8`.
- Đọc CSV bằng schema tường minh (`airline_schema.py`), không dùng `inferSchema`.
- Chạy từ workspace root: `python .\spark-storage-benchmark\eda_airline.py` → output `spark-storage-benchmark/results/eda_task2.json`.

## Findings từ dữ liệu

| Quan sát | Số liệu | Ý nghĩa đối với Task 2 |
|---|---:|---|
| Quy mô và schema | 5,819,079 dòng; 592.4 MB; 31 cột (26 `int`, 5 `string`); 0 duplicate | Schema tường minh nằm trong `airline_schema.py`; các phân bố đầy đủ nằm trong JSON. |
| Null ở cột chính | `AIRLINE`, `YEAR`, `MONTH`, `DISTANCE`: 0; `ARRIVAL_DELAY`: 105,071 (1.81%); `DEPARTURE_DELAY`: 86,153 (1.48%) | Delay null gắn với trạng thái hủy/chuyển hướng; `AVG` bỏ qua null còn `COUNT(*)` vẫn tính mọi dòng. |
| Null cấu trúc theo nghiệp vụ | `CANCELLATION_REASON`: 5,729,195 null (98.45%); cả 5 cột delay-cause cùng null ở 4,755,640 dòng, cùng non-null ở 1,063,439 dòng; mixed-null: 0. | Structured missingness là đặc điểm nên xét khi so sánh encoding/dung lượng. Spark 3.5.9 đang dùng `ignoreNullFields=true`, nên không kết luận JSON lặp các field null; cần dựa vào bytes thực đo. |
| Year / Month | Chỉ có năm 2015; 12 tháng. Tháng 2 thấp nhất: 429,191 (7.38%); tháng 7 cao nhất: 520,718 (8.95%). Month max/min = 1.21×. | Năm không phân biệt được để pruning; phân bố tháng không cho thấy lệch mạnh. |
| Carrier skew | 14 hãng; WN: 1,261,855 (21.68%); VX: 61,903 (1.06%), chênh khoảng 20.4×. | Có lệch theo key; đây chưa phải bằng chứng runtime skew nếu chưa thấy task imbalance trong Spark UI. |
| Delay và distance | Arrival: min -87, median -5, p95 66, p99 167, max 1,971. Departure: median -2, p95 67, p99 168, max 1,988. Distance: min 21, mean 822.36, median 647, p95 2,227, p99 2,588, max 4,983. | Giữ nguyên dữ liệu; các giá trị cực đại cần được hiểu theo phân phối, không tự ý cắt outlier. |
| W2 selective filter | `YEAR=2015, MONTH=1` (469,968 dòng, 8.08% tổng số), đồng bộ với benchmark config. | Dùng cùng tháng trong mọi cell so sánh; xác nhận pruning bằng formatted plan/Spark UI. Tháng 2 ít dòng hơn nhưng không được chọn để giữ nhất quán với config hiện tại. |

## Lưu ý benchmark

- Đọc CSV 31 cột trong khi W3 chỉ cần 2 cột: kiểm tra `ReadSchema`. `PartitionFilters`, `PushedFilters` và `ReadSchema` là bằng chứng cho các cơ chế khác nhau.
- `partition_results.csv` và timing 25.19/8.74 giây là snapshot một lượt của benchmark cũ. Code benchmark mới đã thiết kế layout W1 với warm-up và 5 lượt đo; dùng `layout_raw.csv`/`layout_summary.csv` sau khi chạy mới để kết luận, không trộn snapshot cũ vào kết quả mới.
- Đây là profile Windows local, chưa có Spark UI evidence; lệch theo Carrier mới là skew của key, chưa đủ kết luận runtime skew.