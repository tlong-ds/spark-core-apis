# EDA cho Nhiệm vụ 2: Airline Delays

## Mục tiêu và phạm vi

EDA này profile bộ Kaggle `flights.csv` để xác định các đặc điểm có thể ảnh hưởng đến cách Spark đọc và truy vấn dữ liệu. File được profile nằm tại `../flights.csv` tính từ thư mục `spark-storage-benchmark` (592,406,591 bytes). Không di chuyển hay sửa file nguồn. Đây là profile mô tả trên một máy local, không phải production benchmark.

## Cách profile và môi trường

- Spark 3.5.9, `local[2]`, Python 3.11.9 for the final reproducible EDA run.
- `spark.sql.adaptive.enabled=true`, `spark.sql.shuffle.partitions=8`.
- Đọc CSV có header bằng schema tường minh trong `airline_schema.py`; schema đã đối chiếu khớp 31 tên cột trong header.
- Đếm dòng trùng chính xác bằng `row_count - dropDuplicates().count()`, null, cardinality, phân bố theo hãng/tháng, trạng thái hủy/chuyển hướng, partition count và phân vị delay. Phân vị được tính bằng `approxQuantile` với relative error 0.0001.
- Chạy từ workspace root bằng `python .\spark-storage-benchmark\eda_airline.py`; JSON mặc định được ghi vào `spark-storage-benchmark/results/eda_task2.json`.

## Findings từ dữ liệu

| Quan sát | Số liệu | Ý nghĩa đối với Task 2 |
|---|---:|---|
| Quy mô | 5,819,079 dòng; 31 cột; 592.4 MB CSV | Đủ lớn để chi phí parse và đọc dư cột đáng đo, nhưng kết quả local vẫn phụ thuộc phần cứng và cache. |
| Dòng trùng | 0; số dòng distinct = 5,819,079 | `COUNT(*)` trên input hiện tại không bị tăng bởi duplicate rows chính xác; vẫn nên giữ kiểm tra này như giả định có thể tái xác nhận khi đổi input. |
| CSV input partitions | 5 partitions; `spark.sql.files.maxPartitionBytes=134217728b` (128 MiB), `spark.sql.files.openCostInBytes=4194304b` | Đây là số scan partitions quan sát được ngay sau read, không phải partitions sau shuffle. Ép `repartition(4)` tạo shuffle và giảm số output partitions; có thể tăng dữ liệu mỗi task/giảm song song, nhưng riêng con số 5 không chứng minh nguyên nhân OOM. |
| Khoảng thời gian | Chỉ có năm 2015; đủ 12 tháng | Partition theo `YEAR` không thể lọc theo năm trên dataset này; lọc theo `YEAR=2015, MONTH=7` vẫn có thể thử nghiệm pruning theo tháng. |
| Phân bố tháng | Thấp nhất: tháng 2, 429,191 (7.38%); cao nhất: tháng 7, 520,718 (8.95%) | Các tháng khá đồng đều; pruning một tháng có thể bỏ qua phần lớn input về mặt logic, nhưng bytes/task thực tế phải xác nhận bằng plan và Spark UI. |
| Số hãng | 14 hãng; WN 1,261,855 dòng (21.68%), VX 61,903 (1.06%) | Phân bố theo hãng lệch rõ. Đây là dấu hiệu nên kiểm tra, không tự nó chứng minh task skew; aggregation theo hãng chỉ có 14 key. |
| `ARRIVAL_DELAY` null | 105,071 (1.81%) | Cross-tab xác nhận cả 15,187 dòng `DIVERTED=1` và 89,884 dòng `CANCELLED=1` đều null; nhóm còn lại không null. Missingness gắn với trạng thái chuyến bay, không tự động là malformed record. |
| `DEPARTURE_DELAY` null | 86,153 (1.48%) | Toàn bộ null nằm trong nhóm hủy (86,153/89,884); nhóm không hủy và nhóm chuyển hướng không có null. Không thay bằng 0 hay loại cả dòng khi chưa định nghĩa policy. |
| `CANCELLATION_REASON` null | 5,729,195 (98.45%) | Cột `CANCELLATION_REASON` có giá trị ở toàn bộ 89,884 dòng bị hủy; các dòng không bị hủy, bao gồm diverted flights, đều có giá trị NULL. |
| Năm cột delay-cause | Cả 5 cột `AIR_SYSTEM_DELAY`, `SECURITY_DELAY`, `AIRLINE_DELAY`, `LATE_AIRCRAFT_DELAY`, `WEATHER_DELAY` đều null ở 4,755,640 dòng; cả 5 đều non-null ở 1,063,439 dòng; mixed-null: 0 | Đây là structured missingness đồng bộ. Mẫu lặp null có khả năng thuận lợi cho encoding/nén columnar như Parquet, nhưng riêng EDA chưa chứng minh phần tiết kiệm bytes; cần đối chiếu kích thước/codec thực đo và không quy toàn bộ lợi thế format cho một đặc điểm này. |
| Trạng thái | Không hủy/không chuyển hướng: 5,714,008; chuyển hướng: 15,187; hủy: 89,884 | Cần quy định rõ các dòng hủy/chuyển hướng khi tính query delay; số dòng query output và mẫu số của `AVG` không nên diễn giải như nhau. |
| `ARRIVAL_DELAY` không null | min -87; median -5; p90 34; p95 66; p99 167; max 1,971 phút | Phân phối lệch phải và có outlier lớn. `AVG` nhạy với đuôi phân phối; giữ nguyên dữ liệu và báo cáo quantile để diễn giải, không tự ý cắt outlier. |
| `DEPARTURE_DELAY` | median -2; p90 35; p95 67; p99 168; max 1,988 phút | Tương tự arrival delay, nên kiểm tra đuôi phân phối khi đọc W1 và không chỉ dựa vào trung bình. |

## Bottleneck và giả thuyết cần kiểm chứng

1. **CSV parse và đọc dư cột.** Input là CSV rộng 31 cột, trong khi W3 chỉ cần `AIRLINE` và `ARRIVAL_DELAY`. So sánh cùng logical data/schema giữa CSV, JSON, Parquet, ORC; với W3 kiểm tra `ReadSchema` trên columnar formats và thời gian action thật. EDA đã dùng schema tường minh; benchmark cũng phải dùng cùng schema, không bật inference làm biến số đo (xem finding 6).
2. **Đọc nhiều dòng hơn cần thiết.** Dữ liệu chỉ có một năm, nên partition thêm `YEAR` có thể tạo một lớp thư mục nhưng không tăng khả năng chọn năm trong bộ này. Dùng `partitionBy(YEAR, MONTH)` và W2 lọc một tháng cụ thể; ghi nguyên văn `PartitionFilters`, `PushedFilters`, `ReadSchema` từ formatted plan. Partition pruning, predicate pushdown và column pruning là ba cơ chế khác nhau; không suy ra pushdown chỉ từ việc có `partitionBy`.
3. **Số file và chi phí downstream.** Một lượt chạy local được ghi trong `results/partition_results.csv` cho thấy `repartition(20)` tạo 20 file và mất 25.19 s ghi, còn `coalesce(2)` tạo 2 file và mất 8.74 s. Đây chỉ là một lần ghi; chưa đo cùng W1 sau khi ghi, phân bố kích thước file, cache state hay nhiều lượt. Cần chạy suite Layout Parquet riêng, không `partitionBy`, giữ schema/compression/data cố định và so sánh file count thực tế cùng W1.
4. **Skew là cảnh báo, chưa phải kết luận.** WN có hơn 20 lần số dòng của VX, nhưng đó là phân bố key, không phải số đo input partitions hay task durations. Chỉ gọi là runtime skew nếu Spark UI cho thấy imbalance/straggler; nếu không, ghi đây là giả thuyết chưa xác nhận.
5. **Null và outlier có thể làm sai diễn giải.** `AVG(ARRIVAL_DELAY)` bỏ qua null, trong khi `COUNT(*)` tính tất cả dòng. Báo cáo cả row count và số delay non-null; không tính null delay là 0 và không loại bỏ record hủy/chuyển hướng ngầm định.
6. **Schema consistency giữa EDA và benchmark đang cần xử lý.** EDA dùng `AIRLINE_SCHEMA` trong `airline_schema.py`, còn `format_benchmark.py` hiện vẫn bật `inferSchema=True` cho raw CSV và khi đọc lại CSV. Người đang sửa benchmark cần import cùng `AIRLINE_SCHEMA` cho các lần đọc CSV; Parquet/ORC giữ schema đã lưu trong file. EDA không sửa file benchmark để tránh xung đột với nhánh công việc của đồng đội.
7. **Chi phí phép kiểm tra duplicate.** `dropDuplicates()` trên toàn bộ 31 cột là phép shuffle rộng. Một lượt thử với `local[*]` OOM ở shuffle stage; cùng phép tính đã chạy xong với `local[2]`, trả về 0 duplicate. Đây là evidence cho cấu hình chạy EDA này, không chứng minh nguyên nhân OOM `repartition(4)` của nhóm. Để chẩn đoán lần OOM đó cần đối chiếu stage/task, spill, executor heap và số input/output partitions từ chính lần chạy lỗi.

## Đề xuất benchmark tiếp theo

Các workload dưới đây là đề xuất để kiểm chứng giả thuyết từ EDA; benchmark cuối cùng vẫn cần bao phủ các yêu cầu bắt buộc của Task 2 trong PDF.

- **Representation:** cùng input, logical schema tường minh và compression policy cố định theo format; CSV/JSON/Parquet/ORC × W1, W2, W3. W1 là AVG arrival/departure delay, distance và COUNT theo carrier; W2 là cùng aggregation với lọc năm/tháng; W3 là COUNT, MIN/MAX carrier và AVG arrival delay chỉ trên hai cột carrier/delay.
- **Layout:** Parquet cố định, không `partitionBy`; so sánh `repartition(20)`, `coalesce(2)` và một reasonable file count. Đo write, bytes, data file count/size distribution, input/output partitions và W1 downstream.
- **Pruning:** dùng output Parquet riêng có `partitionBy(YEAR, MONTH)` cho W2; không dùng thiết kế này để tạo small-file arms. Lưu formatted plan và đối chiếu `PartitionFilters`, `PushedFilters`, `ReadSchema` với query tương ứng.
- **Kỷ luật đo:** ít nhất 1 warm-up và 5 lượt đo; rotate thứ tự formats/layouts, không share cache; báo cáo median, min-max và IQR hoặc standard deviation. Ghi Spark version, master/cores, AQE, shuffle partitions, codec/options và cache state. Mỗi query phải có action thật để materialize kết quả.

## Giới hạn

Profile được chạy trên Windows local với Spark 3.5.9; không có cluster benchmark, Spark UI evidence hay đo disk IO riêng trong profile này. Percentile là ước lượng bằng `approxQuantile` với relative error 0.0001. Thời gian trong benchmark cũ là một snapshot đã ghi trong report, không thể dùng làm kết luận benchmark theo rubric. Cần chạy lại theo workload matrix trên cùng bộ dữ liệu và xác nhận plan/UI trước khi đưa ra kết luận về pruning, pushdown, skew hoặc lợi thế format.