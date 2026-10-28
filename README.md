# Phân tích tai nạn giao thông California năm 2025

Dự án thực hành đủ 9 bước HDFS, upload, Spark, cleaning/Parquet, Spark SQL,
RFM điều chỉnh + K-Means, lưu HDFS, Kafka ingest và Airflow DAG. Toàn bộ
xử lý dữ liệu và chụp giao diện diễn ra trên GitHub-hosted Ubuntu runner.
Máy cá nhân chỉ cần trình duyệt. Không cần cài Docker, Hadoop, Spark hay
Airflow trên máy cá nhân.

**Đã kiểm chứng đầy đủ:** hai lần full chạy thành công, mỗi lần đủ 9/9 task
và 14 ảnh; so sánh chín bảng kết quả PASS sau khi băm lại file tải về.
Xem [các lần chạy và bằng chứng](docs/VERIFIED_RUNS.md).

## Chạy bằng trình duyệt

1. Mở **Actions → California crash pipeline → Run workflow**.
2. Chọn `smoke` để kiểm tra nhanh (5000 dòng) hoặc `full` để xử lý toàn bộ
   402354 dòng của CSV cố định.
3. Chờ workflow kết thúc; kiểm tra trạng thái bước **Execute all nine stages
   through Airflow** và **Capture authentic UIs and render report**.
4. Tải artifact `california-2025-...` ở cuối trang lần chạy và giải nén.
5. Mở `evidence/report.html`. Thư mục `evidence` chứa PNG từng bước và ảnh
   giao diện HDFS, Spark History, Airflow Grid/Graph. `results` chứa bảng CSV,
   JSON và biểu đồ. `data-quality` chứa đối soát. `manifest` chứa commit,
   checksum, phiên bản, trạng thái DAG/tasks và chữ ký kết quả.

Artifact giữ 14 ngày; tải và lưu bản cần dùng trong báo cáo trước khi hết hạn.
Đây là môi trường tạm thời: chạy lại sẽ dựng lại dịch vụ và dữ liệu HDFS.

## Dữ liệu

CSV người dùng cung cấp, 187408607 bytes, 402354 dòng, 74 cột, đủ 12 tháng
2025. Bản gốc được giữ trong Release `data-v1`; runner kiểm tra SHA256 trước
khi xử lý. Danh mục nguồn liên quan: [CHP CCRS](https://data.ca.gov/@california-highway-patrol/ccrs).
Chưa xác nhận bản tải này khớp revision cụ thể nào của danh mục; không gọi
đây là thống kê toàn bang đã chốt. Cờ `Is Preliminary=False` không chứng minh
toàn bộ dữ liệu là đầy đủ/cuối cùng.

## Phương pháp

Đọc [phương pháp và giới hạn](docs/METHODOLOGY.md) trước khi sử dụng kết quả.
M của RFM là **Magnitude thương vong quan sát được**, không phải Monetary.
Không có số tiền trong dữ liệu để tính RFM khách hàng nguyên bản. Đồ án giữ
bước RFM bằng một biến thể được định nghĩa minh bạch theo địa phương.

Kafka phát lại tối đa 1000 sự kiện ở smoke, 10000 ở full, thêm 10 bản gửi
lặp để kiểm chứng Spark streaming khử trùng. Batch full luôn xử lý toàn bộ
CSV. Không tuyên bố đây là dữ liệu giao thông trực tiếp.

## Kiến trúc

GitHub Actions cài Java 11, Hadoop 3.3.6, PySpark 3.5.5 và Airflow 2.10.5
trong môi trường tách riêng; Docker Compose chạy Kafka 3.9.0. Các phiên bản
cố định phục vụ lab tương thích, không phải khuyến nghị production.
Airflow dùng scheduler thật và SequentialExecutor/SQLite. Spark `local[2]`,
HDFS một NameNode/một DataNode, replication=1; không đo khả năng mở rộng
đa máy. Chỉ expose giao diện trên loopback của runner, không public UI.

## Cấu trúc mã nguồn

- `.github/workflows/lab.yml`: cài đặt, chạy DAG, chụp ảnh, upload artifact.
- `dags/california_2025.py`: quan hệ phụ thuộc 9 task thật.
- `pipeline/stages.py`: xử lý từng bước và kiểm chứng đầu ra.
- `pipeline/domain.py`: hợp đồng dữ liệu, có unit tests.
- `scripts/`: tải/kiểm checksum, dựng dịch vụ, theo dõi DAG, thu minh chứng.
- `config/data.json`: snapshot, tham số RFM, seed cố định.
- `docs/REPORT_OUTLINE.md`: bố cục báo cáo và ánh xạ minh chứng.

Workflow được dùng để phát triển và kiểm thử phần mềm pipeline trong repo,
không vận hành dịch vụ thường trực hoặc giữ runner chạy chờ.
