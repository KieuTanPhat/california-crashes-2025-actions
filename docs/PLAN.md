# California crashes 2025 — kế hoạch triển khai được duyệt

Mục tiêu: chạy đủ HDFS, upload, PySpark, cleaning/Parquet, Spark SQL,
RFM điều chỉnh và K-Means, lưu HDFS, Kafka Structured Streaming, Airflow DAG
trên GitHub-hosted runner; thu screenshot thật và artifacts.

- [x] Hợp đồng dữ liệu và tests cho thiếu dữ liệu, ngày, tọa độ, đối soát.
- [x] Môi trường runner, nguồn CSV cố định và checksum.
- [x] Chạy batch đầy đủ qua HDFS và Spark.
- [x] RFM điều chỉnh theo địa phương và K-Means.
- [x] Kafka replay, Structured Streaming, đối soát event.
- [x] Airflow scheduler điều phối thật, screenshot UI và báo cáo.
- [x] Chạy smoke, full và full lần hai; kiểm chứng và bàn giao.

Đã nghiệm thu ngày 28/10/2026: smoke 40646643216; full 40648933570 và
40648938757; kiểm chứng tái lập 40649834799 PASS. Mã thực thi được kiểm
chứng: 0e44ed726247e38105def710e840b5ed05278b3b. Các commit bàn giao sau
mốc này chỉ cập nhật tài liệu. Xem docs/VERIFIED_RUNS.md.

Thiết kế: một runner, HDFS pseudo-distributed, Spark local[2], Kafka một
broker. Airflow dùng SequentialExecutor/SQLite phục vụ đồ án. Dịch vụ chỉ
nghe loopback hoặc mạng Docker cục bộ, không mở máy chủ công cộng.

RFM: đơn vị là County Code + City Id, R là số ngày đến 2025-12-31, F là số
vụ, M là tổng người bị thương + tử vong trên các vụ có đủ hai chỉ số.
Ghi kèm coverage; thiếu không đổi thành 0. M không phải tiền tệ và không
đại diện tổng thiệt hại đầy đủ. K-Means dùng các địa phương đủ coverage 95%,
ít nhất 20 vụ, biến đổi log1p(F), log1p(M), chuẩn hóa, seed cố định và chọn
k trong 2..5 theo silhouette. Không diễn giải số vụ thành rủi ro trên lưu
lượng xe khi chưa có mẫu số.

Đầu vào: CSV người dùng, SHA256
5ccb085b505498a8d7f28eaca619456614458f0d7b95631fbd3f4c2855083a32,
402354 dòng, 74 cột, 187408607 bytes. Giữ nguyên bản trong GitHub Release.

Đối soát: input = retained + rejected + duplicate_extras; mỗi query tổng
hợp phải khớp retained. Kafka phát lại tối đa 10000 sự kiện full, 1000 smoke,
ghi rõ là streaming mẫu từ lịch sử. Batch full luôn dùng toàn bộ CSV.

Minh chứng: HDFS overview và file browser, Spark History, Airflow Grid/Graph,
các trang HTML trình bày kết quả thực tế, PNG biểu đồ, log và run manifest.
Screenshot lỗi không được thay thế bằng hình mô phỏng. Không xác nhận
hoàn thành nếu DAG/tasks, đối soát hoặc thu minh chứng chưa đạt.
