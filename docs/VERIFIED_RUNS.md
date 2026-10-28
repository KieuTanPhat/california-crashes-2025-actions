# Các lần chạy đã kiểm chứng

Nghiệm thu ngày 28/10/2026. Toàn bộ xử lý và screenshot thực hiện trong
GitHub-hosted runners, không cài hệ dữ liệu lớn trên máy cá nhân.

| Kiểm chứng | Lần chạy | Kết quả |
|---|---|---|
| Smoke 5000 dòng | [40646643216](https://github.com/KieuTanPhat/california-crashes-2025-actions/actions/runs/40646643216) | success, 9/9 task |
| Full lần 1 | [40648933570](https://github.com/KieuTanPhat/california-crashes-2025-actions/actions/runs/40648933570) | success, 402354 dòng, 14 screenshot |
| Full lần 2 | [40648938757](https://github.com/KieuTanPhat/california-crashes-2025-actions/actions/runs/40648938757) | success, 402354 dòng, 14 screenshot |
| So sánh và ảnh GitHub | [40649834799](https://github.com/KieuTanPhat/california-crashes-2025-actions/actions/runs/40649834799) | PASS, băm lại 9 bảng, 2 ảnh trang workflow thành công |

Hai lần full chạy trên cùng commit
`0e44ed726247e38105def710e840b5ed05278b3b`, cùng checksum nguồn và input,
cùng thống kê làm sạch, cùng chữ ký của monthly, hourly, weekday, weather,
collision_types, top_cities, rfm_clusters, cluster_summary và stream_by_city.
Workflow so sánh băm lại các file tải về, không chỉ so chuỗi hash manifest.

Batch giữ đủ 402354 dòng, rejected=0, duplicate_extras=0. Dữ liệu thiếu số
thương vong được giữ NULL: 19007 bản ghi thiếu/không hợp lệ NumberInjured,
19676 bản ghi thiếu/không hợp lệ NumberKilled; 382673 bản ghi có đủ cả hai.
Kafka gửi 10010 message (gồm 10 message lặp chủ động), Spark tiêu thụ đủ và
ghi đúng tập 10000 event ID duy nhất. RFM có 548 nhóm địa phương, 357 nhóm
đủ điều kiện, 191 nhóm bị loại khỏi ML; chọn k=3, silhouette≈0.67708.

Đây là lab trên một runner với HDFS replication=1, Spark local[2], Kafka
một broker và Airflow SequentialExecutor/SQLite. RFM M là thương vong
quan sát được, không phải tiền; Kafka là historical replay hữu hạn. HDFS
và model trong HDFS tồn tại trong job; artifacts bàn giao chứa bảng kết
quả, biểu đồ, thông số model, ảnh và log, không phải một cluster chạy lâu dài.

Các lần chạy thất bại/cancelled trước nghiệm thu là lịch sử phát triển:
đã sửa đường dẫn CLI cho scheduler và các điều kiện chờ screenshot native.
Không dùng ảnh chụp quá sớm hoặc trạng thái thử lỗi làm ảnh bàn giao cuối.

## Bổ sung ảnh terminal thật

[Full có terminal 40652832369](https://github.com/KieuTanPhat/california-crashes-2025-actions/actions/runs/40652832369)
đã success trên mã 3082a07: vẫn đủ 402354 dòng và 9/9 task, thêm 10 ảnh
terminal ttyd/xterm nối Bash PTY thật. Có 26 lệnh thực thi, tất cả exit=0;
tổng 24 screenshot trong artifact (14 giao diện/kết quả + 10 terminal).
Bộ unit test bổ sung đạt 17/17. Chín bảng CSV của lần chạy này được băm
lại và khớp toàn bộ với bộ kết quả đã kiểm chứng trước đó.

Các lệnh terminal là kiểm chứng sau DAG: mkdir/ls/report HDFS, upload lại
đúng CSV và SHA256, spark-submit đọc HDFS/Parquet/chạy SQL/nạp model,
Kafka describe/offset/consumer, Airflow CLI states. Mọi lệnh và thời điểm
nằm trong terminal-index.json; session.typescript là transcript PTY gốc.
Ảnh không được dựng lại từ log. Spark stderr giữ riêng trong artifact.
