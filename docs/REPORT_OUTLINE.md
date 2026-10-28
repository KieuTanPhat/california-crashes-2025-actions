# Đề cương báo cáo và minh chứng

1. Mục tiêu, câu hỏi nghiên cứu, phạm vi California 2025, hạn chế dữ liệu.
2. Nguồn CSV và snapshot: trích input.json, checksum, quy mô và 12 tháng.
3. Môi trường GitHub Actions: cấu hình runner, phiên bản, kiến trúc một máy.
4. Pipeline 9 bước: thuật toán, đường dẫn HDFS, mã nguồn, đầu ra từng bước.
5. Chất lượng dữ liệu: số dòng giữ/loại/trùng, dữ liệu thiếu, đối soát.
6. Spark SQL: biểu đồ tháng/giờ, loại va chạm, thời tiết, địa phương.
7. RFM điều chỉnh: công thức, coverage, chuẩn hóa, chọn k, diễn giải cụm.
8. Kafka: mô phỏng phát lại, consumer, checkpoint, khử trùng và đối soát ID.
9. Airflow: DAG, phụ thuộc, trạng thái 9 task, thời gian và retry.
10. Tái lập: hai lần full, cùng nguồn/commit/cấu hình, so chữ ký bảng.
11. Kết luận trong phạm vi quan sát; hướng phát triển dữ liệu/môi trường.

| Bước | Ảnh minh chứng | Bằng chứng máy đọc được |
|---|---|---|
| 1 | hdfs-overview.png, 01-stage-results.png | hdfs-report.txt, step_01.json |
| 2 | hdfs-files.png, 02-stage-results.png | checksum HDFS trong step_02.json |
| 3 | 03-stage-results.png, spark-history.png | raw-schema.json, step_03.json |
| 4 | 04-stage-results.png | cleaning.json, HDFS Parquet |
| 5 | 05-stage-results.png, monthly.png, hourly.png | queries.sql và các bảng CSV |
| 6 | 06-stage-results.png, clusters.png | rfm_clusters.csv, clustering-method.json |
| 7 | 07-stage-results.png | hdfs-output-listing.txt |
| 8 | 08-stage-results.png | stream-progress.json, stream-reconciliation.json |
| 9 | airflow-grid.png, airflow-graph.png, 09-stage-results.png | airflow-run.json, airflow-tasks.json |

Ghi chú mỗi ảnh: tên repo, run ID, chế độ smoke/full, thời điểm chạy và ý
nghĩa quan sát được. Dùng ảnh full cho báo cáo chính. Không sửa screenshot
để thay trạng thái task, số dòng, số vụ hay kết quả cụm.

Ảnh lệnh bổ sung: `evidence/terminal/00-environment.png` và các ảnh 01..09
trong cùng thư mục. Đây là terminal Linux/Bash thật, không phải Windows CMD.
Lệnh `hdfs dfs -put -f` upload lại đúng CSV sau DAG để minh chứng thao tác;
các lệnh spark-submit khác kiểm chứng dữ liệu/model đang có trên HDFS.
Airflow CLI hiển thị trạng thái lần DAG đã hoàn thành. Nêu đúng vai trò
kiểm chứng sau thực thi, không mô tả các ảnh này là chụp lúc tạo cluster
hoặc chạy training ban đầu. Toàn bộ trạng thái và lệnh được truy vết qua
GitHub run ID, terminal-index.json và transcript PTY.
