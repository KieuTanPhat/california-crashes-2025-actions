# Phương pháp và giới hạn

## Làm sạch

Chuẩn hóa tên cột, bỏ whitespace đầu/cuối giá trị. Ngày giờ nguồn được hiểu
là giờ địa phương America/Los_Angeles; không tự diễn giải là UTC. Chỉ giữ
năm 2025, collision ID có giá trị, bản ghi không đánh dấu deleted và CSV
không hỏng. Với ID lặp chọn Report Version cao nhất. Dữ liệu gốc được giữ.

Số thương vong rỗng, âm, không phải số nguyên là NULL. Không đổi NULL thành
0. Kết quả SUM là tổng phần đã quan sát; bảng monthly có số bản ghi biết
từng chỉ số. Latitude/Longitude được ép kiểu số và gắn cờ thuộc bounding
envelope California; không coi bounding box là kiểm tra polygon ranh giới.
Các vụ thiếu tọa độ vẫn dùng cho phân tích thời gian/địa phương.

Đối soát bắt buộc: input = retained + rejected + duplicate_extras.
Số dòng Spark đọc phải bằng CSV parser. Tổng từng bảng monthly, hourly,
weekday, weather và collision_types phải bằng retained.

## RFM điều chỉnh

Đơn vị: County Code + City Id. Thành phố unincorporated được giữ với mã nguồn.
R: số ngày từ vụ cuối cùng đến 2025-12-31. F: số vụ trong năm. M: tổng người
bị thương cộng tử vong trên các vụ có đủ hai giá trị; gọi là observed
casualty magnitude. M không phải số tiền và không phải tổng thiệt hại đầy đủ.
Không áp trọng số tử vong tùy ý, không suy diễn chi phí từ thương vong.

Ghi số vụ đủ dữ liệu và coverage theo nhóm. Chỉ phân cụm nhóm có >=20 vụ và
coverage >=95%; các nhóm bị loại vẫn xuất bảng với cluster NULL. Không có
mã địa phương thì không gán giả một địa phương. RFM áp dụng cho cùng kỳ
2025, không diễn giải R thành dự báo rủi ro hiện tại.

Đặc trưng: R, log1p(F), log1p(M), StandardScaler withMean/withStd. Spark ML
K-Means seed=42, k từ 2 đến 5 (giới hạn theo số điểm khác nhau), chọn
silhouette lớn nhất, nếu bằng chọn k nhỏ hơn. Tâm cụm là trên không gian
chuẩn hóa; tên cluster là ID tùy ý, không phải cấp độ nguy hiểm tăng dần.
Xuất scaler và model vào HDFS, bảng đặc trưng và thông số lựa chọn k.
Đầu vào ML được sắp xếp theo mã địa phương và gom một partition (chỉ vài
trăm địa phương), để cùng seed không bị ảnh hưởng bởi thứ tự file Parquet
hoặc thứ tự hash aggregation giữa các lần chạy.

## Streaming

Kafka nhận một phần CSV theo thứ tự file, không đảm bảo thứ tự event time.
Đây là bounded historical replay. Key = run_key:collision_id. Gửi thêm 10
event lặp; producer acks=all, kiểm tra end offset của topic một partition.
Spark Structured Streaming đọc Kafka từ earliest, parse JSON, loại event
ID trống, khử trùng theo event_id với state và checkpoint HDFS, ghi Parquet
Append, trigger availableNow. Đối chiếu toàn bộ tập event ID, số bản ghi
output, numInputRows và offset Kafka. State không watermark phù hợp mẫu hữu
hạn, không phải thiết kế vận hành vô hạn.

## Tái lập và minh chứng

Lưu checksum nguồn, checksum input smoke/full, commit SHA, GitHub run URL,
resolved pip freeze, thông số, kết quả quality, Airflow DagRun và 9 task.
Chữ ký SHA256 bảng tổng hợp/RFM được so giữa hai lần full; cluster labels
được so nguyên trạng vì cùng seed, nguồn, phiên bản và quy mô tính toán.
Nếu nhãn hoán vị thì cần kiểm tra tập thành viên và tâm cụm trước khi kết
luận khác biệt; không coi kết quả khác là tái lập đạt.

Screenshots native ghi giao diện thật trong runner. Các ảnh stage-results
là trang HTML trình bày kết quả thật, không giả giao diện terminal. Luôn
kèm log và JSON vì screenshot riêng lẻ không chứng minh tính đúng đắn.

## Những điều không thể kết luận

Không suy luận quan hệ nhân quả, xác suất tai nạn theo lưu lượng xe, chi phí
kinh tế, thống kê toàn bang đã chốt hoặc năng lực cluster nhiều máy từ lab.
Không coi K-Means là mô hình dự báo. Dữ liệu crash-level chưa có chi tiết
người/xe để phân tích đầy đủ nhân khẩu học hoặc từng loại người tham gia.
