# Lab 18 Reflection

**Anti-pattern: The Small File Problem (Vấn đề file nhỏ)**

Trong quá trình triển khai hệ thống Lakehouse, tôi dễ vướng phải anti-pattern **"Small File Problem"** nhất. 

**Lý do:**
1. **Chiến lược Partitioning quá chi tiết:** Khi mới bắt đầu, tôi thường có xu hướng partition dữ liệu theo các cột có độ đa dạng cao (high-cardinality) như `tenant_id` hoặc `user_id` kết hợp với `date/hour` để tối ưu tốc độ truy vấn cho từng khách hàng. Tuy nhiên, ở quy mô 1 tỷ request/ngày nhưng phân tán cho hàng ngàn tenant, việc này sẽ tạo ra hàng triệu file Parquet cực nhỏ (vài KB) trên S3.
2. **Hệ quả:** Việc có quá nhiều file nhỏ sẽ làm tăng gánh nặng cho Metadata layer (S3 LIST operations) và khiến Spark/DuckDB tốn nhiều thời gian mở/đóng file hơn là xử lý dữ liệu thực tế, dẫn đến hiệu năng giảm sút nghiêm trọng.

**Giải pháp khắc phục:**
Tôi đã áp dụng bài học từ Lab 18 bằng cách chỉ partition theo `date/hour` và sử dụng tính năng **Z-Order Clustering** của Delta Lake trên cột `tenant_id`. Cách tiếp cận này giúp dữ liệu vẫn được gom nhóm vật lý hiệu quả mà không làm tăng số lượng file, đồng thời duy trì hiệu suất ổn định cho các dashboard 5 phút.
