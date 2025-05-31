# DỰ ÁN MOVIES ETL & DASHBOARD

## 1. Tổng Quan Giới Thiệu Dự Án

Dự án này xây dựng một pipeline ETL hoàn chỉnh nhằm xử lý dữ liệu phim từ nhiều nguồn (MySQL, TMDB API, PostgreSQL, MinIO, Spark) và trực quan hóa dữ liệu thông qua Streamlit. Mục tiêu là:
- Trích xuất dữ liệu từ nguồn và tải chúng vào hệ thống.
- Xử lý, biến đổi dữ liệu qua các quy trình tương ứng.
- Đưa ra báo cáo và visualization trên dashboard (Streamlit).
- Tích hợp và tự động hoá các công việc thông qua Dagster.

## 2. Preview Output Trên Streamlit

Dự án tích hợp giao diện Streamlit để hiển thị kết quả:
- **Video Demo:** [Link video demo](#)  
*(Chèn link video demo cập nhật tại đây)*  
- Giao diện chính cho phép tìm kiếm phim, xem khuyến nghị và thống kê các xu hướng phát hành phim.

## 3. Hình Ảnh Pipeline & Mô Tả Toàn Bộ Pipeline ETL

Pipeline ETL của dự án bao gồm:
- **Bronze Layer:** Load dữ liệu gốc từ các nguồn như MySQL hay TMDB API.
- **Silver Layer:** Làm sạch, chuyển đổi dữ liệu (ví dụ: tách cột, loại bỏ giá trị null, chuyển đổi kiểu dữ liệu).
- **Gold Layer:** Trích xuất những dữ liệu quan trọng để phục vụ cho các mục tiêu thống kê, machine learning.
- **Warehouse (Analytics):** Tích hợp dữ liệu vào PostgreSQL cho các báo cáo và trực quan hóa.
- **Automation qua Dagster:** Quản lý và chạy các asset từ các pipeline ETL.

![Pipeline Diagram](link-to-pipeline-diagram.png)  
*(Chèn đường dẫn tới hình ảnh pipeline tại đây)*

---

## Các Bước Cài Đặt & Triển Khai

### Yêu Cầu Ban Đầu
- Docker, Docker Compose
- DBvear hoặc một công cụ quản lý SQL (để quản lý database cho PostgreSQL và MySQL)
- Python 3

### Các Bước Triển Khai

1. **Clone Repository & Cài Đặt Dự Án:**
    ```sh
    git clone <repository-url>
    cd <repository-folder>
    ```
2. **Tải Dataset:**
   - Tải dataset từ Kaggle (giả định link Kaggle) và đặt chúng vào thư mục `dataset`.

3. **Chuẩn Bị File ENV:**
   - Copy mẫu file env:
     ```sh
     cp env.template .env
     ```
   - Điền các thông tin cần thiết vào file [.env](http://_vscodecontentref_/0):
     - **TMDB:** Truy cập [TMDB](https://www.themoviedb.org/) để đăng ký và lấy API, thêm một số phim yêu thích.
     - **Database:** Cấu hình thông tin cho MySQL và PostgreSQL.
     - **MinIO & Spark:** Điền thông tin cấu hình phù hợp.
     
   *(Ví dụ file env mẫu có thể được cung cấp bên dưới)*

4. **Thiết Lập Môi Trường Ảo & Kiểm Tra Python:**
    ```sh
    python3 -V        # Kiểm tra phiên bản Python
    python3 -m venv venv  # Tạo môi trường ảo
    source venv/bin/activate
    ```

5. **Biên Dịch & Xây Dựng Các Container Theo Thứ Tự:**
   - Xây dựng các thành phần riêng lẻ (đọc chi tiết trong Makefile):
     ```sh
     make build-dagster
     make build-spark
     make build-pipeline
     make build-streamlit

     make build
     ```
   - Khởi chạy các container:
     ```sh
     make up
     ```
   - Sau khi chạy, vào Docker Desktop để kiểm tra tiến trình container.  
     *(Chèn hình ảnh minh họa bước kiểm tra container tại đây)*

---

## Load Dataset Vào MySQL & PostgreSQL

### Load Dataset vào MySQL

1. **Vào Container MySQL với Quyền Root:**
    ```sh
    make to_mysql_root
    ```
2. **Thực Hiện Các Lệnh Cấu Hình:**
    ```sql
    SET GLOBAL local_infile=TRUE;
    SHOW VARIABLES LIKE "local_infile";
    exit
    ```
3. **Import Dữ Liệu:**
    ```sh
    make to_mysql
    source /tmp/mysql_schemas.sql;
    show tables;
    source /tmp/load_dataset/mysql_load.sql;
    exit
    ```
4. **Kiểm Tra Dữ Liệu Trên DBveaver:**  
   Kết nối và kiểm tra dữ liệu đã được upload lên MySQL.

### Tạo Database cho PostgreSQL

1. **Vào Container PostgreSQL:**
    ```sh
    make to_psql
    ```
2. **Thực Hiện Lệnh Tạo Database:**
    ```sql
    source /tmp/load_dataset/psql_datasource.sql;
    ```
3. **Kiểm Tra Dữ Liệu:**  
   Sử dụng công cụ quản lý SQL để xác nhận rằng dữ liệu đã được tải lên.

---

## Tiếp Theo: Tự Động Hóa Job & Chạy Các Asset Qua Dagster

- Sau khi hoàn thành việc cài đặt và import dữ liệu, hãy vào giao diện của Dagster theo địa chỉ đã cấu hình (vd: `http://localhost:3001`) để kiểm tra và chạy các asset ETL.
- Từ giao diện Dagster, bạn có thể theo dõi pipeline ETL, chạy thử từng asset, và xem log để đảm bảo quá trình ETL hoạt động bình thường.

---

## Lời Kết

Dự án này hướng đến việc xây dựng một hệ thống ETL toàn diện từ việc thu thập đến trực quan hóa dữ liệu. Hãy theo dõi các hướng dẫn cụ thể trên từng bước và đảm bảo rằng mọi cấu hình đều được thiết lập chính xác theo yêu cầu của file [.env](http://_vscodecontentref_/1) và Makefile.

Happy Coding!