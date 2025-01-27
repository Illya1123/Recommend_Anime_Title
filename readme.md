# Anime Recommendation System using Spark and Kafka

## Giới thiệu đề tài

Với sự phát triển nhanh chóng của công nghệ và các nền tảng phát trực tuyến, ngành công nghiệp anime đã mở rộng đáng kể, dẫn đến những thách thức trong việc cung cấp nội dung cá nhân hóa cho người dùng. Sự đa dạng trong sở thích của khán giả và sự gia tăng các tựa anime đòi hỏi các hệ thống gợi ý hiệu quả. Các hệ thống gợi ý dựa trên nội dung sử dụng các thuộc tính như thể loại, đánh giá và sở thích của người dùng để tạo ra các gợi ý phù hợp. Tuy nhiên, khả năng mở rộng và hiệu quả của các phương pháp truyền thống bị giới hạn bởi khối lượng dữ liệu ngày càng tăng.

Bộ dữ liệu anime được crawl từ trang chủ **animevietsub.page** vào ngày 18/1/2025. Ngoài ra, bạn có thể lấy toàn bộ mã nguồn và dữ liệu từ GitHub tại liên kết sau: [https://github.com/Illya1123/Recommend_Anime_Title.git](https://github.com/Illya1123/Recommend_Anime_Title.git).

## Thành viên

- **Lê Quốc Anh** - MSSV: 21520565
- **Nguyễn Đoàn Nhật Khánh** - MSSV: 21522207

## Công nghệ sử dụng

- **Java 11**
- **Python 3.10.6**
- **Apache Spark 3.5.4**

## Cài đặt thư viện và môi trường cần thiết

- Tạo virtual environment và cài đặt các thư viện cần thiết:
  ```
  python -m venv venv
  source venv/bin/activate  # Đối với hệ điều hành Unix/Mac
  .\venv\Scripts\activate  # Đối với hệ điều hành Windows
  pip install -r requirements.txt
  ```
- Cài sẵn **MongoDB**.

## Hướng dẫn chạy dự án

1. Clone repository từ GitHub:
   ```
   git clone https://github.com/Illya1123/Recommend_Anime_Title.git
   cd Recommend_Anime_Title
   ```
2. Khởi tạo topic trên Kafka với tên `anime`.
3. Chạy song song các file:
   - `streaming.ipynb`
   - `Content_producer.ipynb`
4. Chạy demo trên web app:
   - Chạy các file:
     - `anime_data_cleaning.ipynb`
     - `recommendation_app.py` bằng lệnh:
       ```
       streamlit run recommendation_app.py
       ```
