import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, count, countDistinct, mean
from pyspark.ml.feature import CountVectorizerModel
from pyspark.ml.linalg import DenseVector
from pymongo import MongoClient
import pandas as pd

# Kết nối tới MongoDB và tải dữ liệu
client = MongoClient("mongodb://localhost:27017/")
db = client["AnimeDB"]
collection = db["anime_collection"]

# Đọc dữ liệu từ MongoDB và loại bỏ `_id`
mongo_data = collection.find()
mongo_df = pd.DataFrame(list(mongo_data))
if '_id' in mongo_df.columns:
    mongo_df.drop('_id', axis=1, inplace=True)

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Anime Recommendation App") \
    .getOrCreate()

# Chuyển đổi dữ liệu MongoDB sang DataFrame của Spark
data = spark.createDataFrame(mongo_df).dropna(subset=['link', 'title', 'genres', 'rate', 'image'])
data = data.withColumnRenamed('link', 'item_id')
data = data.withColumn("genres", split(col("genres"), ","))  # Tách genre thành danh sách
data = data.withColumn("rate", col("rate").cast("double"))  # Chuyển đổi cột rate sang kiểu số

# Đường dẫn tới mô hình đã lưu
model_path = "./models/genre_vectorizer"
vectorizer_model = CountVectorizerModel.load(model_path)
data = vectorizer_model.transform(data)

# Hàm gợi ý anime
def recommend_by_title(title, top_n=10):
    matching_items = data.filter(col("title").like(f"%{title}%"))
    item_data = matching_items.select("item_id", "genre_vector", "genres", "image").first()

    if not item_data:
        return f"No anime found with title containing: {title}", []

    item_id = item_data["item_id"]
    genre_vector = DenseVector(item_data["genre_vector"].toArray())
    broadcast_genre_vector = spark.sparkContext.broadcast(genre_vector)

    def calculate_similarity(row):
        target_vector = broadcast_genre_vector.value
        row_vector = DenseVector(row.genre_vector.toArray())
        dot_product = sum(target_vector[i] * row_vector[i] for i in range(len(target_vector)))
        norm_target = sum(x ** 2 for x in target_vector) ** 0.5
        norm_row = sum(x ** 2 for x in row_vector) ** 0.5
        similarity = dot_product / (norm_target * norm_row) if norm_target and norm_row else 0.0
        return row.title, row.item_id, row.genres, row.image, similarity

    similar_items_rdd = data.rdd.map(calculate_similarity)
    similar_items = similar_items_rdd.filter(lambda x: x[1] != item_id).takeOrdered(top_n, key=lambda x: -x[4])

    return f"Recommendations for: {title}", similar_items

# Tạo giao diện Streamlit
st.title("Anime Recommendation System")
st.write("Welcome! Use the tabs below to explore and get recommendations.")

# Tabs
tab1, tab2 = st.tabs(["Recommendations", "Statistics"])

# Tab 1: Recommendations
with tab1:
    st.write("Enter part of an anime title to get similar recommendations.")
    title_input = st.text_input("Anime Title:", "")

    if st.button("Recommend", key="recommend"):
        if title_input:
            message, recommendations = recommend_by_title(title_input)
            st.write(message)
            if recommendations:
                for title, item_id, genres, image, similarity in recommendations:
                    col1, col2 = st.columns([1, 3])
                    with col1:
                        st.image(image, width=120, caption=title)
                    with col2:
                        st.write(f"**Title:** {title}")
                        st.write(f"**Link:** {item_id}")
                        st.write(f"**Genres:** {', '.join(genres)}")
                        st.write(f"**Similarity:** {similarity:.4f}")
                        st.write("---")
        else:
            st.write("Please enter a valid anime title.")

# Tab 2: Statistics
with tab2:
    st.write("**Dataset Overview**")
    st.write(f"Total records: {data.count()}")
    st.write("**Column Statistics**")

    stats = data.select(
        count("*").alias("Total Records"),
        countDistinct("title").alias("Unique Titles"),
        countDistinct("item_id").alias("Unique Links"),
        mean("rate").alias("Average Rating")
    ).collect()[0]

    st.write(f"- Total records: {stats['Total Records']}")
    st.write(f"- Unique titles: {stats['Unique Titles']}")
    st.write(f"- Unique links: {stats['Unique Links']}")
    st.write(f"- Average rating: {stats['Average Rating']:.2f}")

    st.write("**Genres Breakdown**")

    # Tách và chuẩn hóa tất cả các thể loại thành các mục riêng biệt, đếm tần suất
    genre_counts = (
        data.select("genres").rdd
        .flatMap(lambda row: [genre.strip().lower() for genre in row.genres])
        .countByValue()
    )

    genre_counts_df = spark.createDataFrame(
        [(genre, count) for genre, count in genre_counts.items()],
        ["Genre", "Count"]
    ).orderBy(col("Count").desc())

    genres_list = ["All"] + genre_counts_df.select("Genre").rdd.flatMap(lambda x: x).collect()
    selected_genre = st.selectbox("Select a Genre to View Statistics:", genres_list)

    if selected_genre == "All":
        filtered_counts = genre_counts_df
    else:
        filtered_counts = genre_counts_df.filter(col("Genre") == selected_genre)

    # Hiển thị biểu đồ
    pd_filtered_counts = filtered_counts.toPandas()
    st.bar_chart(pd_filtered_counts.set_index("Genre"))
