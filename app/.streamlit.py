import streamlit as st
import psycopg2
import pandas as pd
import polars as pl
from contextlib import contextmanager
import seaborn as sns
import matplotlib.pyplot as plt
# =========================
# Kết nối database
# =========================

@st.cache_resource
def init_connection():
    conn = psycopg2.connect(**st.secrets["postgres"])
    conn.autocommit = True  # bật autocommit để tránh lỗi transaction idle
    return conn

@contextmanager
def get_cursor():
    conn = init_connection()
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()

def run_query(query, params=None):
    with get_cursor() as cur:
        cur.execute(query, params)
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        df_pandas = pd.DataFrame(rows, columns=colnames)
        df_polars = pl.from_pandas(df_pandas)
        return df_polars

# =========================
# Các hàm lấy dữ liệu
# =========================

def search_movie(query):
    query_sql = """
    SELECT * FROM movies.movies_infor mi
    WHERE title ILIKE %s
    JOIN movies.movies_rating rt ON mi.id=rt.id
    """
    return run_query(query_sql, params=('%' + query + '%',))

def get_recent_recommendations():
    query_sql = """
    SELECT mi.title, mi.overview, mi.release_date, mi.runtime, mi.genres
    FROM movies.recommendations r
    JOIN movies.movies_infor mi ON r.id = mi.id
    ORDER BY r.cosine_similarity DESC
    LIMIT 100
    """
    return run_query(query_sql)

def get_favorite_movies():
    query_sql = """
    SELECT title, overview, release_date, vote_average, genres
    FROM movies.favorite_track
    """
    return run_query(query_sql)

# def movie_trends_dashboard():
#     query_sql = """
#     SELECT genre, COUNT(*) as movie_count
#     FROM movies.movies_genres
#     GROUP BY genre
#     ORDER BY movie_count DESC
#     """
#     return run_query(query_sql)

# =========================
# Hàm hiển thị Movie Card
# =========================

def display_movies(df: pl.DataFrame):
    for i in range(0, len(df), 3):
        cols = st.columns(3)  # 3 phim mỗi hàng
        for j in range(3):
            if i + j < len(df):
                movie = df[i + j]
                with cols[j]:
                    title = movie['title'].item()
                    vote_average = movie['vote_average'].item() if movie['vote_average'] is not None else '0'
                    release_year = str(movie['release_date'].item().year) if movie['release_date'] is not None else 'N/A'
                    overview = movie['overview'].item() if movie['overview'] is not None else 'No overview available.'
                    genres = movie['genres'].item() if movie['genres'] is not None else []
                    
                    if isinstance(genres, str):
                        genres = genres.split(',')  # nếu bị lưu dạng chuỗi
                    genres_display = ", ".join(genres[:3]) if genres.len() > 0 else "No genres"

                    st.markdown(
                        f"""
                        <div style="border: 1px solid #ccc; border-radius: 12px; padding: 20px; margin-bottom: 20px; background-color: #f9f9f9;">
                            <h3 style="text-align: center; color: #333;">{title}</h3>
                            <div style="display: flex; justify-content: space-between; margin-top: 10px; font-size: 14px; color: gray;">
                                <span>⏱️ {vote_average} min</span>
                                <span>🎬 {genres_display}</span>
                                <span>📅 {release_year}</span>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                    with st.expander("🔎 Overview"):
                        st.write(overview)




# =========================
# Streamlit App
# =========================

st.set_page_config(page_title="🎬 TMDB Movies", layout="wide")

st.sidebar.title("🎬 TMDB Menu")
page = st.sidebar.radio(
    "Select a page:",
    ("🔎 Search Movie", "✨ Recent Recommendations", "❤️ Your Favorite Movies", "📈 Movie Trends Dashboard")
)

st.title("🎬 TMDB - Search for your Movies")

if page == "🔎 Search Movie":
    st.header("🔎 Search & Filter Movies")

    # --- Nhập từ khóa search ---
    movie_query = st.text_input("🔎 Search by movie title")

    # --- Lấy toàn bộ movies ---
    df = run_query("SELECT * FROM movies.movies_infor")

    # --- Bộ lọc năm ---
    year_filter = st.selectbox(
        "📅 Select release year",
        options=["None"] + sorted(df.select(pl.col('release_date').dt.year()).unique().to_series().drop_nulls().to_list()),
        index=0
    )

    # --- Bộ lọc điểm vote_average ---
    vote_ranges = [(1,2), (2,3), (3,4), (4,5), (5,6), (6,7), (7,8), (8,9), (9,10)]
    vote_filter = st.selectbox(
        "⭐ Select vote average range",
        options=["No filter"] + [f"{r[0]} → {r[1]}" for r in vote_ranges],
        index=0
    )

    # --- Xử lý lọc dữ liệu ---
    filtered_df = df

    if year_filter != "None":
        filtered_df = filtered_df.filter(
            pl.col('release_date').dt.year() == int(year_filter)
        )

    if vote_filter != "No filter":
        selected_range = vote_ranges[[f"{r[0]} → {r[1]}" for r in vote_ranges].index(vote_filter)]
        filtered_df = filtered_df.filter(
            (pl.col('vote_average') >= selected_range[0]) & (pl.col('vote_average') < selected_range[1])
        )

    if movie_query:
        filtered_df = filtered_df.filter(
            pl.col('title').str.contains(movie_query, case=False)
        )

    # --- Hiển thị ---
    if not filtered_df.is_empty():
        display_movies(filtered_df)
    else:
        st.warning("No results found!")
        st.subheader("🎯 Recommended Movies for you:")
        recommendations = get_recent_recommendations()
        display_movies(recommendations)



elif page == "✨ Recent Recommendations":
    st.header("✨ Recent Recommendations")
    recommendations = get_recent_recommendations()
    display_movies(recommendations)

elif page == "❤️ Your Favorite Movies":
    st.header("❤️ Your Favorite Movies")
    favorite_movies = get_favorite_movies()
    display_movies(favorite_movies)
    st.subheader("🎯 Recommended Movies for you:")
    recommendations = get_recent_recommendations()
    display_movies(recommendations)
elif page == "📈 Movie Trends Dashboard":
    st.header("📈 Movie Trends Dashboard")
    
    # Thêm trường nhập ngày cho người dùng
    start_date = st.date_input("Start Date", value=pd.to_datetime('2025-01-01'))
    end_date = st.date_input("End Date", value=pd.to_datetime('2025-12-31'))

    # Câu truy vấn SQL
    query_sql = f"""
    SELECT 
        mi.id,
        mi.title,
        mi.release_date,
        r.vote_count,
        r.vote_average
    FROM movies.movies_infor mi
    JOIN movies.movies_rating r ON mi.id = r.id
    WHERE mi.release_date BETWEEN '{start_date}' AND '{end_date}' AND r.vote_count>0 AND r.vote_average>0
    """
    # Chạy câu truy vấn
    movies = run_query(query_sql)

    # Nếu không có dữ liệu thì báo
    if movies.is_empty():
        st.warning(f"No movies found between {start_date} and {end_date}.")
    else:
        # Top 20 phim có nhiều "vote_count" nhất
        top_vote_count = movies.sort("vote_count", descending=True).head(20)
        st.markdown("### 🗳️ 20 Most Voted Movies")
        st.bar_chart(
            top_vote_count.select(["title", "vote_count"]).to_pandas().set_index("title")
        )

        # Biểu đồ phân phối điểm đánh giá theo năm
        st.subheader("🎯 Distribution of Movie Ratings by Year")
        
        import pandas as pd
        
        movies_pd = movies.to_pandas()

        # Tạo cột phân nhóm theo điểm (rating bin)
        bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        labels = ['0-1', '1-2', '2-3', '3-4', '4-5', '5-6', '6-7', '7-8', '8-9', '9-10']
        movies_pd['rating_bin'] = pd.cut(movies_pd['vote_average'], bins=bins, labels=labels, right=False)

        # Tạo cột năm phát hành
        movies_pd['release_year'] = pd.to_datetime(movies_pd['release_date']).dt.year

        # Lọc phim theo khoảng thời gian người dùng nhập
        movies_pd_filtered = movies_pd[(movies_pd['release_date'] >= pd.to_datetime(start_date)) & 
                                       (movies_pd['release_date'] <= pd.to_datetime(end_date))]

        # Đếm số phim theo từng năm và phân nhóm điểm
        rating_distribution_by_year = movies_pd_filtered.groupby(['release_year', 'rating_bin']).size().unstack(fill_value=0)

        # Vẽ biểu đồ cột cho phân phối điểm theo năm
        st.bar_chart(rating_distribution_by_year)

        # Số lượng phim phát hành theo tháng và năm
        st.subheader("📅 Monthly Movie Releases")
        
        # Truy vấn SQL cho số lượng phim phát hành theo tháng
        query_sql = f"""
        SELECT 
            TO_CHAR(mi.release_date, 'YYYY-MM') AS release_month,
            COUNT(*) AS movie_count
        FROM movies.movies_infor mi
        WHERE mi.release_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY release_month
        ORDER BY release_month
        """
        monthly_data = run_query(query_sql)

        # Vẽ biểu đồ đường cho số lượng phim phát hành theo tháng
        plt.figure(figsize=(12, 6))
        plt.plot(monthly_data["release_month"], monthly_data["movie_count"], marker='o')
        plt.title("Number of Movies Released Per Month")
        plt.xlabel("Month-Year")
        plt.ylabel("Movie Count")
        plt.xticks(rotation=45)
        st.pyplot(plt)
