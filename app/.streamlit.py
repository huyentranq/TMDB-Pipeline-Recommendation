import streamlit as st
import psycopg2
import pandas as pd
import polars as pl
from contextlib import contextmanager

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
    SELECT * FROM movies.infor_rating 
    WHERE title ILIKE %s
    """
    return run_query(query_sql, params=('%' + query + '%',))

def get_recent_recommendations():
    query_sql = """
        SELECT *
        FROM movies.rcm_Infor

    """
    return run_query(query_sql)

def get_favorite_movies():
    query_sql = """
    SELECT title, overview, release_date, vote_average, genres
    FROM movies.favorite_track
    """
    return run_query(query_sql)

def movie_trends_dashboard():
    query_sql = """
    SELECT genre, COUNT(*) as movie_count
    FROM movies.movies_genres
    GROUP BY genre
    ORDER BY movie_count DESC
    """
    return run_query(query_sql)

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
                    runtime = movie['runtime'].item() if movie['runtime'] is not None else 'N/A'
                    release_year = str(movie['release_date'].item().year) if movie['release_date'] is not None else 'N/A'
                    overview = movie['overview'].item() if movie['overview'] is not None else 'No overview available.'
                    genres = movie['genres'].item() if movie['genres'] is not None else []
                    
                    if isinstance(genres, str):
                        genres = genres.split(',')  # nếu bị lưu dạng chuỗi
                    genres_display = ", ".join(genres[:3]) if genres else "No genres"

                    st.markdown(
                        f"""
                        <div style="border: 1px solid #ccc; border-radius: 12px; padding: 20px; margin-bottom: 20px; background-color: #f9f9f9;">
                            <h3 style="text-align: center; color: #333;">{title}</h3>
                            <div style="display: flex; justify-content: space-between; margin-top: 10px; font-size: 14px; color: gray;">
                                <span>⏱️ {runtime} min</span>
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
    df = run_query("SELECT * FROM movies.movies_infor")
    # --- Các bộ lọc ---
    all_genres = df.select('genres').unique().explode('genres').to_series().to_list()
    all_genres = list(set([g.strip() for g in all_genres if g]))
    
    selected_genres = st.multiselect("🎬 Select genres", options=all_genres)
    year_range = st.slider("📅 Select release year", min_value=1950, max_value=2025, value=(2000, 2025))
    vote_ranges = st.select_slider(
        "⭐ Select vote average range",
        options=[(1,2), (2,3), (3,4), (4,5), (5,6), (6,7), (7,8), (8,9), (9,10)],
        format_func=lambda x: f"{x[0]} → {x[1]}"
    )
    
    # --- Xử lý dữ liệu ---
    # Lấy toàn bộ movies trước
    df = run_query("SELECT * FROM movies.movies_infor")

    filtered_df = df.filter(
        (pl.col('release_date').dt.year() >= year_range[0]) &
        (pl.col('release_date').dt.year() <= year_range[1]) &
        (pl.col('vote_average') >= vote_ranges[0]) &
        (pl.col('vote_average') < vote_ranges[1])
    )

    if selected_genres:
        filtered_df = filtered_df.filter(
            pl.col('genres').arr.contains(selected_genres)
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
    movie_trends = movie_trends_dashboard()
    st.dataframe(movie_trends.to_pandas())
