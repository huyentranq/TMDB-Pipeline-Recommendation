from dagster import asset, Output, StaticPartitionsDefinition
from datetime import datetime
import polars as pl
import os
from elt_pipeline.utils.TMDBLoader import TMDBLoader
COMPUTE_KIND = "SQL"
LAYER = "bronze"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1920, datetime.today().year)] + ["unknown"]
)   

def ensure_polars(df):
    """Chuyển đổi DataFrame từ pandas sang polars nếu cần."""
    return df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)
# Định nghĩa các biến môi trường

@asset(
    description="Load table 'movies' from MySQL database as polars DataFrame, and save to minIO",
    partitions_def=YEARLY,
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_movies(context) -> Output[pl.DataFrame]:
    year = context.partition_key

    if year == "unknown":
        query = "SELECT * FROM movies WHERE release_date IS NULL"
    else:
        # Đảm bảo format YYYY-mm-dd
        query = f"""
            SELECT *
            FROM movies
            WHERE release_date IS NOT NULL AND release_date LIKE '{year}%'
        """

    df = context.resources.mysql_io_manager.extract_data(query)

    df = ensure_polars(df)
    context.log.info(f"[{year}] Loaded {df.shape[0]} rows.")

    return Output(
        value=df,
        metadata={
            "table": "movies",
            "year": year,
            "row_count": df.shape[0],
            "column_count": df.shape[1],
            "columns": df.columns,
        }
    )


@asset(
    description="Load my genre_track dataframe relate to TMDB dataset, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "genre_track"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_genre_track(context) ->Output[pl.DataFrame]:
    query = "SELECT * FROM genre_track"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    # Nếu MySQL trả về pandas.DataFrame thì chuyển sang polars
    df_data = ensure_polars(df_data)
    return Output(
        value=df_data,
        metadata={
            "table": "genre_track",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns
        },
    )




@asset(
    description="Load my favorite movies from TMDB API, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys=set(),
    key_prefix=["bronze", "track_my_movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def bronze_favorite_movies(context) -> Output[pl.DataFrame]:
    # Lấy thông tin từ biến môi trường hoặc file cấu hình
    tmdb_params = {
        "access_token": os.getenv("TMDB_ACCESS_TOKEN"),
        "account_id": os.getenv("TMDB_ACCOUNT_ID"),
        "language": "en-US",
        "sort_by": "created_at.asc",
        "page": 1,
        "backup_path": os.getenv("BACKUP_PATH")
    }

    loader = TMDBLoader(tmdb_params)
    df_data = loader.extract_data()
    df_data = ensure_polars(df_data)
    context.log.info(f"Favorite movies extracted: {df_data.shape[0]} rows")

    return Output(
        value=df_data,
        metadata={
            "source": "TMDB API",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": list(df_data.columns),
        },
    )