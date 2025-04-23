import os
from dagster import asset, AssetIn, AssetOut, Output, StaticPartitionsDefinition
import polars as pl
from pyspark.sql.dataframe import DataFrame
from ..resources.spark_io_manager import get_spark_session
from pyspark.sql.functions import monotonically_increasing_id, lit, concat, explode, collect_list, col
from datetime import datetime, timedelta
COMPUTE_KIND = "pyspark"
LAYER = "silver"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1920, datetime.today().year)] 
)   

def ensure_polars(df):
    """Chuyển đổi DataFrame từ pandas sang polars nếu cần."""
    return df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)


## asset definitions

@asset(
    description="Cleaning movies dataset, transform into spark dataframe",
    partitions_def=YEARLY,
    io_manager_key="spark_io_manager",
    ins={
        "bronze_movies": AssetIn(
            key_prefix=["bronze", "movies"],
        ),
    },
    key_prefix=["silver", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def silver_movies_cleaned(context, bronze_movies: pl.DataFrame) -> Output[DataFrame]:
     # 1. Khởi tạo cấu hình Spark session
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    # Kiểm tra đầy đủ config
    if not all(config.values()):
        raise ValueError("Missing MINIO environment variables.")

    context.log.info("Creating Spark session for movies_cleaned ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
            #convert to dataframe
            pandas_df = bronze_movies.to_pandas()
            context.log.debug(
                f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
            )

            spark_df = spark.createDataFrame(pandas_df)
            spark_df.cache()
            context.log.info("Got Spark DataFrame")

            # 2. Bỏ các cột không cần thiết
            columns_to_drop = ["backdrop_path", "spoken_languages", "poster_path"]
            spark_df = spark_df.drop(*columns_to_drop)

            # 3. Bỏ các hàng có id = null
            spark_df = spark_df.filter(spark_df["id"].isNotNull())

            # 4. Tính giá trị trung bình của popularity
            avg_popularity = spark_df.selectExpr("avg(popularity)").first()[0]

            # 5. Fill NA
            df_filled = spark_df.fillna({
                "budget": 0,
                "revenue": 0,
                "adult": "FALSE",
                "original_language": "en",
                "original_title": "unknown",
                "popularity": avg_popularity,
                "production_companies": "unknown",
                "production_countries": "unknown",
                "keywords": "unknown",
                "genres": "unknown"
            })

            context.log.info(f"Finished cleaning movies dataset for partition: {context.partition_key}")

            return Output(
                df_filled,
                metadata={
                    "table": "movies_cleaned",
                    "row_count": df_filled.count(),
                    "column_count": len(df_filled.columns),
                    "columns": df_filled.columns,
                },
            )



@asset(
    description="Extract features prepare for recommend from movies table",
    partitions_def=YEARLY,
    io_manager_key="spark_io_manager",
    ins={
        "silver_movies_cleaned": AssetIn(
            key_prefix=["silver", "movies"],
        ),
    },
    key_prefix=["silver", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def silver_movies_prepared_recommend(context, silver_movies_cleaned: DataFrame) -> Output[DataFrame]:
    context.log.info("Processing favorite movies for genre name mapping...")
     # 1. Khởi tạo cấu hình Spark session
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    if not all(config.values()):
        raise ValueError("Missing MINIO environment variables.")

    context.log.info("Creating Spark session for movies_cleaned ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        df= silver_movies_cleaned
        
        selected_columns = [
            "id",
            "adult",
            "genres",
            "overview",
            "popularity",
            "release_date",
            "title",
            "vote_average",
            "vote_count"
        ]

        context.log.info("Selecting relevant columns for silver_movies_information...")

        df_selected = df.select(*selected_columns)

        context.log.info(f"Finished selecting columns for partition: {context.partition_key}")

        return Output(
            df_selected,
            metadata={
                "table": "silver_movies_collected",
                "row_count": df_selected.count(),
                "column_count": len(df_selected.columns),
                "columns": df_selected.columns,
            },
        )

@asset(
    description="Load and join bronze_favorite_movies and bronze_genre_track tables, transform into spark dataframe",
    io_manager_key="spark_io_manager",
    ins={
        "bronze_genre_track": AssetIn(
            key_prefix=["bronze", "movies"],
        ),
        "bronze_favorite_movies": AssetIn(
            key_prefix=["bronze", "movies"],
        ),

    },
    key_prefix=["silver", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def silver_favorite_track(context, bronze_favorite_movies: pl.DataFrame, bronze_genre_track: pl.DataFrame) -> Output[DataFrame]:
    context.log.info("Processing favorite movies for genre name mapping...")

    # 1. Khởi tạo cấu hình Spark session
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    if not all(config.values()):
        raise ValueError("Missing MINIO environment variables.")

    context.log.info("Creating Spark session for movies_cleaned ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_favorite_movies (polars.DataFrame) to pandas.DataFrame
        pandas_df = bronze_favorite_movies.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )

        # Convert pandas DataFrame to pyspark.sql.DataFrame
        spark_df = spark.createDataFrame(pandas_df)
        spark_df.cache()
        context.log.info("Got Spark DataFrame for favorite movies")

        # Convert bronze_genre_track (polars.DataFrame) to pandas.DataFrame
        pandas_genre_track = bronze_genre_track.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_genre_track.shape}"
        )

        # Convert pandas DataFrame to pyspark.sql.DataFrame
        spark_genre_track = spark.createDataFrame(pandas_genre_track)
        spark_genre_track.cache()
        context.log.info("Got Spark DataFrame for genre track")

        spark_genre_track = spark_genre_track.withColumnRenamed("id", "genre_id")

        # 1. Explode genre_ids (list[int]) thành nhiều dòng
        exploded_df = spark_df.select(
            "id", "genre_ids"
        ).withColumn("genre_id", explode(col("genre_ids")))

        # 2. Join với genre_track theo genre_id
        joined_df = exploded_df.join(
            spark_genre_track,
            exploded_df["genre_id"] == spark_genre_track["genre_id"],
            how="left"
        )

        # 3. Gom lại tên genre theo movie_id
        genre_grouped = joined_df.groupBy("id").agg(
            collect_list("genre_name").alias("genre_names")
        )

        # 4. Join lại với bảng gốc để lấy thông tin chi tiết
        enriched_df = spark_df.join(
            genre_grouped,
            on="id",
            how="left"
        )

        # e. Đổi tên cột genre_names ➝ genres
        enriched_df = enriched_df.withColumnRenamed("genre_names", "genres")


        # 5. Chọn và sắp xếp lại các cột như yêu cầu
        final_df = enriched_df.select(
            "id","adult", "genres",  "overview", "popularity",
            "release_date", "title", "vote_average", "vote_count"
        )

        context.log.info("Finished generating silver_movies_information")

        return Output(
            final_df,
            metadata={
                "table": "silver_favorite_track",
                "row_count": final_df.count(),
                "column_count": len(final_df.columns),
                "columns": final_df.columns,
            }
        )




