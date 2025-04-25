import os
from dagster import asset, AssetIn, AssetOut, Output, StaticPartitionsDefinition
import polars as pl
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from ..resources.spark_io_manager import get_spark_session
from pyspark.sql.functions import monotonically_increasing_id, lit, concat, explode, collect_list, col, udf,split
from pyspark.sql.types import StringType
from datetime import datetime, timedelta
COMPUTE_KIND = "pyspark"
LAYER = "silver"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1920, datetime.today().year)] 
)   

# 1. UDF để decode từng phần tử bytearray sang string
def decode_genre_item(byte_array):
    return byte_array.decode('utf-8') if isinstance(byte_array, (bytes, bytearray)) else byte_array

decode_genre_udf = udf(decode_genre_item, StringType())


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
                "overview": "nan",
                "original_title": "nan",
                "popularity": avg_popularity,
                "production_companies": "nan",
                "production_countries": "nan",
                "keywords": "nan",
                "genres": "nan"
            })

            context.log.info(f"Finished cleaning movies dataset for partition: {context.partition_key}")
            context.log.info(f" Data types:\n{df_filled.dtypes}")

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

def silver_movies_vectors(context, silver_movies_cleaned: DataFrame) -> Output[DataFrame]:
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
                "popularity",
                "release_date",
                "vote_average",
                "vote_count",
                "genres"
        ]

        context.log.info("Selecting relevant columns for silver_movies_information...")

        df_selected = df.select(*selected_columns)

        context.log.info(f"Finished selecting columns for partition: {context.partition_key}")
         # Chuyển cột 'genres' từ string thành list chứa các genres. nếu nan thì chuyển về list rỗng 
        df_selected = df_selected.withColumn(
            "genres", 
        F.when(F.col("genres") == "nan", F.array()).otherwise(F.split(F.col("genres"), ","))
    )

        context.log.info(f"Genres column split into list: {df_selected.select('genres').show(5)}")
        context.log.info(f" Data types:\n{df_selected.dtypes}")
        context.log.info(f"data: {df_selected.head(5)}")
        
        return Output(
            df_selected,
            metadata={
                "table": "silver_movies_vectors",
                "row_count": df_selected.count(),
                "column_count": len(df_selected.columns),
                "columns": df_selected.columns,
            },
        )

@asset(
    description="Load and join bronze_favorite_movies and bronze_genre_track tables, processing encoding error and transform into spark dataframe",
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

         #####################

                 # a. explode genre_ids thành từng dòng genre_id
        exploded_df = spark_df.withColumn("genre_id", explode(col("genre_ids")))

        # b. Join với bảng genre_track để lấy tên genre tương ứng
        joined_df = exploded_df.join(spark_genre_track, on="genre_id", how="left")
        # ⚠️ Lọc bỏ những dòng không match được genre_name
        joined_df = joined_df.filter(col("genre_name").isNotNull())
        # c. Gom nhóm lại theo phim, và tổng hợp genre thành list tên
        movie_columns = [
            "id", "adult", "overview", "popularity",
            "release_date", "title", "vote_average", "vote_count"
        ]

        enriched_df = joined_df.groupBy(*movie_columns).agg(
            collect_list("genre_name").alias("genres")
        )
        final_df= enriched_df
 # 6. Chuyển cột genres từ bytearray sang string bằng explode → decode → group lại
        final_df = enriched_df.select(
                "id",
                "title",
                "adult",
                "popularity",
                "overview",
                "release_date",
                "vote_average",
                "vote_count",
                col("genres").alias("genre_names_raw")
        )
        final_df = final_df.withColumn("genre", explode(col("genre_names_raw")))
        final_df = final_df.withColumn("genre", decode_genre_udf("genre"))
        
        final_df = final_df.groupBy(
            # it's also final columns
                "id",
                "title",
                "adult",
                "popularity",
                "overview",
                "release_date",
                "vote_average",
                "vote_count",
        ).agg(collect_list("genre").alias("genres"))

        # 7. Log và trả kết quả
        genres_list = final_df.select("genres").collect()
        context.log.info(f"Genres: {genres_list}")
        context.log.info(f" Data types:\n{final_df.dtypes}")
        context.log.info("Finished generating silver_movies_information")

        return Output(
            final_df,
            metadata={
                "table": "silver_favorite_track",
                "row_count": final_df.count(),
                "column_count": len(final_df.columns),
                "columns": final_df.columns
            }
        )

@asset(
    description="extract features for vector recommendation from silver_favorite_track",
    io_manager_key="spark_io_manager",
    ins={
        "silver_favorite_track": AssetIn(
            key_prefix=["silver", "movies"],
        )

    },
    key_prefix=["silver", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def silver_my_vector(context, silver_favorite_track: DataFrame) -> Output[DataFrame]:
    context.log.info("extract featurea for vector recommendation from silver_favorite_track")
     # 1. Khởi tạo cấu hình Spark session
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    if not all(config.values()):
        raise ValueError("Missing MINIO environment variables.")


    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        df= silver_favorite_track
        
        selected_columns = [
                "id",
                "popularity",
                "release_date",
                "vote_average",
                "vote_count",
                "genres"
        ]

        context.log.info("Selecting relevant columns for silver_movies_information...")

        df_selected = df.select(*selected_columns)


        return Output(
            df_selected,
            metadata={
                "table": "silver_my_vector",
                "row_count": df_selected.count(),
                "column_count": len(df_selected.columns),
                "columns": df_selected.columns,
            },
        )


