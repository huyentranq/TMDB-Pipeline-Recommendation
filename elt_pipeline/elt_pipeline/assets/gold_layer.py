import os
from dagster import asset, AssetIn, multi_asset,AssetOut, Output, StaticPartitionsDefinition
import polars as pl
from pyspark.sql.dataframe import DataFrame
from ..resources.spark_io_manager import get_spark_session
from pyspark.sql.functions import monotonically_increasing_id, lit, concat, split
from datetime import datetime, timedelta 
import pyarrow as pa

COMPUTE_KIND = "python"
LAYER = "silver"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1920, datetime.today().year)] 
)   

# movies_infor to gold (minIO) and warehouse (postgres)
@multi_asset(
    ins={
        "silver_movies_cleaned": AssetIn(
            key_prefix=["silver", "movies"],
        )
    },
    outs={
        "gold_movies_basic_infor": AssetOut(
            description="extract movies basic information from spark to gold_layer",
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "movies"],
            group_name="gold"
        ),
        "movies_basic_infor": AssetOut(
            description="Load movies basic information from spark to postgres",
            io_manager_key="psql_io_manager",
            key_prefix=["gold", "movies","basic_infor_postgres"],  
            group_name="warehouse",
            metadata={

                "primary_keys": ["id"]
            }
        ),
    },
    compute_kind=COMPUTE_KIND,
)
def gold_movies_basic_infor(context, silver_movies_cleaned: DataFrame):
    """
    Load movies_basic data from spark to minIO and postgres
    """

    spark_df = silver_movies_cleaned
    spark_df = spark_df.select(
            "id",
            "title",
            "overview",
            "release_date",
            "runtime",
            "genres"
        )
    context.log.info("Got spark DataFrame, converting to polars DataFrame")
    # Convert from spark DataFrame to polars DataFrame
    df = pl.from_arrow(
        pa.Table.from_batches(spark_df._collect_as_arrow())
    )
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_movies_basic_infor",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    ), Output(
        value=df,
        metadata={
            "database": "movies",
            "schema": "gold",
            "table": "movies_basic_infor",
            "primary_keys": ["id"],
            "columns": df.columns
        },
    )


# movies_rating
@multi_asset(
    ins={
        "silver_movies_cleaned": AssetIn(
            key_prefix=["silver", "movies"],
        )
    },
    outs={
        "gold_movies_rating": AssetOut(
            description="extract movies rating data from spark to gold_layer",
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "movies"],
            group_name="gold"
        ),
        "movies_rating": AssetOut(
            description="Load movies rating data from spark to postgres",
            io_manager_key="psql_io_manager",
            key_prefix=["gold", "movies","movies_rating_postgres"],  
            group_name="warehouse",
            metadata={
                "primary_keys": ["id"]
            }
        ),
    },
    compute_kind=COMPUTE_KIND,
)
def gold_movies_rating(context, silver_movies_cleaned: DataFrame):
    """
    Load movies rating data from spark to minIO and postgres
    """

    spark_df = silver_movies_cleaned
    spark_df = spark_df.select(
            "id",
            "vote_average",
            "vote_count"
        
        )
    context.log.info("Got spark DataFrame, converting to polars DataFrame")
    # Convert from spark DataFrame to polars DataFrame
    df = pl.from_arrow(
        pa.Table.from_batches(spark_df._collect_as_arrow())
    )
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_movies_rating",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    ), Output(
        value=df,
        metadata={
            "database": "movies",
            "schema": "gold",
            "table": "movies_rating",
            "primary_keys": ["id"],
            "columns": df.columns
        },
    )

# movies_genres
@multi_asset(
    ins={
        "silver_movies_cleaned": AssetIn(
            key_prefix=["silver", "movies"],
        )
    },
    outs={
        "gold_movies_genres": AssetOut(
            description="extract movies genres data from spark to gold_layer",
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "movies"],
            group_name="gold"
        ),
        "movies_rating": AssetOut(
            description="Load movies genres data from spark to postgres",
            io_manager_key="psql_io_manager",
            key_prefix=["gold", "movies","movies_genres_postgres"],  
            group_name="warehouse",
            metadata={
                "primary_keys": ["id"]
            }
        ),
    },
    compute_kind=COMPUTE_KIND,
)
def gold_movies_genres(context, silver_movies_cleaned: DataFrame):
    """
    Load movies genres data from spark to minIO and postgres
    """

    spark_df = silver_movies_cleaned
    spark_df = spark_df.select(
            "id",
            "genres"
        
        )
    context.log.info("Got spark DataFrame, converting to polars DataFrame")
    # Convert from spark DataFrame to polars DataFrame
    df = pl.from_arrow(
        pa.Table.from_batches(spark_df._collect_as_arrow())
    )
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_movies_genres",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns)
        },
    ), Output(
        value=df,
        metadata={
            "database": "movies",
            "schema": "gold",
            "table": "movies_genres",
            "primary_keys": ["id"],
            "columns": df.columns
        },
    )



# @asset(
#     description="Cleaning movies prepared recommendation",
#     partitions_def=YEARLY,
#     io_manager_key="spark_io_manager",
#     ins={
#         "silver_movies_prepared_recommend": AssetIn(
#             key_prefix=["silver", "movies"],
#         ),
#     },
#     key_prefix=["silver", "movies"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def gold_movies_prepared_recommend(context, silver_movies_prepared_recommend: DataFrame):
#     """
#     extract movies business data from spark to minIO and postgres
#     """

#     spark_df = silver_movies_prepared_recommend

    
#     spark_df = spark_df.withColumn("genres", split("genres", ","))

#     return Output(
#         value=spark_df,
#         metadata={
#             "table": "gold_movies_prepared_recommend",
#             "row_count": spark_df.count(),
#             "column_count": len(spark_df.columns),
#             "columns": spark_df.columns,
#         },
#     )


# ## favorite track 
# @asset(
#     description="full load my favorite movies track",
#     partitions_def=YEARLY,
#     io_manager_key="spark_io_manager",
#     ins={
#         "silver_favorite_track": AssetIn(
#             key_prefix=["silver", "movies"],
#         ),
#     },
#     key_prefix=["silver", "movies"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def gold_favorite_track(context, silver_favorite_track: DataFrame):
#     """
#     extract movies business data from spark to minIO and postgres
#     """

#     spark_df = silver_favorite_track

    
#     spark_df = spark_df.withColumn("genres", split("genres", ","))

#     return Output(
#         value=spark_df,
#         metadata={
#             "table": "gold_favorite_track",
#             "row_count": spark_df.count(),
#             "column_count": len(spark_df.columns),
#             "columns": spark_df.columns,
#         },
#     )