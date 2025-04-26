from dagster import (
    asset,
    AssetIn,
    Output,
    StaticPartitionsDefinition,
)
from pyspark.sql import DataFrame
from datetime import datetime
import pyarrow as pa
import polars as pl
import pandas as pd
COMPUTE_KIND = "Postgres"
LAYER = "warehouse"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1975, datetime.today().year)]
)

@asset(
    description="Load movies_infor data from spark to postgres",

    ins={
        "gold_movies_infor": AssetIn(
            key_prefix=["gold", "movies"],
        ),
    },
    metadata={
        "primary_keys": ["id"],
        
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "movies"],  
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def movies_infor(context, gold_movies_infor: DataFrame):


    return Output(
        value=gold_movies_infor,
        metadata={
            "database": "movies",
            "schema": "warehouse",
            "table": "movies_infor",
            "primary_keys": ["id"],

        },
    )


@asset(
    description="Load movies_rating data from spark to postgres",
    ins={
        "gold_movies_rating": AssetIn(
            key_prefix=["gold", "movies"],
        ),
    },
    metadata={
        "primary_keys": ["id"],
        "columns": ["id", "vote_average", "vote_count"]
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse","movies"],  
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def movies_rating(context, gold_movies_rating: DataFrame):
 

    context.log.info("Got spark DataFrame, loading to postgres")
    df = gold_movies_rating
    return Output(
        value=df,
        metadata={
            "database": "love_movies",
            "schema": "movies",
            "table": "movies_rating",
            "primary_keys": ["id"],
            "columns": ["id", "vote_average", "vote_count"],
        },
    )


@asset(
    description="Load movies_genres data from spark to postgres",
    ins={
        "gold_movies_genres": AssetIn(
            key_prefix=["gold", "movies"],
        ),
    },
    metadata={
        "primary_keys": ["id"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse","movies"],  
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def movies_genres(context, gold_movies_genres: DataFrame):
 
    context.log.info("Got spark DataFrame, loading to postgres")

    df = gold_movies_genres
    return Output(
        value=df,
        metadata={
            "database": "love_movies",
            "schema": "movies",
            "table": "movies_genres",
            "primary_keys": ["id"],
        },
    )

@asset(
    description="Load silver_favorite_track data from spark to postgres",
    ins={
        "silver_favorite_track": AssetIn(
            key_prefix=["silver", "movies"],
        ),
    },
    metadata={
        "primary_keys": ["id"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse","movies"],  
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def favorite_track(context, silver_favorite_track: DataFrame):
 

    context.log.info("Got spark DataFrame, loading to postgres")


    return Output(
        silver_favorite_track,
        metadata={
            "database": "love_movies",
            "schema": "movies",
            "table": "favorite_track",

        },
    )


@asset(
    description="Load silver_favorite_track data from spark to postgres",
    ins={
        "gold_recommendations": AssetIn(
            key_prefix=["gold", "movies"],
        ),
    },
    metadata={
        "primary_keys": ["id"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse","movies"],  
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def recommendations(context, gold_recommendations: DataFrame):
 

    context.log.info("Got spark DataFrame, loading to postgres")

    return Output(
        value=gold_recommendations,
        metadata={
            "database": "love_movies",
            "schema": "movies",
            "table": "recommendations",
            "primary_keys": ["id"],
        },
    )