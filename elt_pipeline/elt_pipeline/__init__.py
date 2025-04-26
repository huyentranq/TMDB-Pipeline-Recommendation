from dagster import Definitions
import os

from .assets.bronze_layer import bronze_movies,bronze_genre_track,bronze_favorite_movies
from .assets.silver_layer import silver_movies_cleaned, silver_movies_vectors, silver_favorite_track,silver_my_vector
from .assets.gold_layer import gold_movies_infor,gold_movies_rating, gold_movies_genres,gold_my_vector, gold_movies_vector,gold_recommendations
# from .assets.warehouse import 
from .resources.mysql_io_manager import MySQLIOManager
from .resources.minio_io_manager import MinIOIOManager
from .resources.spark_io_manager import SparkIOManager
from .resources.psql_io_manager import PostgreSQLIOManager

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
}

SPARK_CONFIG = {
    "spark_master": os.getenv("SPARK_MASTER_URL"),
    "spark_version": os.getenv("SPARK_VERSION"),
    "hadoop_version": os.getenv("HADOOP_VERSION"),
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

defs = Definitions(
    assets=[
        
        bronze_movies,
        bronze_genre_track,
        bronze_favorite_movies,
        silver_movies_cleaned, 
        silver_movies_vectors,
        silver_favorite_track,
        silver_my_vector,
        gold_movies_infor,
        gold_movies_rating,
        gold_movies_genres,
        gold_my_vector,
        gold_movies_vector,
        gold_recommendations

    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "spark_io_manager": SparkIOManager(SPARK_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    }
)