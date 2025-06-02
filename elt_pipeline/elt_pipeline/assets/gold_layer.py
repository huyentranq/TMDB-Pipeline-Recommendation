import os
from dagster import asset, AssetIn, multi_asset,AssetOut, Output, StaticPartitionsDefinition, AssetKey
import polars as pl
from pyspark.sql.dataframe import DataFrame
from ..resources.spark_io_manager import get_spark_session
from pyspark.sql.functions import monotonically_increasing_id, lit, concat, split, udf
from datetime import datetime, timedelta 
import pyarrow as pa
import numpy as np
from pyspark.sql.types import FloatType
##
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler, HashingTF, IDF
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors, SparseVector
COMPUTE_KIND = "spark"
LAYER = "gold"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1920, datetime.today().year)] 
)   

@asset(
    description="Load movies_basic data from spark to minIO",
    io_manager_key="spark_io_manager",
    ins={
        "silver_movies_cleaned": AssetIn(
            key_prefix=["silver", "movies"],
            metadata={"full_load": True}
        ),
    },
    key_prefix=["gold", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def gold_movies_infor(context, silver_movies_cleaned: DataFrame):
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
    return Output(
        value=spark_df,
        metadata={
            "table": "gold_movies_infor",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )

@asset(
    description="extract movies rating data from spark to gold_layer",
    io_manager_key="spark_io_manager",
    ins={
        "silver_movies_cleaned": AssetIn(
            key_prefix=["silver", "movies"],
            metadata={"full_load": True}
        ),
    },
    key_prefix=["gold", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def gold_movies_rating(context, silver_movies_cleaned: DataFrame):
    """
    Load movies rating data from spark to minIO 
    """

    spark_df = silver_movies_cleaned
    spark_df = spark_df.select(
            "id",
            "vote_average",
            "vote_count"
        
        )
    return Output(
        value=spark_df,
        metadata={
            "table": "gold_movies_rating",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )

# movies_genres
@asset(
    description="extract movies genres data from spark to gold_layer",
    io_manager_key="spark_io_manager",
    ins={
        "silver_movies_cleaned": AssetIn(
            key_prefix=["silver", "movies"],
            metadata={"full_load": True}
        ),
    },
    key_prefix=["gold", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def gold_movies_genres(context, silver_movies_cleaned: DataFrame):
    """
    Load movies genres data from spark to minIO
    """

    spark_df = silver_movies_cleaned
    spark_df = spark_df.select(
            "id",
            "genres"
        
        )
    return Output(
        value=spark_df,
        metadata={
            "table": "gold_movies_genres",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns)
        },
    )

@asset(
    description="transform my_vector data ",
    io_manager_key="spark_io_manager",
    ins={
        "silver_my_vector": AssetIn(
            key_prefix=["silver", "movies"]
        ),
    },
    key_prefix=["gold", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def gold_my_vector(context, silver_my_vector: DataFrame):
    """
        Transform  my movie vector data, combine all features into a single vector.

    """
    spark = silver_my_vector

    # 1. Lấy năm từ release_date
    df = silver_my_vector.withColumn("release_year", F.year("release_date"))

    # 2. Dùng trực tiếp genres list với HashingTF
    hashingTF = HashingTF(inputCol="genres", outputCol="genres_tf", numFeatures=20)
    idf = IDF(inputCol="genres_tf", outputCol="genres_tfidf")

    # 3. Nhân các biến số với trọng số (weight)

    df = df.withColumn("weighted_popularity", F.col("popularity") * 1.0)
    df = df.withColumn("weighted_release_year", F.col("release_year") * 0.5)
    df = df.withColumn("weighted_vote_average", F.col("vote_average") * 1.2)
    df = df.withColumn("weighted_vote_count", F.col("vote_count") * 0.8)

    # 4. Assemble weighted numeric features
    assembler = VectorAssembler(
        inputCols=["weighted_popularity", "weighted_release_year", "weighted_vote_average", "weighted_vote_count"],
        outputCol="numeric_features"
    )
    scaler = StandardScaler(inputCol="numeric_features", outputCol="numeric_scaled")

    # 5. Combine tất cả lại thành 1 vector cuối
    final_assembler = VectorAssembler(
        inputCols=["numeric_scaled", "genres_tfidf"],
        outputCol="final_vector"
    )

    pipeline = Pipeline(stages=[hashingTF, idf, assembler, scaler, final_assembler])
    model = pipeline.fit(df)
    transformed = model.transform(df)

    # 6. Lấy trung bình vector của 20 phim → tạo user vector
    user_vector = transformed.select("final_vector") \
        .rdd.map(lambda row: row["final_vector"].toArray()) \
        .mean()

    # Convert lại thành Spark DenseVector để đồng bộ
    user_vector_df = silver_my_vector.sparkSession.createDataFrame(
        [(Vectors.dense(user_vector),)],
        ["user_vector"]
    )
    # # Chuyển sang DenseVector
    # dense_vector = Vectors.dense(user_vector)

    # # Chuyển sang SparseVector để giống movie_vector
    # sparse_vector = Vectors.sparse(len(dense_vector), [(i, v) for i, v in enumerate(dense_vector) if v != 0])

    # Đưa vào DataFrame

    user_vector_df = silver_my_vector.sparkSession.createDataFrame(
        [(Vectors.dense(user_vector),)],
        ["user_vector"]
    )
    # user_vector_df.write.mode("overwrite").parquet("s3a://lakehouse/gold/movies/gold_my_vector/user_vector.parquet")
    context.log.info(f"[DEBUG] Row count silver_my_vector: {silver_my_vector.count()}")
    context.log.info(f"[DEBUG] Sample input: {silver_my_vector.select('genres', 'popularity', 'release_date').show(3)}")
    context.log.info(f"[DEBUG] Transformed schema: {transformed.printSchema()}")
    context.log.info(f"[DEBUG] Final vector sample: {transformed.select('final_vector').show(3)}")
    context.log.info(f"[DEBUG] User vector result: {user_vector}")
    return Output(
        value=user_vector_df,
        metadata={
            "table": "gold_my_vector",
            "row_count": user_vector_df.count(),
            "column_count": len(user_vector_df.columns)
        },
    )

@asset(
    description="transform movies vector data ",
    io_manager_key="spark_io_manager",    
    partitions_def=YEARLY,
    ins={
        "silver_movies_vectors": AssetIn(
            key_prefix=["silver", "movies"],
        ),
    },
    key_prefix=["gold", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def gold_movies_vector(context, silver_movies_vectors: DataFrame):
        """
    Transform movies vector data for each movie and generate movie vectors.
    """
        spark = silver_movies_vectors

        # 1. Lấy năm từ release_date
        df = silver_movies_vectors.withColumn("release_year", F.year("release_date"))

        # 2. Dùng trực tiếp genres list với HashingTF
        hashingTF = HashingTF(inputCol="genres", outputCol="genres_tf", numFeatures=20)
        idf = IDF(inputCol="genres_tf", outputCol="genres_tfidf")

        # 3. Nhân các biến số với trọng số (weight)
        df = df.withColumn("weighted_popularity", F.col("popularity") * 1.0)
        df = df.withColumn("weighted_release_year", F.col("release_year") * 0.5)
        df = df.withColumn("weighted_vote_average", F.col("vote_average") * 1.2)
        df = df.withColumn("weighted_vote_count", F.col("vote_count") * 0.8)

        # 4. Assemble weighted numeric features
        assembler = VectorAssembler(
            inputCols=["weighted_popularity", "weighted_release_year", "weighted_vote_average", "weighted_vote_count"],
            outputCol="numeric_features"
        )
        scaler = StandardScaler(inputCol="numeric_features", outputCol="numeric_scaled")

    # 5. Combine tất cả lại thành 1 vector cuối
        final_assembler = VectorAssembler(
            inputCols=["numeric_scaled", "genres_tfidf"],
            outputCol="movie_vector"
        )

        # Đưa final_assembler vào pipeline
        pipeline = Pipeline(stages=[hashingTF, idf, assembler, scaler, final_assembler])
        model = pipeline.fit(df)
        transformed = model.transform(df)

        # Chỉ chọn id và vector cuối cùng
        movies_vector_df = transformed.select("id", "movie_vector")

        context.log.info(f" Data types:\n{movies_vector_df.dtypes}")
        context.log.info(f" Sample:\n{movies_vector_df.show(5)}")

        return Output(
            value=movies_vector_df,
            metadata={
                "table": "movies_vector_df",
                "row_count": movies_vector_df.count(),
                "column_count": len(movies_vector_df.columns)
            },
        )




@asset(
    description="Create recommendations score based on movie vectors and user vector",
    io_manager_key="spark_io_manager",    
    ins={
        "gold_movies_vector": AssetIn(
            key_prefix=["gold", "movies"],
            metadata={"full_load": True}
        ),
        "gold_my_vector": AssetIn(
            key_prefix=["gold", "movies"],
        ),

    },
    key_prefix=["gold", "movies"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def gold_recommendations(context, gold_movies_vector:DataFrame, gold_my_vector:DataFrame)-> Output[DataFrame]:
    """
    Generate recommendations based on the movie vectors and user vector using cosine similarity.
    """
    
    user_vector_df = gold_my_vector
    context.log.info(f" load from minio success :\n{user_vector_df.dtypes}")

    user_vector = user_vector_df.first()["user_vector"].toArray()

    # Define the UDF to calculate cosine similarity
    def cosine_similarity(movie_vector):
        # Convert movie_vector to numpy array
        if isinstance(movie_vector, SparseVector):
            movie_vector_array = movie_vector.toArray()  # Convert SparseVector to DenseVector
        else:
            movie_vector_array = np.array(movie_vector.toArray())  # In case it's already a DenseVector

        # Compute dot product and norms
        dot_product = np.dot(user_vector, movie_vector_array)
        norm_user = np.linalg.norm(user_vector)
        norm_movie = np.linalg.norm(movie_vector_array)

        # Return the cosine similarity
        return float(dot_product / (norm_user * norm_movie)) if norm_user and norm_movie else 0.0

    # Register the UDF
    cosine_similarity_udf = udf(cosine_similarity, FloatType())

    # Apply UDF to calculate the cosine similarity for each movie vector
    recommendations_df = gold_movies_vector.withColumn(
        "cosine_similarity", cosine_similarity_udf(F.col("movie_vector"))
    ).select("id", "cosine_similarity")  # Select only id and cosine_similarity

    # Log the data types and show sample data
    context.log.info(f"Data types:\n{recommendations_df.dtypes}")
    recommendations_df.show(5, truncate=False)  # Show sample data in the console

    # Return the recommendations DataFrame as output
    return Output(
        value=recommendations_df,
        metadata={
            "table": "gold_recommendations",
            "row_count": recommendations_df.count(),
            "column_count": len(recommendations_df.columns),
        },
    )