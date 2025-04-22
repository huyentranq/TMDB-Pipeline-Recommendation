FROM python:3.9.16-slim

# ========== Install Java and required system tools ==========
ARG openjdk_version="17"

RUN apt-get update --yes && \
    apt-get install --yes curl "openjdk-${openjdk_version}-jre-headless" ca-certificates-java procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ========== Download and install Spark ==========
RUN curl -O https://dlcdn.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz \
    && tar zxvf spark-3.4.4-bin-hadoop3.tgz \
    && rm -rf spark-3.4.4-bin-hadoop3.tgz \
    && mv spark-3.4.4-bin-hadoop3/ /usr/local/ \
    && rm -rf /usr/local/spark \
    && ln -s /usr/local/spark-3.4.4-bin-hadoop3 /usr/local/spark

# ========== Add PostgreSQL JDBC driver to Spark ==========
RUN curl -O https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
    && mv postgresql-42.7.1.jar /usr/local/spark/jars

# ========== [SỬA] Chỉ giữ AWS SDK v1.x và tương thích với Hadoop 3.3.2 ==========
RUN curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar \
    && mv aws-java-sdk-1.12.367.jar /usr/local/spark/jars \
    && mv hadoop-aws-3.3.2.jar /usr/local/spark/jars \
    && mv delta-core_2.12-2.2.0.jar /usr/local/spark/jars \
    && mv delta-storage-2.2.0.jar /usr/local/spark/jars

# ========== Setup working directory and install Python deps ==========
WORKDIR /opt/dagster/app/etl_pipeline  

COPY requirements.txt /opt/dagster/app/etl_pipeline
RUN pip install --upgrade pip && pip install -r requirements.txt --default-timeout=300

COPY . /opt/dagster/app/etl_pipeline

# ========== Default CMD ==========
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "elt_pipeline"]
