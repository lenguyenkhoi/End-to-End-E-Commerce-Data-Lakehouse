# ============================================================
# Dockerfile — Spark Streaming Worker
# Context: ./3_spark_processing
# ============================================================

FROM python:3.10-slim-bookworm

# ── System dependencies ──────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ── Python dependencies ──────────────────────────────────────
RUN pip install --no-cache-dir \
        pyspark==3.5.0 \
        kafka-python==2.0.2 \
        google-cloud-bigquery==3.11.4 \
        google-cloud-storage==2.10.0

# ── PRE-BAKE JARs into Spark's system jars directory ────────
#
# CRITICAL: JARs placed in $SPARK_HOME/jars/ are loaded by the
# SYSTEM classloader at JVM startup, not the user classloader.
# This is the ONLY way to make Hadoop's FileSystem registry
# find GoogleHadoopFileSystem — spark.jars.packages puts JARs
# in the user classloader which Hadoop cannot see.
#
# Versions pinned together for compatibility:
#   spark-3.5-bigquery 0.37.0  → fixes Guice injection crash in 0.36.x
#   gcs-connector hadoop3-2.2.22 → gs:// FileSystem driver for Hadoop3
#   spark-sql-kafka 3.5.0     → matches PySpark version above

ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark
ENV SPARK_JARS_DIR=${SPARK_HOME}/jars

RUN curl -fL -o ${SPARK_JARS_DIR}/gcs-connector-hadoop3-2.2.22.jar \
        "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.22.jar" \
    && curl -fL -o ${SPARK_JARS_DIR}/spark-3.5-bigquery-0.37.0.jar \
        "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-3.5-bigquery/0.37.0/spark-3.5-bigquery-0.37.0.jar" \
    && echo "✅ GCS + BigQuery JARs baked into image"

WORKDIR /app