FROM apache/airflow:2.9.1-python3.11

USER root

# ===== system =====
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    ca-certificates \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ===== Spark =====
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
  | tar -xz -C /opt \
  && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
  && chown -R airflow:0 /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV PATH="${SPARK_HOME}/bin:${PATH}"

# ===== GCS Connector =====
RUN curl -fsSL -o ${SPARK_HOME}/jars/gcs-connector-hadoop3-latest.jar \
  https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

# ===== BigQuery Connector (Spark 3.5 / Scala 2.12) =====
RUN curl -fsSL -o ${SPARK_HOME}/jars/spark-bigquery-with-dependencies.jar \
  https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar

# ===== Python deps =====
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt