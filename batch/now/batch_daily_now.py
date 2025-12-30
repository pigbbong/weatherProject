from pyspark.sql import SparkSession
from pyspark.sql.functions import (input_file_name, regexp_extract, col)
from datetime import datetime, timedelta

# Spark Session
spark = (
    SparkSession.builder
    .appName("weather-now-daily-batch")
    .getOrCreate()
)

target_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

RAW_PATH = f"gs://<GCS_BUCKET_NAME>/raw/weather/now/{target_date}*.parquet"
OUTPUT_PATH = "gs://<GCS_BUCKET_NAME>/processed/weather/batch/daily/now"

try:
    # Raw 데이터 로드
    df = spark.read.parquet(RAW_PATH)

    # 파일명에서 날짜 추출
    df = (
        df
        .withColumn("filename", input_file_name())
        .withColumn("year",  regexp_extract(col("filename"), r"(\d{4})\d{8}", 1))
        .withColumn("month", regexp_extract(col("filename"), r"\d{4}(\d{2})\d{6}", 1))
        .withColumn("day",   regexp_extract(col("filename"), r"\d{6}(\d{2})\d{4}", 1))
    )

    # 타입 정리 (문자 → 정수)
    df = (
        df
        .withColumn("year",  col("year").cast("int"))
        .withColumn("month", col("month").cast("int"))
        .withColumn("day",   col("day").cast("int"))
    )

    # Parquet 저장
    (
        df
        .drop("filename")
        .coalesce(1)
        .write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(OUTPUT_PATH)
    )

except Exception as e:
    print(f"{target_date}, 초단기실황 하루치 데이터 처리 중 오류 발생: {e}")

finally:
    spark.stop()
