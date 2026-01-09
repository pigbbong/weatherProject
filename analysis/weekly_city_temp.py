from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, expr
import sys


def main():
    spark = None

    try:
        spark = (
            SparkSession.builder
            .appName("weekly-city-temp-summary")
            .getOrCreate()
        )

        INPUT_PATH = "gs://pigbbong_weather/processed/weather/batch/daily/now/*/*/*/*.parquet"

        BQ_PROJECT = "weather-478204"
        BQ_DATASET = "weather_analytics"
        BQ_TABLE = "weekly_city_temp_summary"

        # 데이터 로드
        df = spark.read.parquet(INPUT_PATH)

        # basetime 기준 최근 7일 필터
        df_7days = df.withColumn(
            "ts",
            to_timestamp(col("basetime"), "yyyyMMddHHmm")
        ).filter(
            col("ts") >= expr("current_timestamp() - INTERVAL 7 DAYS")
        )

        # Temp View
        df_7days.createOrReplaceTempView("weather_daily")

        # Spark SQL 집계
        result_df = spark.sql("""
            WITH base AS (
                SELECT
                    city,
                    to_date(substr(basetime, 1, 8), 'yyyyMMdd') AS date,
                    T1H
                FROM weather_daily
                WHERE T1H NOT IN (-999.0)
            )
            SELECT
                city,
                date,
                MAX(T1H) AS daily_max_temp,
                MIN(T1H) AS daily_min_temp
            FROM base
            GROUP BY city, date
            ORDER BY city, date
        """)

        # BigQuery overwrite
        (
            result_df.write
            .format("bigquery")
            .option("table", f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
            .option("writeMethod", "direct")
            .mode("overwrite")
            .save()
        )

        print("BigQuery overwrite completed successfully.")

    except Exception as e:
        print("Error occurred during Spark job execution")
        print(e)
        sys.exit(1)

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
