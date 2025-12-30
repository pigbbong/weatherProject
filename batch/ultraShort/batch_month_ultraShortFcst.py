from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Spark Session
spark = (
    SparkSession.builder
    .appName("weather-ultraShortFcst-month-batch")
    .getOrCreate()
)

target = datetime.now() - relativedelta(months=1)
target_year = target.year
target_month = target.month

# daily 결과 경로
INPUT_PATH = "gs://<GCS_BUCKET_NAME>/processed/weather/batch/daily/ultraShortFcst"

# monthly 결과 경로
OUTPUT_PATH = "gs://<GCS_BUCKET_NAME>/processed/weather/batch/month/ultraShortFcst"

try:
    # daily 데이터 로드
    df = (
        spark.read.parquet(INPUT_PATH)
        .where(
            (col("year") == target_year) &
            (col("month") == target_month)
        )
    )

    # 월 단위로 재저장
    (
        df.write
        .mode("append")
        .partitionBy("year", "month")
        .parquet(OUTPUT_PATH)
    )

except Exception as e:
    print(
        f"{target_year}{str(target_month).zfill(2)}, "
        f"초단기예보 한달치 데이터 처리 중 오류 발생: {e}"
    )

finally:
    spark.stop()