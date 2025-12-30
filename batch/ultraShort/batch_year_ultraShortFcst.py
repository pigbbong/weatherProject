from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Spark Session
spark = (
    SparkSession.builder
    .appName("weather-ultraShortFcst-year-batch")
    .getOrCreate()
)

target_year = (datetime.now() - relativedelta(years=1)).year

# monthly 결과 경로
INPUT_PATH = "gs://<GCS_BUCKET_NAME>/processed/weather/batch/month/ultraShortFcst"

# yearly 결과 경로
OUTPUT_PATH = "gs://<GCS_BUCKET_NAME>/processed/weather/batch/year/ultraShortFcst"

try:
    # 대상 연도 로드
    df = (
        spark.read.parquet(INPUT_PATH)
        .where(col("year") == target_year)
    )

    # 연 단위 저장
    (
        df.write
        .mode("append")
        .partitionBy("year")
        .parquet(OUTPUT_PATH)
    )

except Exception as e:
    print(
        f"{target_year}, "
        f"초단기예보 일년치 데이터 처리 중 오류 발생: {e}"
    )

finally:
    spark.stop()