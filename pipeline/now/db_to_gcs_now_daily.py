import pandas as pd
from google.cloud import storage
from datetime import datetime
import io
import os
from sqlalchemy import create_engine, text
from zoneinfo import ZoneInfo

# 한국 시간(KST)
now_kst = datetime.now(ZoneInfo("Asia/Seoul"))

# 오늘 날짜
year = now_kst.year
month = now_kst.month
day = now_kst.day

# GCS 업로드 파일명
gcs_filename = f"raw/now/daily/{year}{month:02d}{day:02d}_weather_now.parquet"

BASE_DIR = "/opt/pipelines/now"
KEY_PATH = "<GCP_SERVICE_ACCOUNT_JSON_PATH>"
BUCKET_NAME = "<GCS_BUCKET_NAME>"

# 환경변수 세팅
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

# PostgreSQL 연결
engine = create_engine("<POSTGRESQL_CONNECTION_URL>")


def upload_daily_to_gcs():
    try:
        # daily_temp 초기화
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE weather_now_daily_temp;"))

        # daily 데이터를 temp에 백업
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO weather_now_daily_temp
                SELECT * FROM weather_now_daily;
                            """))
    except Exception as e:
        print(f"{year}년 {month}월 {day}일의 실시간 daily 데이터 교체 중 오류 발생: {e}")
        return

    try:
        # daily 테이블 조회
        df = pd.read_sql("SELECT * FROM weather_now_daily;", con=engine)

        if df.empty:
            print("실시간 daily 테이블이 비어있어서 업로드 중단")
            return

        # Parquet 파일로 변환
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        # GCS로 업로드
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_filename)
        blob.upload_from_file(buffer, content_type="application/octet-stream")

        print("실시간 daily 업로드 완료")
    except Exception as e:
        print(f"{year}년 {month}월 {day}일의 실시간 daily 데이터 GCS로 업로드 중 오류 발생: {e}")
        return


def main():
    upload_daily_to_gcs()


if __name__ == "__main__":
    main()
