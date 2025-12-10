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

# GCS 업로드 파일명
gcs_filename = f"raw/now/month/{year}{month:02d}_weather_now.parquet"

KEY_PATH = "<GCP_SERVICE_ACCOUNT_JSON_PATH>"
BUCKET_NAME = "<GCS_BUCKET_NAME>"

# 환경변수 세팅
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

# PostgreSQL 연결
engine = create_engine("<POSTGRESQL_CONNECTION_URL>")


def upload_monthly_to_gcs():
    try:
        # monthly_temp 초기화
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE weather_now_monthly_temp;"))

        # 기존 monthly 데이터를 temp로 백업
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO weather_now_monthly_temp
                SELECT * FROM weather_now_monthly;
            """))

    except Exception as e:
        print(f"{year}년 {month}월의 실시간 monthly 데이터 교체 중 오류 발생: {e}")
        return

    try:
        # monthly 테이블 조회
        df = pd.read_sql("SELECT * FROM weather_now_monthly;", con=engine)

        if df.empty:
            print("실시간 monthly 테이블이 비어있어 업로드 중단")
            return

        # Parquet 변환
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        # GCS 업로드
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_filename)
        blob.upload_from_file(buffer, content_type="application/octet-stream")

        print("실시간 monthly 업로드 완료")

        with engine.begin() as conn:
            conn.execute(text("TRUNCATE weather_now_monthly;"))
        print("실시간 monthly 테이블 초기화 완료")

    except Exception as e:
        print(f"{year}년 {month}월 monthly 데이터를 GCS 업로드 중 오류 발생: {e}")


def main():
    upload_monthly_to_gcs()


if __name__ == "__main__":
    main()
