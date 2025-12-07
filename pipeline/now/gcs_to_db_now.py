from google.cloud import storage
import io
import pandas as pd
from sqlalchemy import create_engine, text


def get_latest_file(bucket_name, prefix):
    """가장 최근에 업로드한 파일 찾기"""

    KEY_PATH = "<GCP_SERVICE_ACCOUNT_JSON_PATH>"

    client = storage.Client.from_service_account_json(KEY_PATH)
    bucket = client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=prefix))

    if not blobs:
        print("GCS에 해당하는 파일이 없습니다.")
        return None

    latest_blob = max(blobs, key=lambda b: b.updated)

    print("최근 파일:", latest_blob.name)
    return latest_blob


def load_gcs(blob):
    """CSV 파일 로드"""

    buffer = io.BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)

    df = pd.read_csv(buffer, encoding="utf-8")

    return df


# PostgreSQL 연결
engine = create_engine("<POSTGRESQL_CONNECTION_URL>")


def insert_weather_table(df):
    """weather_now 테이블 갱신"""

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE weather_now;"))

    df.to_sql(
        name="weather_now",
        con=engine,
        if_exists="append",
        index=False
    )

    print("weather_now 테이블 갱신 완료")


def insert_daily_table(df):
    """weather_now_daily 테이블에 매 시간 데이터 추가 저장"""

    df.to_sql(
        name="weather_now_daily",
        con=engine,
        if_exists="append",
        index=False
    )
    print("weather_now_daily 테이블 누적 저장 완료")


def insert_monthly_table(df):
    """weather_now_monthly 테이블에 매 시간 데이터 추가 저장"""

    df.to_sql(
        name="weather_now_monthly",
        con=engine,
        if_exists="append",
        index=False
    )
    print("weather_now_monthly 테이블 누적 저장 완료")


def main():
    BUCKET_NAME = "<GCS_BUCKET_NAME>"
    PREFIX = "<GCS_PREFIX_PATH>"

    latest_blob = get_latest_file(BUCKET_NAME, PREFIX)

    if latest_blob:
        df = load_gcs(latest_blob)

        insert_weather_table(df)  # 현재 날씨 테이블 갱신
        insert_daily_table(df)    # 하루치 데이터 테이블에 삽입
        insert_monthly_table(df)  # 월간 누적 테이블에 삽입
    else:
        print("업데이트할 파일이 없습니다.")


if __name__ == "__main__":
    main()
