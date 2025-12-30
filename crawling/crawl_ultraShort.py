import math
import json
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
from collections import defaultdict
from sqlalchemy import create_engine, text
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
import re
import random

# 기본 설정
SERVICE_KEY = os.getenv(
    "SERVICE_KEY",
    "<SERVICE_KEY 입력>"
)

BASE_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst"

BASE_DIR = Path(os.getenv(
    "APP_BASE_PATH",
    Path(__file__).resolve().parent.parent
))

REGION_FILE = BASE_DIR / "region.json"
ICON_MAPPING = BASE_DIR / "icon_mapping.json"

with open(REGION_FILE, "r", encoding="utf-8") as f:
    regions = json.load(f)

with open(ICON_MAPPING, "r", encoding="utf-8") as f:
    icon_map = json.load(f)

# KST로 시간 설정
KST = timezone(timedelta(hours=9))
now = datetime.now(KST)

if now.minute < 45:
    base_dt = now - timedelta(hours=1)
else:
    base_dt = now

base_dt = base_dt.replace(minute=30, second=0, microsecond=0)

base_date = base_dt.strftime("%Y%m%d")
base_time = base_dt.strftime("%H%M")

# GCS 설정
KEY_PATH = "<SERVICE_ACCOUNT_JSON_PATH>"
BUCKET_NAME = "<GCS_BUCKET_NAME>"
GCS_PATH = "raw/weather/ultraShortFcst"

# Postgresql 설정
DB_HOST = "<DB_HOST>"
DB_PORT = "<DB_PORT>"
DB_NAME = "<DB_NAME>"
DB_USER = "<DB_USER>"
DB_PASSWORD = "<DB_PASSWORD>"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

RETRY_STATUS = {502, 503, 504}  # 대표적인 일시 장애


# 공공데이터포털 API 재요청 함수
def get_with_retry(
    url: str,
    params: dict,
    *,
    timeout: int = 10,
    retries: int = 3,
    base_sleep: float = 2.0,
    jitter: float = 0.5,
) -> requests.Response:

    last_exc = None

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)

            # 429: 레이트리밋 -> Retry-After 우선
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else base_sleep * attempt
                sleep_s += random.uniform(0, jitter)
                print(f"[retry {attempt}/{retries}] 429 Too Many Requests → {sleep_s:.1f}s 대기 후 재시도")
                time.sleep(sleep_s)
                continue

            # 502/503/504: 서버/게이트웨이 장애 -> 재시도
            if resp.status_code in RETRY_STATUS:
                sleep_s = base_sleep * attempt + random.uniform(0, jitter)
                print(f"[retry {attempt}/{retries}] {resp.status_code} → {sleep_s:.1f}s 대기 후 재시도")
                time.sleep(sleep_s)
                continue

            # 그 외 4xx는 보통 요청/키/권한 문제 -> 즉시 종료
            if 400 <= resp.status_code < 500:
                # 본문에 공공데이터포털 에러 메시지가 들어오는 경우가 많아서 같이 출력
                try:
                    body = resp.text[:300]
                except Exception:
                    body = ""
                raise RuntimeError(f"HTTP {resp.status_code} (재시도 안 함). response: {body}")

            # 정상/그 외 상태는 raise_for_status로 체크
            resp.raise_for_status()
            return resp

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_exc = e
            sleep_s = base_sleep * attempt + random.uniform(0, jitter)
            print(f"[retry {attempt}/{retries}] 네트워크/타임아웃 → {sleep_s:.1f}s 대기 후 재시도: {e}")
            time.sleep(sleep_s)

        except requests.exceptions.HTTPError as e:
            # resp.raise_for_status()에서 올라오는 5xx 등
            last_exc = e
            # 5xx면 재시도, 그 외면 종료
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status and status >= 500 and attempt < retries:
                sleep_s = base_sleep * attempt + random.uniform(0, jitter)
                print(f"[retry {attempt}/{retries}] HTTPError {status} → {sleep_s:.1f}s 대기 후 재시도")
                time.sleep(sleep_s)
            else:
                raise

    # 여기까지 오면 전부 실패 -> 종료
    raise RuntimeError(f"요청 실패: {retries}회 재시도 모두 실패. 마지막 예외: {last_exc}")


# nx, ny 변환 함수
def latlon_to_grid(lat, lng):
    RE = 6371.00877
    GRID = 5.0
    SLAT1 = 30.0
    SLAT2 = 60.0
    OLON = 126.0
    OLAT = 38.0
    XO = 43
    YO = 136

    DEGRAD = math.pi / 180.0

    re = RE / GRID
    slat1 = SLAT1 * DEGRAD
    slat2 = SLAT2 * DEGRAD
    olon = OLON * DEGRAD
    olat = OLAT * DEGRAD

    sn = math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5)
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(sn)

    sf = math.tan(math.pi * 0.25 + slat1 * 0.5)
    sf = math.pow(sf, sn) * math.cos(slat1) / sn

    ro = math.tan(math.pi * 0.25 + olat * 0.5)
    ro = re * sf / math.pow(ro, sn)

    ra = math.tan(math.pi * 0.25 + lat * DEGRAD * 0.5)
    ra = re * sf / math.pow(ra, sn)

    theta = lng * DEGRAD - olon
    theta = math.atan2(math.sin(theta), math.cos(theta))
    theta *= sn

    x = int(ra * math.sin(theta) + XO + 0.5)
    y = int(ro - ra * math.cos(theta) + YO + 0.5)

    return x, y


# 계절을 기준으로 낮/밤 시간 기준 설정
def is_daytime_by_season(dt: datetime) -> bool:
    month = dt.month
    hour = dt.hour

    if month in (3, 4, 5, 9, 10, 11):
        return 6 <= hour <= 18
    if month in (6, 7, 8):
        return 5 <= hour < 20
    return 8 <= hour <= 17


# PTY로 날씨 결정
def map_icon_key_by_pty(pty: str, rn1: str):
    if pty in ("1", "5"):
        return map_icon_key_by_rain(rn1)
    elif pty in ("2", "6"):
        return "눈비"
    elif pty in ("3", "7"):
        return "눈"
    elif pty == "4":
        return "소나기"
    return None


# 강우량에 따른 날씨 결정
def map_icon_key_by_rain(rn1: str):
    if not rn1:
        return None

    m = re.search(r'\d+(\.\d+)?', rn1)
    if not m:
        return None

    mm = float(m.group())
    if mm < 3.0:
        return "약한비"
    elif mm < 15.0:
        return "비"
    else:
        return "강한비"


# PTY가 0일 때, 구름상태에 따라 날씨 결정
def map_icon_key_by_sky(sky: str):
    if sky == "1":
        return "맑음"
    elif sky == "3":
        return "구름많음"
    elif sky == "4":
        return "흐림"
    return "맑음"


# 날씨 아이콘 결정
def get_weather_icon_key(pty: str, sky: str, lgt: str, rn1: str):
    if lgt:
        m = re.search(r'\d+(\.\d+)?', lgt)
        if m and float(m.group()) != 0.0:
            return "번개,뇌우"

    icon_by_pty = map_icon_key_by_pty(pty, rn1)
    if icon_by_pty:
        return icon_by_pty

    return map_icon_key_by_sky(sky)


# 아이콘 최종 매핑
def get_weather_icon_filename(pty: int, sky: int, lgt: str, rn1: str, icon_map: dict):
    icon_key = get_weather_icon_key(pty, sky, lgt, rn1)

    icon_folder = "day" if IS_DAYTIME_NOW else "night"

    icons = icon_map.get("icons", {})
    icon_set = icons.get(icon_folder, {})

    return icon_set.get(icon_key)


def crawl():
    rows = []

    for city, info in regions.items():
        lat = info["lat"]
        lng = info["lng"]
        province = info.get("province")

        nx, ny = latlon_to_grid(lat, lng)

        params = {
            "ServiceKey": SERVICE_KEY,
            "pageNo": 1,
            "numOfRows": 100,
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": nx,
            "ny": ny
        }

        response = get_with_retry(BASE_URL, params, timeout=10, retries=3)

        items = response.json()["response"]["body"]["items"]["item"]

        fcst_by_time = defaultdict(dict)

        for item in items:
            fcst_time = item["fcstTime"]
            fcst_by_time[fcst_time][item["category"]] = item["fcstValue"]

        base_hour = base_dt.hour
        target_fcst_times = [
            f"{(base_hour + i + 1) % 24:02d}00"
            for i in range(7)   # 6시간 후 까지
        ]

        for idx, fcst_time in enumerate(target_fcst_times, start=1):
            weather_raw = fcst_by_time.get(fcst_time)
            if not weather_raw:
                continue

            fcst_hour = idx

            pty = weather_raw.get("PTY")
            sky = weather_raw.get("SKY")
            lgt = weather_raw.get("LGT")

            rn1 = weather_raw.get("RN1")
            if rn1 in ("강수없음", None, "", "0"):
                rn1 = "0mm"

            row = {
                "city": city,
                "province": province,
                "lat": lat,
                "lng": lng,
                "basetime": f"{base_date}{base_time}",
                "fcsthour": fcst_hour,
                "icon_map": get_weather_icon_filename(pty, sky, lgt, rn1, icon_map),
                "PTY": weather_raw.get("PTY"),
                "SKY": sky,
                "T1H": float(weather_raw.get("T1H")),
                "REH": int(weather_raw.get("REH")),
                "RN1": rn1,
                "LGT": lgt,
                "VEC": int(weather_raw.get("VEC")),
                "WSD": float(weather_raw.get("WSD")),
                "UUU": float(weather_raw.get("UUU")),
                "VVV": float(weather_raw.get("VVV")),
            }

            rows.append(row)

        print(f"완료: {city}")
        time.sleep(1.0)

    return pd.DataFrame(rows)


def save_parquet_to_gcs(df: pd.DataFrame):
    """
    pandas DataFrame을 Parquet으로 변환하여 GCS에 업로드
    """

    client = storage.Client.from_service_account_json(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    bucket = client.bucket(BUCKET_NAME)

    file_name = f"{base_date}{base_time}_weather_ultraShortFsct.parquet"
    blob_path = f"{GCS_PATH}/{file_name}"

    blob = bucket.blob(blob_path)

    # pandas → pyarrow Table
    table = pa.Table.from_pandas(df)

    # 메모리 버퍼
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # 업로드
    blob.upload_from_file(
        buffer,
        content_type="application/octet-stream"
    )

    print(f"GCS parquet 업로드 완료: gs://{BUCKET_NAME}/{blob_path}")


def ultra_fcst_to_db(df):
    """초단기예보 최신 상태 갱신"""

    drop_cols = ["LGT", "UUU", "VVV"]
    df_insert = df.drop(columns=[c for c in drop_cols if c in df.columns])

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE weather_ultrashort_fcst;"))

    df_insert.to_sql(
        name="weather_ultrashort_fcst",
        con=engine,
        if_exists="append",
        index=False
    )

    print("weather_ultrashort_fcst 최신 데이터 갱신 완료")


if __name__ == "__main__":
    df = []

    # 현재 시각 기준 낮/밤 판정
    IS_DAYTIME_NOW = is_daytime_by_season(now)

    try:
        df = crawl()
    except Exception as e:
        print(f"{base_dt}, 초단기예보 크롤링 중 오류 발생: {e}")

    try:
        save_parquet_to_gcs(df)
    except Exception as e:
        print(f"{base_dt}, 초단기예보 Raw data GCS에 적재 중 오류 발생: {e}")

    try:
        ultra_fcst_to_db(df)
    except Exception as e:
        print(f"{base_dt}, 초단기예보 데이터 DB에 적재 중 오류 발생: {e}")
