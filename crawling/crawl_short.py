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

BASE_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"

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

# KST로 설정
KST = timezone(timedelta(hours=9))
now = datetime.now(KST)

# GCS 설정
KEY_PATH = "<SERVICE_ACCOUNT_JSON_PATH>"
BUCKET_NAME = "<GCS_BUCKET_NAME>"
GCS_PATH = "raw/weather/shortFcst"

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


# PTY로 날씨 결정
def map_icon_key_by_pty(pty: str, pcp: str, sno: str):
    if pty == '0':
        return None
    if pty == '1':
        return map_icon_key_by_rain(pcp)
    elif pty == '2':
        return "눈비"
    elif pty == '3':
        return map_icon_key_by_snow(sno)
    elif pty == '4':
        return "소나기"
    else:
        return None


# 강우량에 따른 날씨 결정
def map_icon_key_by_rain(pcp: str):
    if not pcp:
        return None

    m = re.search(r'\d+(\.\d+)?', pcp)
    if not m:
        return None

    mm = float(m.group())

    if mm < 3.0:
        return "약한비"
    elif mm < 15.0:
        return "비"
    else:
        return "강한비"


# 강설량에 따른 날씨 결정
def map_icon_key_by_snow(sno: str):
    if not sno:
        return None

    m = re.search(r'\d+(\.\d+)?', sno)
    if not m:
        return None

    cm = float(m.group())

    if cm < 1.0:
        return "약한눈"
    elif cm < 5.0:
        return "눈"
    else:
        return "강한눈"


# PTY가 0일 때, 구름상태에 따라 날씨 결정
def map_icon_key_by_sky(sky: int):
    if sky == '1':
        return "맑음"
    elif sky == '3':
        return "구름많음"
    elif sky == '4':
        return "흐림"
    return "맑음"


# 날씨 아이콘 결정
def get_weather_icon_key(pty: int, sky: int, pcp: str, sno: str):
    icon_by_pty = map_icon_key_by_pty(pty, pcp, sno)
    if icon_by_pty:
        return icon_by_pty

    return map_icon_key_by_sky(sky)


# 아이콘 최종 매핑
def get_weather_icon_filename(pty: int, sky: int, pcp: str, sno: str, icon_map: dict):
    icon_key = get_weather_icon_key(pty, sky, pcp, sno)
    icon_folder = "day"

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
            "numOfRows": 1100,
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": nx,
            "ny": ny
        }

        response = get_with_retry(BASE_URL, params, timeout=10, retries=3)

        items = response.json()["response"]["body"]["items"]["item"]

        fcst_by_datetime = defaultdict(dict)

        for item in items:
            fcst_date = item["fcstDate"]
            fcst_time = item["fcstTime"]

            key = (fcst_date, fcst_time)
            fcst_by_datetime[key][item["category"]] = item["fcstValue"]

        for (fcst_date, fcst_time), weather_raw in fcst_by_datetime.items():
            pty = weather_raw.get("PTY")
            sky = weather_raw.get("SKY")

            pcp = weather_raw.get("PCP")
            if pcp in ("강수없음", None, "", "0"):
                pcp = "0mm"

            sno = weather_raw.get("SNO")
            if sno in ("적설없음", None, "", "0"):
                sno = "0cm"

            row = {
                "city": city,
                "province": province,
                "lat": lat,
                "lng": lng,
                "basetime": f"{base_date}{base_time}",
                "fcstdate": fcst_date,
                "fcsthour": fcst_time,
                "icon_map": get_weather_icon_filename(pty, sky, pcp, sno, icon_map),
                "PTY": pty,
                "SKY": sky,
                "TMP": float(weather_raw.get("TMP")),
                "REH": int(weather_raw.get("REH")),
                "PCP": pcp,
                "SNO": sno,
                "POP": int(weather_raw.get("POP")),
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

    file_name = f"{base_date}{base_time}_weather_shortFcst.parquet"
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


def short_fcst_to_db(df):
    """단기예보 최신 상태 갱신 (오늘~모레까지만 저장)"""

    if df.empty:
        print("저장할 데이터가 없습니다.")
        return

    # 날짜 컬럼 datetime 변환
    df["fcstdate_dt"] = pd.to_datetime(df["fcstdate"], format="%Y%m%d")

    # 기준일 (basetime 기준 날짜)
    base_date_dt = pd.to_datetime(df["basetime"].iloc[0][:8], format="%Y%m%d")

    # 글피까지
    end_date_dt = base_date_dt + pd.Timedelta(days=3)

    # 필터링
    df_filtered = df[df["fcstdate_dt"] <= end_date_dt].copy()

    # 불필요 컬럼 제거
    df_filtered.drop(columns=["fcstdate_dt"], inplace=True)

    drop_cols = ["LGT", "UUU", "VVV"]
    df_insert = df_filtered.drop(
        columns=[c for c in drop_cols if c in df_filtered.columns]
    )

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE weather_short_fcst;"))

    df_insert.to_sql(
        name="weather_short_fcst",
        con=engine,
        if_exists="append",
        index=False
    )

    print("weather_short_fcst 최신 데이터 갱신 완료")


if __name__ == "__main__":
    df = None
    
    try:
        df = crawl()
    except Exception as e:
        print(f"단기예보 크롤링 중 오류 발생 (이번 회차 스킵): {e}")
        df = None

    if df is None or df.empty:
        print("이번 회차 단기예보 데이터 없음 → 정상 종료")
        exit(0)

    save_parquet_to_gcs(df)
    short_fcst_to_db(df)
