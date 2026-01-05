import math
import json
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
import random

# 기본 설정
SERVICE_KEY = os.getenv(
    "SERVICE_KEY",
    "<SERVICE_KEY 입력>"
)

BASE_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

BASE_DIR = Path(os.getenv(
    "APP_BASE_PATH",
    Path(__file__).resolve().parent.parent
))

REGION_FILE = BASE_DIR / "region.json"

with open(REGION_FILE, "r", encoding="utf-8") as f:
    regions = json.load(f)

# KST 시간 설정
KST = timezone(timedelta(hours=9))
base_dt = datetime.now(KST).replace(minute=0, second=0, microsecond=0)
base_date = base_dt.strftime("%Y%m%d")
base_time = base_dt.strftime("%H") + "00"

# GCS 설정
KEY_PATH = "<SERVICE_ACCOUNT_JSON_PATH>"
BUCKET_NAME = "<GCS_BUCKET_NAME>"
GCS_PATH = "raw/weather/now"


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
            "numOfRows": 10,
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": nx,
            "ny": ny
        }

        response = get_with_retry(BASE_URL, params, timeout=10, retries=3)

        items = response.json()["response"]["body"]["items"]["item"]

        weather_raw = {i["category"]: i["obsrValue"] for i in items}

        rn1 = weather_raw.get("RN1", "0")
        if rn1 in ("강수없음", "적설없음", None, ""):
            rn1 = "0mm"

        row = {
            "city": city,
            "province": province,
            "lat": lat,
            "lng": lng,
            "basetime": f"{base_date}{base_time}",
            "PTY": weather_raw.get("PTY"),
            "T1H": float(weather_raw.get("T1H")),
            "REH": int(weather_raw.get("REH")),
            "RN1": rn1,
            "VEC": int(weather_raw.get("VEC")),
            "WSD": float(weather_raw.get("WSD")),
            "UUU": float(weather_raw.get("UUU")),
            "VVV": float(weather_raw.get("VVV"))
        }

        rows.append(row)
        print(f"완료: {city}")

        time.sleep(1.0)

    df = pd.DataFrame(rows)
    return df


def save_parquet_to_gcs(df: pd.DataFrame):
    """
    pandas DataFrame을 Parquet으로 변환하여 GCS에 업로드
    """

    client = storage.Client.from_service_account_json(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    bucket = client.bucket(BUCKET_NAME)

    file_name = f"{base_date}{base_time}_weather_now.parquet"
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


if __name__ == "__main__":
    df = None
    
    try:
        df = crawl()
    except Exception as e:
        print(f"초단기실황 크롤링 중 오류 발생: {e}")
        df = None
    
    if df is None or df.empty:
        print("데이터 없음 → 이번 실행 스킵")
    else:
        save_parquet_to_gcs(df)
