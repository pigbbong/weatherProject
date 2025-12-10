import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import json
from google.cloud import storage
import io
from zoneinfo import ZoneInfo
import time

# KST 시간 기준
now_kst = datetime.now(ZoneInfo("Asia/Seoul"))

BASE_DIR = "/opt/pipelines/now"
ICON_DIR = "/opt/pipelines/icons"
region_json_path = "/opt/pipelines/region.json"
icon_map_path = "/opt/pipelines/icon_mapping.json" 
GOOGLE_CREDENTIAL = "/opt/keys/service-account.json"

# 브라우저 UA
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://www.google.com/",
    "sec-ch-ua": "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# 지역 데이터 로드
with open(region_json_path, "r", encoding="utf-8") as f:
    region_data = json.load(f)

# 날씨 아이콘 데이터 로드
with open(icon_map_path, "r", encoding="utf-8") as f:
    icon_map = json.load(f)

# DataFrame 컬럼
columns = [
    "year", "month", "day", "hour", "minute",  "weekday",
    "province", "city", "temperature", "status", "rain_mm",
    "humidity", "wind_direction", "wind_speed",
    "fine_dust", "ultra_fine_dust", "uv",
    "lat", "lng",
    "icon"
]

# GCS 설정
SERVICE_ACCOUNT_KEY = "/opt/keys/service-account.json"
BUCKET_NAME = "pigbbong_weather"


def get_sun_info():
    try:
        response = requests.get(
            "https://search.naver.com/search.naver?query=서울+날씨",
            headers=HEADERS,
            timeout=5
        )
        soup = BeautifulSoup(response.text, "html.parser")

        sun_status = soup.select_one("li.item_today.type_sun strong.title").get_text(strip=True)

        # title이 "일몰"이면 지금은 'd' (해 있는 시간)
        if sun_status == "일몰":
            return "d"
        else:
            return "n"

    except Exception as e:
        print(f"밤/낮 정보 추출 실패: {e}")
        return "d"   # 기본값: 낮


def scrape_weather(city_name, sun_info):
    try:
        response = requests.get(
            f"https://search.naver.com/search.naver?query={city_name}+현재+날씨",
            headers=HEADERS,
            timeout=5
        )

        if response.status_code != 200:
            print(f"요청 실패({city_name}): HTTP {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        weather_data = soup.find("div", {"class": "weather_info"})

        if weather_data is None:
            print(f"{city_name}: weather_info 없음")
            return None

        # 기온
        try:
            temperature = weather_data.select_one("div.temperature_text strong") \
                                      .find(string=True, recursive=False).strip()
        except Exception as e:
            print(f"{city_name}의 현재 기온 추출 실패: {e}")
            temperature = None

        # 날씨 상태
        try:
            status = weather_data.select_one("span.weather.before_slash") \
                                 .get_text(strip=True)
        except Exception as e:
            print(f"{city_name}의 현재 날씨 상태 추출 실패: {e}")
            status = None

        # 강수량
        try:
            rain_dt = soup.find("dt", class_="term", string=lambda t: t and t.strip() == "강수")  # "강수" 항목(dt.term)만 정확히 선택

            if rain_dt:
                raw = rain_dt.find_next_sibling("dd", class_="desc").get_text(strip=True)
                rain_mm = float(raw.replace("mm", "").strip())
            else:
                rain_mm = 0.0

        except Exception as e:
            print(f"{city_name}: 현재 강수량 추출 실패: {e}")
            rain_mm = None

        # 습도, 풍향, 풍속
        humidity = wind_direction = wind_speed = None
        for item in weather_data.select("dl.summary_list div.sort"):
            try:
                term = item.select_one("dt.term").get_text(strip=True)
                value = item.select_one("dd.desc").get_text(strip=True)

                if term == "습도":
                    humidity = value
                elif value.endswith("m/s"):
                    wind_direction = term
                    wind_speed = value
            except Exception as e:
                print(f"{city_name}의 현재 습도, 바람 정보 추출 실패: {e}")
                continue

        # 미세먼지, 초미세먼지, UV
        fine_dust = ultra_fine_dust = uv = None
        for li in weather_data.select("ul.today_chart_list li"):
            try:
                title_tag = li.select_one("strong.title")
                grade_tag = li.select_one("span.txt")
                if not title_tag or not grade_tag:
                    continue

                title = title_tag.get_text(strip=True)
                grade = grade_tag.get_text(strip=True)

                if title == "미세먼지":
                    fine_dust = grade
                elif title == "초미세먼지":
                    ultra_fine_dust = grade
                elif title == "자외선":
                    uv = grade
            except Exception as e:
                print(f"{city_name}의 현재 대기질 항목 추출 실패: {e}")
                continue

        # 지역 정보
        province = region_data[city_name]["province"]
        lat = region_data[city_name].get("lat")
        lng = region_data[city_name].get("lng")

        # 아이콘
        if status:
            icon_folder = "day" if sun_info == 'd' else "night"
            fallback = icon_map["icons"][icon_folder]  # 아이콘 파일 전체 가져오기

            # 우선순위 기반 날씨 아이콘 매칭
            if "눈" in status and "비" in status:
                icon_filename = fallback["눈비"]

            elif "눈" in status:
                icon_filename = fallback["눈"]

            elif ("번개" in status) or ("천둥" in status) or ("뇌우" in status):
                icon_filename = fallback["번개"]

            elif "소나기" in status:
                icon_filename = fallback["소나기"]

            elif "비" in status:
                icon_filename = fallback["비"]

            elif ("안개" in status) or ("박무" in status):
                icon_filename = fallback["안개"]

            elif "황사" in status:
                icon_filename = fallback["황사"]

            elif "구름많" in status:
                icon_filename = fallback["구름많음"]

            elif ("구름조금" in status) or ("구름 조금" in status):
                icon_filename = fallback["구름조금"]

            elif "흐" in status:
                icon_filename = fallback["흐림"]

            else:
                icon_filename = fallback["맑음"]

            icon = icon_filename

        else:
            icon = None

        return {
            "year": now_kst.year,
            "month": now_kst.month,
            "day": now_kst.day,
            "hour": now_kst.hour,
            "minute": now_kst.minute,
            "weekday": now_kst.strftime("%A"),
            "province": province,
            "city": city_name,
            "temperature": temperature,
            "status": status,
            "rain_mm": rain_mm,
            "humidity": humidity,
            "wind_direction": wind_direction,
            "wind_speed": wind_speed,
            "fine_dust": fine_dust,
            "ultra_fine_dust": ultra_fine_dust,
            "uv": uv,
            "lat": lat,
            "lng": lng,
            "icon": icon
        }

    except Exception as e:
        print(f"오류 발생 ({city_name}): {e}")
        return None


def upload_csv_to_gcs(df, bucket_name, gcs_path):
    """DataFrame을 CSV 형태로 GCS에 업로드(Parquet이 좋지만, 한번에 추출하는 데이터가 너무 적어서 CSV로 저장해도 문제 없음)"""

    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    csv_buffer = io.StringIO()  # CSV를 메모리에 저장
    df.to_csv(csv_buffer, index=False, encoding="utf-8")

    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    print(f"GCS CSV 업로드 성공: gs://{bucket_name}/{gcs_path}")


def main():
    df = pd.DataFrame(columns=columns)

    # 밤/낮 확인
    sun_info = get_sun_info()

    # 전체 지역 반복
    for city_name in region_data.keys():
        row = scrape_weather(city_name, sun_info)
        if row:
            df.loc[len(df)] = row
            print(f"완료: {city_name}")
        else:
            print(f"실패: {city_name}")
        time.sleep(0.35)

    # CSV 파일명 생성
    file_name = f"raw/now/current/{now_kst.strftime('%Y%m%d%H%M')}_weather_now.csv"

    # CSV 업로드
    upload_csv_to_gcs(df, BUCKET_NAME, file_name)


if __name__ == "__main__":
    main()
