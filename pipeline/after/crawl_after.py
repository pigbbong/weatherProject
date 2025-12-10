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

BASE_DIR = "/opt/pipelines/after"
ICON_DIR = "/opt/pipelines/icons"
region_json_path = "/opt/pipelines/region.json"
icon_map_path = "/opt/pipelines/icon_mapping.json"

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

now_hour = now_kst.hour

today_remain = 23 - now_hour  # 오늘 남은 시간
tomorrow_slicing = today_remain + 1  # 내일 0시가 시작 인덱스
dayafter_slicing = tomorrow_slicing + 24     # 모레 0시 시작 인덱스

# DataFrame 컬럼
columns = [
    "year", "month", "day", "hour", "weekday",
    "forecast", "province", "city",
    "temperature_am", "status_am", "fine_dust_am", "ultra_fine_dust_am", "rain_prob_am", "max_rain_mm_am", "icon_am",
    "temperature_pm", "status_pm", "fine_dust_pm", "ultra_fine_dust_pm", "rain_prob_pm", "max_rain_mm_pm", "icon_pm",
    "lat", "lng"
]

# GCS 설정
SERVICE_ACCOUNT_KEY = "/opt/keys/service-account.json"
BUCKET_NAME = "pigbbong_weather"


def scrape_weather_tomorrow(city_name):
    forecast = "내일"

    try:
        response = requests.get(
            f"https://search.naver.com/search.naver?query={city_name}+날씨",
            headers=HEADERS,
            timeout=5
        )

        if response.status_code != 200:
            print(f"요청 실패({city_name}): HTTP {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        tomorrow_block = soup.select_one("div.weather_info.type_tomorrow")
        if tomorrow_block is None:
            print(f"{city_name}: 내일 예보 블록 없음")
            return None

        # 강수량
        try:
            rain_values = soup.select("div.precipitation_graph_box ul li div.data_inner")

            rain_amounts = []
            for div in rain_values:
                raw = div.get_text(strip=True)
                if raw == "~1":
                    raw = 1  # '~1'은 1로 처리
                try:
                    rain_amounts.append(float(raw))
                except:
                    rain_amounts.append(0.0)

            # 내일 24시간 추출 구간
            start = tomorrow_slicing
            end = tomorrow_slicing + 24

            tomorrow_24h = rain_amounts[start:end]

            # 오전(0~11시), 오후(12~23시)
            max_rain_am = max(tomorrow_24h[:12]) if len(tomorrow_24h) >= 12 else None
            max_rain_pm = max(tomorrow_24h[12:24]) if len(tomorrow_24h) >= 24 else None

        except Exception as e:
            print(f"현재시간 {now}, {city_name} 내일 강수량 추출 에러: {e}")
            max_rain_am = None
            max_rain_pm = None

        # 미세먼지, 초미세먼지
        try:
            dust_block = tomorrow_block.select("ul.today_chart_list li.item_today")

            fine_dust_am = None
            ultra_fine_dust_am = None
            fine_dust_pm = None
            ultra_fine_dust_pm = None

            for li in dust_block:
                title = li.select_one("strong.title").get_text(strip=True)
                value = li.select_one("span.txt").get_text(strip=True)

                if title == "오전미세":
                    fine_dust_am = value
                elif title == "오전초미세":
                    ultra_fine_dust_am = value
                elif title == "오후미세":
                    fine_dust_pm = value
                elif title == "오후초미세":
                    ultra_fine_dust_pm = value

        except Exception as e:
            print(f"현재시간 {now}, {city_name} 내일 미세먼지 추출 에러: {e}")
            fine_dust_am = None
            ultra_fine_dust_am = None
            fine_dust_pm = None
            ultra_fine_dust_pm = None

        def extract_basic(block):
            """나머지 항목 추출"""

            # 기온
            try:
                temp = block.select_one("div.temperature_text strong")
                temperature = temp.find(string=True, recursive=False).strip()
            except Exception as e:
                print(f"현재시간 {now}, {city_name} 내일 기온 추출 에러: {e}")
                temperature = None

            # 날씨 상태
            try:
                status = block.select_one("p.summary").get_text(strip=True)
            except Exception as e:
                print(f"현재시간 {now}, {city_name} 내일 날씨 상태 추출 에러: {e}")
                status = None

            # 강수확률
            try:
                rain_prob = block.select_one("dl.summary_list dd.desc").get_text(strip=True)
            except Exception as e:
                print(f"현재시간 {now}, {city_name} 내일 강수확률 추출 에러: {e}")
                rain_prob = None

            # 아이콘 생성
            if status:
                status_for_icon = status
                icon_folder = "day"  # 예보는 낮 아이콘 그대로
                fallback = icon_map["icons"][icon_folder]

                if "눈" in status_for_icon and "비" in status_for_icon:
                    icon_filename = fallback["눈비"]
                elif "눈" in status_for_icon:
                    icon_filename = fallback["눈"]
                elif "번개" in status_for_icon or "천둥" in status_for_icon:
                    icon_filename = fallback["번개"]
                elif "소나기" in status_for_icon:
                    icon_filename = fallback["소나기"]
                elif "비" in status_for_icon:
                    icon_filename = fallback["비"]
                elif "안개" in status_for_icon or "박무" in status_for_icon:
                    icon_filename = fallback["안개"]
                elif "황사" in status_for_icon:
                    icon_filename = fallback["황사"]
                elif "구름많" in status_for_icon:
                    icon_filename = fallback["구름많음"]
                elif "구름조금" in status_for_icon or "구름 조금" in status_for_icon:
                    icon_filename = fallback["구름조금"]
                elif "흐" in status_for_icon:
                    icon_filename = fallback["흐림"]
                else:
                    icon_filename = fallback["맑음"]

                icon = icon_filename
            else:
                icon = None

            return {
                "temperature": temperature,
                "status": status,
                "rain_prob": rain_prob,
                "icon": icon
            }

        # 지역 정보
        province = region_data[city_name]["province"]
        lat = region_data[city_name].get("lat")
        lng = region_data[city_name].get("lng")

        am_info = extract_basic(tomorrow_block.select_one("div.inner ._am, div.inner .am"))
        pm_info = extract_basic(tomorrow_block.select_one("div.inner ._pm, div.inner .pm"))

        return {
            "year": now_kst.year,
            "month": now_kst.month,
            "day": now_kst.day,
            "hour": now_kst.hour,
            "weekday": now_kst.strftime("%A"),

            "forecast": forecast,
            "province": province,
            "city": city_name,

            # 오전 데이터
            "temperature_am": am_info["temperature"],
            "status_am": am_info["status"],
            "fine_dust_am": fine_dust_am,
            "ultra_fine_dust_am": ultra_fine_dust_am,
            "rain_prob_am": am_info["rain_prob"],
            "max_rain_mm_am": max_rain_am,
            "icon_am": am_info["icon"],

            # 오후 데이터
            "temperature_pm": pm_info["temperature"],
            "status_pm": pm_info["status"],
            "fine_dust_pm": fine_dust_pm,
            "ultra_fine_dust_pm": ultra_fine_dust_pm,
            "rain_prob_pm": pm_info["rain_prob"],
            "max_rain_mm_pm": max_rain_pm,
            "icon_pm": pm_info["icon"],

            # 좌표
            "lat": lat,
            "lng": lng
        }

    except Exception as e:
        print(f"오류 발생({city_name}): {e}")
        return None


def scrape_weather_dayafter(city_name):
    forecast = "모레"

    try:
        response = requests.get(
            f"https://search.naver.com/search.naver?query={city_name}+날씨",
            headers=HEADERS,
            timeout=5
        )

        if response.status_code != 200:
            print(f"요청 실패({city_name}): HTTP {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        dayafter_block = soup.select_one("ul.weather_info_list._after_tomorrow, ul.weather_info_list_after_tomorrow")
        if dayafter_block is None:
            print(f"{city_name}: 모레 예보 블록 없음")
            return None

        # 강수량
        try:
            rain_values = soup.select("div.precipitation_graph_box ul li div.data_inner")

            rain_amounts = []
            for div in rain_values:
                raw = div.get_text(strip=True)
                if raw == "~1":
                    raw = 1  # '~1'은 1로 처리
                try:
                    rain_amounts.append(float(raw))
                except:
                    rain_amounts.append(0.0)

            # 내일 24시간 추출 구간
            start = dayafter_slicing
            end = dayafter_slicing + 24

            dayafter_24h = rain_amounts[start:end]

            # 오전(0~11시), 오후(12~23시)
            max_rain_am = max(dayafter_24h[:12]) if len(dayafter_24h) >= 12 else None
            max_rain_pm = max(dayafter_24h[12:24]) if len(dayafter_24h) >= 24 else None

        except Exception as e:
            print(f"현재시간 {now}, {city_name} 모레 강수량 추출 에러: {e}")
            max_rain_am = None
            max_rain_pm = None

        # 미세먼지, 초미세먼지
        try:
            dust_block = dayafter_block.select("ul.today_chart_list li.item_today")

            fine_dust_am = None
            ultra_fine_dust_am = None
            fine_dust_pm = None
            ultra_fine_dust_pm = None

            for li in dust_block:
                title = li.select_one("strong.title").get_text(strip=True)
                value = li.select_one("span.txt").get_text(strip=True)

                if title == "오전미세":
                    fine_dust_am = value
                elif title == "오전초미세":
                    ultra_fine_dust_am = value
                elif title == "오후미세":
                    fine_dust_pm = value
                elif title == "오후초미세":
                    ultra_fine_dust_pm = value

        except Exception as e:
            print(f"현재시간 {now}, {city_name} 모레 미세먼지 추출 에러: {e}")
            fine_dust_am = None
            ultra_fine_dust_am = None
            fine_dust_pm = None
            ultra_fine_dust_pm = None

        def extract_basic(block):
            """나머지 항목 추출"""

            # 기온
            try:
                temp = block.select_one("div.temperature_text strong")
                temperature = temp.find(string=True, recursive=False).strip()
            except Exception as e:
                print(f"현재시간 {now}, {city_name} 모레 기온 추출 에러: {e}")
                temperature = None

            # 날씨 상태
            try:
                status = block.select_one("p.summary").get_text(strip=True)
            except Exception as e:
                print(f"현재시간 {now}, {city_name} 모레 날씨 상태 추출 에러: {e}")
                status = None

            # 강수확률
            try:
                rain_prob = block.select_one("dl.summary_list dd.desc").get_text(strip=True)
            except Exception as e:
                print(f"현재시간 {now}, {city_name} 모레 강수확률 추출 에러: {e}")
                rain_prob = None

            # 아이콘 생성
            if status:
                status_for_icon = status
                icon_folder = "day"  # 예보는 낮 아이콘 그대로
                fallback = icon_map["icons"][icon_folder]

                if "눈" in status_for_icon and "비" in status_for_icon:
                    icon_filename = fallback["눈비"]
                elif "눈" in status_for_icon:
                    icon_filename = fallback["눈"]
                elif "번개" in status_for_icon or "천둥" in status_for_icon:
                    icon_filename = fallback["번개"]
                elif "소나기" in status_for_icon:
                    icon_filename = fallback["소나기"]
                elif "비" in status_for_icon:
                    icon_filename = fallback["비"]
                elif "안개" in status_for_icon or "박무" in status_for_icon:
                    icon_filename = fallback["안개"]
                elif "황사" in status_for_icon:
                    icon_filename = fallback["황사"]
                elif "구름많" in status_for_icon:
                    icon_filename = fallback["구름많음"]
                elif "구름조금" in status_for_icon or "구름 조금" in status_for_icon:
                    icon_filename = fallback["구름조금"]
                elif "흐" in status_for_icon:
                    icon_filename = fallback["흐림"]
                else:
                    icon_filename = fallback["맑음"]

                icon = icon_filename
            else:
                icon = None

            return {
                "temperature": temperature,
                "status": status,
                "rain_prob": rain_prob,
                "icon": icon
            }

        # 지역 정보
        province = region_data[city_name]["province"]
        lat = region_data[city_name].get("lat")
        lng = region_data[city_name].get("lng")

        am_info = extract_basic(dayafter_block.select_one("li .inner .am, li .inner ._am"))
        pm_info = extract_basic(dayafter_block.select_one("li .inner .pm, li .inner ._pm"))

        return {
            "year": now_kst.year,
            "month": now_kst.month,
            "day": now_kst.day,
            "hour": now_kst.hour,
            "weekday": now_kst.strftime("%A"),

            "forecast": forecast,
            "province": province,
            "city": city_name,

            # 오전 데이터
            "temperature_am": am_info["temperature"],
            "status_am": am_info["status"],
            "fine_dust_am": fine_dust_am,
            "ultra_fine_dust_am": ultra_fine_dust_am,
            "rain_prob_am": am_info["rain_prob"],
            "max_rain_mm_am": max_rain_am,
            "icon_am": am_info["icon"],

            # 오후 데이터
            "temperature_pm": pm_info["temperature"],
            "status_pm": pm_info["status"],
            "fine_dust_pm": fine_dust_pm,
            "ultra_fine_dust_pm": ultra_fine_dust_pm,
            "rain_prob_pm": pm_info["rain_prob"],
            "max_rain_mm_pm": max_rain_pm,
            "icon_pm": pm_info["icon"],

            # 좌표
            "lat": lat,
            "lng": lng
        }

    except Exception as e:
        print(f"오류 발생({city_name}): {e}")
        return None


def upload_csv_to_gcs(df, bucket_name, gcs_path):
    """DataFrame을 CSV 형태로 GCS에 업로드(Parquet이 좋지만, 한번에 추출하는 데이터가 너무 적어서 CSV로 저장해도 문제 없음)"""

    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    csv_buffer = io.StringIO()  # CSV를 메모리에 저장 (BytesIO)
    df.to_csv(csv_buffer, index=False, encoding="utf-8")

    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    print(f"GCS CSV 업로드 성공: gs://{bucket_name}/{gcs_path}")


def main():
    df = pd.DataFrame(columns=columns)

    # 내일 날씨 데이터 크롤링
    for city_name in region_data.keys():
        row = scrape_weather_tomorrow(city_name)
        if row:
            df.loc[len(df)] = row
            print(f"내일 {city_name} 추출 완료")
        else:
            print(f"내일 {city_name} 추출 실패")
        time.sleep(0.35)

    # 모레 날씨 데이터 크롤링
    for city_name in region_data.keys():
        row = scrape_weather_dayafter(city_name)
        if row:
            df.loc[len(df)] = row
            print(f"모레 {city_name} 추출 완료")
        else:
            print(f"모레 {city_name} 추출 실패")
        time.sleep(0.35)

    # CSV 파일명 생성
    file_name = f"raw/after/current/{now_kst.strftime('%Y%m%d%H%M')}_weather_after.csv"

    # CSV 업로드
    upload_csv_to_gcs(df, BUCKET_NAME, file_name)


if __name__ == "__main__":
    main()
