from flask import Flask, render_template, redirect, abort, send_from_directory
import os
import json
import psycopg2
import requests
from bs4 import BeautifulSoup

app = Flask(__name__, static_folder="static", template_folder="templates")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_CONFIG = {
    "host": "<EC2_PUBLIC_IPv4>",
    "dbname": "<POSTGRES_DB_NAME>",
    "user": "<POSTGRES_USER>",
    "password": "<POSTGRES_PASSWORD>",
    "port": "<POSTGRES_PORT>"
}

# 아이콘 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
icon_map_path = os.path.join(BASE_DIR, "icon_mapping.json")

# 날씨 아이콘 데이터 로드
with open(icon_map_path, "r", encoding="utf-8") as f:
    icon_map = json.load(f)


# 네이버 실시간 크롤링 함수
def scrape_current_weather():
    url = "https://search.naver.com/search.naver?query=현재+날씨"

    # 브라우저 UA
    headers = {
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

    try:
        res = requests.get(url, headers=headers, timeout=2)
        soup = BeautifulSoup(res.text, "html.parser")
    except:
        return None

    # 위치
    try:
        location = soup.select_one("h2.title").get_text(strip=True)
    except:
        location = None

    # 현재 기온
    try:
        temperature = soup.select_one("div.temperature_text strong")\
                          .find(string=True, recursive=False).strip()
    except:
        temperature = None

    # 날씨 상태(맑음/흐림 등)
    try:
        status = soup.select_one("span.weather.before_slash").get_text(strip=True)
    except:
        status = None

    # 체감온도
    try:
        feel_text = soup.select_one("dl.summary_list dd.desc").get_text(strip=True)
        feel = feel_text.replace("체감", "").strip()
    except:
        feel = None

    # 습도, 강수, 풍속, 풍향
    humidity = rainfall = wind_direction = wind_speed = None
    try:
        items = soup.select("dl.summary_list div.sort")
        for item in items:
            term = item.select_one("dt.term").get_text(strip=True)
            value = item.select_one("dd.desc").get_text(strip=True)

            if term == "습도":
                humidity = value
            elif term == "강수":
                rainfall = value
            elif value.endswith("m/s"):
                wind_direction = term
                wind_speed = value
    except:
        pass

    # 낮/밤 판정
    try:
        sun_status = soup.select_one("li.item_today.type_sun strong.title")\
                          .get_text(strip=True)
        sun_info = "d" if sun_status == "일몰" else "n"
    except:
        sun_info = "d"

    # 아이콘 선택
    icon = None
    if status:
        icon_folder = "day" if sun_info == 'd' else "night"
        fallback = icon_map["icons"][icon_folder]

        if "눈" in status and "비" in status:
            icon = fallback["눈비"]
        elif "눈" in status:
            icon = fallback["눈"]
        elif "번개" in status or "천둥" in status or "뇌우" in status:
            icon = fallback["번개"]
        elif "소나기" in status:
            icon = fallback["소나기"]
        elif "비" in status:
            icon = fallback["비"]
        elif "안개" in status or "박무" in status:
            icon = fallback["안개"]
        elif "황사" in status:
            icon = fallback["황사"]
        elif "구름많" in status:
            icon = fallback["구름많음"]
        elif "구름조금" in status or "구름 조금" in status:
            icon = fallback["구름조금"]
        elif "흐" in status:
            icon = fallback["흐림"]
        else:
            icon = fallback["맑음"]

    return {
        "location": location,
        "temperature": temperature,
        "status": status,
        "feel": feel,
        "humidity": humidity,
        "rainfall": rainfall,
        "wind_direction": wind_direction,
        "wind_speed": wind_speed,
        "icon": icon
    }


# "/"에서  "/now/cond" url로 이동
@app.route("/")
def index_root():
    return redirect("/now/cond")


# 모든 조합을 index.html로 렌더링
@app.route("/<group>/<layout>")
def index(group, layout):

    allowed_groups  = ["now", "tomorrow_am", "tomorrow_pm", "dayafter_am", "dayafter_pm"]
    allowed_layouts = ["cond", "dust", "detail", "rain", "score"]

    if group not in allowed_groups or layout not in allowed_layouts:
        abort(404)

    return render_template("index.html", INITIAL_GROUP=group, INITIAL_LAYOUT=layout)


# icons 정적 제공
@app.route("/icons/<path:filename>")
def serve_icons(filename):
    return send_from_directory(os.path.join(BASE_DIR, "icons"), filename)


# API: /api/weather/<group>/<layout>
@app.route("/api/weather/<group>/<layout>")
def api_weather(group, layout):

    table_name = f"weather_{group}_{layout}_view"
    allowed_groups  = ["now", "tomorrow_am", "tomorrow_pm", "dayafter_am", "dayafter_pm"]
    allowed_layouts = ["cond", "dust", "detail", "score", "rain"]

    if group not in allowed_groups or layout not in allowed_layouts:
        abort(404)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    query = f"""
        SELECT jsonb_build_object(
            'type', 'FeatureCollection',
            'features', jsonb_agg(
                jsonb_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(geom)::jsonb,
                    'properties', to_jsonb(t) - 'geom'
                )
            )
        )
        FROM {table_name} t;
    """

    cur.execute(query)
    data = cur.fetchone()[0]

    cur.close()
    conn.close()

    return data


# 네이버 실시간 API
@app.route("/api/live_weather")
def api_live_weather():
    data = scrape_current_weather()
    if not data:
        return {"error": "크롤링 실패"}, 500
    return data


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
