import json
from common import get_redis_client, fetch_geojson_from_view

redis_client = get_redis_client()

ULTRASHORT_VIEWS = {
    "cond":  "weather_ultrashortfcst_cond_view",
    "rain":  "weather_ultrashortfcst_rain_view",
    "wind":  "weather_ultrashortfcst_wind_view",
    "humid": "weather_ultrashortfcst_humid_view",
}

TTL = 7200  # 2시간


def main():
    for layout, view in ULTRASHORT_VIEWS.items():
        data = fetch_geojson_from_view(view)
        redis_client.setex(f"ultrashort:{layout}", TTL, json.dumps(data))


if __name__ == "__main__":
    main()
