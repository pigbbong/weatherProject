import json
from datetime import datetime, timedelta
from common import get_redis_client, fetch_geojson_from_view

redis_client = get_redis_client()

SHORT_VIEWS = {
    "cond":  "weather_shortfcst_cond_view",
    "rain":  "weather_shortfcst_rain_view",
    "wind":  "weather_shortfcst_wind_view",
    "humid": "weather_shortfcst_humid_view",
}

DATE_GROUPS = {
    "today": 0,
    "tomorrow": 1,
    "dayafter": 2,
    "twodaysafter": 3,
}

TTL = 21600  # 6시간


def main():
    today = datetime.now().date()

    for group, offset in DATE_GROUPS.items():
        fcstdate = (today + timedelta(days=offset)).strftime("%Y%m%d")

        for layout, view in SHORT_VIEWS.items():
            data = fetch_geojson_from_view(
                view,
                where_clause="WHERE fcstdate = :fcstdate",
                params={"fcstdate": fcstdate}
            )
            redis_client.setex(
                f"short:{group}:{layout}",
                TTL,
                json.dumps(data)
            )


if __name__ == "__main__":
    main()
