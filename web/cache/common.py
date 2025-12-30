import os
import redis
from sqlalchemy import create_engine, text

_redis_client = None
_engine = None


def get_redis_client():
    global _redis_client

    if _redis_client is None:
        _redis_client = redis.Redis(
            host=os.environ.get("REDIS_HOST", "<REDIS_HOST>"),
            port=int(os.environ.get("REDIS_PORT", "<REDIS_PORT>")),
            decode_responses=True
        )
    return _redis_client


def get_engine():
    global _engine

    if _engine is None:
        DB_HOST = os.getenv("DB_HOST", "<DB_HOST>")
        DB_PORT = os.getenv("DB_PORT", "<DB_PORT>")
        DB_NAME = os.getenv("DB_NAME", "<DB_NAME>")
        DB_USER = os.getenv("DB_USER", "<DB_USER>")
        DB_PASSWORD = os.getenv("DB_PASSWORD", "<DB_PASSWORD>")

        _engine = create_engine(
            f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
    return _engine


def fetch_geojson_from_view(view_name, where_clause="", params=None):
    sql = f"""
    SELECT jsonb_build_object(
        'type', 'FeatureCollection',
        'features', COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(geom)::jsonb,
                    'properties', to_jsonb(t) - 'geom'
                )
            ),
            '[]'::jsonb
        )
    )
    FROM {view_name} t
    {where_clause};
    """

    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text(sql), params or {}).scalar()