from flask import Flask, jsonify, render_template, redirect, abort
from cache.common import get_redis_client
import json

app = Flask(__name__)

VALID_LAYOUTS = ["cond", "rain", "wind", "humid"]
VALID_GROUPS = ["today", "tomorrow", "dayafter", "twodaysafter"]


def get_cache():
    return get_redis_client()


# 화면 라우팅
@app.route("/")
def root():
    return redirect("/ultrashort/cond")


@app.route("/ultrashort/<layout>")
def page_ultrashort(layout):
    if layout not in VALID_LAYOUTS:
        abort(404)

    return render_template(
        "index.html",
        forecast_type="ultrashort",
        group=None,
        layout=layout,
    )


@app.route("/short/<group>/<layout>")
def page_short(group, layout):
    if group not in VALID_GROUPS or layout not in VALID_LAYOUTS:
        abort(404)

    return render_template(
        "index.html",
        forecast_type="short",
        group=group,
        layout=layout,
    )


# API – 초단기
@app.route("/api/ultrashort/<layout>")
def api_ultrashort(layout):
    if layout not in VALID_LAYOUTS:
        abort(404)

    cached = get_cache().get(f"ultrashort:{layout}")

    if not cached:
        return jsonify({"type": "FeatureCollection", "features": []})

    return jsonify(json.loads(cached))


# API – 단기
@app.route("/api/short/<group>/<layout>")
def api_short(group, layout):
    if group not in VALID_GROUPS or layout not in VALID_LAYOUTS:
        abort(404)

    cached = get_cache().get(f"short:{group}:{layout}")

    if not cached:
        return jsonify({"type": "FeatureCollection", "features": []})

    return jsonify(json.loads(cached))


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=<DEBUG_FLAG>)
