"""Pruebas HTTP contra Flask y las colecciones reales publicadas."""
import json
import os
import urllib.request

from pymongo import MongoClient


def main():
    base = os.environ.get("API_BASE_URL", "http://127.0.0.1:5000")

    def call(path, payload=None):
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(base + path, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=15) as response:
            assert response.status == 200
            return json.load(response)

    assert call("/health")["status"] == "ok"
    with MongoClient(os.environ["MONGODB_URI"]) as mongo:
        source = mongo.projectbd.accidents.find_one()
    assert source is not None, "No hay muestra publicada"
    longitude, latitude = source["location"]["coordinates"]
    params = f"latitude={latitude}&longitude={longitude}&radius=200&limit=10"
    nearby = call("/accidents/nearby?" + params)
    assert nearby["returned"] > 0
    assert nearby["results"][0]["location"] == source["location"]
    summary = call("/accidents/nearby-summary?" + params)
    assert summary["count"] >= nearby["returned"]
    assert summary["min_distance_meters"] < 0.000001
    ring = [[longitude-.001,latitude-.001], [longitude+.001,latitude-.001],
            [longitude+.001,latitude+.001], [longitude-.001,latitude+.001],
            [longitude-.001,latitude-.001]]
    within = call("/accidents/within", {"polygon":{"type":"Polygon","coordinates":[ring]}, "limit":10})
    assert within["returned"] > 0
    run_ids = set()
    counts = {}
    for kind in ("grid", "hours", "months", "hotspots"):
        result = call("/aggregates/" + kind + "?limit=10")
        assert result["returned"] > 0
        assert all(row["count"] > 0 for row in result["results"])
        run_ids.update(row["run_id"] for row in result["results"])
        counts[kind] = result["returned"]
    assert len(run_ids) == 1, "Las colecciones Spark pertenecen a ejecuciones distintas"
    print("SMOKE_RESULT " + json.dumps({"health":"ok", "nearby":nearby["returned"],
          "within":within["returned"], "nearby_summary_count":summary["count"],
          "spark_results_returned":counts, "spark_run_id":next(iter(run_ids))}))


if __name__ == "__main__":
    main()
