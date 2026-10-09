import math
import uuid

import pytest

from api.app import create_app


@pytest.fixture()
def api():
    database = "api_test_" + uuid.uuid4().hex
    app = create_app({"TESTING": True, "MONGODB_DATABASE": database})
    client = app.extensions["mongo_client"]
    db = client[database]
    db.accidents.create_index([("location", "2dsphere")])
    db.accidents.insert_many([
        {"_id": "origin", "location": {"type": "Point", "coordinates": [-73, 40]}},
        {"_id": "near", "location": {"type": "Point", "coordinates": [-73, 40.001]}},
        {"_id": "far", "location": {"type": "Point", "coordinates": [-73, 40.1]}},
    ])
    db.spark_hours.insert_many([{"_id": "9", "hour": 9, "count": 2}, {"_id": "8", "hour": 8, "count": 1}])
    yield app.test_client(), db
    client.drop_database(database)
    client.close()


def query(radius=200):
    return {"latitude": 40, "longitude": -73, "radius": radius}


def test_near_and_geonear_distance_in_meters_and_parameters(api):
    client, _ = api
    assert client.get("/health").status_code == 200
    results = client.get("/accidents/nearby", query_string=query()).get_json()
    assert [row["_id"] for row in results["results"]] == ["origin", "near"]
    assert client.get("/accidents/nearby", query_string=query(1)).get_json()["returned"] == 1
    summary = client.get("/accidents/nearby-summary", query_string=query()).get_json()
    assert summary["count"] == 2
    assert summary["min_distance_meters"] == 0
    assert 110 < summary["max_distance_meters"] < 112
    assert math.isclose(summary["avg_distance_meters"], summary["max_distance_meters"] / 2)
    empty = client.get("/accidents/nearby-summary", query_string={"latitude": 0, "longitude": 0, "radius": 1}).get_json()
    assert empty["count"] == 0 and empty["avg_distance_meters"] is None


def test_polygon_inclusion_hole_and_invalid_geometry(api):
    client, _ = api
    ring = [[-73.01, 39.99], [-72.99, 39.99], [-72.99, 40.01], [-73.01, 40.01], [-73.01, 39.99]]
    shape = {"type": "Polygon", "coordinates": [ring]}
    response = client.post("/accidents/within", json={"polygon": shape})
    assert response.status_code == 200
    assert {r["_id"] for r in response.get_json()["results"]} == {"origin", "near"}
    hole = [[-73.0001,39.9999], [-73.0001,40.0001], [-72.9999,40.0001], [-72.9999,39.9999], [-73.0001,39.9999]]
    shape["coordinates"].append(hole)
    assert [r["_id"] for r in client.post("/accidents/within", json={"polygon": shape}).get_json()["results"]] == ["near"]
    for shape in [{"type": "Point", "coordinates": [-73,40]}, {"type":"Polygon","coordinates":[ring[:-1]]},
                  {"type":"Polygon","coordinates":[[[0,0],[1,0],[1,91],[0,0]]]}]:
        assert client.post("/accidents/within", json={"polygon":shape}).status_code == 400
    assert client.post("/accidents/within", data="{", content_type="application/json").status_code == 400
    assert client.post("/accidents/within", json=[]).status_code == 400


def test_validation_and_spark_pagination(api):
    client, _ = api
    for values in [{}, {**query(), "latitude": "nan"}, {**query(), "longitude": 181},
                   {**query(), "radius": -1}, {**query(), "limit": 501}, {**query(), "limit": "1.2"}]:
        assert client.get("/accidents/nearby", query_string=values).status_code == 400
    response = client.get("/aggregates/hours?limit=1&offset=1")
    assert response.status_code == 200
    assert response.get_json()["results"] == [{"_id":"9", "hour":9, "count":2}]
    assert client.get("/aggregates/accidents").status_code == 400
    assert client.get("/aggregates/hours?offset=-1").status_code == 400
