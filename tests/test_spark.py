import os
import uuid

import pytest
from pymongo import MongoClient
from pyspark.sql import SparkSession

from processing.aggregate import SCHEMA, aggregates, run


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("projectbd-spark-pytest").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def records():
    return [
        {"_id": "a", "location": {"type": "Point", "coordinates": [-0.001, 0.011]}, "hour": 8, "year": 2022, "month": 1},
        {"_id": "b", "location": {"type": "Point", "coordinates": [-0.002, 0.012]}, "hour": 8, "year": 2023, "month": 1},
        {"_id": "c", "location": {"type": "Point", "coordinates": [0.011, -0.001]}, "hour": 9, "year": 2023, "month": 2},
    ]


def test_grid_orientation_negative_coordinates_and_time(spark):
    result = aggregates(spark.createDataFrame(records(), SCHEMA))
    assert {(r.cell_x, r.cell_y): r["count"] for r in result["grid"].collect()} == {(-1, 1): 2, (1, -1): 1}
    assert {r.hour: r["count"] for r in result["hours"].collect()} == {8: 2, 9: 1}
    assert {r._id: r["count"] for r in result["months"].collect()} == {"2022-01": 1, "2023-01": 1, "2023-02": 1}
    with pytest.raises(ValueError):
        aggregates(spark.createDataFrame(records(), SCHEMA), float("nan"))


def test_connector_publication_repeatability_and_failure_preserves_results(spark):
    uri = os.environ["MONGODB_URI"]
    client = MongoClient(uri)
    database = "spark_test_" + uuid.uuid4().hex
    db = client[database]
    try:
        db.accidents.insert_many(records())
        first = run(spark, uri, database=database, expected_rows=3)
        snapshot = {n: list(db[n].find({}, {"run_id": 0}).sort("_id")) for n in first["collections"]}
        second = run(spark, uri, database=database, expected_rows=3)
        assert first["run_id"] != second["run_id"]
        assert snapshot == {n: list(db[n].find({}, {"run_id": 0}).sort("_id")) for n in snapshot}
        assert db.spark_hotspots.find_one({"_id": "-1:1"})["count"] == 2
        db.accidents.update_one({"_id": "a"}, {"$set": {"hour": 24}})
        with pytest.raises(ValueError, match="inválidos"):
            run(spark, uri, database=database, expected_rows=3)
        assert snapshot == {n: list(db[n].find({}, {"run_id": 0}).sort("_id")) for n in snapshot}
        assert not any("staging" in name for name in db.list_collection_names())
    finally:
        client.drop_database(database)
        client.close()
