"""Pruebas de limpieza, muestreo y carga real con Dask/MongoDB."""

import json
import os
from pathlib import Path
import uuid

import pandas as pd
from pymongo import MongoClient
import pytest

from ingestion.ingest import COLUMNS, allocate_quotas, clean_partition, ingest, select_sample, to_document


def row(identifier="A-1", lat="40", lon="-73", timestamp="2023-01-02 03:04:05"):
    return dict(zip(COLUMNS, [identifier, timestamp, lat, lon, "2", "City", "NY", "US/Eastern"]))


def test_invalid_coordinates_and_dates_are_counted_once():
    data = [row(), row("A-2", lat=None), row("A-3", lon="not-a-number"),
            row("A-4", lat="91"), row("A-5", lon="-181"), row(""),
            row("A-7", timestamp="invalid"), row("A-8", lat="-90", lon="180")]
    cleaned, stats = clean_partition(pd.DataFrame(data))
    assert stats == {"raw": 8, "null_or_nonnumeric_coordinates": 2,
                     "out_of_range_coordinates": 2, "missing_id": 1,
                     "invalid_start_time": 1, "valid": 2}
    document = to_document(cleaned.iloc[0].to_dict())
    assert document["location"] == {"type": "Point", "coordinates": [-73.0, 40.0]}
    assert document["start_time_local"] == "2023-01-02T03:04:05"
    assert document["hour"] == 3


def test_sample_is_exact_and_reproducible():
    assert allocate_quotas([7, 11, 0, 13], 10) == [2, 4, 0, 4]
    frame = clean_partition(pd.DataFrame([row("A-%d" % index) for index in range(100)]))[0]
    expected = select_sample(frame, 20)["ID"].tolist()
    assert len(expected) == 20
    assert select_sample(frame.sample(frac=1, random_state=42), 20)["ID"].tolist() == expected
    with pytest.raises(ValueError):
        allocate_quotas([1, 2], 4)


def test_distributed_load_index_and_repeatability():
    scheduler = os.environ.get("DASK_SCHEDULER_ADDRESS")
    uri = os.environ.get("MONGODB_URI")
    if not scheduler or not uri:
        pytest.skip("Requiere los servicios Compose Dask y MongoDB")
    unique = uuid.uuid4().hex
    csv = Path("/data") / ("test-" + unique + ".csv")
    report_path = Path("/data") / ("test-" + unique + ".json")
    collection = "test_ingestion_" + unique
    pd.DataFrame([row("A-%d" % index) for index in range(100)] + [row("bad", lat="91")]).to_csv(csv, index=False)
    try:
        report = ingest(str(csv), scheduler, uri, "projectbd", collection, target=40,
                        blocksize="1KiB", batch_size=7, report_path=str(report_path))
        assert report["cleaning"]["raw"] == 101
        assert report["cleaning"]["out_of_range_coordinates"] == 1
        with MongoClient(uri) as client:
            target = client.projectbd[collection]
            ids = sorted(document["_id"] for document in target.find({}, {"_id": 1}))
            assert len(ids) == 40
            assert target.index_information()["location_2dsphere"]["key"] == [("location", "2dsphere")]
            assert target.count_documents({"location": {"$geoWithin": {"$geometry": {
                "type": "Polygon", "coordinates": [[[-74,39],[-72,39],[-72,41],[-74,41],[-74,39]]]
            }}}}) == 40
        ingest(str(csv), scheduler, uri, "projectbd", collection, target=40,
               blocksize="1KiB", batch_size=7, report_path=str(report_path))
        with MongoClient(uri) as client:
            assert sorted(document["_id"] for document in client.projectbd[collection].find({}, {"_id": 1})) == ids
        assert json.loads(report_path.read_text())["sample_records"] == 40
        # Una carga inválida no debe reemplazar la colección publicada previamente.
        pd.DataFrame([row("duplicate") for _ in range(100)]).to_csv(csv, index=False)
        with pytest.raises(ValueError, match="IDs únicos"):
            ingest(str(csv), scheduler, uri, "projectbd", collection, target=40,
                   blocksize="1KiB", batch_size=7, report_path=str(report_path))
        with MongoClient(uri) as client:
            assert sorted(document["_id"] for document in client.projectbd[collection].find({}, {"_id": 1})) == ids
            assert not any(name.startswith(collection + "_staging_") for name in client.projectbd.list_collection_names())
    finally:
        with MongoClient(uri) as client:
            client.projectbd.drop_collection(collection)
        csv.unlink(missing_ok=True)
        report_path.unlink(missing_ok=True)
