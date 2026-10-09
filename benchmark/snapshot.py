"""Exporta la misma muestra MongoDB para comparar ambos motores en Parquet."""
import hashlib
import json
import os
from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient


def main():
    root = Path("/data/benchmark")
    temporary = root / "input_staging"
    destination = root / "input"
    root.mkdir(parents=True, exist_ok=True)
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir()
    schema = pa.schema([("id", pa.string()), ("longitude", pa.float64()), ("latitude", pa.float64())])
    digest = hashlib.sha256()
    count, part = 0, 0
    batch = []
    files = []

    def write_batch():
        nonlocal part
        path = temporary / f"part-{part:03d}.parquet"
        pq.write_table(pa.Table.from_pylist(batch, schema=schema), path, compression="snappy")
        files.append({"name": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        part += 1
        batch.clear()

    with MongoClient(os.environ["MONGODB_URI"]) as client:
        source = client.projectbd.accidents
        expected = source.count_documents({})
        if expected < 1_000_000:
            raise ValueError("Se requiere al menos un millón de registros")
        for document in source.find({}, {"_id": 1, "location": 1}).sort("_id", 1).batch_size(10000):
            lon, lat = document["location"]["coordinates"]
            record = {"id": document["_id"], "longitude": lon, "latitude": lat}
            digest.update((json.dumps(record, separators=(",", ":")) + "\n").encode())
            batch.append(record)
            count += 1
            if len(batch) == 100000:
                write_batch()
        if batch:
            write_batch()
        if count != expected or source.count_documents({}) != expected:
            raise ValueError("La entrada cambió durante la exportación")
    metadata = {"source_collection": "projectbd.accidents", "rows": count,
                "records_sha256": digest.hexdigest(), "files": files,
                "schema": "id:string, longitude:float64, latitude:float64"}
    if destination.exists():
        shutil.rmtree(destination)
    temporary.rename(destination)
    (root / "input.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print("SNAPSHOT_RESULT " + json.dumps(metadata))


if __name__ == "__main__":
    main()
