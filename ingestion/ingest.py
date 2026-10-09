"""Limpieza Dask, muestra determinista y carga GeoJSON por lotes en MongoDB."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import time
import uuid

import dask
import dask.dataframe as dd
from distributed import Client, get_worker
import pandas as pd
from pymongo import MongoClient, ReplaceOne

from ingestion.download import CSV_NAME

COLUMNS = ["ID", "Start_Time", "Start_Lat", "Start_Lng", "Severity", "City", "State", "Timezone"]
HASH_KEY = "projectbd2026key"


def clean_partition(frame):
    frame = frame.copy()
    lat = pd.to_numeric(frame["Start_Lat"], errors="coerce")
    lon = pd.to_numeric(frame["Start_Lng"], errors="coerce")
    missing = lat.isna() | lon.isna()
    outside = ~missing & (~lat.between(-90, 90) | ~lon.between(-180, 180))
    spatial = ~missing & ~outside
    missing_id = spatial & (frame["ID"].isna() | frame["ID"].fillna("").str.strip().eq(""))
    timestamps = pd.to_datetime(frame["Start_Time"], errors="coerce", format="mixed")
    invalid_time = spatial & ~missing_id & timestamps.isna()
    valid = spatial & ~missing_id & ~invalid_time
    stats = {
        "raw": len(frame), "null_or_nonnumeric_coordinates": int(missing.sum()),
        "out_of_range_coordinates": int(outside.sum()),
        "missing_id": int(missing_id.sum()), "invalid_start_time": int(invalid_time.sum()),
        "valid": int(valid.sum()),
    }
    frame["Start_Lat"] = lat
    frame["Start_Lng"] = lon
    frame["Start_Time"] = timestamps
    return frame.loc[valid].copy(), stats


def partition_stats(frame):
    return clean_partition(frame)[1]


def allocate_quotas(counts, target):
    total = sum(counts)
    if not total or target <= 0 or target > total:
        raise ValueError("La muestra solicitada excede los registros válidos o es inválida")
    quotas = [target * count // total for count in counts]
    priority = sorted(range(len(counts)), key=lambda index: (-(target * counts[index] % total), index))
    for index in priority[:target - sum(quotas)]:
        quotas[index] += 1
    return quotas


def select_sample(frame, quota):
    if quota == 0:
        return frame.iloc[:0]
    if quota >= len(frame):
        return frame
    frame = frame.copy()
    frame["_sample_hash"] = pd.util.hash_pandas_object(frame["ID"], index=False, hash_key=HASH_KEY)
    return frame.sort_values(["_sample_hash", "ID"], kind="stable").iloc[:quota].drop(columns="_sample_hash")


def optional(value):
    return None if pd.isna(value) else str(value)


def to_document(row):
    timestamp = row["Start_Time"]
    severity = pd.to_numeric(row["Severity"], errors="coerce")
    return {
        "_id": str(row["ID"]),
        "location": {"type": "Point", "coordinates": [float(row["Start_Lng"]), float(row["Start_Lat"])]},
        "start_time_local": timestamp.isoformat(), "timezone": optional(row["Timezone"]),
        "year": int(timestamp.year), "month": int(timestamp.month),
        "date": timestamp.strftime("%Y-%m-%d"), "hour": int(timestamp.hour),
        "severity": None if pd.isna(severity) else int(severity),
        "city": optional(row["City"]), "state": optional(row["State"]),
    }


def load_partition(frame, quota, uri, database, collection, batch_size):
    selected = select_sample(clean_partition(frame)[0], quota)
    batches = 0
    with MongoClient(uri, serverSelectionTimeoutMS=10000) as client:
        target = client[database][collection]
        operations = []
        for row in selected.to_dict(orient="records"):
            document = to_document(row)
            operations.append(ReplaceOne({"_id": document["_id"]}, document, upsert=True))
            if len(operations) >= batch_size:
                target.bulk_write(operations, ordered=False)
                batches += 1
                operations = []
        if operations:
            target.bulk_write(operations, ordered=False)
            batches += 1
    try:
        worker = get_worker().address
    except ValueError:
        worker = "local-test"
    return {"selected": len(selected), "batches": batches, "worker": worker}


def ingest(csv_path, scheduler, uri, database, collection, target=1_000_000,
           blocksize="8MiB", batch_size=1000, report_path="/data/reports/ingestion.json"):
    if batch_size < 1 or target < 1:
        raise ValueError("Cantidad de registros y tamaño de lote deben ser positivos")
    started = time.perf_counter()
    staging = collection + "_staging_" + uuid.uuid4().hex
    totals = Counter()
    with Client(scheduler) as client, MongoClient(uri, serverSelectionTimeoutMS=10000) as mongo:
        client.wait_for_workers(2, timeout=60)
        frame = dd.read_csv(csv_path, usecols=COLUMNS, dtype={column: "string" for column in COLUMNS},
                            blocksize=blocksize, keep_default_na=True)
        parts = frame.to_delayed()
        statistics = client.gather(client.compute([dask.delayed(partition_stats)(part) for part in parts]))
        for stats in statistics:
            totals.update(stats)
        quotas = allocate_quotas([stats["valid"] for stats in statistics], target)
        print("Limpieza completa: %s" % json.dumps(dict(totals)), flush=True)
        print("Carga de %d registros mediante %d particiones." % (target, len(parts)), flush=True)
        db = mongo[database]
        try:
            tasks = [dask.delayed(load_partition)(part, quota, uri, database, staging, batch_size)
                     for part, quota in zip(parts, quotas) if quota]
            loaded = client.gather(client.compute(tasks, retries=1))
            stored = db[staging].count_documents({})
            if stored != target:
                raise ValueError("La cantidad de IDs únicos cargados no coincide con la muestra requerida")
            db[staging].create_index([("location", "2dsphere")], name="location_2dsphere")
            db[staging].rename(collection, dropTarget=True)
        except Exception:
            db.drop_collection(staging)
            raise
    source_path = Path(csv_path).parent / "source.json"
    report = {
        "source": json.loads(source_path.read_text()) if source_path.exists() else None,
        "cleaning": dict(totals), "sample_records": stored,
        "sampling": "proportional quotas by valid partition counts; smallest deterministic ID hashes",
        "hash_key": HASH_KEY, "blocksize": blocksize, "partitions": len(parts),
        "batch_size": batch_size, "batches": sum(item["batches"] for item in loaded),
        "workers_used": sorted({item["worker"] for item in loaded}),
        "collection": collection, "index": "location_2dsphere",
        "seconds": round(time.perf_counter() - started, 3),
        "timestamp_semantics": "source local wall time; timezone retained; not converted to UTC",
    }
    output = Path(report_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print("Ingesta completada: %d documentos únicos, índice 2dsphere, %.3f segundos." % (stored, report["seconds"]), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default="/data/raw/" + CSV_NAME)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--blocksize", default="8MiB")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--collection", default="accidents")
    args = parser.parse_args()
    if args.rows < 1_000_000:
        parser.error("La muestra de la actividad debe contener al menos un millón de registros")
    ingest(args.csv, os.environ["DASK_SCHEDULER_ADDRESS"], os.environ["MONGODB_URI"],
           "projectbd", args.collection, args.rows, args.blocksize, args.batch_size)


if __name__ == "__main__":
    main()
