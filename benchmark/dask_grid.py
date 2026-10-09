"""Mide lectura, grilla y materialización con workers Dask reales."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time

import dask.dataframe as dd
from distributed import Client
import numpy as np
import pandas as pd
from pymongo import MongoClient


def memory_peak():
    return int(Path("/sys/fs/cgroup/memory.peak").read_text())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, required=True)
    args = parser.parse_args()
    metadata = json.loads(Path("/data/benchmark/input.json").read_text())
    for item in metadata["files"]:
        path = Path("/data/benchmark/input") / item["name"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError("La entrada Parquet no coincide con la exportación")
    with Client(os.environ["DASK_SCHEDULER_ADDRESS"]) as client:
        client.wait_for_workers(args.workers, timeout=60)
        workers = client.scheduler_info()["workers"]
        if len(workers) != args.workers:
            raise ValueError("El número de workers no coincide con la configuración")
        if any(worker["nthreads"] != 1 or worker["memory_limit"] != 512 * 1024**2 for worker in workers.values()):
            raise ValueError("Los recursos por worker no coinciden con el protocolo")
        started = time.perf_counter()
        frame = dd.read_parquet("/data/benchmark/input", columns=["longitude", "latitude"])
        partitions = frame.npartitions
        frame = frame.assign(cell_x=np.floor(frame.longitude / 0.01).astype("int64"),
                             cell_y=np.floor(frame.latitude / 0.01).astype("int64"))
        result = frame.groupby(["cell_x", "cell_y"]).size(split_out=8).compute().reset_index(name="count")
        elapsed = time.perf_counter() - started
        driver_peak = memory_peak()
        peaks = client.run(memory_peak)
    result = result.sort_values(["cell_x", "cell_y"]).reset_index(drop=True)
    if int(result["count"].sum()) != metadata["rows"]:
        raise ValueError("El conteo no conserva todas las filas")
    # Comparación completa con la grilla Spark previamente publicada sobre la muestra.
    with MongoClient(os.environ["MONGODB_URI"]) as mongo:
        expected = pd.DataFrame(list(mongo.projectbd.spark_grid.find({}, {
            "_id": 0, "cell_x": 1, "cell_y": 1, "count": 1,
        }))).sort_values(["cell_x", "cell_y"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(result[["cell_x", "cell_y", "count"]], expected[["cell_x", "cell_y", "count"]], check_dtype=False)
    output_digest = hashlib.sha256()
    for x, y, count in result[["cell_x", "cell_y", "count"]].itertuples(index=False, name=None):
        output_digest.update(f"{x},{y},{count}\n".encode())
    output = Path(f"/data/benchmark/dask-{args.workers}.parquet")
    result.to_parquet(output, index=False)
    report = {"engine": "dask", "workers": args.workers, "threads_per_worker": 1,
              "worker_memory_limit_bytes": 512 * 1024**2, "input_rows": metadata["rows"],
              "input_records_sha256": metadata["records_sha256"], "input_partitions": partitions,
              "cell_size_degrees": 0.01, "groups": len(result), "elapsed_seconds": round(elapsed, 6),
              "worker_peak_bytes": peaks, "sum_worker_peaks_bytes": sum(peaks.values()),
              "driver_peak_bytes": driver_peak, "output_sha256": output_digest.hexdigest(),
              "equals_published_spark_grid": True}
    Path(f"/data/benchmark/dask-{args.workers}.json").write_text(json.dumps(report, indent=2) + "\n")
    print("BENCH_RESULT " + json.dumps(report))


if __name__ == "__main__":
    main()
