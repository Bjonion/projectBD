"""Mide la misma lectura y agregación por grilla que Dask."""
import argparse
import hashlib
import json
from pathlib import Path
import time

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, required=True)
    args = parser.parse_args()
    root = Path('/data/benchmark')
    metadata = json.loads((root / 'input.json').read_text())
    files = [root / 'input' / item['name'] for item in metadata['files']]
    for path, item in zip(files, metadata['files']):
        if hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
            raise ValueError('La entrada Parquet no coincide con la exportación')
    # Un archivo por partición, sin agregar un repartition/shuffle de entrada.
    spark = (SparkSession.builder.appName('projectbd-grid-benchmark')
             .config('spark.cores.max', args.workers)
             .config('spark.sql.files.maxPartitionBytes', max(p.stat().st_size for p in files) + 4194304)
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        deadline = time.monotonic() + 60
        while spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1 != args.workers:
            if time.monotonic() >= deadline:
                raise TimeoutError('Los ejecutores no se conectaron al driver')
            time.sleep(0.2)
        started = time.perf_counter()
        frame = spark.read.parquet(str(root / 'input')).select('longitude', 'latitude')
        partitions = frame.rdd.getNumPartitions()
        grid = frame.select(F.floor(F.col('longitude') / 0.01).cast('long').alias('cell_x'),
                            F.floor(F.col('latitude') / 0.01).cast('long').alias('cell_y'))
        result = grid.groupBy('cell_x', 'cell_y').count().collect()
        elapsed = time.perf_counter() - started
        driver_peak = int(Path('/sys/fs/cgroup/memory.peak').read_text())
        executor_count = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
        if executor_count != args.workers:
            raise ValueError('El número de ejecutores activos no coincide con los workers')
        rows = sorted((r.cell_x, r.cell_y, r['count']) for r in result)
        if sum(r[2] for r in rows) != metadata['rows']:
            raise ValueError('El conteo no conserva todas las filas')
        output = ''.join(f'{x},{y},{count}\n' for x, y, count in rows)
        digest = hashlib.sha256(output.encode()).hexdigest()
        expected = json.loads((root / 'dask-2.json').read_text())
        if digest != expected['output_sha256'] or metadata['records_sha256'] != expected['input_records_sha256']:
            raise ValueError('La entrada o todos los conteos no coinciden con Dask')
        (root / f'spark-{args.workers}.csv').write_text(output)
        report = {'engine': 'spark', 'workers': args.workers, 'executors': executor_count,
                  'cores_per_worker': 1, 'executor_memory_limit_bytes': 512 * 1024**2,
                  'input_rows': metadata['rows'], 'input_records_sha256': metadata['records_sha256'],
                  'input_partitions': partitions, 'cell_size_degrees': 0.01,
                  'groups': len(rows), 'elapsed_seconds': round(elapsed, 6),
                  'driver_peak_bytes': driver_peak, 'output_sha256': digest,
                  'equals_dask_grid': True}
        (root / f'spark-{args.workers}.json').write_text(json.dumps(report, indent=2) + '\n')
        print('BENCH_RESULT ' + json.dumps(report), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
