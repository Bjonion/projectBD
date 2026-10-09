"""Agregaciones Spark sobre la muestra GeoJSON publicada por Dask."""
import argparse
import json
import math
import os
import time
import uuid
from urllib.parse import urlsplit, urlunsplit

from pymongo import MongoClient
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

SCHEMA = T.StructType([
    T.StructField("_id", T.StringType()),
    T.StructField("location", T.StructType([
        T.StructField("type", T.StringType()),
        T.StructField("coordinates", T.ArrayType(T.DoubleType())),
    ])),
    T.StructField("hour", T.IntegerType()),
    T.StructField("year", T.IntegerType()),
    T.StructField("month", T.IntegerType()),
])


def aggregates(source, cell_size=0.01):
    if not math.isfinite(cell_size) or not 0 < cell_size <= 1:
        raise ValueError("El tamaño de celda debe estar en (0, 1] grados")
    grid = source.groupBy(
        F.floor(F.col("location.coordinates")[0] / cell_size).cast("long").alias("cell_x"),
        F.floor(F.col("location.coordinates")[1] / cell_size).cast("long").alias("cell_y"),
    ).count().withColumn("cell_size_degrees", F.lit(cell_size))
    grid = grid.withColumn("_id", F.concat_ws(":", "cell_x", "cell_y"))
    hours = source.groupBy("hour").count().withColumn("_id", F.col("hour").cast("string"))
    months = source.groupBy("year", "month").count().withColumn(
        "_id", F.format_string("%04d-%02d", "year", "month"))
    return {"grid": grid, "hours": hours, "months": months}


def run(spark, uri, database="projectbd", source_collection="accidents",
        prefix="spark_", expected_rows=1_000_000, cell_size=0.01):
    started = time.perf_counter()
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    db = client[database]
    # La base indicada en la URI prevalece sobre la opción del conector.
    connector_uri = urlunsplit(urlsplit(uri)._replace(path="/" + database))
    run_id = uuid.uuid4().hex
    staging = {}
    cached = []
    try:
        mongo_rows = db[source_collection].count_documents({})
        if mongo_rows != expected_rows:
            raise ValueError("La colección de entrada no tiene el tamaño esperado")
        source = (spark.read.format("mongodb").schema(SCHEMA)
                  .option("connection.uri", connector_uri).option("database", database)
                  .option("collection", source_collection)
                  .option("partitioner", "com.mongodb.spark.sql.connector.read.partitioner.PaginateIntoPartitionsPartitioner")
                  .option("partitioner.options.max.number.of.partitions", "8")
                  .load().persist(StorageLevel.DISK_ONLY))
        cached.append(source)
        partitions = source.rdd.getNumPartitions()
        if source.count() != expected_rows:
            raise ValueError("Spark y MongoDB discrepan en el conteo de entrada")
        invalid = source.where(
            F.col("location.type").isNull() | (F.col("location.type") != "Point") |
            F.col("location.coordinates").isNull() | (F.size("location.coordinates") != 2) |
            F.col("location.coordinates")[0].isNull() | F.col("location.coordinates")[1].isNull() |
            F.isnan(F.col("location.coordinates")[0]) | F.isnan(F.col("location.coordinates")[1]) |
            (~F.col("location.coordinates")[0].between(-180, 180)) |
            (~F.col("location.coordinates")[1].between(-90, 90)) |
            F.col("hour").isNull() | (~F.col("hour").between(0, 23)) |
            F.col("year").isNull() | F.col("month").isNull() |
            (~F.col("month").between(1, 12))
        ).limit(1).count()
        if invalid:
            raise ValueError("Hay documentos geoespaciales o temporales inválidos")
        frames = aggregates(source, cell_size)
        for name, frame in frames.items():
            frame.persist(StorageLevel.DISK_ONLY)
            cached.append(frame)
            if frame.agg(F.sum("count")).first()[0] != expected_rows:
                raise ValueError("Una agregación no conserva el total de entrada")
        frames["hotspots"] = frames["grid"].orderBy(
            F.desc("count"), F.asc("cell_x"), F.asc("cell_y")).limit(20)
        counts = {}
        # Se escriben TODOS los resultados con el conector antes de publicar.
        for name, frame in frames.items():
            temporary = f"{prefix}{name}_staging_{run_id}"
            staging[name] = temporary
            frame.withColumn("run_id", F.lit(run_id)).write.format("mongodb").mode("append").option(
                "connection.uri", connector_uri).option("database", database).option(
                "collection", temporary).option("ordered", "true").save()
            counts[name] = db[temporary].count_documents({})
            if counts[name] != frame.count():
                raise ValueError("La escritura no conserva el número de grupos")
            if name != "hotspots":
                total = list(db[temporary].aggregate([
                    {"$group": {"_id": None, "total": {"$sum": "$count"}}}
                ]))[0]["total"]
                if total != expected_rows:
                    raise ValueError("La colección escrita no conserva los accidentes")
        db[staging["grid"]].create_index([("count", -1), ("cell_x", 1), ("cell_y", 1)])
        for name, temporary in staging.items():
            db[temporary].rename(f"{prefix}{name}", dropTarget=True)
        report = {
            "run_id": run_id, "source_collection": source_collection,
            "input_rows": expected_rows, "input_partitions": partitions,
            "cell_size_degrees": cell_size, "time_basis": "hora local del registro",
            "spark_master": spark.sparkContext.master,
            "collections": {f"{prefix}{name}": count for name, count in counts.items()},
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "spark_version": spark.version,
        }
        print("SPARK_RESULT " + json.dumps(report, ensure_ascii=False), flush=True)
        return report
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        for temporary in staging.values():
            db.drop_collection(temporary)
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--cell-size", type=float, default=0.01)
    args = parser.parse_args()
    if args.rows < 1_000_000:
        parser.error("La muestra de la actividad exige al menos un millón de filas")
    session = SparkSession.builder.appName("projectbd-aggregations").getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    try:
        run(session, os.environ.get("MONGODB_URI", "mongodb://mongodb:27017/projectbd"),
            expected_rows=args.rows, cell_size=args.cell_size)
    finally:
        session.stop()
