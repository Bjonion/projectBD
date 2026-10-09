"""Consultas geoespaciales parametrizadas sobre la muestra y resultados Spark."""

import math
import os

from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError
from werkzeug.exceptions import BadRequest, RequestEntityTooLarge, UnsupportedMediaType


def number(value, name, minimum, maximum):
    if isinstance(value, bool):
        raise ValueError(f"{name} debe ser numérico")
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} debe ser numérico") from None
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise ValueError(f"{name} debe estar entre {minimum} y {maximum}")
    return result


def integer(value, name, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{name} debe ser entero")
    try:
        result = int(value)
    except (ValueError, TypeError):
        raise ValueError(f"{name} debe ser entero") from None
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} debe estar entre {minimum} y {maximum}")
    return result


def polygon(value):
    if not isinstance(value, dict) or value.get("type") != "Polygon":
        raise ValueError("Se requiere una geometría GeoJSON Polygon")
    rings = value.get("coordinates")
    if not isinstance(rings, list) or not rings:
        raise ValueError("El polígono debe contener al menos un anillo")
    validated = []
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4:
            raise ValueError("Cada anillo requiere al menos cuatro posiciones")
        points = []
        for position in ring:
            if not isinstance(position, list) or len(position) != 2:
                raise ValueError("Cada posición debe ser [longitud, latitud]")
            if not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in position):
                raise ValueError("Las coordenadas GeoJSON deben ser números")
            points.append([number(position[0], "longitud", -180, 180),
                           number(position[1], "latitud", -90, 90)])
        if points[0] != points[-1] or len({tuple(p) for p in points[:-1]}) < 3:
            raise ValueError("Cada anillo debe estar cerrado y tener tres vértices distintos")
        validated.append(points)
    # Solo se trasladan tipo y coordenadas, nunca operadores del cliente.
    return {"type": "Polygon", "coordinates": validated}


def create_app(config=None):
    app = Flask(__name__)
    app.config.from_mapping(MAX_CONTENT_LENGTH=256 * 1024, MONGODB_DATABASE="projectbd")
    if config:
        app.config.update(config)
    client = MongoClient(
        os.environ.get("MONGODB_URI", "mongodb://mongodb:27017/projectbd"),
        serverSelectionTimeoutMS=3000,
        connectTimeoutMS=3000,
    )
    app.extensions["mongo_client"] = client
    db = client[app.config["MONGODB_DATABASE"]]

    @app.errorhandler(ValueError)
    def invalid(error):
        return jsonify(error=str(error)), 400

    @app.errorhandler(BadRequest)
    @app.errorhandler(UnsupportedMediaType)
    def invalid_json(error):
        return jsonify(error="Se requiere un cuerpo JSON válido"), 400

    @app.errorhandler(RequestEntityTooLarge)
    def oversized(error):
        return jsonify(error="El cuerpo excede 256 KiB"), 413

    @app.errorhandler(PyMongoError)
    def unavailable(error):
        return jsonify(error="La consulta a MongoDB no está disponible"), 503

    def nearby_parameters():
        latitude = number(request.args.get("latitude"), "latitude", -90, 90)
        longitude = number(request.args.get("longitude"), "longitude", -180, 180)
        radius = number(request.args.get("radius"), "radius", 0.001, 20_040_000)
        return {"type": "Point", "coordinates": [longitude, latitude]}, radius

    @app.get("/health")
    def health():
        try:
            client.admin.command("ping")
        except PyMongoError:
            return jsonify(status="unavailable", mongodb="unavailable"), 503
        return jsonify(status="ok", mongodb="ok")

    @app.get("/accidents/nearby")
    def nearby():
        point, radius = nearby_parameters()
        limit = integer(request.args.get("limit", "100"), "limit", 1, 500)
        results = list(db.accidents.find({"location": {"$near": {
            "$geometry": point, "$maxDistance": radius,
        }}}).limit(limit).max_time_ms(5000))
        return jsonify(returned=len(results), radius_meters=radius, results=results)

    @app.post("/accidents/within")
    def within():
        body = request.get_json()
        if not isinstance(body, dict):
            raise ValueError("El cuerpo debe ser un objeto con polygon y limit opcional")
        geometry = polygon(body.get("polygon"))
        limit = integer(body.get("limit", 100), "limit", 1, 500)
        try:
            results = list(db.accidents.find({"location": {"$geoWithin": {
                "$geometry": geometry,
            }}}).limit(limit).max_time_ms(5000))
        except OperationFailure as error:
            if error.code in {2, 16755}:
                raise ValueError("MongoDB rechazó la geometría del polígono") from None
            raise
        return jsonify(returned=len(results), results=results)

    @app.get("/accidents/nearby-summary")
    def nearby_summary():
        point, radius = nearby_parameters()
        results = list(db.accidents.aggregate([
            {"$geoNear": {"near": point, "key": "location", "distanceField": "distance_meters",
                          "maxDistance": radius, "spherical": True}},
            {"$group": {"_id": None, "count": {"$sum": 1},
                        "min_distance_meters": {"$min": "$distance_meters"},
                        "max_distance_meters": {"$max": "$distance_meters"},
                        "avg_distance_meters": {"$avg": "$distance_meters"}}},
            {"$project": {"_id": 0}},
        ], maxTimeMS=5000))
        summary = results[0] if results else {
            "count": 0, "min_distance_meters": None,
            "max_distance_meters": None, "avg_distance_meters": None,
        }
        return jsonify(radius_meters=radius, **summary)

    @app.get("/aggregates/<kind>")
    def spark_results(kind):
        sorts = {"grid": [("count", -1), ("cell_x", 1), ("cell_y", 1)],
                 "hotspots": [("count", -1), ("cell_x", 1), ("cell_y", 1)],
                 "hours": [("hour", 1)], "months": [("year", 1), ("month", 1)]}
        if kind not in sorts:
            raise ValueError("kind debe ser grid, hotspots, hours o months")
        limit = integer(request.args.get("limit", "100"), "limit", 1, 500)
        offset = integer(request.args.get("offset", "0"), "offset", 0, 1_000_000)
        results = list(db[f"spark_{kind}"].find().sort(sorts[kind]).skip(offset).limit(limit).max_time_ms(5000))
        return jsonify(kind=kind, offset=offset, returned=len(results), results=results)

    return app
