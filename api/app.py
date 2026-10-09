"""Base de la API; las consultas geoespaciales se implementan en su etapa."""

import os

from flask import Flask, jsonify
from pymongo import MongoClient
from pymongo.errors import PyMongoError


def create_app():
    app = Flask(__name__)
    client = MongoClient(
        os.environ.get("MONGODB_URI", "mongodb://mongodb:27017/projectbd"),
        serverSelectionTimeoutMS=3000,
        connectTimeoutMS=3000,
    )
    app.extensions["mongo_client"] = client

    @app.get("/health")
    def health():
        try:
            client.admin.command("ping")
        except PyMongoError:
            return jsonify(status="unavailable", mongodb="unavailable"), 503
        return jsonify(status="ok", mongodb="ok")

    return app
