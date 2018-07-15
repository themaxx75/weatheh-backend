from flask import Flask, jsonify, request, render_template
from werkzeug.contrib.cache import RedisCache
from sqlalchemy.orm.exc import NoResultFound
from weatheh import database, models, weather

from random import randint

import requests

app = Flask(__name__, static_folder="../dist/static", template_folder="../dist")
cache = RedisCache()


@app.teardown_appcontext
def shutdown_session(exception=None):
    database.db_session.remove()


@app.route("/api/forecats/latlong/", methods=["GET"])
def from_latlong():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except ValueError:
        return jsonify({"error": "Invalid lat long provided."})

    language = request.args.get("lang", "en")

    if None in (lat, lon):
        return jsonify({})

    city = weather.find_nearest_city_from_location(lat=lat, lon=lon)

    if city:
        forecast = city.current_condition(language=language)
        forecast["city"] = city.to_dict()

        return jsonify(forecast)

    return jsonify({"error": "No weather station near provided location"})


@app.route("/api/forecats/city/<int:city_code>", methods=["GET"])
def from_city(city_code):
    error_response = {"error": "Invalid city code provided"}
    language = request.args.get("lang", "en")

    try:
        city = models.City.query.filter(models.City.id == city_code).one()
    except NoResultFound:
        return jsonify(error_response)

    forecast = city.current_condition(language=language)
    forecast["city"] = city.to_dict()

    return jsonify(forecast)


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    if app.debug:
        return requests.get('http://localhost:8080/{}'.format(path)).text
    return render_template("index.html")
