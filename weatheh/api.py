from flask import abort, Flask, jsonify, request, make_response
from werkzeug.contrib.cache import RedisCache
from sqlalchemy.orm.exc import NoResultFound
from . import database, models, utils

from flask_cors import CORS


app = Flask(__name__)
cache = RedisCache()
debug = app.debug

if debug:
    CORS(app)


@app.teardown_appcontext
def shutdown_session(exception=None):
    database.db_session.remove()


@app.route("/api/forecast/coordinates/", methods=["GET"])
def from_coordinates():
    try:
        latitude = float(request.args.get("lat"))
        longitude = float(request.args.get("lon"))
    except (ValueError, TypeError):
        return make_response(
            jsonify({"error": "Invalid lat long provided."}), 400
        )

    language = request.args.get("lang", "en")

    city = utils.find_nearest_city_from_location(lat=latitude, lon=longitude)

    if city:
        forecast = city.current_condition(language=language)
        forecast["city"] = city.to_dict()

        return jsonify(forecast)

    return make_response(
        jsonify({"error": "No weather station near provided location"}), 400
    )


@app.route("/api/forecast/city/<int:city_code>", methods=["GET"])
def from_city(city_code):
    error_response = {"error": "Invalid city code provided"}
    language = request.args.get("lang", "en")

    try:
        city = models.City.query.filter(models.City.id == city_code).one()
    except NoResultFound:
        return make_response(jsonify(error_response), 400)

    forecast = city.current_condition(language=language)
    forecast["city"] = city.to_dict()

    return jsonify(forecast)


@app.route("/api/forecast/search/<search>")
def search_city(search):
    language = request.args.get("lang", "en")

    search = models.remove_accents(search)
    print(search)
    cache_key = f"search_city.{search}"
    cached = cache.get(cache_key)

    if cached:
        if app.debug:
            print('Cached')
        results = cached
    else:
        search_results = (
            models.City.query.filter(
                models.City.name_en_unaccented.ilike("{}%".format(search))
            )
            .order_by(models.City.name_en_unaccented)
            .limit(5)
        )
        results = [c.to_dict() for c in search_results]
        cache.set(cache_key, results, 5 * 60)

    return jsonify(results)
