import json

import falcon
import redis
import requests
from falcon_cors import CORS
from sqlalchemy.orm.exc import NoResultFound

from weatheh import models, utils

cors = CORS(allow_origins_list=["http://127.0.0.1:8080"])
api = application = falcon.API(middleware=[cors.middleware])
cache = redis.StrictRedis(host="localhost", port=6379, db=0)
session = requests.Session()


def process_language(req):
    language = req.params.get("lang")
    if language not in ["en", "fr"]:
        return "en"
    return language


# noinspection PyUnresolvedReferences,PyMethodMayBeStatic
class City:
    def on_get(self, req, resp, city_code):
        language = process_language(req)
        cache_key = f"{city_code}.{language}"
        error_response = {"error": "Invalid city code provided"}

        cached = cache.get(cache_key)

        if cached:
            resp.body = cached
            resp.status = falcon.HTTP_200
        else:
            try:
                city = models.City.query.filter(
                    models.City.id == city_code
                ).one()
                forecast = city.current_condition(language=language)
                forecast["city"] = city.to_dict(language=language)
                resp.body = json.dumps(forecast)
                cache.set(city_code, resp.body, 60)
                resp.status = falcon.HTTP_200
            except NoResultFound:
                resp.body = json.dumps(error_response)
                resp.status = falcon.HTTP_400


# noinspection PyUnresolvedReferences,PyMethodMayBeStatic
class Search:
    def on_get(self, req, resp, search):
        if len(search) < 2:
            resp.body = json.dumps([])
        else:
            search = models.remove_accents(search[:128])
            language = process_language(req)
            cache_key = f"search.{language}.{search}"
            cached = cache.get(cache_key)

            if cached:
                resp.body = cached
            else:
                if language == "en":
                    search_results = (
                        models.City.query.filter(
                            models.City.name_en_unaccented.ilike(
                                "{}%".format(search)
                            )
                        )
                        .order_by(models.City.name_en_unaccented)
                        .limit(5)
                    )
                else:
                    search_results = (
                        models.City.query.filter(
                            models.City.name_en_unaccented.ilike(
                                "{}%".format(search)
                            )
                        )
                        .order_by(models.City.name_fr_unaccented)
                        .limit(5)
                    )

                results = []
                for city in search_results:
                    city_dict = city.to_dict(language)
                    if city_dict.get("condition"):
                        results.append(city_dict)
                resp.body = json.dumps(results)
                cache.set(cache_key, resp.body, 60)


# noinspection PyUnresolvedReferences,PyMethodMayBeStatic
class GeoLocation:
    def on_get(self, req, resp):
        language = process_language(req)
        try:
            latitude = float(req.params.get("lat"))
            longitude = float(req.params.get("lon"))
            city = utils.find_nearest_city_from_location(
                lat=latitude, lon=longitude
            )
            if city:
                forecast = city.current_condition(
                    language=language, caching=False
                )
                forecast["city"] = city.to_dict()
                resp.body = json.dumps(forecast)
        except (ValueError, TypeError):
            resp.body = json.dumps(
                {"error": "No weather station near provided location"}
            )
            resp.status = falcon.HTTP_400


cities = City()
searches = Search()
geo_locations = GeoLocation()

api.add_route("/api/forecast/city/{city_code}", cities)
api.add_route("/api/forecast/search/{search}", searches)
api.add_route("/api/forecast/coordinates/", geo_locations)

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    run_simple("localhost", 5000, application, use_reloader=True)
