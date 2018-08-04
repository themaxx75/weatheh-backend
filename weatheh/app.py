import json
import re

import falcon
import pymongo
import requests
from bson import ObjectId
from bson.errors import InvalidId
from falcon_cors import CORS

from weatheh import utils

MONGO_DB_NAME = "weatheh"

cors = CORS(allow_origins_list=["http://127.0.0.1:8080"])
api = application = falcon.API(middleware=[cors.middleware])
session = requests.Session()

client = pymongo.MongoClient()
db = client[MONGO_DB_NAME]
stations_coll = db.stations
cities_coll = db.cities
db.drop_collection("forecasts")
forecasts_coll = db.forecasts
db.forecasts.create_index("datetime", expireAfterSeconds=120)


# noinspection PyMethodMayBeStatic
class City:
    def on_get(self, req, resp, city_code):
        language = utils.process_language(req)
        error_response = {"error": "Invalid city code provided"}
        try:
            city = cities_coll.find_one({"_id": ObjectId(city_code)})
            if city:
                city["weather"] = utils.fetch_forecast(
                    city, language, hourly=False
                )
                resp.body = json.dumps(utils.normalize_city(city, language))
                resp.status = falcon.HTTP_200
            else:
                resp.body = json.dumps(error_response)
                resp.status = falcon.HTTP_404
        except InvalidId:
            resp.body = json.dumps(error_response)
            resp.status = falcon.HTTP_404


# noinspection PyMethodMayBeStatic
class Search:
    def on_get(self, req, resp, search):
        clean_search = utils.normalize_string(search)[:128]
        if len(clean_search) < 2:
            resp.body = json.dumps([])
        else:
            language = utils.process_language(req)
            search_regex = re.compile(
                f"^{clean_search}",
                # re.VERBOSE | re.DOTALL | re.UNICODE | re.IGNORECASE
            )
            cursor = cities_coll.find(
                {
                    f"searchIndex{language.capitalize()}": {
                        "$regex": search_regex
                    }
                }
            )
            results_docs = []
            tracker = []

            for city in cursor.limit(5):
                results_docs.append(city)
                tracker.append(city["_id"])

            if len(results_docs) < 5:
                for c in cities_coll.find(
                    {"$text": {"$search": utils.normalize_string(clean_search)}}
                ):
                    if len(results_docs) < 5 and c["_id"] not in tracker:
                        results_docs.append(c)
                        tracker.append(c["_id"])
                    if len(results_docs) == 5:
                        break

            results = []

            for result in results_docs:
                result["weather"] = utils.fetch_forecast(
                    result, language, hourly=False, fore_cast=False
                )
                results.append(utils.normalize_city(result, language))

            resp.body = json.dumps(results)


# noinspection PyUnresolvedReferences,PyMethodMayBeStatic
class GeoLocation:
    def on_get(self, req, resp):
        language = process_language(req)
        try:
            latitude = float(req.params.get("lat"))
            longitude = float(req.params.get("lon"))
            city = utils.find_nearest_from_loc([latitude, longitude])
            if city:
                city["weather"] = utils.fetch_forecast(
                    city, language, hourly=False
                )
                resp.body = json.dumps(utils.normalize_city(city, language))
                resp.status = falcon.HTTP_200
            else:
                resp.body = json.dumps(error_response)
                resp.status = falcon.HTTP_404
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
