import csv
import datetime
import os
import shutil
import time
import zipfile

import lxml.etree as etree
import pymongo

from weatheh import app, utils
from bson import ObjectId


SITE_LIST_EN_URL = (
    utils.BASE_HOST_DD_WEATHER + "/citypage_weather/docs/site_list_en.csv"
)
SITE_LIST_FR_URL = (
    utils.BASE_HOST_DD_WEATHER + "/citypage_weather/docs/site_list_fr.csv"
)


GEONAMES_CANADA_DUMP = "http://download.geonames.org/export/dump/CA.zip"
GEONAMES_JSON = "geonames.json"
STATIONS_JSON = "stations.json"
CITIES_JSON = "cities.json"
ICONS_URL = "https://weather.gc.ca/weathericons/"
ICONS_DESCRIPTIONS = (
    "http://dd.weather.gc.ca/citypage_weather/docs/"
    "current_conditions_icon_code_descriptions_e.csv"
)

PROVINCES = {
    "AB": {"en": "Alberta", "fr": "Alberta"},
    "BC": {"en": "British Columbia", "fr": "Colombie-Britannique"},
    "MB": {"en": "Manitoba", "fr": "Manitoba"},
    "NB": {"en": "New Brunswick", "fr": "Nouveau-Brunswick"},
    "NL": {"en": "Newfoundland and Labrador", "fr": "Terre-Neuve-et-Labrador"},
    "NS": {"en": "Nova Scotia", "fr": "Nouvelle-Écosse"},
    "NT": {"en": "Northwest Territories", "fr": "Territoires du Nord-Ouest"},
    "NU": {"en": "Nunavut", "fr": "Nunavut"},
    "ON": {"en": "Ontario", "fr": "Ontario"},
    "PE": {"en": "Prince Edward Island", "fr": "Île-du-Prince-Édouard"},
    "QC": {"en": "Québec", "fr": "Québec"},
    "SK": {"en": "Saskatchewan", "fr": "Saskatchewan"},
    "YT": {"en": "Yukon", "fr": "Yukon"},
}


def clean_location(latitude, longitude):
    lat, lat_dir = float(latitude[:-1]), latitude[-1:]
    lon, lon_dir = float(longitude[:-1]), longitude[-1:]

    if lat_dir in ["S", "W"]:
        lat *= -1
    if lon_dir in ["S", "W"]:
        lon *= -1

    return lat, lon


def get_raw_station_list():
    """
    Has many of the stations used by environment Canada with more precise
    lat long. This populates a temp collection.
    https://open.canada.ca/data/en/dataset/9764d6c6-3044-450c-ac5a-383cedbfef17
    """
    url = "http://dd.weather.gc.ca/observations/doc/swob-xml_station_list.csv"
    r = app.session.get(url)
    # stations = r.text.splitlines()[1:]
    stations_list = []

    for row in csv.reader(r.text.splitlines()[1:]):
        try:
            stations_list.append(
                {
                    "stationCode": row[0].lower(),
                    "loc": [float(row[5]), float(row[6])],
                }
            )
        except ValueError:
            pass
    _stations = app.db["_stations"]

    _stations.create_index([("loc", pymongo.GEO2D)])
    app.stations_coll.create_index([("loc", pymongo.GEO2D)])
    _stations.insert_many(stations_list)


def populate_stations_and_cities():
    """Grabs and saves the list of cities and finds weather stations as json"""
    en = app.session.get(SITE_LIST_EN_URL).content.decode()
    fr = app.session.get(SITE_LIST_FR_URL).content.decode()

    cities_list = []
    _stations = app.db["_stations"]

    for en_list, fr_list in zip(en.splitlines()[2:], fr.splitlines()[2:]):
        code, name_en, province, lat, lon = en_list.split(",")
        name_fr = fr_list.split(",")[1]

        if province == "HEF":
            continue
        city = {
            "authoritative": True,
            "code": code,
            "province": province,
            "provinceEn": PROVINCES[province]["en"],
            "provinceFr": PROVINCES[province]["fr"],
            "nameEn": name_en,
            "nameFr": name_fr,
            "parentNameEn": name_en,
            "parentNameFr": name_fr,
            "loc": clean_location(lat, lon),
        }

        city["searchIndexEn"] = utils.normalize_string(city["nameEn"])
        city["searchIndexFr"] = utils.normalize_string(city["nameFr"])

        url = utils.WEATHER_URL.format(city["province"], city["code"], "e")
        r = app.session.get(url)
        root_en = etree.fromstring(r.content)

        url = utils.WEATHER_URL.format(city["province"], city["code"], "f")
        r = app.session.get(url)
        root_fr = etree.fromstring(r.content)

        if root_en.find("currentConditions/station") is None:
            continue

        station_code = root_en.find("currentConditions/station").get("code")
        station_doc = app.stations_coll.find_one(
            {"stationCode": station_code.lower()}
        )
        if station_doc:
            station_id = station_doc["_id"]
        else:
            station_id = ObjectId()
            _station_doc = _stations.find_one(
                {"stationCode": station_code.lower()}
            )

            if _station_doc:
                _station_doc.pop("_id")
                _station_doc["nameEn"] = root_en.find(
                    "currentConditions/station"
                ).text
                _station_doc["nameFr"] = root_fr.find(
                    "currentConditions/station"
                ).text
                _station_doc["province"] = city["province"]
                app.stations_coll.insert_one(_station_doc)
            else:
                app.stations_coll.insert_one(
                    {
                        "stationCode": station_code,
                        "province": city["province"],
                        "loc": clean_location(
                            latitude=root_en.find(
                                "currentConditions/station"
                            ).get("lat"),
                            longitude=root_en.find(
                                "currentConditions/station"
                            ).get("lon"),
                        ),
                        "nameEn": root_en.find(
                            "currentConditions/station"
                        ).text,
                        "nameFr": root_fr.find(
                            "currentConditions/station"
                        ).text,
                    }
                )

        city["stationCode"] = station_code
        city["stationId"] = station_id

        city["stationEn"] = root_en.find("currentConditions/station").text
        city["stationFr"] = root_fr.find("currentConditions/station").text

        print(city)
        cities_list.append(city)

    if not all([c.get("stationCode") for c in cities_list]):
        print("Missing stationCode")
        raise Exception()

    app.cities_coll.create_index(
        [
            ("searchIndexEn", pymongo.TEXT),
            ("searchIndexFr", pymongo.TEXT),
            ("code", pymongo.TEXT),
        ]
    )
    app.cities_coll.create_index([("loc", pymongo.GEO2D)])
    app.cities_coll.insert_many(cities_list)
    app.db.drop_collection("_stations")


def init_mongodb():
    """
    In a path when weatheh is:
    python -c  "from weatheh import populate; populate.init_mongodb()"
    """
    app.client.drop_database(app.MONGO_DB_NAME)
    app.client.fsync()

    get_raw_station_list()
    app.client.fsync()

    populate_stations_and_cities()
    app.client.fsync()

    add_more_cities()
    app.client.fsync()

    utils.populate_forecast()


def download_file(folder_name, url, unzip=True, clean_target=True):
    """Helper to download and optionally unzip files"""
    if clean_target:
        shutil.rmtree(folder_name, ignore_errors=True)
        os.mkdir(folder_name)

    file_name = url.split("/")[-1]
    r = app.session.get(url, stream=True)
    if not r.ok:
        raise Exception(r.status_code)

    with open(os.path.join(folder_name, file_name), "wb") as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

    if unzip:
        with zipfile.ZipFile(os.path.join(folder_name, file_name), "r") as z:
            z.extractall(folder_name)


def add_more_cities(download=False):
    # https://open.canada.ca/data/en/dataset/e27c6eba-3c5d-4051-9db2-082dc6411c2c
    codes = ["IR", "UNP", "MTN", "MUN1", "GEOG", "VILG", "CITY", "TOWN", "MUN2"]

    generic_terms = [
        "Post Office",
        "Former Post Office",
        "Abandoned Locality",
        "Railway Junction",
        "Police Village",
        "Military Post Office",
        "Railway Yard",
        "Former Railway Point",
        "Judicial District",
        "Landing",
        "Railway Point",
        "Railway Stop",
    ]

    if download:
        en_url = (
            "http://ftp.geogratis.gc.ca/pub/nrcan_rncan/vector/"
            "geobase_cgn_toponyme/prov_csv_eng/cgn_canada_csv_eng.zip"
        )
        download_file(
            folder_name="temp", url=en_url, unzip=True, clean_target=False
        )

    with open("temp/cgn_canada_csv_eng.csv", newline="") as f:
        geo = list(csv.reader(f))

    provinces = {
        "Alberta": "AB",
        "Northwest Territories": "NT",
        "British Columbia": "BC",
        "Ontario": "ON",
        "Prince Edward Island": "PE",
        "Newfoundland and Labrador": "NL",
        "Manitoba": "MB",
        "Nova Scotia": "NS",
        "Nunavut": "NU",
        "Quebec": "QC",
        "Saskatchewan": "SK",
        "New Brunswick": "NB",
        "Yukon": "YT",
    }

    client = pymongo.MongoClient()
    db = client.weatheh
    cities = db.cities
    stations = db.stations

    results_dict = {}
    for row in geo[1:]:
        (
            cgndb_id,
            name,
            _,
            _,
            generic_term,
            _,
            code,
            toponymic_id,
            lat,
            lon,
            location,
            province,
            _,
            _,
            _,
        ) = row

        if generic_term in generic_terms:
            continue

        if not lat or not lon:
            continue

        name = name.strip()
        cgndb_id = cgndb_id.strip()
        code = code.strip()
        toponymic_id = toponymic_id.strip()

        if code not in codes:
            continue

        if ";" in location:
            loc = location.split(";")
            location = list(set([l.strip() for l in loc if l]))
        else:
            if not location:
                continue
            location = [location]

        if cities.find_one({"nameEn": name, "province": provinces[province]}):
            print("ALREADY SAVED:", name, province, location)
            continue

        print("ADDING SAVED:", name, province, location)

        if name in location:
            # location = [name.strip()]
            is_parent = True
        else:
            is_parent = False

        place = {
            "id": cgndb_id,
            "name": name,
            "code": code,
            "toponymicId": toponymic_id,
            "loc": [float(lat), float(lon)],
            "locations": sorted(location),
            "province": provinces[province],
            "provinceEn": PROVINCES[provinces[province]]["en"],
            "provinceFr": PROVINCES[provinces[province]]["fr"],
            "isParent": is_parent,
        }

        key_name = f"{place['name']},{place['province']}"
        if key_name not in results_dict:
            results_dict[key_name] = [place]
        else:
            if not any(
                [
                    p["locations"] == place["locations"]
                    for p in results_dict[key_name]
                ]
            ):
                results_dict[key_name].append(place)

    results = []

    for name, places in results_dict.items():
        if len(places) == 1:
            results.append(places[0])
            continue

        unique_location_tracker = []
        unique_places = []

        for place in places:
            if any(
                [l in unique_location_tracker for l in place["locations"]]
            ) or any(
                [l["toponymicId"] == place["toponymicId"] for l in places]
            ):
                continue

            unique_location_tracker.extend(place["locations"])
            unique_places.append(place)
        to_add = []

        for place in unique_places:
            for p in unique_places:
                if list(set(p["locations"]).intersection(place["locations"])):
                    continue
                to_add.append(place)
        results.extend(to_add)

    inserts = []
    for city in results:
        station = stations.find_one(
            {"province": city["province"], "loc": {"$near": city["loc"]}}
        )

        if not station:
            print("No station in province", city)
            continue
        parent = cities.find_one(
            {
                "stationId": station["_id"],
                "authoritative": True,
                "province": city["province"],
                "loc": {"$near": city["loc"]},
            }
        )

        if not parent:
            print("No near city with station", city)
            continue

        parent.pop("_id")
        parent["authoritative"] = False
        parent["parentNameEn"] = parent["nameEn"]
        parent["parentNameFr"] = parent["nameFr"]
        parent["nameEn"] = city["name"]
        parent["nameFr"] = city["name"]
        parent["searchIndexEn"] = utils.normalize_string(city["name"])
        parent["searchIndexFr"] = utils.normalize_string(city["name"])
        parent["loc"] = city["loc"]

        inserts.append(parent)
        print("ADDING TO MONGODB", city)

    cities.insert_many(inserts)


if __name__ == '__main__':
    while True:
        start = time.time()
        utils.populate_forecast()
        print(
            datetime.datetime.utcnow(),
            datetime.timedelta(seconds=time.time() - start)
        )
        time.sleep(5 * 60)
