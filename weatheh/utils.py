import datetime
import json
import os
import pathlib
import shutil
import sqlite3
import zipfile

import lxml.etree as etree
import requests

from weatheh import database, exceptions, models, weather

SITE_LIST_EN_URL = (
    weather.BASE_HOST_DD_WEATHER + "/citypage_weather/docs/site_list_en.csv"
)
SITE_LIST_FR_URL = (
    weather.BASE_HOST_DD_WEATHER + "/citypage_weather/docs/site_list_fr.csv"
)


GEONAMES_CANADA_DUMP = "http://download.geonames.org/export/dump/CA.zip"
GEONAMES_JSON = "geonames.json"
STATIONS_JSON = "stations.json"
CITIES_JSON = "cities.json"
ICONS_URL = "https://weather.gc.ca/weathericons/"
ICONS_DESCRIPTIONS = ("http://dd.weather.gc.ca/citypage_weather/docs/"
                      "current_conditions_icon_code_descriptions_e.csv")


def fetch_weather_icons():
    target = "raw_icons"
    shutil.rmtree(target, ignore_errors=True)
    os.mkdir(target)
    for i in range(1000):
        for p in ["", "small/"]:
            for extension in ["gif", "png"]:
                url = "{}{}{}.{}".format(
                    ICONS_URL, p, str(i).zfill(2), extension
                )
                try:
                    download_file(
                        folder_name=target,
                        url=url,
                        unzip=False,
                        clean_target=False,
                    )
                    print(url)
                except exceptions.GroundhogDownloadError:
                    pass


def build_icons_json():
    """Fetch icons description and builds json with values"""
    icons_csv = requests.get(ICONS_DESCRIPTIONS).content.decode()
    description_dict = {
        "Day Conditions Only": "d",
        "Day and Night Conditions": "dn",
        "Night Conditions Only": "n",
    }

    result_dict = {}

    for raw_icon in icons_csv.splitlines()[3:]:
        code, name, description = raw_icon.split(',')

        code = int(code)

        if code not in result_dict:
            result_dict[code] = []

        current = {
            "name": name.lower().replace(" ", "-"),
            "when": description_dict[description]
        }

        result_dict[code].append(current)

    names_unique = []
    icons = {}

    for code, values in result_dict.items():
        names = [f"wi-{n['name']}-{n['when']}" for n in values
                 if f"wi-{n['name']}-{n['when']}" not in names_unique]
        names = sorted(names, key=len)

        # if not names:
        #     print(code, values)
        #     continue

        icons[code] = names[0]
        names_unique.append(names[0])

    with open("icons_codes.json", "w+") as f:
        f.write(json.dumps(icons))


def clean_location(latitude, longitude):
    lat, lat_dir = float(latitude[:-1]), latitude[-1:]
    lon, lon_dir = float(longitude[:-1]), longitude[-1:]

    if lat_dir in ["S", "W"]:
        lat *= -1
    if lon_dir in ["S", "W"]:
        lon *= -1

    return lat, lon


def populate_stations_and_cities_json():
    """Grabs and saves the list of cities and finds weather stations as json"""

    en = requests.get(SITE_LIST_EN_URL).content.decode()
    fr = requests.get(SITE_LIST_FR_URL).content.decode()

    cities = []
    for en_list, fr_list in zip(en.splitlines()[2:], fr.splitlines()[2:]):
        print(en_list)
        code, name_en, province, lat, lon = en_list.split(",")
        name_fr = fr_list.split(",")[1]

        if province == "HEF":
            continue
        if not lat or not lon:
            continue

        latitude, longitude = clean_location(lat, lon)
        cities.append(
            {
                "code": code,
                "province": province,
                "name_en": name_en,
                "name_fr": name_fr,
                "latitude": latitude,
                "longitude": longitude,
            }
        )

        print("parsing city", code)

    print("Done parsing cities")

    stations = []
    filtered_cities = []
    for city in cities:
        url = weather.WEATHER_URL.format(city["province"], city["code"], "e")
        r = requests.get(url)
        root = etree.fromstring(r.content)

        url = weather.WEATHER_URL.format(city["province"], city["code"], "e")
        r = requests.get(url)
        root_fr = etree.fromstring(r.content)

        if root.find("currentConditions/station") is None:
            continue

        latitude, longitude = clean_location(
            root.find("currentConditions/station").get("lat"),
            root.find("currentConditions/station").get("lon"),
        )
        station = {
            "name_en": root.find("currentConditions/station").text,
            "name_fr": root_fr.find("currentConditions/station").text,
            "latitude": latitude,
            "longitude": longitude,
        }
        if station not in stations:
            print("adding station", station)
            stations.append(station)
        city["station"] = station
        filtered_cities.append(city)

    for dump in [(STATIONS_JSON, stations), (CITIES_JSON, filtered_cities)]:
        with open(dump[0], "w+") as f:
            f.write(json.dumps(dump[1]))


def populate_stations_and_cities_tables():
    """Populates tables with pre-made local json data"""

    database.init_db()
    database.Base.metadata.drop_all(database.engine)
    database.init_db()

    with open(STATIONS_JSON) as f:
        stations = json.load(f)

    with open(CITIES_JSON) as f:
        cities = json.load(f)

    new_stations = []
    try:
        for station in stations:
            new_stations.append(
                models.Station(
                    name_en=station["name_en"],
                    name_fr=station["name_fr"],
                    latitude=station["latitude"],
                    longitude=station["longitude"],
                )
            )
        database.db_session.add_all(new_stations)
        database.db_session.commit()

        for c in cities:
            query = models.Station.query.filter(
                models.Station.name_en == c["station"]["name_en"]
            )
            station = query.one()
            city = models.City(
                code=c["code"],
                province=c["province"],
                latitude=c["latitude"],
                longitude=c["longitude"],
                authoritative=True,
            )
            city.populate_names(c["name_en"], c["name_fr"])
            database.db_session.add(city)
            database.db_session.flush()
            print(city.name_en)
            station.cities.append(city)
            database.db_session.commit()

    finally:
        database.db_session.close()


def populate_cities_tz_and_non_authoritative(force_update=False):
    """
    Triggers populating local json format from geonames.org CSV dump and
    populates missing time zones.
    Also adds cities if they are missing, as non-authoritative

    """
    cities = models.City.query.all()

    geonames = get_geonames_json(force_update)
    codes = [
        "PPL",
        "PPLA",
        "PPLA2",
        "PPLA3",
        "PPLA4",
        "PPLC",
        "PPLCH",
        "PPLF",
        "PPLG",
        "PPLH",
        "PPLL",
        "PPLQ",
        "PPLR",
        "PPLS",
        "PPLW",
        "PPLX",
        "STLMT",
        "RESV",
    ]

    try:
        for city in geonames:
            if city["feature_code"] not in codes:
                continue
            in_table_city = models.City.query.filter(
                models.City.name_fr.ilike(city["name"])
                | models.City.name_en.ilike(city["name"])
            )
            if in_table_city.count() == 1:
                c = in_table_city.one()
                c.time_zone = city["timezone"]
                database.db_session.commit()
                continue

            near_city = weather.find_nearest_city_from_location(
                city["latitude"], city["longitude"], radius=5
            )
            if not near_city:
                print("NO NEAR STATION", city)
                continue

            c = models.City(
                code=near_city.code,
                province=city["province"],
                latitude=city["latitude"],
                longitude=city["longitude"],
                time_zone=city["timezone"],
                station=near_city.station,
                authoritative=False,
            )
            c.populate_names(city["name"], city["name"])
            database.db_session.add(c)
            database.db_session.commit()

    finally:
        database.db_session.close()


def get_geonames_json(force_update=False):
    """
    Fetches geonames.org data dump for Canada, and builds a json with relevant
    and normalized data. If local json exists, returns it instead.
    """
    if pathlib.Path(GEONAMES_JSON).is_file() and not force_update:
        with open(GEONAMES_JSON) as f:
            return json.load(f)

    folder_name = "temp"
    codes_to_provinces = {
        1: "AB",
        2: "BC",
        3: "MB",
        4: "NB",
        5: "NL",
        7: "NS",
        8: "ON",
        9: "PE",
        10: "QC",
        11: "SK",
        12: "YT",
        13: "NT",
        14: "NU",
    }
    city_list = []
    try:
        download_file(folder_name=folder_name, url=GEONAMES_CANADA_DUMP)
        with open(os.path.join(folder_name, "CA.txt")) as f:
            for line in f:
                row = line.split("\t")

                city_dict = {
                    "geonameid": int(row[0].strip()),
                    "name": row[1].strip(),
                    "asciiname": row[2].strip(),
                    "alternatenames": [
                        x for x in row[3].strip().split(",") if x
                    ],
                    "latitude": row[4].strip(),
                    "longitude": row[5].strip(),
                    "feature_class": row[6].strip(),
                    "feature_code": row[7].strip(),
                    "country_code": row[8].strip(),
                    "cc2": row[9].strip(),
                    "admin1_code": row[10].strip(),
                    "admin2_code": row[11].strip(),
                    "admin3_code": row[12].strip(),
                    "admin4_code": row[13].strip(),
                    "population": row[14].strip(),
                    "elevation": row[15].strip(),
                    "dem": row[16].strip(),
                    "timezone": row[17].strip(),
                    "modification_date": row[18].strip(),
                }

                if any(
                    [
                        v == ""
                        for k, v in city_dict.items()
                        if k
                        in ["latitude", "longitude", "timezone", "admin1_code"]
                    ]
                ):
                    continue

                province = codes_to_provinces.get(int(city_dict["admin1_code"]))

                if not province:
                    continue

                city_dict["admin1_code"] = int(city_dict["admin1_code"])
                city_dict["province"] = province
                city_dict["latitude"] = float(city_dict["latitude"])
                city_dict["longitude"] = float(city_dict["longitude"])

                try:
                    city_dict["admin2_code"] = int(city_dict["admin2_code"])
                except ValueError:
                    pass

                try:
                    city_dict["admin3_code"] = int(city_dict["admin3_code"])
                except ValueError:
                    pass

                try:
                    city_dict["admin4_code"] = int(city_dict["admin4_code"])
                except ValueError:
                    pass

                try:
                    city_dict["population"] = int(city_dict["population"])
                except ValueError:
                    city_dict["population"] = 0

                try:
                    city_dict["elevation"] = int(city_dict["elevation"])
                except ValueError:
                    city_dict["elevation"] = 0

                try:
                    city_dict["dem"] = int(city_dict["dem"])
                except ValueError:
                    city_dict["dem"] = 0

                for k in list(city_dict.keys()):
                    if city_dict[k] == "":
                        city_dict[k] = None

                print(city_dict)
                city_list.append(city_dict)
    finally:
        shutil.rmtree(folder_name, ignore_errors=True)

    with open(GEONAMES_JSON, "w+") as f:
        f.write(json.dumps(city_list))

    return city_list


def download_file(folder_name, url, unzip=True, clean_target=True):
    """Helper to download and optionally unzip files"""
    if clean_target:
        shutil.rmtree(folder_name, ignore_errors=True)
        os.mkdir(folder_name)

    file_name = url.split("/")[-1]
    r = requests.get(url, stream=True)
    if not r.ok:
        raise exceptions.GroundhogDownloadError(r.status_code)

    with open(os.path.join(folder_name, file_name), "wb") as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

    if unzip:
        with zipfile.ZipFile(os.path.join(folder_name, file_name), "r") as z:
            z.extractall(folder_name)


def find_missing_timezone(geo_username):
    """
    Uses geonames.org's API to fetch missing time zones that were not populated
    with populate_cities_tz_and_non_authoritative.
    """
    url = "http://api.geonames.org/timezoneJSON"
    params = {
        "formatted": True,
        "lat": None,
        "lng": None,
        "username": geo_username,
        "style": "full",
    }

    cities = models.City.query.filter(models.City.time_zone == "UTC")

    for city in cities:
        print(city)
        params["lat"] = city.latitude
        params["lng"] = city.longitude
        r = requests.get(url, params=params).json()
        if r.get("timezoneId"):
            city.time_zone = r["timezoneId"]
            database.db_session.commit()
            print("Saved!")
        else:
            print("Missed!", r)
        print()


def build_new_database(geo_username, clear_local_cache=True):
    """Starts a new database (or clears existing if clear_local_cache is False)
       from new data. When starting from scratch
   """
    if clear_local_cache:
        for f in [CITIES_JSON, STATIONS_JSON, GEONAMES_JSON, database.DB_FILE]:
            try:
                os.remove(f)
                print("Removed", f)
            except FileNotFoundError:
                print(f, "already deleted")
                pass
    if clear_local_cache:
        print("populate_stations_and_cities_json")
        populate_stations_and_cities_json()

    print("populate_stations_and_cities_tables")
    populate_stations_and_cities_tables()

    print("populate_cities_tz_and_non_authoritative")
    populate_cities_tz_and_non_authoritative(force_update=clear_local_cache)

    print("find_missing_timezone")
    find_missing_timezone(geo_username)


def export_database():
    """Creates a dump of the current SQLite3 database"""
    now = datetime.datetime.utcnow()
    name = "{}.sql".format(now.strftime("%Y-%m-%dT%Hh%M-UTC"))
    con = sqlite3.connect(database.DB_FILE)
    with open(name, "w+") as f:
        for line in con.iterdump():
            f.write("%s\n" % line)
    return name
