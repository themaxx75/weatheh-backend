import datetime
import json
import os
import unicodedata

import lxml.etree as etree
import pytz
from requests.exceptions import RequestException
from urllib3.exceptions import HTTPError
from bson import ObjectId
from weatheh import app

BASE_HOST_DD_WEATHER = "http://dd.weather.gc.ca"
WEATHER_URL = BASE_HOST_DD_WEATHER + "/citypage_weather/xml/{}/{}_{}.xml"

icons_codes_path = os.path.join(os.path.dirname(__file__), "icons_codes.json")
with open(icons_codes_path) as f:
    WEATHER_ICONS = json.load(f)


# def fetch_forecast(
#     city, language="en", current=True, fore_cast=True, hourly=True
# ):
#
#     cached = app.forecasts_coll.find_one(
#         {
#             "stationId": city["stationId"],
#             "current": current,
#             "foreCast": fore_cast,
#             "hourly": hourly,
#             "language": language,
#         }
#     )
#
#     if cached:
#         return cached["weather"]
#
#     url = WEATHER_URL.format(city["province"], city["code"], language[0])
#     try:
#         r = app.session.get(url)
#     except (RequestException, HTTPError):
#         return {}
#
#     try:
#         root = etree.fromstring(r.content)
#     except Exception:
#         print(r.content)
#         raise
#
#     timestamp_format = "%Y%m%d%H%M%S"
#     response = {}
#     if current:
#         response["current"] = {}
#     if fore_cast:
#         response["foreCast"] = []
#     if hourly:
#         response["hourly"] = []
#
#     observation_datetime_utc = None
#     for child in root.findall("currentConditions/dateTime"):
#         if child.get("zone") == "UTC":
#             observation_datetime_utc = datetime.datetime.strptime(
#                 child.find("timeStamp").text, timestamp_format
#             ).replace(tzinfo=pytz.UTC)
#
#     if observation_datetime_utc:
#         observation_datetime_utc = observation_datetime_utc.astimezone(
#             pytz.UTC
#         ).isoformat()
#     response["observationDatetimeUtc"] = observation_datetime_utc
#
#     if current:
#         current_icon_code = getattr(
#             root.find("currentConditions/iconCode"), "text", None
#         )
#         if current_icon_code:
#             response["current"]["iconClass"] = WEATHER_ICONS.get(
#                 current_icon_code, {}
#             ).get("name", "we-na")
#         else:
#             response["current"]["iconClass"] = "we-na"
#
#         response["current"]["iconCode"] = to_int(current_icon_code)
#
#         current_temperature = getattr(
#             root.find("currentConditions/temperature"), "text", None
#         )
#         response["current"]["temperatureFloat"] = to_float(current_temperature)
#         response["current"]["temperature"] = to_float(
#             current_temperature, rounding=0
#         )
#
#         current_description = getattr(
#             root.find("currentConditions/condition"), "text", None
#         )
#         response["current"]["description"] = current_description
#         response["current"]["forecastPeriod"] = response["current"][
#             "description"
#         ]
#
#         current_dew_point = getattr(
#             root.find("currentConditions/dewpoint"), "text", None
#         )
#         response["current"]["dewPoint"] = to_float(current_dew_point)
#
#         current_humidex = getattr(
#             root.find("currentConditions/humidex"), "text", None
#         )
#         response["current"]["humidex"] = to_int(current_humidex)
#
#         current_pressure_kpa = getattr(
#             root.find("currentConditions/pressure"), "text", None
#         )
#         response["current"]["pressureKpa"] = to_float(current_pressure_kpa)
#
#         current_visibility_km = getattr(
#             root.find("currentConditions/visibility"), "text", None
#         )
#         response["current"]["visibilityKm"] = to_float(current_visibility_km)
#
#         current_relative_humidity = getattr(
#             root.find("currentConditions/relativeHumidity"), "text", None
#         )
#         response["current"]["relativeHumidity"] = to_int(
#             current_relative_humidity
#         )
#
#         current_wind_speed = getattr(
#             root.find("currentConditions/wind/speed"), "text", None
#         )
#         response["current"]["windSpeed"] = to_int(current_wind_speed)
#
#         current_wind_gust = getattr(
#             root.find("currentConditions/wind/gust"), "text", None
#         )
#         response["current"]["windGust"] = current_wind_gust
#
#         current_wind_direction = getattr(
#             root.find("currentConditions/wind/direction"), "text", None
#         )
#         response["current"]["windDirection"] = current_wind_direction
#
#         current_wind_bearing_degree = getattr(
#             root.find("currentConditions/wind/bearing"), "text", None
#         )
#         response["current"]["windBearingDegree"] = to_float(
#             current_wind_bearing_degree
#         )
#
#         regional_normals_summary = getattr(
#             root.find("forecastGroup/regionalNormals/textSummary"), "text", None
#         )
#         response["current"]["regionalNormalsSummary"] = regional_normals_summary
#
#         regional_normal_low, regional_normal_high = None, None
#         for normal in root.findall(
#             "forecastGroup//regionalNormals/temperature"
#         ):
#             if normal.get("class") == "high":
#                 regional_normal_high = normal.text
#             if normal.get("class") == "low":
#                 regional_normal_low = normal.text
#         response["current"]["regionalNormalLow"] = to_int(regional_normal_low)
#         response["current"]["regionalNormalHigh"] = to_int(regional_normal_high)
#
#     if fore_cast:
#         for forecast in root.findall("forecastGroup/forecast"):
#             forecast_dict = {}
#
#             period_group = forecast.find("period")
#             period = period_group.get("textForecastName")
#             forecast_dict["forecastPeriod"] = period
#
#             description = getattr(period_group, "text", None)
#             forecast_dict["description"] = description
#
#             summary = getattr(forecast.find("textSummary"), "text", None)
#             forecast_dict["summary"] = summary
#
#             cloud_precipitation = getattr(
#                 forecast.find("cloudPrecip/textSummary"), "text", None
#             )
#             forecast_dict["cloudPrecipitation"] = cloud_precipitation
#
#             icon_code = getattr(
#                 forecast.find("abbreviatedForecast/iconCode"), "text", None
#             )
#             forecast_dict["iconCode"] = to_int(icon_code)
#             if icon_code:
#                 forecast_dict["iconClass"] = WEATHER_ICONS.get(
#                     icon_code, {}
#                 ).get("name", "we-na")
#             else:
#                 forecast_dict["iconClass"] = "we-na"
#
#             temperature = getattr(
#                 forecast.find("temperatures/temperature"), "text", None
#             )
#             forecast_dict["temperature"] = to_int(temperature)
#
#             relative_humidity = getattr(
#                 forecast.find("relativeHumidity"), "text", None
#             )
#             forecast_dict["relativeHumidity"] = to_int(relative_humidity)
#
#             precipitation_summary = getattr(
#                 forecast.find("precipitation/textSummary"), "text", None
#             )
#             forecast_dict["precipitationSummary"] = precipitation_summary
#
#             response["foreCast"].append(forecast_dict)
#
#     if hourly:
#         for hourly in root.findall("hourlyForecastGroup/hourlyForecast"):
#             hourly_dict = {}
#             datetime_utc = datetime.datetime.strptime(
#                 hourly.get("dateTimeUTC"), timestamp_format
#             ).replace(tzinfo=pytz.UTC)
#             hourly_dict["datetimeUtc"] = datetime_utc.isoformat()
#             if hourly_dict["forLocalHour"] == "0":
#                 hourly_dict["forLocalHour"] = "00"
#
#             condition = getattr(hourly.find("condition"), "text", None)
#             hourly_dict["condition"] = condition
#
#             icon_code = getattr(hourly.find("iconCode"), "text", None)
#             hourly_dict["iconCode"] = to_int(icon_code)
#
#             temperature = getattr(hourly.find("temperature"), "text", None)
#             hourly_dict["temperature"] = to_int(temperature)
#
#             humidex = getattr(hourly.find("humidex"), "text", None)
#             hourly_dict["humidex"] = to_int(humidex)
#
#             wind_speed = getattr(hourly.find("wind/speed"), "text", None)
#             hourly_dict["windSpeed"] = to_int(wind_speed)
#
#             wind_direction = getattr(
#                 hourly.find("wind/direction"), "text", None
#             )
#             hourly_dict["windDirection"] = wind_direction
#
#             response["hourly"].append(hourly_dict)
#
#     app.forecasts_coll.insert_one(
#         {
#             "stationId": city["stationId"],
#             "weather": response,
#             "current": current,
#             "foreCast": fore_cast,
#             "hourly": hourly,
#             "language": language,
#             "datetime": datetime.datetime.utcnow(),
#         }
#     )
#     return response


def populate_forecast():
    """ To replace fetch_forecast. Fetching all forecasts and updating"""
    # TODO Optimize by fetching only when needed
    for city in app.cities_coll.find({"authoritative": True}):
        city["weather"] = {}
        for language in ["en", "fr"]:
            url = WEATHER_URL.format(city["province"], city["code"], language[0])
            try:
                r = app.session.get(url)
            except (RequestException, HTTPError) as e:
                print("FAILED", url, e)
                continue

            try:
                root = etree.fromstring(r.content)
            except Exception:
                print(r.content)
                raise

            timestamp_format = "%Y%m%d%H%M%S"
            response = {
                "warnings": [],
                "current": {},
                "longTerm": [],
                "shortTerm": [],
                "hourly": [],
            }

            # Warnings
            for child in root.findall("warnings/event"):
                if child.get("type", "").strip().lower() != "warning":
                    continue
                url = root.find("warnings").get("url")
                warning = {
                    "priority": child.get("priority", "").strip(),
                    "description": child.get("description", "").strip(),
                    "url": url
                }
                if warning["priority"] not in ["low", "high", "urgent"]:
                    print(warning)
                response["warnings"].append(warning)

            # Current
            observation_datetime_utc = None
            for child in root.findall("currentConditions/dateTime"):
                if child.get("zone") == "UTC":
                    observation_datetime_utc = datetime.datetime.strptime(
                        child.find("timeStamp").text, timestamp_format
                    ).replace(tzinfo=pytz.UTC)

            if observation_datetime_utc:
                observation_datetime_utc = observation_datetime_utc.astimezone(
                    pytz.UTC
                ).isoformat()
            response["observationDatetimeUtc"] = observation_datetime_utc

            current_icon_code = getattr(
                root.find("currentConditions/iconCode"), "text", None
            )
            if current_icon_code:
                response["current"]["iconClass"] = WEATHER_ICONS.get(
                    current_icon_code, {}
                ).get("name", "we-na")
            else:
                response["current"]["iconClass"] = "we-na"

            response["current"]["iconCode"] = to_int(current_icon_code)

            current_temperature = getattr(
                root.find("currentConditions/temperature"), "text", None
            )
            response["current"]["temperatureFloat"] = to_float(current_temperature)
            response["current"]["temperature"] = to_float(
                current_temperature, rounding=0
            )

            current_description = getattr(
                root.find("currentConditions/condition"), "text", None
            )
            response["current"]["description"] = current_description
            response["current"]["forecastPeriod"] = response["current"][
                "description"
            ]

            current_dew_point = getattr(
                root.find("currentConditions/dewpoint"), "text", None
            )
            response["current"]["dewPoint"] = to_float(current_dew_point)

            current_humidex = getattr(
                root.find("currentConditions/humidex"), "text", None
            )
            response["current"]["humidex"] = to_int(current_humidex)

            current_pressure_kpa = getattr(
                root.find("currentConditions/pressure"), "text", None
            )
            response["current"]["pressureKpa"] = to_float(current_pressure_kpa)

            current_visibility_km = getattr(
                root.find("currentConditions/visibility"), "text", None
            )
            response["current"]["visibilityKm"] = to_float(current_visibility_km)

            current_relative_humidity = getattr(
                root.find("currentConditions/relativeHumidity"), "text", None
            )
            response["current"]["relativeHumidity"] = to_int(
                current_relative_humidity
            )

            current_wind_speed = getattr(
                root.find("currentConditions/wind/speed"), "text", None
            )
            response["current"]["windSpeed"] = to_int(current_wind_speed)

            current_wind_gust = getattr(
                root.find("currentConditions/wind/gust"), "text", None
            )
            response["current"]["windGust"] = current_wind_gust

            current_wind_direction = getattr(
                root.find("currentConditions/wind/direction"), "text", None
            )
            response["current"]["windDirection"] = current_wind_direction

            current_wind_bearing_degree = getattr(
                root.find("currentConditions/wind/bearing"), "text", None
            )
            response["current"]["windBearingDegree"] = to_float(
                current_wind_bearing_degree
            )

            regional_normals_summary = getattr(
                root.find("forecastGroup/regionalNormals/textSummary"), "text", None
            )
            response["current"]["regionalNormalsSummary"] = regional_normals_summary

            regional_normal_low, regional_normal_high = None, None
            for normal in root.findall(
                "forecastGroup//regionalNormals/temperature"
            ):
                if normal.get("class") == "high":
                    regional_normal_high = normal.text
                if normal.get("class") == "low":
                    regional_normal_low = normal.text
            response["current"]["regionalNormalLow"] = to_int(regional_normal_low)
            response["current"]["regionalNormalHigh"] = to_int(regional_normal_high)

            # Short and long term
            days = [
                "monday",
                "lundi",
                "tuesday",
                "mardi",
                "wednesday",
                "mercredi",
                "thursday",
                "jeudi",
                "friday",
                "vendredi",
                "saturday",
                "samedi",
                "sunday",
                "dimanche"
            ]
            for forecast in root.findall("forecastGroup/forecast"):
                forecast_dict = {}

                period_group = forecast.find("period")
                period = period_group.get("textForecastName")
                forecast_dict["forecastPeriod"] = period

                description = getattr(period_group, "text", None)
                forecast_dict["description"] = description

                summary = getattr(forecast.find("textSummary"), "text", None)
                forecast_dict["summary"] = summary

                cloud_precipitation = getattr(
                    forecast.find("cloudPrecip/textSummary"), "text", None
                )
                forecast_dict["cloudPrecipitation"] = cloud_precipitation

                icon_code = getattr(
                    forecast.find("abbreviatedForecast/iconCode"), "text", None
                )
                forecast_dict["iconCode"] = to_int(icon_code)
                if icon_code:
                    forecast_dict["iconClass"] = WEATHER_ICONS.get(
                        icon_code, {}
                    ).get("name", "we-na")
                else:
                    forecast_dict["iconClass"] = "we-na"

                temperature = getattr(
                    forecast.find("temperatures/temperature"), "text", None
                )
                forecast_dict["temperature"] = to_int(temperature)

                relative_humidity = getattr(
                    forecast.find("relativeHumidity"), "text", None
                )
                forecast_dict["relativeHumidity"] = to_int(relative_humidity)

                precipitation_summary = getattr(
                    forecast.find("precipitation/textSummary"), "text", None
                )
                forecast_dict["precipitationSummary"] = precipitation_summary

                if any([
                    d in forecast_dict["forecastPeriod"].lower() for d in days
                ]):
                    response["longTerm"].append(forecast_dict)
                else:
                    response["shortTerm"].append(forecast_dict)

            # Hourly
            for hourly in root.findall("hourlyForecastGroup/hourlyForecast"):
                hourly_dict = {}
                datetime_utc = datetime.datetime.strptime(
                    hourly.get("dateTimeUTC"), timestamp_format
                ).replace(tzinfo=pytz.UTC)
                hourly_dict["datetimeUtc"] = datetime_utc.isoformat()
                # if hourly_dict["forLocalHour"] == "0":
                #     hourly_dict["forLocalHour"] = "00"

                condition = getattr(hourly.find("condition"), "text", None)
                hourly_dict["condition"] = condition

                icon_code = getattr(hourly.find("iconCode"), "text", None)
                hourly_dict["iconCode"] = to_int(icon_code)

                temperature = getattr(hourly.find("temperature"), "text", None)
                hourly_dict["temperature"] = to_int(temperature)

                humidex = getattr(hourly.find("humidex"), "text", None)
                hourly_dict["humidex"] = to_int(humidex)

                wind_speed = getattr(hourly.find("wind/speed"), "text", None)
                hourly_dict["windSpeed"] = to_int(wind_speed)

                wind_direction = getattr(
                    hourly.find("wind/direction"), "text", None
                )
                hourly_dict["windDirection"] = wind_direction

                response["hourly"].append(hourly_dict)

            city[f"weather"][language] = response

        app.cities_coll.update_one(
            {'_id': city["_id"]}, {"$set": city}, upsert=False
        )

        for c in app.cities_coll.find(
                {"authoritative": False, "code": city["code"]}
        ):
            c["weather"] = city["weather"]
            app.cities_coll.update_one(
                {'_id': c["_id"]}, {"$set": c}, upsert=False
            )


def find_nearest_from_loc(location):
    station = app.stations_coll.find_one({"loc": {"$near": location}})
    if station:
        return app.cities_coll.find_one(
            {"stationId": station["_id"], "loc": {"$near": location}}
        )
    return None


def to_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return val


def to_float(val, rounding=None):
    try:
        if rounding is None:
            return float(val)
        if rounding == 0:
            return round(float(val))

        return round(float(val), rounding)
    except (ValueError, TypeError):
        return val


def normalize_city(doc, language):
    doc["id"] = str(doc["_id"])
    province_full = f"province{language.capitalize()}"
    name = f"name{language.capitalize()}"
    parent_name = f"parentName{language.capitalize()}"
    station = f"station{language.capitalize()}"

    doc["provinceFull"] = doc[province_full]
    doc["name"] = doc[name]
    doc["parentName"] = doc[parent_name]
    doc["station"] = doc[station]

    for k in [
        "_id",
        "stationId",
        "provinceEn",
        "provinceFr",
        "nameEn",
        "nameFr",
        "parentNameEn",
        "parentNameFr",
        "stationEn",
        "stationFr",
    ]:
        doc.pop(k)
    return doc


def normalize_string(input_str):
    # Replacing - to space
    cleaned = input_str.replace("-", " ").lower()

    # Removing useless characters
    cleaned = "".join(
        [x for x in cleaned if x not in "~!@#$%^&*()_=+[{]}\\|;:\",<.>/?"]
    )

    # Cleaning ` to ' to be nice.
    cleaned = cleaned.replace("`", "'")

    # Keeping only single spaces
    cleaned = " ".join(cleaned.split())

    nfkd_form = unicodedata.normalize("NFKD", cleaned)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def process_language(req):
    language = req.params.get("lang")
    if language not in ["en", "fr"]:
        return "en"
    return language
