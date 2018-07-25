import datetime
import math

import lxml.etree as etree
import pytz
import requests
import json
import os

from . import api, models

BASE_HOST_DD_WEATHER = "http://dd.weather.gc.ca"
WEATHER_URL = BASE_HOST_DD_WEATHER + "/citypage_weather/xml/{}/{}_{}.xml"

icons_codes_path = os.path.join(os.path.dirname(__file__), "icons_codes.json")
with open(icons_codes_path) as f:
    WEATHER_ICONS = json.load(f)


def distance(origin, destination):
    """
    Shamelessly taken from https://stackoverflow.com/a/38187562
    Calculates the distance in km between two lat long
    """
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371  # km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(
        math.radians(lat1)
    ) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return d


def get_current_conditions(station_obj, city_obj=None, language="en"):
    # TODO: Old one.
    if city_obj:
        city = city_obj
    else:
        city = station_obj.cities[0]

    url = WEATHER_URL.format(city.province, city.code, language[0])
    r = requests.get(url)
    root = etree.fromstring(r.content)
    conditions = root.find("currentConditions/condition").text
    temperature = root.find("currentConditions/temperature").text
    humidity = root.find("currentConditions/relativeHumidity").text
    station = root.find("currentConditions/station").text

    # tz = pytz.timezone(nearest["tz"])
    dt, local_dt = None, None

    for child in root.findall("currentConditions/dateTime"):
        if child.get("zone") == "UTC":
            dt = datetime.datetime.strptime(
                child.find("timeStamp").text, "%Y%m%d%H%M%S"
            ).replace(tzinfo=pytz.UTC)
            # local_dt = dt.astimezone(tz)
            break

    result = {
        "city": city.get_name(language),
        "conditions": conditions,
        "temperature_celcius": temperature,
        "humidity": humidity,
        # "distance_from_station": nearest["distance"],
        "station": station,
        # "user_location": {
        #     "longitude": station.longitude,
        #     "latitude": station.latitude,
        # },
        "station_location": {
            "longitude": station_obj.longitude,
            "latitude": station_obj.latitude,
        },
        "observation_datetime": dt,
        "url": url,
    }

    # if dt and local_dt:
    #     result["observation_datetime"] = local_dt.isoformat()

    return result


def _current_conditions(city, language="en"):
    url = WEATHER_URL.format(city.province, city.code, language[0])
    r = requests.get(url)
    root = etree.fromstring(r.content)

    conditions = root.find("currentConditions/condition").text
    temperature = root.find("currentConditions/temperature").text
    humidity = root.find("currentConditions/relativeHumidity").text

    tz = pytz.timezone(city.time_zone)

    taken = None
    for child in root.findall("currentConditions/dateTime"):
        if child.get("zone") == "UTC":
            taken = datetime.datetime.strptime(
                child.find("timeStamp").text, "%Y%m%d%H%M%S"
            ).replace(tzinfo=pytz.UTC)
            break

    if taken:
        taken_local = taken.astimezone(tz).isoformat()
        taken = taken.isoformat()
    else:
        taken_local = None

    result = {
        "city": city.get_name(language),
        "conditions": conditions,
        "temperature_celcius": temperature,
        "humidity": humidity,
        "station": city.station.get_name(language),
        "station_location": {
            "longitude": city.station.longitude,
            "latitude": city.station.latitude,
        },
        "observation_datetime_utc": taken,
        "observation_datetime_local": taken_local,
        "url": url,
    }

    return result


def current_conditions(city, language="en"):
    url = WEATHER_URL.format(city.province, city.code, language[0])
    r = requests.get(url)
    return forecast_xml_parser(r.content)


def find_nearest(lat, lon, results):
    """
    Parses results from City or Station and finds the nearest based on lat lon
    """
    near = None
    for result in results:
        distance_from = result.distance_from(lat, lon)

        if not near:
            near = distance_from, result
            continue
        if distance_from < near[0]:
            near = distance_from, result

    return near[1]


def find_nearest_city_from_location(
    lat, lon, radius=1.5, caching=False
):
    """
    Finds a list of stations based on a given lat lon, and finds the nearest
    city served by the found station.
    """
    cache_key = f"nearest_city_from_location.{lat}{lon}"

    if caching:
        cached = api.cache.get(cache_key)
        if cached:
            return cached

    stations = models.Station.query.filter(
        models.Station.latitude >= lat - radius,
        models.Station.latitude <= lat + radius,
        models.Station.longitude >= lon - radius,
        models.Station.longitude <= lon + radius,
    )

    if not stations.all():
        return None
    station = find_nearest(lat, lon, stations)
    city = find_nearest(lat, lon, station.cities)

    if caching:
        api.cache.set(cache_key, city, 60 * 60)
    return city


def forecast_xml_parser(raw_xml):
    timestamp_format = "%Y%m%d%H%M%S"
    parsed = {"current": {}, "foreCast": [], "hourly": [], "station": {}}
    root = etree.fromstring(raw_xml)

    observation_datetime_utc = None
    utc_offset = 0
    for child in root.findall("currentConditions/dateTime"):
        if child.get("zone") == "UTC":
            observation_datetime_utc = datetime.datetime.strptime(
                child.find("timeStamp").text, timestamp_format
            ).replace(tzinfo=pytz.UTC)
        else:
            utc_offset = int(child.get("UTCOffset"))

    if observation_datetime_utc:
        observation_datetime_utc = observation_datetime_utc.astimezone(
            pytz.UTC
        ).isoformat()
    parsed["observationDatetimeUtc"] = observation_datetime_utc

    region = getattr(root.find("location/region"), "text", None)
    parsed["station"]["region"] = region

    station = root.find("location/name").get("code")
    parsed["station"]["code"] = station

    city = getattr(root.find("location/name"), "text", None)
    parsed["station"]["city"] = city

    current_icon_code = getattr(
        root.find("currentConditions/iconCode"), "text", None
    )
    if current_icon_code:
        parsed["current"]["iconClass"] = WEATHER_ICONS.get(
            current_icon_code, {}
        ).get("name", "we-na")
    else:
        parsed["current"]["iconClass"] = "we-na"

    parsed["current"]["iconCode"] = to_int(current_icon_code)

    current_temperature = getattr(
        root.find("currentConditions/temperature"), "text", None
    )
    parsed["current"]["temperatureFloat"] = to_float(current_temperature)
    parsed["current"]["temperature"] = to_float(
        current_temperature, rounding=0
    )

    current_description = getattr(
        root.find("currentConditions/condition"), "text", None
    )
    parsed["current"]["description"] = current_description
    parsed["current"]["forecastPeriod"] = parsed["current"]["description"]

    current_dew_point = getattr(
        root.find("currentConditions/dewpoint"), "text", None
    )
    parsed["current"]["dewPoint"] = to_float(current_dew_point)

    current_humidex = getattr(
        root.find("currentConditions/humidex"), "text", None
    )
    parsed["current"]["humidex"] = to_int(current_humidex)

    current_pressure_kpa = getattr(
        root.find("currentConditions/pressure"), "text", None
    )
    parsed["current"]["pressureKpa"] = to_float(current_pressure_kpa)

    current_visibility_km = getattr(
        root.find("currentConditions/visibility"), "text", None
    )
    parsed["current"]["visibilityKm"] = to_float(current_visibility_km)

    current_relative_humidity = getattr(
        root.find("currentConditions/relativeHumidity"), "text", None
    )
    parsed["current"]["relativeHumidity"] = to_int(current_relative_humidity)

    current_wind_speed = getattr(
        root.find("currentConditions/wind/speed"), "text", None
    )
    parsed["current"]["windSpeed"] = to_int(current_wind_speed)

    current_wind_gust = getattr(
        root.find("currentConditions/wind/gust"), "text", None
    )
    parsed["current"]["windGust"] = current_wind_gust

    current_wind_direction = getattr(
        root.find("currentConditions/wind/direction"), "text", None
    )
    parsed["current"]["windDirection"] = current_wind_direction

    current_wind_bearing_degree = getattr(
        root.find("currentConditions/wind/bearing"), "text", None
    )
    parsed["current"]["windBearingDegree"] = to_float(
        current_wind_bearing_degree
    )

    regional_normals_summary = getattr(
        root.find("forecastGroup/regionalNormals/textSummary"), "text", None
    )
    parsed["current"]["regionalNormalsSummary"] = regional_normals_summary

    regional_normal_low, regional_normal_high = None, None
    for normal in root.findall("forecastGroup//regionalNormals/temperature"):
        if normal.get("class") == "high":
            regional_normal_high = normal.text
        if normal.get("class") == "low":
            regional_normal_low = normal.text
    parsed["current"]["regionalNormalLow"] = to_int(regional_normal_low)
    parsed["current"]["regionalNormalHigh"] = to_int(regional_normal_high)

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
            forecast_dict["iconClass"] = WEATHER_ICONS.get(icon_code, {}).get(
                "name", "we-na"
            )
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

        parsed["foreCast"].append(forecast_dict)

    current_forecast = {**parsed["current"]}
    current_forecast["cloudPrecipitation"] = current_forecast["description"]
    current_forecast["forecastPeriod"] = "Now"
    # parsed["foreCast"] = [current_forecast] + parsed["foreCast"]

    for hourly in root.findall("hourlyForecastGroup/hourlyForecast"):
        hourly_dict = {}
        datetime_utc = datetime.datetime.strptime(
            hourly.get("dateTimeUTC"), timestamp_format
        ).replace(tzinfo=pytz.UTC)
        hourly_dict["datetimeUtc"] = datetime_utc.isoformat()
        hourly_dict["forLocalHour"] = str(
            (datetime_utc + datetime.timedelta(hours=utc_offset)).hour
        )
        if hourly_dict["forLocalHour"] == "0":
            hourly_dict["forLocalHour"] = "00"

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

        wind_direction = getattr(hourly.find("wind/direction"), "text", None)
        hourly_dict["windDirection"] = wind_direction

        parsed["hourly"].append(hourly_dict)

    return parsed


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
