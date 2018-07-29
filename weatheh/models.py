import unicodedata

import sqlalchemy as sa
from sqlalchemy import orm

from . import app, database, utils


class Station(database.Base):
    """Table to represent EC weather stations, where data is gathered."""

    __tablename__ = "station"

    id = sa.Column(sa.Integer, primary_key=True)
    name_en = sa.Column(sa.String, nullable=False, unique=True)
    name_fr = sa.Column(sa.String, nullable=False, unique=True)
    cities = orm.relationship("City", back_populates="station")
    longitude = sa.Column(sa.Float, nullable=False, unique=False)
    latitude = sa.Column(sa.Float, nullable=False, unique=False)

    def distance_from(self, latitude, longitude):
        return utils.distance(
            [latitude, longitude], [self.latitude, self.longitude]
        )

    def get_name(self, language):
        if language == "en":
            return self.name_en
        return self.name_fr

    def __repr__(self):
        return (
            f"<Station(id={self.id}, "
            f"name_en={self.name_en}, "
            f"name_fr={self.name_fr}, "
            f"latitude={self.latitude}, "
            f"longitude={self.longitude})>"
        )


class City(database.Base):
    """
    Table to represents cities and locations where weather data can be obtained.
    Cities rely on a Station to provide data. This contains all cities
    provided by EC, plus additional cities and location pulled from
    geonames.org. Only cities from EC are authoritative. The non authoritative
    cities/locations are used to provide accurate display city and allow a
    broader search capability.
    """

    __tablename__ = "city"

    id = sa.Column(sa.Integer, primary_key=True)
    code = sa.Column(sa.String(length=8), nullable=False, unique=False)
    province = sa.Column(sa.String, nullable=False, unique=False)
    name_en = sa.Column(sa.String, nullable=False, unique=False)
    name_fr = sa.Column(sa.String, nullable=False, unique=False)
    name_en_unaccented = sa.Column(sa.String, nullable=False, unique=False)
    name_fr_unaccented = sa.Column(sa.String, nullable=False, unique=False)
    station_id = sa.Column(sa.Integer, sa.ForeignKey("station.id"))
    latitude = sa.Column(sa.Float, nullable=False, unique=False)
    longitude = sa.Column(sa.Float, nullable=False, unique=False)
    time_zone = sa.Column(
        sa.String, nullable=False, unique=False, default="UTC"
    )
    authoritative = sa.Column(sa.Boolean)
    station = orm.relationship("Station", back_populates="cities")

    provinces = {
        "AB": {"en": "Alberta", "fr": "Alberta"},
        "BC": {"en": "British Columbia", "fr": "Colombie-Britannique"},
        "MB": {"en": "Manitoba", "fr": "Manitoba"},
        "NB": {"en": "New Brunswick", "fr": "Nouveau-Brunswick"},
        "NL": {
            "en": "Newfoundland and Labrador",
            "fr": "Terre-Neuve-et-Labrador",
        },
        "NS": {"en": "Nova Scotia", "fr": "Nouvelle-Écosse"},
        "NT": {
            "en": "Northwest Territories",
            "fr": "Territoires du Nord-Ouest",
        },
        "NU": {"en": "Nunavut", "fr": "Nunavut"},
        "ON": {"en": "Ontario", "fr": "Ontario"},
        "PE": {"en": "Prince Edward Island", "fr": "Île-du-Prince-Édouard"},
        "QC": {"en": "Québec", "fr": "Québec"},
        "SK": {"en": "Saskatchewan", "fr": "Saskatchewan"},
        "YT": {"en": "Yukon", "fr": "Yukon"},
    }

    def populate_names(self, en, fr):
        """Used as a shortcut to populate all name fields"""
        self.name_en = en
        self.name_fr = fr

        self.name_en_unaccented = remove_accents(en)
        self.name_fr_unaccented = remove_accents(fr)

    def get_name(self, language):
        """Shortcut to get names based on language"""
        if language == "en":
            return self.name_en
        return self.name_fr

    def distance_from(self, latitude, longitude):
        return utils.distance(
            [latitude, longitude], [self.latitude, self.longitude]
        )

    def province_full_name(self, language):
        """Returns the province/territory's full name for a given language."""
        return self.provinces[self.province][language]

    def current_condition(self, language="en", caching=False):
        cache_key = f"City.{self.id}.{language}"

        if caching:
            cached = app.cache.get(cache_key)

            if cached:
                return cached

        result = utils.current_conditions(
            self.province, self.code, language
        )

        if caching:
            app.cache.set(cache_key, result, 120)

        return result

    def to_dict(self, language='en'):
        condition = self.current_condition(language)
        result = {
            "province": {
                "code": self.province,
                "name": self.province_full_name(language),
            },
            "id": self.id,
            "station": {},
            "condition": condition.get("current"),
            # "region": condition["station"]["region"]
            "region": condition.get("station", {}).get("region", "")
        }

        if language == 'en':
            result["name"] = self.name_en
            result["station"]["name"] = self.station.name_en
        else:
            result["name"] = self.name_fr
            result["station"]["name"] = self.station.name_fr

        return result

    def __repr__(self):
        return (
            f"<City(id={self.id}, "
            f"code={self.code}, "
            f"province={self.province}, "
            f"name_en={self.name_en}, "
            f"name_fr={self.name_fr}, "
            f"name_en_unaccented={self.name_en_unaccented}, "
            f"name_fr_unaccented={self.name_fr_unaccented}, "
            f"latitude={self.latitude}, "
            f"longitude={self.longitude}, "
            f"time_zone={self.time_zone}, "
            f"authoritative={self.authoritative}, "
            f"station={self.station.name_en})>"
        )


def remove_accents(input_str):
    nfkd_form = unicodedata.normalize("NFKD", input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])
