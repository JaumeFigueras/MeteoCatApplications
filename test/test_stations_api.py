#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest

from src.meteocat_applications.stations_variables.load_stations_from_api import store_station_to_database
from sqlalchemy.exc import SQLAlchemyError
from gisfire_meteocat_lib.classes.weather_station import WeatherStation

from typing import List


def test_store_station_to_database_01(postgresql_schema, db_session, stations_2022: List[WeatherStation]) -> None:
    count = 0
    cursor = postgresql_schema.cursor()
    for station in stations_2022:
        store_station_to_database(db_session, station)
        cursor.execute("SELECT COUNT(*) FROM meteocat_weather_station")
        result = cursor.fetchone()
        assert result[0] == count + 1
        count += 1


def test_store_station_to_database_02(postgresql_schema, db_session, stations_2022: List[WeatherStation]) -> None:
    cursor = postgresql_schema.cursor()
    for station in stations_2022:
        station.code = None
        with pytest.raises(SQLAlchemyError):
            store_station_to_database(db_session, station)
        cursor.execute("SELECT COUNT(*) FROM meteocat_weather_station")
        result = cursor.fetchone()
        assert result[0] == 0

