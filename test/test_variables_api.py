#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest

from src.meteocat_applications.stations_variables.load_variables_from_api import store_variables_to_database
from sqlalchemy.exc import SQLAlchemyError
from gisfire_meteocat_lib.classes.variable import Variable
from gisfire_meteocat_lib.classes.weather_station import WeatherStation

from typing import List


def test_store_variables_to_database_01(postgresql_schema, db_session, variables_from_station: List[Variable], stations_2022: List[WeatherStation]) -> None:
    cursor = postgresql_schema.cursor()
    station = stations_2022[0]
    db_session.add(station)
    store_variables_to_database(db_session, variables_from_station, station)
    cursor.execute("SELECT COUNT(*) FROM meteocat_variable")
    result = cursor.fetchone()
    assert result[0] == 21
    cursor.execute("SELECT COUNT(*) FROM meteocat_weather_station")
    result = cursor.fetchone()
    assert result[0] == 1
    cursor.execute("SELECT COUNT(*) FROM meteocat_variable_state")
    result = cursor.fetchone()
    assert result[0] == 31


def test_store_variables_to_database_02(postgresql_schema, db_session, variables_from_station: List[Variable], variables_from_station_other: List[Variable], stations_2022: List[WeatherStation]) -> None:
    cursor = postgresql_schema.cursor()
    station = stations_2022[0]
    db_session.add(station)
    store_variables_to_database(db_session, variables_from_station, station)
    cursor.execute("SELECT COUNT(*) FROM meteocat_variable")
    result = cursor.fetchone()
    assert result[0] == 21
    cursor.execute("SELECT COUNT(*) FROM meteocat_weather_station")
    result = cursor.fetchone()
    assert result[0] == 1
    cursor.execute("SELECT COUNT(*) FROM meteocat_variable_state")
    result = cursor.fetchone()
    assert result[0] == 31
    station = stations_2022[2]
    db_session.add(station)
    store_variables_to_database(db_session, variables_from_station_other, station)
    cursor.execute("SELECT COUNT(*) FROM meteocat_variable")
    result = cursor.fetchone()
    assert result[0] == 21
    cursor.execute("SELECT COUNT(*) FROM meteocat_weather_station")
    result = cursor.fetchone()
    assert result[0] == 2
    cursor.execute("SELECT COUNT(*) FROM meteocat_variable_state")
    result = cursor.fetchone()
    assert result[0] == 62


def test_store_variables_to_database_03(db_session, variables_from_station: List[Variable], stations_2022: List[WeatherStation]) -> None:
    station = stations_2022[0]
    db_session.add(station)
    store_variables_to_database(db_session, variables_from_station, station)
    with pytest.raises(SQLAlchemyError):
        store_variables_to_database(db_session, variables_from_station, station)


