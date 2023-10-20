#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest

from src.meteocat_applications.stations_variables.load_variables_from_api import store_variables_to_database
from sqlalchemy.exc import SQLAlchemyError
from meteocat.data_model.variable import Variable
from meteocat.data_model.weather_station import WeatherStation

from typing import List


def test_store_variables_to_database_01(postgresql_schema, db_session, variables_from_station: List[Variable], stations_2022: List[WeatherStation]) -> None:
    cursor = postgresql_schema.cursor()
    station = stations_2022[1]
    db_session.add(station)
    store_variables_to_database(db_session, variables_from_station, station)
    cursor.execute("SELECT COUNT(*) FROM variable")
    result = cursor.fetchone()
    assert result[0] == 24
    cursor.execute("SELECT COUNT(*) FROM weather_station")
    result = cursor.fetchone()
    assert result[0] == 1
    cursor.execute("SELECT COUNT(*) FROM variable_state")
    result = cursor.fetchone()
    assert result[0] == 50


def test_store_variables_to_database_02(postgresql_schema, db_session, variables_from_station: List[Variable], variables_from_station_other: List[Variable], stations_2022: List[WeatherStation]) -> None:
    cursor = postgresql_schema.cursor()
    station = stations_2022[0]
    db_session.add(station)
    store_variables_to_database(db_session, variables_from_station, station)
    cursor.execute("SELECT COUNT(*) FROM variable")
    result = cursor.fetchone()
    assert result[0] == 24
    cursor.execute("SELECT COUNT(*) FROM weather_station")
    result = cursor.fetchone()
    assert result[0] == 1
    cursor.execute("SELECT COUNT(*) FROM variable_state")
    result = cursor.fetchone()
    assert result[0] == 50
    station = stations_2022[2]
    db_session.add(station)
    store_variables_to_database(db_session, variables_from_station_other, station)
    cursor.execute("SELECT COUNT(*) FROM variable")
    result = cursor.fetchone()
    assert result[0] == 24
    cursor.execute("SELECT COUNT(*) FROM weather_station")
    result = cursor.fetchone()
    assert result[0] == 2
    cursor.execute("SELECT COUNT(*) FROM variable_state")
    result = cursor.fetchone()
    assert result[0] == 100


def test_store_variables_to_database_03(postgresql_schema, db_session, variables_from_station: List[Variable], stations_2022: List[WeatherStation]) -> None:
    cursor = postgresql_schema.cursor()
    station = stations_2022[1]
    db_session.add(station)
    db_session.commit()
    cursor.execute("SELECT COUNT(*) FROM variable")
    result = cursor.fetchone()
    assert result[0] == 0
    cursor.execute("SELECT COUNT(*) FROM weather_station")
    result = cursor.fetchone()
    assert result[0] == 1
    cursor.execute("SELECT COUNT(*) FROM variable_state")
    result = cursor.fetchone()
    assert result[0] == 0
    store_variables_to_database(db_session, variables_from_station, station)
    cursor.execute("SELECT COUNT(*) FROM variable")
    result = cursor.fetchone()
    assert result[0] == 24
    cursor.execute("SELECT COUNT(*) FROM weather_station")
    result = cursor.fetchone()
    assert result[0] == 1
    cursor.execute("SELECT COUNT(*) FROM variable_state")
    result = cursor.fetchone()
    assert result[0] == 50
    # with pytest.raises(SQLAlchemyError):
    store_variables_to_database(db_session, variables_from_station, station)
    cursor.execute("SELECT COUNT(*) FROM variable")
    result = cursor.fetchone()
    assert result[0] == 24
    cursor.execute("SELECT COUNT(*) FROM weather_station")
    result = cursor.fetchone()
    assert result[0] == 1
    cursor.execute("SELECT COUNT(*) FROM variable_state")
    result = cursor.fetchone()
    assert result[0] == 50

