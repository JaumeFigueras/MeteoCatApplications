#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import argparse
import datetime
import dateutil.parser
import time

import pytz
import sqlalchemy.exc
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import sys
import csv
from gisfire_meteocat_lib.classes.measure import Measure
from gisfire_meteocat_lib.classes.measure import MeasureValidityCategory
from gisfire_meteocat_lib.classes.measure import MeasureTimeBaseCategory
from gisfire_meteocat_lib.classes.weather_station import WeatherStation
from gisfire_meteocat_lib.classes.variable import Variable
from gisfire_meteocat_lib.classes.variable import VariableState
from gisfire_meteocat_lib.classes.variable import VariableTimeBase
from gisfire_meteocat_lib.classes.variable import VariableStateCategory
from gisfire_meteocat_lib.classes.variable import VariableTimeBaseCategory
from gisfire_meteocat_lib.classes.relations import WeatherStationVariableStateAssociation
from gisfire_meteocat_lib.classes.relations import WeatherStationVariableTimeBaseAssociation
from typing import List
from typing import Dict
from typing import Union


def get_weather_stations_as_dict(db_session: sqlalchemy.orm.Session) -> Dict[str, WeatherStation]:  # pragma: no cover
    """
    TODO:
    :param db_session:
    :type db_session:
    :return:
    :rtype:
    """
    stations_in_database: List[WeatherStation] = db_session.query(WeatherStation).order_by(WeatherStation.code).all()
    return {station.code: station for station in stations_in_database}


def get_variables_as_dict(db_session: sqlalchemy.orm.Session) -> Dict[int, Variable]:  # pragma: no cover
    """
    TODO:
    :param db_session:
    :type db_session:
    :return:
    :rtype:
    """
    variables_in_database: List[Variable] = db_session.query(Variable).order_by(Variable.code).all()
    return {variable.code: variable for variable in variables_in_database}


def process_measure(db_session: sqlalchemy.orm.Session, csv_row: List[str],
                    stations_in_database: Dict[str, WeatherStation],
                    variables_in_database: Dict[int, Variable],
                    write_to_database: bool) -> None:
    """
    TODO:
    :param db_session:
    :type db_session:
    :param csv_row:
    :type csv_row:
    :param stations_in_database: 
    :type stations_in_database: 
    :param variables_in_database:
    :type variables_in_database:
    :param write_to_database:
    :type write_to_database:
    :return:
    :rtype:
    """
    measure_id: str = csv_row[0]
    station_code: str = csv_row[1]
    variable_code: int = int(csv_row[2])
    utc_tz = pytz.timezone("UTC")
    measure_date: datetime.datetime = datetime.datetime.strptime(csv_row[3], "%d/%m/%Y %I:%M:%S %p")
    measure_date = utc_tz.localize(measure_date)
    extreme_measure_date: Union[None, datetime.datetime] = None
    if csv_row[4] != '':
        extreme_measure_date = datetime.datetime.strptime(csv_row[4], "%d/%m/%Y %I:%M:%S %p")
    value: float = float(csv_row[5])
    measure_status: MeasureValidityCategory = MeasureValidityCategory(csv_row[6])
    time_base: MeasureTimeBaseCategory = MeasureTimeBaseCategory(csv_row[7])
    # Check the weather station is in the database
    if station_code not in stations_in_database:
        print("Weather station {0:} not found".format(station_code))
        sys.exit()
    current_station: WeatherStation = stations_in_database[station_code]
    # Check the variable is in the database
    if variable_code not in variables_in_database:
        print("Variable {0:} not found".format(variable_code))
        sys.exit()
    current_variable: Variable = variables_in_database[variable_code]
    # Station and variable exist, so the WeatherStationVariableStateAssociation has to be tested and has to be active
    states_of_variable_in_station: List[WeatherStationVariableStateAssociation] = db_session.\
        query(WeatherStationVariableStateAssociation).\
        filter(WeatherStationVariableStateAssociation.station == current_station).\
        filter(WeatherStationVariableStateAssociation.variable == current_variable).\
        all()
    # IMPROVE: This code can be improved with a more complex join in the query
    if states_of_variable_in_station is None:
        print("State of Variable {0:} in station {1:} not found".format(variable_code, station_code))
        sys.exit()
    variable_states: List[VariableState] = [state_of_variable.state for state_of_variable in states_of_variable_in_station]
    variable_state: VariableState
    found: bool = False
    for variable_state in variable_states:
        if (variable_state.from_date <= measure_date and variable_state.to_date is None) or (variable_state.from_date <= measure_date < variable_state.to_date):
            if variable_state.code == VariableStateCategory.ACTIVE:
                found = True
    if not found:
        print("Active state of Variable {0:} in station {1:} not found for date {2:}".format(variable_code, station_code, measure_date.strftime("%Y-%m-%d %H:%M:%S")))
        sys.exit()
    # END IMPROVE
    # Station and variable exist, so the WeatherStationVariableTimeBaseAssociation has to be tested and has to be the same time base as the measure
    time_base_of_variable_in_station: List[WeatherStationVariableTimeBaseAssociation] = db_session.\
        query(WeatherStationVariableTimeBaseAssociation).\
        filter(WeatherStationVariableTimeBaseAssociation.station == current_station).\
        filter(WeatherStationVariableTimeBaseAssociation.variable == current_variable).\
        all()
    # IMPROVE: This code can be improved with a more complex join in the query
    if time_base_of_variable_in_station is None:
        print("Time base of Variable {0:} in station {1:} not found".format(variable_code, station_code))
        sys.exit()
    variable_time_bases: List[VariableTimeBase] = [time_base_of_variable.time_base for time_base_of_variable in time_base_of_variable_in_station]
    variable_time_base: VariableTimeBase
    found: bool = False
    for variable_time_base in variable_time_bases:
        if (variable_time_base.from_date <= measure_date and variable_time_base.to_date is None) or (variable_time_base.from_date <= measure_date < variable_time_base.to_date):
            if variable_time_base.code.value == time_base.value:
                found = True
    if not found:
        print(VariableTimeBase(time_base.value).code)
        print("Time base of Variable {0:} in station {1:} not found for date {2:} with value {3:}".format(variable_code, station_code, measure_date.strftime("%Y-%m-%d %H:%M:%S"), time_base))
        sys.exit()
    # END IMPROVE
    # All tests are OK so the data can be written to the database
    if write_to_database:
        try:
            m: Measure = Measure(meteocat_id=measure_id, date=measure_date, date_extreme=extreme_measure_date, value=value, validity_state=measure_status, time_base=time_base)
            m.station = stations_in_database[station_code]
            m.variable = variables_in_database[variable_code]
            db_session.add(m)
            db_session.commit()
        except SQLAlchemyError as e:
            print("Error found in database inserting MEASURE. Rolling back all changes. Exception text: {0:}".
                  format(str(e)))
            db_session.rollback()
            raise e


if __name__ == "__main__":  # pragma: no cover
    # Config the program arguments
    # noinspection DuplicatedCode
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-p', '--port', type=int, help='Database cluster port')
    parser.add_argument('-d', '--database', help='Database name')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-w', '--password', help='Database password')
    parser.add_argument('-f', '--file', help='File to retrieve data from')
    parser.add_argument('-e', '--from-date', help='First date to retrieve data from', default=None, nargs='?')
    parser.add_argument('-t', '--to-date', help='Last date to retrieve data from', default=None, nargs='?')
    parser.add_argument('-W', '--write', help='Last date to retrieve data from', default=None, nargs='?')

    args = parser.parse_args()

    # Create the database session with SQL Alchemy
    database_connection_string = 'postgresql+psycopg2://' + args.username + ':' + args.password + '@' + args.host +\
                                 ':' + str(args.port) + '/' + args.database
    try:
        engine = create_engine(database_connection_string)
        session = Session(engine)
    except SQLAlchemyError as ex:
        print(ex)
        sys.exit(-1)

    # Create the CSV file reader
    try:
        csv_file = open(args.file)
        reader = csv.reader(csv_file, delimiter=',')
        next(reader, None)  # Skip header
    except Exception as ex:
        print(ex)
        sys.exit(-1)

    timezone = pytz.timezone("UTC")
    if args.from_date is not None:
        from_date = dateutil.parser.isoparse(args.from_date)
        if from_date.tzinfo is None or from_date.tzinfo.utcoffset(from_date) is None:
            from_date = timezone.localize(from_date)
    else:
        from_date = datetime.datetime(1900, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
    if args.to_date is not None:
        to_date = dateutil.parser.isoparse(args.to_date)
        if to_date.tzinfo is None or to_date.tzinfo.utcoffset(to_date) is None:
            to_date = timezone.localize(to_date)
    else:
        to_date = datetime.datetime(2100, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
    if args.write is not None:
        write = args.write == 'True' or args.write == 'true'
    else:
        write = False

    start = time.time()
    stations = get_weather_stations_as_dict(session)
    variables = get_variables_as_dict(session)
    str_date = None
    for row in reader:
        if row[3] != str_date:
            str_date = row[3]
            print(str_date, ' - Elapsed time: ', int(time.time() - start)), ' s'
        current_date = datetime.datetime.strptime(row[3], "%d/%m/%Y %I:%M:%S %p")
        current_date = timezone.localize(current_date)
        if from_date <= current_date < to_date:
            process_measure(session, row, stations, variables, write)

