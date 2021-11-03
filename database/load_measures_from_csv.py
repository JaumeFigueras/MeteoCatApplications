import argparse
import dateutil.parser
import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import sys
import csv
from gisfire_meteocat_lib.remote_api import meteocat_xema_api as api
from gisfire_meteocat_lib.database.weather_station import WeatherStation
from gisfire_meteocat_lib.database.weather_station import WeatherStationStatus
from gisfire_meteocat_lib.database.meteocat_xema import get_weather_station
from gisfire_meteocat_lib.database.meteocat_xema import get_variable
from gisfire_meteocat_lib.database.measures import Measure


def main(db_session, csv_reader):
    """

    :param db_session:
    :type db_session: sqlalchemy.orm.Session
    :param csv_reader:
    :type csv_reader: csv.Reader
    :return:
    """
    stations = dict()
    variables = dict()
    next(csv_reader) # Remove the header
    i = 0
    for row in csv_reader:
        station_code = row[1]
        variable_code = row[2]
        measure_date = datetime.datetime.strptime(row[3], '%d/%m/%Y %I:%M:%S %p')
        if row[4]:
            measure_date_extreme = datetime.datetime.strptime(row[3], '%d/%m/%Y %I:%M:%S %p')
        else:
            measure_date_extreme = None
        value = float(row[5])
        status = row[6]
        time_basis = row[7]
        measure = Measure(measure_date, value, status, time_basis, measure_date_extreme)
        if not (station_code in stations):
            station = get_weather_station(db_session, station_code)
            stations[station_code] = station
        if not (variable_code in variables):
            variable = get_variable(db_session, variable_code)
            variables[variable_code] = variable
        db_session.add(measure)
        stations[station_code].measures.append(measure)
        variables[variable_code].measures.append(measure)
        if i % 500 == 0:
            db_session.commit()
        if i % 10000 == 0:
            print("Processed {0:} records.".format(i))
        i += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-p', '--port', type=int, help='Database cluster port')
    parser.add_argument('-d', '--database', help='Database name')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-w', '--password', help='Database password')
    parser.add_argument('-f', '--file', help='File to retrieve data from')
    args = parser.parse_args()
    database_connection_string = 'postgresql+psycopg2://' + args.username + ':' + args.password + '@' + args.host +\
                                 ':' + str(args.port) + '/' + args.database
    try:
        engine = create_engine(database_connection_string)
        session = Session(engine)
    except SQLAlchemyError as ex:
        print(ex)
        sys.exit(-1)

    try:
        csv_file = open(args.file)
        reader = csv.reader(csv_file, delimiter=',')
    except Exception as ex:
        print(ex)
        sys.exit(-1)

    main(session, reader)
