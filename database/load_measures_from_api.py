import argparse
import dateutil.parser
import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import sys
import time
from gisfire_meteocat_lib.remote_api import meteocat_xema_api as api
from gisfire_meteocat_lib.database.weather_station import WeatherStation
from gisfire_meteocat_lib.database.weather_station import WeatherStationStatus
from gisfire_meteocat_lib.database.meteocat_xema import get_weather_stations
from gisfire_meteocat_lib.database.meteocat_xema import get_variables
from gisfire_meteocat_lib.database.measures import Measure


def main(api_token, db_session, date):
    weather_stations = get_weather_stations(db_session, True, date)
    for station in weather_stations:
        variables = get_variables(db_session, station.code, date)
        for variable in variables:
            ms = list()
            if variable.category == 'DAT':
                ms = api.get_measures_of_station_measured_variables(api_token, station.code, variable.code, date)
            elif variable.category == 'CMV':
                ms = api.get_measures_of_station_multi_variables(api_token, station.code, variable.code, date)
            elif variable.category == 'CMV':
                ms = api.get_measures_of_station_auxiliar_variables(api_token, station.code, variable.code, date)
            print("Station: " + station.code + " - Variable: " + str(variable.code) + " " + variable.name +
                  " - Number of records: " + str(len(ms)))
            for m in ms:
                measure = Measure(m['data'], m['valor'], m['estat'], m['baseHoraria'])
                if 'dataExtrem' in m:
                    measure.date_extreme_record = m['dataExtrem']
                db_session.add(measure)
                variable.measures.append(measure)
                station.measures.append(measure)
            time.sleep(0.5)
        db_session.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--api-token', help='MeteoCat key to access its api')
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-p', '--port', type=int, help='Database cluster port')
    parser.add_argument('-d', '--database', help='Database name')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-w', '--password', help='Database password')
    parser.add_argument('-t', '--date', help='Date to retrieve data from',
                        default=(datetime.datetime.utcnow().date()-datetime.timedelta(days=1)), nargs='?')
    args = parser.parse_args()
    database_connection_string = 'postgresql+psycopg2://' + args.username + ':' + args.password + '@' + args.host +\
                                 ':' + str(args.port) + '/' + args.database
    try:
        engine = create_engine(database_connection_string)
        session = Session(engine)
    except SQLAlchemyError as ex:
        print(ex)
        sys.exit(-1)

    if type(args.date) == str:
        try:
            args.date = dateutil.parser.isoparse(args.date)
        except Exception as ex:
            print(ex)
            sys.exit(-1)

    main(args.api_token, session, args.date)
