import argparse
import sys

from gisfire_meteocat_lib.classes.weather_station import WeatherStation
from gisfire_meteocat_lib.classes.variable import Variable
from gisfire_meteocat_lib.classes.variable import VariableCategory
from gisfire_meteocat_lib.classes.relations import WeatherStationVariableStateAssociation
from gisfire_meteocat_lib.classes.relations import WeatherStationVariableTimeBaseAssociation
from gisfire_meteocat_lib.remote_api.meteocat_xema_api import get_variables_from_station
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from typing import List


def get_weather_stations(db_session):  # pragma: no cover
    return db_session.query(WeatherStation).order_by(WeatherStation.code).all()


def store_variables_to_database(db_session: Session, variables: List[Variable], station: WeatherStation) -> None:
    for variable in variables:
        try:
            variable_list = db_session.query(Variable).filter(Variable.code == variable.code).all()
            if len(variable_list) == 0:
                db_session.add(variable)
                process_variable = variable
            else:
                process_variable = variable_list[0]
            if process_variable.category != VariableCategory.CMV or (process_variable.category == VariableCategory.CMV and hasattr(variable, 'states') and variable.states is not None):
                for state in variable.states:
                    db_session.add(state)
                    relation = WeatherStationVariableStateAssociation()
                    relation.station = station
                    relation.variable = process_variable
                    relation.state = state
                    db_session.add(relation)
            for time_base in variable.time_bases:
                db_session.add(time_base)
                relation = WeatherStationVariableTimeBaseAssociation()
                relation.station = station
                relation.variable = process_variable
                relation.time_base = time_base
                db_session.add(relation)
            db_session.commit()
        except SQLAlchemyError as e:
            print("Error found in database inserting association. Rolling back all changes. Exception text: {0:}".
                  format(str(e)))
            db_session.rollback()
            raise e


def main():  # pragma: no cover
    # Config the program arguments
    # noinspection DuplicatedCode
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--api-token', help='MeteoCat key to access its api')
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-p', '--port', type=int, help='Database cluster port')
    parser.add_argument('-d', '--database', help='Database name')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-w', '--password', help='Database password')
    # noinspection DuplicatedCode
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

    stations: List[WeatherStation] = get_weather_stations(session)
    for station in stations:
        try:
            print("getting variables from station {0:} - {1:}".format(station.code, station.name))
            variables = get_variables_from_station(args.api_token, station)
            store_variables_to_database(session, variables, station)
        except SQLAlchemyError:
            return


if __name__ == "__main__":  # pragma: no cover
    main()
