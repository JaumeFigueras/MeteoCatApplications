import argparse
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import sys
from gisfire_meteocat_lib.remote_api import meteocat_xema_api as api
from gisfire_meteocat_lib.database.meteocat_xema import get_weather_stations
from gisfire_meteocat_lib.database.meteocat_xema import get_variable
from gisfire_meteocat_lib.database.variable import VariableStatus
from gisfire_meteocat_lib.database.variable import VariableTimeBasis
from gisfire_meteocat_lib.database.variable import WeatherStationVariableTimeBasisAssociation
from gisfire_meteocat_lib.database.weather_station import WeatherStationVariableStatusAssociation


def main(api_token, db_session):
    stations = get_weather_stations(db_session)
    for station in stations:
        station_measured_variables = api.get_station_measured_variables(api_token, station.code)
        station_multivariate_variables = api.get_station_multi_variables(api_token, station.code)
        if station_multivariate_variables is None:
            station_multivariate_variables = list()
        station_auxiliary_variables = api.get_station_auxiliar_variables(api_token, station.code)
        if station_auxiliary_variables is None:
            station_auxiliary_variables = list()
        station_variables = station_measured_variables + station_multivariate_variables + station_auxiliary_variables
        for variable_in_station in station_variables:
            variable = get_variable(db_session, int(variable_in_station['codi']))
            print("Estació: {0:} - Variable: {1:}".format(station.code, variable.name))
            if 'estats' in variable_in_station:
                for status_in_variable in variable_in_station['estats']:
                    status = VariableStatus(int(status_in_variable['codi']), status_in_variable['dataInici'],
                                            status_in_variable['dataFi'])
                    assoc = WeatherStationVariableStatusAssociation(station=station, variable=variable, status=status)
                    db_session.add(assoc)
            if 'basesTemporals' in variable_in_station:
                for time_basis_in_variable in variable_in_station['basesTemporals']:
                    time_basis = VariableTimeBasis(time_basis_in_variable['codi'],
                                                   time_basis_in_variable['dataInici'],
                                                   time_basis_in_variable['dataFi'])
                    assoc = WeatherStationVariableTimeBasisAssociation(station=station, variable=variable,
                                                                       time_basis=time_basis)
                    db_session.add(assoc)
    db_session.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--api-token', help='MeteoCat key to access its api')
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-p', '--port', type=int, help='Database cluster port')
    parser.add_argument('-d', '--database', help='Database name')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-w', '--password', help='Database password')
    args = parser.parse_args()
    database_connection_string = 'postgresql+psycopg2://' + args.username + ':' + args.password + '@' + args.host +\
                                 ':' + str(args.port) + '/' + args.database
    try:
        engine = create_engine(database_connection_string)
        session = Session(engine)
    except SQLAlchemyError as ex:
        print(ex)
        sys.exit(-1)

    main(args.api_token, session)
