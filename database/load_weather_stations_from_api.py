import argparse
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import sys
from gisfire_meteocat_lib.remote_api import meteocat_xema_api as api
from gisfire_meteocat_lib.database.weather_station import WeatherStation
from gisfire_meteocat_lib.database.weather_station import WeatherStationStatus


def main(api_token, db_session):
    weather_stations = api.get_weather_stations(api_token)
    for station in weather_stations:
        w_st = WeatherStation(station['codi'], station['nom'], station['tipus'],
                              float(station['coordenades']['latitud']), float(station['coordenades']['longitud']),
                              station['emplacament'], float(station['altitud']), int(station['municipi']['codi']),
                              station['municipi']['nom'], int(station['comarca']['codi']), station['comarca']['nom'],
                              int(station['provincia']['codi']), station['provincia']['nom'],
                              int(station['xarxa']['codi']), station['xarxa']['nom'])
        for status in station['estats']:
            st_status = WeatherStationStatus(int(status['codi']), status['dataInici'], status['dataFi'])
            w_st.status.append(st_status)
        db_session.add(w_st)
        print("Adding Weather Station: {1:} ({0:})".format(station['codi'], station['nom']))
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
