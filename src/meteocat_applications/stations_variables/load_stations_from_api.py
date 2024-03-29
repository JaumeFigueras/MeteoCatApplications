import argparse
import sys

from meteocat.data_model.weather_station import WeatherStation
from meteocat.api.meteocat_xema_api import get_weather_stations
from sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session


def store_station_to_database(db_session: Session, station: WeatherStation) -> None:
    try:
        db_session.add(station)
        # db_session.add_all(station.states)
        db_session.commit()
    except SQLAlchemyError as e:
        print("Error found in database insert for station {0:}. Rolling back all changes. Exception text: {1:}".format(
            station.code, str(e)))
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
    database_url = URL.create('postgresql+psycopg', username=args.username, password=args.password, host=args.host,
                              port=args.port, database=args.database)
    try:
        engine = create_engine(database_url)
        session = Session(engine)
    except SQLAlchemyError as ex:
        print(ex)
        sys.exit(-1)

    stations = get_weather_stations(args.api_token)
    for station in stations:
        try:
            store_station_to_database(session, station)
        except SQLAlchemyError:
            return


if __name__ == "__main__":  # pragma: no cover
    main()
