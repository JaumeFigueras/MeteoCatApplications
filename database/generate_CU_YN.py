import argparse
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import sys
from gisfire_meteocat_lib.database.weather_station import WeatherStation
from gisfire_meteocat_lib.database.weather_station import WeatherStationStatus
from gisfire_meteocat_lib.database.meteocat_xema import get_weather_station


def main(db_session):
    """

    :param db_session:
    :type db_session: sqlalchemy.orm.Session
    :return:
    """
    yn = get_weather_station(db_session, 'YN')
    cu = WeatherStation('CU', 'Vielha', yn.category, 42.69856,  0.79397, 'Prat de Tixineret',  1002,
                        yn.municipality_code, yn.municipality_name, yn.county_code, yn.county_name, yn.province_code,
                        yn.province_name, yn.network_code, yn.network_name)
    st_1 = WeatherStationStatus(2, '1996-02-15 00:00:00Z', yn.status[0].from_date)
    st_2 = WeatherStationStatus(1, yn.status[0].from_date)
    cu.status.append(st_1)
    cu.status.append(st_2)
    db_session.add(cu)
    db_session.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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

    main(session)
