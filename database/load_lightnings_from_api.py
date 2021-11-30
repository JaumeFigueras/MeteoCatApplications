import argparse
import dateutil.parser
import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import sys
import time
import pytz
from gisfire_meteocat_lib.remote_api import meteocat_xdde_api as api
from gisfire_meteocat_lib.database.lightnings import LightningAPIRequest
from gisfire_meteocat_lib.database.lightnings import Lightning
from gisfire_meteocat_lib.database.meteocat_xdde import get_lightning_api_requests


def main(api_token, db_session, date, hour):
    yesterday = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).date()
    today = datetime.datetime.utcnow().date()
    hours_to_request = range(hour, hour+1)
    if hour == -1:
        if date <= yesterday:
            hours_to_request = range(0, 24)
        elif date > today:
            return
        else:
            hours_to_request = range(0, datetime.datetime.utcnow().hour)
    for i in hours_to_request:
        print("Getting data from: " + str(date) + " at hour " + str(i))
        requests = get_lightning_api_requests(db_session,
                                              datetime.datetime(date.year, date.month, date.day, i, 0, 0,
                                                                tzinfo=pytz.UTC),
                                              datetime.datetime(date.year, date.month, date.day, i, 0, 0,
                                                                tzinfo=pytz.UTC) + datetime.timedelta(hours=1))
        if len(requests) == 1:
            if requests[0].result_code == 200:
                print("This request already exists")
                continue
        elif len(requests) > 1:
            print("database inconsistency")
            return
        result = api.get_lightnings(api_token, date, i)
        print(result)
        status = result['status_code']
        count = len(result['data'])
        lar = LightningAPIRequest(datetime.datetime(date.year, date.month, date.day, i, 0, 0, tzinfo=pytz.UTC), status,
                                  count)
        db_session.add(lar)
        for j in range(0, count):
            item = result['data'][j]
            if 'idMunicipi' in item:
                id_municipi = item['idMunicipi']
            else:
                id_municipi = None
            light = Lightning(item['id'], item['data'], item['correntPic'], item['chi2'], item['ellipse']['eixMajor'],
                              item['ellipse']['eixMenor'], item['ellipse']['angle'], item['numSensors'],
                              item['nuvolTerra'], id_municipi, item['coordenades']['latitud'],
                              item['coordenades']['longitud'])
            db_session.add(light)
        db_session.commit()
        time.sleep(0.5)


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
    parser.add_argument('-o', '--hour', help='Hour to retrieve', default=-1)
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
            args.date = dateutil.parser.isoparse(args.date).date()
        except Exception as ex:
            print(ex)
            sys.exit(-1)

    if type(args.hour) == str:
        try:
            args.hour = int(args.hour)
        except Exception as ex:
            print(ex)
            sys.exit(-1)

    main(args.api_token, session, args.date, args.hour)
