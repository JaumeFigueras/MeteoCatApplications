import argparse
import datetime
import sys

import dateutil.parser
import pytz
import requests.exceptions
from meteocat.data_model.lightning import Lightning
from meteocat.data_model.lightning import LightningAPIRequest
from meteocat.api.meteocat_xdde_api import get_lightnings
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session


def find_dates_to_issue_requests(db_session, date, hour):
    """
    Test the database searching for API requests results to check if it is needed to launch a certain request to the
    MeteoCat API service

    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.orm.Session
    :param date: Date to check for the requests
    :type date: datetime.date
    :param hour: Hour to check for the request. If the hour is set to -1 it will check for the whole day
    :type hour: int
    :return: List of dated that need to be requested (no previous request or failed previous request)
    :rtype: list of datetime.datetime
    """
    yesterday = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).date()
    if hour == -1:  # If no hour is provided
        if date <= yesterday:
            hours_to_request = range(0, 24)
        else:
            hours_to_request = range(0, datetime.datetime.utcnow().hour-1)
    else:
        hours_to_request = range(hour, hour+1)
    request_dates = list()
    for i in hours_to_request:
        request_to_query = datetime.datetime(date.year, date.month, date.day, i, 0, 0, tzinfo=pytz.UTC)
        request: LightningAPIRequest = db_session.query(LightningAPIRequest).\
            filter(LightningAPIRequest.date == request_to_query).\
            first()
        if request is None or request.http_status_code != 200:
            request_dates.append(request_to_query)
    return request_dates


def run_request(api_token, db_session, date):
    """
    Request lightning information and stores it to the database structures

    :param api_token: MeteoCat API token provided by the organization
    :type api_token: str
    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.orm.Session
    :param date: Date and hour to request lightning information
    :type date: datetime.datetime
    :return: None
    """
    # with session.begin():  # Open a transaction
    try:
        result = get_lightnings(api_token, date)
        lightning_api_request: LightningAPIRequest = result['lightning_api_request']
        db_session.add(lightning_api_request)
        if lightning_api_request.http_status_code == 200:
            lightnings: [Lightning] = result['lightnings']
            db_session.add_all(lightnings)
        db_session.commit()
    except SQLAlchemyError as e:
        print("Error found in database insert at date {0:}. Rolling back all changes. Exception text: {1:}".format(
            date.strftime("%Y-%m-%d %H:%M:%S"), str(e)))
        db_session.rollback()
    except requests.exceptions.RequestException as e:
        print("Error found in request date {0:}. Exception text: {1:}".format(date.strftime("%Y-%m-%d %H:%M:%S"),
                                                                              str(e)))


if __name__ == "__main__":  # pragma: no cover
    # Config the program arguments
    # noinspection DuplicatedCode
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--api-token', help='MeteoCat key to access its api')
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-p', '--port', type=int, help='Database cluster port')
    parser.add_argument('-d', '--database', help='Database name')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-w', '--password', help='Database password')
    parser.add_argument('-t', '--date', help='Date to retrieve data from',
                        default=(datetime.datetime.utcnow().date()-datetime.timedelta(days=1)), nargs='?')
    parser.add_argument('-o', '--hour', help='Hour to retrieve (will be treated as UTC)', default=-1)
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

    # If the date is not defaulted try to convert to a real date
    if type(args.date) == str:
        try:
            # noinspection PyTypeChecker
            args.date = dateutil.parser.isoparse(args.date).date()
            today = datetime.datetime.utcnow().date()
            if args.date > today:
                print("Date requested is grater than today date (UTC)")
                sys.exit(-1)
        except Exception as ex:
            print(ex)
            sys.exit(-1)

    # If the hour is not defaulted try to convert to a real hour
    if type(args.hour) == str:
        try:
            args.hour = int(args.hour)
            if not (0 <= args.hour < 24):
                print("Hour must be between 0 and 23")
                sys.exit(-1)
        except Exception as ex:
            print(ex)
            sys.exit(-1)

    requests_to_perform = find_dates_to_issue_requests(session, args.date, args.hour)
    for request_date in requests_to_perform:
        run_request(args.api_token, session, request_date)
