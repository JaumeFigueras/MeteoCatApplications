import argparse
import datetime
import sys

import dateutil.parser
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from load_lightnings_from_api import find_dates_to_issue_requests
from load_lightnings_from_api import run_request


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
    parser.add_argument('-f', '--from-date', help='First date to retrieve data from',
                        default=(datetime.datetime.utcnow().date()-datetime.timedelta(days=1)), nargs='?')
    parser.add_argument('-t', '--to-date', help='Last date to retrieve data from',
                        default=(datetime.datetime.utcnow().date()-datetime.timedelta(days=1)), nargs='?')
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
    if type(args.from_date) == str:
        try:
            # noinspection PyTypeChecker
            args.from_date = dateutil.parser.isoparse(args.from_date).date()
            today = datetime.datetime.utcnow().date()
            if args.from_date > today:
                print("From date requested is grater than today date (UTC)")
                sys.exit(-1)
        except Exception as ex:
            print(ex)
            sys.exit(-1)

    # If the date is not defaulted try to convert to a real date
    if type(args.to_date) == str:
        try:
            # noinspection PyTypeChecker
            args.to_date = dateutil.parser.isoparse(args.to_date).date()
            today = datetime.datetime.utcnow().date()
            if args.to_date > today:
                print("To date requested is grater than today date (UTC)")
                sys.exit(-1)
        except Exception as ex:
            print(ex)
            sys.exit(-1)

    # Check from date is previous tha to date
    if args.from_date > args.to_date:
        print("From date is grater than to date")
        sys.exit(-1)

    while args.from_date <= args.to_date:
        requests_to_perform = find_dates_to_issue_requests(session, args.from_date, -1)
        for request_date in requests_to_perform:
            run_request(args.api_token, session, request_date)
        print("Processed day: {0:}".format(args.from_date.strftime("%Y-%m-%d")))
        args.from_date += datetime.timedelta(days=1)
