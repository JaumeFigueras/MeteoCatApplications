#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import datetime

import pytz
import sqlalchemy.exc
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import sys
import csv
from gisfire_meteocat_lib.classes.lightning import Lightning
from gisfire_meteocat_lib.classes.lightning import LightningAPIRequest


def process_lightnings(db_session, csv_reader):
    """
    Process a CSV file with the lightning information (the CSV file is obtained from MeteoCat) and stores in a database.
    In case of error the data insertions are rolled back.

    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.orm.Session
    :param csv_reader: CSV file reader
    :type csv_reader: csv.Reader
    :return: The processed year
    :rtype: int
    """
    next(csv_reader)  # Remove the header
    i = 0  # Counter for information purposes
    lightning = None
    for row in csv_reader:
        lightning = Lightning()
        try:
            lightning.meteocat_id = int(row[0])
            lightning.date = datetime.datetime.strptime(row[1] + ' UTC', '%Y-%m-%d %H:%M:%S.%f %Z')
            lightning.peak_current = float(row[2])
            lightning.chi_squared = float(row[3])
            lightning.ellipse_major_axis = float(row[4])
            lightning.ellipse_minor_axis = float(row[5])
            lightning.number_of_sensors = int(row[6])
            lightning.hit_ground = row[7] == 't'
            lightning.municipality_id = int(row[8]) if row[8] != '' else None
            lightning.lon = float(row[9])
            lightning.lat = float(row[10])
        except ValueError as e:
            print("Error found in record {0:}. Rolling back all changes. Exception text: {1:}".format(i, str(e)))
            db_session.rollback()
            return None
        db_session.add(lightning)
        if i % 10000 == 0:
            print("Processed {0:} records.".format(i))
        i += 1
    print("Committing all {0:} records".format(i))
    db_session.commit()
    return lightning.date.year


def process_requests(db_session, year):
    """
    Add the equivalent request responses to the database for the given year

    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.orm.Session
    :param year: Year to process requests into the database
    :type year: int
    :return: None
    """
    date = datetime.datetime(year, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
    i = 0
    try:
        while date.year == year:
            number_of_lightnings = db_session.query(Lightning).\
                filter(date <= Lightning.date).\
                filter(Lightning.date < date + datetime.timedelta(hours=1)).\
                count()
            simulated_request = LightningAPIRequest(date, 200, number_of_lightnings)
            date = date + datetime.timedelta(hours=1)
            db_session.add(simulated_request)
            if i % 24 == 0:
                print("Processed day: {0:}".format(date.strftime("%Y-%m-%d")))
            print(i, i % 24)
            i += 1
        db_session.commit()
    except sqlalchemy.exc.SQLAlchemyError as e:
        print("Error found in record {0:}. Rolling back all changes. Exception text: {1:}".format(i, str(e)))
        db_session.rollback()


if __name__ == "__main__":  # pragma: no cover
    # Config the program arguments
    # noinspection DuplicatedCode
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-p', '--port', type=int, help='Database cluster port')
    parser.add_argument('-d', '--database', help='Database name')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-w', '--password', help='Database password')
    parser.add_argument('-f', '--file', help='File to retrieve data from')
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

    # Create the CSV file reader
    try:
        csv_file = open(args.file)
        reader = csv.reader(csv_file, delimiter=';')
    except Exception as ex:
        print(ex)
        sys.exit(-1)

    with session.begin():  # Open a transaction
        # Process the CSV file and store it into the database
        processed_year = process_lightnings(session, reader)
        if processed_year is not None:
            # Add the requests equivalencies to the database
            process_requests(session, processed_year)
