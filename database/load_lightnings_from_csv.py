import argparse
import datetime

import pytz
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import sys
import csv
from gisfire_meteocat_lib.database.meteocat_xdde import get_lightnings
from gisfire_meteocat_lib.database.lightnings import Lightning
from gisfire_meteocat_lib.database.lightnings import LightningAPIRequest


def main(db_session, csv_reader):
    """

    :param db_session:
    :type db_session: sqlalchemy.orm.Session
    :param csv_reader:
    :type csv_reader: csv.Reader
    :return:
    """

    # First step: Load into the database all the lightnings that are stores in the CSV file. The file contains all the
    # data for a year, so the year is stored for next step
    next(csv_reader)  # Remove the header
    i = 0  # Counter for commits and messages
    date = None
    for row in csv_reader:
        meteocat_id = int(row[0])
        date = datetime.datetime.strptime(row[1] + ' UTC', '%Y-%m-%d %H:%M:%S.%f %Z')
        peak_current = float(row[2])
        chi_squared = float(row[3])
        ellipse_major_axis = float(row[4])
        ellipse_minor_axis = float(row[5])
        ellipse_angle = None
        number_of_sensors = int(row[6])
        hits_the_ground = row[7] == 't'
        municipality_id = int(row[8]) if row[8] != '' else None
        lon = float(row[9])
        lat = float(row[10])
        lightning = Lightning(meteocat_id, date, peak_current, chi_squared, ellipse_major_axis, ellipse_minor_axis,
                              ellipse_angle, number_of_sensors, hits_the_ground, municipality_id, lat, lon)
        db_session.add(lightning)
        if i % 500 == 0:
            pass
            # db_session.commit()
        if i % 10000 == 0:
            print("Processed {0:} records.".format(i))
        i += 1
    year = date.year
    date = datetime.datetime(year, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
    i = 0
    while date.year == year:
        next_date = date + datetime.timedelta(hours=1)
        simulated_request = LightningAPIRequest(date, 200, len(get_lightnings(db_session, date, next_date)))
        date = next_date
        db_session.add(simulated_request)
        if i % 24 == 0:
            pass
            # db_session.commit()
            print("Counted {0:} records.".format(i))
        i += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-p', '--port', type=int, help='Database cluster port')
    parser.add_argument('-d', '--database', help='Database name')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-w', '--password', help='Database password')
    parser.add_argument('-f', '--file', help='File to retrieve data from')
    args = parser.parse_args()
    database_connection_string = 'postgresql+psycopg2://' + args.username + ':' + args.password + '@' + args.host +\
                                 ':' + str(args.port) + '/' + args.database
    try:
        engine = create_engine(database_connection_string)
        session = Session(engine)
    except SQLAlchemyError as ex:
        print(ex)
        sys.exit(-1)

    try:
        csv_file = open(args.file)
        reader = csv.reader(csv_file, delimiter=';')
    except Exception as ex:
        print(ex)
        sys.exit(-1)

    main(session, reader)
