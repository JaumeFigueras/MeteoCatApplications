#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from src.meteocat_applications.lightnings.load_lightnings_from_csv import process_lightnings
from src.meteocat_applications.lightnings.load_lightnings_from_csv import process_requests
from gisfire_meteocat_lib.classes.lightning import LightningAPIRequest


def test_process_lightnings_01(postgresql_schema, db_session, lightnings_csv_2017, lightnings_csv_2017_number):
    """
    Tests the reading and database insertion of a lightning CSV file.

    :param postgresql_schema: Fixture of the database connection
    :type postgresql_schema:  psycopg2.connection
    :param db_session: Fixture of the SQL Alchemy database session to work with
    :type db_session: sqlalchemy.scoped_session
    :param lightnings_csv_2017: Fixture of the CSV lightning file
    :type lightnings_csv_2017: csv.reader
    :param lightnings_csv_2017_number: Fixture with the number of lightnings contained by the lightning CSV file
    :type lightnings_csv_2017_number: int
    :return: None
    """
    process_lightnings(db_session, lightnings_csv_2017)
    cursor = postgresql_schema.cursor()
    cursor.execute("SELECT count(*) FROM meteocat_lightning")
    record = cursor.fetchone()
    assert record[0] == lightnings_csv_2017_number


def test_process_lightnings_02(postgresql_schema, db_session, lightnings_csv_2017_error):
    """
    Tests the reading and database insertion of a lightning CSV file when there is an error in the file to process

    :param postgresql_schema: Fixture of the database connection
    :type postgresql_schema:  psycopg2.connection
    :param db_session: Fixture of the SQL Alchemy database session to work with
    :type db_session: sqlalchemy.scoped_session
    :param lightnings_csv_2017_error: Fixture of the CSV lightning file with errors
    :type lightnings_csv_2017_error: csv.reader
    :return: None
    """
    process_lightnings(db_session, lightnings_csv_2017_error)
    cursor = postgresql_schema.cursor()
    cursor.execute("SELECT count(*) FROM meteocat_lightning")
    record = cursor.fetchone()
    assert record[0] == 0


def test_process_requests_01(postgresql_schema, db_session, lightnings_csv_2017, lightnings_csv_2017_number):
    """
    Tests the creation of the requests equivalencies

    :param postgresql_schema: Fixture of the database connection
    :type postgresql_schema:  psycopg2.connection
    :param db_session: Fixture of the SQL Alchemy database session to work with
    :type db_session: sqlalchemy.scoped_session
    :param lightnings_csv_2017: Fixture of the CSV lightning file
    :type lightnings_csv_2017: csv.reader
    :param lightnings_csv_2017_number: Fixture with the number of lightnings contained by the lightning CSV file
    :type lightnings_csv_2017_number: int
    :return: None
    """
    processed_year = process_lightnings(db_session, lightnings_csv_2017)
    process_requests(db_session, processed_year)
    cursor = postgresql_schema.cursor()
    cursor.execute("SELECT count(*) FROM meteocat_xdde_request")
    record = cursor.fetchone()
    assert record[0] == 365 * 24


def test_process_requests_02(postgresql_schema, db_session, lightnings_csv_2017):
    """
    Tests the creation of the requests equivalencies with an exception as it will

    :param postgresql_schema: Fixture of the database connection
    :type postgresql_schema:  psycopg2.connection
    :param db_session: Fixture of the SQL Alchemy database session to work with
    :type db_session: sqlalchemy.scoped_session
    :param lightnings_csv_2017: Fixture of the CSV lightning file
    :type lightnings_csv_2017: csv.reader
    :return: None
    """
    cursor = postgresql_schema.cursor()
    cursor.execute(("INSERT INTO meteocat_xdde_request (request_date, http_status_code, number_of_lightnings)"
                    "    VALUES ('2017-01-01T04:00:00Z', 200, 15)"))
    postgresql_schema.commit()
    assert db_session.query(LightningAPIRequest).count() == 1
    processed_year = process_lightnings(db_session, lightnings_csv_2017)
    process_requests(db_session, processed_year)
    cursor.execute("SELECT count(*) FROM meteocat_xdde_request")
    record = cursor.fetchone()
    assert record[0] == 1
