#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from src.meteocat_applications.lightnings.load_lightnings_from_csv import process_lightnings
from src.meteocat_applications.lightnings.load_lightnings_from_csv import process_requests


def test_process_lightnings_01(postgresql_schema, db_session, lightnings_csv_2017, number_of_lightnings_2017):
    """
    Tests the reading and database insertion of a lightning CSV file

    :param postgresql_schema: Fixture of the database connection
    :type postgresql_schema:  psycopg2.connection
    :param db_session: Fixture of the SQL Alchemy database session to work with
    :type db_session: sqlalchemy.scoped_session
    :param lightnings_csv_2017: Fixture of the CSV lightning file
    :type lightnings_csv_2017: csv.reader
    :param number_of_lightnings_2017: Fixture with the number of lightnings contained by the lightning CSV file
    :type number_of_lightnings_2017: int
    :return: None
    """
    process_lightnings(db_session, lightnings_csv_2017)
    cursor = postgresql_schema.cursor()
    cursor.execute("SELECT count(*) FROM meteocat_lightning")
    record = cursor.fetchone()
    assert record[0] == number_of_lightnings_2017


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


def test_process_requests_01(postgresql_schema, db_session, lightnings_csv_2017, number_of_lightnings_2017):
    """
    Tests the creation of the requests equivalencies

    :param postgresql_schema: Fixture of the database connection
    :type postgresql_schema:  psycopg2.connection
    :param db_session: Fixture of the SQL Alchemy database session to work with
    :type db_session: sqlalchemy.scoped_session
    :param lightnings_csv_2017: Fixture of the CSV lightning file
    :type lightnings_csv_2017: csv.reader
    :param number_of_lightnings_2017: Fixture with the number of lightnings contained by the lightning CSV file
    :type number_of_lightnings_2017: int
    :return: None
    """
    processed_year = process_lightnings(db_session, lightnings_csv_2017)
    process_requests(db_session, processed_year)
    cursor = postgresql_schema.cursor()
    cursor.execute("SELECT count(*) FROM meteocat_xdde_request")
    record = cursor.fetchone()
    assert record[0] == 365 * 24


