#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from src.meteocat_applications.lightnings.load_lightnings_from_api import find_dates_to_issue_requests
from src.meteocat_applications.lightnings.load_lightnings_from_api import run_request
from meteocat.data_model.lightning import Lightning
from meteocat.data_model.lightning import LightningAPIRequest
from meteocat.api import meteocat_urls
import datetime
import pytz
import requests
from freezegun import freeze_time


def test_find_dates_01(db_session):
    """
    Test a correct behaviour with a whole day needed to request. It is a past day

    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.scoped_session
    :return: None
    """
    result = find_dates_to_issue_requests(db_session, datetime.date(2021, 11, 10), -1)
    assert len(result) == 24
    for i in range(24):
        assert result[i] == datetime.datetime(2021, 11, 10, i, 0, 0, tzinfo=pytz.UTC)


@freeze_time("2021-11-10 14:25:36")
def test_find_dates_02(db_session):
    """
    Test a correct behaviour with a whole day needed to request. It is the current day, so not all hours have to be
    requested

    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.scoped_session
    :return: None
    """
    result = find_dates_to_issue_requests(db_session, datetime.datetime.utcnow().date(), -1)
    assert len(result) == 13
    for i in range(len(result)):
        assert result[i] == datetime.datetime(2021, 11, 10, i, 0, 0, tzinfo=pytz.UTC)


def test_find_dates_03(db_session):
    """
    Test a correct behaviour with just one hour to request

    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.scoped_session
    :return: None
    """
    result = find_dates_to_issue_requests(db_session, datetime.date(2021, 11, 10), 13)
    assert len(result) == 1
    assert result[0] == datetime.datetime(2021, 11, 10, 13, 0, 0, tzinfo=pytz.UTC)


def test_find_dates_04(postgresql_schema, db_session):
    """
    Tests a correct behaviour with a whole day needed to request. The system contains previous requests so not all hours
    have to be requested

    :param postgresql_schema: Fixture of the database connection
    :type postgresql_schema:  psycopg2.connection
    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.scoped_session
    :return: None
    """
    cursor = postgresql_schema.cursor()
    cursor.execute(("INSERT INTO xdde_request (request_date, http_status_code, number_of_lightnings)"
                    "    VALUES ('2021-11-10T00:00:00Z', 200, 15)"))
    postgresql_schema.commit()
    result = find_dates_to_issue_requests(db_session, datetime.date(2021, 11, 10), -1)
    assert len(result) == 23
    for i in range(len(result)):
        assert result[i] == datetime.datetime(2021, 11, 10, i+1, 0, 0, tzinfo=pytz.UTC)


def test_run_requests_01(requests_mock, db_session, lightnings_json_2021_11_10):
    """
    Tests the requests for a whole day and how the results are stores in the database

    :param requests_mock: Requests library mock
    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.scoped_session
    :param lightnings_json_2021_11_10: Pytest fixture with the real results of lightnings MeteoCat API calls for the
    whole day of 2021-11-10
    :type lightnings_json_2021_11_10: list of json responses
    :return: None
    """
    for i in range(24):
        url = meteocat_urls.LIGHTNINGS_DATA.format(2021, 11, 10, i)
        requests_mock.get(url, json=lightnings_json_2021_11_10[i], status_code=200)
    requests_to_perform = find_dates_to_issue_requests(db_session, datetime.date(2021, 11, 10), -1)
    for request_date in requests_to_perform:
        run_request("1234", db_session, request_date)
    total = db_session.query(Lightning).count()
    assert total == 324
    # noinspection DuplicatedCode
    cnt = 0
    for i in range(24):
        date = datetime.datetime(2021, 11, 10, i, 0, 0, tzinfo=pytz.UTC)
        request: LightningAPIRequest = db_session.query(LightningAPIRequest).\
            filter(LightningAPIRequest.date == date).\
            first()
        assert request.http_status_code == 200
        cnt += request.number_of_lightnings
    assert cnt == 324
    total = db_session.query(LightningAPIRequest).count()
    assert total == 24


def test_run_requests_02(requests_mock, db_session, lightnings_json_2021_11_10):
    """
    Tests the requests for a whole day and how the results are stores in the database. Contains an HTTP 500 server
    error.

    :param requests_mock: Requests library mock
    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.scoped_session
    :param lightnings_json_2021_11_10: Pytest fixture with the real results of lightnings MeteoCat API calls for the
    whole day of 2021-11-10
    :type lightnings_json_2021_11_10: list of json responses
    :return: None
    """
    for i in range(24):
        url = meteocat_urls.LIGHTNINGS_DATA.format(2021, 11, 10, i)
        if i != 0:
            requests_mock.get(url, json=lightnings_json_2021_11_10[i], status_code=200)
        else:
            requests_mock.get(url, json=[], status_code=500)
    requests_to_perform = find_dates_to_issue_requests(db_session, datetime.date(2021, 11, 10), -1)
    for request_date in requests_to_perform:
        run_request("1234", db_session, request_date)
    total = db_session.query(Lightning).count()
    assert total == 323
    cnt = 0
    for i in range(24):
        date = datetime.datetime(2021, 11, 10, i, 0, 0, tzinfo=pytz.UTC)
        request: LightningAPIRequest = db_session.query(LightningAPIRequest).\
            filter(LightningAPIRequest.date == date).\
            first()
        if i != 0:
            assert request.http_status_code == 200
            cnt += request.number_of_lightnings
        else:
            assert request.http_status_code == 500
            assert request.number_of_lightnings is None
    assert cnt == 323
    total = db_session.query(LightningAPIRequest).count()
    assert total == 24


def test_run_requests_03(requests_mock, db_session, lightnings_json_2021_11_10):
    """
    Tests the requests for a whole day and how the results are stores in the database. Contains an HTTP request
    exception.

    :param requests_mock: Requests library mock
    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.scoped_session
    :param lightnings_json_2021_11_10: Pytest fixture with the real results of lightnings MeteoCat API calls for the
    whole day of 2021-11-10
    :type lightnings_json_2021_11_10: list of json responses
    :return: None
    """
    for i in range(24):
        url = meteocat_urls.LIGHTNINGS_DATA.format(2021, 11, 10, i)
        if i != 0:
            requests_mock.get(url, json=lightnings_json_2021_11_10[i], status_code=200)
        else:
            requests_mock.get(url, exc=requests.exceptions.HTTPError)
    requests_to_perform = find_dates_to_issue_requests(db_session, datetime.date(2021, 11, 10), -1)
    for request_date in requests_to_perform:
        run_request("1234", db_session, request_date)
    total = db_session.query(Lightning).count()
    assert total == 323
    cnt = 0
    for i in range(24):
        date = datetime.datetime(2021, 11, 10, i, 0, 0, tzinfo=pytz.UTC)
        request: LightningAPIRequest = db_session.query(LightningAPIRequest).\
            filter(LightningAPIRequest.date == date).\
            first()
        if i != 0:
            assert request.http_status_code == 200
            cnt += request.number_of_lightnings
        else:
            assert request is None
    assert cnt == 323
    total = db_session.query(LightningAPIRequest).count()
    assert total == 23


def test_run_requests_04(requests_mock, postgresql_schema, db_session, lightnings_json_2021_11_10):
    """
    Tests the requests for a whole day and how the results are stores in the database. Contains SQLAlchemy exception.

    :param requests_mock: Requests library mock
    :param postgresql_schema: Fixture of the database connection
    :type postgresql_schema:  psycopg2.connection
    :param db_session: SQLAlchemy database session
    :type db_session: sqlalchemy.scoped_session
    :param lightnings_json_2021_11_10: Pytest fixture with the real results of lightnings MeteoCat API calls for the
    whole day of 2021-11-10
    :type lightnings_json_2021_11_10: list of json responses
    :return: None
    """
    for i in range(24):
        url = meteocat_urls.LIGHTNINGS_DATA.format(2021, 11, 10, i)
        requests_mock.get(url, json=lightnings_json_2021_11_10[i], status_code=200)
    requests_to_perform = find_dates_to_issue_requests(db_session, datetime.date(2021, 11, 10), -1)
    cursor = postgresql_schema.cursor()
    cursor.execute(("INSERT INTO xdde_request (request_date, http_status_code, number_of_lightnings)"
                    "    VALUES ('2021-11-10T00:00:00Z', 200, 15)"))
    postgresql_schema.commit()
    for request_date in requests_to_perform:
        run_request("1234", db_session, request_date)
    total = db_session.query(Lightning).count()
    assert total == 323
    cnt = 0
    # noinspection DuplicatedCode
    for i in range(24):
        date = datetime.datetime(2021, 11, 10, i, 0, 0, tzinfo=pytz.UTC)
        request: LightningAPIRequest = db_session.query(LightningAPIRequest).\
            filter(LightningAPIRequest.date == date).\
            first()
        assert request.http_status_code == 200
        cnt += request.number_of_lightnings
    assert cnt == 323 + 15  # 15 more from the faked inserted data
    total = db_session.query(LightningAPIRequest).count()
    assert total == 24
