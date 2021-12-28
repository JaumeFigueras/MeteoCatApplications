#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def test_database_init_01(postgresql_schema):
    """
    Test the existence of all required tables in the database

    :param postgresql_schema:
    :return: None
    """
    cursor = postgresql_schema.cursor()
    cursor.execute("SELECT count(*) FROM meteocat_xdde_request")
    record = cursor.fetchone()
    assert record[0] == 0
    cursor = postgresql_schema.cursor()
    cursor.execute("SELECT count(*) FROM meteocat_lightning")
    record = cursor.fetchone()
    assert record[0] == 0
