#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest

from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy.orm import scoped_session, Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from pathlib import Path

from typing import Any
from typing import Union


@pytest.fixture(scope='function')
def db_engine(postgresql_schema) -> Engine:
    """
    Yields a SQLAlchemy engine which is suppressed after the test session

    :param postgresql_schema: Postgresql database fixture
    :return: SqlAlchemy database engine
    """
    def db_creator():
        return postgresql_schema

    engine_ = create_engine('postgresql+psycopg://', creator=lambda: postgresql_schema)

    yield engine_

    engine_.dispose()


@pytest.fixture(scope='function')
def db_session_factory(db_engine: Engine) -> scoped_session[Union[Session, Any]]:
    """
    Returns a SQLAlchemy scoped session factory

    :param db_engine: SqlAlchemy database engine
    :return: A SqlAlchemy scoped session
    """
    return scoped_session(sessionmaker(bind=db_engine))


@pytest.fixture(scope='function')
def db_session(db_session_factory: scoped_session[Union[Session, Any]]) -> Session:
    """
    Yields a SQLAlchemy connection which is rollback after the test

    :param db_session_factory: A SqlAlchemy scoped session
    :return: A SqlAlchemy session
    """
    session_ = db_session_factory()

    yield session_

    session_.rollback()
    session_.close()

