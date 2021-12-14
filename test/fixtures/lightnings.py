#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import zipfile
import csv
from pathlib import Path
from io import TextIOWrapper


@pytest.fixture
def lightnings_csv_2017():
    """
    Opens a ZIP file (for space purposes) with a CSV lightnings file and returns a CSV reader object of the CSV file

    :return: A CSV reader of a lightning file
    :rtype: csv.reader
    """
    current_dir = Path(__file__).parent
    zip_filename = str(current_dir) + '/lightnings-2017.zip'
    archive = zipfile.ZipFile(zip_filename)
    file = archive.open('lightnings-2017.csv', 'r')
    reader = csv.reader(TextIOWrapper(file, 'utf-8'), delimiter=';')
    return reader


@pytest.fixture
def lightnings_csv_2017_error():
    """
    Opens a ZIP file (for space purposes) with a CSV lightnings file and returns a CSV reader object of the CSV file.
    The file contains errors for testing purposes

    :return: A CSV reader of a lightning file
    :rtype: csv.reader
    """
    current_dir = Path(__file__).parent
    zip_filename = str(current_dir) + '/lightnings-2017-error.zip'
    archive = zipfile.ZipFile(zip_filename)
    file = archive.open('lightnings-2017-error.csv', 'r')
    reader = csv.reader(TextIOWrapper(file, 'utf-8'), delimiter=';')
    return reader


@pytest.fixture
def number_of_lightnings_2017():
    """
    Returns the number of lightnings in the lightnings-2017.csv file

    :return: The number of lightnings in the lightnings-2017.csv file
    :rtype: int
    """
    return 22886

