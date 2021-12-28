#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import zipfile
import csv
import json
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
def lightnings_csv_2017_number():
    """
    Returns the number of lightnings in the lightnings-2017.csv file

    :return: The number of lightnings in the lightnings-2017.csv file
    :rtype: int
    """
    return 22886


@pytest.fixture
def lightnings_json_2021_11_10():
    """
    Opens a ZIP file (for space purposes) with a CSV lightnings file and returns a CSV reader object of the CSV file

    :return: A CSV reader of a lightning file
    :rtype: list
    """
    current_dir = Path(__file__).parent
    zip_filename = str(current_dir) + '/lightnings-2021-11-10.zip'
    archive = zipfile.ZipFile(zip_filename)
    lightnings = list()
    for i in range(24):
        file_name = "{0:02d}".format(i) + '.json'
        file = archive.open(file_name, 'r')
        data = json.load(file)
        lightnings.append(data)
    return lightnings
