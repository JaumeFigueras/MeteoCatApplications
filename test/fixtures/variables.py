#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import zipfile
import json
from pathlib import Path
from meteocat.data_model.variable import Variable

from typing import List


@pytest.fixture
def variables_from_station() -> List[Variable]:
    """
    Opens a ZIP file (for space purposes) with a stations JSON file and returns a list of stations

    :return: A list of stations
    :rtype: list(WeatherStation)
    """
    current_dir = Path(__file__).parent
    zip_filename = str(current_dir) + '/variables-station.zip'
    archive = zipfile.ZipFile(zip_filename)
    file_name = 'variables-mesurades.json'
    file = archive.open(file_name, 'r')
    variables_measured = json.load(file, object_hook=Variable.object_hook)
    file_name = 'variables-aux.json'
    file = archive.open(file_name, 'r')
    variables_auxiliar = json.load(file, object_hook=Variable.object_hook)
    file_name = 'variables-cmv.json'
    file = archive.open(file_name, 'r')
    variables_cmv = json.load(file, object_hook=Variable.object_hook)
    return variables_measured + variables_auxiliar + variables_cmv


@pytest.fixture
def variables_from_station_other() -> List[Variable]:
    """
    Opens a ZIP file (for space purposes) with a stations JSON file and returns a list of stations

    :return: A list of stations
    :rtype: list(WeatherStation)
    """
    current_dir = Path(__file__).parent
    zip_filename = str(current_dir) + '/variables-station.zip'
    archive = zipfile.ZipFile(zip_filename)
    file_name = 'variables-mesurades.json'
    file = archive.open(file_name, 'r')
    variables_measured = json.load(file, object_hook=Variable.object_hook)
    file_name = 'variables-aux.json'
    file = archive.open(file_name, 'r')
    variables_auxiliar = json.load(file, object_hook=Variable.object_hook)
    file_name = 'variables-cmv.json'
    file = archive.open(file_name, 'r')
    variables_cmv = json.load(file, object_hook=Variable.object_hook)
    return variables_measured + variables_auxiliar + variables_cmv
