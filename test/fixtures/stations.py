#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import zipfile
import csv
import json
from pathlib import Path
from io import TextIOWrapper
from gisfire_meteocat_lib.classes.weather_station import WeatherStation

from typing import List


@pytest.fixture
def stations_2022() -> List[WeatherStation]:
    """
    Opens a ZIP file (for space purposes) with a stations JSON file and returns a list of stations

    :return: A list of stations
    :rtype: list(WeatherStation)
    """
    current_dir = Path(__file__).parent
    zip_filename = str(current_dir) + '/stations-2022.zip'
    archive = zipfile.ZipFile(zip_filename)
    file_name = 'stations-2022.json'
    file = archive.open(file_name, 'r')
    stations = json.load(file, object_hook=WeatherStation.object_hook)
    return stations
