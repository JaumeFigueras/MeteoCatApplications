# MeteoCatApplications

0. obtain the requirements file

pip3 freeze > requirements.txt

1. create a virtual environment

python3 -m venv ./venv

2. activate the virtual environment

source ./venv/bin/activate

3. install the project requirements

pip3 install -r requirements.txt

4. Download other gisfire libraries

a) meteocatlib: https://github.com/JaumeFigueras/MeteoCatLib/blob/main/dist/gisfire_meteocat_lib-0.1.0.tar.gz

5. Script:

YESTERDAY=$(date -d "yesterday 13:00" +'%Y-%m-%d')
/home/gisfire/soft/MeteoCatApplications/venv/bin/python3 /home/gisfire/soft/MeteoCatApplications/src/meteocat_applications/lightnings/load_lightnings_from_api.py --host=localhost --port=5433 --database=gisfire --username=gisfireuser --password=... --date=$YESTERDAY --api-token=... >> /home/gisfire/logs/xdde.log 2>&1