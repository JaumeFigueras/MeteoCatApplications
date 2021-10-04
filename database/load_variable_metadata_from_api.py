import argparse
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import sys
from gisfire_meteocat_lib.remote_api import meteocat_xema_api as api
from gisfire_meteocat_lib.database.variable import Variable


def main(api_token, db_session):
    measured_variables_metadata = api.get_variables_measured_metadata(api_token)
    multivariate_variables_metadata = api.get_variables_multivariate_metadata(api_token)
    auxiliary_variables_metadata = api.get_variables_auxiliary_metadata(api_token)
    variables_metadata = measured_variables_metadata + multivariate_variables_metadata + auxiliary_variables_metadata
    for variable_metadata in variables_metadata:
        var = Variable(variable_metadata['codi'], variable_metadata['nom'], variable_metadata['unitat'],
                       variable_metadata['acronim'], variable_metadata['tipus'], variable_metadata['decimals'])
        session.add(var)
        print("Adding variable metadata: {0:} ({1:})".format(variable_metadata['nom'], variable_metadata['codi']))
    session.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--api-token', help='MeteoCat key to access its api')
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-p', '--port', type=int, help='Database cluster port')
    parser.add_argument('-d', '--database', help='Database name')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-w', '--password', help='Database password')
    args = parser.parse_args()
    database_connection_string = 'postgresql+psycopg2://' + args.username + ':' + args.password + '@' + args.host +\
                                 ':' + str(args.port) + '/' + args.database
    try:
        engine = create_engine(database_connection_string)
        session = Session(engine)
    except SQLAlchemyError as ex:
        print(ex)
        sys.exit(-1)

    main(args.api_token, session)
