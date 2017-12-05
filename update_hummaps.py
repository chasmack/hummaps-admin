import psycopg2
import time

from const import *

from create_funcs import create_funcs
from load_update import load_update
from load_map import load_map
from load_surveyor import load_surveyor
from load_map_image import load_map_image
from load_cc import load_cc
from load_pdf import load_pdf
from load_scan import load_scan
from load_trs import load_trs

def init_schema():
    # Create the hummaps staging schema

    print('CREATE SCHEMA: {schema_staging} ...'.format(schema_staging=SCHEMA_STAGING))

    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:
        cur.execute("""
            DROP SCHEMA IF EXISTS {schema_staging} CASCADE;
            CREATE SCHEMA {schema_staging};
        """.format(schema_staging=SCHEMA_STAGING))
        con.commit()


if __name__ == '__main__':

    print('\nPerforming update ... ')
    startTime = time.time()

    # Create the staging schema
    # init_schema();

    # Create utility functions
    create_funcs()

    # Load update data from Hollins and create staging tables
    load_update()
    load_map()
    load_surveyor()
    load_map_image()
    load_cc()
    load_pdf()
    load_scan()
    load_trs()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
