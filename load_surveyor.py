import xml.etree.ElementTree as ET
from openpyxl import load_workbook
import psycopg2
import time

from const import *

# load_surveyor.py - create and load the surveyor and signed_by tables from XLSX data

# surveyor.xlsx contains surveyor information for the production surveyor table.
# The column hollins_fullname provides a mapping from hollins_map.surveyor to the
# production surveyor table. The column fullname is a unique key for each surveyor
# in the production table. Multiple hollins_fullname values may map to the same
# production surveyor. In that case all values other than hollins_fullname
# must be identical.

def load_surveyor():

    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:

        print('CREATE TABLE: {table_surveyor} ...'.format(table_surveyor=TABLE_SURVEYOR))

        cur.execute("""
            DROP TABLE IF EXISTS {table_surveyor} CASCADE;
            CREATE TABLE {table_surveyor}  (
              id serial PRIMARY KEY,
              fullname text NOT NULL UNIQUE,
              firstname text,
              secondname text,
              thirdname text,
              lastname text,
              suffix text,
              pls text,
              rce text
            );
        """.format(table_surveyor=TABLE_SURVEYOR))

        # Create a temporary table mapping hollins fullname to surveyor
        cur.execute("""
            DROP TABLE IF EXISTS {table_hollins_fullname};
            CREATE TABLE {table_hollins_fullname}  (
              id serial PRIMARY KEY,
              hollins_fullname text NOT NULL UNIQUE,
              surveyor_id integer REFERENCES {table_surveyor}
            );
        """.format(
            table_hollins_fullname=TABLE_HOLLINS_FULLNAME,
            table_surveyor=TABLE_SURVEYOR
        ))

        ws = load_workbook(filename=XLSX_DATA_SURVEYOR, read_only=True).active

        # Skip over the headers and split out the mapping information and surveyors
        HEADER_LINES = 1

        surveyors = {}
        hollins_surveyors = {}
        for row in ws.iter_rows(min_row=HEADER_LINES + 1):
            hollins_fullname = row[0].value
            fullname = row[1].value

            # check for duplicates
            assert hollins_fullname not in hollins_surveyors
            hollins_surveyors[hollins_fullname] = fullname

            if fullname not in surveyors:
                surveyors[fullname] = list(c.value for c in row[1:])

        cur.executemany("""
            INSERT INTO {table_surveyor} (
              fullname, firstname, secondname, thirdname, lastname, suffix, pls, rce
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """.format(table_surveyor=TABLE_SURVEYOR), list(surveyors.values()))
        con.commit()

        print('INSERT: %d rows affected.' % (cur.rowcount))

        print('CREATE TABLE: {table_signed_by} ...'.format(table_signed_by=TABLE_SIGNED_BY))

        cur.execute("""
            DROP TABLE IF EXISTS {table_signed_by};
            CREATE TABLE {table_signed_by}  (
              map_id integer REFERENCES {table_map},
              surveyor_id integer REFERENCES {table_surveyor},
              PRIMARY KEY (map_id, surveyor_id)
            );
        """.format(
            table_signed_by=TABLE_SIGNED_BY,
            table_map=TABLE_MAP,
            table_surveyor=TABLE_SURVEYOR
        ))
        con.commit()

        # Vacuum up dead tuples from the update
        tables = (
            TABLE_SURVEYOR,
            TABLE_SIGNED_BY
        )

        # Vacuum must run outside of a transaction
        con.autocommit = True
        for t in tables:
            cur.execute('VACUUM FREEZE ' + t)







if __name__ == '__main__':

    print('\nLoading tables ... ')
    startTime = time.time()

    load_surveyor()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
