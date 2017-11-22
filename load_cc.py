import xml.etree.ElementTree as ET
from openpyxl import load_workbook
import psycopg2
import time

from const import *

# load_cc.py - create and load the cc and cc_image tables from XLSX data

# The file cc.xlsx contains data for the production cc table.

def load_cc():

    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:

        # Read the XLSX worksheet
        ws = load_workbook(filename=XLSX_DATA_CC, read_only=True).active
        HEADER_LINES = 1

        cc_data = []
        for row in ws.iter_rows(min_row=HEADER_LINES + 1):
            cc_data.append(list(c.value for c in row))

        # Create and populate a temporary table for the cc data
        print('CREATE TABLE: {table_temp} ...'.format(table_temp=TABLE_TEMP))

        cur.execute("""
            DROP TABLE IF EXISTS {table_temp};
            CREATE TABLE {table_temp} (
              id serial PRIMARY KEY,
              maptype text,
              book integer,
              firstpage integer,
              lastpage integer,
              recdate date,
              surveyor text,
              donefor text,
              doc_number text,
              npages integer
            );
        """.format(table_temp=TABLE_TEMP))

        cur.executemany("""
            INSERT INTO {table_temp} (
              maptype, book, firstpage, lastpage, recdate, surveyor, donefor, doc_number, npages
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """.format(table_temp=TABLE_TEMP), cc_data)
        con.commit()

        print('INSERT: %d rows affected.' % (cur.rowcount))

        # Create and populate the cc table
        print('CREATE TABLE: {table_cc} ...'.format(table_cc=TABLE_CC))

        cur.execute("""
            DROP TABLE IF EXISTS {table_cc};
            CREATE TABLE {table_cc} (
              id serial PRIMARY KEY,
              map_id integer NOT NULL REFERENCES {table_map},
              doc_number text,
              npages integer
            );
        """.format(
            table_cc=TABLE_CC,
            table_map=TABLE_MAP
        ))

        cur.execute("""
            INSERT INTO {table_cc} (map_id, doc_number, npages)
            SELECT m.id, t.doc_number, t.npages
            FROM {table_temp} t
            JOIN {table_hollins_map} m ON
              t.maptype = m.maptype AND
              t.book = m.book AND
              t.firstpage = m.firstpage AND
              t.lastpage = m.lastpage AND
              t.recdate = m.recdate AND
              substring(m.surveyor for length(t.surveyor)) = t.surveyor
            ;
        """.format(
            table_cc=TABLE_CC,
            table_temp=TABLE_TEMP,
            table_hollins_map=TABLE_HOLLINS_MAP
        ))
        con.commit()

        print('INSERT: %d rows affected.' % (cur.rowcount))

        if cur.rowcount == len(cc_data):
            # Drop the temporary table
            print('DROP TABLE: {table_temp} ...'.format(table_temp=TABLE_TEMP))

            cur.execute("""DROP TABLE {table_temp};""".format(table_temp=TABLE_TEMP))
            con.commit()

        else:
            # Find the CCs that didn't join with their map
            cur.execute("""
                SELECT t.*
                FROM {table_temp} t
                LEFT JOIN {table_hollins_map} m ON
                  t.maptype = m.maptype AND
                  t.book = m.book AND
                  t.firstpage = m.firstpage AND
                  t.lastpage = m.lastpage AND
                  t.recdate = m.recdate AND
                  substring(m.surveyor for length(t.surveyor)) = t.surveyor
                WHERE m.id IS NULL;
            """.format(
                table_temp=TABLE_TEMP,
                table_hollins_map=TABLE_HOLLINS_MAP
            ))
            con.commit()

            for row in cur:
                print('>>> NO MAP FOR CC #%d: %s' % (row[0], ', '.join(str(c) for c in row[1:])))


        # Vacuum up dead tuples from the update
        tables = (
            TABLE_CC,
            # TABLE_CC_IMAGE
        )

        # Vacuum must run outside of a transaction
        con.autocommit = True
        for t in tables:
            cur.execute('VACUUM FREEZE ' + t)

if __name__ == '__main__':

    print('\nLoading tables ... ')
    startTime = time.time()

    load_cc()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
