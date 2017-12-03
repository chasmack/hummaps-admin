import psycopg2
from openpyxl import load_workbook
import time

from const import *

# load_map.py - create and load the map and maptype tables

def load_map():

    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:

        # Load the maptype table
        print('CREATE TABLE: {table_maptype} ...'.format(table_maptype=TABLE_MAPTYPE))

        cur.execute("""
            DROP TABLE IF EXISTS {table_maptype} CASCADE;
            CREATE TABLE {table_maptype}  (
               id serial PRIMARY KEY,
               maptype text,
               abbrev text
            );
        """.format(table_maptype=TABLE_MAPTYPE))

        cur.executemany("""
            INSERT INTO {table_maptype} (maptype, abbrev) VALUES (%s, %s);
        """.format(table_maptype=TABLE_MAPTYPE), MAPTYPES)
        rowcount = cur.rowcount
        con.commit()

        # Check we have all the maptypes listed in hollins
        cur.execute("""
            SELECT DISTINCT m.id, maptype
            FROM {table_hollins_map} m
            LEFT JOIN {table_maptype} t USING (maptype)
            WHERE t.id is NULL;
        """.format(
            table_hollins_map=TABLE_HOLLINS_MAP,
            table_maptype=TABLE_MAPTYPE,
        ))
        con.commit()
        assert cur.rowcount == 0

        print('INSERT: ' + str(rowcount) + ' rows effected.')

        # Load the map tables from Hollins update
        print('CREATE TABLE: {table_map} ...'.format(table_map=TABLE_MAP))

        cur.execute("""
            DROP TABLE IF EXISTS {table_map} CASCADE;
            CREATE TABLE {table_map}  (
              id serial PRIMARY KEY,
              maptype_id integer REFERENCES {table_maptype},
              book integer,
              page integer,
              npages integer,
              recdate date,
              client text,
              description text,
              note text
            );
        """.format(
            table_map=TABLE_MAP,
            table_maptype=TABLE_MAPTYPE
        ))

        cur.execute("""
            INSERT INTO {table_map} (
               id, maptype_id, book, page, npages,
               recdate, client, description, note
            )
            SELECT
              {table_hollins_map}.id,
              {table_maptype}.id,
              {table_hollins_map}.book,
              {table_hollins_map}.firstpage,
              {table_hollins_map}.lastpage - {table_hollins_map}.firstpage + 1,
              {table_hollins_map}.recdate,
              {table_hollins_map}.donefor,
              {table_hollins_map}.descript,
              {table_hollins_map}.comment
            FROM {table_hollins_map}
            LEFT JOIN {table_maptype} USING (maptype)
            ;
        """.format(
            table_map=TABLE_MAP,
            table_maptype=TABLE_MAPTYPE,
            table_hollins_map=TABLE_HOLLINS_MAP
        ))
        con.commit()

        print('INSERT (HOLLINS): ' + str(cur.rowcount) + ' rows effected.')

        # Read additional map data from the XLSX file
        ws = load_workbook(filename=XLSX_DATA_MAP, read_only=True).active
        HEADER_LINES = 1

        maps = []
        for map in ws.iter_rows(min_row=HEADER_LINES + 1):
            maps.append(tuple(c.value for c in map))

        cur.executemany("""
            -- Reset the primary key sequence
            SELECT setval('{sequence_map_id}', max(id) + 1) FROM {table_map};
            WITH q1 AS (
            SELECT
                (%s)::text maptype,
                (%s)::integer book,
                (%s)::integer page,
                (%s)::integer npages,
                (%s)::date recdate,
                (%s)::text surveyor,
                (%s)::text client,
                (%s)::text description,
                (%s)::text trs,
                (%s)::text note
            )
            INSERT INTO {table_map} (
               maptype_id, book, page, npages,
               recdate, client, description, note
            )
            SELECT t.id, q1.book, q1.page, q1.npages,
                q1.recdate, q1.client, q1.description, q1.note
            FROM q1
            JOIN {table_maptype} t USING (maptype)
            ;
        """.format(
            table_map=TABLE_MAP,
            table_maptype=TABLE_MAPTYPE,
            sequence_map_id=SEQUENCE_MAP_ID
        ), maps)
        con.commit()

        print('INSERT (EXTRAS): ' + str(cur.rowcount) + ' rows effected.')

        # Vacuum up dead tuples from the update
        tables = (
            TABLE_MAP,
            TABLE_MAPTYPE
        )

        # Vacuum must run outside of a transaction
        con.autocommit = True
        for t in tables:
            cur.execute('VACUUM FREEZE ' + t)


if __name__ == '__main__':

    print('\nCreating staging tables ... ')
    startTime = time.time()

    load_map()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
