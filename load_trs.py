import psycopg2
from openpyxl import load_workbook
import time

from const import *

# load_trs.py - create and load the trs and source tables

def load_trs():

    with psycopg2.connect(DSN_PROD) as con, con.cursor() as cur:

        # Create and load the source table
        print('CREATE TABLE: {table_source} ...'.format(table_source=TABLE_SOURCE))

        cur.execute("""
            DROP TABLE IF EXISTS {table_source} CASCADE;
            CREATE TABLE {table_source}  (
               id integer PRIMARY KEY,
               description text,
               quality integer
            );
        """.format(table_source=TABLE_SOURCE))
        con.commit()

        cur.execute("""
            INSERT INTO {table_source} (id, description, quality)
            VALUES
                ({trs_source_hollins_section}, 'Hollins full-section records', 10),
                ({trs_source_hollins_subsection}, 'Hollins subsection records', 40),
                ({trs_source_parsed_section}, 'Parsed full-section records', 40),
                ({trs_source_parsed_subsection}, 'Parsed subsection records', 50),
                ({trs_source_xlsx_data}, 'Additional XLSX records', 40)
            ;
        """.format(
            table_source=TABLE_SOURCE,
            trs_source_hollins_section=TRS_SOURCE_HOLLINS_SECTION,
            trs_source_hollins_subsection=TRS_SOURCE_HOLLINS_SUBSECTION,
            trs_source_parsed_section=TRS_SOURCE_PARSED_SECTION,
            trs_source_parsed_subsection=TRS_SOURCE_PARSED_SUBSECTION,
            trs_source_xlsx_data=TRS_SOURCE_XLSX_DATA
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        # Load the trs table from Hollins full section trs data
        print('CREATE TABLE: {table_trs} ...'.format(table_trs=TABLE_TRS))

        cur.execute("""
            DROP TABLE IF EXISTS {table_trs};
            CREATE TABLE {table_trs}  (
              id serial PRIMARY KEY,
              map_id integer REFERENCES {table_map},
              tshp integer,
              rng integer,
              sec integer,
              subsec integer,
              source_id integer REFERENCES {table_source}
            );
        """.format(
            table_trs=TABLE_TRS,
            table_map=TABLE_MAP,
            table_source=TABLE_SOURCE
        ))
        con.commit()

        # Insert full section trs records from Hollins trs data
        cur.execute("""
            WITH q1 AS (
            SELECT map_id,
                {function_township_number}(township) tshp,
                {function_range_number}(range) rng,
                regexp_split_to_table(trim(',' from section), ',')::int sec
            FROM {table_hollins_trs}
            ORDER BY map_id, tshp, rng, sec
            )
            INSERT INTO {table_trs} (map_id, tshp, rng, sec, source_id)
            SELECT q1.*, {trs_source_hollins_section}::integer source_id
            FROM q1
            ;
        """.format(
            table_trs=TABLE_TRS,
            table_hollins_trs=TABLE_HOLLINS_TRS,
            function_township_number=FUNCTION_TOWNSHIP_NUMBER,
            function_range_number=FUNCTION_RANGE_NUMBER,
            trs_source_hollins_section=TRS_SOURCE_HOLLINS_SECTION
        ))
        con.commit()

        print('INSERT (HOLLINS SECTION): ' + str(cur.rowcount) + ' rows effected.')

        # Insert trs records from Hollins subsection data
        cur.execute("""
            INSERT INTO {table_trs} (map_id, tshp, rng, sec, subsec, source_id)
            SELECT
                map_id, tshp, rng, sec, subsec,
                {trs_source_hollins_subsection} source_id
            FROM {function_hollins_subsec}()
            ;
        """.format(
            table_trs=TABLE_TRS,
            function_hollins_subsec=FUNCTION_HOLLINS_SUBSEC,
            trs_source_hollins_subsection=TRS_SOURCE_HOLLINS_SUBSECTION
        ))
        con.commit()

        print('INSERT (HOLLINS SUBSECTION): ' + str(cur.rowcount) + ' rows effected.')

        # Read additional map data from the XLSX file
        ws = load_workbook(filename=XLSX_DATA_MAP, read_only=True).active
        HEADER_LINES = 1

        maps = []
        for map in ws.iter_rows(min_row=HEADER_LINES + 1):
            maps.append(tuple(c.value for c in map))

        cur.executemany("""
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
                regexp_matches(
                  regexp_split_to_table((%s)::text, ',\s*'),
                  '(.* )?S(\d+) T?(\d+[NS]) R?(\d+[EW])'
                ) trs,
                (%s)::text note
            )
            INSERT INTO {table_trs} (
               map_id, tshp, rng, sec, subsec, source_id
            )
            SELECT m.id map_id,
                {function_township_number}(trs[3]) tshp,
                {function_range_number}(trs[4]) rng,
                trs[2]::integer sec,
                {function_subsec_bits}(trs[1]) subsec,
                {trs_source_xlsx_data}::integer soutce_id
            FROM q1
            JOIN {table_maptype} t ON q1.maptype = t.maptype
            JOIN {table_map} m ON t.id = m.maptype_id
                AND q1.book = m.book AND q1.page = m.page
            ;
        """.format(
            table_trs=TABLE_TRS,
            table_map=TABLE_MAP,
            table_maptype=TABLE_MAPTYPE,
            function_township_number=FUNCTION_TOWNSHIP_NUMBER,
            function_range_number=FUNCTION_RANGE_NUMBER,
            function_subsec_bits=FUNCTION_SUBSEC_BITS,
            trs_source_xlsx_data=TRS_SOURCE_XLSX_DATA
        ), maps)
        con.commit()

        print('INSERT (EXTRAS): ' + str(cur.rowcount) + ' rows effected.')

        # Vacuum must run outside of a transaction
        con.autocommit = True
        for t in (TABLE_TRS, TABLE_SOURCE):
            cur.execute('VACUUM FREEZE ' + t)


if __name__ == '__main__':

    print('\nCreating staging tables ... ')
    startTime = time.time()

    load_trs()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
