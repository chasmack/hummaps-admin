import psycopg2
from openpyxl import load_workbook
import time

from const import *

# create_funcs.py - create some useful functions

def create_funcs():


    with psycopg2.connect(DSN_PROD) as con, con.cursor() as cur:

        # Get the map id given maptype abbrev, book and page.
        print('CREATE FUNCTION: {function_map_id} ...'.format(function_map_id=FUNCTION_MAP_ID))

        cur.execute("""
            CREATE OR REPLACE FUNCTION
            {function_map_id}(
              IN param_maptype text,
              IN param_book integer,
              IN param_page integer,
              OUT map_id integer
            ) AS $$
            SELECT m.id
            FROM {table_map} m
            JOIN {table_maptype} t ON m.maptype_id = t.id
            WHERE t.abbrev = upper(param_maptype)
            AND m.book = param_book
            AND m.page <= param_page
            AND (m.page + m.npages) > param_page;
            $$ LANGUAGE SQL
            IMMUTABLE
            ;
        """.format(
            function_map_id=FUNCTION_MAP_ID,
            table_map=TABLE_MAP,
            table_maptype=TABLE_MAPTYPE
        ))
        con.commit()

        # Assemble the map name, e.g. 073pm123, given the map id.
        print('CREATE FUNCTION: {function_map_name} ...'.format(function_map_name=FUNCTION_MAP_NAME))

        cur.execute("""
            CREATE OR REPLACE FUNCTION
            {function_map_name}(
              IN param_map_id integer,
              OUT map_name text)
              RETURNS text AS
            $$
              SELECT
                lpad(m.book::text, 3, '0') || lower(t.abbrev) || lpad(m.page::text, 3, '0') as map_name
              FROM {table_map} m
              JOIN {table_maptype} t ON m.maptype_id = t.id
              WHERE m.id = param_map_id;
            $$
            LANGUAGE sql IMMUTABLE
            ;
        """.format(
            function_map_name=FUNCTION_MAP_NAME,
            table_map=TABLE_MAP,
            table_maptype=TABLE_MAPTYPE
        ))
        con.commit()

        # Translate a township string, e.g. 't7n', into an integer township number.
        # Encoding is: t2s => -2, t1s => -1, t1n => 0, t2n => 1
        print('CREATE FUNCTION: {function_township_number} ...'.format(
            function_township_number=FUNCTION_TOWNSHIP_NUMBER))

        cur.execute("""
            CREATE OR REPLACE FUNCTION
            {function_township_number}(
              IN param_township text,
              OUT tshp integer
            ) AS $$
            WITH q1 AS (
              SELECT regexp_matches(upper(param_township), '^T?(\d+)([NS])$') t
            )
            SELECT CASE
              WHEN t[1]::int = 0 THEN NULL
              WHEN t[2] = 'N' THEN t[1]::int + -1
              WHEN t[2] = 'S' THEN t[1]::int * -1
              ELSE NULL
            END tshp
            FROM q1;
            $$ LANGUAGE SQL
            IMMUTABLE
            ;
        """.format(function_township_number=FUNCTION_TOWNSHIP_NUMBER))
        con.commit()

        # Translate a township number into an township string.
        print('CREATE FUNCTION: {function_township_str} ...'.format(
            function_township_str=FUNCTION_TOWNSHIP_STR))

        cur.execute("""
            CREATE OR REPLACE FUNCTION
            {function_township_str}(
              IN param_township integer,
              OUT tshp text
            ) AS $$
            SELECT CASE
              WHEN param_township between   0 and 98 THEN (param_township +  1)::text || 'N'
              WHEN param_township between -99 and -1 THEN (param_township * -1)::text || 'S'
              ELSE NULL
            END rng;
            $$ LANGUAGE SQL
            IMMUTABLE
            ;
        """.format(function_township_str=FUNCTION_TOWNSHIP_STR))
        con.commit()

        # Translate a range string, e.g. 'r4e', into an integer range number.
        # Encoding is: r2w => -2, r1w => -1, r1e => 0, r2e => 1
        print('CREATE FUNCTION: {function_range_number} ...'.format(
            function_range_number=FUNCTION_RANGE_NUMBER))

        cur.execute("""

            CREATE OR REPLACE FUNCTION
            {function_range_number}(
              IN param_range text,
              OUT rng integer
            ) AS $$
            WITH q1 AS (
              SELECT regexp_matches(upper(param_range), '^R?(\d+)([EW])$') r
            )
            SELECT CASE
              WHEN r[1]::int = 0 THEN NULL
              WHEN r[2] = 'E' THEN r[1]::int + -1
              WHEN r[2] = 'W' THEN r[1]::int * -1
              ELSE NULL
            END rng
            FROM q1;
            $$ LANGUAGE SQL
            IMMUTABLE
            ;
        """.format(function_range_number=FUNCTION_RANGE_NUMBER))
        con.commit()

        # Translate a range number into an range string.
        print('CREATE FUNCTION: {function_range_str} ...'.format(
            function_range_str=FUNCTION_RANGE_STR))

        cur.execute("""
            CREATE OR REPLACE FUNCTION
            {function_range_str}(
              IN param_range integer,
              OUT rng text
            ) AS $$
            SELECT CASE
              WHEN param_range between  0 and  98 THEN (param_range +  1)::text || 'E'
              WHEN param_range between -99 and -1 THEN (param_range * -1)::text || 'W'
              ELSE NULL
            END rng;
            $$ LANGUAGE SQL
            IMMUTABLE
            ;
        """.format(function_range_str=FUNCTION_RANGE_STR))
        con.commit()

        # A table for converting between subsection strings and integer bit maps
        print('CREATE TABLE: {table_subsec_names} ...'.format(
            table_subsec_names=TABLE_SUBSEC_NAMES))

        cur.execute("""
            DROP TABLE IF EXISTS {table_subsec_names};
            CREATE TABLE {table_subsec_names}  (
              bits integer,
              str text,
              rank integer
            );
            INSERT INTO {table_subsec_names} (bits, str, rank)
            VALUES
            -- 16 bits
            (x'ffff'::int, '1/1', 16),
            -- 8 bits
            (x'00ff'::int, 'N/2', 8), (x'ff00'::int, 'S/2', 8),
            (x'cccc'::int, 'E/2', 8), (x'3333'::int, 'W/2', 8),
            -- 4 bits
            (x'00cc'::int, 'NE/4', 4), (x'cc00'::int, 'SE/4', 4),
            (x'3300'::int, 'SW/4', 4), (x'0033'::int, 'NW/4', 4),
            (x'000f'::int, 'N/2 N/2', 4), (x'00f0'::int, 'S/2 N/2', 4),
            (x'0f00'::int, 'N/2 S/2', 4), (x'f000'::int, 'S/2 S/2', 4),
            (x'8888'::int, 'E/2 E/2', 4), (x'4444'::int, 'W/2 E/2', 4),
            (x'2222'::int, 'E/2 W/2', 4), (x'1111'::int, 'W/2 W/2', 4),
            -- 2 bits
            (x'000c'::int, 'N/2 NE/4', 2), (x'00c0'::int, 'S/2 NE/4', 2),
            (x'0088'::int, 'E/2 NE/4', 2), (x'0044'::int, 'W/2 NE/4', 2),
            (x'0c00'::int, 'N/2 SE/4', 2), (x'c000'::int, 'S/2 SE/4', 2),
            (x'8800'::int, 'E/2 SE/4', 2), (x'4400'::int, 'W/2 SE/4', 2),
            (x'0300'::int, 'N/2 SW/4', 2), (x'3000'::int, 'S/2 SW/4', 2),
            (x'2200'::int, 'E/2 SW/4', 2), (x'1100'::int, 'W/2 SW/4', 2),
            (x'0003'::int, 'N/2 NW/4', 2), (x'0030'::int, 'S/2 NW/4', 2),
            (x'0022'::int, 'E/2 NW/4', 2), (x'0011'::int, 'W/2 NW/4', 2),
            -- 1 bit
            (x'0008'::int, 'NE/4 NE/4', 1), (x'0080'::int, 'SE/4 NE/4', 1),
            (x'0040'::int, 'SW/4 NE/4', 1), (x'0004'::int, 'NW/4 NE/4', 1),
            (x'0800'::int, 'NE/4 SE/4', 1), (x'8000'::int, 'SE/4 SE/4', 1),
            (x'4000'::int, 'SW/4 SE/4', 1), (x'0400'::int, 'NW/4 SE/4', 1),
            (x'0200'::int, 'NE/4 SW/4', 1), (x'2000'::int, 'SE/4 SW/4', 1),
            (x'1000'::int, 'SW/4 SW/4', 1), (x'0100'::int, 'NW/4 SW/4', 1),
            (x'0002'::int, 'NE/4 NW/4', 1), (x'0020'::int, 'SE/4 NW/4', 1),
            (x'0010'::int, 'SW/4 NW/4', 1), (x'0001'::int, 'NW/4 NW/4', 1)
            ;
        """.format(table_subsec_names=TABLE_SUBSEC_NAMES))
        con.commit()

        # A function to convert a subsection string to an integer bit map
        print('CREATE FUNCTION: {function_subsec_bits} ...'.format(
            function_subsec_bits=FUNCTION_SUBSEC_BITS))

        cur.execute("""
            CREATE OR REPLACE FUNCTION
            {function_subsec_bits}(
              IN param_subsec text,
              OUT bits integer
            ) AS $$
            SELECT bits FROM {table_subsec_names}
            WHERE regexp_replace(upper(trim(from param_subsec)), '\s\s', ' ', 'g') = str;
            $$ LANGUAGE SQL
            IMMUTABLE
            ;
        """.format(
            function_subsec_bits=FUNCTION_SUBSEC_BITS,
            table_subsec_names=TABLE_SUBSEC_NAMES
        ))
        con.commit()

        # A function to convert an integer bit map to a subsection string
        print('CREATE FUNCTION: {function_subsec_str} ...'.format(
            function_subsec_str=FUNCTION_SUBSEC_STR))

        cur.execute("""
            CREATE OR REPLACE FUNCTION
            {function_subsec_str}(
              IN param_subsec integer,
              OUT str text
            ) AS $$
            SELECT str FROM {table_subsec_names}
            WHERE param_subsec = bits;
            $$ LANGUAGE SQL
            IMMUTABLE
            ;
        """.format(
            function_subsec_str=FUNCTION_SUBSEC_STR,
            table_subsec_names=TABLE_SUBSEC_NAMES
        ))
        con.commit()

        # A function to convert legacy comma separated qq strings into bit patterns.
        #
        # This function also addresses the problem of accessing the "unique"
        # structure of the subsection data in the Hollins map table. It runs
        # through the column names returning a set of map ids and subsection
        # bitmaps for that column in the hollins map qq table. It also returns
        # township, range and section parsed from the column name.
        #
        # Hollins numbered the qq sections 1 through 16 starting in the upper
        # right hand corner and working back and forth down the section in the
        # same way sections are numbered in a township (boustrophedon).
        #
        # This scheme numbers the qq sections in a more conventional manner
        # starting in the upper left corner filling rows from left to right.
        # These numbers are translated into a 32 bit integer with bit 0 (lsb)
        # representing qq section #1 (NW/4 NW/4) and bit 15 representing
        # qq section #16 (SE/4 SE/4). For example -
        #
        # 0x0008 => #4 (NE/4 NE/4)
        # 0x0100 => #9 (NW/4 SW/4)
        # 0xcc00 => #11,12,15,16 (SE/4)
        #

        print('CREATE FUNCTION: {function_hollins_subsec} ...'.format(
            function_hollins_subsec=FUNCTION_HOLLINS_SUBSEC))

        cur.execute("""
            CREATE OR REPLACE FUNCTION
            {function_hollins_subsec}()
            RETURNS TABLE(
                map_id int,
                tshp int,
                rng int,
                sec int,
                subsec int
            ) AS $$
            DECLARE
                column_name text;
            BEGIN
                FOR column_name IN
                SELECT c.column_name
                FROM information_schema.columns c
                WHERE c.table_schema = '{schema_staging}'
                AND c.table_name = substring('{table_hollins_map_qq}', '\.(.*)')
                AND c.column_name != 'id'
                ORDER BY c.column_name
            LOOP
            RETURN QUERY EXECUTE format('
                WITH q1 AS (
                SELECT
                    ''%s''::text AS column_name,
                    id AS map_id,
                    regexp_split_to_table(trim('','' from %I), '','')::int AS hollins_subsec
                FROM {table_hollins_map_qq}
                ), q2 as (
                SELECT
                    map_id,
                    {function_township_number}(substring(column_name from ''(\d+[ns])'')) tshp,
                    {function_range_number}(substring(column_name from ''\d+[ns](\d+[ew])'')) rng,
                    substring(column_name from ''\d+[ns]\d+[ew](\d+)'')::integer sec,
                    CASE
                        WHEN hollins_subsec = 1  THEN x''0008''::int
                        WHEN hollins_subsec = 2  THEN x''0004''::int
                        WHEN hollins_subsec = 3  THEN x''0002''::int
                        WHEN hollins_subsec = 4  THEN x''0001''::int
                        WHEN hollins_subsec = 5  THEN x''0010''::int
                        WHEN hollins_subsec = 6  THEN x''0020''::int
                        WHEN hollins_subsec = 7  THEN x''0040''::int
                        WHEN hollins_subsec = 8  THEN x''0080''::int
                        WHEN hollins_subsec = 9  THEN x''0800''::int
                        WHEN hollins_subsec = 10 THEN x''0400''::int
                        WHEN hollins_subsec = 11 THEN x''0200''::int
                        WHEN hollins_subsec = 12 THEN x''0100''::int
                        WHEN hollins_subsec = 13 THEN x''1000''::int
                        WHEN hollins_subsec = 14 THEN x''2000''::int
                        WHEN hollins_subsec = 15 THEN x''4000''::int
                        WHEN hollins_subsec = 16 THEN x''8000''::int
                        ELSE NULL
                    END AS subsec
                FROM q1
                WHERE hollins_subsec IS NOT NULL
                )
                SELECT
                    map_id, tshp, rng, sec,
                    sum(subsec)::int AS subsec
                FROM q2
                GROUP BY map_id, tshp, rng, sec
                ORDER BY map_id
            ', column_name, column_name);
            END LOOP;
            END;
            $$ LANGUAGE plpgsql
            IMMUTABLE;
        """.format(
            function_hollins_subsec=FUNCTION_HOLLINS_SUBSEC,
            function_township_number=FUNCTION_TOWNSHIP_NUMBER,
            function_range_number=FUNCTION_RANGE_NUMBER,
            table_hollins_map_qq=TABLE_HOLLINS_MAP_QQ,
            schema_staging=SCHEMA_STAGING
        ))
        con.commit()


if __name__ == '__main__':

    print('\nCreating staging functions ... ')
    startTime = time.time()

    create_funcs()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))