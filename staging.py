import psycopg2
import time

# init_staging.py - create the hummaps_staging schema

PG_DATABASE = 'production'
PG_USER = 'ubuntu'
PG_HOST = 'localhost'
PG_PASSWORD = 'pg'

# IPV4 connection
# PG_DSN = 'dbname={database} user={user} host={host} password={password}'.format(
#     database=PG_DATABASE, user=PG_USER, host=PG_HOST, password=PG_PASSWORD
# )

# UNIX domain socket connection
PG_DSN = 'dbname={database} user={user}'.format(
    database=PG_DATABASE, user=PG_USER
)

USER_ADMIN = 'ubuntu'
USER_PROD = 'hummaps'

STAGING_SCHEMA = 'hummaps_staging'

STAGING_TABLE_CC = STAGING_SCHEMA + '.' + 'cc'
STAGING_TABLE_CC_IMAGE = STAGING_SCHEMA + '.' + 'cc_image'
STAGING_TABLE_MAP = STAGING_SCHEMA + '.' + 'map'
STAGING_TABLE_MAP_IMAGE = STAGING_SCHEMA + '.' + 'map_image'
STAGING_TABLE_MAPTYPE = STAGING_SCHEMA + '.' + 'maptype'
STAGING_TABLE_SUBSECTION_NAMES = STAGING_SCHEMA + '.' + 'subsection_names'
STAGING_TABLE_SIGNED_BY = STAGING_SCHEMA + '.' + 'signed_by'
STAGING_TABLE_SURVEYOR = STAGING_SCHEMA + '.' + 'surveyor'
STAGING_TABLE_SOURCE = STAGING_SCHEMA + '.' + 'source'
STAGING_TABLE_TRS = STAGING_SCHEMA + '.' + 'trs'

UPDATE_SCHEMA = 'hummaps_update'

UPDATE_TABLE_HOLLINS_MAP = UPDATE_SCHEMA + '.' + 'hollins_map'
UPDATE_TABLE_HOLLINS_MAP_QQ = UPDATE_SCHEMA + '.' + 'hollins_map_qq'
UPDATE_TABLE_HOLLINS_SUBSECTION_LIST = UPDATE_SCHEMA + '.' + 'hollins_subsection_list'
UPDATE_TABLE_HOLLINS_SURVEYOR = UPDATE_SCHEMA + '.' + 'hollins_surveyor'
UPDATE_TABLE_HOLLINS_TRS = UPDATE_SCHEMA + '.' + 'hollins_trs'
UPDATE_TABLE_SURVEYOR = UPDATE_SCHEMA + '.' + 'surveyor'
UPDATE_TABLE_CC = UPDATE_SCHEMA + '.' + 'cc'


def init():
    # Create the hummaps_staging schema

    print('CREATE SCHEMA: {schema} ...'.format(schema=STAGING_SCHEMA))

    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:

        pg_qstr = """
            DROP SCHEMA IF EXISTS {staging_schema} CASCADE;
            CREATE SCHEMA {staging_schema};
        """.format(staging_schema=STAGING_SCHEMA)
        cur.execute(pg_qstr)
        con.commit()


def load_map():
    # Load the map and maptype tables

    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:

        # Load the maptype table
        print('CREATE TABLE: {staging_table_maptype} ...'.format(staging_table_maptype=STAGING_TABLE_MAPTYPE))

        MAPTYPES = (
            ("Corner Record", "CR"),
            ("Highway Map", "HM"),
            ("Monument Map", "MM"),
            ("Parcel Map", "PM"),
            ("Record Map", "RM"),
            ("Survey", "RS"),
            ("Unrecorded Map", "UR"),
        )

        cur.execute("""
            CREATE TABLE {staging_table_maptype}  (
               id serial PRIMARY KEY,
               maptype text,
               abbrev text
            );
        """.format(staging_table_maptype=STAGING_TABLE_MAPTYPE))

        cur.executemany("""
            INSERT INTO {staging_table_maptype} (maptype, abbrev) VALUES (%s, %s);
        """.format(staging_table_maptype=STAGING_TABLE_MAPTYPE), MAPTYPES)
        rowcount = cur.rowcount
        con.commit()

        # Check we have all the maptypes listed in hollins
        cur.execute("""
            SELECT maptype
            FROM {update_table_hollins_map} m
            LEFT JOIN {staging_table_maptype} t USING (maptype)
            WHERE t.id is NULL;
        """.format(
            update_table_hollins_map=UPDATE_TABLE_HOLLINS_MAP,
            staging_table_maptype=STAGING_TABLE_MAPTYPE,
        ))
        con.commit()
        assert cur.rowcount == 0

        print('INSERT: ' + str(rowcount) + ' rows effected.')

        # Load the map tables from Hollins update
        print('CREATE TABLE: {staging_table_map} ...'.format(staging_table_map=STAGING_TABLE_MAP))

        cur.execute("""
            CREATE TABLE {staging_table_map}  (
              id integer PRIMARY KEY,
              maptype_id integer REFERENCES {staging_table_maptype},
              book integer,
              page integer,
              npages integer,
              recdate date,
              client text,
              description text,
              note text
            );
        """.format(
            staging_table_map=STAGING_TABLE_MAP,
            staging_table_maptype=STAGING_TABLE_MAPTYPE
        ))
        con.commit()

        cur.execute("""
            INSERT INTO {staging_table_map} (
               id, maptype_id, book, page, npages,
               recdate, client, description, note
            )
            SELECT
              {update_table_hollins_map}.id,
              {staging_table_maptype}.id,
              {update_table_hollins_map}.book,
              {update_table_hollins_map}.firstpage,
              {update_table_hollins_map}.lastpage - {update_table_hollins_map}.firstpage + 1,
              {update_table_hollins_map}.recdate,
              {update_table_hollins_map}.donefor,
              {update_table_hollins_map}.descript,
              {update_table_hollins_map}.comment
            FROM {update_table_hollins_map}
            LEFT JOIN {staging_table_maptype} USING (maptype)
            ;
        """.format(
            staging_table_map=STAGING_TABLE_MAP,
            staging_table_maptype=STAGING_TABLE_MAPTYPE,
            update_table_hollins_map=UPDATE_TABLE_HOLLINS_MAP
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')


if __name__ == '__main__':

    print('\nCreating staging tables ... ')
    startTime = time.time()

    init()
    load_map()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
