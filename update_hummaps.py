import psycopg2
import subprocess
from subprocess import check_output, Popen
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
from load_trs import load_trs, load_trs_parsed_subsection


def apache2_stop():
    print(check_output(('sudo', 'service', 'apache2', 'stop'), universal_newlines=True))


def apache2_start():
    print(check_output(('sudo', 'service', 'apache2', 'start'), universal_newlines=True))


# Create the hummaps staging schema
def init_database():

    print('CREATE DATABASE: {database_prod} ...'.format(database_prod=DATABASE_PROD))

    sql = """
        DROP DATABASE IF EXISTS {database_prod};
        CREATE DATABASE {database_prod}
            OWNER {user_admin}
            ENCODING 'UTF8'
            TABLESPACE pg_default
            LC_COLLATE 'en_US.UTF-8'
            LC_CTYPE'en_US.UTF-8'
        ;
        DROP ROLE IF EXISTS {user_prod};
        CREATE ROLE {user_prod}
          LOGIN NOINHERIT PASSWORD '{password_prod}';
    """.format(
        database_prod=DATABASE_PROD,
        user_admin=USER_ADMIN,
        user_prod=USER_PROD,
        password_prod=PASSWORD_PROD
    )

    # Need superuser to run the database creation commands
    cmd = ('sudo', '--user', USER_POSTGRES, 'psql')
    proc = Popen(cmd, universal_newlines=True,
                 stdin=subprocess.PIPE,
                 stdout=subprocess.PIPE,
                 stderr=subprocess.STDOUT)
    out = proc.communicate(input=sql)[0]
    print(out)


# Create the hummaps staging schema
def init_staging():

    print('CREATE SCHEMA: {schema_staging} ...'.format(schema_staging=SCHEMA_STAGING))

    with psycopg2.connect(DSN_PROD) as con, con.cursor() as cur:

        cur.execute("""
            DROP SCHEMA IF EXISTS {schema_staging} CASCADE;
            CREATE SCHEMA {schema_staging};
        """.format(schema_staging=SCHEMA_STAGING))
        con.commit()


# Create and load data from staging to the hummaps production schema
def load_prod():

    with psycopg2.connect(DSN_PROD) as con, con.cursor() as cur:

        print('CREATE SCHEMA: {schema_prod} ...'.format(
            schema_prod=SCHEMA_PROD))

        cur.execute("""
            DROP SCHEMA IF EXISTS {schema_prod} CASCADE;
            CREATE SCHEMA {schema_prod};
            GRANT USAGE ON SCHEMA {schema_prod} TO {user_prod};
        """.format(
            schema_prod=SCHEMA_PROD,
            user_prod=USER_PROD
        ))
        con.commit()

        print('CREATE TABLE: {table_prod_maptype} ...'.format(
            table_prod_maptype=TABLE_PROD_MAPTYPE))

        cur.execute("""
            CREATE TABLE {table_prod_maptype} (LIKE {table_maptype} INCLUDING INDEXES);
            GRANT SELECT ON TABLE {table_prod_maptype} TO {user_prod};
            INSERT INTO {table_prod_maptype} SELECT * FROM {table_maptype};
        """.format(
            table_prod_maptype=TABLE_PROD_MAPTYPE,
            table_maptype=TABLE_MAPTYPE,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        print('CREATE TABLE: {table_prod_map} ...'.format(
            table_prod_map=TABLE_PROD_MAP))

        cur.execute("""
            CREATE TABLE {table_prod_map} (LIKE {table_map} INCLUDING INDEXES);
            ALTER TABLE  {table_prod_map} ADD FOREIGN KEY (maptype_id) REFERENCES {table_prod_maptype};
            GRANT SELECT ON TABLE {table_prod_map} TO {user_prod};
            INSERT INTO {table_prod_map} SELECT * FROM {table_map};
        """.format(
            table_prod_map=TABLE_PROD_MAP,
            table_prod_maptype=TABLE_PROD_MAPTYPE,
            table_map=TABLE_MAP,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        print('CREATE TABLE: {table_prod_surveyor} ...'.format(
            table_prod_surveyor=TABLE_PROD_SURVEYOR))

        cur.execute("""
            CREATE TABLE {table_prod_surveyor} (LIKE {table_surveyor} INCLUDING INDEXES);
            GRANT SELECT ON TABLE {table_prod_surveyor} TO {user_prod};
            INSERT INTO {table_prod_surveyor} SELECT * FROM {table_surveyor};
        """.format(
            table_prod_surveyor=TABLE_PROD_SURVEYOR,
            table_surveyor=TABLE_SURVEYOR,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        print('CREATE TABLE: {table_prod_signed_by} ...'.format(
            table_prod_signed_by=TABLE_PROD_SIGNED_BY))

        cur.execute("""
            CREATE TABLE {table_prod_signed_by} (LIKE {table_signed_by} INCLUDING INDEXES);
            ALTER TABLE  {table_prod_signed_by} ADD FOREIGN KEY (map_id) REFERENCES {table_prod_map};
            ALTER TABLE  {table_prod_signed_by} ADD FOREIGN KEY (surveyor_id) REFERENCES {table_prod_surveyor};
            GRANT SELECT ON TABLE {table_prod_signed_by} TO {user_prod};
            INSERT INTO {table_prod_signed_by} SELECT * FROM {table_signed_by};
        """.format(
            table_prod_signed_by=TABLE_PROD_SIGNED_BY,
            table_prod_surveyor=TABLE_PROD_SURVEYOR,
            table_prod_map=TABLE_PROD_MAP,
            table_signed_by=TABLE_SIGNED_BY,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        print('CREATE TABLE: {table_prod_cc} ...'.format(
            table_prod_cc=TABLE_PROD_CC))

        cur.execute("""
            CREATE TABLE {table_prod_cc} (LIKE {table_cc} INCLUDING INDEXES);
            ALTER TABLE  {table_prod_cc} ADD FOREIGN KEY (map_id) REFERENCES {table_prod_map};
            GRANT SELECT ON TABLE {table_prod_cc} TO {user_prod};
            INSERT INTO {table_prod_cc} SELECT * FROM {table_cc};
        """.format(
            table_prod_cc=TABLE_PROD_CC,
            table_prod_map=TABLE_PROD_MAP,
            table_cc=TABLE_CC,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        print('CREATE TABLE: {table_prod_cc_image} ...'.format(
            table_prod_cc_image=TABLE_PROD_CC_IMAGE))

        cur.execute("""
            CREATE TABLE {table_prod_cc_image} (LIKE {table_cc_image} INCLUDING INDEXES);
            ALTER TABLE  {table_prod_cc_image} ADD FOREIGN KEY (cc_id) REFERENCES {table_prod_cc};
            GRANT SELECT ON TABLE {table_prod_cc_image} TO {user_prod};
            INSERT INTO {table_prod_cc_image} SELECT * FROM {table_cc_image};
        """.format(
            table_prod_cc_image=TABLE_PROD_CC_IMAGE,
            table_prod_cc=TABLE_PROD_CC,
            table_cc_image=TABLE_CC_IMAGE,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        print('CREATE TABLE: {table_prod_scan} ...'.format(
            table_prod_scan=TABLE_PROD_SCAN))

        cur.execute("""
            CREATE TABLE {table_prod_scan} (LIKE {table_scan} INCLUDING INDEXES);
            ALTER TABLE  {table_prod_scan} ADD FOREIGN KEY (map_id) REFERENCES {table_prod_map};
            GRANT SELECT ON TABLE {table_prod_scan} TO {user_prod};
            INSERT INTO {table_prod_scan} SELECT * FROM {table_scan};
        """.format(
            table_prod_scan=TABLE_PROD_SCAN,
            table_prod_map=TABLE_PROD_MAP,
            table_scan=TABLE_SCAN,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        print('CREATE TABLE: {table_prod_pdf} ...'.format(
            table_prod_pdf=TABLE_PROD_PDF))

        cur.execute("""
            CREATE TABLE {table_prod_pdf} (LIKE {table_pdf} INCLUDING INDEXES);
            ALTER TABLE  {table_prod_pdf} ADD FOREIGN KEY (map_id) REFERENCES {table_prod_map};
            GRANT SELECT ON TABLE {table_prod_pdf} TO {user_prod};
            INSERT INTO {table_prod_pdf} SELECT * FROM {table_pdf};
        """.format(
            table_prod_pdf=TABLE_PROD_PDF,
            table_prod_map=TABLE_PROD_MAP,
            table_pdf=TABLE_PDF,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        print('CREATE TABLE: {table_prod_map_image} ...'.format(
            table_prod_map_image=TABLE_PROD_MAP_IMAGE))

        cur.execute("""
            CREATE TABLE {table_prod_map_image} (LIKE {table_map_image} INCLUDING INDEXES);
            ALTER TABLE  {table_prod_map_image} ADD FOREIGN KEY (map_id) REFERENCES {table_prod_map};
            GRANT SELECT ON TABLE {table_prod_map_image} TO {user_prod};
            INSERT INTO {table_prod_map_image} SELECT * FROM {table_map_image};
        """.format(
            table_prod_map_image=TABLE_PROD_MAP_IMAGE,
            table_prod_map=TABLE_PROD_MAP,
            table_map_image=TABLE_MAP_IMAGE,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        print('CREATE TABLE: {table_prod_source} ...'.format(
            table_prod_source=TABLE_PROD_SOURCE))

        cur.execute("""
           CREATE TABLE {table_prod_source} (LIKE {table_source} INCLUDING INDEXES);
           GRANT SELECT ON TABLE {table_prod_source} TO {user_prod};
           INSERT INTO {table_prod_source} SELECT * FROM {table_source};
       """.format(
            table_prod_source=TABLE_PROD_SOURCE,
            table_source=TABLE_SOURCE,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        print('CREATE TABLE: {table_prod_trs} ...'.format(
            table_prod_trs=TABLE_PROD_TRS))

        cur.execute("""
           CREATE TABLE {table_prod_trs} (LIKE {table_trs} INCLUDING INDEXES);
            ALTER TABLE  {table_prod_trs} ADD FOREIGN KEY (map_id) REFERENCES {table_prod_map};
            ALTER TABLE  {table_prod_trs} ADD FOREIGN KEY (source_id) REFERENCES {table_prod_source};
           GRANT SELECT ON TABLE {table_prod_trs} TO {user_prod};
           INSERT INTO {table_prod_trs} SELECT * FROM {table_trs};
       """.format(
            table_prod_trs=TABLE_PROD_TRS,
            table_prod_map=TABLE_PROD_MAP,
            table_prod_source=TABLE_PROD_SOURCE,
            table_trs=TABLE_TRS,
            user_prod=USER_PROD
        ))
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        # Vacuum must run outside of a transaction
        con.autocommit = True
        tables = (
            TABLE_PROD_CC,
            TABLE_PROD_CC_IMAGE,
            TABLE_PROD_MAP,
            TABLE_PROD_MAP,
            TABLE_PROD_MAPTYPE,
            TABLE_PROD_PDF,
            TABLE_PROD_SCAN,
            TABLE_PROD_SIGNED_BY,
            TABLE_PROD_SOURCE,
            TABLE_PROD_SURVEYOR,
            TABLE_PROD_TRS
        )
        for t in tables:
            cur.execute('VACUUM FREEZE ' + t)


def show_totals():

    with psycopg2.connect(DSN_PROD) as con, con.cursor() as cur:

        print('CREATE SCHEMA: {schema_prod} ...'.format(
            schema_prod=SCHEMA_PROD))


if __name__ == '__main__':

    print('\nPerforming update ... ')
    startTime = time.time()

    # Create the database and set up the users
    # apache2_stop()
    # init_database()

    # Load update data from Hollins and create staging tables
    init_staging()
    load_update()
    load_map()
    create_funcs()
    load_surveyor()
    load_map_image()
    load_cc()
    load_pdf()
    load_scan()
    load_trs()
    load_trs_parsed_subsection()

    # Copy tables and data from staging to production
    apache2_stop()
    load_prod()
    apache2_start()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
