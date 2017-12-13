import psycopg2
import boto3
import time
import re

from const import *

# load_scan.py - create and load the scan table

# Create and load the scan table.
def load_scan():

    with psycopg2.connect(DSN_PROD) as con, con.cursor() as cur:

        # Create the scan table
        print('CREATE TABLE: {table_scan} ...'.format(table_scan=TABLE_SCAN))

        cur.execute("""
            DROP TABLE IF EXISTS {table_scan};
            CREATE TABLE {table_scan} (
              id serial PRIMARY KEY,
              map_id integer REFERENCES {table_map},
              page integer,
              scanfile text
            );
        """.format(
            table_scan=TABLE_SCAN,
            table_map=TABLE_MAP
        ))
        con.commit()

        # Get a list of scan files in the maps bucket
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(S3_BUCKET_MAPS)

        # Process one map type at a time
        cur.execute("""
            SELECT lower(abbrev) FROM {table_maptype}
        """.format(table_maptype=TABLE_MAPTYPE))
        con.commit()

        for maptype in list(row[0] for row in cur):

            scanfiles = ((obj.key,) for obj in bucket.objects.filter(Prefix='scan/%s/' % (maptype)))
            cur.executemany("""
                 WITH q1 AS (
                    SELECT '/' || (%s)::text scanfile
                ), q2 AS (
                    SELECT
                        map_id,
                        substring(scanfile from '.*-(\d{{3}})')::integer scan_page,
                        scanfile
                    FROM q1
                    -- need left join on map and maptype together
                    LEFT JOIN (
                        SELECT m.id map_id, m.book, m.page, t.abbrev
                        FROM {table_map} m
                        JOIN {table_maptype} t ON m.maptype_id = t.id
                    ) AS s1
                    ON book = substring(scanfile from '.*/(\d{{3}})')::integer
                    AND page = substring(scanfile from '.*/\d{{3}}..(\d{{3}})')::integer
                    AND abbrev = upper(substring(scanfile from '.*/\d{{3}}(..)'))
                )
                INSERT INTO {table_scan} (map_id, page, scanfile)
                SELECT map_id, scan_page, scanfile FROM q2
                ;
            """.format(
                table_scan=TABLE_SCAN,
                table_map=TABLE_MAP,
                table_maptype=TABLE_MAPTYPE
            ), scanfiles)
            con.commit()

            print('INSERT (%s): %d rows affected.' % (maptype.upper(), cur.rowcount))

        # Vacuum up dead tuples from the update
        # Vacuum must run outside of a transaction
        con.autocommit = True
        cur.execute('VACUUM FREEZE ' + TABLE_SCAN)


if __name__ == '__main__':

    print('\nLoading tables ... ')
    startTime = time.time()

    load_scan()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
