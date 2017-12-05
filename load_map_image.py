from openpyxl import load_workbook
import psycopg2
import boto3
import time

from const import *

# load_map_image.py - create and load the map_image table

def load_map_image():
    # Load the map_image table.

    # Get a list of map images in the maps bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET_MAPS)

    with psycopg2.connect(DSN_PROD) as con, con.cursor() as cur:

        # Create the map_image table
        print('CREATE TABLE: {table_map_image} ...'.format(table_map_image=TABLE_MAP_IMAGE))

        cur.execute("""
            DROP TABLE IF EXISTS {table_map_image};
            CREATE TABLE {table_map_image} (
              id serial PRIMARY KEY,
              map_id integer REFERENCES {table_map},
              page integer,
              imagefile text
            );
        """.format(
            table_map_image=TABLE_MAP_IMAGE,
            table_map=TABLE_MAP
        ))
        con.commit()

        # Process one map type at a time
        cur.execute("""
            SELECT lower(abbrev) FROM {table_maptype}
        """.format(table_maptype=TABLE_MAPTYPE))
        con.commit()

        for maptype in list(row[0] for row in cur):

            imagefiles = ((obj.key,) for obj in bucket.objects.filter(Prefix='map/%s/' % (maptype)))
            cur.executemany("""
                WITH q1 AS (
                    SELECT '/' || (%s)::text imagefile
                ), q2 AS (
                    SELECT
                        map_id,
                        substring(imagefile from '.*-(\d{{3}})')::integer image_page,
                        imagefile
                    FROM q1
                    -- need left join on map and maptype together
                    LEFT JOIN (
                        SELECT m.id map_id, m.book, m.page, t.abbrev
                        FROM {table_map} m
                        JOIN {table_maptype} t ON m.maptype_id = t.id
                    ) AS s1
                    ON book = substring(imagefile from '.*/(\d{{3}})')::integer
                    AND page = substring(imagefile from '.*/\d{{3}}..(\d{{3}})')::integer
                    AND abbrev = upper(substring(imagefile from '.*/\d{{3}}(..)'))
                )
                INSERT INTO {table_map_image} (map_id, page, imagefile)
                SELECT map_id, image_page, imagefile FROM q2
                ;
            """.format(
                table_map=TABLE_MAP,
                table_maptype=TABLE_MAPTYPE,
                table_map_image=TABLE_MAP_IMAGE
            ), imagefiles)
            con.commit()

            print('INSERT (%s): %d rows affected.' % (maptype.upper(), cur.rowcount))

        # Vacuum up dead tuples from the update
        # Vacuum must run outside of a transaction
        con.autocommit = True
        cur.execute('VACUUM FREEZE ' + TABLE_MAP_IMAGE)


if __name__ == '__main__':

    print('\nLoading tables ... ')
    startTime = time.time()

    load_map_image()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
