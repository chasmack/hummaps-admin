import psycopg2
import boto3
import time
import re

from const import *

# load_pdf.py - create and load the pdf table

def load_pdf():
    # Create and load the pdf table.

    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:

        # Create the pdf table
        print('CREATE TABLE: {table_pdf} ...'.format(table_pdf=TABLE_PDF))

        cur.execute("""
            DROP TABLE IF EXISTS {table_pdf};
            CREATE TABLE {table_pdf} (
              id serial PRIMARY KEY,
              map_id integer REFERENCES {table_map},
              pdffile text
            );
        """.format(
            table_pdf=TABLE_PDF,
            table_map=TABLE_MAP
        ))
        con.commit()

        # Get a list of pdf files in the maps bucket
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(S3_BUCKET_MAPS)

        # Process one map type at a time
        cur.execute("""
            SELECT lower(abbrev) FROM {table_maptype}
        """.format(table_maptype=TABLE_MAPTYPE))
        con.commit()

        for maptype in list(row[0] for row in cur):

            pdffiles = ((obj.key,) for obj in bucket.objects.filter(Prefix='pdf/%s/' % (maptype)))
            cur.executemany("""
                 WITH q1 AS (
                    SELECT '/' || (%s)::text pdffile
                ), q2 AS (
                    SELECT
                        map_id, pdffile
                    FROM q1
                    -- need left join on map and maptype together
                    LEFT JOIN (
                        SELECT m.id map_id, m.book, m.page, t.abbrev
                        FROM {table_map} m
                        JOIN {table_maptype} t ON m.maptype_id = t.id
                    ) AS s1
                    ON book = substring(pdffile from '.*/(\d{{3}})')::integer
                    AND page = substring(pdffile from '.*/\d{{3}}..(\d{{3}})')::integer
                    AND abbrev = upper(substring(pdffile from '.*/\d{{3}}(..)'))
                )
                INSERT INTO {table_pdf} (map_id, pdffile)
                SELECT map_id, pdffile FROM q2
                ;
            """.format(
                table_pdf=TABLE_PDF,
                table_map=TABLE_MAP,
                table_maptype=TABLE_MAPTYPE
            ), pdffiles)
            con.commit()

            print('INSERT (%s): %d rows affected.' % (maptype.upper(), cur.rowcount))

        # Vacuum up dead tuples from the update
        # Vacuum must run outside of a transaction
        con.autocommit = True
        cur.execute('VACUUM FREEZE ' + TABLE_PDF)


if __name__ == '__main__':

    print('\nLoading tables ... ')
    startTime = time.time()

    load_pdf()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
