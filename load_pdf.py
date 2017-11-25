import psycopg2
import boto3
import time
import re

from const import *

# load_pdf.py - create and load the pdf table


def load_pdf():
    # Create and load the pdf table.

    # Get a list of pdf files in the maps bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET_MAPS)
    obj_iter = bucket.objects.filter(Prefix='pdf/')

    # Collect pdf files indexed by map name
    pdffiles = []
    for pdf in obj_iter:
        pdffile = '/' + pdf.key
        filename = pdffile.split('/')[-1]
        m = re.match('(\d{3})([a-z]{2})(\d{3})', filename)
        if not m:
            print('>> PDF FILENAME ERROR: %s' % (pdffile))
            continue
        book, maptype, page = m.groups()
        pdffiles.append((maptype, book, page, pdffile))

    print('PDF FILES FOUND: %d' % (len(pdffiles)))

    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:

        # Create and populate the pdf table
        print('CREATE TABLE: {table_pdf} ...'.format(table_pdf=TABLE_PDF))

        cur.execute("""
            DROP TABLE IF EXISTS {table_pdf};
            CREATE TABLE {table_pdf} (
              id serial PRIMARY KEY,
              map_id integer NOT NULL REFERENCES {table_map},
              pdffile text
            );
        """.format(
            table_pdf=TABLE_PDF,
            table_map=TABLE_MAP
        ))

        rowcount = 0
        for pdf in pdffiles:
            cur.execute("""
                WITH q1 AS (
                    SELECT
                      upper(%s) abbrev,
                      CAST (%s AS integer) book,
                      CAST (%s AS integer) page,
                      %s pdffile
                ), q2 AS (
                    SELECT
                        m.id map_id,
                        q1.pdffile pdffile
                    FROM q1
                    LEFT JOIN {table_map} m USING (book, page)
                    JOIN {table_maptype} t ON m.maptype_id = t.id
                    WHERE t.abbrev = q1.abbrev
                )
                INSERT INTO {table_pdf} (map_id, pdffile)
                SELECT map_id, pdffile
                FROM q2
                WHERE map_id IS NOT NULL
                ;
            """.format(
                table_pdf=TABLE_PDF,
                table_map=TABLE_MAP,
                table_maptype=TABLE_MAPTYPE
            ), pdf)

            if cur.rowcount == 1:
                rowcount += 1
            else:
                print('>> NO MAP RECORD FOR PDF: %s' % (pdf[3]))

        con.commit()

        print('INSERT: %d rows affected.' % (rowcount))

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
