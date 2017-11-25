from openpyxl import load_workbook
import psycopg2
import boto3
import time

from const import *

# load_cc.py - create and load the cc and cc_image tables from XLSX data

def load_cc():
    # Load the cc table from cc.xlsx.

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
            DROP TABLE IF EXISTS {table_cc} CASCADE;
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
        # Vacuum must run outside of a transaction
        con.autocommit = True
        cur.execute('VACUUM FREEZE ' + TABLE_CC)


def load_cc_image():
    # Load the cc_image table.

    # Get a list of cc images in the maps bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET_MAPS)
    obj_iter = bucket.objects.filter(Prefix='map/cc/')

    # Collect image files indexed by cc basename
    cc_image = {}
    for cc in obj_iter:
        imagefile = '/' + cc.key
        filename = imagefile.split('/')[-1]
        basename = filename.rstrip('.jpg')[:-4]
        if basename not in cc_image:
            cc_image[basename] = [imagefile]
        else:
            cc_image[basename].append(imagefile)

    # Replace the basename keys with doc numbers
    for basename in list(cc_image.keys()):
        npages = len(cc_image[basename])
        p0, cc_type, p1 = (p.lstrip('0') for p in basename.split('-'))
        if cc_type == 'or':
            doc_number = '%s OR %s' % (p0, p1)
        else:
            doc_number = '%s-%s-%d' % (p0, p1, npages)
        cc_image[doc_number] = cc_image.pop(basename)

    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:

        # Get a list of CCs and do some checks
        cur.execute("""
            SELECT doc_number, npages FROM {table_cc};
        """.format(table_cc=TABLE_CC))
        con.commit()

        cc = dict(cur)
        for doc_number, npages in cc.items():
            if doc_number not in cc_image:
                print('>> NO IMAGEFILE FOR CC: %s' % (doc_number))

            elif npages != len(cc_image[doc_number]):
                print('>> CC PAGE COUNT ERROR: cc=%d cc_image=%d' % (npages, len(cc_image[doc_number])))

        for doc_number in cc_image.keys():
            if doc_number not in cc:
                print('>> NO CC FOR IMAGEFILE: %s' % (doc_number))

        # Create and populate the cc_image table
        print('CREATE TABLE: {table_cc_image} ...'.format(table_cc_image=TABLE_CC_IMAGE))

        cur.execute("""
            DROP TABLE IF EXISTS {table_cc_image};
            CREATE TABLE {table_cc_image} (
              id serial PRIMARY KEY,
              cc_id integer NOT NULL REFERENCES {table_cc},
              page integer,
              imagefile text
            );
        """.format(
            table_cc_image=TABLE_CC_IMAGE,
            table_cc=TABLE_CC
        ))

        # Join the list of image files so we can pass them as a single parameter
        recs = ((doc_number, ','.join(imagefiles)) for doc_number, imagefiles in cc_image.items())
        cur.executemany("""
            WITH q1 AS (
                SELECT
                  CAST (%s AS text) doc_number,
                  regexp_split_to_table(%s, ',') imagefile
            ), q2 AS (
                SELECT
                    cc.id cc_id,
                    CAST (substring(q1.imagefile from '.*-(\d{{3}})') AS integer) page,
                    q1.imagefile
                FROM q1
                JOIN {table_cc} cc USING (doc_number)
            )
            INSERT INTO {table_cc_image} (cc_id, page, imagefile)
            SELECT cc_id, page, imagefile
            FROM q2
            ;
        """.format(
            table_cc=TABLE_CC,
            table_cc_image=TABLE_CC_IMAGE
        ), recs)
        con.commit()

        print('INSERT: %d rows affected.' % (cur.rowcount))

        # Vacuum up dead tuples from the update
        # Vacuum must run outside of a transaction
        con.autocommit = True
        cur.execute('VACUUM FREEZE ' + TABLE_CC_IMAGE)


if __name__ == '__main__':

    print('\nLoading tables ... ')
    startTime = time.time()

    load_cc()
    load_cc_image()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
