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

    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:

        # Create the map_image table
        print('CREATE TABLE: {table_map_image} ...'.format(table_map_image=TABLE_MAP_IMAGE))

        cur.execute("""
            DROP TABLE IF EXISTS {table_map_image};
            CREATE TABLE {table_map_image} (
              id serial PRIMARY KEY,
              map_id integer NOT NULL REFERENCES {table_map},
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

            obj_iter = bucket.objects.filter(Prefix='map/%s/' % maptype)

            # Collect image files indexed by image basename
            map_image = {}
            for map in obj_iter:
                imagefile = '/' + map.key
                filename = imagefile.split('/')[-1]
                basename = filename.rstrip('.jpg')[:-4]
                if basename not in map_image:
                    map_image[basename] = [imagefile]
                else:
                    map_image[basename].append(imagefile)

            recs = []
            for map, imagefiles in map_image.items():
                recs.append((
                    # easier to ssplit out book/type/page here
                    map[0:3], map[3:5].upper(), map[5:8], ','.join(sorted(imagefiles))))

            rowcount = 0
            for rec in recs:
                cur.execute("""
                    WITH q1 AS (
                        SELECT
                          CAST (%s AS integer) book,
                          CAST (%s AS text) abbrev,
                          CAST (%s AS integer) page,
                          regexp_split_to_table(%s, ',') imagefile
                    ), q2 AS (
                        SELECT
                            m.id map_id,
                            CAST (substring(q1.imagefile from '.*-(\d{{3}})') AS integer) page,
                            q1.imagefile
                        FROM q1
                        JOIN {table_map} m ON q1.book = m.book AND q1.page = m.page
                        JOIN {table_maptype} t ON m.maptype_id = t.id AND q1.abbrev = t.abbrev
                    )
                    INSERT INTO {table_map_image} (map_id, page, imagefile)
                    SELECT map_id, page, imagefile
                    FROM q2
                    ;
                """.format(
                    table_map=TABLE_MAP,
                    table_maptype=TABLE_MAPTYPE,
                    table_map_image=TABLE_MAP_IMAGE
                ), rec)

                if cur.rowcount > 0:
                    rowcount += cur.rowcount
                else:
                    print('>> NO MAP RECORD FOR MAP IMAGE: %s' % (''.join(rec[0:3])))

            con.commit()

            print('INSERT (%s): %d rows affected.' % (maptype.upper(), rowcount))

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
