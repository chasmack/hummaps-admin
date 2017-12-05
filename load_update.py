import xml.etree.ElementTree as ET
from openpyxl import load_workbook
import psycopg2
from os.path import join
import time

from const import *

# load_update.py - load data from hollins

def load_update():
    # Load the hummaps staging tables from XML data

    with psycopg2.connect(DSN_PROD) as con, con.cursor() as cur:

        # Load Hollins surveyor from XML
        print('CREATE TABLE: {table_hollins_surveyor} ...'.format(table_hollins_surveyor=TABLE_HOLLINS_SURVEYOR))

        cur.execute("""
            DROP TABLE IF EXISTS {table_hollins_surveyor};
            CREATE TABLE {table_hollins_surveyor} (
              id serial PRIMARY KEY,
              fullname text,
              lastname text
            );
        """.format(table_hollins_surveyor=TABLE_HOLLINS_SURVEYOR))

        tree = ET.parse(join(UPDATE_DIR, 'surveyor.xml'))
        root = tree.getroot()
        rows = []
        keys = ('Surveyor', 'lastname')
        for rec in root:
            rows.append(list(rec.find(k).text for k in keys))

        cur.executemany("""
          INSERT INTO {table} (fullname, lastname) VALUES ( %s, %s );
        """.format(table=TABLE_HOLLINS_SURVEYOR), rows)
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        # Load Hollins subsection list from XML
        print('CREATE TABLE: {table_hollins_subsection_list} ...'.format(
            table_hollins_subsection_list=TABLE_HOLLINS_SUBSECTION_LIST
        ))

        cur.execute("""
            DROP TABLE IF EXISTS {table_hollins_subsection_list};
            CREATE TABLE {table_hollins_subsection_list} (
              id serial PRIMARY KEY,
              order_code integer,
              subsection text
            );
        """.format(table_hollins_subsection_list=TABLE_HOLLINS_SUBSECTION_LIST))

        tree = ET.parse(join(UPDATE_DIR, 'subsectionlist.xml'))
        root = tree.getroot()
        rows = []
        keys = ('OrderCode', 'subsection')
        for rec in root:
            rows.append(list(rec.find(k).text for k in keys))

        cur.executemany("""
            INSERT INTO {table_hollins_subsection_list} (order_code, subsection)
            VALUES ( %s, %s );
        """.format(table_hollins_subsection_list=TABLE_HOLLINS_SUBSECTION_LIST), rows)
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        # Load Hollins map from XML
        print('CREATE TABLE: {table_hollins_map} ...'.format(table_hollins_map=TABLE_HOLLINS_MAP))

        cur.execute("""
            DROP TABLE IF EXISTS {table_hollins_map} CASCADE;
            CREATE TABLE {table_hollins_map} (
              id integer PRIMARY KEY,
              maptype text,
              book integer,
              firstpage integer,
              lastpage integer,
              recdate date,
              surveyor text,
              donefor text,
              descript text,
              image text,
              comment text
            );
        """.format(table_hollins_map=TABLE_HOLLINS_MAP))

        tree = ET.parse(join(UPDATE_DIR, 'map.xml'))
        root = tree.getroot()
        rows = []
        keys = ('ID', 'maptype', 'BOOK', 'FIRSTPAGE', 'LASTPAGE', 'RECDATE',
                'SURVEYOR', 'DONEFOR', 'DESCRIP', 'Picture', 'Comment')
        for rec in root:
            rows.append(list(c if c is None else c.text for c in (rec.find(k) for k in keys)))

        # Changed a couple field names from the original
        cur.executemany("""
            INSERT INTO {table} (
              id, maptype, book, firstpage, lastpage, recdate,
              surveyor, donefor, descript, image, comment
            ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """.format(table=TABLE_HOLLINS_MAP), rows)
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        # Split out the quarter-quarter section info from Hollins map into its own table
        print('CREATE TABLE: {table_hollins_map_qq} ...'.format(table_hollins_map_qq=TABLE_HOLLINS_MAP_QQ))

        # get a list of the qq section columns from map
        tree = ET.parse(join(UPDATE_DIR, 'map.xml'))
        root = tree.getroot()
        skip_tags = (
            'id', 'maptype', 'book', 'firstpage', 'lastpage',
            'recdate', 'surveyor', 'donefor', 'descrip', 'picture', 'comment')

        qq_cols = set()
        for row in root.findall('map'):
            for col in row:
                tag = col.tag.lower()
                if tag in skip_tags:
                    continue
                qq_cols.add(tag[7:])
        qq_cols = sorted(qq_cols)

        # Create the maps_qq table
        cur.execute("""
            DROP TABLE IF EXISTS {table_hollins_map_qq};
            CREATE TABLE {table_hollins_map_qq} (
              id integer PRIMARY KEY,
              FOREIGN KEY (id) REFERENCES {table_hollins_map}
            );
        """.format(table_hollins_map_qq=TABLE_HOLLINS_MAP_QQ, table_hollins_map=TABLE_HOLLINS_MAP))

        for col in qq_cols:
            cur.execute("""
                ALTER TABLE {table_hollins_map_qq} ADD COLUMN "{col}" text;
            """.format(table_hollins_map_qq=TABLE_HOLLINS_MAP_QQ, col=col))
        con.commit()

        # Populate map_qq with maps with qq information
        for row in root.findall('map'):
            qq_cols = []
            for col in row:
                tag = col.tag.lower()
                if tag in skip_tags:
                    continue
                qq_cols.append((tag[7:], col.text))
            if len(qq_cols) == 0:
                continue
            id = row.find('ID').text
            qq_values = ', '.join("'%s'" % qq[1] for qq in qq_cols)
            qq_cols = ', '.join('"%s"' % qq[0] for qq in qq_cols)
            cur.execute("""
                INSERT INTO {table_hollins_map_qq} (id, {qq_cols})
                VALUES ({id}, {qq_values});
            """.format(
                table_hollins_map_qq=TABLE_HOLLINS_MAP_QQ,
                id=id, qq_cols=qq_cols, qq_values=qq_values
            ))

        cur.execute("""
            SELECT count(*) FROM {table_hollins_map_qq};
        """.format(table_hollins_map_qq=TABLE_HOLLINS_MAP_QQ))
        con.commit()

        print('INSERT: %d rows affected.' % cur.fetchone())

        # Load Hollins trs from XML
        print('CREATE TABLE: {table_hollins_trs} ...'.format(table_hollins_trs=TABLE_HOLLINS_TRS))

        cur.execute("""
            DROP TABLE IF EXISTS {table_hollins_trs};
            CREATE TABLE {table_hollins_trs} (
              id serial PRIMARY KEY,
              map_id integer,
              township text,
              range text,
              section text
            );
        """.format(table_hollins_trs=TABLE_HOLLINS_TRS))

        tree = ET.parse(join(UPDATE_DIR, 'trs.xml'))
        root = tree.getroot()
        rows = []
        keys = ('ID', 'TOWNSHIP', 'RANGE', 'SECTION')
        for rec in root:
            rows.append(list(rec.find(k).text for k in keys))

        cur.executemany("""
            INSERT INTO {table_hollins_trs} (map_id, township, range, section)
            VALUES ( %s, %s, %s, %s );
        """.format(table_hollins_trs=TABLE_HOLLINS_TRS), rows)
        con.commit()

        print('INSERT: ' + str(cur.rowcount) + ' rows effected.')

        # Vacuum up dead tuples from the update
        tables = (
            TABLE_HOLLINS_MAP,
            TABLE_HOLLINS_MAP_QQ,
            TABLE_HOLLINS_SUBSECTION_LIST,
            TABLE_HOLLINS_SURVEYOR,
            TABLE_HOLLINS_TRS
        )

        # Vacuum must run outside of a transaction
        con.autocommit = True
        for t in tables:
            cur.execute('VACUUM FREEZE ' + t)


if __name__ == '__main__':

    print('\nLoading tables ... ')
    startTime = time.time()

    load_update()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
