import psycopg2
from openpyxl import load_workbook
from datetime import datetime
import xml.etree.ElementTree as etree
import xml.dom.minidom as minidom

from hollins.const import *

#
# export_xml - Create an XML file from Hummaps data for import into Hollins tables.mdb
#

def export_xml(xml_file):

    time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    attrib = {
        'generated': time,
        'xmlns:od': 'urn:schemas-microsoft-com:officedata'
    }
    root = etree.Element('dataroot', attrib=attrib)

    with psycopg2.connect(DSN_PROD) as con, con.cursor() as cur:

        # Extract the TRS records.
        cur.execute("""
            SELECT map_id,
              lpad({function_township_str}(tshp), 3, '0') tshp,
              {function_range_str}(rng) rng,
              ',' || string_agg(sec::text, ',') || ',' secs
            FROM (
              SELECT DISTINCT map_id, tshp, rng, sec
              FROM {table_trs}
              WHERE source_id = 0
              ORDER BY map_id, tshp, rng, sec
            ) q1
            GROUP BY map_id, tshp, rng
            ORDER BY map_id;
        """.format(
            table_trs=TABLE_PROD_TRS,
            function_township_str=FUNCTION_TOWNSHIP_STR,
            function_range_str=FUNCTION_RANGE_STR)
        )

        nrecs = 0
        for row in cur:
            map_id, tshp, rng, secs = row
            elem = etree.SubElement(root, 'TRS')
            etree.SubElement(elem, 'ID').text = str(map_id)
            # Unknown township/range appear as None
            etree.SubElement(elem, 'TOWNSHIP').text = tshp if tshp else '0'
            etree.SubElement(elem, 'RANGE').text = rng if rng else '0'
            etree.SubElement(elem, 'SECTION').text = secs
            nrecs += 1

        print('Created %d TRS records.' % nrecs)

        # Extract the MAP records.
        cur.execute("""
            WITH q1 AS (
                SELECT m.id map_id,
                    s.firstname ||
                    coalesce(' ' || left(s.secondname, 1), '') ||
                    coalesce(' ' || left(s.thirdname, 1), '') ||
                    ' ' || s.lastname ||
                    coalesce(' ' || s.suffix, '') AS surveyor
                FROM {table_map} m
                LEFT JOIN {table_signed_by} sb ON sb.map_id = m.id
                LEFT JOIN {table_surveyor} s ON sb.surveyor_id = s.id
                ORDER BY m.id, s.lastname, s.firstname
            ), q2 AS (
                SELECT map_id,
                    coalesce(string_agg(q1.surveyor, ' & '), 'UNKNOWN') surveyors
                FROM q1
                GROUP BY map_id
            )
            SELECT m.id map_id, t.maptype, m.book, m.page firstpage, m.page + m.npages - 1 lastpage,
              m.recdate, q2.surveyors, m.client donefor, m.description descrip
            FROM {table_map} m
            JOIN {table_maptype} t ON t.id = m.maptype_id
            JOIN q2 ON q2.map_id = m.id
            ORDER BY map_id;
        """.format(
            table_map=TABLE_PROD_MAP,
            table_maptype=TABLE_PROD_MAPTYPE,
            table_signed_by=TABLE_PROD_SIGNED_BY,
            table_surveyor=TABLE_PROD_SURVEYOR)
        )

        map_records = {}
        for row in cur:
            map_id, maptype, book, firstpage, lastpage, recdate, surveyors, donefor, descrip = row

            elem = etree.Element('map')
            etree.SubElement(elem, 'ID').text = str(map_id)
            etree.SubElement(elem, 'maptype').text = maptype
            etree.SubElement(elem, 'BOOK').text = str(book)
            etree.SubElement(elem, 'FIRSTPAGE').text = str(firstpage)
            etree.SubElement(elem, 'LASTPAGE').text = str(lastpage)
            if recdate is not None:
                etree.SubElement(elem, 'RECDATE').text = recdate.strftime('%Y-%m-%dT00:00:00')
            etree.SubElement(elem, 'SURVEYOR').text = surveyors
            if donefor is not None:
                etree.SubElement(elem, 'DONEFOR').text = donefor
            if descrip is not None:
                etree.SubElement(elem, 'DESCRIP').text = descrip

            map_records[map_id] = elem

        # Read the subsection list
        ws = load_workbook(filename=XLSX_SUBSECTION_LIST, read_only=True).active
        for subsec in ws.iter_rows():

            # Add subsection data for one section at a time.
            subsec = subsec[0].value

            cur.execute("""
                SELECT m.id map_id, trs.subsec
                FROM {table_map} m
                JOIN {table_trs} trs ON trs.map_id = m.id
                WHERE trs.source_id = 1
                AND trs.tshp = {function_township_number}('{township}')
                AND trs.rng = {function_range_number}('{range}')
                AND trs.sec = {section};
            """.format(
                table_map=TABLE_PROD_MAP,
                table_trs=TABLE_PROD_TRS,
                function_township_number=FUNCTION_TOWNSHIP_NUMBER,
                function_range_number=FUNCTION_RANGE_NUMBER,
                township=subsec[0:3],
                range=subsec[3:5],
                section=int(subsec[5:7]))
            )

            # Relate Hummaps subsection bit positions (lsb -> msb) to Hollins subsection numbers
            hollins_subsecs =  [4, 3, 2, 1, 5, 6, 7, 8, 12, 11, 10, 9, 13, 14, 15, 16]

            for map_id, subsec_bits in cur:
                subsec_list = []
                for i in range(16):
                    if (1 << i) & subsec_bits > 0:
                        subsec_list.append(hollins_subsecs[i])
                subsec_list = ','.join((str(n) for n in sorted(subsec_list)))
                elem = map_records[map_id]

                if subsec[0].isdigit():
                    # Can't start an element label with a digit.
                    subsec = '_x%04x_%s' % (ord(subsec[0]), subsec[1:])

                etree.SubElement(elem, subsec).text = ',%s,' % subsec_list

        nrecs = 0
        for id in sorted(map_records.keys()):
            root.append(map_records[id])
            nrecs += 1

        print('Created %d MAP records.' % nrecs)

        # Extract the SURVEYOR lastnames.
        cur.execute("""

            SELECT
                firstname ||
                coalesce(' ' || left(secondname, 1), '') ||
                coalesce(' ' || left(thirdname, 1), '') ||
                ' ' || lastname ||
                coalesce(' ' || suffix, '') AS surveyor,
                lastname
            FROM {table_surveyor}
            ORDER BY lastname, firstname;
        """.format(
            table_surveyor=TABLE_PROD_SURVEYOR)
        )

        nrecs = 0
        for surveyor, lastname in cur:
            elem = etree.SubElement(root, 'Surveyor')
            etree.SubElement(elem, 'Surveyor').text = surveyor
            etree.SubElement(elem, 'lastname').text = lastname
            nrecs += 1

        print('Created %d SURVEYOR records.' % nrecs)

    # with open(xml_file, 'wb') as f:
    #     etree.ElementTree(root).write(f)

    # Reparse the etree xml with minidom and write pretty xml.
    dom = minidom.parseString(etree.tostring(root, encoding='utf-8'))
    with open(xml_file, 'wb') as f:
        f.write(dom.toprettyxml(indent='', encoding='utf-8'))

    return


def check_trs(xml_file, check_file):

    trs = {}
    nrecs = 0
    root = etree.parse(xml_file).getroot()
    for elem in root.findall('TRS'):
        map_id = elem.find('ID').text
        tshp = elem.find('TOWNSHIP').text
        rng = elem.find('RANGE').text
        secs = elem.find('SECTION').text
        key = '_'.join((map_id, tshp, rng))
        trs[key] = secs
        nrecs += 1

    print('Found %d records in %s.' % (nrecs, xml_file))

    nrecs = 0
    root = etree.parse(check_file).getroot()
    for elem in root.findall('TRS'):
        map_id = elem.find('ID').text
        tshp = elem.find('TOWNSHIP').text
        rng = elem.find('RANGE').text
        secs = elem.find('SECTION').text
        key = '_'.join((map_id, tshp, rng))
        if key not in trs:
            print('Missing record: map_id=%s tshp=%s rng=%s' % (map_id, tshp, rng))
        elif trs[key] != secs:
            print('Bad record: map_id=%s tshp=%s rng=%s expected=%s found=%s' % (map_id, tshp, rng, trs[key], secs))
        trs['_'.join((map_id, tshp, rng))]
        nrecs += 1

    print('Compared %d records in %s.' % (nrecs, check_file))


if __name__ == '__main__':

    export_xml('hollins/hollins.xml')
    check_trs('hollins/hollins.xml', 'hollins/update64_trs.xml')
