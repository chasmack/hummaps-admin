from datetime import datetime
import re
import time
import requests
from lxml import html

from openpyxl import Workbook

# 'https://hummaps.com/maps/?page=1&ds=2018-01-01&de=2018-12-31'

URL_BASE = 'https://hummaps.com/maps/'
XLSX_FILE = r'd:\Projects\Python\hummaps-admin\scrape\scrapings.xlsx'
XLSX_COLS = ('MAPTYPE', 'BOOK', 'PAGE', 'NPAGES', 'SURVEYORS', 'RECDATE', 'CLIENT', 'DESC', 'PDF', 'TRS', 'NOTE')

def scrape_maps(start, end, xlsx_file):

    wb = Workbook()
    ws = wb.active
    ws.append(XLSX_COLS)

    params = {'ds': start.strftime('%Y-%m-%d'), 'de': end.strftime('%Y-%m-%d')}
    params['page'] = 1

    nrecs = 0
    while True:
        r = requests.get(URL_BASE, params=params)
        if r.status_code != 200:
            print('Done.')
            break

        print(r.url)
        doc = html.fromstring(r.content)
        for map in doc.find_class('hmps-map'):
            maprec = []

            bookpage = map.find('./div/h4').text
            m = re.match('(\d+)\s+(.*)\s+(\d+)(?:\D(\d+))?', bookpage)
            if not m:
                print('BDA BOOK/PAGE: %s' % bookpage)
                continue
            book, maptype, page, lastpage = m.groups()
            book = int(book)
            page = int(page)
            if lastpage is None:
                npages = 1
            else:
                npages = int(lastpage) - page + 1
            maprec += (maptype, book, page, npages)

            elems = map.findall('./div/p')
            e = elems.pop(0).find('span')
            if e is None:
                maprec.append('')
            else:
                maprec.append(re.sub('.+?: ', '', e.text))
            maprec += list(re.sub('.+?: ', '', e.text) for e in elems)

            maprec += [map.find('.//a[@role="button"]').attrib['href']]

            ws.append(maprec)
            nrecs += 1

        params['page'] += 1

    wb.save(filename=xlsx_file)

    return nrecs

if __name__ == '__main__':

    print('\nScraping maps ... ')
    startTime = time.time()

    nrecs = scrape_maps(datetime(2018, 1, 1), datetime(2018, 12, 31), XLSX_FILE)

    endTime = time.time()
    print('Found {0:d} maps in {1:.3f} sec.'.format(nrecs, endTime - startTime))


