import psycopg2
import requests
from openpyxl import Workbook, load_workbook
import time
import re

XLSX_FILE = r'd:\Projects\Python\hummaps-admin\batch\test.xlsx'

PG_HOST = 'p3'
PG_USER_ADMIN = 'pgadmin'
PG_DATABASE_PROD = 'production'

# IPV4 connection
DSN_PROD = 'dbname={database} user={user} host={host}'.format(
    database=PG_DATABASE_PROD,
    user=PG_USER_ADMIN,
    host=PG_HOST
)

SCHEMA_STAGING = 'hummaps_staging'

TABLE_STAGING_HOLLINS_MAP = SCHEMA_STAGING + '.' + 'hollins_map'
TABLE_STAGING_HOLLINS_MAP_QQ = SCHEMA_STAGING + '.' + 'hollins_map_qq'
TABLE_STAGING_HOLLINS_SUBSECTION_LIST = SCHEMA_STAGING + '.' + 'hollins_subsection_list'
TABLE_STAGING_HOLLINS_SURVEYOR = SCHEMA_STAGING + '.' + 'hollins_surveyor'
TABLE_STAGING_HOLLINS_TRS = SCHEMA_STAGING + '.' + 'hollins_trs'
TABLE_STAGING_HOLLINS_FULLNAME = SCHEMA_STAGING + '.' + 'hollins_fullname'

TABLE_STAGING_CC = SCHEMA_STAGING + '.' + 'cc'
TABLE_STAGING_CC_IMAGE = SCHEMA_STAGING + '.' + 'cc_image'
TABLE_STAGING_MAP = SCHEMA_STAGING + '.' + 'map'
TABLE_STAGING_MAP_IMAGE = SCHEMA_STAGING + '.' + 'map_image'
TABLE_STAGING_MAPTYPE = SCHEMA_STAGING + '.' + 'maptype'
TABLE_STAGING_PDF = SCHEMA_STAGING + '.' + 'pdf'
TABLE_STAGING_SCAN = SCHEMA_STAGING + '.' + 'scan'
TABLE_STAGING_SIGNED_BY = SCHEMA_STAGING + '.' + 'signed_by'
TABLE_STAGING_SOURCE = SCHEMA_STAGING + '.' + 'source'
TABLE_STAGING_SUBSEC_NAMES = SCHEMA_STAGING + '.' + 'subsec_names'
TABLE_STAGING_SURVEYOR = SCHEMA_STAGING + '.' + 'surveyor'
TABLE_STAGING_TRS = SCHEMA_STAGING + '.' + 'trs'
TABLE_STAGING_TRS_PATH = SCHEMA_STAGING + '.' + 'trs_path'

SEQUENCE_MAP_ID = SCHEMA_STAGING + '.' + 'map_id_seq'

FUNCTION_MAP_ID = SCHEMA_STAGING + '.' + 'map_id'
FUNCTION_MAP_NAME = SCHEMA_STAGING + '.' + 'map_name'
FUNCTION_TOWNSHIP_NUMBER = SCHEMA_STAGING + '.' + 'township_number'
FUNCTION_TOWNSHIP_STR = SCHEMA_STAGING + '.' + 'township_str'
FUNCTION_RANGE_NUMBER = SCHEMA_STAGING + '.' + 'range_number'
FUNCTION_RANGE_STR = SCHEMA_STAGING + '.' + 'range_str'
FUNCTION_SUBSEC_BITS = SCHEMA_STAGING + '.' + 'subsec_bits'
FUNCTION_SUBSEC_STR = SCHEMA_STAGING + '.' + 'subsec_str'
FUNCTION_HOLLINS_SUBSEC = SCHEMA_STAGING + '.' + 'hollins_subsec'

SCHEMA_PROD = 'hummaps'

TABLE_PROD_CC = SCHEMA_PROD + '.' + 'cc'
TABLE_PROD_CC_IMAGE = SCHEMA_PROD + '.' + 'cc_image'
TABLE_PROD_MAP = SCHEMA_PROD + '.' + 'map'
TABLE_PROD_MAP_IMAGE = SCHEMA_PROD + '.' + 'map_image'
TABLE_PROD_MAPTYPE = SCHEMA_PROD + '.' + 'maptype'
TABLE_PROD_PDF = SCHEMA_PROD + '.' + 'pdf'
TABLE_PROD_SCAN = SCHEMA_PROD + '.' + 'scan'
TABLE_PROD_SIGNED_BY = SCHEMA_PROD + '.' + 'signed_by'
TABLE_PROD_SOURCE = SCHEMA_PROD + '.' + 'source'
TABLE_PROD_SURVEYOR = SCHEMA_PROD + '.' + 'surveyor'
TABLE_PROD_TRS = SCHEMA_PROD + '.' + 'trs'
TABLE_PROD_TRS_PATH = SCHEMA_PROD + '.' + 'trs_path'

TOWNSHIP_NORTH_MAX = 15
TOWNSHIP_SOUTH_MAX = 5
RANGE_EAST_MAX = 8
RANGE_WEST_MAX = 3

def validate_path(path):
    m = re.fullmatch('(\d{1,2}N|\d{1}S)\.(\d{1}[EW])\.(\d{1,2})(?:\.[A-P])?', path)
    if m is None:
        return False
    tshp, rng, sec = m.groups()
    if tshp[-1] == 'N' and not (1 <= int(tshp[:-1]) <= TOWNSHIP_NORTH_MAX):
        return False
    if tshp[-1] == 'S' and not 1 <= int(tshp[:-1]) <= TOWNSHIP_SOUTH_MAX:
        return False
    if rng[-1] == 'E' and not 1 <= int(rng[:-1]) <= RANGE_EAST_MAX:
        return False
    if rng[-1] == 'W' and not 1 <= int(rng[:-1]) <= RANGE_WEST_MAX:
        return False
    if not 1 <= int(sec) <= 36:
        return False

    return True


# A path spec represents one or more trs paths.
# The leaf node of a path spec can be a single lable or
# a regular expression style character class.
#
# 4N.1W.[1,2,3,11,12]
# 4N.1W.[1-3,11,12]
# 4N.1W.11.[A,B,C,D]
# 4N.1W.11.[A-F,I,J]
#
# Subsections also be listed individually without brackets
# or separators.
#
# 4N.1W.11.ABCDEFIJ
#
# No space is permitted anywhere within an individual path spec.
# Multiple individual path specs can be joined with space.
#
# 4N.1W.1.ABCD 5N.1W.36.MNOP
#

def expand_paths(path_spec):
    paths = []
    for path_spec in path_spec.split():

        # Check for a simple path
        if validate_path(path_spec):
            paths.append(path_spec)
            continue

        # Check for a list of sections
        m = re.fullmatch('(\d+[NS]\.\d+[EW]\.)\[((?:(?:\d+-)?\d+,)*(?:(?:\d+-)?\d+))\]', path_spec)
        if m:
            root = m.group(1)
            secs = []
            for sec in m.group(2).split(','):
                if not '-' in sec:
                    secs.append(sec)
                else:
                    low, high = map(int, sec.split('-'))
                    if high < low:
                        low, high = high, low
                    secs += map(str, range(low, high + 1))
            for path in (root + s for s in secs):
                if not validate_path(path):
                    raise ValueError('Bad path spec: ' + path_spec)
                paths.append(path)
            continue

        # Check for a list of subsections
        m = re.fullmatch('(\d+[NS]\.\d+[EW]\.\d+\.)\[((?:(?:[A-P]-)?[A-P],)*(?:(?:[A-P]-)?[A-P]))\]', path_spec)
        if m:
            root = m.group(1)
            subsecs = []
            for subsec in m.group(2).split(','):
                if not '-' in subsec:
                    subsecs.append(subsec)
                else:
                    low ,high = map(ord, subsec.split('-'))
                    if high < low:
                        low, high = high, low
                    subsecs += map(chr, range(low, high + 1))
            for path in (root + ss for ss in subsecs):
                if not validate_path(path):
                    raise ValueError('Bad path spec: ' + path_spec)
                paths.append(path)
            continue

        # Check for a simplified list of subsections
        m = re.fullmatch('(\d+[NS]\.\d+[EW]\.\d+\.)([A-P]+)', path_spec)
        if m:
            root = m.group(1)
            for path in (root + ss for ss in list(m.group(2))):
                if not validate_path(path):
                    raise ValueError('Bad path spec: ' + path_spec)
                paths.append(path)
            continue

        # No match
        raise ValueError('Bad path spec: ' + path_spec)

    return paths


if __name__ == '__main__':

    for path in expand_paths('7N.5E.12 7N.4E.[1,2,4-8,15] 4N.1W.7.[A,B,C-G,H,I,J,K-P] 4N.1E.3.KLOP'):
        print(path)

    assert validate_path('7N.3E.0') is False
    assert validate_path('7N.3E.1')
    assert validate_path('7N.3E.36')
    assert validate_path('7N.3E.37') is False
    assert validate_path('7N.3E.1.A')
    assert validate_path('7N.3E.1.P')
    assert validate_path('7N.3E.1.Q') is False
    assert validate_path('0N.3E.1.A') is False
    assert validate_path('1N.3E.1.A')
    assert validate_path('15N.3E.1.A')
    assert validate_path('16N.3E.1.A') is False
    assert validate_path('0S.3E.1.A') is False
    assert validate_path('1S.3E.1.A')
    assert validate_path('5S.3E.1.A')
    assert validate_path('6S.3E.1.A') is False
    assert validate_path('1N.0E.1.A') is False
    assert validate_path('1N.1E.1.A')
    assert validate_path('1N.8E.1.A')
    assert validate_path('1N.9E.1.A') is False
    assert validate_path('1N.0W.1.A') is False
    assert validate_path('1N.1W.1.A')
    assert validate_path('1N.3W.1.A')
    assert validate_path('1N.4W.1.A') is False