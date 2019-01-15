
PG_DATABASE = 'production'
PG_USER = 'ubuntu'
PG_HOST = 'hummaps.com'

# IPV4 connection
DSN_PROD = 'dbname={database} user={user} host={host}'.format(
    database=PG_DATABASE, user=PG_USER, host=PG_HOST
)

XLSX_SUBSECTION_LIST = 'hollins/subsection_list.xlsx'

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

SCHEMA_STAGING = 'hummaps_staging'

FUNCTION_TOWNSHIP_NUMBER = SCHEMA_STAGING + '.' + 'township_number'
FUNCTION_TOWNSHIP_STR = SCHEMA_STAGING + '.' + 'township_str'
FUNCTION_RANGE_NUMBER = SCHEMA_STAGING + '.' + 'range_number'
FUNCTION_RANGE_STR = SCHEMA_STAGING + '.' + 'range_str'
FUNCTION_SUBSEC_BITS = SCHEMA_STAGING + '.' + 'subsec_bits'
FUNCTION_SUBSEC_STR = SCHEMA_STAGING + '.' + 'subsec_str'
FUNCTION_HOLLINS_SUBSEC = SCHEMA_STAGING + '.' + 'hollins_subsec'
