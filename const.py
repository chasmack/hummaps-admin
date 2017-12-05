
# const.py - common constants

UPDATE_DIR = 'data/update60'

XLSX_DATA_SURVEYOR = 'data/surveyor.xlsx'
XLSX_DATA_CC = 'data/cc.xlsx'
XLSX_DATA_MAP = 'data/map.xlsx'

S3_BUCKET_MAPS = 'maps.hummaps.com'

MAPTYPES = (
    ("Corner Record", "CR"),
    ("Highway Map", "HM"),
    ("Monument Map", "MM"),
    ("Parcel Map", "PM"),
    ("Record Map", "RM"),
    ("Survey", "RS"),
    ("Unrecorded Map", "UR"),
)

# Source list for the trs records
TRS_SOURCE_HOLLINS_SECTION = 0
TRS_SOURCE_HOLLINS_SUBSECTION = 1
TRS_SOURCE_PARSED_SECTION = 2
TRS_SOURCE_PARSED_SUBSECTION = 3
TRS_SOURCE_XLSX_DATA = 4

USER_POSTGRES = 'postgres'
USER_ADMIN = 'ubuntu'
USER_PROD = 'hummaps'
PASSWORD_PROD = 'g712X$'

DATABASE_PROD = 'production'
DATABASE_POSTGRES = 'postgres'

SQL_INIT_DB = 'sql/init_db.sql'

# IPV4 connection
# DSN_PROD = 'dbname={database} user={user} host={host}'.format(
#     database=DATABASE_PROD,
#     user=USER_ADMIN,
#     host='localhost'
# )

# UNIX domain socket connection
DSN_PROD = 'dbname={database} user={user}'.format(
    database=DATABASE_PROD, user=USER_ADMIN
)

SCHEMA_STAGING = 'hummaps_staging'

TABLE_HOLLINS_MAP = SCHEMA_STAGING + '.' + 'hollins_map'
TABLE_HOLLINS_MAP_QQ = SCHEMA_STAGING + '.' + 'hollins_map_qq'
TABLE_HOLLINS_SUBSECTION_LIST = SCHEMA_STAGING + '.' + 'hollins_subsection_list'
TABLE_HOLLINS_SURVEYOR = SCHEMA_STAGING + '.' + 'hollins_surveyor'
TABLE_HOLLINS_TRS = SCHEMA_STAGING + '.' + 'hollins_trs'
TABLE_HOLLINS_FULLNAME = SCHEMA_STAGING + '.' + 'hollins_fullname'

TABLE_CC = SCHEMA_STAGING + '.' + 'cc'
TABLE_CC_IMAGE = SCHEMA_STAGING + '.' + 'cc_image'
TABLE_MAP = SCHEMA_STAGING + '.' + 'map'
TABLE_MAP_IMAGE = SCHEMA_STAGING + '.' + 'map_image'
TABLE_MAPTYPE = SCHEMA_STAGING + '.' + 'maptype'
TABLE_PDF = SCHEMA_STAGING + '.' + 'pdf'
TABLE_SCAN = SCHEMA_STAGING + '.' + 'scan'
TABLE_SIGNED_BY = SCHEMA_STAGING + '.' + 'signed_by'
TABLE_SOURCE = SCHEMA_STAGING + '.' + 'source'
TABLE_SUBSEC_NAMES = SCHEMA_STAGING + '.' + 'subsec_names'
TABLE_SURVEYOR = SCHEMA_STAGING + '.' + 'surveyor'
TABLE_TRS = SCHEMA_STAGING + '.' + 'trs'

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
