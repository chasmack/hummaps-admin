
# const.py - common constants

UPDATE_DIR = 'data/update59'
UPDATE_ZIPFILE = 'update59_171024.zip'

XLSX_DATA_SURVEYOR = 'data/surveyor.xlsx'
XLSX_DATA_CC = 'data/cc.xlsx'

S3_BUCKET_MAPS = 'maps.hummaps.com'
S3_BUCKET_UPDATE = 'update.hummaps.com'

TMP_DIR = '/home/ubuntu/tmp'

PG_DATABASE = 'production'
PG_USER = 'ubuntu'
PG_HOST = 'localhost'
PG_PASSWORD = 'pg'

# IPV4 connection
# PG_DSN = 'dbname={database} user={user} host={host} password={password}'.format(
#     database=PG_DATABASE, user=PG_USER, host=PG_HOST, password=PG_PASSWORD
# )

# UNIX domain socket connection
PG_DSN = 'dbname={database} user={user}'.format(
    database=PG_DATABASE, user=PG_USER
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
TABLE_SUBSECTION_NAMES = SCHEMA_STAGING + '.' + 'subsection_names'
TABLE_SIGNED_BY = SCHEMA_STAGING + '.' + 'signed_by'
TABLE_SOURCE = SCHEMA_STAGING + '.' + 'source'
TABLE_SURVEYOR = SCHEMA_STAGING + '.' + 'surveyor'
TABLE_TRS = SCHEMA_STAGING + '.' + 'trs'

USER_ADMIN = 'ubuntu'
USER_PRODUCTION = 'hummaps'

MAPTYPES = (
    ("Corner Record", "CR"),
    ("Highway Map", "HM"),
    ("Monument Map", "MM"),
    ("Parcel Map", "PM"),
    ("Record Map", "RM"),
    ("Survey", "RS"),
    ("Unrecorded Map", "UR"),
)
