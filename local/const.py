
SOURCE_DIR = r'C:\Temp\hummaps\staging\files'
TMP_DIR = r'C:\Temp\hummaps\staging\tmp'

CC_DIR = r'C:\Temp\hummaps\staging\cc'
MAP_DIR = r'C:\Temp\hummaps\staging\map'
PDF_DIR = r'C:\Temp\hummaps\staging\pdf'
SCAN_DIR = r'C:\Temp\hummaps\staging\scan'

MAP_ARCHIVE = r'T:\hummaps\map'
PDF_ARCHIVE = r'T:\hummaps\pdf'
SCAN_ARCHIVE = r'T:\hummaps\scan'

XLSX_DATA_CC = r'data\cc.xlsx'

S3_BUCKET_MAPS = 'static.hummaps.com'

MAP_DPI = 160
MAP_QUALITY = 75

MAPTYPES = dict([
    ("Corner Record", "CR"),
    ("Highway Map", "HM"),
    ("Monument Map", "MM"),
    ("Parcel Map", "PM"),
    ("Record Map", "RM"),
    ("Survey", "RS"),
    ("Unrecorded Map", "UR"),
])

MAGICK = r'C:\Program Files\ImageMagick-6.9.5-Q16\magick.exe'

PG_DATABASE = 'production'
PG_USER = 'ubuntu'
# PG_HOST = 'ec2.cmack.org'
PG_HOST = 'hummaps.com'

# IPV4 connection
PG_DSN = 'dbname={database} user={user} host={host}'.format(
    database=PG_DATABASE, user=PG_USER, host=PG_HOST
)

SCHEMA_STAGING = 'hummaps_staging'

TABLE_CC = SCHEMA_STAGING + '.' + 'cc'
TABLE_CC_IMAGE = SCHEMA_STAGING + '.' + 'cc_image'
TABLE_MAP = SCHEMA_STAGING + '.' + 'map'
TABLE_MAP_IMAGE = SCHEMA_STAGING + '.' + 'map_image'
TABLE_MAPTYPE = SCHEMA_STAGING + '.' + 'maptype'
TABLE_PDF = SCHEMA_STAGING + '.' + 'pdf'
TABLE_SCAN = SCHEMA_STAGING + '.' + 'scan'
TABLE_SIGNED_BY = SCHEMA_STAGING + '.' + 'signed_by'
TABLE_SOURCE = SCHEMA_STAGING + '.' + 'source'
TABLE_SUBSECTION_NAMES = SCHEMA_STAGING + '.' + 'subsection_names'
TABLE_SURVEYOR = SCHEMA_STAGING + '.' + 'surveyor'
TABLE_TRS = SCHEMA_STAGING + '.' + 'trs'

FUNC_MAP_NAME = SCHEMA_STAGING + '.' + 'map_name'
