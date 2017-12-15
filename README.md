# hummaps-admin 

### Update tool for Hummaps

These tools combine data from a number of sources to rebuild 
the production hummaps datebase.
The production hummaps database resides in a schema named *hummaps* 
of a PostgreSQL database named *production*.
A second schema named *hummaps_staging* is used to store production 
and staging tables and sql helper functions before the production 
tables are copied to *hummaps*.

The structure of *hummaps* is described by the 
[Hummaps ER Diagram](https://github.com/chasmack/hummaps-admin/blob/master/docs/hummaps_ER.pdf).

Other tables and file names are given in the constants definition file
[const.py](https://github.com/chasmack/hummaps-admin/blob/master/const.py).

### Source Data

The MS Access database originally designed by Michael Hollins,
typically named *tables.mdb*, is converted into XML using MS Access.
These XML files are read into tables in *hummaps_staging*.

The Excel file *surveyors.xlsx* lists information on surveyors and provides
a link from the original Hollins surveyor name and hummaps *surveyor* record.
For historical reasons multiple Hollins surveyors can point to a 
single *surveyor* record. 
In that case all fields except HOLLINS_FULLNAME must be identical.

The Excel file *cc.xlsx* lists Certificate of Corrections and the map
to which they apply. The RECDATE field refers to the recording date
of the map being corrected and must agree with the map record.

The Excel file *map.xlsx* lists additional map records to be added.
The TRS field lists township-range-section records to be added to the
hummaps *trs* table. Multiple comma-separated TRS records can be listed.
Each TRS record is in regular hummaps subsection-section format.

The Excel file *pm.xlsx* lists parcel map numbers for the Parcel Maps.
This information is missing from the original Hollins map records.
If a parcel map is amended the parcel map number is reused and the
word AMENDED added.

The Excel file *tract.xlsx* lists tract numbers for Record Maps.
Not all Record Maps were assigned tract numbers.

### Update Workflow

Workflow is controlled by *update_hummaps.py*.

1. Stop the Apache web server, drop and then recreate the *production* database.
The production user's role and the staging schema are also created.

2. The XML data is read into tables. Hollin's unique structure for encoding
subsection information is split out into its own table.

3. The *map* table is loaded from the Hollins map records.
Records from the Excel map data are added. A *maptype* table is also
created from information in *const.py*.

TODO: Functionality could be added to allow replacement of Hollins map
records with data provided in the Excel map file. Currently if there is
already a map record in Hollins the Excel map data will add a second duplicate
record to *map*. 

4. SQL utility functions are created. This must be done after the *map* and *maptype*
tables have been created since some functions reference those tables.

5. The *surveyor* table is loaded from the Excel surveyor file.
The *signed_by* table is is also populated linking *map* and *surveyor*
in an many-to-many relationship.

6. The *map_image* table is populated linking *map* records to the 
JPEG map image stored in an Amazon S3 bucket.

7. The *cc* table is loaded from Excel data and the *cc_image* table
is populated linking *cc* records to the JPEG image files in S3 storage.

8. The *pdf* table is populated linking *map* records to PDF files in S3 storage.

9. The *scan* table is populated linking *map* records to the original TIFF scans
in S3 storage.

10. The *trs* table is loaded from the Hollins trs and subsection data. 
Additional *trs* records are added from the Excel map data. 
A *source* table is also created from data in *const.py*. 

TODO: Earlier versions of the procedures parsed section and subsection data from the
map descriptions greatly increasing the number of map records with subsection data.
That functionality has not yet been brought forward into this version of the update process.

11. The production schema is created and the the production tables copied from staging.

12. THe Apache web server is restarted. 

The update process runs from a command line on the 
EC2 Linux server hosting Hummaps -

$ cd /home/ubuntu/www/hummaps-staging
$ python3 update_hummaps.py

The processing takes about 5 minutes during which time the 
Hummaps web site is unavailable.

### Map Images

The update process described above assumes JPEG and TIFF map images and PDF files 
have been properly named and copied to the Amazon S3 bucket. The EC2 Linux server
does not have the resources necessary to handle image conversion. At this time 
this processing is done on a local Windows workstation and the resulting images
copied to S3 storage -

\> aws s3 cp --recursive --storage-class STANDARD_IA map pdf scan s3://bucket_name/

JPEG image files are 160 dpi 8-bit greyscale or 24-bit color.

PDF files are simply collections of the JPEG map images with any 
Certificate of Corrections appended to the end.

Scan images are the original resolution at 1-bit black and white or
24-bit color. Color map images are converted to 24-bit color. 
Original scan images for Certificates of Correction and Corner Records 
are not provided. 

Note that as part of the current update procedure map images are
provided as full resolution multi-page TIFF images. Single page 
TIFF images must be extracted for use by Hummaps.

