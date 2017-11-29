
import boto3
import os
import os.path as path
from zipfile import ZipFile
from PIL import Image
import time
import re

from const import *

# Unpack images from the zip update into S3
def unpack_images():

    # First read the zipfile into local temporary storage
    s3 = boto3.resource('s3')
    obj = s3.Object(S3_BUCKET_UPDATE, UPDATE_ZIPFILE)
    obj.download_file(path.join(TMP_DIR, UPDATE_ZIPFILE))

    namelist = []
    with ZipFile(path.join(TMP_DIR, UPDATE_ZIPFILE)) as zip:
        for name in zip.namelist():
            if name.endswith('.tif'):
                namelist.append(name)

        zip.extractall(path=TMP_DIR, members=namelist)

    # Turn of warnings about oversize image files
    Image.warnings.simplefilter('ignore', Image.DecompressionBombWarning)

    for name in namelist:

        map = re.match('.*/(.*)\.', name).group(1).lower()
        book, maptype = re.match('(\d+)(\D+)', map).groups()
        dest_dir = path.join(TMP_DIR, 'scan', maptype, book)
        if not path.isdir(dest_dir):
            os.makedirs(dest_dir)

        with Image.open(path.join(TMP_DIR, name)) as img:
            frame = 0
            while True:
                try:
                    img.seek(frame)
                    img_name = '%s-%03d.tif' % (map, frame + 1)
                    dest = path.join(dest_dir, img_name)
                    img.save(dest)

                except EOFError:
                    break

                frame += 1

        print('%s: %d frame%s' % (name, frame, 's' if frame > 1 else ''))


















if __name__ == '__main__':

    print('\nUpdating map images ... ')
    startTime = time.time()

    unpack_images()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
