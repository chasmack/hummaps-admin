import boto3
import time

def s3_check():

    S3_BUCKET_MAPS = 'maps.hummaps.com'

    # Get a list of cc images from the maps bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET_MAPS)

    # Put each imagefile in a length = 1 list to be later extended
    imagefiles = list([obj.key] for obj in bucket.objects.filter(Prefix='map/cc/'))

    for f in imagefiles:
        print(f[0])

if __name__ == '__main__':

    print('\nGeting CC images ... ')
    startTime = time.time()

    s3_check()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))

