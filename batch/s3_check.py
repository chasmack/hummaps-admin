import boto3
import time


S3_BUCKET_MAPS = 'maps.hummaps.com'
MAPS_LIST = r'd:\Projects\Python\hummaps-admin\batch\imagefiles.txt'

def s3_imagefiles():

    # Get a list of imagefiles from the maps bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET_MAPS)

    with open(MAPS_LIST, 'w') as f:
        for maptype in ('cc', 'cr', 'hm', 'mm', 'pm', 'rm', 'rs', 'ur'):

            imagefiles = list([obj.key] for obj in bucket.objects.filter(Prefix='map/%s/' % maptype))

            for img in imagefiles:
                f.write('/' + img[0] + '\n')

if __name__ == '__main__':

    print('\nGeting imagefiles ... ')
    startTime = time.time()

    s3_imagefiles()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))

