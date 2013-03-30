import io

from boto.s3.connection import S3Connection

from django.conf import settings

from datatap.loading import register_datatap
from datatap.datataps.streams import StreamDataTap


class S3Upload(io.BytesIO):
    def __init__(self, bucket_key):
        self.bucket_key = bucket_key
        super(S3Upload, self).__init__()
    
    def close(self):
        self.bucket_key.set_contents_from_stream(self)

class S3BucketDataTap(StreamDataTap):
    '''
    A stream data tap that stores to an S3 Bucket
    
    S3BucketDT(JSONDT(ModelDT)).send(key_name) => write to key name
    S3BucketDT(key_name) => bytes stream
    
    S3BucketDT(ZipDT(ModelDT)).send(key_name) => write a zip archive to key name
    ModelDT(ZipDt(S3BucketDT(key_name))) => load models from zip archive at key name
    '''
    def __init__(self, instream=None, key_name=None, aws_access_key_id=None, aws_secret_access_key=None, bucket_name=None, **kwargs):
        if aws_access_key_id is None:
            aws_access_key_id = settings.AWS_ACCESS_KEY_ID
        if aws_secret_access_key is None:
            aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY
        if bucket_name is None:
            bucket_name = settings.AWS_STORAGE_BUCKET_NAME
        self.connection = S3Connection(aws_access_key_id, aws_secret_access_key)
        self.bucket = self.connection.get_bucket(bucket_name)
        if key_name: 
            #CONSIDER: without a key name or instream we are a primitive serializer acting much like a tarfile and assets in a dir
            #would require paramater: key_directory
            assert instream is None, 'You cannot read from two sources, use .send(key_name) if you wish to write'
            key = self.bucket.get_key(key_name)
            instream = io.BytesIO()
            key.get_contents_to_file(instream)
            instream = instream
        super(S3BucketDataTap, self).__init__(instream, **kwargs)
    
    def send(self, key_name):
        key = self.bucket.new_key(key_name)
        fileobj = S3Upload(key)
        return super(S3BucketDataTap, self).send(fileobj)

register_datatap('S3Bucket', S3BucketDataTap)
