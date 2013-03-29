import io

from boto.s3.connection import S3Connection

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
    
    S3BucketDT(JSONDT(ModelDT)).write(key_name) => write to key name
    S3BucketDT(key_name) => bytes stream
    
    S3BucketDT(ZipDT(ModelDT)).write(key_name) => write a zip archive to key name
    ModelDT(ZipDt(S3BucketDT(key_name))) => load models from zip archive at key name
    '''
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket_name, key_name=None, **kwargs):
        self.connection = S3Connection(aws_access_key_id, aws_secret_access_key)
        self.bucket = self.connection.get_bucket(bucket_name)
        if key_name:
            key = self.bucket.get_key(key_name)
            instream = io.BytesIO()
            key.get_contents_to_file(instream)
            kwargs['instream'] = instream
        super(S3BucketDataTap, self).__init__(**kwargs)
    
    def save(self, key_name):
        key = self.bucket.new_key(key_name)
        fileobj = S3Upload(key)
        return super(S3BucketDataTap, self).save(fileobj)

register_datatap('S3Bucket', S3BucketDataTap)
