import io
import os
from optparse import Option, OptionParser

from boto.s3.connection import S3Connection

from django.conf import settings

from datatap.loading import register_datatap
from datatap.datataps.streams import StreamDataTap


class S3Upload(io.BytesIO):
    def __init__(self, bucket_key):
        self.bucket_key = bucket_key
        self._closed = False
        super(S3Upload, self).__init__()
    
    def __del__(self):
        self.close()
    
    def close(self):
        if not self._closed:
            #TODO dont convert to a string first
            self.seek(0)
            self.bucket_key.set_contents_from_string(self.read())
        self._closed = True
        

class S3DataTap(StreamDataTap):
    '''
    A stream data tap that stores to an S3 Bucket. Reads off django-storages for aws credentials.
    
    S3BucketDT(JSONDT(ModelDT)).send(key_name) => write to key name
    S3BucketDT(key_name) => bytes stream
    
    S3BucketDT(ZipDT(ModelDT)).send(key_name) => write a zip archive to key name
    ModelDT(ZipDt(S3BucketDT(key_name))) => load models from zip archive at key name
    '''
    def __init__(self, instream=None, key_name=None, aws_access_key_id=None, aws_secret_access_key=None, bucket_name=None, **kwargs):
        if aws_access_key_id is None:
            aws_access_key_id = getattr(settings, 'AWS_ACCESS_KEY_ID', None) or os.environ['AWS_ACCESS_KEY_ID']
        if aws_secret_access_key is None:
            aws_secret_access_key = getattr(settings, 'AWS_SECRET_ACCESS_KEY', None) or os.environ['AWS_SECRET_ACCESS_KEY']
        if bucket_name is None:
            bucket_name = getattr(settings, 'AWS_STORAGE_BUCKET_NAME', None) or os.environ['AWS_STORAGE_BUCKET_NAME']
        self.connection = S3Connection(aws_access_key_id, aws_secret_access_key)
        self.bucket = self.connection.get_bucket(bucket_name)
        if key_name: 
            #CONSIDER: without a key name or instream we are a primitive serializer acting much like a tarfile and assets in a dir
            #would require paramater: key_directory
            assert instream is None, 'You cannot read from two sources, use .send(key_name) if you wish to write'
            key = self.bucket.get_key(key_name)
            instream = io.BytesIO()
            key.get_contents_to_file(instream)
            instream.seek(0)
            instream = instream
        super(S3DataTap, self).__init__(instream, **kwargs)
    
    def send(self, key_name):
        key = self.bucket.new_key(key_name)
        fileobj = S3Upload(key)
        return super(S3DataTap, self).send(fileobj)
    
    command_option_list = [
        Option('--key-name', action='store', dest='key_name'),
        Option('--bucket', action='store', dest='bucket_name'),
        Option('--access-key-id', action='store', dest='aws_access_key_id'),
        Option('--secret-access-key', action='store', dest='aws_secret_access_key'),
    ]
    
    @classmethod
    def load_from_command_line(cls, arglist, instream=None):
        parser = OptionParser(option_list=cls.command_option_list)
        options, args = parser.parse_args(arglist)
        kwargs = options.__dict__
        kwargs['instream'] = instream
        if not kwargs.get('key_name') and args:
            kwargs['key_name'] = args.pop(0)
        return cls(**kwargs)
    
    @classmethod
    def load_from_command_line_for_write(cls, arglist, instream):
        '''
        Retuns an instantiated DataTap with the provided arguments from commandline
        '''
        parser = OptionParser(option_list=cls.command_option_list)
        options, args = parser.parse_args(arglist)
        kwargs = options.__dict__
        kwargs['instream'] = instream
        
        if args:
            target = args.pop(0)
        else:
            target = kwargs.pop('key_name')
        datatap = cls(*args, **kwargs)
        def commit(*a, **k):
            datatap.send(target)
        datatap.commit = commit
        return datatap

register_datatap('S3', S3DataTap)

