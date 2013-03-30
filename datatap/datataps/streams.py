import sys
import urllib2 #TODO requests
from io import BytesIO
from optparse import OptionParser

from datatap.encoders import DataTapJSONEncoder, DataTapJSONDecoder
from datatap.loading import register_datatap
from datatap.datataps.base import DataTap


class StreamDataTap(DataTap):
    '''
    A data tap that fowards the instream or saves the instream to a real stream
    '''
    def get_domain(self):
        return 'bytes' #bytes go in, bytes go out
    
    def get_bytes_stream(self, instream):
        return instream
    
    def read(self, *args, **kwargs):
        return self.item_stream.read(*args, **kwargs)
    
    def send(self, fileobj):
        #err?
        return self.instream.send(fileobj)
    
    def write(self, chunk):
        self.instream.write(chunk)

register_datatap('Stream', StreamDataTap)

class BufferedStreamDataTap(StreamDataTap):
    '''
    A stream data tap that uses io.BytesIO to buffer read operations.
    This is useful for processing streams from the internet into modules
    that require random position reads like with zipfile.
    '''
    def __init__(self, instream=None, **kwargs):
        if instream is not None:
            instream = BytesIO(instream)
        return super(BufferedStreamDataTap, self).__init__(instream=instream, **kwargs)
    
    def send(self, fileobj):
        fileobj = BytesIO(fileobj)
        return super(BufferedStreamDataTap, self).send(fileobj)

class FileDataTap(StreamDataTap):
    '''
    A stream data tap that opens files for io
    
    FileDT(JSONDT(ModelDT)).write(filename) => write to filename
    FileDT(filename=filename) => bytes stream
    '''
    def __init__(self, filename=None, **kwargs):
        '''
        :param filename: A filename
        '''
        if filename:
            kwargs['instream'] = open(filename, 'r')
        super(FileDataTap, self).__init__(**kwargs)
    
    def send(self, filename):
        if isinstance(filename, basestring):
            fileobj = open(filename, 'w')
        else:
            fileobj = filename
        return super(FileDataTap, self).send(fileobj)

register_datatap('File', FileDataTap)

class URLDataTap(BufferedStreamDataTap):
    '''
    A stream data tap that opens a url for io
    
    URLDT(JSONDT(ModelDT)).write(url) => big post
    URLDT(url=url) => bytes stream
    '''
    def __init__(self, url=None, **kwargs):
        '''
        :param filename: A filename
        '''
        if url:
            kwargs['instream'] = urllib2.open(url)
        super(URLDataTap, self).__init__(**kwargs)
    
    def send(self, url):
        #TODO i think requests might make this easier and better
        fileobj = requests.open(url, 'w')
        return super(URLDataTap, self).send(fileobj)

register_datatap('URL', URLDataTap)

class JSONDataTap(DataTap):
    '''
    A data tap that converts primitive objects to and from a text datatap
    
    JSONDT(ModelDT) => json representation of models
    JSONDT(FileDT(filename)) => decoded json from filename
    '''
    def get_domain(self):
        if self.instream.domain == 'primitive':
            return 'bytes'
        if self.instream.domain == 'bytes':
            return 'primitive'
    
    def get_bytes_stream(self, instream):
        encoder = DataTapJSONEncoder(filetap=self.filetap)
        return encoder.iterencode(iter(instream))
    
    def get_primitive_stream(self, instream):
        decoder = DataTapJSONDecoder(filetap=self.filetap)
        return decoder.decode(instream.read())

register_datatap('JSON', JSONDataTap)
