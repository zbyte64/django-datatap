'''
ModelStoreDT(JSONEncodeDT(FileDT(filename))) => iterable, .commit()
FileDT(JSONDecodeDT(ModelSourceDT), filename)

S3DT(ZipFileDT(ModelDT))

TarDT(S3DT).autoload() => ModelDT(TarDT(S3DT))

STDOUT(JSONDT(ModelDT)) => StreamDT(JSONDT(ModelDT), sys.stdout)
FileDT(JSONDT(ModelDT), 'myfile.json') => StreamDT(JSONDT(ModelDT), open('myfile.json', r'))

'''

import sys
from optparse import OptionParser

from datatap.encoders import DataTapJSONEncoder, DataTapJSONDecoder
from datatap.loading import register_datatap
from datatap.datataps.base import DataTap


class StreamDataTap(DataTap):
    inner_domain = 'text'
    outer_domain = 'text'
    
    def __init__(self, outstream=sys.stdout, **kwargs):
        self.outstream = outstream
        super(StreamDataTap, self).__init__(**kwargs)
    
    def write(self, chunk):
        self.outstream.write(chunk)

class FileDataTap(StreamDataTap):
    def __init__(self, outstream=None, instream=None):
        '''
        :param outstream: A filename or file like object
        :param instream: A filename or file like object
        '''
        if isinstance(outstream, basestring):
            outstream = open(outstream, 'w')
        if isinstance(instream, basestring):
            instream = open(instream, 'r')
        super(FileDataTap, self).__init__(outstream=outstream, instream=instream)

class JSONDecodeDataTap(DataTap):
    '''
    Decodes JSON objects from a text domain
    '''
    inner_domain = 'text'
    outer_domain = 'primitive'
    
    def get_raw_item_stream(self, filetap=None):
        '''
        Returns json decoded objects from the stream
        
        :param filetap: The filetap to load files from
        '''
        decoder = DataTapJSONDecoder(filetap=filetap)
        return decoder.decode(self.instream.read())

class JSONEncodeDataTap(DataTap):
    inner_domain = 'primitive'
    outer_domain = 'text'
    
    def get_raw_item_stream(self, filetap=None):
        encoder = DataTapJSONEncoder(filetap=filetap)
        return encoder.iterencode(self.instream)

