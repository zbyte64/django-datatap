import sys
from optparse import OptionParser

from datatap.encoders import DataTapJSONEncoder, DataTapJSONDecoder
from datatap.loading import register_datatap
from datatap.datataps.base import DataTap


class JSONStreamDataTap(DataTap):
    '''
    Reads and writes from a stream serialized with json
    '''
    def __init__(self, stream, **kwargs):
        self.stream = stream
        super(JSONStreamDataTap, self).__init__(**kwargs)
    
    def get_raw_item_stream(self, filetap=None):
        '''
        Returns json decoded objects from the stream
        '''
        decoder = DataTapJSONDecoder(filetap=filetap)
        return decoder.decode(self.stream.read())
    
    def write_stream(self, instream, filetap=None):
        '''
        Writes JSON encoded objects from instream to the stream
        '''
        encoder = DataTapJSONEncoder(filetap=filetap)
        for chunk in encoder.iterencode(instream):
            self.stream.write(chunk)
    
    def write_item(self, item, filetap=None):
        encoder = DataTapJSONEncoder(filetap=filetap)
        for chunk in encoder.iterencode(item):
            self.stream.write(chunk)
    
    @classmethod
    def load_from_command_line(cls, arglist):
        parser = OptionParser(option_list=cls.command_option_list)
        options, args = parser.parse_args(arglist)
        if len(args):
            #perhaps this should be subclassed to be a JSONFile
            options.__dict__['stream'] = open(args[0], 'r')
        else:
            options.__dict__['stream'] = sys.stdout
        return cls(**options.__dict__)

register_datatap('JSONStream', JSONStreamDataTap)
