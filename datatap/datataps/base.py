import logging
import sys
from optparse import OptionParser

try:
    from django.utils.encoding import force_text
except ImportError:
    from django.utils.encoding import force_unicode as force_text


class DataTap(object):
    def __init__(self, instream=None, filetap=None, domain=None):
        '''
        
        :param instream: A source for the datatap to read from
        :param filetap: A `FileTap` instance that handles reading and writing of assets
        :param domain: Force the datatap to export to this specified domain
        '''
        self.instream = instream
        self.filetap = filetap
        self._domain = domain
        super(DataTap, self).__init__()
        self.item_stream = self.get_item_stream()
    
    def get_domain(self):
        '''
        Detect the target domain typically based on the instream. 
        '''
        return None
    
    @classmethod
    def open(cls, *args, **kwargs):
        return cls(*args, **kwargs)
    
    @property
    def domain(self):
        '''
        Returns the target domain this data tap will be exporting to
        '''
        if self._domain:
            return self._domain
        return self.get_domain()
    
    def close(self):
        '''
        Closes the DataTap
        '''
        self.item_stream = None
    
    def read(self, size=None):
        '''
        Read a number of chunks read from this datatap.
        '''
        if not hasattr(self, 'read_iter'):
            self.read_iter = iter(self)
        result = list()
        if size is None:
            for item in self.read_iter:
                result.append(item)
        else:
            for i in range(size):
                result.append(self.read_iter.next())
        return result
    
    def write(self, chunk):
        raise NotImplementedError, 'This backend does not support writes'
    
    def __iter__(self):
        return iter(self.item_stream)
    
    def get_item_stream(self):
        '''
        Returns an iterable based on the instream. By default return the 
        iterable defined by the method `get_<domain>_stream` where 
        domain is the result of `get_domain`.
        '''
        return getattr(self, 'get_%s_stream' % self.domain)(self.instream)
    
    def send(self, datatap):
        '''
        Send this datatap stream to another datatap or stream
        '''
        for item in self:
            if not isinstance(item, basestring):
                item = force_text(item)
            datatap.write(item)
    
    def detect_originating_datatap(self):
        '''
        Returns the originating datatap class or None if it doesn't know.
        If this is stored datatap open for reading then it returns the datatap class that was used for populating this datatap
        '''
        return None
    
    def get_logger(self):
        '''
        Returns a logger for logging events
        '''
        return logging.getLogger(__name__)
    
    command_option_list = []
    
    @classmethod
    def load_from_command_line(cls, arglist, instream=None):
        '''
        Retuns an instantiated DataTap with the provided arguments from commandline
        '''
        parser = OptionParser(option_list=cls.command_option_list)
        options, args = parser.parse_args(arglist)
        kwargs = options.__dict__
        if instream is not None:
            kwargs['instream'] = instream
        return cls(*args, **kwargs)
    
    @classmethod
    def load_from_command_line_for_write(cls, arglist, instream):
        '''
        Retuns an instantiated DataTap with the provided arguments from commandline
        '''
        parser = OptionParser(option_list=cls.command_option_list)
        options, args = parser.parse_args(arglist)
        kwargs = options.__dict__
        kwargs['instream'] = instream
        
        #the default is to assume that args[0] is where we wish to save
        #of if we already have a commit function, use that instead
        if not hasattr(cls, 'commit'):
            if args:
                target = args.pop(0)
            else:
                target = sys.stdout
            datatap = cls(*args, **kwargs)
            def commit(*a, **k):
                datatap.send(target)
            datatap.commit = commit
        else:
            datatap = cls(*args, **kwargs)
        return datatap

class FileTap(object):
    '''
    An object that handles the storage and retrieval of serialized files
    '''
    def write_file(self, file_obj, path):
        '''
        Store a data file. 
        Returns the internal location of the path to be used for deserialization
        '''
        return path
    
    def read_file(self, storage_path, original_path):
        '''
        Returns a django file object belonging to the serialized path
        '''
        return None
    
