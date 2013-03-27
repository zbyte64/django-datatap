import logging

from optparse import OptionParser

from datatap.encoders import ObjectIteratorAdaptor


class DataTap(object):
    def __init__(self, mode=None, for_datatap=None):
        self.mode = mode
        if mode:
            self.open(mode, for_datatap)
        super(DataTap, self).__init__()
    
    def __del__(self):
        if self.mode is not None:
            self.close()
    
    def open(self, mode='r', for_datatap=None):
        '''
        Open the DataTap for data operations.
        
        :param mode: r for read, w for write
        :param for_datatap: The datatap we are opening for. If this is a read then attempt to check if it is compatible with the originating datap. If this is a write then store the datatap type.
        '''
        self.mode = mode
    
    def close(self):
        '''
        Closes the DataTap
        '''
        self.mode = None
    
    @classmethod
    def store(cls, datatap, *args, **kwargs):
        '''
        Writes objects to the datatap from this data tap class
        Passes args and kwargs to instantiate the source data tap
        
        :param datatap: The datatap to write to
        '''
        source = cls(*args, **kwargs)
        source.open(mode='r', for_datatap=datatap)
        return source.dump(datatap)
    
    def dump(self, datatap):
        '''
        Writes objects form this datatap to the datatap passed in
        
        :param datatap: The datatap to write to
        '''
        return datatap.write_stream(self.get_item_stream(filetap=datatap.get_filetap()))
    
    @classmethod
    def load(cls, datatap, *args, **kwargs):
        '''
        Loads objects from the datatap to this data tap class
        Passes args and kwargs to instantiate the destination data tap
        
        :param datatap: The datatap to load from
        '''
        destination = cls(*args, **kwargs)
        destination.open(mode='r', for_datatap=datatap)
        response = destination.write(datatap)
        return response
    
    def write(self, datatap):
        '''
        Writes objects from the passed in datatap to this datatap
        
        :param datatap: The datatap to read from
        '''
        result = self.write_stream(datatap.get_item_stream(filetap=self.get_filetap()))
        return result
    
    def get_item_stream(self, filetap=None):
        '''
        Returns an iterable of standardized objects belonging to this data tap
        
        :param filetap: The datatap to use to store files
        '''
        object_iterator = self.get_raw_item_stream(filetap)
        return self.get_object_iterator_adaptor(object_iterator=object_iterator)
    
    def get_filetap(self):
        '''
        Returns a datatap to store the files
        '''
        return self
    
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
    
    def get_object_iterator_class(self):
        return ObjectIteratorAdaptor
    
    def get_object_iterator_adaptor_kwargs(self, **kwargs):
        return kwargs
    
    def get_object_iterator_adaptor(self, object_iterator):
        '''
        Returns an iterable that standardizes the incomming object iterable
        
        :param object_iterator: An iterable containing the native objects
        '''
        klass = self.get_object_iterator_class()
        kwargs = self.get_object_iterator_adaptor_kwargs(object_iterator=object_iterator)
        return klass(**kwargs)
    
    def write_stream(self, instream):
        '''
        Processes an incomming data tap into this data tap
        
        :param instream: an iterable of standardized items
        '''
        collector = list()
        for item in instream:
            collector.append(self.write_item(item))
        return collector
    
    def write_item(self, item):
        '''
        Stores an item
        
        :param item: a json serializable dictionary
        '''
        for path, file_obj in item.files.iteritems():
            self.write_file(file_obj, path)
        return item.data
    
    def write_file(self, file_obj, path):
        '''
        Store a data file. 
        Returns the internal location of the path to be used for deserialization
        '''
        return None
    
    def read_file(self, path):
        '''
        Returns a django file object belonging to the serialized path
        '''
        return None
    
    def get_raw_item_stream(self, filetap=None):
        '''
        Returns an iterable of items belonging to this data tap
        '''
        return []
    
    command_option_list = []
    
    @classmethod
    def load_from_command_line(cls, arglist):
        '''
        Retuns an instantiated DataTap with the provided arguments from commandline
        '''
        parser = OptionParser(option_list=cls.command_option_list)
        options, args = parser.parse_args(arglist)
        return cls(*args, **options.__dict__)
