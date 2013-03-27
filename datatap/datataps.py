'''
Example usage:

    #with django models
    outstream = JSONStreamDataTap(stream=sys.stdout)
    outstream.open('w')
    ModelDataTap.store(outstream, MyModel, User.objects.filter(is_active=True))
    outstream.close()
    
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ModelDataTap.load(instream)
    
    
    #with hyperadmin resources
    outstream = JSONStreamDataTap(stream=sys.stdout)
    outstream.open('w')
    ResourceDataTap.store(outstream, MyResource)
    outstream.close()
    
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ResourceDataTap.load(instream)
    
    #or with substitutions
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ResourceDataTap.load(instream, mapping={'myresource_resource':'target_resource'})
'''

import sys
import logging
import zipfile
from StringIO import StringIO
from optparse import make_option, OptionParser

from django.db import models
from django.utils.encoding import smart_unicode, is_protected_type

from datatap.encoders import ObjectIteratorAdaptor, DataTapJSONEncoder, DataTapJSONDecoder
from datatap.loading import register_datatap, lookup_datatap


class DataTap(object):
    @classmethod
    def store(cls, datatap, *args, **kwargs):
        '''
        Writes objects to the datatap from this data tap class
        Passes args and kwargs to instantiate the source data tap
        
        :param datatap: The datatap to write to
        '''
        source = cls(*args, **kwargs)
        return source._store(datatap)
    
    def _store(self, datatap):
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
        return destination._load(datatap)
    
    def _load(self, datatap):
        '''
        Writes objects from the passed in datatap to this datatap
        
        :param datatap: The datatap to read from
        '''
        self.open(mode='r', for_datatap=datatap)
        result = self.write_stream(datatap.get_item_stream(filetap=self.get_filetap()))
        self.close()
        return result
    
    def get_item_stream(self, filetap=None):
        '''
        Returns an iterable of standardized objects belonging to this data tap
        '''
        object_iterator = self.get_raw_item_stream(filetap)
        return self.get_object_iterator_adaptor(object_iterator=object_iterator)
    
    def get_filetap(self):
        '''
        Returns a datatap to store the files
        '''
        return self
    
    #begin non-public methods that should be implemented
    
    def open(self, mode='r', for_datatap=None):
        '''
        Open the DataTap for data operations.
        
        :param mode: r for read, w for write
        :param for_datatap: The datatap we are opening for. If this is a read then attempt to check if it is compatible with the originating datap. If this is a write then store the datatap type.
        '''
        self.mode = mode
    
    def detect_originating_datatap(self):
        '''
        If this is a datatap we are reading from then return the datatap that was used for populating this datatap
        '''
        pass

    def close(self):
        self.mode = None
    
    def get_logger(self):
        return logging.getLogger(__name__)
    
    def get_object_iterator_class(self):
        return ObjectIteratorAdaptor
    
    def get_object_iterator_adaptor_kwargs(self, **kwargs):
        return kwargs
    
    def get_object_iterator_adaptor(self, object_iterator):
        '''
        Returns an iterable that standardizes the incomming object iterable
        
        :param object_iterator: An iterable containing the native objects
        :param filetap_promise: A function returning the promise of a serialized file
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

class MemoryDataTap(DataTap):
    '''
    Reads and writes from a stream stored in memory
    '''
    def __init__(self, object_stream=None, **kwargs):
        if object_stream is None:
            object_stream = list()
        self.object_stream = object_stream
        super(MemoryDataTap, self).__init__(**kwargs)
    
    def get_raw_item_stream(self, filetap):
        def consumer():
            while self.object_stream:
                yield self.object_stream.pop(0)
        return consumer()
    
    def write_item(self, item):
        self.object_stream.append(item)

class JSONStreamDataTap(DataTap):
    '''
    Reads and writes from a stream serialized with json
    '''
    def __init__(self, stream, **kwargs):
        self.stream = stream
        super(JSONStreamDataTap, self).__init__(**kwargs)
    
    def get_raw_item_stream(self, filetap=None):
        decoder = DataTapJSONDecoder(filetap=filetap)
        return decoder.decode(self.stream.read())
    
    def write_stream(self, instream, filetap=None):
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
        options.__dict__['stream'] = sys.stdout
        return cls(*args, **options.__dict__)

register_datatap('JSONStream', JSONStreamDataTap)


class ZipFileDataTap(DataTap):
    '''
    Reads and writes objects from a zipfile
    '''
    command_option_list = [
        make_option('--file',
            action='store',
            type='string',
            dest='filename',
        )
    ]
    
    def __init__(self, filename, **kwargs):
        self.filename = filename
        super(ZipFileDataTap, self).__init__(**kwargs)
    
    def open(self, mode='r', for_datatap=None):
        self.writing_files = set()
        self.zipfile = zipfile.ZipFile(self.filename, mode)
        if 'w' in mode:
            self.object_stream_file = self.get_write_file_object('manifest.json')
            if for_datatap:
                self.zipfile.writestr('originator.txt', for_datatap.get_ident())
        else:
            self.object_stream_file = self.zipfile.open('manifest.json', mode)
        self.object_stream = JSONStreamDataTap(self.object_stream_file)
    
    def detect_originating_datatap(self):
        return lookup_datatap(self.zipfile.open('originator.txt').read())
    
    class OutFile(StringIO):
        def __init__(self, datatap, path):
            self.datatap = datatap
            self.path = path
            StringIO.__init__(self)
        
        @property
        def zipfile(self):
            return self.datatap.zipfile
        
        def close(self):
            self.zipfile.writestr(self.path, self.getvalue())
            self.datatap.writing_files.remove(self)
    
    def get_write_file_object(self, path):
        outfile = self.OutFile(self, path)
        self.writing_files.add(outfile)
        return outfile
    
    def close(self):
        self.object_stream.close()
        for outfile in list(self.writing_files):
            outfile.close()
        self.zipfile.close()
    
    def write_stream(self, instream):
        self.object_stream.write_stream(instream, filetap=self.get_filetap())
    
    def write_item(self, item):
        self.object_stream.write_item(item, filetap=self.get_filetap())
    
    def write_file(self, file_obj, path):
        #TODO write in chunks
        #TODO write in a directory
        self.get_write_file_object(path).write(file_obj.read())
        return path
    
    def read_file(self, path):
        return self.zipfile.open(path, 'r')
    
    def get_raw_item_stream(self, filetap):
        return self.object_stream.get_item_stream(filetap=filetap)

register_datatap('ZipFile', ZipFileDataTap)


class ModelIteratorAdaptor(ObjectIteratorAdaptor):
    def transform(self, obj):
        fields = dict()
        for field in obj._meta.fields:
            value = field._get_val_from_obj(obj)
            #TODO foreign keys and many to many fields
            fields[field.name] = value
            
        return {
            "model"  : smart_unicode(obj._meta),
            "pk"     : smart_unicode(obj._get_pk_val(), strings_only=True),
            "fields" : fields,
        }

#TODO m2m
#TODO should we return deserialized object?
#TODO natural keys
class ModelDataTap(DataTap):
    '''
    Reads and writes from Django's ORM
    '''
    def __init__(self, *model_sources, **kwargs):
        self.model_sources = model_sources or []
        super(ModelDataTap, self).__init__(**kwargs)
    
    def get_object_iterator_class(self):
        return ModelIteratorAdaptor
    
    def get_raw_item_stream(self, filetap=None):
        for source in self.model_sources:
            try:
                is_model = issubclass(source, models.Model)
            except TypeError:
                is_model = False
            
            if is_model:
                queryset = source.objects.all()
            else:
                queryset = source
            for item in queryset.iterator():
                yield item
    
    #TODO store_stream => bulk create
    #def write_stream(self, instream):
    #    
    
    def write_item(self, item):
        model = self.get_model(item['model'])
        params = item['fields']
        params['pk'] = item['pk']
        return model.objects.create(**params)
    
    def get_model(self, model_identifier):
        """
        Helper to look up a model from an "app_label.module_name" string.
        """
        try:
            Model = models.get_model(*model_identifier.split("."))
        except TypeError:
            Model = None
        if Model is None:
            raise ValueError(u"Invalid model identifier: '%s'" % model_identifier)
        return Model
    
    @classmethod
    def load_from_command_line(cls, arglist):
        parser = OptionParser(option_list=cls.command_option_list)
        options, args = parser.parse_args(arglist)
        model_sources = list()
        for arg in args: #list of apps and model names
            if '.' in arg:
                model_sources.append(models.get_model(*arg.split(".", 1)))
            else:
                #get models form appname
                model_sources.extend(models.get_models(models.get_app(arg)))
        return cls(*model_sources, **options.__dict__)

register_datatap('Model', ModelDataTap)


class ResourceIteratorAdaptor(ObjectIteratorAdaptor):
    def prepare_field_value(self, val):
        #if isinstance(val, File):
        #    if hasattr(val, 'name'):
        #        val = val.name
        #    else:
        #        val = None
        return val
    
    def get_form_instance_values(self, form):
        fields = dict()
        for name, field in form.fields.iteritems():
            val = form[name].value()
            val = self.prepare_field_value(val)
            fields[name] = val
        return fields
    
    def transform(self, obj):
        fields = self.get_form_instance_values(obj.form)
        return {
            'endpoint': obj.endpoint.get_url_name(),
            'fields': fields,
        }

class ResourceDataTap(DataTap):
    '''
    Binds a data tap to the items belonging to a resource
    '''
    def __init__(self, *item_sources, **kwargs):
        self.item_sources = item_sources or []
        super(ResourceDataTap, self).__init__(**kwargs)
    
    def get_object_iterator_class(self):
        return ResourceIteratorAdaptor
    
    def get_items_from_resource(self, endpoint):
        #TODO: wrap in an api call
        return endpoint.get_resource_items() #or get_items()
    
    def get_resource(self, urlname):
        raise NotImplementedError, 'TODO'
    
    def get_raw_item_stream(self):
        from hyperadmin.endpoints import Endpoint
        for source in self.item_sources:
            try:
                is_endpoint = issubclass(source, Endpoint)
            except TypeError:
                is_endpoint = False
            if is_endpoint:
                items = self.get_items_from_resource(source)
            else:
                items = source
            for item in items:
                yield item

class CRUDResourceDataTap(ResourceDataTap):
    '''
    Allows for read write to a CRUD based resources
    '''
    def write_item(self, item):
        #item key/value => create form of resource
        url_name = item['endpoint']
        endpoint = self.get_resource(url_name).endpoints['create']
        #TODO what about files?
        link = endpoint.get_link().submit({'data':item['fields']})
        return link.state.get_resource_item()


