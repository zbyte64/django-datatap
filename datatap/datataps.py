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

import json
import logging

from django.db import models
from django.utils.encoding import smart_unicode, is_protected_type

from datatap.encoders import ObjectIteratorAdaptor, DataTapJSONEncoder


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
        return datatap.write_stream(self.get_item_stream())
    
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
        self.open(mode='r')
        result = self.write_stream(datatap.get_item_stream())
        self.close()
        return result
    
    def get_item_stream(self):
        '''
        Returns an iterable of standardized items belonging to this data tap
        '''
        object_iterator = self.get_raw_item_stream()
        return self.get_object_iterator_adaptor(object_iterator=object_iterator)
    
    def open(self, mode='r'):
        pass

    def close(self):
        pass
    
    #begin non-public methods that should be implemented
    
    def get_logger(self):
        return logging.getLogger(__name__)
    
    def get_object_iterator_class(self):
        return ObjectIteratorAdaptor
    
    def get_object_iterator_adaptor_kwargs(self, **kwargs):
        return kwargs
    
    def get_object_iterator_adaptor(self, object_iterator):
        '''
        Returns an iterable that standardizes the incomming object iterable
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
        return item
    
    def write_file(self, file_obj, path):
        '''
        Store a data file
        '''
        pass
    
    def get_raw_item_stream(self):
        '''
        Returns an iterable of items belonging to this data tap
        '''
        return []

class MemoryDataTap(DataTap):
    '''
    Reads and writes from a stream stored in memory
    '''
    def __init__(self, object_stream=None, **kwargs):
        if object_stream is None:
            object_stream = list()
        self.object_stream = object_stream
        super(MemoryDataTap, self).__init__(**kwargs)
    
    def get_raw_item_stream(self):
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
    
    def get_raw_item_stream(self):
        return json.load(self.stream)
    
    def write_stream(self, instream):
        json.dump(instream, self.stream, cls=DataTapJSONEncoder)
    
    def write_item(self, item):
        json.dump(item, self.stream, cls=DataTapJSONEncoder)

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
            "fields" : fields
        }

class ModelDataTap(DataTap):
    '''
    Reads and writes from Django's ORM
    '''
    def __init__(self, *model_sources, **kwargs):
        self.model_sources = model_sources or []
        super(ModelDataTap, self).__init__(**kwargs)
    
    def get_object_iterator_class(self):
        return ModelIteratorAdaptor
    
    def get_raw_item_stream(self):
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

class ResourceIteratorAdaptor(ObjectIteratorAdaptor):
    def prepare_field_value(self, val):
        from django.core.files import File
        if isinstance(val, File):
            if hasattr(val, 'name'):
                val = val.name
            else:
                val = None
        return val
    
    def get_form_instance_values(self, form):
        data = dict()
        for name, field in form.fields.iteritems():
            val = form[name].value()
            val = self.prepare_field_value(val)
            data[name] = val
        return data
    
    def transform(self, obj):
        standard_item = self.get_form_instance_values(obj.form)
        return standard_item

class ResourceDataTap(DataTap):
    '''
    Binds a data tap to the items belonging to a resource
    '''
    def __init__(self, resource, **kwargs):
        self.resource = resource
        super(ResourceDataTap, self).__init__(**kwargs)
    
    def get_object_iterator_class(self):
        return ResourceIteratorAdaptor
    
    def get_resource(self):
        #TODO: wrap in an api call
        return self.resource
    
    def get_raw_item_stream(self):
        #TODO convert item representation
        return self.get_resource().get_resource_items()

class CRUDResourceDataTap(ResourceDataTap):
    '''
    Allows for read write to a CRUD based resources
    '''
    def write_item(self, item):
        #item key/value => create form of resource
        endpoint = self.get_resource().endpoints['create']
        #TODO what about files?
        link = endpoint.get_link().submit({'data':item})
        return link.state.get_resource_item()


