from optparse import OptionParser

from django.db import models
from django.core.files import File
from django.core.serializers.python import Serializer, Deserializer
from django.utils.encoding import is_protected_type

from datatap.encoders import ObjectIteratorAdaptor
from datatap.loading import register_datatap
from datatap.datataps.base import DataTap, WriteStream


class ModelWriteStream(WriteStream):
    def __init__(self, datatap, itemstream):
        super(ModelWriteStream, self).__init__(datatap, Deserializer(itemstream))
    
    def process_item(self, item):
        #item is a deserialized object
        item.save()
        return item.object

class FileAwareSerializer(Serializer):
    def handle_field(self, obj, field):
        value = field._get_val_from_obj(obj)
        if isinstance(value, File) or is_protected_type(value):
            self._current[field.name] = value
        else:
            self._current[field.name] = field.value_to_string(obj)

class ModelIteratorAdaptor(ObjectIteratorAdaptor):
    def __init__(self, use_natural_keys=True, **kwargs):
        self.serializer = FileAwareSerializer()
        self.use_natural_keys = use_natural_keys
        super(ModelIteratorAdaptor, self).__init__(**kwargs)
    
    def transform(self, obj):
        #TODO not thread safe
        self.serializer.serialize([obj], use_natural_keys=self.use_natural_keys)
        return self.serializer.objects.pop()

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
        '''
        Yields objects from the model sources
        '''
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
    
    def write_stream(self, instream):
        a_stream = ModelWriteStream(self, instream)
        self.open_writes.add(a_stream)
        return a_stream
    
    def write_item(self, item):
        '''
        Creates and returns a model instance
        '''
        result = Deserializer([item]).next()
        result.save()
        return result.object
    
    @classmethod
    def load_from_command_line(cls, arglist):
        parser = OptionParser(option_list=cls.command_option_list)
        options, args = parser.parse_args(arglist)
        model_sources = list()
        for arg in args: #list of apps and model names
            if '.' in arg:
                model_sources.append(models.get_model(*arg.split(".", 1)))
            else:
                #get models from appname
                model_sources.extend(models.get_models(models.get_app(arg)))
        return cls(*model_sources, **options.__dict__)

register_datatap('Model', ModelDataTap)
