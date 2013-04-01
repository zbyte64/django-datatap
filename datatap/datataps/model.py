from optparse import OptionParser, Option
from collections import deque

from django.db import models
from django.db import transaction
from django.core.files import File
from django.core.serializers.python import Serializer, Deserializer
from django.utils.encoding import is_protected_type

from datatap.loading import register_datatap
from datatap.datataps.base import DataTap


class FileAwareSerializer(Serializer):
    def handle_field(self, obj, field):
        value = field._get_val_from_obj(obj)
        if isinstance(value, File) or is_protected_type(value):
            self._current[field.name] = value
        else:
            self._current[field.name] = field.value_to_string(obj)

class ModelDataTap(DataTap):
    '''
    Reads and writes from Django's ORM
    
    ModelDT([MyModel, Queryset, ModelInstance]) => primitive representation of sources
    ModelDT(ZipDT(...)) => deserialized objects from the zip datatap
    ModelDT(ZipDT(...)).commit() => save the deserialized objects
    '''
    def __init__(self, instream=None, use_natural_keys=True, track_uncommitted=True, **kwargs):
        self.use_natural_keys = use_natural_keys
        
        #this is so we can view objects and then easily commit
        if track_uncommitted:
            self.deserialized_objects = deque()
        else:
            self.deserialized_objects = None
        super(ModelDataTap, self).__init__(instream, **kwargs)
    
    def get_domain(self):
        if self.instream is None: #no instream, I guess we write?
            return 'deserialized_model'
        if isinstance(self.instream, (list, tuple)):
            return 'primitive'
        if self.instream.domain == 'primitive':
            return 'deserialized_model'
    
    def get_instance_stream(self, instream):
        for source in instream:
            try:
                is_model = issubclass(source, models.Model)
                is_instance = False
            except TypeError:
                is_model = False
                is_instance = isinstance(source, models.Model)
            
            if is_model:
                queryset = source.objects.all().iterator()
            elif is_instance:
                queryset = [source]
            else:
                if hasattr(source, 'iterator'):
                    queryset = source.iterator()
                else:
                    queryset = source
            for item in queryset:
                yield item
    
    def get_primitive_stream(self, instream):
        '''
        Convert various model sources to primitive objects
        '''
        serializer = FileAwareSerializer()
        instances = self.get_instance_stream(instream)
        return serializer.serialize(instances, use_natural_keys=self.use_natural_keys)
    
    def get_deserialized_model_stream(self, instream):
        '''
        Convert primitive objects to deserialized model instances
        '''
        for deserialized_model in Deserializer(instream):
            if self.deserialized_objects is not None:
                self.deserialized_objects.append(deserialized_model)
            yield deserialized_model
    
    def get_model_stream(self, instream):
        '''
        Convert primitive objects to saved model instances
        '''
        for item in self.get_deserialized_model_stream(instream):
            item.save()
            yield item.object
    
    @transaction.commit_manually
    def commit(self):
        if transaction.is_dirty():
            transaction.commit()
        while self.deserialized_objects:
            instance = self.deserialized_objects.popleft()
            instance.save()
            transaction.commit()
        self.deserialized_objects = None
        for instance in self:
            instance.save()
            transaction.commit()
        if transaction.is_dirty():
            transaction.commit()
    
    command_option_list = [
        Option('--disable_natural_keys', action='store_false', dest='use_natural_keys'),
    ]
    
    @classmethod
    def load_from_command_line(cls, arglist, instream=None):
        parser = OptionParser(option_list=cls.command_option_list)
        options, args = parser.parse_args(arglist)
        kwargs = options.__dict__
        if instream is None:
            model_sources = list()
            for arg in args: #list of apps and model names
                if '.' in arg:
                    model_sources.append(models.get_model(*arg.split(".", 1)))
                else:
                    #get models from appname
                    model_sources.extend(models.get_models(models.get_app(arg)))
            kwargs['instream'] = model_sources
        else:
            kwargs['instream'] = instream
        return cls(**kwargs)

register_datatap('Model', ModelDataTap)
