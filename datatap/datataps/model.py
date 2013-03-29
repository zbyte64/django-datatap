from optparse import OptionParser

from django.db import models
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
    ModelDT(ZipDT(...)) => deserialized saved objects from the zip datatap
    '''
    def __init__(self, instream=None, use_natural_keys=True, **kwargs):
        self.use_natural_keys = use_natural_keys
        super(ModelDataTap, self).__init__(instream, **kwargs)
    
    def get_domain(self):
        if self.instream is None:
            return 'model'
        if isinstance(self.instream, (list, tuple)):
            return 'primitive'
        if self.instream.domain == 'primitive':
            return 'model'
    
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
        return Deserializer(instream)
    
    def get_model_stream(self, instream):
        '''
        Convert primitive objects to saved model instances
        '''
        for item in self.get_deserialized_model_stream(instream):
            item.save()
            yield item.object
    
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
    
    @classmethod
    def load_from_command_line_for_write(cls, arglist, instream):
        '''
        Retuns an instantiated DataTap with the provided arguments from commandline
        '''
        parser = OptionParser(option_list=cls.command_option_list)
        options, args = parser.parse_args(arglist)
        kwargs = options.__dict__
        kwargs['instream'] = instream
        
        datatap = cls(*args, **kwargs)
        def commit(*a, **k):
            #by simply reading we are saving
            return datatap.read()
        datatap.commit = commit
        return datatap

register_datatap('Model', ModelDataTap)
