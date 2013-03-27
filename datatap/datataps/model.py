from optparse import OptionParser

from django.db import models
from django.utils.encoding import smart_unicode

from datatap.encoders import ObjectIteratorAdaptor
from datatap.loading import register_datatap
from datatap.datataps.base import DataTap


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
    
    #TODO store_stream => bulk create
    #def write_stream(self, instream):
    #    
    
    def write_item(self, item):
        '''
        Creates and returns a model instance
        '''
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
