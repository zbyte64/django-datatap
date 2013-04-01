import json
import types

from django.utils.functional import Promise
from django.core.files import File
try:
    from django.utils.encoding import force_text
except ImportError:
    from django.utils.encoding import force_unicode as force_text
from django.core.serializers.json import DjangoJSONEncoder


class DataTapJSONEncoder(DjangoJSONEncoder):
    def __init__(self, *args, **kwargs):
        self.filetap = kwargs.pop('filetap', None)
        super(DataTapJSONEncoder, self).__init__(*args, **kwargs)
    
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, File):
            #CONSIDER: is this document safe?
            if self.filetap:
                desired_path = getattr(obj, 'name', None) or getattr(obj, 'path')
                return {'__type__':'File', 
                        'path': desired_path,
                        'storage_path':self.filetap.write_file(obj, desired_path)} 
            return obj.name
        if isinstance(obj, Promise):
            return force_text(obj)
        
        #TODO can we iterate through this instead?
        if isinstance(obj, types.GeneratorType):
            return list(obj)
        if hasattr(obj, 'next') and hasattr(obj, '__iter__'): #an iterator:
            return list(obj)
        
        return super(DataTapJSONEncoder, self).default(obj)

class DataTapJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('object_hook', self.decode_objects)
        self.filetap = kwargs.pop('filetap', None)
        super(DataTapJSONDecoder, self).__init__(*args, **kwargs)

    def decode_objects(self, dct):
        if '__type__' in dct and dct['__type__'] == 'File':
            if self.filetap:
                return self.filetap.read_file(dct['storage_path'], dct['path'])
            else:
                return dct['path']
        return dct
