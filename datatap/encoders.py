import collections
import json

from django.utils.functional import Promise
from django.core.files import File
try:
    from django.utils.encoding import force_text
except ImportError:
    from django.utils.encoding import force_unicode as force_text
from django.core.serializers.json import DjangoJSONEncoder


class ObjectIteratorAdaptor(collections.Iterable):
    '''
    Helper class to adapt an object stream to a standardized object representation stream.
    Standardized objects are any python datastructures that are json serializable.
    '''
    def __init__(self, datatap, object_iterator):
        '''
        :param datatap: The datatap we are reading from
        :param object_iterator: An iterable of native objects
        '''
        self.datatap = datatap
        self.object_iterator = object_iterator
        self.readers = set()
    
    def transform(self, obj):
        return obj
    
    def __iter__(self):
        for obj in self.object_iterator:
            yield self.transform(obj)
    
    def close(self):
        if self in self.datatap.open_reads:
            self.datatap.open_reads.remove(self)
            self.notify_readers_of_close()
    
    def notify_readers_of_close(self):
        for reader in self.readers:
            reader.close()

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
                desired_path = getattr(obj, 'path', None) or getattr(obj, 'name')
                return {'__type__':'File', 
                        'path':self.filetap.write_file(obj, desired_path)} 
            return obj.name
        if isinstance(obj, Promise):
            return force_text(obj)
        if isinstance(obj, ObjectIteratorAdaptor):
            #TODO can we iterate through this instead?
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
                return self.filetap.read_file(dct['path'])
            else:
                return dct['path']
        return dct
