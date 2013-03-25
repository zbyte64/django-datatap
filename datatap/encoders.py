import collections

from django.utils.functional import Promise
try:
    from django.utils.encoding import force_text
except ImportError:
    from django.utils.encoding import force_unicode as force_text
from django.core.serializers.json import DjangoJSONEncoder


class SerializableObject(object):
    def __init__(self, data, files={}):
        self.data = data
        self.files = files #path=file_obj

class ObjectIteratorAdaptor(collections.Iterable):
    '''
    Helper class to adapt an object stream to a standardized object representation stream.
    Standardized objects are any python datastructures that are json serializable.
    '''
    def __init__(self, object_iterator):
        self.object_iterator = object_iterator
    
    def transform(self, obj):
        return SerializableObject(obj)
    
    def __iter__(self):
        for obj in self.object_iterator:
            yield self.transform(obj)

class DataTapJSONEncoder(DjangoJSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, Promise):
            return force_text(obj)
        if isinstance(obj, ObjectIteratorAdaptor):
            #TODO can we iterate through this instead?
            return list(obj)
        if isinstance(obj, SerializableObject):
            return obj.data
        return super(HyperadminJSONEncoder, self).default(obj)
