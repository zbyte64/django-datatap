from datatap.encoders import ObjectIteratorAdaptor
from datatap.datataps.base import DataTap


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


