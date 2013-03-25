import json


class DataTap(object):
    def load(self, instream):
        self.start_commit()
        for item in instream.get_item_stream():
            self.store(item)
        self.commit()
    
    def start_commit(self):
        pass
    
    def commit(self):
        pass
    
    def store(self, item):
        '''
        Stores an item
        :param item: a json serializable dictionary
        '''
        return item
    
    def store_file(self, file_obj, path):
        '''
        Store a data file
        '''
        pass
    
    def get_item_stream(self):
        '''
        Returns an iterable of items belonging to this data tap
        '''
        return []

class JSONStreamDataTap(DataTap):
    def __init__(self, stream, **kwargs):
        self.stream = stream
        super(JSONStreamDataTap, self).__init__(**kwargs)
    
    def get_item_stream(self):
        return json.load(self.stream)
    
    def load(self, instream):
        self.start_commit()
        json.dump(instream.get_item_stream(), self.stream)
        self.commit()

class ResourceDataTap(DataTap):
    '''
    Binds a data tap to the items belonging to a resource
    '''
    def __init__(self, resource, **kwargs):
        self.resource = resource
        super(ResourceDataTap, self).__init__(**kwargs)
    
    def get_resource(self):
        #TODO: wrap in an api call
        return self.resource
    
    def get_item_stream(self):
        #TODO convert item representation
        return self.get_resource().get_resource_items()

class CRUDResourceDataTap(ResourceDataTap):
    '''
    Allows for read write to a CRUD based resources
    '''
    def store(self, item):
        #item key/value => create form of resource
        endpoint = self.get_resource().endpoints['create']
        #TODO what about files?
        link = endpoint.get_link().submit({'data':item})
        return link.state.get_resource_item()


