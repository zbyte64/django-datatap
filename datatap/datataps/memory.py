from datatap.datataps.base import DataTap


class MemoryDataTap(DataTap):
    '''
    Reads and writes from a stream stored in memory
    '''
    def __init__(self, object_stream=None, **kwargs):
        if object_stream is None:
            object_stream = list()
        self.object_stream = object_stream
        super(MemoryDataTap, self).__init__(**kwargs)
    
    def get_raw_item_stream(self, filetap):
        def consumer():
            while self.object_stream:
                yield self.object_stream.pop(0)
        return consumer()
    
    def write_item(self, item):
        self.object_stream.append(item)
