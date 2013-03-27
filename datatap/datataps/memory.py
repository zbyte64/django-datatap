from collections import deque

from datatap.datataps.base import DataTap


class MemoryDataTap(DataTap):
    '''
    Reads and writes from a que stored in memory
    '''
    def __init__(self, object_stream=None, **kwargs):
        if object_stream is None:
            object_stream = list()
        self.object_stream = deque(object_stream)
        super(MemoryDataTap, self).__init__(**kwargs)
    
    def get_raw_item_stream(self, filetap):
        '''
        Returns an iterable that pops items from the begining of the stack and stops when there are no more items
        '''
        def consumer():
            while self.object_stream:
                try:
                    yield self.object_stream.popleft()
                except IndexError:
                    return
        return consumer()
    
    def write_item(self, item):
        '''
        Add an item to the stack
        '''
        self.object_stream.append(item)
