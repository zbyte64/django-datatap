from datatap.datataps.base import DataTap


class MemoryDataTap(DataTap):
    '''
    Reads and writes from a que stored in memory
    '''
    def get_domain(self):
        if not isinstance(self.instream, DataTap):
            return 'primitive'
        return self.instream.domain
    
    def get_item_stream(self):
        '''
        Iterates through the objects in the instream
        '''
        for item in self.instream:
            yield item
    
    def write(self, chunk):
        self.instream.append(chunk)

