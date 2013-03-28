import zipfile
from StringIO import StringIO
from optparse import make_option

from django.core.files.base import File

from datatap.loading import register_datatap, lookup_datatap
from datatap.datataps.base import DataTap
from datatap.datataps.jsonstream import JSONStreamDataTap


class DjangoZipExtFile(File):
    def __init__(self, zipextfile, zipinfo):
        self.file = zipextfile
        self.zipinfo = zipinfo
        self.mode = 'r'
        self.name = zipinfo.filename
        self._size = zipinfo.file_size
    
    def seek(self, position):
        if position != 0:
            #this will raise an unsupported operation
            return self.file.seek(position)
        #TODO if we have already done a read, reopen file

class ZipFileDataTap(DataTap):
    '''
    Reads and writes objects from a zipfile
    '''
    command_option_list = [
        make_option('--file',
            action='store',
            type='string',
            dest='filename',
        )
    ]
    
    def __init__(self, filename, **kwargs):
        self.filename = filename
        super(ZipFileDataTap, self).__init__(**kwargs)
    
    def open(self, mode='r', for_datatap=None):
        if self.mode == mode:
            return
        self.writing_files = set()
        self.zipfile = zipfile.ZipFile(self.filename, mode)
        if 'w' in mode:
            self.object_stream_file = self.get_write_file_object('manifest.json')
            if for_datatap:
                self.zipfile.writestr('originator.txt', for_datatap.get_ident())
        else:
            self.object_stream_file = self.zipfile.open('manifest.json', mode)
        self.object_stream = JSONStreamDataTap(self.object_stream_file)
        return super(ZipFileDataTap, self).open(mode, for_datatap)
    
    def detect_originating_datatap(self):
        return lookup_datatap(self.zipfile.open('originator.txt').read())
    
    class OutFile(StringIO):
        def __init__(self, datatap, path):
            self.datatap = datatap
            self.path = path
            StringIO.__init__(self)
        
        @property
        def zipfile(self):
            return self.datatap.zipfile
        
        def close(self):
            if self in self.datatap.writing_files:
                self.zipfile.writestr(self.path, self.getvalue())
                self.datatap.writing_files.remove(self)
    
    def get_write_file_object(self, path):
        outfile = self.OutFile(self, path)
        self.writing_files.add(outfile)
        return outfile
    
    def close(self):
        self.object_stream.close()
        for outfile in list(self.writing_files):
            outfile.close()
        self.zipfile.close()
    
    def write_stream(self, instream):
        self.object_stream.write_stream(instream, filetap=self.get_filetap())
    
    def write_item(self, item):
        self.object_stream.write_item(item, filetap=self.get_filetap())
    
    def write_file(self, file_obj, path):
        #TODO write in chunks
        #TODO write in a directory
        self.get_write_file_object(path).write(file_obj.read())
        return path
    
    def read_file(self, path):
        zipextfile = self.zipfile.open(path, 'r')
        zipinfo = self.zipfile.getinfo(path)
        return DjangoZipExtFile(zipextfile, zipinfo)
    
    def get_raw_item_stream(self, filetap=None):
        if filetap is None:
            filetap = self.get_filetap()
        return self.object_stream.get_item_stream(filetap=filetap)

register_datatap('ZipFile', ZipFileDataTap)

