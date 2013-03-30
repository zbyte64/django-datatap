from __future__ import absolute_import

import tarfile
from io import BytesIO
from optparse import make_option

from django.core.files.base import File

from datatap.loading import register_datatap, lookup_datatap
from datatap.datataps.base import DataTap
from datatap.datataps.jsonstream import JSONStreamDataTap


class DjangoTarExtFile(File):
    def __init__(self, tarextfile, tarinfo):
        self.file = tarextfile
        self.tarinfo = tarinfo
        self.mode = 'r'
        self.name = tarinfo.name
        self._size = tarinfo.size
    
    #def seek(self, position):
    #    if position != 0:
    #        #this will raise an unsupported operation
    #        return self.file.seek(position)
    #    #TODO if we have already done a read, reopen file

class TarFileDataTap(DataTap):
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
        super(TarFileDataTap, self).__init__(**kwargs)
    
    def open(self, mode='r', for_datatap=None):
        if self.mode == mode:
            return
        self.writing_files = set()
        self.tarfile = tarfile.TarFile(self.filename, mode)
        if 'w' in mode:
            self.object_stream_file = self.get_write_file_object('manifest.json')
            if for_datatap:
                payload = for_datatap.get_ident()
                tarinfo = tarfile.TarInfo('originator.txt')
                tarinfo.size = len(payload)
                self.tarfile.addfile(tarinfo, BytesIO(payload))
        else:
            self.object_stream_file = self.tarfile.extractfile('manifest.json')
        self.object_stream = JSONStreamDataTap(self.object_stream_file)
        return super(TarFileDataTap, self).open(mode, for_datatap)
    
    def detect_originating_datatap(self):
        return lookup_datatap(self.tarfile.extractfile('originator.txt').read())
    
    class OutFile(BytesIO):
        def __init__(self, datatap, path):
            self.datatap = datatap
            self.path = path
            BytesIO.__init__(self)
        
        @property
        def tarfile(self):
            return self.datatap.tarfile
        
        def close(self):
            if self in self.datatap.writing_files:
                tarinfo = tarfile.TarInfo(self.path)
                tarinfo.size = len(self.getvalue())
                self.seek(0)
                payload = self
                self.tarfile.addfile(tarinfo, payload)
                self.datatap.writing_files.remove(self)
    
    def get_write_file_object(self, path):
        outfile = self.OutFile(self, path)
        self.writing_files.add(outfile)
        return outfile
    
    def close(self):
        super(TarFileDataTap, self).close()
        self.object_stream.close()
        for outfile in list(self.writing_files):
            outfile.close()
        self.tarfile.close()
    
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
        tarextfile = self.tarfile.open(path)
        tarinfo = self.tarfile.getmember(path)
        return DjangoTarExtFile(tarextfile, tarinfo)
    
    def get_raw_item_stream(self, filetap=None):
        if filetap is None:
            filetap = self.get_filetap()
        return self.object_stream.get_item_stream(filetap=filetap)

register_datatap('TarFile', TarFileDataTap)

