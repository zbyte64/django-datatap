from __future__ import absolute_import

import tarfile
from io import BytesIO

from django.core.files.base import File

from datatap.loading import register_datatap
from datatap.datataps.base import DataTap, FileTap
from datatap.datataps.streams import StreamDataTap, JSONDataTap


class DjangoTarExtFile(File):
    def __init__(self, tarextfile, tarinfo):
        self.file = tarextfile
        self.tarinfo = tarinfo
        self.mode = 'r'
        self.name = tarinfo.name
        self._size = tarinfo.size

class WritableTarExtFile(BytesIO):
    def __init__(self, archive, path, payload):
        self.archive = archive
        self.path = path
        super(WritableTarExtFile, self).__init__(payload)
    
    def save(self):
        self.seek(0)
        payload = self.getvalue()
        tinfo = tarfile.TarInfo(self.path)
        tinfo.size = len(payload)
        self.seek(0)
        self.archive.addfile(tinfo, self)
    

class TarFileTap(FileTap):
    def __init__(self, archive):
        self.archive = archive
    
    def write_file(self, file_obj, path):
        #TODO write in chunks
        #TODO write in a directory
        payload = file_obj.read()
        tarinfo = tarfile.TarInfo(path)
        tarinfo.size = len(payload)
        self.archive.addfile(tarinfo, BytesIO(payload))
        return path
    
    def read_file(self, path):
        zipextfile = self.archive.open(path, 'r')
        zipinfo = self.archive.getinfo(path)
        return DjangoTarExtFile(zipextfile, zipinfo)

class TarFileDataTap(DataTap):
    '''
    Reads and writes objects from a zipfile
    '''
    def get_domain(self):
        if self.instream.domain == 'bytes':
            #file as our input, we emit text representation of primitives
            return 'primitive'
        if self.instream.domain == 'primitive':
            #text representation of primitives as our input, we write to file
            #CONSIDER: if our target is a file, then the parent constructor must pass it in!
            #or we return a callable that takes the desired file object
            return 'bytes'
        assert False, 'Unrecognized instream domain: %s' % self.instream.domain
    
    def get_filetap(self, archive):
        return TarFileTap(archive)
    
    def send(self, fileobj):
        archive = tarfile.TarFile(fileobj=fileobj, mode='w')
        filetap = self.get_filetap(archive)
        encoded_stream = JSONDataTap(self.item_stream, filetap=filetap) #encode our objects into json
        if isinstance(encoded_stream, basestring):
            manifest = encoded_stream
        else:
            manifest = ''.join(encoded_stream)
        
        manifest_file = WritableTarExtFile(archive, 'manifest.json', manifest)
        manifest_file.save()
        archive.close()
    
    def get_primitive_stream(self, instream):
        #instream is a bytes datatap but we want the file like object it reads
        archive = tarfile.TarFile(fileobj=instream.item_stream, mode='r')
        filetap = self.get_filetap(archive)
        return JSONDataTap(StreamDataTap(archive.extractfile('manifest.json')), filetap=filetap)
    
    def get_bytes_stream(self, instream):
        return instream

register_datatap('TarFile', TarFileDataTap)

