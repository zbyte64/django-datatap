from __future__ import absolute_import

import tarfile
from optparse import Option
from io import BytesIO
from copy import copy

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
    def __init__(self, archive, files=None):
        self.archive = archive
        self.files = files
    
    def write_file(self, file_obj, path):
        #TODO write in chunks
        #TODO write in a directory
        payload = file_obj.read()
        tarinfo = tarfile.TarInfo(path)
        tarinfo.size = len(payload)
        self.archive.addfile(tarinfo, BytesIO(payload))
        return path
    
    def read_file(self, path, original_path):
        loaded_file = copy(self.files[path])
        loaded_file.name = original_path
        loaded_file._committed = False
        return loaded_file

class TarFileDataTap(DataTap):
    '''
    Reads and writes objects from a zipfile
    '''
    def __init__(self, instream=None, compression=None, **kwargs):
        self.compression = compression
        super(TarFileDataTap, self).__init__(instream, **kwargs)
    
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
    
    def get_filetap(self, archive, files=None):
        return TarFileTap(archive, files)
    
    def send(self, fileobj):
        mode = 'w'
        if self.compression:
            mode += ':' + self.compression
        archive = tarfile.TarFile(fileobj=fileobj, mode=mode)
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
        mode = 'r'
        if self.compression:
            mode += ':' + self.compression
        archive = tarfile.TarFile(fileobj=instream.item_stream, mode=mode)
        manifest = None
        files = {}
        for tarinfo in archive:
            if not tarinfo.isreg():
                continue
            fileobj = archive.extractfile(tarinfo)
            assert fileobj is not None
            files[tarinfo.path] = DjangoTarExtFile(fileobj, tarinfo)
            if tarinfo.name == 'manifest.json':
                manifest = fileobj
        if manifest is None:
            archive.extractfile('manifest.json') #TODO raise a proper exception
        filetap = self.get_filetap(archive, files)
        return JSONDataTap(StreamDataTap(manifest), filetap=filetap)
    
    def get_bytes_stream(self, instream):
        return instream
    
    command_option_list = [
        Option('--gzip', action='store_const', const='gz', dest='compression'),
        Option('--bz', action='store_const', const='bz', dest='compression'),
    ]

register_datatap('TarFile', TarFileDataTap)

