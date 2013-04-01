import zipfile
import io
from optparse import make_option

from django.core.files.base import File

from datatap.loading import register_datatap, lookup_datatap
from datatap.datataps.base import DataTap, FileTap
from datatap.datataps.streams import StreamDataTap, JSONDataTap


class DjangoZipExtFile(File):
    def __init__(self, zipextfile, zipinfo):
        #we could try to not load it in all at once, but zipfile's crc checker seems to break
        self.file = io.BytesIO(zipextfile.read())
        self.zipinfo = zipinfo
        self.mode = 'r'
        self.name = zipinfo.filename
        self._size = zipinfo.file_size

class ZipFileTap(FileTap):
    def __init__(self, archive):
        self.archive = archive
    
    def write_file(self, file_obj, path):
        #TODO write in chunks
        #TODO write in a directory
        self.archive.writestr(path, file_obj.read())
        return path
    
    def read_file(self, path, original_path):
        zipextfile = self.archive.open(path, 'r')
        zipinfo = self.archive.getinfo(path)
        loaded_file = DjangoZipExtFile(zipextfile, zipinfo)
        loaded_file.name = original_path
        loaded_file._committed = False
        return loaded_file

class ZipFileDataTap(DataTap):
    '''
    Reads and writes objects from a zipfile with asset storage support
    
    FileDT(ZipDT(ModelDT)).write(filename) => write models to filename
    ModelDT(ZipDT(FileDT(filename))) => read from filename into models
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
        return ZipFileTap(archive)
    
    def send(self, fileobj):
        archive = zipfile.ZipFile(fileobj, 'w')
        filetap = self.get_filetap(archive)
        encoded_stream = JSONDataTap(self.item_stream, filetap=filetap) #encode our objects into json
        if isinstance(encoded_stream, basestring):
            manifest = encoded_stream
        else:
            manifest = ''.join(encoded_stream)
        archive.writestr('manifest.json', manifest)
        archive.close()
    
    def get_primitive_stream(self, instream):
        #instream is a bytes datatap but we want the file like object it reads
        archive = zipfile.ZipFile(instream.item_stream, 'r')
        filetap = self.get_filetap(archive)
        return JSONDataTap(StreamDataTap(archive.open('manifest.json')), filetap=filetap)
    
    def get_bytes_stream(self, instream):
        return instream
    
    #def detect_originating_datatap(self):
    #    return lookup_datatap(self.zipfile.read('originator.txt'))

register_datatap('Zip', ZipFileDataTap)

