'''
Perhaps the funky usage of this code follows closely to the command line usage.

Example usage::

    from datatap.dataps import JSONDataTap, StreamDataTap, FileDataTap, ModelDataTap, ZipFileDataTap
    
    #with django models
    iostream = BytesIO()
    tap = StreamDataTap(JSONDataTap(ModelDataTap([ContentType])))
    tap.send(iostream)
    
    #read a model fixture
    tap = ModelDataTap(JSONDataTap(FileDataTap('fixtures.json')))
    for item in tap:
        print item #a deserialized object with a save method
    tap.commit() #save all the items
    
    #write a zip archive of active users and groups
    tap = FileDataTap(ZipFileDataTap(ModelDataTap([User.objects.filter(is_active=True), Group])))
    tap.send('users.zip')
    
    #comming soon:
    tap = S3DataTap(ZipFileDataTap(ModelDataTap([User.objects.filter(is_active=True), Group])))
    tap.send('exports/users.zip') #sends it to your s3 storage bucket

'''
from datatap.datataps.base import DataTap, FileTap
from datatap.datataps.memory import MemoryDataTap
from datatap.datataps.streams import StreamDataTap, BufferedStreamDataTap, FileDataTap, JSONDataTap
from datatap.datataps.zip import ZipFileDataTap
from datatap.datataps.model import ModelDataTap
try:
    import boto
except ImportError:
    pass
else:
    from datatap.datataps.s3bucket import S3DataTap

