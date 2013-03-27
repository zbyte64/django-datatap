'''
Example usage::

    from datatap.dataps import JSONStreamDataTap, ModelDataTap, ZipFileDataTap
    
    #with django models
    outstream = JSONStreamDataTap(stream=sys.stdout)
    outstream.open('w'. for_datatap=ModelDataTap)
    source = ModelDataTap(MyModel, User.objects.filter(is_active=True))
    source.dump(outstream)
    
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ModelDataTap.load(instream)
    
    #give me all active users to stdout
    ModelDataTap.store(JSONStreamDataTap(stream=sys.stdout), User.objects.filter(is_active=True))
    
    #write Blog and BlogImages to a zipfile
    archive = ZipFileDataTap(filename='myblog.zip')
    archive.open('w', for_datatap=ModelDataTap)
    #or do it in one line: archive = ZipFileDataTap(filename='myblog.zip', mode='w', for_datatap=ModelDataTap)
    ModelDataTap.store(archive, Blog, BlogImages)
    archive.close()
    
    archive = ZipFileDataTap(filename='myblog.zip', mode='r')
    ModedDataTap.load(archive)

'''
from datatap.datataps.base import DataTap
from datatap.datataps.memory import MemoryDataTap
from datatap.datataps.zip import ZipFileDataTap
from datatap.datataps.jsonstream import JSONStreamDataTap
from datatap.datataps.model import ModelDataTap

