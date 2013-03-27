'''
Example usage::

    #with django models
    outstream = JSONStreamDataTap(stream=sys.stdout)
    outstream.open('w')
    ModelDataTap.store(outstream, MyModel, User.objects.filter(is_active=True))
    outstream.close()
    
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ModelDataTap.load(instream)
    
    
    #with hyperadmin resources
    outstream = JSONStreamDataTap(stream=sys.stdout)
    outstream.open('w')
    ResourceDataTap.store(outstream, MyResource)
    outstream.close()
    
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ResourceDataTap.load(instream)
    
    #or with substitutions
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ResourceDataTap.load(instream, mapping={'myresource_resource':'target_resource'})
'''
from datatap.datataps.base import DataTap
from datatap.datataps.memory import MemoryDataTap
from datatap.datataps.zip import ZipFileDataTap
from datatap.datataps.jsonstream import JSONStreamDataTap
from datatap.datataps.model import ModelDataTap

