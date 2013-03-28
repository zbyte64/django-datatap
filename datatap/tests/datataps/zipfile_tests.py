from tempfile import mkstemp
import zipfile
import json

from django.utils import unittest
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Group
from django.core.files.base import File, ContentFile as BaseContentFile
from django.core.files.storage import DefaultStorage

from datatap.datataps import ZipFileDataTap, ModelDataTap


class ContentFile(BaseContentFile): #for ease with Django 1.3
    def __init__(self, content, name=None):
        super(ContentFile, self).__init__(content)
        self.name = name

class ZipFileDataTapTestCase(unittest.TestCase):
    def test_store(self):
        #outdir = mkdtemp('datatap-testcase')
        #filename = os.path.join(outdir, 'zipfiletest.zip')
        filename = mkstemp('zip', 'datataptest')[1]
        outstream = ZipFileDataTap(filename=filename)
        outstream.open('w', for_datatap=ModelDataTap)
        response = ModelDataTap.store(outstream, ContentType)
        outstream.close()
        archive = zipfile.ZipFile(filename)
        self.assertTrue('manifest.json' in archive.namelist())
        manifest = json.load(archive.open('manifest.json', 'r'))
        self.assertEqual(len(manifest), ContentType.objects.all().count())
    
    def test_load(self):
        Group.objects.all().delete()
        item = {
            'model': 'auth.group',
            'pk':5,
            'fields': {
                'name': 'testgroup',
            }
        }
        filename = mkstemp('zip', 'datataptest')[1]
        archive = zipfile.ZipFile(filename, 'w')
        archive.writestr('manifest.json', json.dumps([item]))
        archive.close()
        
        instream = ZipFileDataTap(filename=filename)
        instream.open('r', for_datatap=ModelDataTap)
        result = list(ModelDataTap.load(instream))
        instream.close()
        
        self.assertEqual(Group.objects.all().count(), 1)
        self.assertEqual(len(result), 1)
        self.assertTrue(isinstance(result[0], Group))
        self.assertEqual(result[0].name, 'testgroup')

class ZipFileDataTapAssetsTestCase(unittest.TestCase):
    def test_write_item_with_a_file(self):
        filename = mkstemp('zip', 'datataptest')[1]
        tap = ZipFileDataTap(filename=filename)
        tap.open('w')
        
        sample_file = ContentFile('Just some file, move along', 'readme.txt')
        tap.write_item({
            'test':'item',
            'readme':sample_file,
        })
        tap.close()
        
        archive = zipfile.ZipFile(filename, 'r')
        payload = archive.read('manifest.json')
        self.assertEqual('{"test": "item", "readme": {"path": "readme.txt", "__type__": "File"}}', payload)
        readme = archive.read('readme.txt')
        self.assertEqual(readme, 'Just some file, move along')
    
    def test_write_stream_with_files(self):
        filename = mkstemp('zip', 'datataptest')[1]
        tap = ZipFileDataTap(filename=filename)
        tap.open('w')
        
        sample_file = ContentFile('Just some file, move along', 'readme.txt')
        sample_file2 = ContentFile('Just some file, move along', 'readme2.txt')
        in_stream = [
            {'test1': 'item', 'readme': sample_file,},
            {'test2': 'item2', 'readme': sample_file2,},
        ]
        tap.write_stream(in_stream)
        tap.close()
        
        archive = zipfile.ZipFile(filename, 'r')
        payload = archive.read('manifest.json')
        self.assertEqual([{"test1": "item", "readme": {"path": "readme.txt", "__type__": "File"}}, {"test2": "item2", "readme": {"path": "readme2.txt", "__type__": "File"}}], json.loads(payload))
    
    def test_get_item_stream_with_files(self):
        filename = mkstemp('zip', 'datataptest')[1]
        
        archive = zipfile.ZipFile(filename, 'w')
        in_stream = [
            {'test1': 'item', 'readme': 
                {'__type__':'File',
                 'path':'assets/readme.txt',}
            },
            {'test2': 'item2', 'readme': 
                {'__type__':'File',
                 'path':'assets/readme2.txt',}
            },
        ]
        archive.writestr('manifest.json', json.dumps(in_stream))
        archive.writestr('assets/readme.txt', 'readme1')
        archive.writestr('assets/readme2.txt', 'readme2')
        archive.close()
        
        tap = ZipFileDataTap(filename=filename)
        tap.open('r')
        items = list(tap.get_item_stream())
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]['test1'], 'item')
        
        self.assertTrue(isinstance(items[0]['readme'], File))
        self.assertEqual(items[1]['test2'], 'item2')
        self.assertTrue(isinstance(items[1]['readme'], File))
        
        #test that the file object passed back can be properly saved
        file_to_save = items[0]['readme']
        storage = DefaultStorage()
        result = storage.save(file_to_save.name, file_to_save)
        assert result
        
        tap.close()
