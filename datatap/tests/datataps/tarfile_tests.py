from tempfile import mkstemp
import tarfile
import json
from io import BytesIO

from django.utils import unittest
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Group
from django.core.files.base import File, ContentFile as BaseContentFile
from django.core.files.storage import DefaultStorage

from datatap.datataps import ModelDataTap
from datatap.datataps.tarfile import TarFileDataTap


class ContentFile(BaseContentFile): #for ease with Django 1.3
    def __init__(self, content, name=None):
        super(ContentFile, self).__init__(content)
        self.name = name

class TarFileDataTapTestCase(unittest.TestCase):
    def test_store(self):
        filename = mkstemp('tar', 'datataptest')[1]
        outstream = TarFileDataTap(filename=filename)
        outstream.open('w', for_datatap=ModelDataTap)
        response = ModelDataTap.store(outstream, ContentType)
        outstream.close()
        archive = tarfile.TarFile(filename)
        self.assertTrue('manifest.json' in archive.getnames())
        manifest = json.load(archive.extractfile('manifest.json'))
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
        filename = mkstemp('tar', 'datataptest')[1]
        archive = tarfile.TarFile(filename, 'w')
        tinfo = tarfile.TarInfo('manifest.json')
        file_obj = BytesIO(json.dumps([item]))
        archive.addfile(tinfo, file_obj)
        archive.close()
        
        instream = TarFileDataTap(filename=filename)
        instream.open('r', for_datatap=ModelDataTap)
        result = list(ModelDataTap.load(instream))
        instream.close()
        
        self.assertEqual(Group.objects.all().count(), 1)
        self.assertEqual(len(result), 1)
        self.assertTrue(isinstance(result[0], Group))
        self.assertEqual(result[0].name, 'testgroup')

class TarFileDataTapAssetsTestCase(unittest.TestCase):
    def test_write_item_with_a_file(self):
        filename = mkstemp('tar', 'datataptest')[1]
        tap = TarFileDataTap(filename=filename)
        tap.open('w')
        
        sample_file = ContentFile('Just some file, move along', 'readme.txt')
        tap.write_item({
            'test':'item',
            'readme':sample_file,
        })
        tap.close()
        
        archive = tarfile.TarFile(filename, 'r')
        payload = archive.extractfile('manifest.json').read()
        self.assertEqual('{"test": "item", "readme": {"path": "readme.txt", "__type__": "File"}}', payload)
        readme = archive.extractfile('readme.txt').read()
        self.assertEqual(readme, 'Just some file, move along')
    
    def test_write_stream_with_files(self):
        filename = mkstemp('tar', 'datataptest')[1]
        tap = TarFileDataTap(filename=filename)
        tap.open('w')
        
        sample_file = ContentFile('Just some file, move along', 'readme.txt')
        sample_file2 = ContentFile('Just some file, move along', 'readme2.txt')
        in_stream = [
            {'test1': 'item', 'readme': sample_file,},
            {'test2': 'item2', 'readme': sample_file2,},
        ]
        tap.write_stream(in_stream)
        tap.close()
        
        archive = tarfile.TarFile(filename, 'r')
        payload = archive.extractfile('manifest.json').read()
        self.assertEqual([{"test1": "item", "readme": {"path": "readme.txt", "__type__": "File"}}, {"test2": "item2", "readme": {"path": "readme2.txt", "__type__": "File"}}], json.loads(payload))
    
    def test_get_item_stream_with_files(self):
        filename = mkstemp('tar', 'datataptest')[1]
        
        archive = tarfile.TarFile(filename, 'w')
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
        
        tinfo = tarfile.TarInfo('manifest.json')
        file_obj = BytesIO(json.dumps(in_stream))
        tinfo.size = len(file_obj.read())
        file_obj.seek(0)
        archive.addfile(tinfo, file_obj)
        
        tinfo = tarfile.TarInfo('assets/readme.txt')
        file_obj = BytesIO('readme1')
        tinfo.size = len(file_obj.read())
        file_obj.seek(0)
        archive.addfile(tinfo, file_obj)
        
        tinfo = tarfile.TarInfo('assets/readme2.txt')
        file_obj = BytesIO('readme2')
        tinfo.size = len(file_obj.read())
        file_obj.seek(0)
        archive.addfile(tinfo, file_obj)
        
        archive.close()
        
        tap = TarFileDataTap(filename=filename)
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
