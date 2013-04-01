import zipfile
import json
import io

from django.utils import unittest
from django.core.files.base import File, ContentFile as BaseContentFile
from django.core.files.storage import DefaultStorage

from datatap.datataps import MemoryDataTap, StreamDataTap, ZipFileDataTap


class ContentFile(BaseContentFile): #for ease with Django 1.3
    def __init__(self, content, name=None):
        super(ContentFile, self).__init__(content)
        self.name = name

class ZipFileDataTapTestCase(unittest.TestCase):
    def test_store(self):
        instream = MemoryDataTap([{
            'field1':'value1',
        }])
        ziptap = ZipFileDataTap(instream)
        archive_stream = io.BytesIO()
        response = ziptap.send(archive_stream)
        archive = zipfile.ZipFile(archive_stream)
        self.assertTrue('manifest.json' in archive.namelist())
        manifest = json.load(archive.open('manifest.json', 'r'))
        self.assertEqual(len(manifest), 1)
    
    def test_load(self):
        item = {
            'field1': 'value1',
        }
        archive_stream = io.BytesIO()
        archive = zipfile.ZipFile(archive_stream, 'w')
        archive.writestr('manifest.json', json.dumps([item]))
        archive.close()
        
        intap = MemoryDataTap(ZipFileDataTap(StreamDataTap(archive_stream)))
        result = list(intap)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['field1'], 'value1')
    
    def test_store_with_file(self):
        sample_file = ContentFile('Just some file, move along', 'readme.txt')
        payload = [{
            'test':'item',
            'readme':sample_file,
        }]
        ziptap = ZipFileDataTap(MemoryDataTap(payload))
        archive_stream = io.BytesIO()
        response = ziptap.send(archive_stream)
        
        archive = zipfile.ZipFile(archive_stream, 'r')
        payload = archive.read('manifest.json')
        self.assertEqual('[{"test": "item", "readme": {"path": "readme.txt", "__type__": "File", "storage_path": "readme.txt"}}]', payload)
        readme = archive.read('readme.txt')
        self.assertEqual(readme, 'Just some file, move along')
    
    def test_load_with_file(self):
        archive_stream = io.BytesIO()
        
        archive = zipfile.ZipFile(archive_stream, 'w')
        in_stream = [
            {'test1': 'item', 'readme': 
                {'__type__':'File',
                 'storage_path':'assets/readme.txt',
                 'path':'assets/readme.txt',}
            },
            {'test2': 'item2', 'readme': 
                {'__type__':'File',
                 'storage_path':'assets/readme2.txt',
                 'path':'assets/readme2.txt',}
            },
        ]
        archive.writestr('manifest.json', json.dumps(in_stream))
        archive.writestr('assets/readme.txt', 'readme1')
        archive.writestr('assets/readme2.txt', 'readme2')
        archive.close()
        
        archive = zipfile.ZipFile(archive_stream, 'r')
        archive.testzip()
        archive.close()
        
        tap = ZipFileDataTap(StreamDataTap(archive_stream))
        items = list(tap)
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
