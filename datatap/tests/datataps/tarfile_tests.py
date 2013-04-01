import tarfile
import json
import io

from django.utils import unittest
from django.core.files.base import File, ContentFile as BaseContentFile
from django.core.files.storage import DefaultStorage

from datatap.datataps.tarfile import TarFileDataTap, WritableTarExtFile
from datatap.datataps import MemoryDataTap, StreamDataTap


class ContentFile(BaseContentFile): #for ease with Django 1.3
    def __init__(self, content, name=None):
        super(ContentFile, self).__init__(content)
        self.name = name

class TarFileDataTapTestCase(unittest.TestCase):
    def test_store(self):
        instream = MemoryDataTap([{
            'field1':'value1',
        }])
        tartap = TarFileDataTap(instream)
        archive_stream = io.BytesIO()
        response = tartap.send(archive_stream)
        
        archive_stream.seek(0)
        archive = tarfile.TarFile(fileobj=archive_stream)
        
        self.assertTrue('manifest.json' in archive.getnames())
        manifest = json.load(archive.extractfile('manifest.json'))
        self.assertEqual(len(manifest), 1)
    
    def test_load(self):
        item = {
            'field1': 'value1',
        }
        archive_stream = io.BytesIO()
        archive = tarfile.TarFile(fileobj=archive_stream, mode='w')
        payload = json.dumps([item])
        tarextfile = WritableTarExtFile(archive, 'manifest.json', payload)
        tarextfile.save()
        archive.close()
        
        archive_stream.seek(0)
        archive = tarfile.TarFile(fileobj=archive_stream)
        self.assertTrue('manifest.json' in archive.getnames(), "Apparently I don't know how to make proper tarfiles")
        
        archive_stream.seek(0)
        intap = MemoryDataTap(TarFileDataTap(StreamDataTap(archive_stream)))
        result = list(intap)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['field1'], 'value1')
    
    def test_store_with_file(self):
        sample_file = ContentFile('Just some file, move along', 'readme.txt')
        payload = [{
            'test':'item',
            'readme':sample_file,
        }]
        tartap = TarFileDataTap(MemoryDataTap(payload))
        archive_stream = io.BytesIO()
        response = tartap.send(archive_stream)
        
        archive_stream.seek(0)
        archive = tarfile.TarFile(fileobj=archive_stream)
        payload = archive.extractfile('manifest.json').read()
        self.assertEqual('[{"test": "item", "readme": {"path": "readme.txt", "__type__": "File", "storage_path": "readme.txt"}}]', payload)
        readme = archive.extractfile('readme.txt').read()
        self.assertEqual(readme, 'Just some file, move along')
    
    def test_load_with_file(self):
        archive_stream = io.BytesIO()
        
        archive = tarfile.TarFile(fileobj=archive_stream, mode='w')
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
        tarextfile = WritableTarExtFile(archive, 'manifest.json', json.dumps(in_stream))
        tarextfile.save()
        
        tarextfile = WritableTarExtFile(archive, 'assets/readme.txt', 'readme1')
        tarextfile.save()
        
        tarextfile = WritableTarExtFile(archive, 'assets/readme2.txt', 'readme2')
        tarextfile.save()
        archive.close()
        
        archive_stream.seek(0)
        tap = TarFileDataTap(StreamDataTap(archive_stream))
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
