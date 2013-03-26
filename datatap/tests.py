from StringIO import StringIO
from tempfile import mkdtemp, mkstemp
import os
import zipfile
import json

from django.utils import unittest
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Group

from datatap.datataps import MemoryDataTap, JSONStreamDataTap, ZipFileDataTap, ModelDataTap


class MemoryDataTapTestCase(unittest.TestCase):
    def test_write_item(self):
        tap = MemoryDataTap()
        tap.open('w')
        item = {'test':'item'}
        tap.write_item(item)
        tap.close()
        self.assertEqual(item, tap.object_stream[0])
    
    def test_write_stream(self):
        tap = MemoryDataTap()
        tap.open('w')
        in_stream = [
            {'test1': 'item'},
            {'test2': 'item2'},
        ]
        tap.write_stream(in_stream)
        tap.close()
        self.assertEqual(in_stream, tap.object_stream)
    
    def test_get_item_stream(self):
        in_stream = [
            {'test1': 'item'},
            {'test2': 'item2'},
        ]
        tap = MemoryDataTap(object_stream=in_stream)
        tap.open('r')
        items = list(tap.get_item_stream())
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0], {'test1': 'item'})
        self.assertEqual(items[1], {'test2': 'item2'})
        tap.close()

class JSONStreamDataTapTestCase(unittest.TestCase):
    def test_write_item(self):
        out_stream = StringIO()
        tap = JSONStreamDataTap(stream=out_stream)
        tap.open('w')
        tap.write_item({'test':'item'})
        tap.close()
        self.assertEqual('{"test": "item"}', out_stream.getvalue())
    
    def test_write_stream(self):
        out_stream = StringIO()
        tap = JSONStreamDataTap(stream=out_stream)
        tap.open('w')
        in_stream = [
            {'test1': 'item'},
            {'test2': 'item2'},
        ]
        tap.write_stream(in_stream)
        tap.close()
        self.assertEqual('[{"test1": "item"}, {"test2": "item2"}]', out_stream.getvalue())
    
    def test_get_item_stream(self):
        in_stream = StringIO('[{"test1": "item"}, {"test2": "item2"}]')
        tap = JSONStreamDataTap(stream=in_stream)
        tap.open('r')
        items = list(tap.get_item_stream())
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0], {'test1': 'item'})
        self.assertEqual(items[1], {'test2': 'item2'})
        tap.close()

class ModelDataTapTestCase(unittest.TestCase):
    def test_write_item(self):
        tap = ModelDataTap()
        tap.open('w')
        result = tap.write_item({
            'model': 'auth.group',
            'pk':5,
            'fields': {
                'name': 'testgroup',
            }
        })
        self.assertTrue(isinstance(result, Group))
        tap.close()
    
    def test_get_item_stream(self):
        tap = ModelDataTap(ContentType, Group.objects.all())
        tap.open('r')
        items = list(tap.get_item_stream())
        self.assertTrue(items)
        self.assertEqual(len(items), ContentType.objects.all().count() + Group.objects.all().count())
        tap.close()

class ModelToJsonIntegrationTestCase(unittest.TestCase):
    def test_store(self):
        iostream = StringIO()
        outstream = JSONStreamDataTap(stream=iostream)
        outstream.open('w')
        response = ModelDataTap.store(outstream, ContentType)
        items = json.loads(iostream.getvalue())
        outstream.close()
        self.assertEqual(len(items), ContentType.objects.all().count())
    
    def test_load(self):
        Group.objects.all().delete()
        item = {
            'model': 'auth.group',
            'pk':5,
            'fields': {
                'name': 'testgroup',
            }
        }
        iostream = StringIO(json.dumps([item]))
        instream = JSONStreamDataTap(stream=iostream)
        instream.open('r')
        result = ModelDataTap.load(instream)
        instream.close()
        
        self.assertEqual(len(result), 1)
        self.assertTrue(isinstance(result[0], Group))
        self.assertEqual(result[0].name, 'testgroup')

class ZipFileDataTapTestCase(unittest.TestCase):
    def test_store(self):
        #outdir = mkdtemp('datatap-testcase')
        #filename = os.path.join(outdir, 'zipfiletest.zip')
        filename = mkstemp('zip', 'datataptest')[1]
        outstream = ZipFileDataTap(filename=filename)
        outstream.open('w')
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
        instream.open('r')
        result = ModelDataTap.load(instream)
        instream.close()
        
        self.assertEqual(len(result), 1)
        self.assertTrue(isinstance(result[0], Group))
        self.assertEqual(result[0].name, 'testgroup')

