from StringIO import StringIO

from django.utils import unittest
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Group

from datatap.datataps import MemoryDataTap, JSONStreamDataTap, ModelDataTap


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

'''
Example usage:

    #with django models
    outstream = JSONDataTap(stream=sys.stdout)
    outstream.open('w')
    ModelDataTap.store(outstream, MyModel, User.objects.filter(is_active=True))
    outstream.close()
    
    instream = JSONDataTap(stream=open('fixture.json', 'r'))
    ModelDataTap.load(instream)
    
    
    #with hyperadmin resources
    outstream = JSONDataTap(stream=sys.stdout)
    outstream.open('w')
    ResourceDataTap.store(outstream, MyResource)
    outstream.close()
    
    instream = JSONDataTap(stream=open('fixture.json', 'r'))
    ResourceDataTap.load(instream)
    
    #or with substitutions
    instream = JSONDataTap(stream=open('fixture.json', 'r'))
    ResourceDataTap.load(instream, mapping={'myresource_resource':'target_resource'})
'''
