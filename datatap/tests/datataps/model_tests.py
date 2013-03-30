from django.utils import unittest
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Group

from datatap.datataps import MemoryDataTap, ModelDataTap


class ModelDataTapTestCase(unittest.TestCase):
    def test_load_item(self):
        Group.objects.all().delete()
        source = MemoryDataTap([{
            'model': 'auth.group',
            'pk':5,
            'fields': {
                'name': 'testgroup',
            }
        }])
        tap = ModelDataTap(instream=source)
        #by default we get deserialized objects that have yet to be saved
        result = list(tap)
        self.assertTrue(len(result), 1)
        self.assertTrue(hasattr(result[0], 'save'))
        tap.commit() #this saves said objects
        tap.close()
        self.assertTrue(Group.objects.filter(pk=5).exists())
    
    def test_get_item_stream(self):
        tap = ModelDataTap(instream=[ContentType, Group.objects.all()])
        items = list(tap)
        self.assertTrue(items)
        self.assertEqual(len(items), ContentType.objects.all().count() + Group.objects.all().count())
        tap.close()

