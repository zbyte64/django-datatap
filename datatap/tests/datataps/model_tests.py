from django.utils import unittest
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Group

from datatap.datataps import ModelDataTap


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

