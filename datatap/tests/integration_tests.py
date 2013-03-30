from io import BytesIO
import json

from django.utils import unittest
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Group

from datatap.datataps import StreamDataTap, JSONDataTap, ModelDataTap


class ModelToJsonIntegrationTestCase(unittest.TestCase):
    def test_store(self):
        iostream = BytesIO()
        tap = StreamDataTap(JSONDataTap(ModelDataTap([ContentType])))
        tap.send(iostream)
        items = json.loads(iostream.getvalue())
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
        iostream = BytesIO(json.dumps([item]))
        tap = ModelDataTap(JSONDataTap(StreamDataTap(iostream)))
        tap.commit()
        
        self.assertTrue(Group.objects.filter(pk=5).exists())
        self.assertEqual(Group.objects.get(pk=5).name, 'testgroup')
