from StringIO import StringIO
import json

from django.utils import unittest
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Group

from datatap.datataps import JSONStreamDataTap, ModelDataTap



class ModelToJsonIntegrationTestCase(unittest.TestCase):
    def test_store(self):
        iostream = StringIO()
        outstream = JSONStreamDataTap(stream=iostream)
        outstream.open('w', for_datatap=ModelDataTap)
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
        instream.open('r', for_datatap=ModelDataTap)
        result = list(ModelDataTap.load(instream))
        instream.close()
        
        self.assertEqual(len(result), 1)
        self.assertTrue(isinstance(result[0], Group))
        self.assertEqual(result[0].name, 'testgroup')


