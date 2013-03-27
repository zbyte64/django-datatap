from tempfile import mkstemp
import zipfile
import json

from django.utils import unittest
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Group

from datatap.datataps import ZipFileDataTap, ModelDataTap


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
        result = ModelDataTap.load(instream)
        instream.close()
        
        self.assertEqual(len(result), 1)
        self.assertTrue(isinstance(result[0], Group))
        self.assertEqual(result[0].name, 'testgroup')

