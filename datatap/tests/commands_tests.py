from tempfile import mkstemp
import zipfile
import json

from django.utils import unittest
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Group

from datatap.management.commands import dumpdatatap, loaddatatap


class ModelToZipCommandIntregrationTestCase(unittest.TestCase):
    def test_dumpdatatap(self):
        filename = mkstemp('zip', 'datataptest')[1]
        cmd = dumpdatatap.Command()
        argv = ['manage.py', 'dumpdatatap', 'Model', 'contenttypes', '--', 'ZipFile', '--file', filename]
        cmd.run_from_argv(argv)
        
        archive = zipfile.ZipFile(filename)
        self.assertTrue('manifest.json' in archive.namelist())
        manifest = json.load(archive.open('manifest.json', 'r'))
        self.assertEqual(len(manifest), ContentType.objects.all().count())
    
    def test_loaddatatap(self):
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
        archive.writestr('originator.txt', 'Model')
        archive.close()
        
        cmd = loaddatatap.Command()
        argv = ['manage.py', 'loaddatatap', 'ZipFile', '--file', filename]
        cmd.run_from_argv(argv)
        
        result = Group.objects.all()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, 'testgroup')