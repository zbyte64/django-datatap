from StringIO import StringIO

from django.utils import unittest

from datatap.datataps import JSONStreamDataTap


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
