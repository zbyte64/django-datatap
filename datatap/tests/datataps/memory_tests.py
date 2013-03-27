from django.utils import unittest

from datatap.datataps import MemoryDataTap


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
