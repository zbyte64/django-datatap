from django.utils import unittest

from datatap.datataps import MemoryDataTap


class MemoryDataTapTestCase(unittest.TestCase):
    def test_get_item_stream(self):
        in_stream = [
            {'test1': 'item'},
            {'test2': 'item2'},
        ]
        tap = MemoryDataTap(instream=in_stream)
        items = list(tap)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0], {'test1': 'item'})
        self.assertEqual(items[1], {'test2': 'item2'})
        tap.close()
