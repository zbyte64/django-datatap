from io import BytesIO

from django.utils import unittest

from datatap.datataps import StreamDataTap, JSONDataTap, MemoryDataTap


class JSONDataTapTestCase(unittest.TestCase):
    def test_encode(self):
        out_stream = BytesIO()
        source = MemoryDataTap([{'test':'item'}])
        tap = JSONDataTap(instream=source)
        tap.send(out_stream)
        tap.close()
        self.assertEqual('[{"test": "item"}]', out_stream.getvalue())
    
    def test_decode(self):
        payload = BytesIO('[{"test1": "item"}, {"test2": "item2"}]')
        source = StreamDataTap(payload)
        tap = JSONDataTap(instream=source)
        items = list(tap)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0], {'test1': 'item'})
        self.assertEqual(items[1], {'test2': 'item2'})
        tap.close()
