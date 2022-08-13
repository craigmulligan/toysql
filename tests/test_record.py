from toysql.record import Record, DataType, Integer
from unittest import TestCase



# class TestRecord(TestCase):
#     def test_success(self):
#         payload = [
#             [DataType.INTEGER, 3],
#             [DataType.TEXT, "Hello world"]
#         ]

#         record = Record(payload)
#         bytes = record.to_bytes()

#         assert bytes 


class TestInteger(TestCase):
    def test_constants(self):
        # Works in with int constants
        for c in [0, 1]:
            d = Integer(c)

            assert d.content_length == 0
            assert d.value == c
            # Should return empty
            assert d.to_bytes() == b"" 


    def test_varint(self):
        value = 201
        d = Integer(value)

        assert d.content_length == 1 
        assert d.value == value 
        raw_bytes = d.to_bytes()
        assert raw_bytes == b'\xc9\x01'
        assert Integer.from_bytes(raw_bytes).value == value 
