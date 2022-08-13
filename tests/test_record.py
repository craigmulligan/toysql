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

            assert d.value == c
            # Should return empty
            assert len(d.to_bytes()) == 0
            assert d.to_bytes() == b""

    def test_varint(self):
        value = 201
        d = Integer(value)

        assert d.value == value 
        raw_bytes = d.to_bytes()
        assert Integer.from_bytes(raw_bytes).value == value 
        assert len(d.to_bytes()) == 2

    def test_bigvarint(self):
        value = 2147483647300 
        d = Integer(value)

        assert d.value == value 
        raw_bytes = d.to_bytes()
        assert Integer.from_bytes(raw_bytes).value == value 
        assert len(d.to_bytes()) == 6
