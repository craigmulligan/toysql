from toysql.record import Record, DataType, Integer
from unittest import TestCase

class TestRecord(TestCase):
    def test_success(self):
        payload = [
            [DataType.INTEGER, 3],
            [DataType.INTEGER, 124]
        ]
        raw_bytes = Record(payload).to_bytes()
        assert raw_bytes
        record = Record.from_bytes(raw_bytes)
        assert record.values == payload 


class TestInteger(TestCase):
    def test_varint(self):
        for value, size in [(99, 1), (201, 2), (2147483647, 4)]:
            d = Integer(value)
            assert d.value == value 
            raw_bytes = d.to_bytes()
            assert Integer.from_bytes(raw_bytes).value == value 
            assert len(d.to_bytes()) == size
