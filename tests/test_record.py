from toysql.record import Record, DataType, Integer, Text, Null
from unittest import TestCase

class TestRecord(TestCase):
    def test_success(self):
        payload = [
            [DataType.INTEGER, 3],
            [DataType.INTEGER, 124],
            [DataType.TEXT, "Craig"],
            [DataType.NULL, None]
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


class TestText(TestCase):
    def test_vartext(self):
        value = "Hello world"
        s = Text(value)
        assert s.value == value 
        raw_bytes = s.to_bytes()
        assert Text.from_bytes(raw_bytes).value == value 
        assert s.content_length() == 35 


class TestNull(TestCase):
    def test_null(self):
        s = Null()
        assert s.value is None 
        raw_bytes = s.to_bytes()
        assert len(raw_bytes) == 0
        assert Null.from_bytes().value is None 
        assert s.content_length() == 0 
