from toysql.record import Record, DataType, Integer, Text, Null
from unittest import TestCase


class TestRecord(TestCase):
    def test_success(self):
        payload = [
            [DataType.integer, 3],
            [DataType.integer, 124],
            [DataType.text, "Craig"],
            [DataType.null, None],
        ]
        raw_bytes = Record(payload).to_bytes()
        assert raw_bytes
        record = Record.from_bytes(raw_bytes)
        assert record.values == payload

    def test_empty(self):
        raw_bytes = Record([[DataType.integer, 3]]).to_bytes()
        assert raw_bytes
        record = Record.from_bytes(raw_bytes)
        assert record.row_id == 3

    def test_different_order(self):
        payload = [
            [DataType.integer, 124],
            [DataType.text, "Craig"],
            [DataType.integer, 3],
            [DataType.null, None],
        ]
        raw_bytes = Record(payload).to_bytes()
        assert raw_bytes
        record = Record.from_bytes(raw_bytes)
        assert record.values == payload


class TestInteger(TestCase):
    def test_varint(self):
        for value, size in [(99, 1), (201, 2), (2147483647, 5)]:
            d = Integer(value)
            assert d.value == value
            raw_bytes = d.to_bytes()
            assert Integer.from_bytes(raw_bytes).value == value
            assert len(d.to_bytes()) == size

    def test_varint_offset(self):
        """
        We we are decoding we likely don't know how many bites to read.
        When reading we should set the correct offset.
        """
        buf = b"\xff\xff\xff\xff\x07\x04\xc9\x01"
        buf_values = [2147483647, 4, 201]

        for value in buf_values:
            result = Integer.from_bytes(buf)
            assert result.value == value
            buf = buf[result.content_length() :]


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
