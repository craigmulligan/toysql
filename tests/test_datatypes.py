from toysql.datatypes import (
    VarInt8,
    VarInt16,
    VarInt32,
    Int8,
    Int16,
    Int32,
    UInt8,
    UInt16,
    UInt32,
)
from io import BytesIO


class FixedIntTestGroup:
    """All the fixed width integer tests"""

    def test_int_8(self):
        i = Int8.to_bytes(14)
        assert i == b"\x0e"
        assert 14 == Int8.from_bytes(BytesIO(i))

    def test_int_16(self):
        i = Int16.to_bytes(1000)
        assert i == b"\x03\xe8"
        assert 1000 == Int16.from_bytes(BytesIO(i))

    def test_int_32(self):
        i = Int32.to_bytes(200)
        assert i == b"\x00\x00\x00\xc8"
        assert 200 == Int32.from_bytes(BytesIO(i))

    def test_uint_8(self):
        i = UInt8.to_bytes(14)
        assert i == b"\x0e"
        assert 14 == Int8.from_bytes(BytesIO(i))

    def test_uint_16(self):
        i = UInt16.to_bytes(1000)
        assert i == b"\x03\xe8"
        assert 1000 == Int16.from_bytes(BytesIO(i))

    def test_uint_32(self):
        i = UInt32.to_bytes(200)
        assert i == b"\x00\x00\x00\xc8"
        assert 200 == Int32.from_bytes(BytesIO(i))


def test_varint_8():
    i = VarInt8.to_bytes(14)
    assert i == b"\x0e"
    assert 14 == VarInt8.from_bytes(BytesIO(i))


def test_varint_16():
    i = VarInt16.to_bytes(1000)
    assert i == b"\x87h"
    assert 1000 == VarInt16.from_bytes(BytesIO(i))


def test_varint_32():
    i = VarInt32.to_bytes(200)
    assert i == b"\x80\x80\x81H"
    assert 200 == VarInt32.from_bytes(BytesIO(i))
