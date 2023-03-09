from toysql.datatypes import (
    Int8,
    Int16,
    Int32,
    UInt8,
    UInt16,
    UInt32,
    VarInt,
    VarInt8,
    VarInt16,
    VarInt32,
)
from io import BytesIO


class TestGroupFixedInt:
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


class TestGroupVarInt:
    def test_varint_encode(self):
        i = 1000
        width = 16
        # First convert it to binary with
        # one less bit per byte (14)
        binary_str = VarInt.int_to_str(i, width - 2)
        assert binary_str == "00001111101000"
        assert len(binary_str) == 14
        # Next split it into 7 bit chunks.
        chunks = VarInt.chunk(binary_str)
        assert chunks == ["0000111", "1101000"]
        # Next we prepend a flag bit to each chunk
        # to indicate wether there are more chunks succeeding the flag.
        encoded = VarInt.encode(chunks, width)
        assert encoded == "1000011101101000"
        # Finally we need to convert these bits to hexidecimal bytes.
        assert VarInt.binary_to_hex(encoded, width) == b"\x87h"

    def test_varint_8(self):
        i = VarInt8.to_bytes(14)
        assert i == b"\x0e"
        assert 14 == VarInt8.from_bytes(BytesIO(i))

    def test_varint_16(self):
        i = VarInt16.to_bytes(1000)
        assert i == b"\x87h"
        assert 1000 == VarInt16.from_bytes(BytesIO(i))

    def test_varint_32(self):
        i = VarInt32.to_bytes(200)
        assert i == b"\x80\x80\x81H"
        assert 200 == VarInt32.from_bytes(BytesIO(i))
