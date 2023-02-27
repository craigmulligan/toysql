import pytest
from toysql.datatypes import (
    varint_32,
    decoder,
    int_to_str,
    varint_encode,
    varint_16,
    uint16,
    fixed_decode,
)


def test_varint_16():
    x = varint_16(1000)
    assert x == b"\x87h"
    assert 1000 == decoder(x)


def test_varint_32():
    x = varint_32(200)
    assert x == b"\x80\x80\x81H"
    assert 200 == decoder(x)


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (0, b"\x00\x03"),
        (3, b"\x91\x03"),
        (145, b"\xe3\x03"),
    ],
)
def test_uint16(test_input, expected):
    print(len(expected), int.from_bytes(expected, "little", signed=False))
    assert test_input == fixed_decode(expected)
    assert False


@pytest.mark.parametrize(
    "test_input,size,expected",
    [
        (1000, 16, "1000011101101000"),
        (1000, 32, "10000000100000001000000101111010100"),
        (42, 4, "1101010"),
        (42, 16, "1000000000101010"),
    ],
)
def test_varint(test_input, size, expected):
    bin_str = int_to_str(test_input, size - 2)
    assert varint_encode(bin_str, size) == expected
