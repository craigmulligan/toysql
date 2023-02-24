from typing import List


def varint_decode(data: str) -> str:
    """
    given a varint encoded string
    this decodes it and returns the binary str

    for instance:

    in:   10000111 01101000
    out:   0000111  1101000
    """

    chunks = split_into_chunks(data, 8)
    res = ""

    for chunk in chunks:
        first_bit = chunk[1]
        if first_bit == 0:
            break

        res += chunk[1:]

    return res


def varint_encode(data: str, size: int) -> str:
    """
    converts a binary str into a varint encoded str

    in:   0000111  1101000
    out: 10000111 01101000
    """

    chunk_size = 7
    last_chunk_index = (size / 8) - 1
    res = ""
    chunks = split_into_chunks(data, chunk_size)

    for i, chunk in enumerate(chunks):
        first_bit = "1"

        if i == last_chunk_index:
            first_bit = "0"

        res += first_bit + chunk

    return res


def str_to_int(data: str) -> int:
    """
    converts a binary string to an int

    in:   0000111  1101000
    out:  1000
    """
    return int(data, 2)


def int_to_str(i: int, length: int) -> str:
    return format(i, "b").zfill(length)


def to_bytes(binary: str):
    return int(binary, 2).to_bytes(len(binary) // 8, byteorder="big")


def from_bytes(h: bytes):
    return format(int.from_bytes(h, byteorder="big"), "b")


def split_into_chunks(v: str, chunk_size: int) -> List[str]:
    """
    splits a string into chunk_size length chunks
    """
    return [v[i : i + chunk_size] for i in range(0, len(v), chunk_size)]


def varint_16(i: int) -> bytes:
    s = 16
    bin_str = int_to_str(i, s - 2)
    encoded = varint_encode(bin_str, s)
    return to_bytes(encoded)


def test_varint_32():
    n = 1000
    s = 16

    bin_str = int_to_str(n, s - 2)
    assert bin_str == "00001111101000"
    encoded = varint_encode(bin_str, s)
    expected_encoding = "1000011101101000"
    assert len(encoded) == len(expected_encoding)
    assert encoded == expected_encoding
    bytearr = to_bytes(encoded)
    assert bytearr == b"\x87h"
    assert str_to_int(varint_decode(from_bytes(bytearr))) == 1000


def test_varint_16():
    x = varint_16(1000)
    assert str_to_int(varint_decode(from_bytes(x))) == 1000
