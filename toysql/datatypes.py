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

    size is the number of bits

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


def to_bytes(binary: str, size: int):
    """
    converts a binary string into hex bytes

    in: 1000011101101000
    out: b'\x87h'
    """
    return int(binary, 2).to_bytes(size // 8, byteorder="big")


def from_bytes(h: bytes):
    return format(int.from_bytes(h, byteorder="big"), "b")


def split_into_chunks(v: str, chunk_size: int) -> List[str]:
    """
    splits a string into chunk_size length chunks
    """
    return [v[i : i + chunk_size] for i in range(0, len(v), chunk_size)]


def encoder(i: int, size: int):
    """
    i: is integer to encode
    size: is number of bits

    in: 1000, 8
    out: b'\x87h'
    """
    byte_count = size // 8
    bin_str = int_to_str(i, size - byte_count)

    encoded = varint_encode(bin_str, size)
    return to_bytes(encoded, size)


def decoder(s):
    return str_to_int(varint_decode(from_bytes(s)))


def varint_16(i: int) -> bytes:
    return encoder(i, 16)


def varint_8(i: int) -> bytes:
    return encoder(i, 8)


def varint_32(i: int) -> bytes:
    return encoder(i, 32)


def fixed_encode(i: int, size: int, signed=False):
    return (i).to_bytes(size // 8, byteorder="big", signed=signed)


def fixed_decode(data, signed=False):
    return int.from_bytes(data, "big", signed=signed)


def uint8(i: int):
    return fixed_encode(i, 8)


def uint16(i: int):
    return fixed_encode(i, 16)


def uint32(i: int):
    return fixed_encode(i, 32)


def int8(i: int):
    return fixed_encode(i, 8, signed=True)


def int16(i: int):
    return fixed_encode(i, 16, signed=True)


def int32(i: int):
    return fixed_encode(i, 32, signed=True)
