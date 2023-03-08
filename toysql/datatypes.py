from typing import List, Type
from io import BytesIO


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
    last_chunk_index = (size // 8) - 1
    print("last_chunk_index", last_chunk_index)
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


def to_bytes(binary: str, size: int):
    """
    converts a binary string into hex bytes

    in: 1000011101101000
    out: b'\x87h'
    """
    return int(binary, 2).to_bytes(size // 8, byteorder="big")


def from_bytes(h: bytes):
    return format(int.from_bytes(h, byteorder="big"), "08b")


def split_into_chunks(v: str, chunk_size: int) -> List[str]:
    """
    splits a string into chunk_size width chunks
    """
    return [v[i : i + chunk_size] for i in range(0, len(v), chunk_size)]


class FixedInt:
    width = 0
    signed = False

    @classmethod
    def to_bytes(cls: Type["FixedInt"], i: int) -> bytes:
        return (i).to_bytes(cls.width // 8, byteorder="big", signed=cls.signed)

    @classmethod
    def from_bytes(cls: Type["FixedInt"], buffer: BytesIO) -> int:
        return int.from_bytes(buffer.read(cls.width // 8), "big")


class Int32(FixedInt):
    width = 32
    signed = True


class Int16(FixedInt):
    width = 16
    signed = True


class Int8(FixedInt):
    width = 8
    signed = True


class UInt32(FixedInt):
    width = 32
    signed = False


class UInt16(FixedInt):
    width = 16
    signed = False


class UInt8(FixedInt):
    width = 8
    signed = False


class VarInt:
    width = 0

    @staticmethod
    def str_to_int(data: str) -> int:
        """
        converts a binary string to an int

        in:   0000111  1101000
        out:  1000
        """
        return int(data, 2)

    @staticmethod
    def int_to_str(i: int, width: int) -> str:
        """
        Converts an int a string
        """
        return format(i, f"0{width}b")

    @classmethod
    def bytes_to_str(cls: Type["VarInt"], h: bytes) -> str:
        """
        converts bytes object to binary string
        """
        return format(int.from_bytes(h, byteorder="big"), f"0{cls.width}b")

    @classmethod
    def to_bytes(cls: Type["VarInt"], i: int) -> bytes:
        """
        in: 1000, 8
        out: b'\x87h'
        """
        byte_count = cls.width // 8
        bin_str = cls.int_to_str(i, cls.width - byte_count)

        encoded = varint_encode(bin_str, cls.width)
        return to_bytes(encoded, cls.width)

    @classmethod
    def from_bytes(cls: Type["VarInt"], buffer: BytesIO) -> int:
        y = cls.bytes_to_str(buffer.read(cls.width // 8))
        z = varint_decode(y)
        x = str_to_int(z)
        return x


class VarInt8(VarInt):
    width = 8


class VarInt16(VarInt):
    width = 16


class VarInt32(VarInt):
    width = 32
