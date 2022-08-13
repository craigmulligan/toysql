from enum import Enum, auto

class DataType(Enum):
    """
    NULL. The value is a NULL value.
    INTEGER. The value is a signed integer, stored in 0, 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
    REAL. The value is a floating point value, stored as an 8-byte IEEE floating point number.
    TEXT. The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
    BLOB. The value is a blob of data, stored exactly as it was input.
    """
    NULL = auto()
    INTEGER = auto()
    TEXT = auto()
    BLOB = auto()


class Integer():
    """
    Uses variable integer encoding

    see: https://fly.io/blog/sqlite-internals-btree/#the-header-variable-length-integers
    Logic copied from: https://github.com/fmoo/python-varint/blob/master/varint.py
    """
    def __init__(self, value) -> None:
        self.value = value
        self.content_length = self.get_content_length(value)
        self.byte_order = "little"

    def get_content_length(self, value):
        if value == 0:
            return 0 

        if value == 1:
            return 0

        if value < 255:
            # Value is an 8-bit twos-complement integer.
            return 1

        raise NotImplemented("Number greater than 255 used for type Integer")

    def to_bytes(self):
        """
        Pack `value` into varint bytes
        """

        buf = b''
        number = self.value
        if self.content_length == 0:
            return buf

        while True:
            towrite = number & 0x7f
            number >>= 7
            if number:
                buf += bytes((towrite | 0x80, ))
            else:
                buf += bytes((towrite, ))
                break

        return buf

    @staticmethod
    def from_bytes(value):
        """Read a varint from bytes"""
        shift = 0
        result = 0
        for i in value:
            result |= (i & 0x7f) << shift
            shift += 7
            if not (i & 0x80):
                break

        return Integer(result)


class Record:
    def __init__(self, payload):
        pass 

    def to_bytes(self):
        pass

    @staticmethod
    def from_bytes(): 
        pass
