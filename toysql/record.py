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
    Python stdlib has handy varint methods.
    """
    def __init__(self, value) -> None:
        self.value = value
        self.byte_order = "little"

    def to_bytes(self):
        """
        Pack `value` into varint bytes
        """
        if self.value == 0 or self.value == 1:
            return b"" 
        return self.value.to_bytes(self.value.bit_length()//8 + 1, 'little', signed=False)

    @staticmethod
    def from_bytes(value):
        """Read a varint from bytes"""
        result = int.from_bytes(value, 'little', signed=False)
        return Integer(result)


class Record:
    def __init__(self, payload):
        pass 

    def to_bytes(self):
        pass

    @staticmethod
    def from_bytes(): 
        pass
