from enum import Enum, auto
from typing import cast, Literal
from io import BytesIO

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


class Null():
    """
    Variable length text encoded to bytes as UTF-8. 
    """
    def __init__(self) -> None:
        self.value = None 

    def serial_type(self):
        return 0 

    def content_length(self):
        """
        Null is always stored as empty. 
        """
        return 0 

    def to_bytes(self):
        """
        Pack `value` into varint bytes
        """
        return b"" 

    @staticmethod
    def from_bytes():
        """Read a varint from bytes"""
        return Null()


class Text():
    """
    Variable length text encoded to bytes as UTF-8. 
    """
    def __init__(self, value) -> None:
        self.value = value

    def serial_type(self):
        # anything greater than 12 and odd is type TEXT.
        return self.content_length()

    def content_length(self):
        """
        Copied the sqllite variable length formula (n*2) + 13
        """
        n = len(self.to_bytes())
        return (n*2) + 13

    def to_bytes(self):
        """
        Pack `value` into varint bytes
        """
        return bytes(self.value, "utf-8")

    @staticmethod
    def from_bytes(value):
        """Read a varint from bytes"""
        result = value.decode("utf-8")
        return Text(result)


IntSizes = Literal[1,2,3,4,6,8]

class Integer():
    """
    Uses variable integer encoding

    Python stdlib does most of the hard work here. But you can read this post on how variable integer incoding works here:
    https://fly.io/blog/sqlite-internals-btree/#the-header-variable-length-integers

    TODO: Should handle big-endian IEEE 754-2008 64-bit floating point number.
    """
    def __init__(self, value, offset=0) -> None:
        self.value = value
        self.offset = offset

    def content_length(self) -> IntSizes:
        return cast(IntSizes, len(self.to_bytes()))

    def serial_type(self):
        serial_type_map = dict([(1, 1), (2, 2), (3,3), (4,4), (6, 5), (8, 6)])
        content_length = self.content_length()
        return serial_type_map[content_length]

    def to_bytes(self):
        """
        Pack `value` into varint bytes
        """
        #return self.value.to_bytes(self.value.bit_length()//8 + 1, "little", signed=True)
        number = self.value
        buf = b''
        while True:
            towrite = number & 0x7f
            number >>= 7
            if number:
                buf += bytes((towrite | 0x80,))
            else:
                buf += bytes((towrite,))
                break
        return buf
        
    @staticmethod
    def from_bytes(value: bytes):
        """Read a varint from bytes"""
        # result = int.from_bytes(value, "little", signed=True)
        shift = 0
        result = 0
        i = 0

        for i, b in enumerate(value):
            result |= (b & 0x7f) << shift
            shift += 7
            if not (b & 0x80):
                break

        return Integer(result, offset=i)


class Record:
    def __init__(self, payload):
        self.values = payload
        self.row_id = payload[0][0]

    def to_bytes(self):
        header_data = b""
        body_data = b""

        for type, value in self.values: 
            if type == DataType.INTEGER:
                content_length = Integer(value).serial_type()
                header_data += Integer(content_length).to_bytes()
                body_data += Integer(value).to_bytes()

            if type == DataType.TEXT:
                content_length = Text(value).serial_type()
                header_data += Integer(content_length).to_bytes()
                body_data += Text(value).to_bytes()

            if type == DataType.NULL:
                content_length = Null().serial_type()
                header_data += Integer(content_length).to_bytes()
                body_data += Null().to_bytes()

        header_length = len(header_data)

        return Integer(header_length).to_bytes() + header_data + body_data

    @staticmethod
    def from_bytes(data): 
        # TODO this is a varint so it's not always necesscarily one byte. 
        header_size = Integer.from_bytes(data[0:1]).value
        header_data = data[1:header_size + 1]
        body_data = data[header_size + 1:]
        serial_types = []
        values = []

        for i in range(len(header_data)):
            b = header_data[i:i+1]
            serial_types.append(Integer.from_bytes(b).value)

        for serial_type in serial_types:
            chunk = body_data[:serial_type]

            if serial_type == 0:
                values.append([DataType.NULL, Null.from_bytes().value])

            if 0 < serial_type < 7:
                values.append([DataType.INTEGER, Integer.from_bytes(chunk).value])

            if serial_type >= 13 and (serial_type % 2) != 0:
                values.append([DataType.TEXT, Text.from_bytes(chunk).value])

            body_data =  body_data[serial_type:]

        return Record(values)
