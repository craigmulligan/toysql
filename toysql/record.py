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

    def content_length(self):
        return len(self.to_bytes())

    def to_bytes(self):
        """
        Pack `value` into varint bytes
        """
        return self.value.to_bytes(self.value.bit_length()//8 + 1, "little", signed=True)

    @staticmethod
    def from_bytes(value):
        """Read a varint from bytes"""
        result = int.from_bytes(value, "little", signed=True)
        return Integer(result)


class Record:
    def __init__(self, payload):
        self.values = payload

    def header(self):
        header_data = b""
        for type, value in self.values: 
            if type == DataType.INTEGER:
                content_length = Integer(value).content_length()
                header_data += Integer(content_length).to_bytes()

        header_length = len(header_data)
        return Integer(header_length).to_bytes() + header_data

    def body(self):
        body_data = b""
        for type, value in self.values: 
            if type == DataType.INTEGER:
                body_data += Integer(value).to_bytes()

        return body_data


    def to_bytes(self):
        return self.header() + self.body()

    @staticmethod
    def from_bytes(data): 
        header_size = Integer.from_bytes(data[0:1]).value
        header_data = data[1:header_size + 1]
        body_data = data[header_size + 1:]
        serial_types = []
        values = []

        for i in range(len(header_data)):
            b = header_data[i:i+1]
            serial_types.append(Integer.from_bytes(b).value)

        for type_length in serial_types:
            chunk = body_data[:type_length]
            values.append([DataType.INTEGER, Integer.from_bytes(chunk).value])
            body_data =  body_data[type_length:]

        return Record(values)
