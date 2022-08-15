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


class Null():
    """
    Variable length text encoded to bytes as UTF-8. 
    """
    def __init__(self) -> None:
        self.value = None 

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



class Integer():
    """
    Uses variable integer encoding

    Python stdlib does most of the hard work here. But you can read this post on how variable integer incoding works here:
    https://fly.io/blog/sqlite-internals-btree/#the-header-variable-length-integers
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

            if type == DataType.TEXT:
                content_length = Text(value).content_length()
                header_data += Integer(content_length).to_bytes()

            if type == DataType.NULL:
                content_length = Null().content_length()
                header_data += Integer(content_length).to_bytes()

        header_length = len(header_data)
        return Integer(header_length).to_bytes() + header_data

    def body(self):
        body_data = b""
        for type, value in self.values: 
            if type == DataType.INTEGER:
                body_data += Integer(value).to_bytes()

            if type == DataType.TEXT:
                body_data += Text(value).to_bytes()

            if type == DataType.NULL:
                body_data += Null().to_bytes()

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