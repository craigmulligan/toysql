from typing import cast, Literal
from toysql.lexer import DataType
from toysql.datatypes import UInt32, UInt8, Int8, VarInt32, VarInt8
import io


class Null:
    """
    Variable length text encoded to bytes as UTF-8.
    """

    def __init__(self) -> None:
        self.value = None

    def serial_type(self):
        return DataType.null.value

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


class Text:
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
        return (n * 2) + 13

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


IntSizes = Literal[1, 2, 3, 4, 6, 8]
IntSerialType = Literal[1, 2, 3, 4, 5, 6]


class Byte:
    """
    Variable length text encoded to bytes as UTF-8.
    """

    def __init__(self, value) -> None:
        self.value = value

    def serial_type(self):
        # anything greater than 12 and odd is type TEXT.
        return DataType.byte.value

    def content_length(self):
        """
        Copied the sqllite variable length formula (n*2) + 13
        """
        return 1

    def to_bytes(self):
        """
        Pack `value` into varint bytes
        """
        return Int8.to_bytes(self.value)

    @staticmethod
    def from_bytes(value):
        """Read a varint from bytes"""
        result = value.decode("utf-8")
        return Text(result)


class Integer:
    """
    Uses variable integer encoding

    Python stdlib does most of the hard work here. But you can read this post on how variable integer incoding works here:
    https://fly.io/blog/sqlite-internals-btree/#the-header-variable-length-integers
    """

    serial_type_map = dict([(1, 1), (2, 2), (3, 3), (4, 4), (6, 5), (8, 6)])
    content_length_map = dict([(1, 1), (2, 2), (3, 3), (4, 4), (5, 6), (6, 8)])

    def __init__(self, value) -> None:
        self.value = int(value)

    def content_length(self) -> IntSizes:
        return cast(IntSizes, len(self.to_bytes()))

    def serial_type(self):
        return DataType.integer.value

    @staticmethod
    def content_length_from_serial_type(serial_type: IntSerialType) -> IntSizes:
        return Integer.content_length_map[serial_type]

    def to_bytes(self):
        """
        Pack `value` into varint bytes
        """
        number = self.value
        buf = b""
        while True:
            towrite = number & 0x7F
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
        shift = 0
        result = 0
        for b in value:
            result |= (b & 0x7F) << shift
            shift += 7
            if not (b & 0x80):
                break

        return Integer(result)


class Record:
    def __init__(self, payload, row_id=None):
        self.values = payload

        if len(payload) == 0:
            raise Exception("Empty record")

        # row_id is always the first value in row.
        if row_id is None:
            if not isinstance(payload[0][1], int):
                raise Exception("Key is not an integer")

            self.row_id = payload[0][1]
        else:
            self.row_id = row_id

    def __eq__(self, o: "Record") -> bool:
        return o.row_id == self.row_id

    def to_bytes(self):
        header_data = b""
        body_data = b""

        # ignore pk so start from 1.
        for i, [type, value] in enumerate(self.values):
            if type == DataType.integer:
                serial_type = Integer(value).serial_type()
                if i == 0:
                    # primary key
                    # header_data += varint_8(0)
                    serial_type = Null().serial_type()
                    header_data += VarInt8.to_bytes(serial_type)
                    body_data += Null().to_bytes()
                else:
                    header_data += VarInt8.to_bytes(serial_type)
                    body_data += VarInt32.to_bytes(value)

            if type == DataType.byte:
                serial_type = Byte(value).serial_type()
                header_data += VarInt8.to_bytes(serial_type)
                body_data += Byte(value).to_bytes()

            if type == DataType.text:
                serial_type = Text(value).serial_type()
                header_data += VarInt8.to_bytes(serial_type)
                body_data += Text(value).to_bytes()

            if type == DataType.null:
                serial_type = Null().serial_type()
                header_data += VarInt8.to_bytes(serial_type)
                body_data += Null().to_bytes()

        # we need to +1 here to account for the extra uint8 at the start.
        return UInt8.to_bytes(len(header_data) + 1) + header_data + body_data

    @staticmethod
    def from_bytes(data, row_id=None):
        buff = io.BytesIO(data)
        header_size = Integer.from_bytes(buff.read())

        # seek to after header_size varint inorder
        # to read the rest of the header varints
        buff.seek(header_size.content_length())

        serial_types = []
        values = []

        while True:
            cursor = buff.tell()
            serial_type = Integer.from_bytes(buff.read())
            serial_types.append(serial_type.value)
            cursor += serial_type.content_length()
            buff.seek(cursor)

            if cursor == header_size.value + 1:
                break

        for serial_type in serial_types:
            if serial_type == 0:
                values.append([DataType.null, Null.from_bytes().value])

            if 0 < serial_type < 7:
                content_length = Integer.content_length_from_serial_type(serial_type)
                values.append(
                    [
                        DataType.integer,
                        Integer.from_bytes(buff.read(content_length)).value,
                    ]
                )

            if serial_type >= 13 and (serial_type % 2) != 0:
                content_length = int((serial_type - 13) / 2)
                values.append(
                    [DataType.text, Text.from_bytes(buff.read(content_length)).value]
                )

        return Record(values, row_id=row_id)
