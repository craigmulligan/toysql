from typing import Literal, Any
from dataclasses import dataclass

ByteOrder = Literal["little", "big"]


# TODO this should be an Abstract class
# But hitting this mypy issue: https://github.com/python/mypy/issues/5374
@dataclass
class DataType:
    length: int
    Byteorder: ByteOrder = "little"

    def __len__(self):
        return self.length

    def serialize(self, value: Any) -> bytearray:
        return bytearray()

    def deserialize(self, value: bytearray) -> Any:
        pass


@dataclass
class Boolean(DataType):
    length: int = 1
    byteorder: ByteOrder = "little"

    def serialize(self, value: bool) -> bytearray:
        return bytearray(value.to_bytes(self.length, self.byteorder))

    def deserialize(self, value) -> bool:
        return bool(int.from_bytes(value, self.byteorder))

    def read(self, content: bytearray, offset: int) -> bool:
        return bool(self.deserialize(content[offset : offset + self.length]))

    def write(self, content: bytearray, offset: int, value: bool) -> bytearray:
        content[offset : offset + self.length] = self.serialize(value)
        return content


@dataclass
class Integer(DataType):
    length: int = 4
    byteorder: ByteOrder = "little"

    def serialize(self, value: int) -> bytearray:
        return bytearray(value.to_bytes(self.length, self.byteorder))

    def deserialize(self, value: bytearray) -> int:
        return int.from_bytes(value, self.byteorder)

    def read(self, content: bytearray, offset: int) -> int:
        return self.deserialize(content[offset : offset + self.length])

    def write(self, content: bytearray, offset: int, value: int) -> bytearray:
        content[offset : offset + self.length] = self.serialize(value)
        return content


@dataclass
class String(DataType):
    # TODO implement __len__ feature.
    length: int
    byteorder: ByteOrder = "little"

    def serialize(self, value) -> bytearray:
        return bytearray(value.encode("utf-8").ljust(self.length, b"\0"))

    def deserialize(_self, value) -> str:
        return value.decode("utf-8").rstrip("\x00")

    def read(self, content: bytearray, offset: int) -> str:
        return self.deserialize(content[offset : offset + self.length])

    def write(self, content: bytearray, offset: int, value: str) -> bytearray:
        new_content = content[offset : offset + self.length]
        new_content[offset : offset + self.length] = self.serialize(value)
        return new_content
