from typing import Literal, Any
from dataclasses import dataclass

ByteOrder = Literal["little", "big"]


@dataclass
class Boolean:
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
class Integer:
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
class String:
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
