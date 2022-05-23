from typing import Literal, TypeVar

T = TypeVar("T")

ByteOrder = Literal["little", "big"]

from typing import Protocol


class DataTypeContainer(Protocol[T]):
    def serialize(self, _value: T) -> bytearray:
        ...

    def deserialize(self, _value: bytearray) -> T:
        ...


class DataType(DataTypeContainer[T]):
    length: int
    byteorder: ByteOrder = "little"

    def __init__(self, length: int) -> None:
        self.length = length

    def __len__(self):
        return self.length

    def read(self, content: bytearray, offset: int) -> T:
        return self.deserialize(content[offset : offset + self.length])

    def write(self, content: bytearray, offset: int, value: T) -> bytearray:
        content[offset : offset + self.length] = self.serialize(value)
        return content

    def serialize(self, _value: T) -> bytearray:
        ...

    def deserialize(self, _value: bytearray) -> T:
        ...


class Boolean(DataType[bool]):
    def __init__(self) -> None:
        self.length = 1

    def serialize(self, value: bool) -> bytearray:
        return bytearray(value.to_bytes(self.length, self.byteorder))

    def deserialize(self, value: bytearray) -> bool:
        return bool(int.from_bytes(value, self.byteorder))


class Integer(DataType[int]):
    def __init__(self) -> None:
        self.length = 4

    def serialize(self, value: int) -> bytearray:
        return bytearray(value.to_bytes(self.length, self.byteorder))

    def deserialize(self, value: bytearray) -> int:
        return int.from_bytes(value, self.byteorder)


class String(DataType[str]):
    def serialize(self, value: str) -> bytearray:
        return bytearray(value.encode("utf-8").ljust(self.length, b"\0"))

    def deserialize(_self, value: bytearray) -> str:
        return value.decode("utf-8").rstrip("\x00")
