from typing import Iterable, Dict
from toysql.btree import BTree
from toysql.record import Record, DataType


class Table:
    """
    A table is a simple wrapper around a
    table BTree and it's metadata from the system "schema"
    table.
    """

    def __init__(self, name: str, input: str, btree: BTree):
        self.name = name
        self.input = input
        self.btree = btree
        self.tree = btree

    def insert(self, record) -> Record:
        self.tree.insert(record)
        return record

    def select(self) -> Iterable[Record]:
        return self.tree.scan()
