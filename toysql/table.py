from typing import Iterable
from toysql.btree import BTree, Cursor
from toysql.record import Record


class Table:
    """
    A table is a simple wrapper around a
    table BTree and it's metadata from the system "schema"
    table.
    """

    def __init__(self, name: str, input: str, btree: BTree):
        self.name = name
        self.input = input
        self.tree = btree
        self.cursor = Cursor(btree)

    def insert(self, record) -> Record:
        self.cursor.insert(record)
        return record

    def select(self) -> Iterable[Record]:
        return self.tree.scan()
