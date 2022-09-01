from typing import List, Any, Dict
from toysql.btree import BTree
from toysql.lexer import Token
from toysql.record import Record, DataType


class Table:
    """
    A table is a simple wrapper around a
    table BTree and it's metadata from the system "schema"
    table.
    """

    def __init__(self, name, btree: BTree):
        self.name = name
        self.btree = btree
        self.tree = btree

    def insert(self, record) -> Record:
        self.tree.insert(record)
        return record

    def select(self) -> List[Record]:
        return self.tree.scan()
