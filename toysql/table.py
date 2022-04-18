from typing import Tuple, List, Any
from toysql.pager import Pager
from toysql.constants import *
from toysql.btree import Cursor, TableLike
from toysql.tree import BPlusTree

# from toysql.btree import Tree
import toysql.datatypes as datatypes


Row = Any


class Table(TableLike):
    # support two operations: inserting a row and printing all rows
    # currently supports a single, hard-coded table

    def __init__(self, file_path: str):
        self.pager = Pager(file_path, page_size=PAGE_SIZE)
        self.root_page_num = 0
        self.schema = [
            datatypes.Integer(),
            datatypes.String(USERNAME_SIZE),
            datatypes.String(EMAIL_SIZE),
        ]
        self.primary_key = self.schema[0]
        self.tree = BPlusTree(self)

    def insert(self, row: Row) -> Row:
        self.tree.insert(row[0], row)
        return row

    def select(self) -> List[Row]:
        result = []
        for r in self.tree.traverse():
            [_, value] = r
            result.append(value)

        return result

    def row_length(self):
        """
        Returns the length of the row as stored bytes
        """
        l = 0
        for s in self.schema:
            l += s.length

        return l

    def serialize_row(self, row: Row) -> bytearray:
        cell = bytearray()
        for i, datatype in enumerate(self.schema):
            cell += datatype.serialize(row[i])

        return cell

    def deserialize_row(self, cell: bytearray) -> Row:
        row = []
        index = 0
        for datatype in self.schema:
            value = datatype.deserialize(cell[index : datatype.length])
            row.append(value)
            index += datatype.length

        return tuple(row)
