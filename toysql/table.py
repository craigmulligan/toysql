from typing import List, Any

from toysql.pager import Pager
from toysql.constants import *
from toysql.tree import BPlusTree
from toysql.lexer import Token
import toysql.datatypes as datatypes

Row = Any


class Table:
    # support two operations: inserting a row and printing all rows
    # currently supports a single, hard-coded table

    # Table should take a pager
    def __init__(self, pager: Pager):
        self.pager = pager
        self.root_page_num = 0
        self.schema: List[datatypes.DataType] = [
            datatypes.Integer(),
            datatypes.String(USERNAME_SIZE),
            datatypes.String(EMAIL_SIZE),
        ]
        self.primary_key = self.schema[0]
        self.tree = BPlusTree(self)

    def insert(self, tokens: List[Token]) -> Row:
        row = [token.value for token in tokens]
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
            l += len(s)

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
