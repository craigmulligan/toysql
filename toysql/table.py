from typing import Tuple, List, Any
from toysql.pager import Pager
from toysql.constants import *
from toysql.btree import Cursor, TableLike
from toysql.btree import Tree
import toysql.datatypes as datatypes


Row = Any


class Table(TableLike):
    # support two operations: inserting a row and printing all rows
    # reside only in memory (no persistence to disk)
    # support a single, hard-coded table

    def __init__(self, file_path: str):
        self.pager = Pager(file_path, page_size=PAGE_SIZE)
        self.root_page_num = 0
        self.tree = Tree(self, self.pager)
        self.schema = [
            datatypes.Integer(),
            datatypes.String(USERNAME_SIZE),
            datatypes.String(EMAIL_SIZE),
        ]
        self.primary_key = self.schema[0]

    def insert(self, row: Row) -> Row:
        self.tree.insert(row[0], self.serialize_row(row))
        return row

    def select(self) -> List[Row]:
        cursor = Cursor(self)
        rows = []
        while not cursor.end_of_table:
            node = cursor.get_node(cursor.page_num)
            for i in range(node.leaf_node_num_cells()):
                row_as_bytes = node.get_cell(i).value
                rows.append(self.deserialize_row(row_as_bytes))

            cursor.advance()

        return rows

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
