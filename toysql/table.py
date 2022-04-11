from typing import Tuple, List, Any
from toysql.pager import Pager
from toysql.constants import *
from toysql.btree import Cursor, TableLike, Node
from toysql.exceptions import DuplicateKeyException
import toysql.datatypes as datatypes


Row = Any


class Table(TableLike):
    # support two operations: inserting a row and printing all rows
    # reside only in memory (no persistence to disk)
    # support a single, hard-coded table

    def __init__(self, file_path: str):
        self.pager = Pager(file_path, page_size=PAGE_SIZE)
        self.root_page_num = 0
        self.schema = [
            datatypes.Integer(),
            datatypes.String(USERNAME_SIZE),
            datatypes.String(EMAIL_SIZE),
        ]
        self.primary_key = self.schema[0]

    def insert(self, row: Row) -> Row:
        root_node = self.get_root_node()
        num_cells = root_node.leaf_node_num_cells()

        # if num_cells >= LEAF_NODE_MAX_CELLS:
        #     raise Exception("Need to implement splitting a leaf node")

        key_to_insert = row[0]
        cursor = root_node.find_cell(self, key_to_insert)
        if cursor.cell_num < num_cells:
            key_at_index = root_node.get_cell(cursor.cell_num).key
            if key_at_index == key_to_insert:
                raise DuplicateKeyException(
                    f"{key_to_insert} key already exists in {self}"
                )

        page = root_node.insert_cell(cursor, row[0], self.serialize_row(row))
        self.pager[cursor.page_num] = page
        return row

    def get_root_node(self):
        page = self.pager[self.root_page_num]
        return Node(page)

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
