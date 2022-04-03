from typing import Tuple, List
from toysql.pager import Pager, Cursor, TableLike
from toysql.constants import *


Row = Tuple[int, str, str]


class Table(TableLike):
    # support two operations: inserting a row and printing all rows
    # reside only in memory (no persistence to disk)
    # support a single, hard-coded table

    def __init__(self, file_path: str):
        self.pager = Pager(file_path, page_size=PAGE_SIZE)
        self.root_page_num = 0

    def insert(self, row: Row) -> Row:
        cursor = Cursor(self).table_end()
        node = cursor.get_node(cursor.page_num)
        num_cells = node.leaf_node_num_cells()

        if num_cells >= LEAF_NODE_MAX_CELLS:
            raise Exception("Need to implement splitting a leaf node")

        if cursor.cell_num < num_cells:
            pass

        page = node.insert_cell(
            cursor, self.serialize_key(row[0]), self.serialize_row(row)
        )
        self.pager[cursor.page_num] = page
        return row

    def select(self) -> List[Row]:
        cursor = Cursor(self)
        rows = []
        while not cursor.end_of_table:
            node = cursor.get_node(cursor.page_num)
            for i in range(node.leaf_node_num_cells()):
                row_as_bytes = node.cell_value(i)
                rows.append(self.deserialize_row(row_as_bytes))

            cursor.advance()

        return rows

    def serialize_key(self, key: int) -> bytearray:
        return bytearray(key.to_bytes(LEAF_NODE_KEY_SIZE, BYTE_ORDER))

    def serialize_row(self, row: Row) -> bytearray:
        id, username, email = row
        id_bytes = bytearray(id.to_bytes(4, BYTE_ORDER))
        username_bytes = bytearray(username.encode("utf-8").ljust(USERNAME_SIZE, b"\0"))
        email_bytes = bytearray(email.encode("utf-8").ljust(EMAIL_SIZE, b"\0"))

        return id_bytes + username_bytes + email_bytes

    def deserialize_row(self, page: bytearray) -> Row:
        id = int.from_bytes(page[ID_OFFSET:ID_SIZE], BYTE_ORDER)
        username = page[USERNAME_OFFSET:USERNAME_SIZE].decode("utf-8").rstrip("\x00")
        email = page[EMAIL_OFFSET:EMAIL_SIZE].decode("utf-8").rstrip("\x00")

        return (id, username, email)
