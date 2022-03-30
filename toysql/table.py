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
        self.row_count = int(self.pager.__sizeof__() / ROW_SIZE)

    def insert(self, row: Row) -> Row:
        cursor = Cursor(self).table_end()
        page_num, byte_offset = self.get_position(cursor)
        page = self.pager[page_num]
        v = self.serialize_row(row)
        page[byte_offset : byte_offset + ROW_SIZE] = v
        self.pager[page_num] = page
        self.row_count += 1
        return row

    def select(self) -> List[Row]:
        cursor = Cursor(self)
        rows = []
        while not cursor.end_of_table:
            page_num, byte_offset = self.get_position(cursor)
            page = self.pager[page_num]
            row_as_bytes = page[byte_offset : byte_offset + ROW_SIZE]
            rows.append(self.deserialize_row(row_as_bytes))
            cursor.advance()

        return rows

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

    def get_position(self, cursor: Cursor) -> Tuple[int, int]:
        page_num = cursor.row_num // ROWS_PER_PAGE
        row_offset = cursor.row_num % ROWS_PER_PAGE
        byte_offset = row_offset * ROW_SIZE

        return (int(page_num), int(byte_offset))
