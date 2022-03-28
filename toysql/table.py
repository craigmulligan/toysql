# class Pager:
#     def __init__(self, file_path: str):
#         # TODO use mmap?
#         self.f = open(file_path, "wb+")
#         self.page_size = 4 * 1000  # 4kb

#     def get_page(self, page_number: int):
#         self.f.seek(page_number)
#         self.f.read(self.page_size)

#     def set_page(self, page_number: int, page):
#         self.f.seek(page_number)
#         self.f.write(page)

#     def flush(self):
#         self.f.close()

#     def file_length(self):
#         return self.f.tell()


ID_SIZE = 4
USERNAME_SIZE = 32
EMAIL_SIZE = 255
ID_OFFSET = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES
BYTE_ORDER = "little"


class Table:
    # support two operations: inserting a row and printing all rows
    # reside only in memory (no persistence to disk)
    # support a single, hard-coded table

    def __init__(self):
        self.pager = []
        self.total_rows = 0

    def insert(self, row):
        page, byte_offset = self.get_position(self.total_rows)
        if len(self.pager) == page:
            # initialise new page
            self.pager.append(bytearray(b"".ljust(ROW_SIZE, b"\0")))

        self.pager[page][byte_offset:ROW_SIZE] = self.serialize_row(row)
        self.total_rows += 1

    def select(self):
        rows = []
        for row_number in range(self.total_rows):
            page_num, byte_offset = self.get_position(row_number)
            row_as_bytes = self.pager[page_num][byte_offset : byte_offset + ROW_SIZE]
            if len(row_as_bytes) == 0:
                # If the row is empty.
                continue

            rows.append(self.deserialize_row(row_as_bytes))

        return rows

    def serialize_row(self, row):
        id, username, email = row
        id_bytes = bytearray(id.to_bytes(4, BYTE_ORDER))
        username_bytes = bytearray(username.encode("utf-8").ljust(USERNAME_SIZE, b"\0"))
        email_bytes = bytearray(email.encode("utf-8").ljust(EMAIL_SIZE, b"\0"))

        return id_bytes + username_bytes + email_bytes

    def deserialize_row(self, page):
        id = int.from_bytes(page[ID_OFFSET:ID_SIZE], BYTE_ORDER)
        username = page[USERNAME_OFFSET:USERNAME_SIZE].decode("utf-8").rstrip("\x00")
        email = page[EMAIL_OFFSET:EMAIL_SIZE].decode("utf-8").rstrip("\x00")

        return (id, username, email)

    def get_position(self, row_num):
        page_num = int(row_num / ROWS_PER_PAGE)
        row_offset = int(row_num % ROWS_PER_PAGE)
        byte_offset = int(row_offset * ROW_SIZE)

        return (page_num, byte_offset)
