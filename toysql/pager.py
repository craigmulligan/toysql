from pathlib import Path
import os


class Pager:
    def __init__(self, file_path, page_size=4096):
        file_name = Path(file_path)
        file_name.touch(exist_ok=True)
        self.f = open(file_name, "rb+")
        self.page_size = page_size

    def __getitem__(self, i):
        self.f.seek(i * self.page_size)
        page = bytearray(self.f.read(self.page_size))
        return page

    def __setitem__(self, page_number: int, page):
        self.f.seek(page_number * self.page_size)
        self.f.write(page)
        self.f.flush()
        return page

    def __len__(self):
        return int(self.__sizeof__() / self.page_size) + 1

    def __sizeof__(self):
        self.f.seek(0, os.SEEK_END)
        return self.f.tell()


class Cursor:
    ## TODO switch this to an iterator.
    def __init__(self, table):
        self.table = table
        self.row_num = 0

    def table_end(self):
        self.row_num = self.table.row_count
        return self

    def advance(self):
        self.row_num += 1

    @property
    def end_of_table(self):
        return self.row_num >= self.table.row_count
