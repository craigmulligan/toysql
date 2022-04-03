from typing import Protocol, Any
from pathlib import Path
from toysql.btree import Node
import os


class Pager:
    """
    Abstracts the filesystem into pages.
    Everything is stored into a file. This allows you to
    get and set pages (chunks) of data.
    """

    def __init__(self, file_path: str, page_size=4096):
        file_name = Path(file_path)
        file_name.touch(exist_ok=True)
        self.f = open(file_name, "rb+")
        self.page_size = page_size
        self.num_pages = len(self)

        # TODO check for corrupt file.
        # file_length % PAGE_SIZE != 0

    def __getitem__(self, i: int) -> bytes:
        if i >= self.num_pages:
            self.num_pages += 1
        self.f.seek(i * self.page_size)
        page = bytearray(self.f.read(self.page_size))
        return page

    def __setitem__(self, page_number: int, page: bytearray):
        self.f.seek(page_number * self.page_size)
        self.f.write(page)
        self.f.flush()
        return page

    def __len__(self):
        return int(self.__sizeof__() / self.page_size)

    def __sizeof__(self):
        self.f.seek(0, os.SEEK_END)
        return self.f.tell()


class TableLike(Protocol):
    root_page_num: int
    pager: Any


class Cursor:
    def __init__(self, table: TableLike):
        self.table = table
        self.page_num = self.table.root_page_num
        self.cell_num = 0
        self.end_of_table = False
        self.table_start()

    def table_start(self):
        node = self.get_node(self.table.root_page_num)
        self.cell_num = node.leaf_node_num_cells()
        self.end_of_table = self.cell_num == 0
        return self

    def table_end(self):
        node = self.get_node(self.table.root_page_num)
        self.cell_num = node.leaf_node_num_cells()
        self.end_of_table = True
        return self

    def value(self):
        node = self.get_node(self.page_num)
        return node.leaf_node_value(self.cell_num)

    def get_node(self, page_num):
        page = self.table.pager[page_num]
        return Node(page)

    def advance(self):
        node = self.get_node(self.page_num)
        self.cell_num += 1

        if self.cell_num >= node.leaf_node_num_cells():
            self.end_of_table = True
