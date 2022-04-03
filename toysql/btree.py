from enum import Enum
from toysql.constants import *


class BTree:
    pass


class NodeType:
    internal = 0
    leaf = 1


class Node:
    def __init__(self, page: bytearray):
        if page is None or len(page) == 0:
            page = bytearray(b"".ljust(PAGE_SIZE, b"\0"))
        self.page = page

    def read_content(self, start, length):
        return self.page[start : start + length]

    def write_content(self, start: int, length: int, content: bytes):
        page = self.page

        page[start : start + length] = content
        self.page = page

    def leaf_node_num_cells(self):
        num_cells_header = self.read_content(
            LEAF_NODE_NUM_CELLS_OFFSET, LEAF_NODE_NUM_CELLS_SIZE
        )
        return int.from_bytes(num_cells_header, BYTE_ORDER)

    def set_num_cells(self, num):
        num_cells_header = num.to_bytes(LEAF_NODE_NUM_CELLS_SIZE, BYTE_ORDER)
        self.write_content(
            LEAF_NODE_NUM_CELLS_OFFSET, LEAF_NODE_NUM_CELLS_SIZE, num_cells_header
        )

    def leaf_node_cell(self, cell_num):
        return LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE)

    def leaf_node_key(self, cell_num):
        return self.leaf_node_cell(cell_num)

    def leaf_node_value(self, cell_num):
        return self.leaf_node_cell(cell_num) + LEAF_NODE_KEY_SIZE

    def cell_offset(self, cell_num):
        return LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE)

    def cell_value(self, cell_num):
        """returns a row"""
        cell_offset = self.leaf_node_cell(cell_num)
        # Just return the value
        return self.read_content(cell_offset + LEAF_NODE_KEY_SIZE, LEAF_NODE_VALUE_SIZE)

    def insert_cell(self, cursor, key_as_bytes, row_as_bytes):
        num_cells = self.leaf_node_num_cells()
        if num_cells >= LEAF_NODE_MAX_CELLS:
            raise Exception("Need to implement splitting a leaf node")

        if cursor.cell_num < num_cells:
            raise Exception("dont know")

        cell_offset = self.cell_offset(cursor.cell_num)
        self.write_content(
            cell_offset, LEAF_NODE_CELL_SIZE, key_as_bytes + row_as_bytes
        )
        # print(b"fred@flintstone.com" in self.page[10:305])
        self.set_num_cells(num_cells + 1)
        return self.page
