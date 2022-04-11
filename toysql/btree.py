from typing import Protocol, Any
from enum import Enum
from toysql.constants import *
from toysql.datatypes import Integer


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
        self.cell_key = Integer()
        self.num_cells_header = Integer()

    def read_content(self, start, length):
        return self.page[start : start + length]

    def write_content(self, start: int, length: int, content: bytes):
        page = self.page
        page[start : start + length] = content
        self.page = page

    def leaf_node_num_cells(self):
        return self.num_cells_header.read(self.page, LEAF_NODE_NUM_CELLS_OFFSET)

    def set_num_cells(self, num):
        page = self.num_cells_header.write(self.page, LEAF_NODE_NUM_CELLS_OFFSET, num)
        self.page = page
        return page

    def leaf_node_cell(self, cell_num):
        return LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE)

    def leaf_node_key(self, cell_num):
        return self.leaf_node_cell(cell_num)

    def leaf_node_value(self, cell_num):
        return self.leaf_node_cell(cell_num) + LEAF_NODE_KEY_SIZE

    def cell_offset(self, cell_num):
        return LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE)

    def get_cell_key(self, cell_num):
        cell_offset = self.leaf_node_cell(cell_num)
        return self.cell_key.read(self.page, cell_offset)

    def cell_value(self, cell_num):
        """returns a row"""
        cell_offset = self.leaf_node_cell(cell_num)
        # Just return the value
        return self.read_content(cell_offset + LEAF_NODE_KEY_SIZE, LEAF_NODE_VALUE_SIZE)

    def leaf_node_split_and_insert(self, cursor, key, cell_value):
        """
        Create a new node and move half the cells over.
        Insert the new value in one of the two nodes.
        Update parent or create a new parent.
        """
        old_node = cursor.get_node(cursor.page_num)
        new_page_num = len(cursor.table.pager)
        new_node = cursor.get_node(new_page_num)

        # /*
        # All existing keys plus new key should be divided
        # evenly between old (left) and new (right) nodes.
        # Starting from the right, move each key to correct position.
        # */
        for i in range(LEAF_NODE_MAX_CELLS):
            if i >= LEAF_NODE_LEFT_SPLIT_COUNT:
                destination_node = new_node
            else:
                destination_node = old_node

            index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT
            destination = destination_node.leaf_node_cell(
                destination_node, index_within_node
            )

            if i == cursor.cell_num:
                pass
                # serialize_row(value, destination)
            elif i > cursor.cell_num:
                pass
                # memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE)
            else:
                pass
                # memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE)

    def insert_cell(self, cursor, key, cell_value):
        num_cells = self.leaf_node_num_cells()
        if num_cells >= LEAF_NODE_MAX_CELLS:
            raise Exception("Need to implement splitting a leaf node")

        if cursor.cell_num < num_cells:
            # Node full
            raise Exception(
                f"cursor.cell_num: {cursor.cell_num} < num_cells: {num_cells}"
            )

        cell_offset = self.cell_offset(cursor.cell_num)
        self.write_content(
            cell_offset,
            LEAF_NODE_CELL_SIZE,
            cursor.table.primary_key.serialize(key) + cell_value,
        )
        self.set_num_cells(num_cells + 1)
        return self.page

    def find_cell(self, table, key: int):
        num_cells = self.leaf_node_num_cells()
        cursor = Cursor(table)
        min_index = 0
        one_past_max_index = num_cells

        while one_past_max_index != min_index:
            index = int((min_index + one_past_max_index) / 2)
            key_at_index = self.get_cell_key(index)

            if key == key_at_index:
                cursor.cell_num = index
                return cursor
            if key < key_at_index:
                one_past_max_index = index
            else:
                min_index = index + 1

        cursor.cell_num = min_index
        return cursor


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
