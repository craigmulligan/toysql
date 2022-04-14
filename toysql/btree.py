from typing import Protocol, Any
from toysql.constants import *
from dataclasses import dataclass
import toysql.datatypes as datatypes
from toysql.exceptions import DuplicateKeyException


class Tree:
    def __init__(self, table, pager) -> None:
        self.table = table
        self.pager = pager
        self.root_node = self.get_node(self.table.root_page_num)

    def get_node(self, page_num):
        return Node(self.pager[page_num])

    def create_new_root(self, right_child_page_num):
        """
        Handle splitting the root.
        Old root copied to new page, becomes left child.
        Address of right child passed in.
        Re-initialize root page to contain the new root node.
        New root node points to two children.
        """

        root = self.get_node(self.table.root_page_num)
        right_child = self.get_node(right_child_page_num)
        # left_child_page_num = get_unused_page_num(table->pager);
        # left_child = get_page(table->pager, left_child_page_num);

    def split_and_insert(self, cursor, key, cell_value):
        """
        Create a new node and move half the cells over.
        Insert the new value in one of the two nodes.
        Update parent or create a new parent.
        """
        old_node = self.get_node(cursor.page_num)
        new_page_num = len(self.pager)
        new_node = self.get_node(new_page_num)

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

            index_within_node = int(i % LEAF_NODE_LEFT_SPLIT_COUNT)
            cell = Cell(index_within_node, destination_node.page)

            cell.write(key, cell_value)
            old_node.set_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT)
            new_node.set_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT)

            if old_node.is_root():
                self.root_node = self.get_node(self.table.root_page_num)

            # if i == cursor.cell_num:
            #     pass
            #     # serialize_row(value, destination)
            # elif i > cursor.cell_num:
            #     pass
            #     # memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE)
            # else:
            #     pass
            #     # memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE)

    def insert(self, key_to_insert, row: bytearray):
        root_node = self.root_node
        num_cells = root_node.leaf_node_num_cells()
        cursor = self.get_cursor(key_to_insert)

        if num_cells >= LEAF_NODE_MAX_CELLS:
            self.split_and_insert(cursor, key_to_insert, row)
            raise Exception("Need to implement splitting a leaf node")

        if cursor.cell_num < num_cells:
            key_at_index = root_node.get_cell(cursor.cell_num).key
            if key_at_index == key_to_insert:
                raise DuplicateKeyException(
                    f"{key_to_insert} key already exists in {self}"
                )

        page = root_node.insert_cell(cursor, key_to_insert, row)
        self.pager[cursor.page_num] = page

    def get_cursor(self, key):
        num_cells = self.root_node.leaf_node_num_cells()
        cursor = Cursor(self.table)
        min_index = 0
        one_past_max_index = num_cells

        while one_past_max_index != min_index:
            index = int((min_index + one_past_max_index) / 2)
            key_at_index = self.root_node.get_cell(index).key

            if key == key_at_index:
                cursor.cell_num = index
                return cursor
            if key < key_at_index:
                one_past_max_index = index
            else:
                min_index = index + 1

        cursor.cell_num = min_index
        return cursor


# class LeafNode():
#      def __init__(self, page: bytearray):
#         self.page = page

# class RootNode:
#     def __init__(self, page: bytearray):
#         self.page = page


@dataclass
class Header:
    offset: int
    datatype: Any
    page: bytearray

    def write(self, content: Any) -> None:
        self.page[
            self.offset : self.offset + self.datatype.length
        ] = self.datatype.serialize(content)

    def read(self):
        return self.datatype.deserialize(
            self.page[self.offset : self.offset + self.datatype.length]
        )


@dataclass
class Cell:
    """
    Cell is pk + row block of bytes
    """

    cell_index: int
    page: bytearray
    key_datatype: Any = datatypes.Integer()

    @property
    def offset(self):
        return LEAF_NODE_HEADER_SIZE + (self.cell_index * LEAF_NODE_CELL_SIZE)

    def write(self, key, content: Any) -> None:
        self.page[self.offset : self.offset + LEAF_NODE_CELL_SIZE] = (
            self.key_datatype.serialize(key) + content
        )

    def read(self):
        return self.page[self.offset : self.offset + LEAF_NODE_CELL_SIZE]

    @property
    def value(self) -> bytearray:
        cell = self.read()
        return cell[self.key_datatype.length : -1]

    @property
    def key(self) -> int:
        cell = self.read()
        return self.key_datatype.deserialize(cell[0 : self.key_datatype.length])


class Node:
    """
    A Node is backed by a page (4kb) of bytes
    it can be either a leaf or internal node.
    Internal nodes store keys
    And leaf nodes store keys + values.
    """

    def __init__(self, page: bytearray):
        if page is None or len(page) == 0:
            page = bytearray(b"".ljust(PAGE_SIZE, b"\0"))
        self.page = page
        self.cell_key = datatypes.Integer()
        self.num_cells_header = Header(
            LEAF_NODE_NUM_CELLS_OFFSET, datatypes.Integer(), self.page
        )
        self.is_leaf_header = Header(NODE_TYPE_OFFSET, datatypes.Boolean(), self.page)
        self.parent_point = Header(
            PARENT_POINTER_OFFSET, datatypes.Integer(), self.page
        )

    def is_root(self):
        """
        TODO should return true if there are no parent pointers.
        """
        return True

    def read_content(self, start, length):
        return self.page[start : start + length]

    def write_content(self, start: int, length: int, content: bytes):
        self.page[start : start + length] = content

    def leaf_node_num_cells(self):
        return self.num_cells_header.read()

    def set_num_cells(self, num):
        page = self.num_cells_header.write(num)
        return page

    def set_node_type(self, is_leaf):
        page = self.is_leaf_header.write(is_leaf)
        return page

    def get_node_type(self):
        return self.is_leaf_header.read()

    def get_cell(self, cell_num):
        return Cell(cell_num, self.page)

    def insert_cell(self, cursor, key, cell_value):
        num_cells = self.leaf_node_num_cells()
        if num_cells >= LEAF_NODE_MAX_CELLS:
            raise Exception("Need to implement splitting a leaf node")

        if cursor.cell_num < num_cells:
            # Node full
            raise Exception(
                f"cursor.cell_num: {cursor.cell_num} < num_cells: {num_cells}"
            )

        cell = self.get_cell(cursor.cell_num)
        cell.write(key, cell_value)
        self.set_num_cells(num_cells + 1)
        return self.page


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

    def get_node(self, page_num):
        page = self.table.pager[page_num]
        return Node(page)

    def advance(self):
        node = self.get_node(self.page_num)
        self.cell_num += 1

        if self.cell_num >= node.leaf_node_num_cells():
            self.end_of_table = True
