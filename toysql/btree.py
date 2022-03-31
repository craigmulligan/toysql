from enum import Enum
from toysql.constants import *


class BTree:
    pass


class NodeType:
    internal = 0
    leaf = 1


class Node:
    def __init__(self, page: bytearray):
        self.page = page

    def __len__(self):
        return int(len(self.page) + LEAF_NODE_NUM_CELLS_OFFSET)

    def leaf_node_num_cells(self):
        return len(self.page) + LEAF_NODE_NUM_CELLS_OFFSET

    def leaf_node_cell(self, cell_num):
        return len(self.page) + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE

    def leaf_node_key(self, cell_num):
        return self.leaf_node_cell(cell_num)

    def leaf_node_value(self, cell_num):
        return self.leaf_node_cell(cell_num) + LEAF_NODE_KEY_SIZE

    def initialize_leaf_node(self):
        pass

    def cell_value(self, cell_num):

        pass

    def insert_cell(self, cursor, key, row):
        num_cells = self.leaf_node_num_cells()

        if num_cells >= LEAF_NODE_MAX_CELLS:
            raise Exception("Need to implement splitting a leaf node")

        if cursor.cell_num < num_cells:
            pass

        key_offset = self.leaf_node_key(cursor.cell_num)
        value_offset = self.leaf_node_value(cursor.cell_num)

        self.page = self.page[key_offset:value_offset] = key.to_bytes(1, "big") + row
