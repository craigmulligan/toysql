from toysql.exceptions import DuplicateKeyException
import toysql.datatypes as datatypes
from dataclasses import dataclass
from toysql.constants import PAGE_SIZE


Page: bytearray


class Node:
    """
    Base node object.
    Each node stores keys and values.

    Internal nodes will store pointers to child nodes as there values
    Leaf nodes will store real values as there values.

    Internal nodes keys will be the value of keys in it's child node pointer
    for that same index.

    Leaf nodes will store the primary key for each leaf node value at the same index.

    for instance:
        -> InternalNode:
            keys: [4, 8, 12]
            values: [Node0, Node1, Node2]

        -> Node0:
            keys: 0, 1, 2, 3
            values: ['value-0', 'value-1', 'value-2', 'value-3']

        -> Node1
        ...

    Attributes:
        order (int): The maximum number of keys each node can hold.
    """

    header_length = 9

    def __init__(self, order, table):
        """Child nodes can be converted into parent nodes by setting self.leaf = False. Parent nodes
        simply act as a medium to traverse the tree."""

        self.order = order
        self.keys = []
        self.values = []
        self.leaf = True
        self.key_datatype = datatypes.Integer()
        self.table = table
        self.page_number = 34

    def ensure_full_page(self, page):
        """TODO move to Pager"""
        return bytearray(page.ljust(PAGE_SIZE, b"\0"))

    def ensure_header_size(self, page):
        """TODO move to Pager"""
        header_size = 9
        return bytearray(page.ljust(header_size, b"\0"))

    def to_bytes(self):
        """
        return the bytes representation
        """
        page = self.ensure_header_size(bytearray())

        # Write the header
        datatypes.Boolean().write(page, 0, self.leaf)
        datatypes.Integer().write(page, 1, len(self.keys))

        # Write the body based on node_type
        if self.leaf:
            for i, key in enumerate(self.keys):
                page += self.key_datatype.serialize(key)
                page += self.table.serialize_row(self.values[i])
        else:
            for i, key in enumerate(self.keys):
                page += self.key_datatype.serialize(key)
                page += self.key_datatype.serialize(self.values[i].page_number)

        return self.ensure_full_page(page)

    def from_bytes(self, page: bytearray) -> "Node":
        """
        Given a page of bytes
        initialize a Node with keys + values.

        Node header:
        byte 0: is_leaf
        byte 1: is_root
        byte 2-5: parent_pointer
        byte 6-9: num_cells

        leaf node:
        byte 10-13: key0
        byte 14-306: value 0
        ...

        internal node:
        byte 10-13: key0
        byte 14-18: child pointer
        ...
        """
        self.leaf = datatypes.Boolean().read(page, 0)
        num_keys = datatypes.Integer().read(page, 1)

        if self.leaf:
            row_length = self.table.row_length()
            cell_length = datatypes.Integer().length + row_length

            for i in range(num_keys):
                key_offset = (cell_length * i) + Node.header_length
                value_offset = key_offset + datatypes.Integer.length
                self.keys.append(datatypes.Integer().read(page, key_offset))
                self.values.append(
                    self.table.deserialize_row(
                        page[value_offset : value_offset + row_length]
                    )
                )
        else:
            cell_length = datatypes.Integer().length * 2
            for i in range(num_keys):
                key_offset = (cell_length * i) + Node.header_length
                self.keys.append(datatypes.Integer().read(page, key_offset))
                # page_number_offset = (
                #     (cell_length * i) + datatypes.Integer.length + Node.header_length
                # )
                # self.values.append(datatypes.Integer().read(page, page_number_offset))

        return self

    def find(self, key):
        """
        For a given key, returns the index where
        the key should be inserted and the
        list of values at that index.
        """
        i = 0
        for i, item in enumerate(self.keys):
            if key < item:
                return self.values[i], i

        return self.values[i + 1], i + 1

    def add(self, key, value):
        """Adds a key-value pair to the node."""
        # If the node is empty, simply insert the key-value pair.
        if not self.keys:
            self.keys.append(key)
            self.values.append(value)
            return None

        for i, item in enumerate(self.keys.copy()):
            # If new key matches existing key raise exception.
            if key == item:
                raise DuplicateKeyException(f"{i} key already exists")

            # If new key is smaller than existing key, insert new key to the left of existing key.
            elif key < item:
                self.keys = self.keys[:i] + [key] + self.keys[i:]
                self.values = self.values[:i] + [value] + self.values[i:]
                break

            # If new key is larger than all existing keys, insert new key to the right of all
            # existing keys.
            elif i + 1 == len(self.keys):
                self.keys.append(key)
                self.values.append(value)

    def split(self):
        """Splits the node into two and stores them as child nodes."""
        left = Node(self.order, self.table)
        right = Node(self.order, self.table)
        # // is floor division.
        mid = self.order // 2

        left.keys = self.keys[:mid]
        left.values = self.values[:mid]

        right.keys = self.keys[mid:]
        right.values = self.values[mid:]

        # When the node is split, set the parent key to the left-most key of the right child node.
        self.keys = [right.keys[0]]
        self.values = [left, right]
        self.leaf = False

    def is_full(self):
        """Returns True if the node is full."""
        return len(self.keys) == self.order

    def show(self, counter=0):
        """Prints the keys at each level."""
        print(counter, str(self.keys))

        # Recursively print the key of child nodes (if these exist).
        if not self.leaf:
            for item in self.values:
                item.show(counter + 1)

    def traverse(self, rows):
        if not self.leaf:

            for item in self.values:
                item.traverse(rows)

        if self.leaf:
            for i, key in enumerate(self.keys):
                rows.append([key, self.values[i]])

        return rows


class BPlusTree:
    """B+ tree, consisting of nodes.
    Nodes will automatically be split into two once it is full. When a split occurs, a key will
    'float' upwards and be inserted into the parent node to act as a pivot.
    Attributes:
        order (int): The maximum number of keys each node can hold.
    """

    def __init__(self, table, order=8):
        self.table = table
        self.root = Node(order, self.table)

    def _merge(self, parent, child, index):
        """For a parent and child node, extract a pivot from the child to be inserted into the keys
        of the parent. Insert the values from the child into the values of the parent.
        """
        parent.values.pop(index)
        pivot = child.keys[0]

        for i, item in enumerate(parent.keys):
            if pivot < item:
                parent.keys = parent.keys[:i] + [pivot] + parent.keys[i:]
                parent.values = parent.values[:i] + child.values + parent.values[i:]
                break

            elif i + 1 == len(parent.keys):
                parent.keys += [pivot]
                parent.values += child.values
                break

    def insert(self, key, value):
        """Inserts a key-value pair after traversing to a leaf node. If the leaf node is full, split
        the leaf node into two.
        """
        parent = None
        child = self.root
        index = 0

        # Traverse tree until leaf node is reached.
        while not child.leaf:
            parent = child
            child, index = child.find(key)

        child.add(key, value)

        # If the leaf node is full, split the leaf node into two.
        if child.is_full():
            child.split()

            # Once a leaf node is split, it consists of a internal node and two leaf nodes. These
            # need to be re-inserted back into the tree.
            if parent and not parent.is_full():
                self._merge(parent, child, index)

    def find(self, key):
        """Returns a value for a given key, and None if the key does not exist."""
        child = self.root

        while not child.leaf:
            child, _ = child.find(key)

        for i, item in enumerate(child.keys):
            if key == item:
                return child.values[i]

        return None

    def traverse(self):
        values = self.root.traverse([])
        return values

    def show(self):
        """Prints the keys at each level."""
        self.root.show()
