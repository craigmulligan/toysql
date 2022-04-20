from typing import List, Tuple
from toysql.exceptions import DuplicateKeyException
import toysql.datatypes as datatypes

from toysql.exceptions import PageNotFoundException
from toysql.constants import PAGE_SIZE
from toysql.pager import Page


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
            values: [Node0.page_number, Node1.page_number, Node2.page_number]

        -> Node0:
            keys: 0, 1, 2, 3
            values: ['value-0', 'value-1', 'value-2', 'value-3']

        -> Node1
        ...

    Attributes:
        order (int): The maximum number of keys each node can hold.
    """

    header_length = 9

    def __init__(self, table, page_number):
        """Child nodes can be converted into parent nodes by setting self.leaf = False. Parent nodes
        simply act as a medium to traverse the tree."""

        # TODO use bisect
        self.keys = []
        self.values = []
        self.leaf = True
        self.key_datatype = datatypes.Integer()
        self.table = table
        self.page_number = page_number

    @property
    def cell_length(self):
        if self.leaf:
            return self.key_datatype.length + self.table.row_length()

        return self.key_datatype.length * 2

    @property
    def order(self):
        """
        Determines the number of cells that can be stored per body.
        """
        body_length = PAGE_SIZE - Node.header_length
        order = body_length // self.cell_length
        return order

    def ensure_full_page(self, page):
        """TODO move to Pager"""
        return bytearray(page.ljust(PAGE_SIZE, b"\0"))

    def ensure_header_size(self, page):
        """TODO move to Pager"""
        return bytearray(page.ljust(Node.header_length, b"\0"))

    @staticmethod
    def read(table, page_number) -> "Node":
        try:
            page = table.pager.read(page_number)
            node = Node(table, page_number)
            node.from_bytes(page)
        except PageNotFoundException:
            page_number = table.pager.new()
            node = Node(table, page_number)
            # Write the initialized values to the page.
            node.write()

        return node

    def write(self):
        """
        Flush self to pager
        """
        data = self.to_bytes()
        self.table.pager.write(self.page_number, data)

    def to_bytes(self) -> Page:
        """
        return the bytes representation
        """
        page = self.ensure_header_size(bytearray())

        # Write the header
        datatypes.Boolean().write(page, 0, self.leaf)
        datatypes.Integer().write(page, 5, len(self.keys))

        # Write the body based on node_type
        if self.leaf:
            for i, key in enumerate(self.keys):
                page += self.key_datatype.serialize(key)
                page += self.table.serialize_row(self.values[i])
        else:
            for i, key in enumerate(self.keys):
                # There will always be 1 more value than key:
                # Eg: keys [6] , values: [1, 2]
                # Which means keys below 6 stored in page 1,
                # Keys above stored in page 2.
                page += self.key_datatype.serialize(key)
                page += self.key_datatype.serialize(self.values[i])

            # Write the last value for internal node
            page += self.key_datatype.serialize(self.values[len(self.keys)])

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

        internal node: (internal nodes will always have one more value (page_number) than keys)
        byte 10-13: key0
        byte 14-18: page_number (child pointer)
        byte 19-23: key1
        byte 24-28: page_number (child pointer)
        byte 29-33: page_number (child pointer)
        ...
        """
        self.leaf = datatypes.Boolean().read(page, 0)
        num_keys = datatypes.Integer().read(page, 5)

        if self.leaf:
            row_length = self.table.row_length()
            cell_length = datatypes.Integer().length + row_length

            for i in range(num_keys):
                key_offset = (cell_length * i) + Node.header_length
                value_offset = key_offset + datatypes.Integer.length
                self.keys.append(datatypes.Integer().read(page, key_offset))
                value = self.table.deserialize_row(
                    page[value_offset : value_offset + row_length]
                )
                self.values.append(value)
        else:
            cell_length = datatypes.Integer().length * 2
            for i in range(num_keys):
                key_offset = (cell_length * i) + Node.header_length
                self.keys.append(datatypes.Integer().read(page, key_offset))
                page_number_offset = key_offset + datatypes.Integer.length
                value = datatypes.Integer().read(page, page_number_offset)
                self.values.append(value)

            # There is always 1 more value than key per internal node.
            # let's manually read it.
            page_number_offset = (cell_length * num_keys) + Node.header_length
            value = datatypes.Integer().read(page, page_number_offset)
            self.values.append(value)

        return self

    def find(self, search_key) -> Tuple[int, int]:
        """
        For a given key, returns the index where
        the key should be inserted and the
        page_number at that index.
        """
        i = 0
        for i, key in enumerate(self.keys):
            if search_key < key:
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

    def split(self) -> List["Node"]:
        """Splits the node into two and stores them as child nodes."""
        # Passing in None will append a new page
        # because we are creating one.
        left = Node.read(self.table, None)
        right = Node.read(self.table, None)
        # // is floor division.
        mid = self.order // 2

        left.keys = self.keys[:mid]
        left.values = self.values[:mid]

        right.keys = self.keys[mid:]
        right.values = self.values[mid:]

        # When the node is split, set the parent key to the left-most key of the right child node.
        self.keys = [right.keys[0]]
        self.values = [left.page_number, right.page_number]
        self.leaf = False
        return [right, left]

    def is_full(self):
        """Returns True if the node is full."""
        return len(self.keys) == self.order

    def show(self, counter=0):
        """Prints the keys at each level."""
        print(counter, self.page_number, str(self.keys))
        if not self.leaf:
            print("page numbers:", self.values)

        # Recursively print the key of child nodes (if these exist).
        if not self.leaf:
            for page_num in self.values:
                child = Node.read(self.table, page_num)
                child.show(counter + 1)

    def traverse(self, rows):
        if not self.leaf:

            for page_number in self.values:
                node = Node.read(self.table, page_number)
                node.traverse(rows)

        if self.leaf:
            for i, key in enumerate(self.keys):
                rows.append([key, self.values[i]])

        return rows


class BPlusTree:
    """B+ tree, consisting of nodes.
    Nodes will automatically be split into two once it is full. When a split occurs, a key will
    'float' upwards and be inserted into the parent node to act as a pivot.
    """

    def __init__(self, table):
        self.table = table
        self.root = Node.read(self.table, self.table.root_page_num)

    def _merge(self, parent, child, index):
        """
        For a parent and child node, extract a pivot from the child to be inserted into the keys
        of the parent. Insert the values from the child into the values of the parent.

        Once we have merge the childs keys & values into the parent
        it is no longer attached to the parent as it's point is gone.
        this means it's essentially deleted.

        Ofcoarse this means we'll have "unused" pages in our db file,
        which is something we'll need to handle with a free node list.

        https://stackoverflow.com/questions/9227769/why-are-there-unused-pages-in-my-sqlite-database

        Old state
        parent:
          keys: [1]
          values: [1, 2]

        child:
          keys: [2]
          values: [3, 4]

        After merge:

        parent:
          keys: [1, 2]
          values: [1, 3, 4]
        """
        parent.values[index]
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
        the leaf node into two and try merge the internal node into the parent.
        """
        parent = None
        child = self.root
        index = 0

        # Traverse tree until leaf node is reached.
        while not child.leaf:
            parent = child
            page_num, index = child.find(key)
            child = Node.read(self.table, page_num)

        child.add(key, value)

        nodes_updated = [child]
        # If the leaf node is full, split the leaf node into two.
        if child.is_full():
            [left, right] = child.split()
            nodes_updated.extend([left, right])

            # Once a leaf node is split, it consists of a internal node and two leaf nodes. These
            # need to be re-inserted back into the tree.
            if parent:
                if not parent.is_full():
                    self._merge(parent, child, index)
                    nodes_updated.append(parent)
                else:
                    print("Parent full can't merge back to tree")

        for node in nodes_updated:
            # Flush changes to pager backend
            node.write()

    def find(self, key):
        """Returns a value for a given key, and None if the key does not exist."""
        child = self.root

        while not child.leaf:
            page_num, _ = child.find(key)
            child = Node.read(self.table, page_num)

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
