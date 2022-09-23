from io import SEEK_CUR
from toysql.page import PageType, LeafPageCell, Page, InteriorPageCell
from toysql.record import Record, DataType
from typing import Optional


class BTree:
    """
        https://www.sqlite.org/fileformat.html#b_tree_pages

    https://massivealgorithms.blogspot.com/2014/12/b-tree-wikipedia-free-encyclopedia.html?m=1
    ★ Definition of B+ Tree
        A B+ Tree of order m has these properties:
        - The root is either a leaf or has at least two children;
        - Each internal node, except for the root, has between ⎡order/2⎤ and order children;
        - Internal nodes do not store record, only store key values to guild the search;
        - Each leaf node, has between ⎡order/2⎤ and order keys and values;
        - Leaf node store keys and records or pointers to records;
        - All leaves are at the same level in the tree, so the tree is always height balanced.
    """

    root: Page

    def __init__(self, pager, root_page_number) -> None:
        # TODO:
        # order should be variable not 3.
        self.order = 3
        self.pager = pager
        self.root = self.read_page(root_page_number)

        self.current_page_number = 0
        self.current_cell_index = 0

    def read_page(self, page_number: int) -> Page:
        return self.pager.read(page_number)

    def write_page(self, page) -> None:
        self.pager.write(page)

    def new_page(self, page_type) -> Page:
        page_number = self.pager.new()
        return Page(page_type, page_number)

    def is_full(self, page) -> bool:
        if len(page.cells) >= self.order:
            return True

        return False

    def _split_leaf(self, page):
        """
        Given a full leaf page.
        1. Splits it into two leaf pages.
        2. If there is no parent it creates a new InteriorPage.
        3. It then takes the left most key of the right split page.
        4. Inserts that key into the parent.
        5. If the parent is full it splits that.
        """
        index = self.order // 2
        left = self.new_page(PageType.leaf)

        left.cells = page.cells[:index]
        page.cells = page.cells[index:]
        key = page.cells[0].row_id

        parent = page.parent
        if parent is None:
            parent = self.new_page(PageType.interior)
            self.root = parent

            # Swap page numbers to keep the root_page_number static.
            parent.page_number, page.page_number = page.page_number, parent.page_number

            self.root.right_child_page_number = page.page_number

        parent.add_cell(InteriorPageCell(key, left.page_number))

        for p in [left, page, parent]:
            self.write_page(p)

        if self.is_full(parent):
            self._split_internal(parent)

    def _split_internal(self, page: Page):
        """
        Given a full InteriorPage

        1. Splits the page in half
        2. Takes the left most cell of the right page (middle cell)
        3. It makes the left_page.right_child = middle_cell.left_child
        4. It adds the middle_cell.row_id to the parent which points the the left child.
        """
        index = self.order // 2

        left = self.new_page(PageType.interior)
        left.cells = page.cells[:index]
        page.cells = page.cells[index:]

        middle = page.cells.pop(0)
        left.right_child_page_number = middle.left_child_page_number

        parent = page.parent
        if parent is None:
            parent = self.new_page(PageType.interior)
            self.root = parent

            # Keep the root_page_number static.
            parent.page_number, page.page_number = page.page_number, parent.page_number

            self.root.right_child_page_number = page.page_number

        parent.add_cell(InteriorPageCell(middle.row_id, left.page_number))

        for p in [left, page, parent]:
            self.write_page(p)

        if self.is_full(parent):
            self._split_internal(parent)

    def insert(self, record: Record):
        """
        1. Perform a search to determine which leaf node the new key should go into.
        2. If the node is not full, insert the new key, done!
        3. Otherwise, split the leaf node.
            a. Allocate a new leaf node and move half keys to the new node.
            b. Insert the new leaf's smallest key into the parent node.
            c. If the parent is full, split it too, repeat the split process above until a parent is found that need not split.
            d. If the root splits, create a new root which has one key and two children.
        """
        parent = None
        child = self.root
        cell = LeafPageCell(record)

        # Traverse tree until leaf page is reached.
        while not child.is_leaf():
            parent = child
            child = self.find_in_interior(record.row_id, child)
            child.parent = parent

        child.add_cell(cell)

        if self.is_full(child):
            self._split_leaf(child)
        else:
            self.write_page(child)

    def find_in_interior(self, key, page: Page) -> Page:
        for cell in page.cells:
            if key < cell.row_id:
                return self.read_page(cell.left_child_page_number)

        assert page.right_child_page_number
        return self.read_page(page.right_child_page_number)

    def find(self, key) -> Optional[Record]:
        """Returns a value for a given key, and None if the key does not exist."""
        child = self.root
        while not child.is_leaf():
            child = self.find_in_interior(key, child)

        if child is None:
            return child

        cell = child.find_cell(key)
        if cell:
            return cell.record

    def show(self):
        return self.root.show(0, self.read_page)

    def child_pages(self, page):
        for cell in page.cells:
            yield self.read_page(cell.left_child_page_number)

        yield self.read_page(page.right_child_page_number)

    def scan(self):
        return Cursor(self)

    def new_row_id(self):
        """
        This retuns the next unused row_id for a btree.
        TODO: Instead of full table scan, we should just
        traverse right to the highest record.
        """
        last = None
        try:
            for record in self.scan():
                last = record
        except StopIteration:
            pass

        new_row_id = 1
        if last is not None:
            new_row_id = last.row_id + 1

        record = Record([], row_id=new_row_id)
        self.insert(record)
        return new_row_id


class Cursor:
    def __init__(self, btree: BTree) -> None:
        self.tree = btree
        self.current_leaf_cell_index = 0
        self.current_page_number = self.tree.root.page_number
        self.stack = [self.tree.root.page_number]
        self.visited = []

    def seek_start(self):
        # go to first row.
        self.current_page_number = self.tree.root.page_number
        self.current_leaf_cell_index = 0
        self.stack = [self.tree.root.page_number]
        self.visited = []

    def __iter__(self):
        return self

    @staticmethod
    def page_at_index(page, index):
        if index == len(page.cells):
            return page.right_child_page_number
        else:
            return page.cells[index].left_child_page_number

    @staticmethod
    def child_page_numbers(page):
        for cell in page.cells:
            yield cell.left_child_page_number

        yield page.right_child_page_number

    def __next__(self):
        if len(self.stack) == 0:
            raise StopIteration()

        current_page = self.tree.read_page(int(self.current_page_number))

        if current_page.is_leaf():
            try:
                v = current_page.cells[self.current_leaf_cell_index]
                self.current_leaf_cell_index += 1
                return v.record
            except IndexError:
                # End of the LeafPage
                # Walk back up to parent.
                self.current_leaf_cell_index = 0
                parent_page_number = self.stack.pop()
                self.current_page_number = parent_page_number
                return self.__next__()
        else:
            # InteriorPage
            # Here we keep track of each path we have been down
            # with visited.
            # If we have visited each child
            # We pop off the stack to the parent
            for page_number in self.child_page_numbers(current_page):
                if page_number in self.visited:
                    continue
                else:
                    self.stack.append(self.current_page_number)
                    self.visited.append(page_number)
                    self.current_page_number = page_number
                    return self.__next__()

            # End of the InteriorPage
            # Walk back up to parent.
            self.current_page_number = self.stack.pop()
            return self.__next__()

    def peek(self):
        # iterate then restore.
        visited = self.visited.copy()
        stack = self.stack.copy()
        current_leaf_cell_index = self.current_leaf_cell_index
        current_page = self.current_page_number

        try:
            v = next(self)
        except StopIteration:
            return None

        self.visited = visited
        self.stack = stack
        self.current_leaf_cell_index = current_leaf_cell_index
        self.current_page_number = current_page

        return v

    def __getattr__(self, name):
        # Proxy all other calls to btree.
        # TODO this is a hack.
        return getattr(self.tree, name)
