# Create a page
# https://www.programiz.com/dsa/b-plus-tree
# https://gist.github.com/savarin/69acd246302567395f65ad6b97ee503d
from toysql.page import PageType, LeafPageCell, Cell, Page, InteriorPageCell
from toysql.record import Record, DataType

# class InteriorPageCell(Cell):
#     """   
#     Table B-Tree Interior Cell (header 0x05):

#     A 4-byte big-endian page number which is the left child pointer.
#     A varint which is the integer key.
#     """
#     def __init__(self, row_id, left_child) -> None:
#         self.row_id = row_id
#         self.left_child = left_child

#     def __eq__(self, o: "InteriorPageCell") -> bool:
#         return self.row_id == o.row_id


class BTree():
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

    def __init__(self, order, pager) -> None:
        self.order = order
        self.pager = pager
        self.root = self.new_page(PageType.leaf)

    def read_page(self, page_number: int) -> Page:
        raw_bytes = self.pager.read(page_number)
        return Page.from_bytes(raw_bytes)

    def write_page(self, page):
        self.pager.write(page.page_number, page.to_bytes())

    def new_page(self, page_type) -> Page:
        page_number = self.pager.new() 
        return Page(page_type, page_number)

    def is_full(self, page):
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
           self.root.right_page_number = page.page_number

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
        middle_page = self.read_page(middle.left_child_page_number)
        left.right_page_number = middle_page.page_number
        key = middle.row_id

        parent = page.parent
        if parent is None:
           parent = self.new_page(PageType.interior)
           self.root = parent
           self.root.right_page_number = page.page_number

        parent.add_cell(InteriorPageCell(key, left.page_number))

        for p in [left, page, parent]:
            self.write_page(p)

        if self.is_full(parent):
            self._split_internal(parent)

    def keyval_to_cell(self, key, val):
        return LeafPageCell(Record([
           [DataType.INTEGER, key],
           [DataType.TEXT, val],
        ]))

    def add(self, key, val):
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
        cell = self.keyval_to_cell(key, val)

        # Traverse tree until leaf page is reached.
        while not child.is_leaf():
            parent = child 
            child = self.find_in_interior(key, child)
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
        return self.read_page(page.right_page_number)

    def find(self, key):
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
