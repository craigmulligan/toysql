# Create a page
# https://www.programiz.com/dsa/b-plus-tree
# https://gist.github.com/savarin/69acd246302567395f65ad6b97ee503d
from toysql.page import PageType, LeafPageCell, Cell
from toysql.record import Record, DataType
import math

class InteriorPageCell(Cell):
    """   
    Table B-Tree Interior Cell (header 0x05):

    A 4-byte big-endian page number which is the left child pointer.
    A varint which is the integer key.
    """
    def __init__(self, row_id, left_child) -> None:
        self.row_id = row_id
        self.left_child = left_child

    def __eq__(self, o: "InteriorPageCell") -> bool:
        return self.row_id == o.row_id


class Page:
  def __init__(self, page_type=PageType.leaf, right_child=None):
    self.parent = None
    self.page_type = page_type 
    self.right_child = right_child
    self.cells = []

  def __repr__(self):
    cell_ids = [str(cell.row_id) for cell in self.cells]

    return ",".join(cell_ids)

  def is_leaf(self):
      return self.page_type == PageType.leaf

  def add(self, cell):
    for c in self.cells:
        if c.row_id == cell.row_id:
            raise Exception("key exists!")

    self.cells.append(cell)
    self.cells.sort()


  def show(self, counter):
    """Prints the keys at each level."""
    output = counter * "\t" 

    if not self.is_leaf():
        output += str(self)
        output += "\n"
        counter += 1 
        for cell in self.cells:
            output += cell.left_child.show(counter)
        
        output += self.right_child.show(counter)

    else:
        # Green is the leaf values
        output += "\033[1;32m " + ", ".join(str(cell.row_id) for cell in self.cells) + "\033[0m"
        
    output += "\n"
    return output


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

    def __init__(self, order) -> None:
        self.root = Page(PageType.leaf)
        self.order = order

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
        left = Page(PageType.leaf)

        left.cells = page.cells[:index]
        page.cells = page.cells[index:]
        key = page.cells[0].row_id

        parent = page.parent
        if parent is None: 
           parent = Page(PageType.interior)
           self.root = parent
           self.root.right_child = page 

        parent.add(InteriorPageCell(key, left))

        if self.is_full(parent):
            self._split_internal(parent)

    def _split_internal(self, page):
        """
           Given a full InteriorPage 

           1. Splits the page in half
           2. Takes the left most cell of the right page (middle cell)
           3. It makes the left_page.right_child = middle_cell.left_child 
           4. It adds the middle_cell.row_id to the parent which points the the left child.
        """
        index = self.order // 2

        left = Page(PageType.interior)
        left.cells = page.cells[:index]
        page.cells = page.cells[index:]

        middle = page.cells.pop(0)
        left.right_child = middle.left_child 
        key = middle.row_id

        parent = page.parent
        if parent is None:
           parent = Page(PageType.interior)
           self.root = parent
           self.root.right_child = page

        parent.add(InteriorPageCell(key, left))

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

        child.add(cell)

        if self.is_full(child):
            self._split_leaf(child)


    def find_in_leaf(self, key, page):
        for cell in page.cells:
            if key == cell.row_id:
                return cell.record

        return None 

    def find_in_interior(self, key, page):
        for cell in page.cells:
            if key < cell.row_id:
                return cell.left_child
        return page.right_child

    def find(self, key):
        """Returns a value for a given key, and None if the key does not exist."""
        child = self.root
        while not child.is_leaf():
            child = self.find_in_interior(key, child)

        return self.find_in_leaf(key, child)

    def show(self):
        return self.root.show(0)