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
    self.page_type = page_type 
    self.right_child = right_child
    self.cells = []

  def is_leaf(self):
      return self.page_type == PageType.leaf

  def add(self, cell):
    for c in self.cells:
        if c.row_id == cell.row_id:
            raise Exception("key exists!")

    self.cells.append(cell)
    self.cells.sort()

  def merge(self, key, left, right):
      """
         Need to first add the left key.
         Then bump things along if they need to be on the right page.
      """
      self.add(InteriorPageCell(key, left))
      self.right_child = right

      for cell in self.cells:
          if cell.row_id > key: 
              breakpoint()


  def find(self, key):
    if self.is_leaf():
        for cell in self.cells:
            if key == cell.row_id:
                return cell.record

        return None 
    else:
        for cell in self.cells:
            if key <= cell.row_id:
                return cell.left_child

        return self.right_child


  def show(self, counter):
    """Prints the keys at each level."""
    output = counter * "\t"
    print(counter)
    if counter == 0:
        output += "root: "

    # Recursively print the key of child pages (if these exist).
    if not self.is_leaf():
        for cell in self.cells:
            output += str(cell.row_id)
            output += str(cell.left_child.show(counter + 1))
        if self.right_child:
            output += self.right_child.show(counter + 1)
    else:
        output += str([c.row_id for c in self.cells])

    output += "\n"
    return output



class BTree():
    """
        https://www.sqlite.org/fileformat.html#b_tree_pages
    """
    root: Page

    def __init__(self, order) -> None:
        self.root = Page(PageType.leaf)
        self.order = order

    def is_full(self, page):

      if len(page.cells) >= self.order: 
          return True 

      return False 

    def split(self, page):
        index = math.ceil(self.order / 2)

        left = Page(PageType.leaf)
        right = Page(PageType.leaf)

        left.cells = page.cells[:index]
        right.cells = page.cells[index:]

        return [left, right]

    def keyval_to_cell(self, key, val):
        return LeafPageCell(Record([
           [DataType.INTEGER, key],
           [DataType.TEXT, val],
        ]))

    def add(self, key, val):
        parent = None
        child = self.root
        cell = self.keyval_to_cell(key, val)

        # Traverse tree until leaf page is reached.
        while not child.is_leaf():
            parent = child
            child = child.find(key)

        if not self.is_full(child):
            child.add(cell)
        else:
            # TODO dont want to add it to full page here..
            child.add(cell)
            [left, right] = self.split(child)
            key = left.cells[-1].row_id

            if parent is None: 
                parent = Page(PageType.interior)
                self.root = parent

            if not self.is_full(parent):
                parent.add(InteriorPageCell(key, left))
                parent.right_child = right
            else:
                raise Exception("Parent full.")

    def find(self, key):
        """Returns a value for a given key, and None if the key does not exist."""
        child = self.root
        while not child.is_leaf():
            child = child.find(key)

        return child.find(key)

    def show(self):
        return self.root.show(0)
