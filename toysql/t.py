# Create a page
# https://www.programiz.com/dsa/b-plus-tree
# https://gist.github.com/savarin/69acd246302567395f65ad6b97ee503d
from toysql.page import PageType, LeafPageCell, InteriorPageCell

class Page:
  def __init__(self, page_type=PageType.leaf):
    self.page_type = page_type 
    self.keys = []
    self.children = []

  def is_leaf(self):
      return self.page_type == PageType.leaf

  def add(self, key, val):
    if key not in self.keys:
        self.keys.append(key)

    self.keys.sort()
    index = self.keys.index(key)
    self.children.insert(index, val)
   

  def find(self, key):
    if self.is_leaf():
        for i, v in enumerate(self.keys):
            if key == v:
                return self.children[i]

        return None 
    else:
        for i, k in enumerate(self.keys):
            if key <= k:
                return self.children[i]

        return self.children[-1]


  def show(self, counter=0):
    """Prints the keys at each level."""
    output = counter * "\t" + str(self.keys)

# Recursively print the key of child pages (if these exist).
    if not self.is_leaf():
        for item in self.children:
            output += item.show(counter + 1)
    else:
        pass
        #output += "\n" + str(self.children)

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

      if len(page.keys) >= self.order: 
          return True 

      return False 

    def split(self, page):
        index = (self.order)//2

        left = Page(PageType.leaf)
        right = Page(PageType.leaf)

        left.keys = page.keys[:index]
        left.children = page.children[:index]
        
        right.keys = page.keys[index:]
        right.children = page.children[index:]

        return [left, right]

    def add(self, key, val):
        parent = None
        child = self.root

        # Traverse tree until leaf page is reached.
        while not child.is_leaf():
            parent = child
            child = child.find(key)

        if not self.is_full(child):
            child.add(key, val)
        else:
            child.add(key, val)
            [left, right] = self.split(child)
            key = left.keys[-1]

            if parent is None: 
                parent = Page(False)
                self.root = parent

            if not self.is_full(parent):
                parent.add(key, right)
                parent.add(key, left)
            else:
                # This shouldn't ever happen.
                raise Exception("Parent full.")

    def find(self, key):
        """Returns a value for a given key, and None if the key does not exist."""
        child = self.root
        while not child.is_leaf():
            child = child.find(key)

        return child.find(key) 

    def show(self):
        """Prints the keys at each level."""
        return self.root.show()
