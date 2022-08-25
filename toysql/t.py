from math import ceil
# Create a node
# https://www.programiz.com/dsa/b-plus-tree
# https://gist.github.com/savarin/69acd246302567395f65ad6b97ee503d

class Node:
  def __init__(self, leaf=False):
    self.leaf = leaf
    self.keys = []
    self.children = []

  def add(self, key, val):
    if key not in self.keys:
        self.keys.append(key)

    self.keys.sort()
    index = self.keys.index(key)
    self.children.insert(index, val)
   

  def find(self, key):
    if self.leaf:
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
    print(counter * "\t", str(self.keys))

# Recursively print the key of child nodes (if these exist).
    if not self.leaf:
        for item in self.children:
            item.show(counter + 1)
    else:
        print(self.children)



class BTree():
    root: Node

    def __init__(self, order) -> None:
        self.root = Node(True)
        self.order = order

    def is_full(self, node):
      if len(node.keys) >= self.order: 
          return True 

      return False 

    def split(self, node):
        index = ceil((self.order)/2)

        left = Node(node.leaf)
        right = Node(node.leaf)

        left.keys = node.keys[:index]
        left.children = node.children[:index]
        
        right.keys = node.keys[index:]
        right.children = node.children[index:]

        return [left, right]

    def add(self, key, val):
        parent = None
        child = self.root

        # Traverse tree until leaf node is reached.
        while not child.leaf:
            parent = child
            child = child.find(key)

        if not self.is_full(child):
            child.add(key, val)
        else:
            child.add(key, val)
            [left, right] = self.split(child)
            key = left.keys[-1]

            if parent is None: 
                parent = Node(False)
                self.root = parent

            if not self.is_full(parent):
                parent.add(key, right)
                parent.add(key, left)

    def find(self, key):
        """Returns a value for a given key, and None if the key does not exist."""
        child = self.root
        while not child.leaf:
            child = child.find(key)

        return child.find(key) 

    def show(self):
        """Prints the keys at each level."""
        self.root.show()
