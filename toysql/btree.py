from toysql.page import Page, PageType
from toysql.exceptions import PageFullException

class BPlusTree:
    """
    Balanced Tree
    Each node can have at most m keys and m + 1 pointer fields.

    B-tree has a height of log(M*N) (Where ‘M’ is the order of tree and N is the number of nodes).
    """
    def __init__(self, page_number, page_size=None) -> None:
        self.page_size = page_size or 4096
        self.root = Page(page_number, PageType.leaf) 

    def add(self, values) -> None:
        """
        This adds a tuple value to the tree. 
        """
        try:
            self.root.add(values)
        except PageFullException:
            # TODO split node.
            pass

    def search(self, key):
        return self.root.search(key)
