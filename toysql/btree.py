from toysql.page import Page, PageType
from toysql.exceptions import PageFullException

class BPlusTree:
    """
    All leaves are at the same level.

    The root has at least two children.

    Each node except root can have a maximum of m children and at least m/2 children.

    Each node can contain a maximum of m - 1 keys and a minimum of ⌈m/2⌉ - 1 keys.
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
