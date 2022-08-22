from toysql.page import Page, PageType
from toysql.exceptions import PageFullException

class BPlusTree:
    """
    All leaves are at the same level.

    The root has at least two children.

    Each node except root can have a maximum of m children and at least m/2 children.

    Each node can contain a maximum of m - 1 keys and a minimum of ⌈m/2⌉ - 1 keys.
    """
    def __init__(self, page_number, pager) -> None:
        self.page_size = pager.page_size 
        self.pager = pager 
        # TODO should read from pager.read_page(page_number)
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

    def search_node(self, node,  row_id):
        if self.page_type == PageType.leaf:
            for cell in self.cells:
                if cell.row_id == row_id:
                    return cell

            raise CellNotFoundException(f"Could not find cell with row_id {row_id}")


        if self.page_type == PageType.interior:
            for cell in self.cells:
                if row_id <= cell.row_id:
                   Page 

