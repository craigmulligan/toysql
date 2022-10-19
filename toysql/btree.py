from toysql.page import PageType, LeafPageCell, Page, InteriorPageCell
from toysql.record import Record
from typing import Optional
from dataclasses import dataclass
from copy import deepcopy


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


@dataclass
class Frame:
    page_number: int
    child_index: int


class Cursor:
    """
    sqlite definition:


    #define BTREE_WRCSR     0x00000004     /* read-write cursor */
    #define BTREE_FORDELETE 0x00000008     /* Cursor is for seek/delete only */

    int sqlite3BtreeCursor(
      Btree*,                              /* BTree containing table to open */
      int iTable,                          /* Index of root page */
      int wrFlag,                          /* 1 for writing.  0 for read-only */
      struct KeyInfo*,                     /* First argument to compare function */
      BtCursor *pCursor                    /* Space to write cursor structure */
    );
    int sqlite3BtreeCursorSize(void);
    int sqlite3BtreeCloseCursor(BtCursor*);
    void sqlite3BtreeClearCursor(BtCursor *);
    int sqlite3BtreeFirst(BtCursor*, int *pRes);
    int sqlite3BtreeLast(BtCursor*, int *pRes);
    int sqlite3BtreeNext(BtCursor*, int *pRes);
    int sqlite3BtreePrevious(BtCursor*, int *pRes);
    int sqlite3BtreeEof(BtCursor*);
    int sqlite3BtreeKeySize(BtCursor*, i64 *pSize);
    int sqlite3BtreeKey(BtCursor*, u32 offset, u32 amt, void*);
    const void *sqlite3BtreeKeyFetch(BtCursor*, u32 *pAmt);
    const void *sqlite3BtreeDataFetch(BtCursor*, u32 *pAmt);
    int sqlite3BtreeDataSize(BtCursor*, u32 *pSize);
    int sqlite3BtreeData(BtCursor*, u32 offset, u32 amt, void*);
    int sqlite3BtreeCount(BtCursor *, i64 *);

    StackBased cursor.

    As we move to each node we will keep the
    Stack = List[Frame]

    Frame:
        page_number: int
        child_index: int
    """

    def __init__(self, btree: BTree) -> None:
        self.tree = btree
        self.seek_start()

    def seek_start(self):
        self.stack = [Frame(self.tree.root.page_number, 0)]
        self.visited = []

    def __iter__(self):
        return self

    def seek(self, row_id: int):
        """
        Cursor seek to a specified rowid in the Btree
        """
        if len(self.stack) == 0:
            raise StopIteration()

        frame = self.stack[-1]
        current_page = self.tree.read_page(frame.page_number)

        if current_page.is_leaf():
            try:
                v = current_page.cells[frame.child_index]
                frame.child_index += 1
                return v.record
            except IndexError:
                # End of the LeafPage
                # Walk back up to parent.
                self.stack.pop()
                return self.__next__()
        else:
            # InteriorPage
            # Here we keep track of each branch we have been down
            # in the stack. If we have already been down a child branch
            # we skip it.
            # If we have been down all child paths we pop off the stack
            # and traverse the parent.
            for i, page_number in enumerate(self.child_page_numbers(current_page)):
                if not isinstance(page_number, int):
                    raise Exception("page_number not int")

                # Find the branch we haven't been down yet.
                if i < frame.child_index:
                    continue
                else:
                    frame.child_index += 1
                    self.stack.append(Frame(page_number, 0))
                    return self.__next__()

            # We have exhausted all child branches
            # Pop off back up to parent.
            self.stack.pop()
            return self.__next__()

    def current(self):
        """
        Get the current record
        if cursor is pointing a leaf node.
        else move to next record.
        I think this means we don't need peek.
        """
        frame = self.stack[-1]
        current_page = self.tree.read_page(frame.page_number)

        try:
            v = current_page.cells[frame.child_index]
            frame.child_index += 1
            return v.record
        except IndexError:
            # End of the LeafPage
            # Walk back up to parent.
            self.stack.pop()
            return self.__next__()

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

        frame = self.stack[-1]
        current_page = self.tree.read_page(frame.page_number)

        if current_page.is_leaf():
            return self.current()
            # try:
            #     v = current_page.cells[frame.child_index]
            #     frame.child_index += 1
            #     return v.record
            # except IndexError:
            #     # End of the LeafPage
            #     # Walk back up to parent.
            #     self.stack.pop()
            #     return self.__next__()
        else:
            # InteriorPage
            # Here we keep track of each branch we have been down
            # in the stack. If we have already been down a child branch
            # we skip it.
            # If we have been down all child paths we pop off the stack
            # and traverse the parent.
            for i, page_number in enumerate(self.child_page_numbers(current_page)):
                if not isinstance(page_number, int):
                    raise Exception("page_number not int")

                # Find the branch we haven't been down yet.
                if i < frame.child_index:
                    continue
                else:
                    frame.child_index += 1
                    self.stack.append(Frame(page_number, 0))
                    return self.__next__()

            # We have exhausted all child branches
            # Pop off back up to parent.
            self.stack.pop()
            return self.__next__()

    def peek(self):
        # iterate then restore.
        # We need deep copy so we capture
        # Frame objects too.
        stack = deepcopy(self.stack)

        try:
            v = next(self)
        except StopIteration:
            return None

        self.stack = stack

        return v

    def __getattr__(self, name):
        # Proxy all other calls to btree.
        # TODO this is a hack.
        return getattr(self.tree, name)
