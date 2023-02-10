from toysql.page import PageType, LeafPageCell, Page, InteriorPageCell
from toysql.record import Record
from toysql.exceptions import NotFoundException
from typing import Optional
from dataclasses import dataclass
import sys


@dataclass
class Frame:
    page_number: int
    child_index: int


class BTree:
    """
        https://www.sqlite.org/fileformat.html#b_pages

    https://massivealgorithms.blogspot.com/2014/12/b-wikipedia-free-encyclopedia.html?m=1
    ★ Definition of B+ Tree
    Note order in this case is relative to page size.
        A B+ of order m has these properties:
        - The root is either a leaf or has at least two children;
        - Each internal node, except for the root, has between ⎡order/2⎤ and order children;
        - Internal nodes do not store record, only store key values to guild the search;
        - Each leaf node, has between ⎡order/2⎤ and order keys and values;
        - Leaf node store keys and records or pointers to records;
        - All leaves are at the same level in the  so the is always height balanced.
    StackBased cursor.

    As we move to each node we will keep the
    Stack = List[Frame]

    Frame:
        page_number: int
        child_index: int
    """

    def __init__(self, pager, root_page_number) -> None:
        self.pager = pager
        self.root_page_number = root_page_number
        self.reset()

    @property
    def root(self) -> Page:
        return self.pager.read(self.root_page_number)

    def new_page(self, page_type) -> Page:
        page_number = self.pager.new()
        return Page(page_type, page_number)

    def show(self):
        return self.root.show(0, self.pager.read)

    def reset(self):
        self.stack = [Frame(self.root.page_number, 0)]
        # rewind = True tells us that the cursor
        # has not moved yet
        # TODO: Better way to do this?
        self.rewind = True

    def seek_start(self):
        self.reset()

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
        cell = LeafPageCell(record)

        try:
            self.seek(record.row_id)
        except NotFoundException:
            pass

        # Get current position
        frame = self.stack[-1]

        page = self.pager.read(frame.page_number)
        page.add_cell(cell)

        if page.is_full():
            self._split_leaf(page)
        else:
            self.pager.write(page)

        # Useful for debugging
        # To see how the changes on insert
        # print(self.show())

    def _split_leaf(self, page):
        """
        Given a full leaf page.
        1. Splits it into two leaf pages.
        2. If there is no parent it creates a new InteriorPage.
        3. It then takes the left most key of the right split page.
        4. Inserts that key into the parent.
        5. If the parent is full it splits that.
        """
        index = len(page.cells) // 2
        left = self.new_page(PageType.leaf)

        left.cells = page.cells[:index]
        page.cells = page.cells[index:]
        key = page.cells[0].row_id

        # Pop of self.
        self.stack.pop()
        if len(self.stack) == 0:
            parent = self.new_page(PageType.interior)

            # Swap page numbers to keep the root_page_number static.
            parent.page_number, page.page_number = page.page_number, parent.page_number
            parent.right_child_page_number = page.page_number
            self.stack.append(Frame(parent.page_number, 0))
        else:
            frame = self.stack[-1]
            parent = self.pager.read(frame.page_number)

        parent.add_cell(InteriorPageCell(key, left.page_number))

        for p in [left, page, parent]:
            self.pager.write(p)

        if parent.is_full():
            self._split_internal(parent)

    def _split_internal(self, page: Page):
        """
        Given a full InteriorPage

        1. Splits the page in half
        2. Takes the left most cell of the right page (middle cell)
        3. It makes the left_page.right_child = middle_cell.left_child
        4. It adds the middle_cell.row_id to the parent which points the the left child.
        """
        index = len(page.cells) // 2

        left = self.new_page(PageType.interior)
        left.cells = page.cells[:index]
        page.cells = page.cells[index:]

        middle = page.cells.pop(0)
        left.right_child_page_number = middle.left_child_page_number

        parent = page.parent

        self.stack.pop()
        if len(self.stack) == 0:
            parent = self.new_page(PageType.interior)

            # Keep the root_page_number static.
            parent.page_number, page.page_number = page.page_number, parent.page_number
            parent.right_child_page_number = page.page_number
            self.stack.append(Frame(parent.page_number, 0))
        else:
            frame = self.stack[-1]
            parent = self.pager.read(frame.page_number)

        parent.add_cell(InteriorPageCell(middle.row_id, left.page_number))

        for p in [left, page, parent]:
            self.pager.write(p)

        if parent.is_full():
            self._split_internal(parent)

    def seek_end(self):
        try:
            # Seek to the last value
            # using maxsize
            self.seek(sys.maxsize)
        except NotFoundException:
            # It's always going to raise.
            pass

    def __iter__(self):
        self.reset()
        return self

    def is_empty(self) -> bool:
        """
        Returns true if the root page is empty.
        """
        root_page = self.pager.read(self.root_page_number)
        return len(root_page.cells) == 0

    def find(self, row_id: int) -> Optional[Record]:
        """
        Convenience wrapper around seek & current.
        """
        try:
            self.seek(row_id)
            return self.current()
        except NotFoundException:
            return None

    def seek(self, row_id: int) -> None:
        self.reset()
        self._seek(row_id)

    def _seek(self, row_id: int) -> None:
        """
        Cursor seek to a specified row_id in the Btree

        If the row_id doesn't exist in the b

        it'll set the cursor to point at the insert location.
        """
        self.rewind = False

        if len(self.stack) == 0:
            raise StopIteration()

        frame = self.stack[-1]
        current_page = self.pager.read(frame.page_number)

        if current_page.is_leaf():
            for cell in current_page.cells:
                frame.child_index += 1

                if row_id == cell.row_id:
                    return

                if frame.child_index == len(current_page.cells):
                    raise NotFoundException(f"Couldn't seek to row {row_id}")
        else:
            # InteriorPage
            # Here we keep track of each branch we have been down
            # in the stack. If we have already been down a child branch
            # we skip it.
            # If we have been down all child paths we pop off the stack
            # and traverse the parent.
            for cell in current_page.cells:
                if row_id < cell.row_id:
                    # found branch to follow
                    self.stack.append(Frame(cell.left_child_page_number, 0))
                    return self._seek(row_id)
                else:
                    frame.child_index += 1

            # Didn't find branch take right most child
            # Follow the right most branch
            assert current_page.right_child_page_number
            self.stack.append(Frame(current_page.right_child_page_number, 0))
            return self._seek(row_id)

    def current(self) -> Record:
        """
        Get the current record
        if cursor is pointing a leaf node.
        else move to next record.
        """
        if self.rewind:
            # If we are calling .current()
            # on a cursor which hasn't moved.
            return self.__next__()

        frame = self.stack[-1]
        current_page = self.pager.read(frame.page_number)

        if current_page.is_leaf():
            if len(current_page.cells) == 0:
                # This should only happen when the root page is empty.
                raise NotFoundException(
                    f"Couldn't find current row because leaf page is empty"
                )

            idx = max(0, frame.child_index - 1)
            v = current_page.cells[idx]

            return v.record
        else:
            return self.__next__()

    def __next__(self):
        self.rewind = False
        if len(self.stack) == 0:
            raise StopIteration()

        frame = self.stack[-1]
        current_page = self.pager.read(frame.page_number)

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

    @staticmethod
    def child_page_numbers(page):
        for cell in page.cells:
            yield cell.left_child_page_number

        yield page.right_child_page_number
