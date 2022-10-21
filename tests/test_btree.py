import random
from snapshottest import TestCase

from toysql.btree import BTree
from toysql.exceptions import NotFoundException
from toysql.record import Record, DataType
from tests.fixtures import Fixtures


class TestBTree(Fixtures, TestCase):
    def setUp(self) -> None:
        def create_record(row_id: int, text: str):
            return Record([[DataType.integer, row_id], [DataType.text, text]])

        self.create_record = create_record
        return super().setUp()

    def test_btree_x(self):
        """
        Given and ordered set of data
        Ensure the tree is the same.
        """
        btree = BTree(self.pager, self.pager.new())
        cursor = btree.cursor()
        inputs = [5, 15, 25, 35, 45]

        for n in inputs:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        self.assertMatchSnapshot(btree.show())

        for key in inputs:
            record = cursor.find(key)
            assert record
            assert record.row_id == key

    def test_random(self):
        btree = BTree(self.pager, self.pager.new())
        cursor = btree.cursor()

        keys = [3, 1, 9, 6, 4, 2, 0, 5, 7, 8]
        # keys = [n for n in range(10)]
        # random.shuffle(keys)

        # insert in random order.
        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        for key in keys:
            record = cursor.find(key)
            assert record
            assert record.row_id == key

    def test_traverse(self):
        """
        Asserts we can get all the values in leaf nodes.
        """
        btree = BTree(self.pager, self.pager.new())
        cursor = btree.cursor()

        inputs = [45, 15, 5, 35, 25]

        for n in inputs:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        # sort inputs because thats
        # the order we expect them out of scan.
        inputs.sort()
        for i, record in enumerate(cursor):
            assert record.row_id == inputs[i]

    def test_from_disk_single_page(self):
        page_number = self.pager.new()
        btree = BTree(self.pager, page_number)
        cursor = btree.cursor()

        keys = [n for n in range(2)]
        # insert in random order.
        random.shuffle(keys)

        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        loaded_tree = BTree(self.pager, page_number)
        loaded_cursor = loaded_tree.cursor()

        # Check we can search all keys.
        records = [r for r in loaded_cursor]
        assert len(records) == len(keys)

        for key in keys:
            record = loaded_cursor.find(key)
            assert record
            assert record.row_id == key

    def test_from_disk(self):
        page_number = self.pager.new()
        btree = BTree(self.pager, page_number)
        cursor = btree.cursor()

        keys = [n for n in range(100)]
        # insert in random order.
        random.shuffle(keys)

        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        loaded_tree = BTree(self.pager, page_number)
        loaded_cursor = loaded_tree.cursor()

        # Check we can search all keys.
        records = [r for r in loaded_cursor]
        assert len(records) == len(keys)

        for key in keys:
            record = loaded_cursor.find(key)
            assert record
            assert record.row_id == key

    def test_new_row_id(self):
        page_number = self.pager.new()
        btree = BTree(self.pager, page_number)
        cursor = btree.cursor()

        assert cursor.new_row_id() == 1

        keys = [n for n in range(100)]
        # insert in random order.
        random.shuffle(keys)

        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        assert cursor.new_row_id() == 100

    def test_cursor_traverse(self):
        """
        Asserts we can get all the values in leaf nodes.
        """
        btree = BTree(self.pager, self.pager.new())
        cursor = btree.cursor()
        keys = [n for n in range(10)]

        random.shuffle(keys)
        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        keys.sort()

        records = [x for x in enumerate(cursor)]
        assert len(records) == len(keys)
        for i, record in records:
            assert record
            assert record.row_id == keys[i]

    def test_cursor_current(self):
        """
        Asserts by calling next(iter)
        """
        btree = BTree(self.pager, self.pager.new())
        cursor = btree.cursor()

        keys = [n for n in range(1, 4)]

        random.shuffle(keys)
        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        keys.sort()

        records = [x for x in cursor]
        assert len(records) == 3

        cursor.reset()
        # ensure calling it twice without next
        # returns same row.
        next(cursor)
        x = cursor.current()
        y = cursor.current()

        assert x.row_id == y.row_id
        assert x.row_id == 1

        assert next(cursor).row_id == 2
        assert cursor.current().row_id == 2

        next(cursor)
        assert cursor.current().row_id == 3

        # for key in keys:
        #     next(cursor)
        #     current_row = cursor.current()

        #     assert current_row
        #     assert current_row.row_id == key

    def test_cursor_seek(self):
        """
        Asserts we can seek to a specific key
        """
        btree = BTree(self.pager, self.pager.new())
        cursor = btree.cursor()
        keys = [n for n in range(10)]

        random.shuffle(keys)
        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        keys.sort()

        cursor.seek(7)
        record = cursor.current()
        assert record
        assert record.row_id == 7

        cursor.seek_start()
        cursor.seek(3)

        record = cursor.current()
        assert record
        assert record.row_id == 3

    def test_cursor_seek_not_exists(self):
        """
        Asserts we can seek to a key
        that doesn't exist in the btree.

        It should point the cursor to the leaf node + index where it will in
        inserted.
        """
        btree = BTree(self.pager, self.pager.new())
        cursor = btree.cursor()
        keys = [1, 3, 5, 9, 11]

        random.shuffle(keys)
        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        keys.sort()

        try:
            cursor.seek(7)
        except NotFoundException:
            pass

        record = cursor.current()
        assert record
        assert record.row_id == 5

    def test_cursor_seek_end(self):
        btree = BTree(self.pager, self.pager.new())
        cursor = btree.cursor()
        total = 10
        keys = [n for n in range(total)]

        # random.shuffle(keys)
        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        keys.sort()

        cursor.seek_start()
        record = cursor.current()
        assert record
        # Last row.
        assert record.row_id == 0

        cursor.seek_end()
        print(cursor.tree.show())
        print(cursor.stack[-1])
        record = cursor.current()
        assert record
        # Last row has key 9
        assert record.row_id == total - 1
