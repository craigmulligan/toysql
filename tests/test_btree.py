import random
from snapshottest import TestCase

from toysql.btree import BTree
from toysql.exceptions import NotFoundException
from toysql.record import Record, DataType
from tests.fixtures import Fixtures
from unittest.mock import patch


def is_full(page):
    """
    This is so that we don't need to insert
    a bunch of rows for the Page to be considered
    full.
    """
    if len(page.cells) >= 3:
        return True
    return False


@patch("toysql.page.Page.is_full", is_full)
class TestBTree(Fixtures, TestCase):
    def setUp(self) -> None:
        def create_record(row_id: int, text: str):
            return Record([[DataType.integer, row_id], [DataType.text, text]])

        self.create_record = create_record
        return super().setUp()

    def test_btree(self):
        """
        Given and ordered set of data
        Ensure the tree is the same.
        """

        def is_full(page):
            if len(page.cells) >= 3:
                return True
            return False

        with patch("toysql.page.Page.is_full", is_full):
            btree = BTree(self.pager, self.pager.new())
            inputs = [5, 15, 25, 35, 45]

            for n in inputs:
                btree.insert(self.create_record(n, f"hello-{n}"))

            self.assertMatchSnapshot(btree.show())

            for key in inputs:
                record = btree.find(key)
                assert record
                assert record.row_id == key

    def test_random(self):
        cursor = BTree(self.pager, self.pager.new())

        keys = [n for n in range(10)]
        random.shuffle(keys)

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
        cursor = BTree(self.pager, self.pager.new())

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
        cursor = BTree(self.pager, page_number)

        keys = [n for n in range(2)]
        # insert in random order.
        random.shuffle(keys)

        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        loaded_cursor = BTree(self.pager, page_number)

        # Check we can search all keys.
        records = [r for r in loaded_cursor]
        assert len(records) == len(keys)

        for key in keys:
            record = loaded_cursor.find(key)
            assert record
            assert record.row_id == key

    def test_from_disk(self):
        page_number = self.pager.new()
        cursor = BTree(self.pager, page_number)

        keys = [n for n in range(100)]
        # insert in random order.
        random.shuffle(keys)

        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        loaded_cursor = BTree(self.pager, page_number)

        # Check we can search all keys.
        records = [r for r in loaded_cursor]
        assert len(records) == len(keys)

        for key in keys:
            record = loaded_cursor.find(key)
            assert record
            assert record.row_id == key

    def test_cursor_traverse(self):
        """
        Asserts we can get all the values in leaf nodes.
        """
        cursor = BTree(self.pager, self.pager.new())
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
        cursor = BTree(self.pager, self.pager.new())

        keys = [n for n in range(1, 3)]

        random.shuffle(keys)
        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        keys.sort()

        rows = [x for x in cursor]
        cursor.reset()
        # ISSUE with inserting 2 rows.
        assert len(rows) == 2

        assert cursor.current().row_id == 1
        assert cursor.current().row_id == 1

        assert next(cursor).row_id == 2
        assert cursor.current().row_id == 2

        with self.assertRaises(StopIteration):
            next(cursor)

    def test_cursor_seek(self):
        """
        Asserts we can seek to a specific key
        """
        cursor = BTree(self.pager, self.pager.new())
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
        cursor = BTree(self.pager, self.pager.new())
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
        cursor = BTree(self.pager, self.pager.new())
        total = 10
        keys = [n for n in range(total)]

        random.shuffle(keys)
        for n in keys:
            cursor.insert(self.create_record(n, f"hello-{n}"))

        keys.sort()

        cursor.seek_start()
        record = cursor.current()
        assert record
        # Last row.
        assert record.row_id == 0

        cursor.seek_end()
        record = cursor.current()
        assert record
        # Last row has key 9
        assert record.row_id == total - 1
