from snapshottest import TestCase
import random
from toysql.btree import BTree
from toysql.pager import Pager
from toysql.record import Record, DataType
import tempfile

class TestBTree(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_file_path = self.temp_dir.name + "/__testdb__.db"
        self.pager = Pager(self.db_file_path)

        def create_record(row_id: int, text: str):
            return Record([[DataType.INTEGER, row_id], [DataType.TEXT, text]])

        self.create_record = create_record
        return super().setUp()

    def cleanUp(self) -> None:
        self.temp_dir.cleanup()

    def test_btree(self):
        """
        Given and ordered set of data 
        Ensure the tree is the same.
        """
        btree = BTree(3, self.pager)
        inputs = [5, 15, 25, 35, 45]

        for n in inputs:
            btree.insert(self.create_record(n, f'hello-{n}'))

        self.assertMatchSnapshot(btree.show())       
        
        for key in inputs:
            record = btree.find(key)
            assert record
            assert record.row_id == key

    def test_random(self):
        btree = BTree(3, self.pager)

        keys = [n for n in range(100)]
        random.shuffle(keys)

        # insert in random order.
        for n in keys: 
            btree.insert(self.create_record(n, f'hello-{n}'))
        
        for key in keys:
            record = btree.find(key)
            assert record
            assert record.row_id == key

    def test_traverse(self):
        """
        Asserts we can get all the values in leaf nodes.
        """
        btree = BTree(3, self.pager)
        inputs = [45, 15, 5, 35, 25]

        for n in inputs:
            btree.insert(self.create_record(n, f'hello-{n}'))

        # sort inputs because thats
        # the order we expect them out of scan.
        inputs.sort()
        for i, record in enumerate(btree.scan()):
            assert record.row_id == inputs[i]


    def test_from_disk(self):
        btree = BTree(3, self.pager)

        keys = [n for n in range(100)]
        # insert in random order.
        random.shuffle(keys)

        for n in keys: 
            btree.insert(self.create_record(n, f'hello-{n}'))

        loaded_tree = BTree(3, self.pager)

        # Check we can search all keys.
        for key in keys:
            record = btree.find(key)
            assert record
            assert record.row_id == key
