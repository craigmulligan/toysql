from snapshottest import TestCase
import random
from toysql.btree import BTree
from toysql.pager import Pager
import tempfile

class TestBTree(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_file_path = self.temp_dir.name + "/__testdb__.db"
        self.pager = Pager(self.db_file_path)
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
            btree.add(n, f'hello-{n}')

        self.assertMatchSnapshot(btree.show())       
        
        for key in inputs:
            record = btree.find(key)
            assert record.values[1][1] == f"hello-{key}"

    def test_random(self):
        btree = BTree(3, self.pager)

        keys = [n for n in range(100)]
        random.shuffle(keys)

        # insert in random order.
        for key in keys: 
            btree.add(key, f'hello-{key}')
        
        for key in keys:
            x = btree.find(key)
            assert x.values[1][1] == f"hello-{key}"
