from snapshottest import TestCase
import random
from toysql.t import BTree

class TestBTree(TestCase):
    def test_btree(self):
        """
        Given and ordered set of data 
        Ensure the tree is the same.
        """
        btree = BTree(order=3)
        inputs = [5, 15, 25, 35, 45]

        for n in inputs:
            btree.add(n, f'hello-{n}')

        self.assertMatchSnapshot(btree.show())       

        for key in inputs:
            record = btree.find(key)
            assert record.values[1][1] == f"hello-{key}"

    def test_random(self):
        btree = BTree(order=3)

        keys = [n for n in range(100)]
        random.shuffle(keys)

        # insert in random order.
        for key in keys: 
            btree.add(key, f'hello-{key}')
        
        for key in keys:
            x = btree.find(key)
            assert x.values[1][1] == f"hello-{key}"
