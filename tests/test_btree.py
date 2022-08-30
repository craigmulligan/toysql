from unittest import TestCase
import random
from toysql.t import BTree, Page, PageType, InteriorPageCell, LeafPageCell

class TestBTree(TestCase):
    def test_btree_x(self):
        order = 3
        btree = BTree(order)
        inputs = [5, 15, 25, 35, 45]

        for n in inputs[:2]:
            btree.add(n, f'hello-{n}')

        # Basic search only a root node.
        key = 15
        x = btree.find(key)
        assert x.values[1][1] == f"hello-{key}"
        assert btree.root.is_leaf()

        btree.add(25, f"hello-{key}")
        key = 15
        x = btree.find(key)
        assert x.values[1][1] == f"hello-{key}"
        assert not btree.root.is_leaf()
        assert [cell.row_id for cell in btree.root.cells] == [15]

        btree.add(35, f"hello-{key}")
        assert len(btree.root.cells) == 2
        assert [cell.row_id for cell in btree.root.cells] == [15, 25]

        btree.add(45, f"hello-{key}")

        assert len(btree.root.cells) == 1
        assert [cell.row_id for cell in btree.root.cells] == [25]

        print(btree.show())

    def test_max(self):
        order = 3
        btree = BTree(order)

        #keys = [n for n in range(100)]         
        #random.shuffle(keys)
        keys = [36, 55, 68, 51, 19, 43, 48, 25, 84, 16, 33, 95, 58, 92, 22, 5, 57, 35, 90, 82, 2, 10, 89, 12, 27, 50, 21, 49, 26, 94, 88, 96, 34, 37, 61, 81, 85, 46, 11, 87, 79, 59, 53, 8, 44, 64, 83, 40, 31, 56, 67, 38, 14, 71, 54, 45, 98, 75, 1, 47, 86, 91, 15, 32, 66, 18, 41, 65, 0, 6, 9, 17, 39, 74, 62, 72, 23, 99, 97, 28, 29, 52, 30, 93, 7, 42, 4, 73, 80, 13, 63, 24, 76, 78, 69, 70, 20, 77, 60, 3]

        for key in keys: 
            btree.add(key, f'hello-{key}')
        
        print(btree.show())
        # key = random.choice(keys)
        key = 94
        x = btree.find(key)
        assert x.values[1][1] == f"hello-{key}"
