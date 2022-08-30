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
        
        # assert len(btree.root.cells) == 2
        # assert [cell.row_id for cell in btree.root.cells] == [25, 45]
        # print(btree.show())
        # assert False

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

    def test_page(self):
        n = Page(PageType.interior)
        left = Page(PageType.leaf)
        right = Page(PageType.leaf)
        
        n.add(InteriorPageCell(16, left)) 
        n.right_child = right
        x = n.find(16)
        assert x == left 

        x = n.find(18)
        assert x == right

        x = n.find(14)
        assert x == left 

    def test_page_multi_keys(self):
        n = Page(PageType.interior)
        left = Page(PageType.leaf)
        right = Page(PageType.leaf)
        middle = Page(PageType.leaf)
        
        n.add(InteriorPageCell(16, left))
        n.add(InteriorPageCell(18, middle))
        n.right_child = right

        x = n.find(16)
        assert x == left 

        x = n.find(17)
        assert x == middle 

        x = n.find(14)
        assert x == left 

        x = n.find(19)
        assert x == right 
