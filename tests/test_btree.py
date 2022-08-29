from unittest import TestCase
import random
from toysql.t import BTree, Page, PageType, InteriorPageCell, LeafPageCell

class TestBTree(TestCase):
    def test_btree_x(self):
        order = 3
        btree = BTree(order)
        inputs = [6, 16, 26, 36, 46]

        for n in inputs:
            btree.add(n, f'hello-{n}')

        # Basic search
        print(btree.show())
        key = 36
        x = btree.find(key)
        assert x.values[1][1] == f"hello-{key}"
        assert False


    def test_max(self):
        order = 3
        btree = BTree(order)

        for n in range(1000): 
            key = random.randint(0, 1000)
            btree.add(n, f'hello-{key}')

        btree.show()  


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
