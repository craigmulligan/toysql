from unittest import TestCase
import random
from toysql.t import BTree, Page

class TestBTree(TestCase):
    def test_btree(self):
        order = 3
        btree = BTree(order)
        inputs = [6, 16, 26, 36, 46]

        for n in inputs:
            btree.add(n, f'hello-{n}')

        # Basic search
        key = 36
        x = btree.find(key)
        assert x == f"hello-{key}"


    def test_max(self):
        order = 3
        btree = BTree(order)

        for n in range(1000): 
            key = random.randint(0, 1000)
            btree.add(n, f'hello-{key}')

        btree.show()  


    def test_page(self):
        n = Page(False)
        left = Page(True)
        right = Page(True)
        n.keys = [16]
        n.children = [left, right] 
        x = n.find(16)
        assert x == left 

        x = n.find(18)
        assert x == right

        x = n.find(14)
        assert x == left 
