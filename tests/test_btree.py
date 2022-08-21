from toysql.record import DataType
from toysql.btree import BPlusTree 
from toysql.page import LeafPageCell
from toysql.exceptions import NotFoundException
from unittest import TestCase

class TestBTree(TestCase):
    def test_root_node(self):
        tree = BPlusTree(0)

        for n in range(3):
            payload = [
                [DataType.INTEGER, n],
                [DataType.INTEGER, 124],
                [DataType.TEXT, "Craig"],
                [DataType.NULL, None]
            ]
            tree.add(payload)

        # get the record with
        cell = tree.search(1)
        assert isinstance(cell, LeafPageCell)
        assert cell.record.row_id == 1
        assert cell.record.values[0][1] == 1

        with self.assertRaises(NotFoundException):
            tree.search(22)
