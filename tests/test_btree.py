from toysql.record import DataType
from toysql.btree import BPlusTree 
from toysql.page import LeafPageCell
from toysql.exceptions import NotFoundException
from toysql.pager import Pager
from unittest import TestCase
import tempfile

class TestBTree(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_file_path = self.temp_dir.name + "/__testdb__.db"

        def create_pager(page_size):
            return Pager(self.db_file_path, page_size=page_size)

        self.create_pager = create_pager

        return super().setUp()

    def cleanUp(self) -> None:
        self.temp_dir.cleanup()


    def test_root_node(self):
        pager = self.create_pager(100)
        page_number = pager.new()
        tree = BPlusTree(page_number, pager)

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
