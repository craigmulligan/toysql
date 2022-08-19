from toysql.record import Record, DataType
from toysql.page import LeafPageCell, InteriorPageCell, Page, PageType
from unittest import TestCase

class TestCell(TestCase):
    def test_leaf_page_cell(self):
        payload = [
            [DataType.INTEGER, 3],
            [DataType.INTEGER, 124],
            [DataType.TEXT, "Craig"],
            [DataType.NULL, None]
        ]
        record = Record(payload)
        cell = LeafPageCell(record) 
        new_cell = LeafPageCell.from_bytes(cell.to_bytes())
        assert new_cell.record.values == payload

    def test_leaf_page_cell_handles_extra_bytes(self):
        payload = [
            [DataType.INTEGER, 3],
            [DataType.INTEGER, 124],
            [DataType.TEXT, "Craig"],
            [DataType.NULL, None]
        ]
        record = Record(payload)
        cell = LeafPageCell(record)
        raw_bytes = cell.to_bytes() + b"x05"
        new_cell = LeafPageCell.from_bytes(raw_bytes)
        assert new_cell.record.values == payload


    def test_interior_page_cell(self):
        cell = InteriorPageCell(3, Page(12, PageType.interior)) 
        raw_bytes = cell.to_bytes()
        new_cell = InteriorPageCell.from_bytes(raw_bytes)
        assert new_cell.row_id == 3
        assert new_cell.left_child.page_number == 12
