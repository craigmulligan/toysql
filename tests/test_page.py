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


class TestPage(TestCase):
    def test_left_page(self):
        cells = []
        page_number = 1

        for n in range(3):
            payload = [
                [DataType.INTEGER, n],
                [DataType.INTEGER, 124],
                [DataType.TEXT, "Craig"],
                [DataType.NULL, None]
            ]
            record = Record(payload)
            cells.append(LeafPageCell(record))

        leaf_page = Page(page_number, PageType.leaf, cells)
        raw_bytes = leaf_page.to_bytes()
        new_leaf_page = Page.from_bytes(raw_bytes) 
        assert new_leaf_page.page_number == page_number
        assert new_leaf_page.page_type == PageType.leaf
        # assert new_leaf_page.cells == cells

