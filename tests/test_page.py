from toysql.record import Record, DataType
from toysql.page import LeafPageCell, InteriorPageCell, Page, PageType
from unittest import TestCase
from toysql.exceptions import DuplicateKeyException


class TestCell(TestCase):
    def test_leaf_page_cell(self):
        payload = [
            [DataType.INTEGER, 3],
            [DataType.INTEGER, 124],
            [DataType.TEXT, "Craig"],
            [DataType.NULL, None],
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
            [DataType.NULL, None],
        ]
        record = Record(payload)
        cell = LeafPageCell(record)
        raw_bytes = cell.to_bytes() + b"x05"
        new_cell = LeafPageCell.from_bytes(raw_bytes)
        assert new_cell.record.values == payload

    def test_interior_page_cell(self):
        cell = InteriorPageCell(3, 12)
        raw_bytes = cell.to_bytes()
        new_cell = InteriorPageCell.from_bytes(raw_bytes)
        assert new_cell.row_id == 3
        assert new_cell.left_child_page_number == 12


class TestPage(TestCase):
    def test_leaf_page(self):
        cells = []
        page_number = 1

        leaf_page = Page(PageType.leaf, page_number)

        for n in range(3, 1, -1):
            payload = [
                [DataType.INTEGER, n],
                [DataType.INTEGER, 124],
                [DataType.TEXT, "Craig"],
                [DataType.NULL, None],
            ]
            leaf_page.add_cell(LeafPageCell(Record(payload)))

        raw_bytes = leaf_page.to_bytes()
        new_leaf_page = Page.from_bytes(raw_bytes)
        assert new_leaf_page.page_number == page_number
        assert new_leaf_page.page_type == PageType.leaf
        assert len(new_leaf_page.cells) == len(leaf_page.cells)

        for i, cell in enumerate(new_leaf_page.cells):
            assert cell == leaf_page.cells[i]

    def test_interior_page(self):
        """
        Bug where only adding one cell caused issues.
        """
        interior_page = Page(PageType.interior, 2, right_child_page_number=3)
        cells = []

        for n in [5, 3, 1]:
            interior_page.add_cell(InteriorPageCell(n, n + 1))

        raw_bytes = interior_page.to_bytes()
        new_interior_page = Page.from_bytes(raw_bytes)

        for i, cell in enumerate(new_interior_page.cells):
            assert cell == interior_page.cells[i]

    def test_page_order(self):
        leaf_page = Page(PageType.leaf, 0)
        cells = []

        # Add cells in reverse order by PK
        for n in range(5, 0, -1):
            payload = [
                [DataType.INTEGER, n],
                [DataType.INTEGER, 124],
                [DataType.TEXT, "Craig"],
                [DataType.NULL, None],
            ]
            cells.append(leaf_page.add(payload))

        assert sorted(cells) == leaf_page.cells

    def test_duplicate_row_id(self):
        leaf_page = Page(PageType.leaf, 1)
        payload = [
            [DataType.INTEGER, 3],
        ]

        leaf_page.add(payload)

        with self.assertRaises(DuplicateKeyException):
            leaf_page.add(payload)
