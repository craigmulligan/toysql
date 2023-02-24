from toysql.record import Record, DataType
from toysql.page import LeafPageCell, InteriorPageCell, Page, PageType, FixedInteger
from unittest import TestCase
from os import path, SEEK_END


class TestCell(TestCase):
    def test_leaf_page_cell(self):
        payload = [
            [DataType.integer, 3],
            [DataType.integer, 124],
            [DataType.text, "Craig"],
            [DataType.null, None],
        ]
        record = Record(payload)
        cell = LeafPageCell(record)
        new_cell = LeafPageCell.from_bytes(cell.to_bytes())
        assert new_cell.record.values == payload

    def test_leaf_page_cell_handles_extra_bytes(self):
        payload = [
            [DataType.integer, 3],
            [DataType.integer, 124],
            [DataType.text, "Craig"],
            [DataType.null, None],
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
    def test_page_to_bytes(self):
        # 21000  "Programming Languages"   75    89
        # 23500  "Databases"               NULL  42
        # 27500  "Operating Systems"       NULL  89
        p = path.join("tests", "files", "databases", "1table-1page.cdb")

        expected = ""
        page_number = 1
        page_size = 0
        with open(p, "rb") as f:
            f.seek(0)
            f.seek(15)
            page_size = FixedInteger.from_bytes(f.read(2))
            f.seek(0)
            # seek past first page
            f.seek(page_size)
            expected = f.read(page_size)
            assert len(expected) == page_size

        payload = [
            [
                [DataType.integer, 21000],
                [DataType.text, "Programming Languages"],
                [DataType.integer, 75],
                [DataType.integer, 89],
            ],
            [
                [DataType.integer, 23500],
                [DataType.text, "Databases"],
                [DataType.null, None],
                [DataType.integer, 42],
            ],
            [
                [DataType.integer, 27500],
                [DataType.text, "Operating Systems"],
                [DataType.null, None],
                [DataType.integer, 89],
            ],
        ]

        leaf_page = Page(PageType.leaf, page_number, page_size=page_size)
        for p in payload:
            leaf_page.add_cell(LeafPageCell(Record(p)))

        raw_bytes = leaf_page.to_bytes()
        print(raw_bytes)
        print("-------")
        print(expected)

        assert raw_bytes == expected

    def test_leaf_page(self):
        page_number = 1

        leaf_page = Page(PageType.leaf, page_number)

        for n in range(3, 1, -1):
            payload = [
                [DataType.integer, n],
                [DataType.integer, 124],
                [DataType.text, "Craig"],
                [DataType.null, None],
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
                [DataType.integer, n],
                [DataType.integer, 124],
                [DataType.text, "Craig"],
                [DataType.null, None],
            ]
            cells.append(leaf_page.add(payload))

        assert sorted(cells) == leaf_page.cells
