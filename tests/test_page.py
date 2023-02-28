import pytest
from toysql.record import Record, DataType
from toysql.page import LeafPageCell, InteriorPageCell, Page, PageType, FixedInteger
from os import path


def get_file_data(filename, start, end):
    p = path.join("tests", "files", "databases", filename)

    with open(p, "rb") as f:
        f.seek(start)
        return f.read(end - start)


@pytest.mark.skip("TODO")
def test_leaf_page_cell():
    """
    This test asserts that the leaf page cells are serialized and deserialized
    according to spec.
    """
    pass


@pytest.mark.skip("TODO")
def test_interior_page_cell():
    """
    This test asserts that the leaf page cells are serialized and deserialized
    according to spec.
    """
    pass


def test_leaf_page():
    """
    This test asserts that the leaf pages are serialized and deserialized
    according to spec.
    """

    page_size = 1024
    page_start = page_size
    page_end = page_size * 2
    expected = get_file_data("1table-1page.cdb", page_start, page_end)

    payload = [
        [
            [DataType.integer, 21000],
            [DataType.text, "Programming Languages"],
            [DataType.integer, 75],
            [DataType.integer, 89],
        ],
        [
            [DataType.integer, 27500],
            [DataType.text, "Operating Systems"],
            [DataType.null, None],
            [DataType.integer, 89],
        ],
        [
            [DataType.integer, 23500],
            [DataType.text, "Databases"],
            [DataType.null, None],
            [DataType.integer, 42],
        ],
    ]

    leaf_page = Page(PageType.leaf, 1, page_size=page_size)
    for p in payload:
        leaf_page.add_cell(LeafPageCell(Record(p)))

    assert leaf_page.to_bytes() == expected


@pytest.mark.skip("TODO")
def test_interior_page():
    """
    This test asserts that the interiot pages are serialized and deserialized
    according to spec.
    """
    pass


@pytest.mark.skip("TODO")
def test_schema_page():
    """
    This test asserts that the root (schema) page is serialized and deserialized
    according to spec.
    """
    pass


# class TestPage(TestCase):
#     def test_interior_page_new(self):
#         pass


#         # header_size = leaf_page.header_size()

#         # print("current")
#         # print(raw_bytes[913:958])
#         # print("-------")
#         # print("expected")
#         # print(expected[913:958])

#         # # print(FixedInteger.from_bytes(expected[5:7]))
#         # # print(FixedInteger.from_bytes(raw_bytes[5:7]))
#         # # assert raw_bytes[0:header_size] == expected[0:header_size]
#         # # assert b"\x00\x00'\x00e\x00" == b'\x03\x91\x03\xe3\x03\xbe'
#         # start = header_size
#         # stop = header_size + (2 * 3)

#         # for i in range(start, stop, 2):
#         #     v = FixedInteger.from_bytes(raw_bytes[i : i + 2])
#         #     x = FixedInteger.from_bytes(expected[i : i + 2])
#         #     print(i, i + 2, v, x)

#         assert raw_bytes == expected

#     def test_leaf_page(self):
#         page_number = 1

#         leaf_page = Page(PageType.leaf, page_number)

#         for n in range(3, 1, -1):
#             payload = [
#                 [DataType.integer, n],
#                 [DataType.integer, 124],
#                 [DataType.text, "Craig"],
#                 [DataType.null, None],
#             ]
#             leaf_page.add_cell(LeafPageCell(Record(payload)))

#         raw_bytes = leaf_page.to_bytes()
#         new_leaf_page = Page.from_bytes(raw_bytes)
#         assert new_leaf_page.page_number == page_number
#         assert new_leaf_page.page_type == PageType.leaf
#         assert len(new_leaf_page.cells) == len(leaf_page.cells)

#         for i, cell in enumerate(new_leaf_page.cells):
#             assert cell == leaf_page.cells[i]

#     def test_interior_page(self):
#         """
#         Bug where only adding one cell caused issues.
#         """
#         interior_page = Page(PageType.interior, 2, right_child_page_number=3)

#         for n in [5, 3, 1]:
#             interior_page.add_cell(InteriorPageCell(n, n + 1))

#         raw_bytes = interior_page.to_bytes()
#         new_interior_page = Page.from_bytes(raw_bytes)

#         for i, cell in enumerate(new_interior_page.cells):
#             assert cell == interior_page.cells[i]

#     def test_page_order(self):
#         leaf_page = Page(PageType.leaf, 0)
#         cells = []

#         # Add cells in reverse order by PK
#         for n in range(5, 0, -1):
#             payload = [
#                 [DataType.integer, n],
#                 [DataType.integer, 124],
#                 [DataType.text, "Craig"],
#                 [DataType.null, None],
#             ]
#             cells.append(leaf_page.add(payload))

#         assert sorted(cells) == leaf_page.cells
# class TestCell(TestCase):
#     def test_leaf_page_cell(self):
#         payload = [
#             [DataType.integer, 3],
#             [DataType.integer, 124],
#             [DataType.text, "Craig"],
#             [DataType.null, None],
#         ]
#         record = Record(payload)
#         cell = LeafPageCell(record)
#         new_cell = LeafPageCell.from_bytes(cell.to_bytes())
#         assert new_cell.record.values == payload

#     def test_leaf_page_cell_handles_extra_bytes(self):
#         payload = [
#             [DataType.integer, 3],
#             [DataType.integer, 124],
#             [DataType.text, "Craig"],
#             [DataType.null, None],
#         ]
#         record = Record(payload)
#         cell = LeafPageCell(record)
#         raw_bytes = cell.to_bytes() + b"x05"
#         new_cell = LeafPageCell.from_bytes(raw_bytes)
#         assert new_cell.record.values == payload

#     def test_interior_page_cell(self):
#         cell = InteriorPageCell(3, 12)
#         raw_bytes = cell.to_bytes()
#         new_cell = InteriorPageCell.from_bytes(raw_bytes)
#         assert new_cell.row_id == 3
#         assert new_cell.left_child_page_number == 12
