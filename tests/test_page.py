from toysql.record import Record, DataType
from toysql.page import LeafPageCell, InteriorPageCell, Page, PageType, FixedInteger
from os import path
import sqlite3


def read_file(filename, start, end):
    p = path.join("tests", "files", "databases", filename)

    with open(p, "rb") as f:
        f.seek(start)
        return f.read(end - start)


def get_page_size(filename):
    return FixedInteger.from_bytes(read_file(filename, 16, 18))


def test_interior_page_cell():
    """
    This test asserts that the interiot pages are serialized and deserialized
    according to spec.
    """
    expected_bytes = b"\x00\x00\x00g\x80\x80\x988"
    page_number = 103
    row_id = 3128

    cell = InteriorPageCell(row_id, page_number)
    assert cell.to_bytes() == expected_bytes

    cell_deserialized = InteriorPageCell.from_bytes(expected_bytes)

    assert cell_deserialized.left_child_page_number == page_number
    assert cell_deserialized.row_id == row_id


def test_leaf_page_cell():
    """
    This test asserts that the leaf page cells are serialized and deserialized
    according to spec.
    """
    expected_bytes = b"\x80\x80\x80%\x80\x81\xa4\x08\x08\x00\x80\x80\x807\x04\x04Programming Languages\x00\x00\x00K\x00\x00\x00Y"
    row_id = 21000

    record = Record(
        [
            [DataType.integer, row_id],
            [DataType.text, "Programming Languages"],
            [DataType.integer, 75],
            [DataType.integer, 89],
        ],
        row_id=row_id,
    )

    cell = LeafPageCell(record)
    assert cell.to_bytes() == expected_bytes

    cell_deserialized = LeafPageCell.from_bytes(expected_bytes)

    assert cell_deserialized.row_id == row_id


def test_schema_page():
    """
    This test asserts that the root (schema) page is serialized and deserialized
    according to spec.
    """
    filename = "1table-1page.cdb"
    page_size = get_page_size(filename)
    page_start = 0
    page_end = page_size
    expected = read_file(filename, page_start, page_end)

    schema_sql = "CREATE TABLE courses(code INTEGER PRIMARY KEY, name TEXT, prof BYTE, dept INTEGER)"

    record = Record(
        [
            [DataType.text, "table"],  # type (index or table)
            [DataType.text, "courses"],  # item name
            [DataType.text, "courses"],  # associated table name
            [DataType.integer, 2],  # root_page number
            [DataType.text, schema_sql],
        ],
        row_id=1,
    )

    root_page = Page(PageType.leaf, 0, page_size=page_size)
    root_page.add_cell(LeafPageCell(record))

    assert expected == root_page.to_bytes()


def test_leaf_page_from_bytes():
    filename = "1table-1page.cdb"
    page_size = get_page_size(filename)
    page_start = page_size
    page_end = page_size * 2

    expected = read_file(filename, page_start, page_end)

    p = Page.from_bytes(expected, 2)

    assert p.page_size == page_size
    assert p.page_type == PageType.leaf
    assert len(p.cells) == 3
    assert p.page_number == 2
    expected_rows = [
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

    expected_cells = [LeafPageCell(Record(row)) for row in expected_rows]
    assert p.cells == expected_cells


def test_interior_page_from_bytes():
    """
    This test asserts that the leaf page cells are serialized and deserialized
    according to spec.
    """
    filename = "1table-largebtree.cdb"
    page_size = get_page_size(filename)
    page_start = page_size
    page_end = page_size * 2
    right_child_page_number = 102
    expected = read_file(filename, page_start, page_end)

    payload = [[3128, 103], [6424, 157]]
    page = Page.from_bytes(expected, 2)

    assert page.right_child_page_number == right_child_page_number
    assert page.cells == [InteriorPageCell(*p) for p in payload]
    assert page.page_number == 2


def test_leaf_page():
    """
    This test asserts that the leaf pages are serialized and deserialized
    according to spec.
    """
    filename = "1table-1page.cdb"
    page_size = get_page_size(filename)
    page_start = page_size
    page_end = page_size * 2
    expected = read_file(filename, page_start, page_end)

    payload = [
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
        [
            [DataType.integer, 21000],
            [DataType.text, "Programming Languages"],
            [DataType.integer, 75],
            [DataType.integer, 89],
        ],
    ]

    leaf_page = Page(PageType.leaf, 2, page_size=page_size)
    for p in payload:
        leaf_page.add_cell(
            LeafPageCell(Record(p, row_id=p[0][1])),
        )

    assert expected == leaf_page.to_bytes()


def test_page_e2e(tmp_path):
    """
    This test writes a single schema page + leafpage and checks that
    sqlite can read it back.

    # TODO add check for page_size + schema.
    """
    page_size = 1024
    schema_sql = "CREATE TABLE courses(code INTEGER PRIMARY KEY, name TEXT, prof BYTE, dept INTEGER)"

    record = Record(
        [
            [DataType.text, "table"],  # type (index or table)
            [DataType.text, "courses"],  # item name
            [DataType.text, "courses"],  # associated table name
            [DataType.integer, 2],  # root_page number
            [DataType.text, schema_sql],
        ],
        row_id=1,
    )

    root_page = Page(PageType.leaf, 0, page_size=page_size)
    # Add the schema.
    root_page.add_cell(LeafPageCell(record))

    # Next add the actual table rows to the 1st page
    rows = [
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
        [
            [DataType.integer, 21000],
            [DataType.text, "Programming Languages"],
            [DataType.integer, 75],
            [DataType.integer, 89],
        ],
    ]

    leaf_page = Page(PageType.leaf, 1, page_size=page_size)
    for r in rows:
        leaf_page.add_cell(
            LeafPageCell(Record(r, row_id=r[0][1])),
        )

    db_filename = path.join(tmp_path, "toysql.db")

    with open(db_filename, "wb+") as db_file:
        db_file.write(root_page.to_bytes() + leaf_page.to_bytes())

    con = sqlite3.connect(db_filename)
    cursor = con.cursor()

    # Transform the input into the output format.
    # (id, language, prof, dept)
    expected_rows = sorted(
        [(r[0][1], r[1][1], r[2][1], r[3][1]) for r in rows], key=lambda r: r[0]
    )

    assert expected_rows == cursor.execute("SELECT * FROM courses").fetchall()
