from typing import Optional, List
from enum import Enum
from toysql.record import Record, Integer
from toysql.datatypes import uint8, uint16, uint32, varint_32
import bisect
import io


class PageType(Enum):
    leaf = 13
    interior = 5


class FixedInteger:
    @staticmethod
    def to_bytes(length, value):
        return value.to_bytes(length, "big")

    @staticmethod
    def from_bytes(data):
        return int.from_bytes(data, "big")


class Cell:
    """
    Cell interface
    """

    row_id = 0

    def to_bytes(self):
        return b""

    def __eq__(self, other: "Cell") -> bool:
        return self.row_id == other.row_id

    def __lt__(self, other: "Cell"):
        return self.row_id < other.row_id

    def __len__(self):
        return len(self.to_bytes())


class LeafPageCell(Cell):
    """
    A cell is a wrapper for the Record format
    which adds some metadata depending on the surrounding

    A cell should be sortable by key. (PK)
    """

    def __init__(self, payload: Record) -> None:
        if isinstance(payload, Record):
            self.record = payload
        else:
            self.record = Record(payload)

    @property
    def row_id(self):
        return self.record.row_id

    def __eq__(self, o: "LeafPageCell") -> bool:
        return self.record == o.record

    def to_bytes(self):
        """
        Cell contains a record
        """
        buff = io.BytesIO()
        record_bytes = self.record.to_bytes()
        record_size = varint_32(len(record_bytes))
        row_id = varint_32(self.record.row_id)

        buff.write(record_size)
        buff.write(row_id)
        buff.write(record_bytes)

        buff.seek(0)
        return buff.read()

    @staticmethod
    def from_bytes(data) -> "LeafPageCell":
        """
        First read two varints record_size + row_id
        Then read the record payload
        """
        buff = io.BytesIO(data)
        record_size = Integer.from_bytes(buff.read())
        buff.seek(record_size.content_length())
        row_id = Integer.from_bytes(buff.read())
        buff.seek(record_size.content_length() + row_id.content_length())
        record = Record.from_bytes(buff.read(record_size.value))

        return LeafPageCell(record)


class InteriorPageCell(Cell):
    """
    Table B-Tree Interior Cell (header 0x05):

    A 4-byte big-endian page number which is the left child pointer.
    A varint which is the integer key.
    """

    def __init__(self, row_id, left_child_page_number) -> None:
        self.row_id = row_id
        self.left_child_page_number = left_child_page_number

    def __eq__(self, o: "InteriorPageCell") -> bool:
        return self.row_id == o.row_id

    def to_bytes(self):
        buff = io.BytesIO()
        page_number = uint32(self.left_child_page_number)
        row_id = varint_32(self.row_id)

        buff.write(page_number)
        buff.write(row_id)

        buff.seek(0)
        return buff.read()

    @staticmethod
    def from_bytes(data) -> "InteriorPageCell":
        """
        Just reading the left_child_page and the varint.
        """
        buff = io.BytesIO(data)

        left_child_page_number = FixedInteger.from_bytes(buff.read(4))
        row_id = Integer.from_bytes(buff.read()).value

        return InteriorPageCell(row_id, left_child_page_number)


class Page:
    """
    header is 8 bytes in size for leaf pages and 12 bytes for interior pages

    Cells are expected to be sorted before hand useing cells.sort()
    """

    parent: Optional["Page"]

    def __init__(
        self,
        page_type,
        page_number,
        cells=None,
        right_child_page_number=None,
        page_size=4096,
    ) -> None:
        self.page_type = PageType(page_type)
        self.page_number = page_number
        self.cells = cells or []

        self.parent = None
        # Only for Interior Pages
        self.right_child_page_number = right_child_page_number
        self.page_size = page_size

    def __repr__(self):
        cell_ids = [str(cell.row_id) for cell in self.cells]
        return ",".join(cell_ids)

    def is_full(self) -> bool:
        if len(self) >= self.page_size:
            return True

        return False

    def show(self, counter, read_page):
        """Prints the keys at each level."""
        output = counter * "\t"

        if not self.is_leaf():
            output += str(self)
            output += "\n"
            counter += 1
            for cell in self.cells:
                try:
                    output += read_page(cell.left_child_page_number).show(
                        counter, read_page
                    )
                except:
                    pass

            output += read_page(self.right_child_page_number).show(counter, read_page)

        else:
            # Green is the leaf values
            output += (
                "\033[1;32m "
                + ", ".join(str(cell.row_id) for cell in self.cells)
                + "\033[0m"
            )

        output += "\n"
        return output

    def is_leaf(self):
        return self.page_type == PageType.leaf

    def add(self, *args, **kwargs):
        """
        convencience wrapper for add_cell
        """
        if self.page_type == PageType.leaf:
            record = Record(*args, **kwargs)
            cell = LeafPageCell(record)
            self.add_cell(cell)
            return cell

        if self.page_type == PageType.interior:
            cell = InteriorPageCell(*args, **kwargs)
            self.add_cell(cell)
            return cell

    def add_cell(self, cell):
        """
        First we need to see if there is space to write the new values.
        if there is we add it if not we raise PageFullException
        """
        # now check the key doesnt exist.
        exists = self.find_cell(cell.row_id)

        if exists:
            self.remove_cell(exists)

        # bisect.insort(self.cells, cell)
        self.cells.append(cell)

        return cell

    def remove_cell(self, cell):
        self.cells.remove(cell)

    def find_cell(self, row_id):
        for cell in self.cells:
            if cell.row_id == row_id:
                return cell

        return None

    def header_size(self):
        if self.page_type == PageType.leaf:
            return 8

        return 12

    def __len__(self):
        # TODO can't use to_bytes because it always returns page size.
        return len(self.to_bytes())

    def to_bytes(self) -> bytes:
        """
        Page header: https://www.sqlite.org/fileformat.html#:~:text=B%2Dtree%20Page%20Header%20Format
        """
        # create a buffer of page_size
        buff = io.BytesIO(b"".ljust(self.page_size, b"\0"))

        # get_header size dependent on page_type
        header_size = self.header_size()

        cell_data = []
        for cell in self.cells:
            cell_data.append(cell.to_bytes())

        cell_data_len = len(b"".join(cell_data))
        free_area_index = header_size + (len(self.cells) * 2)

        # get_header size dependent on page_type
        cell_content_offset = self.page_size - cell_data_len

        # Seek negative offset of cell_content area.
        buff.seek(cell_content_offset)

        cell_offsets = []

        for cell_bytes in cell_data:
            cell_offsets.append(buff.tell())
            buff.write(cell_bytes)

        # now right the header.
        buff.seek(0)
        # Page type.
        buff.write(uint8(self.page_type.value))
        # Free area start
        buff.write(uint16(free_area_index))
        # Number of cells.
        buff.write(uint16(len(self.cells)))
        # Cell Content Offset
        buff.write(uint16(cell_content_offset))

        if self.page_type == PageType.interior and self.right_child_page_number:
            buff.write(uint32(self.right_child_page_number))

        # but 7 must be Something sqlite needs but we don't
        buff.write(uint8(0))

        # Right after the header we add the cell_offsets
        for offset in cell_offsets:
            buff.write(uint16(offset))

        # Go to the beginning so we can read everything.
        buff.seek(0)

        return buff.read()

    @staticmethod
    def cell_from_bytes(page_type, raw_bytes):
        if page_type == PageType.leaf:
            return LeafPageCell.from_bytes(raw_bytes)
        if page_type == PageType.interior:
            return InteriorPageCell.from_bytes(raw_bytes)

        raise Exception(f"Unknown page type {page_type}")

    @staticmethod
    def from_bytes(data) -> "Page":
        buffer = io.BytesIO(data)
        # page_number = FixedInteger.from_bytes(buffer.read(1))
        page_type = PageType(FixedInteger.from_bytes(buffer.read(1)))
        # Free block pointer.
        _ = PageType(FixedInteger.from_bytes(buffer.read(2)))
        number_of_cells = FixedInteger.from_bytes(buffer.read(2))
        cell_content_offset = FixedInteger.from_bytes(buffer.read(2))

        right_child_page_number = None

        # This is the right most child pointer. All the other pointers
        # are in an InteriorPageCell[key, pointer] but the right most
        # one is stored seperately.
        if page_type == PageType.interior:
            right_child_page_number = FixedInteger.from_bytes(buffer.read(4))

        cells = []

        # Cell pointers
        cell_offsets = []

        for _ in range(number_of_cells):
            cell_offsets.append(FixedInteger.from_bytes(buffer.read(2)))

        # Now read cells
        buffer.seek(-cell_content_offset, 2)

        for offset in cell_offsets:
            cell_content = buffer.read(offset)
            cell = Page.cell_from_bytes(page_type, cell_content)
            cells.append(cell)

        return Page(
            page_type,
            page_number,
            cells=cells,
            right_child_page_number=right_child_page_number,
        )
