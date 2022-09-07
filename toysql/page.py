from typing import Optional, List
from enum import Enum
from toysql.record import Record, Integer
from toysql.exceptions import DuplicateKeyException
import bisect
import io


class PageType(Enum):
    leaf = 0
    interior = 1


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

    def __lt__(self, other):
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
        pass
        """
        buff = io.BytesIO()
        record_bytes = self.record.to_bytes()
        record_size = Integer(len(record_bytes)).to_bytes()
        row_id = Integer(self.record.row_id).to_bytes()

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
        page_number = FixedInteger.to_bytes(4, self.left_child_page_number)
        row_id = Integer(self.row_id).to_bytes()

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
                    breakpoint()

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
            raise DuplicateKeyException(f"key already exists {cell.row_id} in page")

        bisect.insort(self.cells, cell)
        return cell

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
        header_size = self.header_size()

        [cell_offsets, cell_data] = self.cells_to_bytes()
        return header_size + len(cell_offsets) + len(cell_data)

    def cells_to_bytes(self) -> List[bytes]:
        """
        Returns the body as bytes
        """
        cell_offsets = b""
        cell_data = b""

        for cell in self.cells:
            cell_bytes = cell.to_bytes()
            # Add offset for each cell from the cell Content area.
            # TODO this isn't a true offset. But it makes it easy to read
            # All of them.
            offset = FixedInteger.to_bytes(2, len(cell_bytes))
            cell_offsets += offset
            cell_data += cell_bytes

        return [cell_offsets, cell_data]

    def to_bytes(self) -> bytes:
        """
        Page header: https://www.sqlite.org/fileformat.html#:~:text=B%2Dtree%20Page%20Header%20Format
        """
        buff = io.BytesIO(b"".ljust(self.page_size, b"\0"))
        [cell_offsets, cell_data] = self.cells_to_bytes()

        cell_content_offset = len(cell_data)

        # Seek negative offset of cell_content area.
        buff.seek(-cell_content_offset, 2)
        buff.write(cell_data)

        buff.seek(0)
        # Header
        buff.write(FixedInteger.to_bytes(1, self.page_number))
        # Header type.
        buff.write(FixedInteger.to_bytes(1, self.page_type.value))
        # Free block pointer. (Not implemented)
        buff.write(FixedInteger.to_bytes(2, 0))
        # Number of cells.
        buff.write(FixedInteger.to_bytes(2, len(self.cells)))
        # Cell Content Offset
        buff.write(FixedInteger.to_bytes(2, cell_content_offset))

        if self.page_type == PageType.interior:
            buff.write(FixedInteger.to_bytes(4, self.right_child_page_number))

        # Right after the header we add the cell_offsets
        buff.write(cell_offsets)

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
        page_number = FixedInteger.from_bytes(buffer.read(1))
        page_type = PageType(FixedInteger.from_bytes(buffer.read(1)))
        _free_block_pointer = PageType(FixedInteger.from_bytes(buffer.read(2)))
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
