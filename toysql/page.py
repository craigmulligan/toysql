from typing import Optional, List
from enum import Enum
from toysql.record import Record, Integer
from toysql.exceptions import CellNotFoundException, PageFullException
import io

class PageType(Enum): 
    leaf = 0
    interior = 1


class FixedInteger():
    @staticmethod
    def to_bytes(length, value):
        return value.to_bytes(length, "big")

    @staticmethod
    def from_bytes(data):
        return int.from_bytes(data, "big")


class Cell:
    def to_bytes(self):
        return b"" 

    def __len__(self):
        return len(self.to_bytes())


class LeafPageCell(Cell):
    """
    A cell is a wrapper for the Record format 
    which adds some metadata depending on the surrounding 

    A cell should be sortable by key. (PK)
    """
    def __init__(self,  payload:Record) -> None:
        self.record = payload

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
    A cell is a wrapper for the Record format 
    which adds some metadata depending on the surrounding 

    This is an Interior B+Tree page.

    Contains:
    A 4-byte big-endian page number which is the left child pointer
    A varint which is the integer key
    
    https://sqlite.org/src4/doc/trunk/www/bt.wiki#cell_formats:~:text=Number%20of%20bytes%20in,prefix%20as%20a%20varint.
    Number of bytes in the key-prefix (nKey), as a varint. Or, if the key-prefix for this cell is too large to be stored on an internal node (see above), zero.
    nKey bytes of key data.
    Page number of b-tree page containing keys equal to or smaller than the key-prefix as a varint.

    For internal pages, each cell contains a 4-byte child pointer; 

    The internal nodes store search navigational information and leaf nodes tuples. There is an upper and a lower bound on the number of entries an internal node can have. This chapter gives an overview of how point lookup, search next, insert, and delete operations are performed on B+-trees. 
    

    stores keys and pointers to children
    
    Number of keys: up to m-1

    Number of pointers: number of keys + 1

    Table B-Tree Interior Cell (header 0x05):

    A 4-byte big-endian page number which is the left child pointer.
    A varint which is the integer key
    """
    def __init__(self, row_id, left_child: Optional["Page"]=None) -> None:
        self.row_id = row_id
        self.left_child = left_child 

    def __eq__(self, o: "InteriorPageCell") -> bool:
        return self.row_id == o.row_id

    def to_bytes(self):
        buff = io.BytesIO()
        left_child_page_number = 0
        if self.left_child:
            left_child_page_number = self.left_child.page_number

        page_number = FixedInteger.to_bytes(4, left_child_page_number)
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

        return InteriorPageCell(row_id, Page(left_child_page_number, PageType.interior))


class Page:
    """
    header is 8 bytes in size for leaf pages and 12 bytes for interior pages
    """
    def __init__(self, page_number, page_type, cells=None) -> None:
        self.page_number = page_number
        self.page_type = PageType(page_type)
        self.cell_content_offset = 65536
        self.cells = cells or []

    def add(self, *args, **kwargs):
        """
            First we need to see if there is space to write the new values. 
            if there is we add it if not we raise PageFullException
        """
        remaining_space = 4096 - len(self)
        cell = None

        if self.page_type == PageType.leaf:
            record = Record(*args, **kwargs)
            cell = LeafPageCell(record)

        if self.page_type == PageType.interior:
            cell = InteriorPageCell(*args, **kwargs)

        if cell is None:
            raise Exception(f"Unknown page_type {self.page_type}")

        if len(cell) > remaining_space:
            raise PageFullException(f"No space left in page: {self.page_number}")

        self.cells.append(cell)
        return cell

    def search(self, row_id):
        for cell in self.cells:
            if cell.row_id == row_id:
                return cell

        raise CellNotFoundException(f"Could not find cell with row_id {row_id}")


    def __len__(self):
        """
        header size is 12
        """
        [cell_offsets, cell_data] = self.cells_to_bytes()
        return 12 + len(cell_offsets) + len(cell_data)


    def cells_to_bytes(self) -> List[bytes]:
        """
        Returns the body as bytes 
        """
        cell_offsets = b""
        cell_data = b""

        for cell in self.cells:
            cell_bytes = cell.to_bytes()
            # Add offset for each cell from the cell Content area.
            offset = FixedInteger.to_bytes(2, len(cell_bytes))
            cell_offsets += offset
            cell_data += cell_bytes

        return [cell_offsets, cell_data]
    
    def to_bytes(self) -> bytes:
        """
        Page header: https://www.sqlite.org/fileformat.html#:~:text=B%2Dtree%20Page%20Header%20Format
        """
        buff = io.BytesIO(b"".ljust(4096, b"\0"))
        [cell_offsets, cell_data] = self.cells_to_bytes()

        self.cell_content_offset = len(cell_data)

        buff.seek(self.cell_content_offset)
        buff.write(cell_data)

        buff.seek(0)
        buff.write(FixedInteger.to_bytes(1, self.page_number))
        # Header type.
        buff.write(FixedInteger.to_bytes(1, self.page_type.value))
        # Free block pointer. (Not implemented)
        buff.write(FixedInteger.to_bytes(2, 0))
        # Number of cells. 
        buff.write(FixedInteger.to_bytes(2, len(self.cells)))
        # Cell Content Offset 
        buff.write(FixedInteger.to_bytes(2, self.cell_content_offset))
        # Right most cell pointer (Not implemented)
        buff.write(FixedInteger.to_bytes(4, 0))

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
        _right_page_number = FixedInteger.from_bytes(buffer.read(4))

        cells = []

        # Cell pointers
        cell_offsets = []

        for _ in range(number_of_cells):
            cell_offsets.append(FixedInteger.from_bytes(buffer.read(2)))

        # Now read cells
        buffer.seek(cell_content_offset)

        for offset in cell_offsets:
            cell_content = buffer.read(offset) 
            cell = Page.cell_from_bytes(page_type, cell_content)
            cells.append(cell)

        return Page(page_number, page_type, cells)
