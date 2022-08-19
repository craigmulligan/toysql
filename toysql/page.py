from enum import Enum
from toysql.record import Record, Integer

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


class LeafPageCell():
    """
    A cell is a wrapper for the Record format 
    which adds some metadata depending on the surrounding 

    A cell should be sortable by key. (PK)
    """
    def __init__(self,  payload:Record) -> None:
        self.record = payload

    def to_bytes(self):
        """
        pass
        """
        data = b""
        record_bytes = self.record.to_bytes()
        record_size = Integer(len(record_bytes)).to_bytes()
        row_id = Integer(self.record.row_id).to_bytes()

        data += record_size
        data += row_id
        data += record_bytes

        return data 

    @staticmethod
    def from_bytes(data) -> "LeafPageCell":
        """
        First read the cell header.
        Then read the record.
        """
        record_size = Integer.from_bytes(data)
        data = data[record_size.content_length():]
        row_id = Integer.from_bytes(data)
        data = data[row_id.content_length():]
        #data = data[:record_size]
        record = Record.from_bytes(data)

        return LeafPageCell(record)


class InteriorPageCell():
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
    def __init__(self, row_id, left_child: "Page") -> None:
        self.row_id = row_id
        self.left_child = left_child 

    def to_bytes(self):
        data = b""
        page_number = FixedInteger.to_bytes(4, self.left_child.page_number)
        row_id = Integer(self.row_id).to_bytes()

        data += page_number 
        data += row_id

        return data 

    @staticmethod
    def from_bytes(data) -> "InteriorPageCell":
        """
        First read the cell header.
        """
        left_child_page_number = FixedInteger.from_bytes(data[:4])
        data = data[4:]
        row_id = Integer.from_bytes(data).value

        return InteriorPageCell(row_id, Page(left_child_page_number, PageType.interior))


class Page:
    """
    header is 8 bytes in size for leaf pages and 12 bytes for interior pages (RN we just make em all 12 bytes)
    """
    def __init__(self, page_number, page_type, cells=None) -> None:
        self.page_number = page_number
        self.page_type = PageType(page_type) 
        self.cell_content_offset = 65536
        self.cells = cells or []
        
    def add_cell(self):
        """Write the cell to the beginning of the cell_content area + update the cell_content_offset"""
        pass

    def to_bytes(self) -> bytes:
        """
        Page header: https://www.sqlite.org/fileformat.html#:~:text=B%2Dtree%20Page%20Header%20Format
        """
        header_data = b"" 
        cell_offsets = b""
        cell_data = b""
        data = bytearray(b"".ljust(4096, b"\0"))

        for cell in self.cells:
            cell_bytes = cell.to_bytes()
            # Add offset for each cell from the cell Content area.
            offset = FixedInteger.to_bytes(len(cell_bytes), 2)
            cell_offsets += offset
            cell_data += cell_bytes

        self.cell_content_offset = len(cell_data)
        data[:-self.cell_content_offset] = cell_data

        # Header type.
        header_data += FixedInteger.to_bytes(1, self.page_type.value)
        # Free block pointer. (Not implemented)
        header_data += FixedInteger.to_bytes(2, 0)
        # Number of cells. 
        header_data += FixedInteger.to_bytes(2, len(self.cells))
        # Cell Content Offset 
        header_data += FixedInteger.to_bytes(2, self.cell_content_offset)
        # Right most cell pointer (Not implemented)
        header_data += FixedInteger.to_bytes(4, 0)
        data[:len(header_data)] = header_data

        return bytes(data)
        
    @staticmethod
    def from_bytes(raw_bytes) -> "Page":
        data = bytearray(raw_bytes)
        page_number = FixedInteger.from_bytes(data[0])
        page_type = PageType(FixedInteger.from_bytes(data[1]))
        number_of_cells = FixedInteger.from_bytes(data[3:5])
        cell_content_offset = FixedInteger.from_bytes(data[5:7])
        cells = []

        # Cell pointers
        cell_offsets = []
        cell_offset_index = 12

        for _ in range(number_of_cells):
            cell_offsets.append(data[cell_offset_index:2])
            cell_offset_index += 2

        # Now read cells
        for i, offset in enumerate(cell_offsets):
            cell_content = bytes(data[offset + cell_content_offset:cell_offsets[i + 1]])
            cells.append(LeafPageCell.from_bytes(cell_content))
        
        return Page(page_number, page_type, cells)
