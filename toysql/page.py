from enum import Enum
from toysql.record import Record, Integer

class PageType(Enum): 
    leaf = 0
    interior = 1


class FixedInteger():
    @staticmethod
    def to_bytes(value, length):
        return int.to_bytes(length, value, "big") 

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

        data += row_id
        data += record_size
        data += record_bytes

        return b""

    @staticmethod
    def from_bytes(data) -> "LeafPageCell":
        """
        First read the cell header.
        Then read the record.
        """
        record = Record.from_bytes(data)
        return LeafPageCell(record)


class Page:
    """

    header is 8 bytes in size for leaf pages and 12 bytes for interior pages (RN we just make em all 12 bytes)

    """
    def __init__(self, page_number, page_type, cells) -> None:
        self.page_number = page_number
        self.page_type = PageType 
        self.cell_content_offset = 65536
        self.cells = []
        
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
            cells.append(Cell.from_bytes(cell_content))
        
        return Page(page_number, page_type, cells)
