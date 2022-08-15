from typing import List, Any, Dict

from toysql.pager import Pager
from toysql.constants import *
from toysql.tree import BPlusTree
from toysql.lexer import Token
import toysql.datatypes as datatypes
from toysql.record import Record, DataType

# TODO type rows properly
Row = Any


class Table:
    def __init__(self, pager: Pager, columns: Dict[str, DataType], root_page_num: int):
        self.pager = pager
        self.root_page_num = root_page_num 
        self.schema = list(columns.values())
        self.columns = columns
        self.primary_key = self.schema[0]
        self.tree = BPlusTree(self)

    def insert(self, tokens: List[Token]) -> Row:
        row = [token.value for token in tokens]
        self.tree.insert(row[0], row)
        return row

    def select(self) -> List[Row]:
        result = []
        for r in self.tree.traverse():
            [_, value] = r
            result.append(value)

        return result

    def serialize_row(self, record: Record) -> bytearray:
        raw_bytes = record.to_bytes()
        return bytearray(raw_bytes)

    def deserialize_row(self, cell: bytearray) -> Record:
        return Record.from_bytes(bytes(cell))
