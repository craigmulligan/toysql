from toysql.record import Record, DataType
from toysql.page import LeafPageCell
from unittest import TestCase

class TestCell(TestCase):
    def test_leaf_page_cell(self):
        payload = [
            [DataType.INTEGER, 3],
            [DataType.INTEGER, 124],
            [DataType.TEXT, "Craig"],
            [DataType.NULL, None]
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
            [DataType.NULL, None]
        ]
        record = Record(payload)
        cell = LeafPageCell(record)
        raw_bytes = cell.to_bytes() + b"x05"
        new_cell = LeafPageCell.from_bytes(raw_bytes)
        assert new_cell.record.values == payload
