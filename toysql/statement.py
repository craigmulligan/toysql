from typing import Tuple
from dataclasses import dataclass


class Statement:
    pass


@dataclass
class SelectStatement(Statement):
    pass


@dataclass
class InsertStatement(Statement):
    row: Tuple[int, str, str]
