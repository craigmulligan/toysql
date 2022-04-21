from typing import Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum, auto


class Op(Enum):
    GreaterThan = auto()
    LessThan = auto()
    Equal = auto()
    NotEqual = auto()


class Expression:
    # TODO we will need expressions without left right.
    a: Any
    b: Any
    op: Op


class KeywordKind(Enum):
    SymbolKind = auto()
    IdentifierKind = auto()
    StringKind = auto()
    NumericKind = auto()
    BoolKind = auto()
    NullKind = auto()


class Statement:
    pass


@dataclass
class Token:
    keyword: str
    keywork_kind: str


@dataclass
class SelectStatement(Statement):
    _from: Optional[Token] = None
    where: Optional[Expression] = None
    limit: Optional[Expression] = None
    offset: Optional[Expression] = None


@dataclass
class InsertStatement(Statement):
    row: Tuple[int, str, str]
