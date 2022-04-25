from typing import Tuple
from dataclasses import dataclass
from enum import Enum, auto

Keyword = str
Symbol = str
TokenKind = int


@dataclass
class Location:
    line: int
    col: int


@dataclass
class Cursor:
    pointer: int
    loc: Location


@dataclass
class Token:
    value: str
    kind: TokenKind
    loc: Location

    def __eq__(self, other: "Token"):  # type: ignore[override]
        return self.value == other.value and self.kind == other.kind

    def lex(self, data: str, cursor: Cursor) -> Tuple["Token", Cursor, bool]:
        ...


class Keywords(Enum):
    selectKeyword = "select"
    fromKeyword = "from"
    asKeyword = "as"
    tableKeyword = "table"
    createKeyword = "create"
    insertKeyword = "insert"
    intoKeyword = "into"
    valuesKeyword = "values"
    intKeyword = "int"
    textKeyword = "text"


class Symbols(Enum):
    semicolonSymbol = ";"
    asteriskSymbol = "*"
    commaSymbol = ","
    leftparenSymbol = "("
    rightparenSymbol = ")"


class KeywordKind(Enum):
    keywordKind = auto()
    symbolKind = auto()
    identifierKind = auto()
    stringKind = auto()
    numericKind = auto()
