from typing import Tuple, Optional, Any, List
from dataclasses import dataclass
from enum import Enum, auto
from toysql.lexer import Token


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


class Statement:
    @staticmethod
    def parse(tokens: List[Token], cursor: int) -> Tuple[Optional["Statement"], int]:
        ...


@dataclass
class SelectStatement(Statement):
    _from: Optional[Token] = None
    where: Optional[Expression] = None
    limit: Optional[Expression] = None
    offset: Optional[Expression] = None

    @staticmethod
    def parse(tokens: List[Token], cursor: int) -> Tuple[Optional["Statement"], int]:
        # Implement parse for select statement.
        return SelectStatement(), cursor


@dataclass
class InsertStatement(Statement):
    row: Tuple[int, str, str]


class Parser:
    def parse(self, tokens: List[Token]):
        stmts = []
        cursor = 0

        parsers = [SelectStatement, InsertStatement]

        while cursor < len(tokens):
            for parser in parsers:
                stmt, cursor = parser.parse(tokens, cursor)

                if stmt:
                    stmts.append(stmt)
                    break

        return stmts
