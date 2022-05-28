from typing import Tuple, Optional, Any, List, NewType
from dataclasses import dataclass
from enum import Enum, auto
from toysql.lexer import Token

Cursor = NewType("ParserCursor", int)


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
    def parse(tokens: Token, cursor: int) -> Tuple[Optional["Statement"], Cursor]:
        ...


@dataclass
class SelectStatement(Statement):
    _from: Optional[Token] = None
    where: Optional[Expression] = None
    limit: Optional[Expression] = None
    offset: Optional[Expression] = None

    @staticmethod
    def parse(tokens: Token, cursor: Cursor) -> Tuple[Optional["Statement"], Cursor]:
        # Implement parse for select statement.
        return SelectStatement(), cursor


@dataclass
class InsertStatement(Statement):
    row: Tuple[int, str, str]


class Parser:
    def parse(self, tokens: List[Token]):
        stmts = []
        cursor = 0

        parsers = [SelectStatement.parse, InsertStatement.parse]

        while cursor < len(tokens):
            for parse in parsers:
                stmt, cursor = parse(tokens, cursor)

                if stmt:
                    stmts.append(stmt)
                    break

        return stmts
