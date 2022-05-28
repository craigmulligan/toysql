from typing import Tuple, Optional, Any, List
from dataclasses import dataclass
from enum import Enum, auto
from toysql.lexer import Token, Kind, Keyword
from toysql.exceptions import ParsingException


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

    @staticmethod
    def expect_token(tokens: List[Token], cursor: int, token: Token) -> bool:
        """
        Returns True if the token at current cursor index is equal to the token provided.
        """
        if cursor >= len(tokens):
            return False

        return tokens[cursor] == token

    @staticmethod
    def find_token_by_kind(
        tokens: List[Token], cursor: int, kind: Kind
    ) -> Tuple[Optional[Token], int]:
        """
        Will find a token of
        """
        c = cursor

        if c >= len(tokens):
            return None, cursor

        current = tokens[c]
        if current.kind == kind:
            return current, c + 1

        return None, cursor


@dataclass
class SelectStatement(Statement):
    _from: Optional[Token] = None
    where: Optional[Expression] = None
    limit: Optional[Expression] = None
    offset: Optional[Expression] = None

    @staticmethod
    def parse(tokens: List[Token], cursor: int) -> Tuple[Optional["Statement"], int]:
        """
        Parsing SELECT statements is easy. We'll look for the following token pattern:

        SELECT
        $expression [, ...]
        FROM
        $table-name
        """
        # Implement parse for select statement.
        if not Statement.expect_token(
            tokens, cursor, Token(Keyword.select.value, Kind.keyword)
        ):
            # Not a select statement, let's bail.
            return None, cursor

        # Okay we have a select statement.
        # TODO: Look for expression.

        cursor += 1
        cursor += 1

        if Statement.expect_token(
            tokens, cursor, Token(Keyword._from.value, Kind.keyword)
        ):
            cursor += 1
            from_identifier, cursor = Statement.find_token_by_kind(
                tokens, cursor, Kind.identifier
            )
            if not from_identifier:
                raise ParsingException("Expected FROM token")

            return SelectStatement(from_identifier), cursor

        return None, cursor


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
