from typing import Tuple, Optional, List, Union
from dataclasses import dataclass
from enum import Enum, auto
from toysql.lexer import Token, Kind, Keyword, Symbol
from toysql.exceptions import ParsingException

Expression = Token


@dataclass
class BinaryExpression:
    a: Expression
    b: Expression
    op: Token


class TokenCursor:
    tokens: List[Token]
    pointer: int

    def __init__(self, tokens) -> None:
        self.tokens = tokens
        self.pointer = 0

    def current(self):
        return self.tokens[self.pointer]

    def peek(self):
        return self.tokens[self.pointer + 1]

    def move(self):
        self.pointer += 1
        return self.tokens[self.pointer]

    def expect(self, token):
        if self.peek() == token:
            return self.move()

        raise LookupError("Unexpected token")

    def expect_kind(self, kind: Kind):
        if self.peek().kind == kind:
            return self.move()

        raise LookupError("Unexpected token")

    def is_complete(self):
        return self.pointer >= len(self.tokens)

    def parse_expressions(self, delimiters: List[Token]) -> List[Expression]:
        expressions = []

        while not self.is_complete():
            for delimiter in delimiters:
                if delimiter == self.peek():
                    return expressions

            if len(expressions) > 0:
                if self.peek() != Token(Symbol.comma.value, Kind.symbol):
                    raise ParsingException("Expected comma")

                self.move()

            # Now look for one of identifier kind
            exp = None
            kinds = [Kind.numeric, Kind.string, Kind.identifier]

            for kind in kinds:
                try:
                    exp = self.expect_kind(kind)
                except LookupError:
                    pass

            if not exp:
                # Didn't find an identifier
                # Let's look for a asterisk
                try:
                    exp = self.expect(Token("*", Kind.symbol))
                except:
                    raise ParsingException("Expected expression")

            expressions.append(exp)

        return expressions


class Statement:
    @staticmethod
    def parse(tokens: List[Token], cursor: int) -> Tuple[Optional["Statement"], int]:
        ...


@dataclass
class SelectStatement(Statement):
    _from: Token
    items: List[Expression]

    @staticmethod
    def parse(cursor: TokenCursor) -> "Statement":
        """
        Parsing SELECT statements is easy. We'll look for the following token pattern:

        SELECT
        $expression [, ...]
        FROM
        $table-name
        """
        # Implement parse for select statement.
        if cursor.current() != Token(Keyword.select.value, Kind.keyword):
            # Not a select statement, let's bail.
            raise LookupError()

        select_items = cursor.parse_expressions(
            [
                Token(Keyword._from.value, Kind.keyword),
                Token(Symbol.semicolon.value, Kind.symbol),
            ],
        )

        if len(select_items) == 0:
            raise LookupError()

        try:
            cursor.expect(Token(Keyword._from.value, Kind.keyword))
        except LookupError:
            raise ParsingException("Expected FROM token")

        try:
            from_identifier = cursor.expect_kind(Kind.identifier)
        except LookupError:
            raise ParsingException("Expected table name")

        return SelectStatement(_from=from_identifier, items=select_items)


@dataclass
class InsertStatement(Statement):
    row: Tuple[int, str, str]


class Parser:
    def parse(self, tokens: List[Token]):
        stmts = []
        parsers = [SelectStatement]
        cursor = TokenCursor(tokens)

        while not cursor.is_complete():
            for parser in parsers:
                try:
                    stmt = parser.parse(cursor)
                    stmts.append(stmt)
                except LookupError:
                    continue

        return stmts
