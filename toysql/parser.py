from typing import Tuple, Optional, Any, List
from dataclasses import dataclass
from enum import Enum, auto
from toysql.lexer import Token, Kind, Keyword, Symbol
from toysql.exceptions import ParsingException


class Op(Enum):
    GreaterThan = auto()
    LessThan = auto()
    Equal = auto()
    NotEqual = auto()


class Comparer:
    # TODO we will need expressions without left right.
    a: Any
    b: Any
    op: Op


@dataclass
class Expression:
    token: Token


@dataclass
class BinaryExpression:
    token: Token
    comparer: Comparer


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

    @staticmethod
    def parse_expression(
        tokens: List[Token], cursor: int
    ) -> Tuple[Optional[Expression], int]:

        """
        The parseExpression helper (for now) will look for a numeric, string, or identifier token.
        """
        c = cursor

        kinds = [Kind.numeric, Kind.string, Kind.identifier]

        for kind in kinds:
            token, c = Statement.find_token_by_kind(tokens, c, kind)
            if token:
                return Expression(token), c

        return None, cursor

    @staticmethod
    def parse_expressions(
        tokens: List[Token], cursor: int, delimiters: List[Token]
    ) -> Tuple[List[Expression], int]:
        expressions = []
        c = cursor

        while True:
            if cursor >= len(tokens):
                return expressions, cursor

            current = tokens[c]
            for delimiter in delimiters:
                if delimiter == current:
                    break

            if len(expressions) > 0:
                if not Statement.expect_token(
                    tokens, cursor, Token(Symbol.comma.value, Kind.symbol)
                ):
                    raise ParsingException("Expected comma")
                cursor += 1

            # // Look for expression
            exp, c = Statement.parse_expression(tokens, cursor)
            if not exp:
                raise ParsingException("Expected expression")

            expressions.append(exp)

        return expressions, c


@dataclass
class SelectItem:
    exp: Expression
    asterisk: bool


@dataclass
class SelectStatement(Statement):
    _from: Token
    items: Optional[List[SelectItem]] = None
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
        c = cursor
        if not Statement.expect_token(
            tokens, cursor, Token(Keyword.select.value, Kind.keyword)
        ):
            # Not a select statement, let's bail.
            return None, cursor

        # Okay we have a select statement.
        # TODO: Look for expression.
        exps, c = Statement.parse_expressions(
            tokens,
            cursor,
            [
                Token(Keyword._from.value, Kind.keyword),
                Token(Symbol.semicolon.value, Kind.symbol),
            ],
        )
        if len(exps) == 0:
            return None, cursor

        if Statement.expect_token(tokens, c, Token(Keyword._from.value, Kind.keyword)):
            from_identifier, c = Statement.find_token_by_kind(
                tokens, cursor, Kind.identifier
            )
            if not from_identifier:
                raise ParsingException("Expected FROM token")

            return SelectStatement(_from=from_identifier), c

        return None, c


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
