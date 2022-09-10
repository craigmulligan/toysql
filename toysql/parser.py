from typing import Tuple, Optional, List
from dataclasses import dataclass
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
        try:
            return self.tokens[self.pointer + 1]
        except IndexError:
            return None

    def move(self):
        self.pointer += 1

        if self.pointer == len(self.tokens):
            # If pointer is on the last token return None
            raise StopIteration

        return self.tokens[self.pointer]

    def expect(self, token):
        if self.peek() == token:
            return self.move()

        raise LookupError("Unexpected token kind")

    def expect_kind(self, kind: Kind):
        next_token = self.peek()
        if next_token and next_token.kind == kind:
            return self.move()

        raise LookupError("Unexpected token kind")

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
            kinds = [Kind.integer, Kind.text, Kind.identifier]

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
    def parse(tokens: List[Token], _: int) -> Tuple[Optional["Statement"], int]:
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
            raise ParsingException("Expected FROM keyword")

        try:
            from_identifier = cursor.expect_kind(Kind.identifier)
        except LookupError:
            raise ParsingException("Expected table name")

        if cursor.peek() == Token(Symbol.semicolon.value, Kind.symbol):
            try:
                cursor.move()
                cursor.move()
            except StopIteration:
                pass
        else:
            try:
                cursor.move()
            except StopIteration:
                pass

        return SelectStatement(_from=from_identifier, items=select_items)


@dataclass
class InsertStatement(Statement):
    values: List[Token]
    into: Token
    columns: List[Token]

    @staticmethod
    def parse_values(cursor: TokenCursor) -> List[Token]:
        """
        Looks for a comma seperated list of values
        """
        if cursor.peek() != Token(Symbol.left_paren.value, Kind.symbol):
            # Not a select statement, let's bail.
            raise LookupError()

        cursor.move()
        tokens = []
        delimiter = Token(Symbol.right_paren.value, Kind.symbol)

        while not cursor.is_complete():
            if delimiter == cursor.peek():
                break

            if len(tokens) > 0:
                if cursor.peek() != Token(Symbol.comma.value, Kind.symbol):
                    raise ParsingException("Expected comma")

                cursor.move()

            # Now look for one of identifier kind
            token = None
            kinds = [Kind.integer, Kind.text, Kind.identifier]

            for kind in kinds:
                try:
                    token = cursor.expect_kind(kind)
                except LookupError:
                    pass

            if token is None:
                raise LookupError()

            tokens.append(token)

        try:
            cursor.expect(Token(Symbol.right_paren.value, Kind.symbol))
        except LookupError:
            raise ParsingException(f"Expected {Symbol.right_paren.value}")

        return tokens

    @staticmethod
    def parse(cursor: TokenCursor) -> "Statement":
        """
        Parses a insert statement in the format:

            INSERT INTO table_name (column1, column2, column3, ...)
            VALUES (value1, value2, value3, ...);

        or

            INSERT INTO table_name
            VALUES (value1, value2, value3, ...);
        """
        if cursor.current() != Token(Keyword.insert.value, Kind.keyword):
            # Not a select statement, let's bail.
            raise LookupError()

        try:
            cursor.expect(Token(Keyword.into.value, Kind.keyword))
        except LookupError:
            raise ParsingException("Expected into keyword")

        try:
            table_identifier = cursor.expect_kind(Kind.identifier)
        except LookupError:
            raise ParsingException("Expected table name")

        columns = []
        try:
            cursor.expect(Token(Keyword.values.value, Kind.keyword))
        except LookupError:
            # if next token is not VALUES it might be declaring columns
            try:
                columns = InsertStatement.parse_values(cursor)
            except LookupError:
                raise ParsingException("Expected values keyword")

        values = InsertStatement.parse_values(cursor)

        if cursor.peek() == Token(Symbol.semicolon.value, Kind.symbol):
            try:
                cursor.move()
                cursor.move()
            except StopIteration:
                pass

        return InsertStatement(into=table_identifier, values=values, columns=columns)


@dataclass
class ColumnDefinition:
    name: Token
    datatype: Token
    length: Optional[Token]


@dataclass
class CreateStatement(Statement):
    table: Token
    columns: List[ColumnDefinition]

    @staticmethod
    def parse_columns(cursor: TokenCursor) -> List[ColumnDefinition]:
        if cursor.peek() != Token(Symbol.left_paren.value, Kind.symbol):
            raise ParsingException(f"Expected {Symbol.left_paren.value} found")

        cursor.move()
        columns = []
        delimiter = Token(Symbol.right_paren.value, Kind.symbol)

        while not cursor.is_complete():
            if delimiter == cursor.peek():
                break

            if len(columns) > 0:
                if cursor.peek() != Token(Symbol.comma.value, Kind.symbol):
                    raise ParsingException(f"Expected {Symbol.comma.value}")

                cursor.move()

            # Now look for one of identifier kind
            try:
                name = cursor.expect_kind(Kind.identifier)
            except LookupError:
                raise ParsingException(f"Expected {Kind.identifier.value}")

            try:
                datatype = cursor.expect_kind(Kind.keyword)
            except LookupError:
                raise ParsingException(f"Expected {Kind.keyword.value}")

            length = None
            # Let's look for length which is (<integer)
            if cursor.peek() == Token(Symbol.left_paren.value, Kind.symbol):
                cursor.move()
                try:
                    length = cursor.expect_kind(Kind.integer)
                except LookupError:
                    raise ParsingException(f"Expected {Kind.integer.value} found")

                try:
                    cursor.expect(Token(Symbol.right_paren.value, Kind.symbol))
                except LookupError:
                    raise ParsingException(f"Expected {Symbol.right_paren.value}")

            column = ColumnDefinition(name, datatype, length)
            columns.append(column)

        try:
            cursor.expect(Token(Symbol.right_paren.value, Kind.symbol))
        except LookupError:
            raise ParsingException(f"Expected {Symbol.right_paren.value}")

        return columns

    @staticmethod
    def parse(cursor: TokenCursor) -> "Statement":
        """
        Parses a create statement in the format:
            CREATE TABLE table_name (
                column1 datatype,
                column2 datatype,
                column3 datatype,
               ....
            );
        """
        if cursor.current() != Token(Keyword.create.value, Kind.keyword):
            # Not a select statement, let's bail.
            raise LookupError()

        # First look for a TABLE keyword.
        try:
            cursor.expect(Token(Keyword.table.value, Kind.keyword))
        except LookupError:
            raise ParsingException(f"Expected table keyword")

        try:
            table_identifier = cursor.expect_kind(Kind.identifier)
        except LookupError:
            raise ParsingException(f"Expected table name")

        columns = CreateStatement.parse_columns(cursor)

        if cursor.peek() == Token(Symbol.semicolon.value, Kind.symbol):
            try:
                cursor.move()
                cursor.move()
            except StopIteration:
                pass

        return CreateStatement(table=table_identifier, columns=columns)


class Parser:
    def parse(self, tokens: List[Token]):
        stmts = []
        parsers = [SelectStatement, CreateStatement, InsertStatement]
        cursor = TokenCursor(tokens)

        while not cursor.is_complete():
            pointer = cursor.pointer
            for parser in parsers:
                try:
                    stmt = parser.parse(cursor)
                    stmts.append(stmt)
                    break
                except LookupError:
                    continue

            if pointer == cursor.pointer:
                break
        return stmts
