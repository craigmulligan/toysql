from typing import Optional, List, Protocol
from dataclasses import dataclass
from toysql.lexer import Token, Kind, Keyword, Symbol, DataType
from toysql.exceptions import ParsingException

Expression = Token


def expect(token: Optional[Token], **kwargs):
    """
    utility function to match a token
    against a set of attr + values.

    match(token, type=Keyword._from)
    """
    if token is None:
        raise LookupError(f"Unexpected - token is None")

    for key, value in kwargs.items():
        if getattr(token, key) != value:
            raise LookupError(
                f"Unexpected {token.value} for {key} was expecting {value}"
            )

    return


def match(token: Optional[Token], **kwargs) -> bool:
    """
    Same as expect but doesnt raise, always returns bool.
    """
    try:
        expect(token, **kwargs)
    except LookupError:
        return False

    return True


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

    def is_complete(self):
        return self.pointer >= len(self.tokens)


class Statement(Protocol):
    @staticmethod
    def parse(cursor: TokenCursor) -> "Statement":
        ...


@dataclass
class SelectStatement(Statement):
    _from: Token
    items: List[Expression]

    @staticmethod
    def parse_expressions(cursor: TokenCursor, delimiters: List[Token]) -> List[Token]:
        expressions = []

        while not cursor.is_complete():
            for delimiter in delimiters:
                if match(cursor.peek(), type=delimiter.type):
                    return expressions

            if len(expressions) > 0:
                try:
                    expect(cursor.peek(), type=Symbol.comma)
                    cursor.move()
                except LookupError:
                    raise ParsingException("Expected comma")

            # Now look for one of identifier kind
            exp = None
            kinds = [Kind.datatype, Kind.identifier]

            for kind in kinds:
                try:
                    expect(cursor.peek(), kind=kind)
                    exp = cursor.move()
                    break
                except LookupError:
                    pass

            if not exp:
                # Didn't find an identifier
                # Let's look for a asterisk
                try:
                    expect(cursor.peek(), type=Symbol.asterisk)
                    exp = cursor.move()
                except:
                    raise ParsingException("Expected expression")

            expressions.append(exp)

        return expressions

    @staticmethod
    def parse(cursor: TokenCursor) -> "SelectStatement":
        """
        Parsing SELECT statements is easy. We'll look for the following token pattern:

        SELECT
        $expression [, ...]
        FROM
        $table-name
        """
        # Implement parse for select statement.
        expect(cursor.current(), type=Keyword.select)

        # TODO come back to this.
        select_items = SelectStatement.parse_expressions(
            cursor,
            [
                Token(type=Keyword._from),
                Token(type=Symbol.semicolon),
            ],
        )

        if len(select_items) == 0:
            raise LookupError()

        try:
            expect(cursor.peek(), type=Keyword._from)
            cursor.move()
        except LookupError:
            raise ParsingException("Expected FROM keyword")

        try:
            expect(cursor.peek(), kind=Kind.identifier)
            from_identifier = cursor.move()
        except LookupError:
            raise ParsingException("Expected table name")

        if match(cursor.peek(), type=Symbol.semicolon):
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
        expect(cursor.peek(), type=Symbol.left_paren)

        cursor.move()
        tokens = []
        delimiter = Token(type=Symbol.right_paren)

        while not cursor.is_complete():
            if match(cursor.peek(), type=delimiter.type):
                break

            if len(tokens) > 0:
                if not match(cursor.peek(), type=Symbol.comma):
                    raise ParsingException("Expected comma")

                cursor.move()

            # Now look for one of identifier kind
            token = None
            kinds = [Kind.datatype, Kind.identifier]

            for kind in kinds:
                m = match(cursor.peek(), kind=kind)
                if m:
                    token = cursor.move()
                    break

            if token is None:
                raise LookupError()

            tokens.append(token)

        try:
            expect(cursor.peek(), type=Symbol.right_paren)
            cursor.move()
        except LookupError:
            raise ParsingException(f"Expected {Symbol.right_paren.value}")

        return tokens

    @staticmethod
    def parse(cursor: TokenCursor) -> "InsertStatement":
        """
        Parses a insert statement in the format:

            INSERT INTO table_name (column1, column2, column3, ...)
            VALUES (value1, value2, value3, ...);

        or

            INSERT INTO table_name
            VALUES (value1, value2, value3, ...);
        """
        expect(cursor.current(), type=Keyword.insert)

        try:
            expect(cursor.peek(), type=Keyword.into)
            cursor.move()
        except LookupError:
            raise ParsingException("Expected into keyword")

        try:
            expect(cursor.peek(), kind=Kind.identifier)
            table_identifier = cursor.move()
        except LookupError:
            raise ParsingException("Expected table name")

        columns = []
        try:
            expect(cursor.peek(), type=Keyword.values)
            cursor.move()
        except LookupError:
            # if next token is not VALUES it might be declaring columns
            try:
                columns = InsertStatement.parse_values(cursor)
            except LookupError:
                raise ParsingException("Expected values keyword")

        values = InsertStatement.parse_values(cursor)

        if match(cursor.peek(), type=Symbol.semicolon):
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
    is_primary_key: bool


@dataclass
class CreateStatement(Statement):
    table: Token
    columns: List[ColumnDefinition]

    @staticmethod
    def parse_columns(cursor: TokenCursor) -> List[ColumnDefinition]:
        try:
            expect(cursor.peek(), type=Symbol.left_paren)
        except LookupError:
            raise ParsingException(f"Expected {Symbol.left_paren.value} found")

        cursor.move()
        columns = []
        delimiter = Token(type=Symbol.right_paren)

        while not cursor.is_complete():
            if match(cursor.peek(), type=delimiter.type):
                break

            if len(columns) > 0:
                try:
                    expect(cursor.peek(), type=Symbol.comma)
                except LookupError:
                    raise ParsingException(f"Expected {Symbol.comma.value}")

                cursor.move()

            # Now look for one of identifier kind
            try:
                expect(cursor.peek(), kind=Kind.identifier)
                name = cursor.move()
            except LookupError:
                raise ParsingException(f"Expected {Kind.identifier.value}")

            try:
                expect(cursor.peek(), kind=Kind.keyword)
                datatype = cursor.move()
            except LookupError:
                raise ParsingException(f"Expected {Kind.keyword.value}")

            length = None
            is_primary_key = False
            # Let's look for length which is (<integer)
            # TODO: we can remove this. As our SQL only has
            # varints and text which don't need a length.
            if match(cursor.peek(), type=Symbol.left_paren):
                cursor.move()

                try:
                    expect(cursor.peek(), type=DataType.integer)
                    length = cursor.move()
                except LookupError:
                    raise ParsingException(f"Expected {DataType.integer}")

                try:
                    expect(cursor.peek(), type=Symbol.right_paren)
                    cursor.move()
                except LookupError:
                    raise ParsingException(f"Expected {Symbol.right_paren.value}")

            if match(cursor.peek(), type=Keyword.primary):
                cursor.move()
                try:
                    expect(cursor.peek(), type=Keyword.key)
                    is_primary_key = True
                    cursor.move()
                except LookupError:
                    raise ParsingException(f"Expected {DataType.integer}")

            column = ColumnDefinition(
                name, datatype, length, is_primary_key=is_primary_key
            )
            columns.append(column)
        try:
            expect(cursor.peek(), type=Symbol.right_paren)
            cursor.move()
        except LookupError:
            raise ParsingException(f"Expected {Symbol.right_paren.value}")

        return columns

    @staticmethod
    def parse(cursor: TokenCursor) -> "CreateStatement":
        """
        Parses a create statement in the format:
            CREATE TABLE table_name (
                column1 datatype,
                column2 datatype,
                column3 datatype,
               ....
            );
        """
        expect(cursor.current(), type=Keyword.create)

        try:
            expect(cursor.peek(), type=Keyword.table)
            cursor.move()
        except LookupError:
            raise ParsingException(f"Expected table keyword")

        try:
            expect(cursor.peek(), kind=Kind.identifier)
            table_identifier = cursor.move()
        except LookupError:
            raise ParsingException(f"Expected table name")

        columns = CreateStatement.parse_columns(cursor)

        if match(cursor.peek(), type=Symbol.semicolon):
            try:
                cursor.move()
                cursor.move()
            except StopIteration:
                pass

        return CreateStatement(table=table_identifier, columns=columns)


def parse(tokens: List[Token]):
    stmts = []
    parsers: List[Statement] = [SelectStatement, CreateStatement, InsertStatement]
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
