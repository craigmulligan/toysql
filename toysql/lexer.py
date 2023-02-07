from dataclasses import dataclass
from enum import Enum, auto
from io import StringIO
from toysql.exceptions import LexingException
from typing import List, Protocol, Optional, Union


class Keyword(Enum):
    select = "select"
    _from = "from"
    _as = "as"
    where = "where"
    _and = "and"
    create = "create"
    insert = "insert"
    table = "table"
    into = "into"
    values = "values"
    int = "int"
    bool = "bool"
    text = "text"


class Symbol(Enum):
    semicolon = ";"
    asterisk = "*"
    comma = ","
    left_paren = "("
    right_paren = ")"
    equal = "="
    gt = ">"
    gteq = ">="
    lt = "<"
    lteq = ">="


class DataType(Enum):
    """
    NULL. The value is a NULL value.
    INTEGER. The value is a signed integer, stored in 0, 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
    TEXT. The value is a text text, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
    """
    null = auto()
    integer = auto()
    text = auto()

class Kind(Enum):
    """
    Kind is a superset of datatypes

    You can't nicely extend Enums in python.
    """

    keyword = auto()
    symbol = auto()
    identifier = auto()

    # datatypes
    null = DataType.null 
    integer = DataType.integer 
    text = DataType.text


@dataclass
class Location:
    line: int
    col: int


class Cursor:
    def __init__(self, text) -> None:
        self.reader = StringIO(text)

    @property
    def pointer(self):
        return self.reader.tell()

    def __len__(self):
        """
        length of underlying str.
        """
        return len(self.reader.getvalue())

    def peek(self, size=1) -> Optional[str]:
        """
        stringIO doesn't have peek
        peek reads the current character
        without advancing the cursor.
        """
        pointer = self.reader.tell()
        value = self.read(size)
        self.reader.seek(pointer)

        return value

    def read(self, size=None) -> str:
        """
        Reads remaining text from current index without moving the cursor
        """
        return self.reader.read(size)

    def is_complete(self) -> bool:
        return self.reader.tell() == len(self)

    def line_no(self) -> int:
        """
        Calculates the current line number
        of the cursor.
        """
        pointer = self.reader.tell()
        count = 0

        self.reader.seek(0)
        buffer = self.reader.read(pointer)
        count += buffer.count("\n")

        self.reader.seek(pointer)
        return count

    def column_no(self) -> int:
        """
        Calculates the character count since the last
        line break.
        """
        pointer = self.reader.tell()
        start = pointer

        while True:
            # Find the beginning of the
            # line
            self.reader.seek(start)
            value = self.peek()
            if start == 0:
                break

            if value == "\n":
                # This counts as previous line
                # so add one char.
                start += 1
                break

            start -= 1

        count = pointer - start

        self.reader.seek(pointer)

        return count

    def location(self) -> Location:
        """Returns (line_number, col) of `index` in `s`."""
        return Location(self.line_no(), self.column_no())


@dataclass
class Token:
    value: Union[str, int, float]
    kind: Kind
    loc: Optional[Location] = None

    def match(self, other: Optional["Token"]):
        if other is None:
            return False

        return self.value == other.value and self.kind == other.kind


class Lexer(Protocol):
    def lex(self, cursor: Cursor) -> Optional[Token]:
        ...


class KeywordLexer:
    def lex(self, cursor):
        cursor_start = cursor.location()
        match = None
        options = [e.value for e in Keyword]
        options.sort(key=len, reverse=True)

        for option in options:
            l = len(option)

            substr = cursor.peek(l)
            lower_substr = substr.lower()
            if lower_substr == option:
                cursor.read(l)
                match = lower_substr

        if match is None:
            return None

        return Token(match, Kind.keyword, cursor_start)


class NullLexer:
    """
    Matches against literals currently only for NULL.
    """

    def lex(self, cursor):
        cursor_start = cursor.location()

        search_term = "null"
        l = len(search_term)
        substr = cursor.peek(l)
        lower_substr = substr.lower()

        if lower_substr != search_term:
            return
        else:
            cursor.read(l)

        return Token(search_term, Kind.null, cursor_start)


class NumericLexer:
    @staticmethod
    def is_digit(c: str):
        return c >= "0" and c <= "9"

    @staticmethod
    def is_period(c: str):
        return c == "."

    @staticmethod
    def is_exp_marker(c: str):
        return c == "e"

    def lex(self, cursor):
        cursor_start = cursor.location()
        period_found = False
        exp_marker_found = False

        c = cursor.peek()
        value = ""

        if not self.is_digit(c) and not self.is_period(c):
            return None

        while not cursor.is_complete():
            c = cursor.peek()

            if self.is_period(c):
                if period_found:
                    # What cases would you have ".."?
                    return None

                period_found = True
                value += cursor.read(1)
                continue

            if self.is_exp_marker(c):
                if exp_marker_found:
                    return None

                # No periods allowed after expMarker
                period_found = True
                exp_marker_found = True

                c_next = cursor.peek()
                if c_next == "-" or c_next == "+":
                    value += cursor.read(1)

                value += cursor.read(1)
                continue

            if not self.is_digit(c):
                break

            value += cursor.read(1)

        # No characters accumulated
        if len(value) == 0:
            return None

        return Token(
            float(value),
            Kind.integer,
            cursor_start,
        )


class SymbolLexer:
    def lex(self, cursor):
        options = [e.value for e in Symbol]
        cursor_start = cursor.location()
        current = cursor.peek()

        try:
            options.index(current)
        except ValueError:
            return None

        cursor.read(1)

        return Token(
            current,
            Kind.symbol,
            cursor_start,
        )


class DelimitedLexer:
    def __init__(self, delimiter: str, kind: Kind) -> None:
        self.delimiter = delimiter
        self.kind = kind

    def lex(self, cursor):
        cursor_start = cursor.location()
        if cursor.peek() != self.delimiter:
            return None

        # Now we have found the delimiter
        # we want to continue until we find the
        # end delimiter.
        cursor.read(1)

        value = ""
        while not cursor.is_complete():
            if cursor.peek() == self.delimiter:
                # Move over the delimiter.
                cursor.read(1)

                return Token(
                    value,
                    self.kind,
                    cursor_start,
                )

            value += cursor.read(1)

        return None


class StringLexer(DelimitedLexer):
    def __init__(self):
        super().__init__("'", Kind.text)


class IdentifierLexer:
    def __init__(self) -> None:
        self.double_quote = DelimitedLexer('"', Kind.identifier)

    def lex(self, cursor):
        # Look for double quote texts.
        cursor_start = cursor.location()
        token = self.double_quote.lex(cursor)

        if token:
            return token

        c = cursor.peek()

        is_alphabetical = (c >= "A" and c <= "Z") or (c >= "a" and c <= "z")

        if not is_alphabetical:
            return None

        value = cursor.read(1)

        while not cursor.is_complete():
            c = cursor.peek()

            is_alphabetical = (c >= "A" and c <= "Z") or (c >= "a" and c <= "z")
            is_numeric = c >= "0" and c <= "9"

            if is_alphabetical or is_numeric or c == "$" or c == "_":
                value += cursor.read(1)
                continue

            break

        if len(value) == 0:
            return None

        return Token(value.lower(), Kind.identifier, cursor_start)


class StatementLexer:
    @staticmethod
    def lex(source: str) -> List[Token]:
        source = source.strip()
        discard_characters = [" ", "\n", "\r"]
        tokens = []
        cursor = Cursor(source)
        # Explicitly describe type here
        # So we ensure each of these is a lexer.
        lexers: List[Lexer] = [
            KeywordLexer(),  # Note keyword should always have first pick.
            NullLexer(),
            SymbolLexer(),
            NumericLexer(),
            StringLexer(),
            IdentifierLexer(),
        ]

        while not cursor.is_complete():
            if cursor.peek() in discard_characters:
                # move the cursor forward
                # when discarding things.
                cursor.read(1)
                continue

            for lexer in lexers:
                token = lexer.lex(cursor)
                if token:
                    tokens.append(token)
                    break
            else:
                # Else in a for loop is executed when
                # it no breaks are called.
                # Therefore this means no tokens were found.
                location = cursor.location()
                raise LexingException(
                    f"Lexing error at location {location.line}:{location.col}"
                )

        return tokens
