from dataclasses import dataclass
from enum import Enum, auto
from io import StringIO
from toysql.exceptions import LexingException
from typing import List, Optional, Union


class Identifier(Enum):
    # Only one type of identifier
    long = auto()
    # Dont support delimited
    # identifiers yet.
    delimited = auto()


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
    integer = "integer"
    bool = "bool"
    text = "text"
    null = "null"
    primary = "primary"
    key = "key"


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

    null = 0
    byte = 2
    smallint = 3
    integer = 4
    text = 5

    @staticmethod
    def infer(v):
        if isinstance(v, str):
            return DataType.text

        if v == None:
            return DataType.null

        if isinstance(v, int):
            return DataType.integer

        raise Exception(f"Unable to infer datatype of {v}")


TokenType = Union[DataType, Symbol, Keyword, Identifier]


class Kind(Enum):
    keyword = auto()
    symbol = auto()
    identifier = auto()
    datatype = auto()


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

    def __len__(self) -> int:
        """
        length of underlying str.
        """
        return len(self.reader.getvalue())

    def peek(self, size=1) -> str:
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
    value: str
    type: TokenType
    loc: Optional[Location]

    def __init__(self, type: TokenType, loc: Optional[Location] = None, value=None):
        self.type = type
        self.loc = loc

        if isinstance(type, Keyword):
            self.kind = Kind.keyword
        elif isinstance(type, Symbol):
            self.kind = Kind.keyword
        elif isinstance(type, Identifier):
            self.kind = Kind.identifier
        elif isinstance(type, DataType):
            self.kind = Kind.datatype
        else:
            raise Exception("Unknown token type -> kind mapping")

        if value is None:
            self.value = self.type.value
        else:
            self.value = value


def is_alphabetical(c: str):
    return (c >= "A" and c <= "Z") or (c >= "a" and c <= "z")


def is_digit(c: str):
    return c >= "0" and c <= "9"


def is_period(c: str):
    return c == "."


def is_exp_marker(c: str):
    return c == "e"


def keyword_lexer(cursor: Cursor) -> Optional[Token]:
    cursor_start = cursor.location()
    match = None
    options = [e.value for e in Keyword]
    options.sort(key=len, reverse=True)

    for option in options:
        l = len(option)

        substr = cursor.peek(l)
        lower_substr = substr.lower()
        if lower_substr == option:
            # matched.

            # Now we need to make sure it's either
            # a full word OR it's the end of the string.
            # cursor.peek will return as much as it can
            # even if you give a length greater than
            # the underlying string. That's why we
            # check l == len(with_next_char)
            with_next_char = cursor.peek(l + 1)

            if l == len(with_next_char) or not is_alphabetical(with_next_char[-1]):
                # now makesure it's a complete word
                # but checking the next char is a space.
                cursor.read(l)
                match = lower_substr

    if match is None:
        return None

    return Token(type=Keyword(match), loc=cursor_start)


def numeric_lexer(cursor: Cursor):
    # TODO - this currently handles
    # floating points - we should
    # instead just do floats.
    cursor_start = cursor.location()
    period_found = False
    exp_marker_found = False

    c = cursor.peek()
    value = ""

    if not is_digit(c) and not is_period(c):
        return None

    while not cursor.is_complete():
        c = cursor.peek()

        if is_period(c):
            if period_found:
                # What cases would you have ".."?
                return None

            period_found = True
            value += cursor.read(1)
            continue

        if is_exp_marker(c):
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

        if not is_digit(c):
            break

        value += cursor.read(1)

    # No characters accumulated
    if len(value) == 0:
        return None

    return Token(type=DataType.integer, loc=cursor_start, value=value)


def symbol_lexer(cursor: Cursor):
    options = [e.value for e in Symbol]
    cursor_start = cursor.location()
    current = cursor.peek()

    try:
        options.index(current)
    except ValueError:
        return None

    cursor.read(1)

    return Token(
        type=Symbol(current),
        loc=cursor_start,
    )


class DelimitedLexer:
    def __init__(self, delimiter: str, type: TokenType, kind: Kind) -> None:
        self.delimiter = delimiter
        self.type = type
        self.kind = kind

    def lex(self, cursor: Cursor):
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

                return Token(type=self.type, loc=cursor_start, value=value)

            value += cursor.read(1)

        return None


def text_lexer(cursor: Cursor):
    lexer = DelimitedLexer("'", DataType.text, Kind.datatype)
    return lexer.lex(cursor)


def identifier_lexer(cursor: Cursor):
    # Look for double quote texts.
    cursor_start = cursor.location()
    token = DelimitedLexer('"', Identifier.long, Kind.identifier).lex(cursor)

    if token:
        return token

    c = cursor.peek()

    if not is_alphabetical(c):
        return None

    value = cursor.read(1)

    while not cursor.is_complete():
        c = cursor.peek()

        is_numeric = c >= "0" and c <= "9"

        if is_alphabetical(c) or is_numeric or c == "$" or c == "_":
            value += cursor.read(1)
            continue

        break

    if len(value) == 0:
        return None

    return Token(type=Identifier.long, loc=cursor_start, value=value.lower())


def lex(source: str) -> List[Token]:
    source = source.strip()
    discard_characters = [" ", "\n", "\r"]
    tokens = []
    cursor = Cursor(source)
    # Explicitly describe type here
    # So we ensure each of these is a lexer.
    lexers = [
        keyword_lexer,  # Note keyword should always have first pick.
        symbol_lexer,
        numeric_lexer,
        text_lexer,
        identifier_lexer,
    ]

    while not cursor.is_complete():
        if cursor.peek() in discard_characters:
            # move the cursor forward
            # when discarding things.
            cursor.read(1)
            continue

        for lexer in lexers:
            token = lexer(cursor)
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
