from typing import Tuple, List, Protocol, Optional, Union
from dataclasses import dataclass
from enum import Enum, auto
from toysql.exceptions import LexingException


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
    null = "null"
    true = "true"
    false = "true"


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
    REAL. The value is a floating point value, stored as an 8-byte IEEE floating point number.
    TEXT. The value is a text text, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
    BLOB. The value is a blob of data, stored exactly as it was input.
    """

    null = auto()
    integer = auto()
    text = auto()
    blob = auto()


class Kind(Enum):
    """
    Kind is a superset of datatypes

    You can't nicely extend Enums in python.
    """

    keyword = auto()
    symbol = auto()
    identifier = auto()
    bool = auto()
    discard = auto()
    null = auto()
    integer = auto()
    text = auto()
    blob = auto()


@dataclass
class Location:
    line: int
    col: int


@dataclass
class Cursor:
    pointer: int
    loc: Location

    def copy(self):
        return Cursor(self.pointer, Location(self.loc.line, self.loc.col))


@dataclass
class Token:
    value: Union[str, int]
    kind: Kind
    loc: Optional[Location] = None

    def __eq__(self, other: Optional["Token"]):  # type: ignore[override]
        if other is None:
            return False
        return self.value == other.value and self.kind == other.kind


class Lexer(Protocol):
    def lex(self, data: str, cursor: Cursor) -> Tuple[Optional[Token], Cursor, bool]:
        ...


def longest_match(source: str, options: List[str]) -> Optional[str]:
    """
    Given a text we find the longest_match from the first character.
    """
    options.sort(key=len, reverse=True)
    for option in options:
        l = len(option)
        substr = source[:l]
        lower_substr = substr.lower()
        if lower_substr == option:
            return lower_substr


class KeywordLexer(Lexer):
    def lex(self, source, ic):
        cursor = ic.copy()
        options = [e.value for e in Keyword]

        match = longest_match(source[cursor.pointer :], options)

        if match is None:
            return None, ic

        cursor.pointer = cursor.pointer + len(match)
        cursor.loc.col = cursor.loc.col + len(match)

        kind = Kind.keyword

        # TODO this is a hack.
        # Should be handled in own Lexer.
        if match == Keyword.true or match == Keyword.false:
            kind = Kind.bool

        if match == Keyword.null:
            kind = Kind.null

        return Token(match, kind, cursor.loc), cursor


def is_digit(c: str):
    return c >= "0" and c <= "9"


def is_period(c: str):
    return c == "."


def is_exp_marker(c: str):
    return c == "e"


class NumericLexer(Lexer):
    def lex(self, source, ic):
        period_found = False
        exp_marker_found = False

        cursor = ic.copy()

        c = source[cursor.pointer]

        if not is_digit(c) and not is_period(c):
            return None, ic

        while cursor.pointer < len(source):
            c = source[cursor.pointer]
            if is_period(c):
                if period_found:
                    return None, ic

                period_found = True
                cursor.loc.col += 1
                cursor.pointer += 1
                continue

            if is_exp_marker(c):
                if exp_marker_found:
                    return None, ic

                # No periods allowed after expMarker
                period_found = True
                exp_marker_found = True

                # expMarker must be followed by digits
                if cursor.pointer == len(source) - 1:
                    return None, ic

                c_next = source[cursor.pointer + 1]
                if c_next == "-" or c_next == "+":
                    cursor.loc.col += 1
                    cursor.pointer += 1

                cursor.loc.col += 1
                cursor.pointer += 1
                continue

            if not is_digit(c):
                break

            cursor.loc.col += 1
            cursor.pointer += 1

        # No characters accumulated
        if cursor.pointer == ic.pointer:
            return None, ic

        return (
            Token(
                int(source[ic.pointer : cursor.pointer]),
                Kind.integer,
                cursor.loc,
            ),
            cursor,
        )


class SymbolLexer(Lexer):
    def lex(self, source, ic):
        cursor = ic.copy()
        c = source[cursor.pointer]
        # Will get overwritten later if not an ignored syntax

        options = [e.value for e in Symbol]
        match = longest_match(source[cursor.pointer :], options)

        if match is None:
            return None, cursor

        cursor.pointer = cursor.pointer + len(match)

        return (
            Token(
                c,
                Kind.symbol,
                ic.loc,
            ),
            cursor,
        )


class DelimitedLexer(Lexer):
    def __init__(self, delimiter: str, kind: Kind) -> None:
        self.delimiter = delimiter
        self.kind = kind

    def lex(self, source, ic):
        cursor = ic.copy()

        remaining_source = source[cursor.pointer :]
        if len(remaining_source) == 0:
            return None, cursor

        if source[cursor.pointer] != self.delimiter:
            # we haven't found the delimiter
            # break exit early
            return None, cursor

        # Now we have found the delimiter
        # we want to continue until we find the
        # end delimiter.
        value = ""
        cursor.pointer += 1
        cursor.loc.col += 1

        while cursor.pointer < len(source):
            c = source[cursor.pointer]
            cursor.loc.col += 1
            cursor.pointer += 1

            if c == self.delimiter:
                # SQL escapes are via double characters, not backslash.
                if (
                    cursor.pointer + 1 >= len(source)
                    or source[cursor.pointer + 1] != self.delimiter
                ):
                    return (
                        Token(
                            value,
                            self.kind,
                            cursor.loc,
                        ),
                        cursor,
                    )
                else:
                    value = value + self.delimiter

            value = value + c
        return None, cursor


class StringLexer(DelimitedLexer):
    def __init__(self):
        super().__init__("'", Kind.text)


class IdentifierLexer(Lexer):
    def __init__(self) -> None:
        self.double_quote = DelimitedLexer('"', Kind.identifier)

    def lex(self, source, ic):
        # Look for double quote texts.
        token, cursor = self.double_quote.lex(source, ic)

        if token:
            return token, cursor

        c = source[cursor.pointer]

        is_alphabetical = (c >= "A" and c <= "Z") or (c >= "a" and c <= "z")

        if not is_alphabetical:
            return None, ic

        value = c
        cursor.loc.col += 1
        cursor.pointer += 1

        while cursor.pointer < len(source):
            c = source[cursor.pointer]
            cursor.loc.col += 1
            cursor.pointer += 1

            is_alphabetical = (c >= "A" and c <= "Z") or (c >= "a" and c <= "z")
            is_numeric = c >= "0" and c <= "9"

            if is_alphabetical or is_numeric or c == "$" or c == "_":

                value += c
                continue

            break

        if len(value) == 0:
            return None, ic

        return (
            Token(
                value.lower(),
                Kind.identifier,
                ic.loc,
            ),
            cursor,
        )


class DiscardLexer(Lexer):
    """
    This is the simplest lexer, it finds things like whitespace
    and line break and moves the cursor forward accordingly.

    The tokens of Kind.discard are removed later.
    """

    def lex(self, data, cursor):
        c = data[cursor.pointer]

        if c == "\n":
            cursor.pointer += 1
            cursor.loc.line = +1
            cursor.loc.col = 0
            return Token(c, Kind.discard, cursor.loc), cursor

        if c == " ":
            cursor.pointer += 1
            cursor.loc.col += 1
            return Token(c, Kind.discard, cursor.loc), cursor

        return None, cursor


class StatementLexer:
    @staticmethod
    def lex(source: str) -> List[Token]:
        source = source.strip()
        tokens = []
        cursor = Cursor(0, Location(0, 0))
        lexers = [
            DiscardLexer(),
            KeywordLexer(),  # Note keyword should always have first pick.
            SymbolLexer(),
            NumericLexer(),
            StringLexer(),
            IdentifierLexer(),
        ]

        while cursor.pointer < len(source):
            pointer = cursor.pointer
            new_tokens = []
            for lexer in lexers:
                # Something

                token, cursor = lexer.lex(source, cursor)
                # Omit nil tokens for valid, but empty syntax like newlines
                if token is not None:
                    if token.kind != Kind.discard:
                        new_tokens.append(token)
                    break

            if pointer == cursor.pointer:
                raise Exception(
                    f"Cursor Pointer hasn't changed {pointer} - next few chars '{source[pointer:pointer+5]}'"
                )

            tokens.extend(new_tokens)
            hint = ""
            if len(tokens) > 0:
                hint = "after " + str(tokens[-1].value)

            LexingException(
                f"Unable to lex token {hint}, at {cursor.loc.line}:{cursor.loc.col}"
            )
        return tokens
