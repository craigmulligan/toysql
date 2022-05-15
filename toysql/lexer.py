from typing import Tuple, List, Protocol, Optional
from dataclasses import dataclass
from enum import Enum, auto
from toysql.exceptions import LexingException

# Keyword = str
# Symbol = str
# TokenKind = int


class Keyword(Enum):
    select = "select"
    _from = "from"
    _as = "as"
    where = "where"
    _and = "and"
    create = "create"
    insert = "insert"
    into = "into"
    values = "values"
    int = "int"
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


class Kind(Enum):
    keyword = auto()
    symbol = auto()
    identifier = auto()
    string = auto()
    numeric = auto()
    bool = auto()
    null = auto()


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
    value: str
    kind: Kind
    loc: Location

    def __eq__(self, other: "Token"):  # type: ignore[override]
        return self.value == other.value and self.kind == other.kind


class Lexer(Protocol):
    def lex(self, data: str, cursor: Cursor) -> Tuple[Optional[Token], Cursor, bool]:
        ...


def longest_match(source: str, options: List[str]) -> Optional[str]:
    """
    Given a string we find the longest_match from the first character.
    """
    options.sort(key=len, reverse=True)
    for option in options:
        l = len(option)
        substr = source[:l]
        if substr == option:
            return substr


class KeywordLexer(Lexer):
    def lex(self, source, ic):
        cursor = ic.copy()
        options = [e.value for e in Keyword]

        match = longest_match(source[cursor.pointer :], options)

        if match is None:
            return None, cursor

        cursor.pointer = cursor.pointer + len(match)
        cursor.loc.col = cursor.loc.col + len(match)

        kind = Kind.keyword
        if match == Keyword.true or match == Keyword.false:
            kind = Kind.bool

        if match == Keyword.null:
            kind = Kind.null

        return Token(match, kind, cursor.loc), cursor


class NumericLexer(Lexer):
    def lex(self, source, ic):
        period_found = False
        exp_marker_found = False

        cursor = ic.copy()
        while cursor.pointer < len(source):
            c = source[cursor.pointer]
            cursor.loc.col += 1
            is_digit = c >= "0" and c <= "9"
            is_period = c == "."
            is_exp_marker = c == "e"

            if cursor.pointer == ic.pointer:
                if not is_digit and not is_period:
                    return None, ic

                period_found = is_period
                cursor.pointer += 1
                continue

            if is_period:
                if period_found:
                    return None, ic

                period_found = True
                cursor.pointer += 1
                continue

            if is_exp_marker:
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
                    cursor.pointer += 1
                    cursor.loc.col += 1

                cursor.pointer += 1
                continue

            if not is_digit:
                break

            cursor.pointer += 1

        if cursor.pointer == ic.pointer:
            return None, ic

        return (
            Token(
                source[ic.pointer : cursor.pointer],
                Kind.numeric,
                ic.loc,
            ),
            cursor,
        )


class DelimitedLexer(Lexer):
    def __init__(self, delimiter: str, kind: Kind) -> None:
        self.delimiter = delimiter
        self.kind = kind

    def lex(self, source, cursor):
        remaining_source = source[cursor.pointer :]
        if len(remaining_source) == 0:
            return None, cursor

        if source[cursor.pointer] != self.delimiter:
            # we haven't found the delimiter
            # break exit early
            return None, cursor

        value = ""

        cursor.loc.col += 1
        cursor.pointer += 1

        # Now we have found the delimiter
        # we want to continue until we find the
        # end delimiter.
        while cursor.pointer < len(source):
            c = source[cursor.pointer]
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
                    cursor.pointer += 1
                    cursor.loc.col += 1

            value = value + c
            cursor.loc.col += 1
        return None, cursor


class StringLexer(DelimitedLexer):
    def __init__(self):
        super().__init__("'", Kind.string)


class IdentifierLexer(Lexer):
    def __init__(self) -> None:
        self.double_quote = DelimitedLexer('"', Kind.identifier)

    def lex(self, source, ic):
        # Look for double quote texts.
        token, cursor = self.double_quote.lex(source, ic)

        if token:
            return token, cursor

        # TODO find the rest.
        return None, cursor


class StatementLexer:
    @staticmethod
    def lex(source: str) -> List[Token]:
        tokens = []
        cursor = Cursor(0, Location(0, 0))
        lexers = [
            NumericLexer(),
            StringLexer(),
            IdentifierLexer(),
            KeywordLexer(),
        ]
        while cursor.pointer < len(source):
            new_tokens = []
            for lexer in lexers:
                token, cursor = lexer.lex(source, cursor)
                # Omit nil tokens for valid, but empty syntax like newlines
                if token is not None:
                    new_tokens.append(token)
                continue

            if len(new_tokens) == 0:
                # Should be able to remove this once all
                # lexers are implemented.
                cursor.pointer += 1

            tokens.extend(new_tokens)
            hint = ""
            if len(tokens) > 0:
                hint = "after " + tokens[-1].value

            LexingException(
                f"Unable to lex token {hint}, at {cursor.loc.line}:{cursor.loc.col}"
            )
        return tokens
