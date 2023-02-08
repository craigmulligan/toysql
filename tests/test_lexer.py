from typing import List
from toysql.lexer import (
    Kind,
    Keyword,
    Identifier,
    DataType,
    Symbol,
    Token,
    Cursor,
    Location,
    symbol_lexer,
    numeric_lexer,
    text_lexer,
    keyword_lexer,
    identifier_lexer,
    lex,
)
from unittest import TestCase
from toysql.exceptions import LexingException


class TestSymbolLexer(TestCase):
    def test_lex(self):
        cases = [(",b", ",", 1), ("*", "*", 1), (" *", None, 0), ("select", None, 0)]

        for source, value, pointer in cases:
            cursor = Cursor(source)
            token = symbol_lexer(cursor)

            assert cursor.pointer == pointer
            if token:
                assert token.value == value
            else:
                assert value is None


class TestNumericLexer(TestCase):
    def test_lex(self):
        cases = [
            ("123 adf", "123", 3),
            ("123 ", "123", 3),
            (" 123", None, 0),
            ("1.11 ", "1.11", 4),
            ("select", None, 0),
        ]

        for source, value, pointer in cases:
            cursor = Cursor(source)
            token = numeric_lexer(cursor)
            if token:
                assert token.value == value
                assert cursor.pointer == pointer
            else:
                assert value is None
                assert cursor.pointer == pointer


class TestKeywordLexer(TestCase):
    def test_lex(self):
        cases = [
            ("select", "select"),
            (" select", None),
            ("hello", None),
            ("from tablename", "from"),
        ]

        for source, value in cases:
            cursor = Cursor(source)
            token = keyword_lexer(cursor)

            if token:
                assert token.value == value
            else:
                assert value is None


class TestStringLexer(TestCase):
    def test_lex(self):
        cases = [("'abc'", "abc", 5), (" 'abc'", None, 0), ("select", None, 0)]

        for source, value, index in cases:
            cursor = Cursor(source)
            token = text_lexer(cursor)
            if token:
                assert token.value == value
                assert cursor.pointer == index
            else:
                assert value is None
                assert cursor.pointer == index


class TestIdentifierLexer(TestCase):
    def test_lex(self):
        cases = [
            ("my_table", "my_table"),
            ('"hello"', "hello"),
            ("12345", None),
        ]

        for source, value in cases:
            cursor = Cursor(source)
            token = identifier_lexer(cursor)
            if token:
                assert token.value == value
            else:
                assert value is None


class TestStatementLexer(TestCase):
    @staticmethod
    def get_token_by_kind(tokens: List[Token], kind: Kind):
        return (token for token in tokens if token.kind == kind)

    def test_select_x(self):
        query = """select * from "my_table"\nwhere x = 'hi'\nand y = 123;"""

        tokens = lex(query)

        expected_tokens = [
            Token(type=Keyword.select, loc=Location(0, 0)),
            Token(type=Symbol.asterisk, loc=Location(0, 7)),
            Token(type=Keyword._from, loc=Location(0, 9)),
            Token(type=Identifier.long, value="my_table", loc=Location(0, 14)),
            Token(type=Keyword.where, loc=Location(1, 0)),
            Token(value="x", type=Identifier.long, loc=Location(1, 6)),
            Token(type=Symbol.equal, loc=Location(1, 8)),
            Token(value="hi", type=DataType.text, loc=Location(1, 10)),
            Token(type=Keyword._and, loc=Location(2, 0)),
            Token(value="y", type=Identifier.long, loc=Location(2, 4)),
            Token(type=Symbol.equal, loc=Location(2, 6)),
            Token(value="123", type=DataType.integer, loc=Location(2, 8)),
            Token(type=Symbol.semicolon, loc=Location(2, 11)),
        ]

        assert tokens == expected_tokens

    def test_select_multi_columns(self):
        query = """select x,y from "my_table"\nwhere x = 'hi'\nand y = 123;"""

        tokens = lex(query)

        expected_tokens = [
            Token(type=Keyword.select, loc=Location(0, 0)),
            Token(value="x", type=Identifier.long, loc=Location(0, 7)),
            Token(type=Symbol.comma, loc=Location(0, 8)),
            Token(type=Identifier.long, value="y", loc=Location(0, 9)),
            Token(type=Keyword._from, loc=Location(0, 11)),
            Token(value="my_table", type=Identifier.long, loc=Location(0, 16)),
            Token(type=Keyword.where, loc=Location(1, 0)),
            Token(value="x", type=Identifier.long, loc=Location(1, 6)),
            Token(type=Symbol.equal, loc=Location(1, 8)),
            Token(value="hi", type=DataType.text, loc=Location(1, 10)),
            Token(type=Keyword._and, loc=Location(2, 0)),
            Token(value="y", type=Identifier.long, loc=Location(2, 4)),
            Token(type=Symbol.equal, loc=Location(2, 6)),
            Token(value="123", type=DataType.integer, loc=Location(2, 8)),
            Token(type=Symbol.semicolon, loc=Location(2, 11)),
        ]

        assert tokens == expected_tokens

    def test_create_table(self):
        query = """CREATE TABLE u (id INT, name TEXT)"""

        tokens = lex(query)

        expected_tokens = [
            Token(type=Keyword.create, loc=Location(0, 0)),
            Token(type=Keyword.table, loc=Location(0, 7)),
            Token(value="u", type=Identifier.long, loc=Location(0, 13)),
            Token(type=Symbol.left_paren, loc=Location(0, 15)),
            Token(value="id", type=Identifier.long, loc=Location(0, 16)),
            Token(type=Keyword.int, loc=Location(0, 19)),
            Token(type=Symbol.comma, loc=Location(0, 22)),
            Token(value="name", type=Identifier.long, loc=Location(0, 24)),
            Token(type=Keyword.text, loc=Location(0, 29)),
            Token(type=Symbol.right_paren, loc=Location(0, 33)),
        ]

        assert tokens == expected_tokens

    def test_insert(self):
        query = """INSERT INTO users VALUES (1, 'Phil');"""

        tokens = lex(query)

        expected_tokens = [
            Token(type=Keyword.insert, loc=Location(0, 0)),
            Token(type=Keyword.into, loc=Location(0, 7)),
            Token(value="users", type=Identifier.long, loc=Location(0, 12)),
            Token(type=Keyword.values, loc=Location(0, 18)),
            Token(type=Symbol.left_paren, loc=Location(0, 25)),
            Token(value="1", type=DataType.integer, loc=Location(0, 26)),
            Token(type=Symbol.comma, loc=Location(0, 27)),
            Token(value="Phil", type=DataType.text, loc=Location(0, 29)),
            Token(type=Symbol.right_paren, loc=Location(0, 35)),
            Token(type=Symbol.semicolon, loc=Location(0, 36)),
        ]

        assert tokens == expected_tokens

    def test_invalid_sql_symbol(self):
        query = """INSERT $$"""

        with self.assertRaises(LexingException) as exec_info:
            lex(query)

        assert str(exec_info.exception) == "Lexing error at location 0:7"

    def test_partial_keywork(self):
        query = """CREATE TABLE schema (id INT, schema_type TEXT, name TEXT, associated_table_name TEXT, sql_text TEXT, root_page_number INT);"""
        tokens = lex(query)

        assert (
            Token(type=Keyword._as, loc=Location(line=0, col=58))
            not in tokens
        )
