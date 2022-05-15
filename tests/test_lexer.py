from typing import List
from toysql.lexer import StatementLexer, Kind, Token
from unittest import TestCase


class TestRepl(TestCase):
    @staticmethod
    def get_token_by_kind(tokens: List[Token], kind: Kind):
        return (token for token in tokens if token.kind == kind)

    def test_select(self):
        query = """
            select * from my_table 
            where x = 'hi'
            and y = 123;
        """
        tokens = StatementLexer().lex(query)
        assert len(tokens) == 7
        string_tokens = self.get_token_by_kind(tokens, Kind.string)
        assert next(string_tokens).value == "hi"

        numeric_tokens = self.get_token_by_kind(tokens, Kind.numeric)
        assert next(numeric_tokens).value == "123"

        keyword_tokens = self.get_token_by_kind(tokens, Kind.keyword)
        expected_keywords = ["select", "from", "table", "where", "and"]

        for keyword in expected_keywords:
            assert next(keyword_tokens).value == keyword
