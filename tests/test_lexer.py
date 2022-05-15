from typing import List
from toysql.lexer import StatementLexer, Kind, Token
from unittest import TestCase


class TestRepl(TestCase):
    @staticmethod
    def get_token_by_kind(tokens: List[Token], kind: Kind):
        return [token for token in tokens if token.kind == kind][0]

    def test_select(self):
        query = """
            select * from my_table 
            where x = 'hi'
            and y = 123;
        """
        tokens = StatementLexer().lex(query)
        assert len(tokens) == 2
        string_token = self.get_token_by_kind(tokens, Kind.string)
        assert string_token.value == "hi"

        numeric_token = self.get_token_by_kind(tokens, Kind.numeric)
        assert numeric_token.value == "123"

        # Once we have all lexers the cursor will be correct.
        # assert string_token.loc.col == 34
        # assert string_token.loc.line == 0
