import logging
from toysql.repl import repl
from unittest import mock


def test_success(caplog):
    mock_callback = mock.Mock()
    query = "select * from my_table;"

    with caplog.at_level(logging.INFO), mock.patch("builtins.input", lambda _: query):
        repl(mock_callback)
        assert f"your query was {query}" in caplog.text

    mock_callback.assert_called_once()


def test_failure(caplog):
    mock_callback = mock.Mock()
    query = "selct * from my_table;"

    with caplog.at_level(logging.ERROR), mock.patch("builtins.input", lambda _: query):
        repl(mock_callback)
        assert f"Oops something went wrong" in caplog.text

    mock_callback.assert_called_once()
