import sys
from typing import Callable
import logging


class InvalidQuery(Exception):
    pass


def parse(query: str):
    if not query.startswith("select"):
        # fake parsing.
        raise InvalidQuery("Invalid query")

    return f"your query was {query}"


def repl(callback: Callable):
    """
    repl is responsible for interacting with the
    db.
    """
    text = input("Enter your query:\n")

    try:
        logging.info(parse(text))
    except Exception as e:
        logging.error(f"Oops something went wrong: {e}")
    finally:
        callback(callback)


if __name__ == "__main__":
    print("Welcome to toysql.")
    # We pass itself as the callback
    # so that it infinitely continues
    try:
        repl(repl)
    except KeyboardInterrupt:
        sys.exit(0)
