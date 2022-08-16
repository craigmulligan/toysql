test:
	python -m unittest discover ./tests

pytest:
	pytest tests/test_page.py tests/test_record.py tests/test_repl.py tests/test_parser.py tests/test_lexer.py

typetest:
	pyright .
