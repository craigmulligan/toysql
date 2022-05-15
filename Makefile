test:
	python -m unittest discover ./tests

pytest:
	pytest .	

typetest:
	pyright .
