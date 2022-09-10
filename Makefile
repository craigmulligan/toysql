.PHONY: test
test:
	python -m unittest discover ./tests

.PHONY: pytest
pytest:
	pytest .

.PHONY: typetest
typetest:
	pyright .

.PHONY: ci
ci: pytest typetest
	echo "Success"
