.PHONY: install lint test-unit test

install:
	pip install -r requirements-dev.txt

lint:
	ruff check .

test-unit:
	pytest -m "not integration"
