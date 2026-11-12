.PHONY: install lint test-unit test

install:
	pip install -r requirements-dev.txt

lint:
	ruff check .

test-unit:
	.venv/bin/pytest -m "not integration"

test:
	docker compose -f docker-compose.test.yml up -d --wait
	.venv/bin/pytest; status=$$?; docker compose -f docker-compose.test.yml down -v; exit $$status
