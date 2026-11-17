# Local quality gates. Everything here runs offline, and no target destroys
# anything unless its name says so.
#
# Tools are taken from the project venv by default. Two reasons, both real: a bare
# `ruff` on PATH means `make lint` silently checks whatever ruff some other project
# left there, and `make install`'s pip does not exist on a venv bootstrapped with
# `uv` — so the README's setup instructions were unrunnable on this machine as
# written. Override to use a system environment: `make lint VENV=`.
VENV ?= .venv/bin/
PYTEST ?= $(VENV)python -m pytest
RUFF ?= $(VENV)ruff
COMPOSE ?= docker compose -f docker-compose.test.yml

.PHONY: install install-uv lint test-unit test-integration ci test test-ci \
        stack-up stack-down stack-digests

install:
	pip install -r requirements-dev.txt

# The same install for a uv-managed venv, which is what the README now describes.
install-uv:
	uv pip install -r requirements-dev.txt --python $(VENV)python

# Lint covers the whole tree except docs/superpowers/probes/, excluded in
# pyproject.toml: those scripts are measurement evidence, and reformatting them
# would sever a quoted log from the file that produced it.
lint:
	$(RUFF) check .

test-unit:
	$(PYTEST) -m "not integration"

# The stack-backed suite, against a stack that is ALREADY RUNNING. It deliberately
# brings nothing up and takes nothing down: `down -v` is correct in CI and on a
# machine where this is the only Papra, it deletes the scratch instance someone is
# mid-measurement against.
#
# PAPRA_REQUIRE_STACK=1 is not decoration. Without it the integration tests SKIP
# when the stack is absent, and a skipped suite exits green — with it, an
# unreachable stack is a hard failure, which is the only way "I ran the tests"
# means anything.
test-integration:
	PAPRA_REQUIRE_STACK=1 $(PYTEST) tests/integration

# Every gate that needs no docker lifecycle. Assumes the stack for the integration
# half (start it with `make stack-up`); if the stack is not there, it fails rather
# than skipping.
ci: lint test-unit test-integration

# CI-shaped run: bring the stack up, run everything, then destroy it. Destructive
# by design. Papra's data does not survive `down` either way — the compose file
# declares no named volumes, so its database is container filesystem — so treat
# both targets here as "this scratch stack is disposable".
test test-ci:
	$(COMPOSE) up -d --wait
	PAPRA_REQUIRE_STACK=1 $(PYTEST); status=$$?; $(COMPOSE) down -v; exit $$status

stack-up:
	$(COMPOSE) up -d --wait

stack-down:
	$(COMPOSE) down

# Print the digests this repo pins next to what upstream now serves, which is the
# comparison needed to change a pin on purpose. The pinned refs are already
# immutable, so `docker compose pull` alone would re-fetch exactly what is already
# here and report success — the :latest pulls below are the only way to learn that
# upstream moved. After updating a pin: `make test-ci` before committing it.
stack-digests:
	@grep -E '^[[:space:]]+image:' docker-compose.test.yml | sed 's/^[[:space:]]*/  pinned: /'
	@echo "  newest upstream now:"
	@docker pull --quiet ghcr.io/papra-hq/papra:latest | sed 's/^/    papra     /'
	@docker pull --quiet greenmail/standalone:latest | sed 's/^/    greenmail /'
