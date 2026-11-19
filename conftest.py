# Empty conftest at project root so pytest prepends the repo root to sys.path,
# making the `papra_suite` package importable in tests without an install step.

import logging

import pytest

from papra_suite.logging_setup import SECRET_REGISTRY


@pytest.fixture(autouse=True)
def _isolate_secret_registry():
    """Snapshot the process-global SECRET_REGISTRY's secret set before each
    test and restore it after, so no test can leak registered secrets into
    another module: poller tests register mailbox passwords (e.g. "p") for
    redaction, and a leaked secret redacts other tests' sentinels (observed:
    "idem[REDACTED]otency sentinel line"). One mechanism, one place — module
    -level copies of this isolation are unnecessary."""
    saved = set(SECRET_REGISTRY._secrets)
    yield
    SECRET_REGISTRY._secrets.clear()
    SECRET_REGISTRY._secrets.update(saved)


@pytest.fixture(autouse=True)
def _isolate_root_logger():
    """Snapshot the root logger's handlers and level, and restore them after each test.

    `setup_logging()` clears the root handlers and attaches a stdout handler bound to
    whatever `sys.stdout` was at that moment, plus a `RingBufferHandler` holding that
    test's store. Left installed they outlive the test, and the next test's log line is
    written to a **closed** captured stream: the logging module answers by dumping a
    `--- Logging error ---` traceback onto *that* test's stderr. It bit
    `test_mcp_tokens.py`, whose claim is that minting writes nothing to stderr, and only
    in full-suite ordering.

    One mechanism in one place, following the fixture above and the note in
    `test_logging_setup.py`, which has a module-local copy of this and says why the global
    one is the right shape.
    """
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
