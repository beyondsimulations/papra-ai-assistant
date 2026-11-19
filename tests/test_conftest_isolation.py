"""Pin the autouse isolation fixtures in the root conftest.py.

Dropping those fixtures keeps a full-suite (alphabetical) run green because the
default collection order masks the pollution: the leak only surfaces for
non-alphabetical invocations. These tests enforce the isolation contracts within
one module instead — pytest runs tests in definition order, so if an autouse
fixture is removed, the `_b_` test fails (the `_a_` test's state leaks into it),
no matter the surrounding suite order.

The second pair pins the root **logger**, which leaks the same way: `setup_logging()`
clears the root handlers and attaches ones bound to that test's captured stream and
closed store, and a later test's log line then fails against a closed file and prints
a `--- Logging error ---` traceback onto the *wrong* test's stderr.
"""

import logging

from papra_suite.logging_setup import SECRET_REGISTRY

SENTINEL = "isolation-pin-secret-9f2c"


def test_a_registers_sentinel_secret():
    # poller tests register mailbox passwords directly into the process-global
    # registry; simulate that registration
    SECRET_REGISTRY.add(SENTINEL)
    assert SENTINEL in SECRET_REGISTRY._secrets


def test_b_asserts_sentinel_not_leaked():
    # the autouse fixture snapshots and restores the registry around every
    # test: test_a's sentinel must NOT be visible here
    assert SENTINEL not in SECRET_REGISTRY._secrets


_WRITES: list[str] = []


class _Recorder(logging.Handler):
    """Stands in for the handler `setup_logging()` installs: it remembers what it was told."""

    def emit(self, record: logging.LogRecord) -> None:
        _WRITES.append(record.getMessage())


def test_a_installs_a_root_handler_the_way_setup_logging_does():
    logging.getLogger().addHandler(_Recorder())
    logging.getLogger("papra_suite.isolation.pin").warning("installed inside a test")
    assert _WRITES, "the handler was never installed, so this pin would prove nothing"
    _WRITES.clear()


def test_b_asserts_that_root_handler_did_not_leak():
    logging.getLogger("papra_suite.isolation.pin").warning("a line in the next test")
    assert _WRITES == [], "a test's root logger handler leaked into another test's logging"
