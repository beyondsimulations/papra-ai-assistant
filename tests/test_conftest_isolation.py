"""Pin the autouse SECRET_REGISTRY isolation fixture in the root conftest.py.

Dropping that fixture keeps a full-suite (alphabetical) run green because the
default collection order masks the pollution: the leak only surfaces for
non-alphabetical invocations. These two tests enforce the isolation contract
within one module instead — pytest runs tests in definition order, so if
the autouse fixture is removed, test_b fails (test_a's sentinel leaks into
it), no matter the surrounding suite order.
"""

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
