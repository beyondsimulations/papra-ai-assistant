# Empty conftest at project root so pytest prepends the repo root to sys.path,
# making the `papra_suite` package importable in tests without an install step.

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
