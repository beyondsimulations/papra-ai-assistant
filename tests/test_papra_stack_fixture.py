"""Unit tests for the papra_stack fixture's skip-or-fail decision (no Docker)."""

import pytest

from tests.integration.conftest import _skip_or_fail

REASON = "scratch Papra not running; use `make test`"


def test_skip_or_fail_fails_when_stack_required(monkeypatch):
    """With PAPRA_REQUIRE_STACK=1 (the `make test` leg), an unreachable stack
    must fail — not skip — so `make test` cannot exit green on a silent skip."""
    monkeypatch.setenv("PAPRA_REQUIRE_STACK", "1")
    try:
        _skip_or_fail(REASON)
    except pytest.skip.Exception:
        # A skip here would report this test as skipped (exit 0) — the exact
        # silent-skip bug this guard exists for — so make it a hard failure.
        pytest.fail(f"_skip_or_fail skipped despite PAPRA_REQUIRE_STACK=1: {REASON}")
    except pytest.fail.Exception as exc:
        assert "PAPRA_REQUIRE_STACK=1" in str(exc)
        assert REASON in str(exc)
    else:
        pytest.fail("_skip_or_fail returned without failing or skipping")


def test_skip_or_fail_skips_when_stack_not_required(monkeypatch):
    """Without the flag (manual runs), the fixture keeps skipping."""
    monkeypatch.delenv("PAPRA_REQUIRE_STACK", raising=False)
    with pytest.raises(pytest.skip.Exception, match="scratch Papra not running"):
        _skip_or_fail(REASON)
