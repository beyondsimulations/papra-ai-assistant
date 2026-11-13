"""Unit tests for the integration readiness probe (no Docker required)."""

import imaplib
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from tests.integration.conftest import _probe_readiness, _wait_for_greenmail_delivery


def test_wait_for_greenmail_delivery_raises_on_timeout(monkeypatch):
    """The delivery wait must fail loudly: an IMAP server that never finds
    the message must produce a TimeoutError within the window — never a
    hang (each per-attempt connection carries its own bounded timeout)."""

    class _NeverDelivered:
        def login(self, user, password):
            pass

        def select(self, mailbox):
            return ("OK", [b"1"])

        def uid(self, command, *args):
            return ("OK", [b""])   # SEARCH: the message is never found

        def logout(self):
            pass

    monkeypatch.setattr(imaplib, "IMAP4", lambda *args, **kwargs: _NeverDelivered())
    start = time.monotonic()
    with pytest.raises(TimeoutError, match="did not deliver"):
        _wait_for_greenmail_delivery("<missing@test>", timeout_s=0.2)
    assert time.monotonic() - start < 2.0


def test_probe_retries_on_503_until_200():
    """A server answering 503 while booting must not fail the probe outright;
    the probe retries on any response until the window expires."""
    start = time.monotonic()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if time.monotonic() - start < 2.0:
                self.send_response(503)
                self.end_headers()
            else:
                self.send_response(200)
                self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{port}"
        assert _probe_readiness(url, readiness_timeout_s=10.0, poll_interval_s=0.1)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def test_probe_accepts_401_as_up():
    """A healthy Papra stack may answer 401 (no API key sent); the probe must
    treat 401 as up, not keep retrying (mutating the acceptance set to (200,)
    would make this test fail)."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(401)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        start = time.monotonic()
        url = f"http://127.0.0.1:{port}"
        assert _probe_readiness(url, readiness_timeout_s=10.0, poll_interval_s=0.1)
        assert time.monotonic() - start < 2.0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def test_probe_returns_false_after_deadline():
    """When nothing ever answers 200/401 within the window, the probe must
    return False (not True) after roughly the deadline — the deadline-expiry
    branch must stay covered (mutating it to return True fails this test)."""
    # Verify the port is closed right before probing: bind to an ephemeral
    # port and close it, then immediately probe it. Connection-refused fails
    # fast, so the elapsed time is dominated by the deadline window.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    start = time.monotonic()
    result = _probe_readiness(
        f"http://127.0.0.1:{port}",
        readiness_timeout_s=1.0,
        poll_interval_s=0.1,
    )
    elapsed = time.monotonic() - start
    assert result is False
    assert elapsed >= 1.0


def test_probe_poll_interval_sleeps(monkeypatch):
    """Pins the time.sleep between probe attempts: deleting it turns the
    probe into a busy spin against a booting stack (thousands of calls within
    the readiness window instead of ~window/interval)."""
    import httpx

    import tests.integration.conftest as conftest

    calls = {"n": 0}

    def fake_get(url, **kwargs):
        calls["n"] += 1
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(conftest.httpx, "get", fake_get)

    start = time.monotonic()
    assert not _probe_readiness(
        "http://127.0.0.1:1", readiness_timeout_s=0.3, poll_interval_s=0.05
    )
    elapsed = time.monotonic() - start
    # With the sleep: ~6-7 calls in 0.3s. Without it: thousands.
    assert calls["n"] <= 20
    assert elapsed >= 0.3
