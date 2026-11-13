import os
import time
import uuid

import httpx
import pytest

PAPRA_TEST_URL = "http://127.0.0.1:12210"
API_KEY_PERMISSIONS = [
    "organizations:read",
    "documents:create",
    "documents:read",
    "documents:update",
    "tags:create",
    "tags:read",
]


def _probe_readiness(url: str, *, readiness_timeout_s: float,
                     poll_interval_s: float = 0.5) -> bool:
    # `docker compose up -d --wait` reports the container healthy as soon as it is
    # *running* — the papra image defines no healthcheck, so the app may still be
    # booting and refuse connections (or answer with a non-200/401 status, e.g.
    # 503 while booting) for a few seconds. Retry on ANY outcome — connection
    # error or HTTP status — within a bounded window; only after the full window
    # passes without a 200/401 do we report the stack as down.
    deadline = time.monotonic() + readiness_timeout_s
    while True:
        try:
            response = httpx.get(f"{url}/api/api-keys/current", timeout=2.0)
            if response.status_code in (200, 401):
                return True
        except httpx.TransportError:
            pass
        if time.monotonic() >= deadline:
            return False
        time.sleep(poll_interval_s)


def _skip_or_fail(reason: str) -> None:
    # `make test` exports PAPRA_REQUIRE_STACK=1: the stack it just brought up
    # MUST answer, so an expired readiness window is a hard failure, not a
    # silent skip that would let `make test` exit green without running the
    # round-trip. Without the flag (manual runs), keep skipping.
    if os.environ.get("PAPRA_REQUIRE_STACK") == "1":
        pytest.fail(f"{reason} (PAPRA_REQUIRE_STACK=1)")
    pytest.skip(reason)


def _papra_is_up(*, readiness_timeout_s: float = 0.0) -> bool:
    return _probe_readiness(PAPRA_TEST_URL, readiness_timeout_s=readiness_timeout_s)


GREENMAIL_HOST = "127.0.0.1"
GREENMAIL_SMTP_PORT = 3025
GREENMAIL_IMAP_PORT = 3143
GREENMAIL_USER = "ingest@localhost"
GREENMAIL_PASSWORD = "ingest-pass-123"


def _greenmail_is_up() -> bool:
    import socket

    try:
        with socket.create_connection((GREENMAIL_HOST, GREENMAIL_IMAP_PORT), timeout=2.0):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def greenmail():
    if not _greenmail_is_up():
        _skip_or_fail("scratch GreenMail not running; use `make test`")
    yield (GREENMAIL_HOST, GREENMAIL_IMAP_PORT, GREENMAIL_SMTP_PORT,
           GREENMAIL_USER, GREENMAIL_PASSWORD)


def purge_greenmail_inbox():
    """Empty the shared GreenMail INBOX so each test starts from a clean mailbox."""
    import imaplib

    # timeout: a stalled GreenMail must fail loudly, never wedge the suite
    imap = imaplib.IMAP4(GREENMAIL_HOST, GREENMAIL_IMAP_PORT, timeout=5)
    try:
        imap.login(GREENMAIL_USER, GREENMAIL_PASSWORD)
        imap.select("INBOX")
        imap.store("1:*", "+FLAGS", "\\Deleted")
        imap.expunge()
    finally:
        imap.logout()


def _wait_for_greenmail_delivery(message_id: str, *, timeout_s: float = 5.0) -> None:
    """Block until GreenMail has delivered the message to the IMAP INBOX.

    GreenMail's SMTP->IMAP delivery is asynchronous (~185ms measured): without
    this wait, tests race it (machine-dependent flakiness, cross-test
    pollution). Search for the injected Message-ID, retrying every 50ms up
    to a 5s window; raise on timeout so a broken GreenMail fails loudly.
    """
    import imaplib

    deadline = time.monotonic() + timeout_s
    while True:
        try:
            # per-attempt timeout: a stalled GreenMail must surface as an
            # OSError (retryable) or a TimeoutError at the window — never a
            # wedged test suite hanging on a socket read forever
            imap = imaplib.IMAP4(GREENMAIL_HOST, GREENMAIL_IMAP_PORT, timeout=5)
            try:
                imap.login(GREENMAIL_USER, GREENMAIL_PASSWORD)
                imap.select("INBOX")
                status, data = imap.uid(
                    "SEARCH", None, f'HEADER Message-ID "{message_id}"'
                )
                if status == "OK" and data and data[0] and data[0].split():
                    return
            finally:
                try:
                    imap.logout()
                except Exception:  # noqa: BLE001 - cleanup must not mask the result
                    pass
        except OSError:
            pass  # GreenMail briefly refused the connection: retry within the window
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"GreenMail did not deliver message {message_id} within {timeout_s}s"
            )
        time.sleep(0.05)


def inject_message(*, to, subject, attachments=()):
    """Send a MIME message with (filename, bytes, mime-tuple) attachments via
    GreenMail SMTP, then wait until it is visible in the IMAP INBOX."""
    import smtplib
    from email.message import EmailMessage

    message = EmailMessage()
    message["From"] = "sender@example.com"
    message["To"] = to
    message["Subject"] = subject
    message["Message-ID"] = f"<{uuid.uuid4().hex}@test>"
    for filename, content, mime in attachments:
        message.add_attachment(content, maintype=mime[0], subtype=mime[1], filename=filename)
    message_id = message["Message-ID"]
    with smtplib.SMTP(GREENMAIL_HOST, GREENMAIL_SMTP_PORT, timeout=10) as smtp:
        smtp.send_message(message)
    _wait_for_greenmail_delivery(message_id)


# Function-scoped on purpose: each integration test gets a fresh Papra
# user/org/api key. A session-scoped org leaks documents between tests (the
# M2 ingestion suite asserts on org-emptiness and Papra 409-dedups identical
# file content), so per-test isolation is required.
@pytest.fixture
def papra_stack():
    if not _papra_is_up(readiness_timeout_s=15.0):
        _skip_or_fail("scratch Papra not running; use `make test`")

    # unique per test so a reused container cannot collide
    suffix = uuid.uuid4().hex[:8]
    with httpx.Client(base_url=PAPRA_TEST_URL) as http:
        sign_up = http.post(
            "/api/auth/sign-up/email",
            json={
                "email": f"seed-{suffix}@papra-suite.test",
                "password": "seed-password-123456",
                "name": "Seed User",
            },
        )
        sign_up.raise_for_status()

        org = http.post("/api/organizations", json={"name": f"Test Org {suffix}"})
        org.raise_for_status()
        org_id = org.json()["organization"]["id"]

        api_key = http.post(
            "/api/api-keys",
            json={"name": f"suite-{suffix}", "permissions": API_KEY_PERMISSIONS},
        )
        api_key.raise_for_status()
        token = api_key.json()["token"]

    yield PAPRA_TEST_URL, org_id, token
