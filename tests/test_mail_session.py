"""Unit tests for MailboxSession's IMAP-level contract (fake imap objects)."""

import imaplib
import socket
import ssl
import subprocess
import threading

import pytest

from papra_suite.mail import (
    MAX_MESSAGE_BYTES,
    MAX_POLL_BYTES,
    MailboxSession,
    subject_matches,
)


def make_session(ssl_mode="plain"):
    return MailboxSession(
        host="imap.test", port=143, username="u", password="p", ssl_mode=ssl_mode
    )


class FakeImap:
    """Minimal imaplib double: canned SEARCH result, per-uid FETCH responses."""

    def __init__(self, search_uids=(1, 2, 3), bodies=None, fetch_responses=None,
                 sizes=None, size_responses=None):
        self.search_uids = list(search_uids)
        self.bodies = bodies or {}                    # uid -> raw bytes (well-formed)
        self.fetch_responses = fetch_responses or {}  # uid -> raw body-fetch_data list
        self.sizes = sizes or {}                       # uid -> explicit RFC822.SIZE
        self.size_responses = size_responses or {}    # uid -> raw size-fetch_data list
        self.size_probes = []   # uids probed with (RFC822.SIZE)
        self.fetch_calls = []    # uids whose BODY.PEEK[] was actually fetched

    def uid(self, command, *args):
        if command == "SEARCH":
            payload = " ".join(str(u) for u in self.search_uids).encode()
            return ("OK", [payload])
        uid = int(args[0])
        if args[1] == "(RFC822.SIZE)":
            self.size_probes.append(uid)
            if uid in self.size_responses:
                return ("OK", self.size_responses[uid])
            size = self.sizes.get(uid, len(self.bodies.get(uid, b"")))
            return ("OK", [f"{uid} (RFC822.SIZE {size} UID {uid})".encode()])
        self.fetch_calls.append(uid)
        if uid in self.fetch_responses:
            return ("OK", self.fetch_responses[uid])
        raw = self.bodies[uid]
        return ("OK", [(f"{uid} (UID {uid} BODY[])".encode(), raw)])


def test_fetch_since_skips_vanished_messages():
    # a message expunged between SEARCH and FETCH yields a bare bytes status
    # line (fetch_data[0] is bytes -> int on blind indexing) or [None]
    # (TypeError on blind indexing): both must be skipped, not crash the poll
    session = make_session()
    good = b"Subject: ok\r\n\r\nbody"
    session._imap = FakeImap(
        search_uids=(1, 2, 3),
        fetch_responses={
            1: [b"1 (FLAGS (\\Deleted) UID 1)"],
            2: [None],
            3: [(b"3 (UID 3 BODY[])", good)],
        },
    )

    batch = session.fetch_since(None)

    assert batch.messages == [(3, good)]
    assert batch.oversized_uids == []


def test_fetch_since_skips_vanished_size_probe():
    # the RFC822.SIZE probe can vanish too (message expunged between SEARCH
    # and the metadata fetch): [None] or a bare status line must skip the
    # message without a TypeError — the guard must survive any vanish shape
    session = make_session()
    good = b"Subject: ok\r\n\r\nbody"
    session._imap = FakeImap(
        search_uids=(1, 2),
        bodies={2: good},
        size_responses={1: [None]},   # vanished between SEARCH and size probe
    )

    batch = session.fetch_since(None)

    assert batch.messages == [(2, good)]
    assert batch.oversized_uids == []
    assert session._imap.size_probes == [1, 2]
    assert session._imap.fetch_calls == [2]   # uid 1 is never fetched


def test_fetch_since_filters_uid_edge():
    # RFC 3501: "UID n:*" must include the mailbox's last message even when
    # its uid is below n — the client-side filter must drop it or the last
    # message is perpetually re-fetched
    session = make_session()
    session._imap = FakeImap(search_uids=(1, 2, 7), bodies={7: b"raw-7"})

    batch = session.fetch_since(5)

    assert [uid for uid, _raw in batch.messages] == [7]


def test_fetch_since_skips_oversized_message():
    session = make_session()
    # size probe reports an oversized message; no body is ever stored (a body
    # fetch would KeyError), so the skip must happen on metadata alone
    session._imap = FakeImap(
        search_uids=(1,), sizes={1: MAX_MESSAGE_BYTES + 1}
    )

    batch = session.fetch_since(None)

    assert batch.messages == []
    assert batch.oversized_uids == [1]   # still counts: the poller advances past it
    assert session._imap.size_probes == [1]
    assert session._imap.fetch_calls == []  # the BODY fetch is never issued


def test_fetch_since_stops_at_aggregate_cap():
    session = make_session()
    big = 30 * 1024 * 1024               # 30MB each: five would be 150MB
    session._imap = FakeImap(
        search_uids=(1, 2, 3, 4, 5), sizes={uid: big for uid in (1, 2, 3, 4, 5)}
    )

    batch = session.fetch_since(None)

    # fetching stops once accumulated bytes exceed MAX_POLL_BYTES (4 x 30MB):
    # the returned batch is bounded and uid 5 is never even probed
    assert batch.messages == []
    assert batch.oversized_uids == [1, 2, 3, 4]
    assert session._imap.size_probes == [1, 2, 3, 4]
    assert session._imap.fetch_calls == []   # all four are oversized: no body reads
    assert 4 * 30 * 1024 * 1024 > MAX_POLL_BYTES


def test_fetch_since_fetches_message_at_exact_per_message_cap():
    # boundary: a message of EXACTLY MAX_MESSAGE_BYTES is within the cap and
    # must be FETCHED — only strictly larger messages are skipped as oversized
    session = make_session()
    raw = b"Subject: ok\r\n\r\nbody"
    session._imap = FakeImap(
        search_uids=(1,), bodies={1: raw}, sizes={1: MAX_MESSAGE_BYTES}
    )

    batch = session.fetch_since(None)

    assert batch.messages == [(1, raw)]
    assert batch.oversized_uids == []
    assert session._imap.fetch_calls == [1]


def test_fetch_since_aggregate_cap_boundary_and_break(monkeypatch):
    # boundary: accumulated bytes EXACTLY at the cap do NOT stop the poll —
    # only strictly exceeding does
    monkeypatch.setattr("papra_suite.mail.MAX_POLL_BYTES", 1000)
    raw = b"Subject: ok\r\n\r\nbody"
    session = make_session()
    session._imap = FakeImap(
        search_uids=(1, 2), bodies={1: raw, 2: raw}, sizes={1: 1000, 2: 1000}
    )

    batch = session.fetch_since(None)

    assert [uid for uid, _body in batch.messages] == [1, 2]

    # cap-break with FETCHABLE messages: once accumulated bytes exceed the
    # cap the batch stops; the remaining uids stay unfetched for the next poll
    session = make_session()
    session._imap = FakeImap(
        search_uids=(1, 2, 3),
        bodies={uid: raw for uid in (1, 2, 3)},
        sizes={uid: 600 for uid in (1, 2, 3)},
    )

    batch = session.fetch_since(None)

    assert [uid for uid, _body in batch.messages] == [1, 2]
    assert session._imap.size_probes == [1, 2]   # uid 3 is never even probed
    assert session._imap.fetch_calls == [1, 2]


def test_connect_ssl_rejects_self_signed_certificate(tmp_path):
    # imaplib's implicit TLS context verifies NOTHING (CERT_NONE, no hostname
    # check): without the explicit verified context, an on-path attacker with
    # any self-signed certificate captures the mailbox LOGIN credentials.
    # The handshake against a scratch self-signed listener must fail closed.
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", str(key), "-out", str(cert), "-days", "1",
         "-subj", "/CN=127.0.0.1"],
        check=True, capture_output=True,
    )
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(cert, key)

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def _serve():
        # cooperative IMAP double: with verification the handshake never
        # completes; against an unverified (mutant) client it answers the
        # greeting and the tagged LOGIN so the failure is unambiguous
        try:
            conn, _addr = listener.accept()
            with server_context.wrap_socket(conn, server_side=True) as tls:
                tls.sendall(b"* OK scratch imap ready\r\n")
                data = tls.recv(4096)
                tls.sendall(data.split()[0] + b" OK LOGIN completed\r\n")
                tls.recv(4096)
        except OSError:
            pass  # client aborted the handshake (the expected path)

    threading.Thread(target=_serve, daemon=True).start()

    session = MailboxSession(
        host="127.0.0.1", port=port, username="u", password="p", ssl_mode="ssl"
    )
    with pytest.raises(ssl.SSLCertVerificationError):
        session.connect()


def test_connect_passes_socket_timeout_and_context(monkeypatch):
    # imaplib constructors have no default timeout: a stalled server would
    # wedge the poll task forever with no failure accounting. Every mode must
    # pass an explicit timeout, and both TLS modes must pass the verified
    # module-level ssl context.
    captured = {"ssl": None, "plain": [], "starttls": None}

    class _FakeImap:
        def starttls(self, **kwargs):
            captured["starttls"] = kwargs

        def login(self, username, password):
            pass

    def _fake_ssl_ctor(host, port, **kwargs):
        captured["ssl"] = kwargs
        return _FakeImap()

    def _fake_plain_ctor(host, port, **kwargs):
        captured["plain"].append(kwargs)
        return _FakeImap()

    monkeypatch.setattr(imaplib, "IMAP4_SSL", _fake_ssl_ctor)
    monkeypatch.setattr(imaplib, "IMAP4", _fake_plain_ctor)

    make_session(ssl_mode="ssl").connect()
    make_session(ssl_mode="starttls").connect()
    make_session(ssl_mode="plain").connect()

    assert captured["ssl"].get("timeout") == 30
    assert captured["ssl"].get("ssl_context") is not None
    assert captured["starttls"].get("ssl_context") is not None
    assert len(captured["plain"]) == 2   # starttls + plain both via IMAP4
    assert all(kwargs.get("timeout") == 30 for kwargs in captured["plain"])


def test_connect_unknown_ssl_mode_fails_closed(monkeypatch):
    def _no_socket(*args, **kwargs):
        raise AssertionError("socket must not be used for an unknown ssl_mode")

    monkeypatch.setattr(imaplib, "IMAP4_SSL", _no_socket)
    monkeypatch.setattr(imaplib, "IMAP4", _no_socket)
    session = make_session(ssl_mode="TLS")  # legacy/hand-edited row value

    with pytest.raises(ValueError, match="ssl_mode"):
        session.connect()


def test_subject_matches_truncates_beyond_cap():
    # the match lives beyond char 1000: the cap must hide it (bounds the
    # input for the super-linear regex backtracking class)
    assert subject_matches("x" * 1000 + "needle", "needle") is False
    # a match within the first 1000 chars still works on a long subject
    assert subject_matches("needle" + "x" * 2000, "needle") is True
