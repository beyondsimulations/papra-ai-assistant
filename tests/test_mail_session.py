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
)


def make_session(ssl_mode="plain"):
    return MailboxSession(
        host="imap.test", port=143, username="u", password="p", ssl_mode=ssl_mode
    )


class FakeImap:
    """Minimal imaplib double: canned SEARCH result, per-uid FETCH responses."""

    def __init__(self, search_uids=(1, 2, 3), bodies=None, fetch_responses=None,
                 sizes=None, size_responses=None, capabilities=("IMAP4REV1",),
                 deleted_uids=(), no_store=False, no_uid_expunge=False,
                 no_search_deleted=False, no_expunge=False):
        self.search_uids = list(search_uids)
        self.bodies = bodies or {}                    # uid -> raw bytes (well-formed)
        self.fetch_responses = fetch_responses or {}  # uid -> raw body-fetch_data list
        self.sizes = sizes or {}                       # uid -> explicit RFC822.SIZE
        self.size_responses = size_responses or {}    # uid -> raw size-fetch_data list
        self.size_probes = []   # uids probed with (RFC822.SIZE)
        self.fetch_calls = []    # uids whose BODY.PEEK[] was actually fetched
        self.capabilities = tuple(capabilities)   # imaplib exposes these at connect
        self.deleted_uids = set(deleted_uids)     # what UID SEARCH DELETED reports
        self.store_calls = []        # (uid, mode, flags) STOREs issued
        self.uid_expunge_calls = []  # uid-sets passed to UID EXPUNGE
        self.expunge_calls = 0       # plain (mailbox-wide) EXPUNGEs issued
        # M2 carry-over hardening: any of these makes the corresponding
        # tagged response come back as ("NO", [None])
        self.no_store = no_store
        self.no_uid_expunge = no_uid_expunge
        self.no_search_deleted = no_search_deleted
        self.no_expunge = no_expunge

    def expunge(self):
        self.expunge_calls += 1
        if self.no_expunge:
            return ("NO", [None])
        return ("OK", [None])

    def uid(self, command, *args):
        if command == "SEARCH":
            if "DELETED" in args:
                if self.no_search_deleted:
                    return ("NO", [None])
                payload = " ".join(str(u) for u in sorted(self.deleted_uids)).encode()
                return ("OK", [payload])
            payload = " ".join(str(u) for u in self.search_uids).encode()
            return ("OK", [payload])
        if command == "STORE":
            self.store_calls.append((int(args[0]), args[1], args[2]))
            if self.no_store:
                return ("NO", [None])
            return ("OK", [b"Store completed"])
        if command == "EXPUNGE":
            self.uid_expunge_calls.append(args[0])
            if self.no_uid_expunge:
                return ("NO", [None])
            return ("OK", [None])
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
    # message is perpetually re-fetched. The boundary uid == last_seen must be
    # dropped too (strictly greater): it was already fetched last poll, so a
    # >= filter would re-fetch it forever on RFC-compliant servers.
    session = make_session()
    session._imap = FakeImap(
        search_uids=(1, 2, 5, 7), bodies={5: b"raw-5", 7: b"raw-7"}
    )

    batch = session.fetch_since(5)

    assert [uid for uid, _raw in batch.messages] == [7]


def test_fetch_since_skips_size_probe_without_size_pattern():
    # the size probe can also answer with a bare bytes status line that has
    # NO RFC822.SIZE at all (message vanished; server reports FLAGS only):
    # the no-match arm must return None -> the uid is skipped, never an
    # AttributeError from a guardless int(match.group(1))
    session = make_session()
    good = b"Subject: ok\r\n\r\nbody"
    session._imap = FakeImap(
        search_uids=(1, 2),
        bodies={2: good},
        size_responses={1: [b"1 (FLAGS (\\Deleted) UID 1)"]},  # no size pattern
    )

    batch = session.fetch_since(None)

    assert batch.messages == [(2, good)]
    assert batch.oversized_uids == []
    assert session._imap.size_probes == [1, 2]
    assert session._imap.fetch_calls == [2]   # uid 1 is never fetched


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


def test_delete_and_expunge_uses_uid_expunge_with_uidplus():
    # UIDPLUS advertised: RFC 4315 UID EXPUNGE removes ONLY our uids — an
    # operator's flagged-but-unexpunged message in the same mailbox survives
    session = make_session()
    session._imap = FakeImap(
        capabilities=("IMAP4REV1", "UIDPLUS"), deleted_uids={2, 7},
    )

    session.delete_and_expunge([2])

    assert session._imap.store_calls == [(2, "+FLAGS", "(\\Deleted)")]
    assert session._imap.uid_expunge_calls == ["2"]   # scoped to our uid
    assert session._imap.expunge_calls == 0            # no mailbox-wide EXPUNGE


def test_delete_and_expunge_plain_expunge_when_deleted_set_exactly_ours():
    # no UIDPLUS: plain EXPUNGE destroys every \Deleted message, so it is
    # only safe when the mailbox's deleted set is EXACTLY the uids we flagged
    session = make_session()
    session._imap = FakeImap(deleted_uids={3, 4})

    session.delete_and_expunge([3, 4])

    assert session._imap.store_calls == [
        (3, "+FLAGS", "(\\Deleted)"), (4, "+FLAGS", "(\\Deleted)"),
    ]
    assert session._imap.expunge_calls == 1            # safe: the set is exactly ours
    assert session._imap.uid_expunge_calls == []


def test_delete_and_expunge_skips_expunge_on_foreign_deleted_mail(caplog):
    # no UIDPLUS + a foreign \Deleted message (the operator flagged it in
    # their own client without expunging): a plain EXPUNGE would permanently
    # destroy it — the poll must SKIP the expunge and log a warning instead;
    # our \Deleted flags stay (harmless, deduped on the next poll)
    session = make_session()
    session._imap = FakeImap(deleted_uids={2, 5})   # 5 is the operator's

    with caplog.at_level("WARNING", logger="papra_suite.mail"):
        session.delete_and_expunge([2])

    assert session._imap.store_calls == [(2, "+FLAGS", "(\\Deleted)")]
    assert session._imap.expunge_calls == 0           # nothing destroyed
    assert session._imap.uid_expunge_calls == []
    # NOTE: asserted on a p-free fragment — the redaction filter replaces
    # every registered secret substring in log records, and earlier unit
    # fixtures register "p" as a mailbox password secret
    assert any("foreign \\Deleted" in record.message for record in caplog.records)


def test_select_inbox_parses_uidvalidity_from_both_response_shapes():
    # after SELECT, imaplib's response("UIDVALIDITY") data is the prefixed
    # shape [b'UIDVALIDITY <n>'] on most servers, but some return the bare
    # [b'<n>']. Splitting off the LAST token handles both; a [0] mutant would
    # turn the prefixed shape into the constant "UIDVALIDITY" — the rescan
    # trigger would never fire and mail after a reissued-lower-UID UIDVALIDITY
    # would silently never be fetched.
    class _SelectImap:
        def __init__(self, uidvalidity_data):
            self.select_calls = []
            self._uidvalidity_data = uidvalidity_data

        def select(self, mailbox):
            self.select_calls.append(mailbox)
            return ("OK", [b"1"])

        def response(self, key):
            assert key == "UIDVALIDITY"
            return ("OK", self._uidvalidity_data)

    session = make_session()

    session._imap = _SelectImap([b"UIDVALIDITY 12345"])
    assert session.select_inbox() == "12345"
    assert session._imap.select_calls == ["INBOX"]

    session._imap = _SelectImap([b"12345"])
    assert session.select_inbox() == "12345"


def test_close_swallows_logout_failure_and_is_idempotent():
    # a server RST before LOGOUT must never propagate out of the poller's
    # finally block: close() swallows the error AND still clears _imap, so a
    # second close() is a no-op (and _record_success_quietly still runs,
    # persisting last_seen on successful polls).
    class _RstImap:
        def __init__(self):
            self.logout_calls = 0

        def logout(self):
            self.logout_calls += 1
            raise imaplib.IMAP4.error("connection reset by peer")

    session = make_session()
    rude = _RstImap()
    session._imap = rude

    session.close()  # must not raise

    assert rude.logout_calls == 1
    assert session._imap is None

    session.close()  # idempotent: no imap, no second logout, no raise
    assert rude.logout_calls == 1
def test_mark_seen_raises_on_store_no():
    # M2 carry-over: an unchecked ("NO", ...) STORE in mark_seen would mean
    # the \Seen flags were never set — the message would be re-fetched every
    # poll with no error surfaced. The NO must raise into the poller's
    # never-raises guard (failure accounting engages).
    session = make_session()
    session._imap = FakeImap(no_store=True)

    with pytest.raises(imaplib.IMAP4.error, match="STORE"):
        session.mark_seen([1])


def test_delete_and_expunge_raises_on_store_no():
    session = make_session()
    session._imap = FakeImap(capabilities=("IMAP4REV1", "UIDPLUS"), no_store=True)

    with pytest.raises(imaplib.IMAP4.error, match="STORE"):
        session.delete_and_expunge([2])

    assert session._imap.uid_expunge_calls == []  # the expunge never runs


def test_delete_and_expunge_raises_on_uid_expunge_no():
    # the \Deleted flags ARE set, but a failed UID EXPUNGE must not be
    # mistaken for success: the messages stay flagged (harmless) and the
    # error flows into the poller's failure accounting
    session = make_session()
    session._imap = FakeImap(
        capabilities=("IMAP4REV1", "UIDPLUS"), no_uid_expunge=True
    )

    with pytest.raises(imaplib.IMAP4.error, match="EXPUNGE"):
        session.delete_and_expunge([2])

    assert session._imap.store_calls == [(2, "+FLAGS", "(\\Deleted)")]


def test_delete_and_expunge_raises_on_search_deleted_no():
    # no UIDPLUS: the plain-EXPUNGE safety check needs UID SEARCH DELETED;
    # a NO there means we cannot know whether foreign mail would be
    # destroyed — raise rather than guess (fail closed)
    session = make_session()
    session._imap = FakeImap(no_search_deleted=True)

    with pytest.raises(imaplib.IMAP4.error, match="SEARCH"):
        session.delete_and_expunge([3, 4])

    assert session._imap.expunge_calls == 0  # nothing destroyed


def test_delete_and_expunge_raises_on_plain_expunge_no():
    session = make_session()
    session._imap = FakeImap(deleted_uids={3, 4}, no_expunge=True)

    with pytest.raises(imaplib.IMAP4.error, match="EXPUNGE"):
        session.delete_and_expunge([3, 4])


def test_select_inbox_raises_on_select_no():
    # a failed SELECT (mailbox missing, permissions) must not fall through
    # into fetching against a stale/unselected mailbox state
    class _NoSelectImap:
        def select(self, mailbox):
            return ("NO", [None])

        def response(self, key):
            return ("OK", [b"UIDVALIDITY 1"])

    session = make_session()
    session._imap = _NoSelectImap()

    with pytest.raises(imaplib.IMAP4.error, match="select"):
        session.select_inbox()
