"""Unit tests for MailboxSession's IMAP-level contract (fake imap objects)."""

import imaplib

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

    def __init__(self, search_uids=(1, 2, 3), bodies=None, fetch_responses=None):
        self.search_uids = list(search_uids)
        self.bodies = bodies or {}                    # uid -> raw bytes (well-formed)
        self.fetch_responses = fetch_responses or {}  # uid -> raw fetch_data list
        self.fetch_calls = []

    def uid(self, command, *args):
        if command == "SEARCH":
            payload = " ".join(str(u) for u in self.search_uids).encode()
            return ("OK", [payload])
        uid = int(args[0])
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
    big = b"x" * (MAX_MESSAGE_BYTES + 1)
    session._imap = FakeImap(search_uids=(1,), bodies={1: big})

    batch = session.fetch_since(None)

    assert batch.messages == []
    assert batch.oversized_uids == [1]   # still counts: the poller advances past it


def test_fetch_since_stops_at_aggregate_cap():
    session = make_session()
    big = b"x" * (30 * 1024 * 1024)      # 30MB each: five would be 150MB
    session._imap = FakeImap(
        search_uids=(1, 2, 3, 4, 5), bodies={uid: big for uid in (1, 2, 3, 4, 5)}
    )

    batch = session.fetch_since(None)

    # fetching stops once accumulated bytes exceed MAX_POLL_BYTES (4 x 30MB):
    # the returned batch is bounded and uid 5 is never fetched
    assert batch.messages == []
    assert batch.oversized_uids == [1, 2, 3, 4]
    assert session._imap.fetch_calls == [1, 2, 3, 4]
    assert 4 * 30 * 1024 * 1024 > MAX_POLL_BYTES


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
