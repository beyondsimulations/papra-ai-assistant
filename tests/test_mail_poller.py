from dataclasses import dataclass, field

import pytest

from papra_suite.mail import poll_mailbox_once
from papra_suite.store import Mailbox, Org, Store


def make_mailbox(**overrides):
    base = dict(
        id="mb1", org_id="org_local", host="imap.test", port=993, username="u",
        password="box-secret-pw", ssl_mode="ssl", poll_interval_s=300,
        subject_filter=None, delete_after_download=False, enabled=True,
        last_seen_uid=None, uidvalidity=None, last_poll_at=None, last_error=None,
        consecutive_failures=0,
    )
    base.update(overrides)
    return Mailbox(**base)


ORG = Org(id="org_local", papra_org_id="org_papra", name="Test")


def build_raw(subject="Invoice", message_id="<m1@x>", attachments=(("a.pdf", b"%PDF-1"),)):
    from tests.test_mail_parsing import build_raw as _build
    return _build(subject=subject, message_id=message_id, attachments=[
        (name, content, ("application", "pdf")) for name, content in attachments
    ])


@dataclass
class FakeSession:
    uidvalidity: str = "111"
    raws: list = field(default_factory=list)     # [(uid, raw)]
    seen: list = field(default_factory=list)
    deleted: list = field(default_factory=list)
    fail_on_connect: Exception | None = None
    closed: bool = False

    def connect(self):
        if self.fail_on_connect:
            raise self.fail_on_connect

    def select_inbox(self):
        return self.uidvalidity

    def fetch_since(self, last_seen_uid):
        if last_seen_uid is None:
            return list(self.raws)
        return [(uid, raw) for uid, raw in self.raws if uid > last_seen_uid]

    def mark_seen(self, uids):
        self.seen.extend(uids)

    def delete_and_expunge(self, uids):
        self.deleted.extend(uids)

    def close(self):
        self.closed = True


class StubClient:
    def __init__(self, fail_for=()):
        self.fail_for = set(fail_for)   # filenames that should fail
        self.uploads = []

    async def create_document(self, org_id, filename, content):
        if filename in self.fail_for:
            from papra_suite.papra_client import PapraError
            raise PapraError(f"upload failed for {filename}", 500)
        document = {"id": f"doc_{len(self.uploads)}", "name": filename}
        self.uploads.append((org_id, filename, content))
        return document


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


async def test_poll_uploads_pdf_and_enqueues_ocr(store, monkeypatch):
    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(store, client, make_mailbox(), ORG)

    assert result.documents_uploaded == 1 and result.error is None
    assert client.uploads == [("org_papra", "a.pdf", b"%PDF-1")]
    job = store.claim_next_job("ocr")
    assert job.document_id == "doc_0" and job.content_hash == \
        __import__("papra_suite.mail", fromlist=["compute_file_hash"]).compute_file_hash(b"%PDF-1")
    assert store.document_seen("org_papra", "doc_0")
    # mb1 is an in-memory fixture (not persisted); poller state is asserted
    # via the result and the session, and via the persisted-state tests below
    assert session.seen == [1]
    assert session.deleted == []


async def test_poll_skips_non_pdf_and_marks_seen(store, monkeypatch, caplog):
    raw = build_raw(attachments=[("photo.jpg", b"\xff\xd8jpeg")])
    session = FakeSession(raws=[(1, raw)])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(store, StubClient(), make_mailbox(), ORG)

    assert result.documents_uploaded == 0
    assert result.attachments_skipped == 1
    assert session.seen == [1]              # handled, read
    assert session.deleted == []           # never deleted (not processed)


async def test_poll_respects_subject_filter(store, monkeypatch):
    session = FakeSession(raws=[
        (1, build_raw(subject="Spam")),
        (2, build_raw(subject="Rechnung 42", message_id="<m2@x>")),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(
        store, client, make_mailbox(subject_filter=".*rechnung.*"), ORG
    )

    assert result.documents_uploaded == 1
    assert client.uploads[0][1] == "a.pdf"
    assert session.seen == [1, 2]          # both handled (one filtered, one uploaded)


async def test_poll_dedup_prevents_double_upload(store, monkeypatch):
    raw = build_raw()
    session = FakeSession(raws=[(1, raw)])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    await poll_mailbox_once(store, client, make_mailbox(), ORG)
    # same message again (e.g. failed upload of a LATER message kept it fetchable)
    result = await poll_mailbox_once(store, client, make_mailbox(), ORG)

    assert result.documents_uploaded == 0
    assert len(client.uploads) == 1


async def test_poll_upload_failure_does_not_advance_last_seen(store, monkeypatch):
    session = FakeSession(raws=[
        (1, build_raw(message_id="<m1@x>", attachments=[("bad.pdf", b"%PDF-1")])),
        (2, build_raw(message_id="<m2@x>", attachments=[("good.pdf", b"%PDF-2")])),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(fail_for={"bad.pdf"})

    result = await poll_mailbox_once(store, client, make_mailbox(), ORG)

    assert result.documents_uploaded == 1 and result.messages_failed == 1
    assert session.seen == [2]             # only the successful message is read
    # mailbox state: failure recorded (upload failures count toward error state)
    # NOTE: mb1 is not persisted; the poller records failure only for stored
    # mailboxes — assert the result instead
    assert result.error is None            # per-message failure is not a poll error


async def test_poll_delete_after_download(store, monkeypatch):
    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    await poll_mailbox_once(store, StubClient(), make_mailbox(delete_after_download=True), ORG)

    assert session.seen == [1] and session.deleted == [1]


async def test_poll_delete_not_triggered_on_upload_failure(store, monkeypatch):
    session = FakeSession(raws=[(1, build_raw(attachments=[("bad.pdf", b"%PDF-1")]))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(fail_for={"bad.pdf"})

    await poll_mailbox_once(store, client, make_mailbox(delete_after_download=True), ORG)

    assert session.deleted == []           # stays in the mailbox for the next poll
    assert session.seen == []


async def test_poll_uidvalidity_change_rescans(store, monkeypatch):
    session = FakeSession(uidvalidity="222", raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    # mailbox claims uid 5 already seen under the OLD validity 111
    result = await poll_mailbox_once(
        store, StubClient(), make_mailbox(last_seen_uid=5, uidvalidity="111"), ORG
    )

    assert result.documents_uploaded == 1   # uid 1 re-fetched despite last_seen 5


async def test_poll_connection_failure_records_error_not_crash(store, monkeypatch):
    session = FakeSession(fail_on_connect=ConnectionRefusedError("refused"))
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(store, StubClient(), make_mailbox(), ORG)

    assert result.error is not None and "refused" in result.error
    assert result.documents_uploaded == 0


async def test_poll_registers_mailbox_password_for_redaction(store, monkeypatch, capsys):
    from papra_suite.logging_setup import setup_logging

    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    import logging
    setup_logging(store)
    await poll_mailbox_once(store, StubClient(), make_mailbox(password="box-secret-pw"), ORG)
    logging.getLogger("test").info("connection used password box-secret-pw")
    line = capsys.readouterr().out.strip().splitlines()[-1]
    assert "box-secret-pw" not in line


async def test_poll_updates_persisted_mailbox_state(store, monkeypatch):
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    session = FakeSession(raws=[(7, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    await poll_mailbox_once(store, StubClient(), mailbox, org)

    updated = store.get_mailbox(mailbox_id)
    assert updated.last_seen_uid == 7
    assert updated.uidvalidity == "111"
    assert updated.last_error is None
    assert updated.consecutive_failures == 0
    assert updated.enabled is True


async def test_upload_failure_freezes_last_seen_prefix(store, monkeypatch):
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    session = FakeSession(raws=[
        (1, build_raw(message_id="<m1@x>", attachments=[("bad.pdf", b"%PDF-1")])),
        (2, build_raw(message_id="<m2@x>", attachments=[("good.pdf", b"%PDF-2")])),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(fail_for={"bad.pdf"})

    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    result = await poll_mailbox_once(store, client, mailbox, org)

    # uid 1 failed: last_seen must not advance past it (uid 2's success must
    # not strand uid 1) — the empty-poll default 0 keeps uid 1 re-fetchable
    assert store.get_mailbox(mailbox_id).last_seen_uid == 0
    assert result.documents_uploaded == 1

    # a follow-up poll re-fetches uid 1 (and retries its upload)
    second = await poll_mailbox_once(
        store, client, store.get_mailbox(mailbox_id), org
    )
    assert second.messages_scanned == 2
    assert second.messages_failed == 1
    assert second.documents_uploaded == 0  # good.pdf dedup-skipped


async def test_rescan_failure_persists_zero_last_seen(store, monkeypatch):
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    store.record_poll_success(
        mailbox_id, last_seen_uid=5, uidvalidity="111"
    )
    session = FakeSession(
        uidvalidity="222",
        raws=[(1, build_raw(message_id="<m1@x>", attachments=[("bad.pdf", b"%PDF-1")]))],
    )
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(fail_for={"bad.pdf"})

    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    await poll_mailbox_once(store, client, mailbox, org)

    # rescan saw nothing succeed: stale pre-rescan uid 5 must NOT be
    # restored — persist 0 so the failed message stays re-fetchable
    updated = store.get_mailbox(mailbox_id)
    assert updated.last_seen_uid == 0
    assert updated.uidvalidity == "222"


async def test_poll_never_raises_on_internal_error(store, monkeypatch):
    def _boom(raw):
        raise RuntimeError("boom")

    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    monkeypatch.setattr("papra_suite.mail.parse_message", _boom)

    result = await poll_mailbox_once(store, StubClient(), make_mailbox(), ORG)

    assert result.error is not None and "boom" in result.error
    assert session.closed is True


async def test_poll_connection_failure_disables_after_three(store, monkeypatch):
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    mailbox = store.get_mailbox(mailbox_id)
    org = store.get_org(org_id)

    for _attempt in range(3):
        session = FakeSession(fail_on_connect=ConnectionRefusedError("refused"))
        monkeypatch.setattr(
            "papra_suite.mail.MailboxSession", lambda _s=session, **kw: _s
        )
        result = await poll_mailbox_once(store, StubClient(), mailbox, org)

    assert result.error is not None
    updated = store.get_mailbox(mailbox_id)
    assert updated.enabled is False
    assert updated.consecutive_failures == 3
    assert "refused" in updated.last_error
