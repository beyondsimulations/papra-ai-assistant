import asyncio
import imaplib
import re
import time
from dataclasses import dataclass, field

import pytest

from papra_suite import mail, safematch
from papra_suite.mail import FetchBatch, PollResult, poll_mailbox_once
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
    oversized: list = field(default_factory=list)  # uids above the per-message cap
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
        messages = self.raws if last_seen_uid is None \
            else [(uid, raw) for uid, raw in self.raws if uid > last_seen_uid]
        oversized = self.oversized if last_seen_uid is None \
            else [uid for uid in self.oversized if uid > last_seen_uid]
        return FetchBatch(messages=list(messages), oversized_uids=list(oversized))

    def mark_seen(self, uids):
        self.seen.extend(uids)

    def delete_and_expunge(self, uids):
        self.deleted.extend(uids)

    def close(self):
        self.closed = True


class StubClient:
    def __init__(self, fail_for=(), conflict_for=()):
        self.fail_for = set(fail_for)   # filenames that should fail
        self.conflict_for = set(conflict_for)  # filenames papra already has (409)
        self.uploads = []

    async def create_document(self, org_id, filename, content):
        if filename in self.fail_for:
            from papra_suite.papra_client import PapraError
            raise PapraError(f"upload failed for {filename}", 500)
        if filename in self.conflict_for:
            from papra_suite.papra_client import PapraError
            raise PapraError("document.already_exists", 409)
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


async def test_poll_parses_messages_off_the_event_loop(store, monkeypatch):
    # parse_message on a crafted 22MB multipart measured a 238ms block of the
    # event loop: the parse must go through asyncio.to_thread like every other
    # blocking call in the poller, never run inline on the loop
    real_to_thread = asyncio.to_thread
    threaded_calls = []

    async def _spy_to_thread(func, *args, **kwargs):
        threaded_calls.append(getattr(func, "__name__", str(func)))
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", _spy_to_thread)
    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(store, StubClient(), make_mailbox(), ORG)

    assert result.error is None and result.documents_uploaded == 1
    assert "parse_message" in threaded_calls


async def test_empty_non_rescan_poll_carries_last_seen_forward(store, monkeypatch):
    # an empty fetch on an established (non-rescan) mailbox must carry the
    # persisted last_seen forward — NOT reset it to 0, which would re-fetch
    # the whole mailbox on every empty poll
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    store.record_poll_success(mailbox_id, last_seen_uid=42, uidvalidity="111")

    session = FakeSession(uidvalidity="111")   # empty fetch: no raws, no oversized
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    assert mailbox.last_seen_uid == 42

    result = await poll_mailbox_once(store, StubClient(), mailbox, org)

    assert result.error is None and result.messages_scanned == 0
    assert store.get_mailbox(mailbox_id).last_seen_uid == 42


async def test_poll_resumes_after_cap_truncated_batch(store, monkeypatch):
    # poller-level aggregate-cap interaction: a fetch truncated at the cap
    # (3 of 5 uids) must persist the last PROCESSED uid; a second poll fetches
    # the tail — no message is lost or double-processed
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )

    class TruncatingSession(FakeSession):
        def fetch_since(self, last_seen_uid):
            batch = super().fetch_since(last_seen_uid)
            batch.messages = batch.messages[:3]   # simulates the aggregate break
            return batch

    session = TruncatingSession(raws=[
        (1, build_raw(message_id="<m1@x>")),
        (2, build_raw(message_id="<m2@x>", attachments=[("b.pdf", b"%PDF-2")])),
        (3, build_raw(message_id="<m3@x>", attachments=[("c.pdf", b"%PDF-3")])),
        (4, build_raw(message_id="<m4@x>", attachments=[("d.pdf", b"%PDF-4")])),
        (5, build_raw(message_id="<m5@x>", attachments=[("e.pdf", b"%PDF-5")])),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    result = await poll_mailbox_once(store, client, mailbox, org)

    assert result.documents_uploaded == 3
    assert store.get_mailbox(mailbox_id).last_seen_uid == 3

    second = await poll_mailbox_once(store, client, store.get_mailbox(mailbox_id), org)

    assert second.documents_uploaded == 2
    assert len(client.uploads) == 5
    assert store.get_mailbox(mailbox_id).last_seen_uid == 5


async def test_poll_never_raises_on_internal_error(store, monkeypatch):
    def _boom(raw):
        raise RuntimeError("boom")

    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    monkeypatch.setattr("papra_suite.mail.parse_message", _boom)

    result = await poll_mailbox_once(store, StubClient(), make_mailbox(), ORG)

    assert result.error is not None and "boom" in result.error
    assert session.closed is True


async def test_upload_409_counts_as_already_ingested(store, monkeypatch):
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    session = FakeSession(raws=[
        (3, build_raw(message_id="<m409@x>", attachments=[("dupe.pdf", b"%PDF-1")])),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(conflict_for={"dupe.pdf"})

    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    result = await poll_mailbox_once(store, client, mailbox, org)

    # 409 = already ingested in papra: success, not a per-message failure
    assert result.messages_failed == 0 and result.error is None
    assert result.documents_uploaded == 0
    assert session.seen == [3]
    assert store.claim_next_job("ocr") is None   # no job: document pre-exists
    updated = store.get_mailbox(mailbox_id)
    assert updated.last_seen_uid == 3
    assert updated.last_error is None and updated.consecutive_failures == 0

    # dedup hash recorded: a re-poll uploads nothing and still succeeds
    second = await poll_mailbox_once(store, client, updated, org)
    assert second.messages_failed == 0 and second.error is None
    assert second.documents_uploaded == 0 and client.uploads == []


async def test_poll_survives_store_errors_in_recording(store, monkeypatch):
    import sqlite3

    def _locked(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(store, "record_poll_failure", _locked)
    session = FakeSession(fail_on_connect=ConnectionRefusedError("refused"))
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    # the store error must not escape poll_mailbox_once (never-raises):
    # the result still reports the connection failure
    result = await poll_mailbox_once(store, StubClient(), make_mailbox(), ORG)

    assert isinstance(result, PollResult)
    assert result.error is not None and "refused" in result.error
    assert result.documents_uploaded == 0


async def test_poll_survives_store_errors_on_success_recording(store, monkeypatch):
    import sqlite3

    def _locked(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(store, "record_poll_success", _locked)
    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    # the success-recording twin: a locked store must not escape
    # poll_mailbox_once on a SUCCESSFUL poll either (the failure twin above
    # pins record_poll_failure); the result still reports the upload
    result = await poll_mailbox_once(store, StubClient(), make_mailbox(), ORG)

    assert isinstance(result, PollResult)
    assert result.documents_uploaded == 1 and result.error is None


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


async def test_delete_not_triggered_for_non_pdf_only_message(store, monkeypatch):
    # pin the had_pdfs guard: a jpg-only message under delete_after_download
    # must be marked seen but NEVER expunged (silent data loss otherwise)
    raw = build_raw(attachments=[("photo.jpg", b"\xff\xd8jpeg")])
    session = FakeSession(raws=[(1, raw)])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(
        store, StubClient(), make_mailbox(delete_after_download=True), ORG
    )

    assert result.documents_uploaded == 0 and result.error is None
    assert session.deleted == []
    assert session.seen == [1]


async def test_filtered_message_does_not_advance_past_failure(store, monkeypatch):
    # pin the freeze on the subject-filtered branch: a filtered message after
    # a failed one must not advance last_seen past the failure (silent loss)
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=".*invoice.*",
    )
    session = FakeSession(raws=[
        (1, build_raw(message_id="<m1@x>", attachments=[("bad.pdf", b"%PDF-1")])),
        (2, build_raw(subject="Newsletter", message_id="<m2@x>")),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(fail_for={"bad.pdf"})

    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    result = await poll_mailbox_once(store, client, mailbox, org)

    # uid 1 (matches filter, upload failed) stays re-fetchable: last_seen frozen
    assert store.get_mailbox(mailbox_id).last_seen_uid == 0
    # uid 2 was processed (subject-filtered -> seen), just not counted for advance
    assert session.seen == [2]
    assert result.messages_failed == 1 and result.error is None


async def test_oversized_message_does_not_advance_past_failure(store, monkeypatch):
    # pin the freeze guard on the oversized arm: a failed upload (uid 1)
    # followed by an oversized message (uid 2) must NOT advance last_seen
    # past the failure — deleting the `if not prefix_failed` guard on that
    # arm strands uid 1 (silent loss)
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    session = FakeSession(
        raws=[(1, build_raw(message_id="<m1@x>", attachments=[("bad.pdf", b"%PDF-1")]))],
        oversized=[2],
    )
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(fail_for={"bad.pdf"})

    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    result = await poll_mailbox_once(store, client, mailbox, org)

    # uid 1 failed: last_seen stays 0 so it stays re-fetchable
    assert store.get_mailbox(mailbox_id).last_seen_uid == 0
    # the oversized uid 2 was still counted as processed
    assert result.oversized_skipped == 1
    assert result.messages_failed == 1


async def test_store_stage_imap_error_never_raises(store, monkeypatch):
    # pin the STORE-stage never-raises guard: an IMAP error inside
    # mark_seen/delete_and_expunge must land in result.error (with failure
    # accounting), never escape poll_mailbox_once — removing the try/except
    # around the STORE stage lets the exception propagate and skips
    # record_poll_failure entirely
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )

    class ExplodingStoreSession(FakeSession):
        def mark_seen(self, uids):
            raise imaplib.IMAP4.error("STORE failed")

    session = ExplodingStoreSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    result = await poll_mailbox_once(store, StubClient(), mailbox, org)

    assert isinstance(result, PollResult)          # never raises
    assert result.error is not None and "STORE failed" in result.error
    assert session.closed is True
    updated = store.get_mailbox(mailbox_id)
    assert updated.consecutive_failures == 1       # failure accounting engaged
    assert "STORE failed" in updated.last_error


async def test_poll_advances_past_oversized_message(store, monkeypatch):
    # per-message cap at the poller level: an oversized uid is skipped without
    # parsing but still counts as processed — the poll advances past it
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    session = FakeSession(raws=[(2, build_raw(message_id="<m2@x>"))], oversized=[1])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    result = await poll_mailbox_once(store, client, mailbox, org)

    assert result.documents_uploaded == 1          # only the small message parsed
    assert len(client.uploads) == 1
    assert session.seen == [1, 2]                  # oversized still marked seen
    assert store.get_mailbox(mailbox_id).last_seen_uid == 2


async def test_poll_counts_oversized_messages(store, monkeypatch):
    # oversized_skipped: every uid the fetch batch reports as oversized is
    # counted once — the parsing-free skip is visible in the result, and the
    # parseable messages in the same batch must not inflate the counter
    session = FakeSession(
        raws=[(1, build_raw(message_id="<m1@x>")), (4, build_raw(message_id="<m4@x>"))],
        oversized=[2, 3, 5],
    )
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(store, client, make_mailbox(), ORG)

    assert result.error is None
    assert result.oversized_skipped == 3
    assert result.messages_scanned == 5            # oversized still scanned
    assert result.documents_uploaded == 2          # only the fetched bodies


async def test_poll_oversized_skipped_defaults_to_zero(store, monkeypatch):
    # a poll without oversized mail reports 0 (the dataclass default), and the
    # new field is appended last so positional construction of the pre-existing
    # fields — including `error` — keeps working
    legacy = PollResult(1, 2, 3, 4, "boom")
    assert (legacy.messages_scanned, legacy.documents_uploaded,
            legacy.attachments_skipped, legacy.messages_failed, legacy.error) == \
        (1, 2, 3, 4, "boom")
    assert PollResult().oversized_skipped == 0 and legacy.oversized_skipped == 0

    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(store, StubClient(), make_mailbox(), ORG)

    assert result.error is None
    assert result.oversized_skipped == 0


async def test_subject_filter_redos_is_bounded(store, monkeypatch, caplog):
    # Killable-child pin for the IN-CAP class: an admin regex that PASSES the
    # per-poll pre-validation (probe growth ratios ~4.4x and ~2.5x, under the
    # 10x threshold) but is still super-linearly slow against a crafted
    # subject within the 1000-char cap. The match runs in the killable child,
    # so the 2s budget ENDS it — SIGTERM is kernel-delivered, so it lands
    # mid-search and the event loop stays free — and the message is treated
    # as a non-match: warning logged, non-match, message seen.
    # What this pin does NOT claim: that validation rejected this pattern.
    # The ratio gate only detects patterns whose PROBE INPUTS are near-misses;
    # patterns whose ambiguity sits behind an anchor plus a literal
    # ((Rechnung|Invoice)\s*(\d+\s*)+$) pass validation untouched and are
    # bounded here by the kill alone — see
    # test_poll_kills_a_catastrophic_filter_match_and_still_advances.
    # (a|aa)+b, whose probes do reach its ambiguity, is still refused before
    # any message is read — see
    # test_poll_with_unsafe_subject_filter_records_failure.)
    crafted = "a" * 180 + "!" * (5000 - 180)
    session = FakeSession(raws=[(1, build_raw(subject=crafted, message_id="<redos@x>"))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    start = time.monotonic()
    with caplog.at_level("WARNING", logger="papra_suite.mail"):
        result = await poll_mailbox_once(
            store, StubClient(), make_mailbox(subject_filter=r"a*a*a*a*b"), ORG
        )
    elapsed = time.monotonic() - start

    assert result.error is None
    assert result.documents_uploaded == 0      # non-match: no upload attempted
    assert session.seen == [1]                 # intentionally skipped like a filter miss
    # the child was killed at the budget: the message became a non-match
    assert any("subject filter timed out" in record.message for record in caplog.records)
    # bounded by the kill, not by the search draining: a quartic search over
    # a 1000-char subject is still running long after this poll returned,
    # and it now burns a child's CPU rather than the service's event loop
    assert elapsed < 30.0


# The anchored-literal catastrophic class: its ambiguous group sits behind a
# literal the operator typed, so validate_subject_filter's probes ("a"*n and
# "1"*n near-misses) never reach the ambiguity, the growth ratio measures
# ~1x, and the pattern PASSES pre-validation. Measured on this machine the
# match costs ~2x per added digit: 20 digits 0.04s, 24 digits 0.69s,
# 26 digits 2.4s, 40 digits effectively forever — and MAX_SUBJECT_CHARS=1000
# bounds nothing of that. This is the pattern class the killable child exists
# for, and the reason "validation passed" must never be read as "safe".
ANCHORED_CATASTROPHIC = r"(Rechnung|Invoice)\s*(\d+\s*)+$"
POISON_SUBJECT = "Rechnung " + "1" * 40 + "!"


async def test_poll_kills_a_catastrophic_filter_match_and_still_advances(
    store, monkeypatch, caplog
):
    r"""C1: a subject filter that passes pre-validation but blows up on the
    operator's own literal must be bounded AT MATCH TIME by killing the
    match — and the poll must then behave like a filter miss.

    Before the killable child this froze the entire serve process: the
    search held the GIL, so wait_for could not fire until it drained, and
    \Seen plus the DB cursor are written AFTER the message loop — so the
    restart re-fetched the same mail and re-froze, forever, with the
    3-strike disable never engaging (a frozen poll never raises).
    """
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=ANCHORED_CATASTROPHIC,
    )
    session = FakeSession(
        raws=[(7, build_raw(subject=POISON_SUBJECT, message_id="<poison@x>"))]
    )
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    # proof that the event loop stayed responsive for the whole poll: a 10ms
    # ticker must get roughly its share of the wall clock. Frozen loop -> a
    # handful of ticks over ~2s of elapsed time; healthy -> ~100 per second.
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    ticker_task = asyncio.create_task(ticker())
    start = time.monotonic()
    with caplog.at_level("WARNING", logger="papra_suite.mail"):
        result = await poll_mailbox_once(
            store, client, store.get_mailbox(mailbox_id), store.get_org(org_id)
        )
    elapsed = time.monotonic() - start
    ticker_task.cancel()
    await asyncio.gather(ticker_task, return_exceptions=True)

    # the killed match is a NON-MATCH, not a poll failure: nothing uploaded,
    # no error recorded, the mailbox stays enabled
    assert result.error is None
    assert result.messages_scanned == 1
    assert result.documents_uploaded == 0
    assert client.uploads == []
    # ...and the poll still finished its bookkeeping, which is what stops the
    # next poll from re-fetching the poison message
    assert session.seen == [7]
    updated = store.get_mailbox(mailbox_id)
    assert updated.last_seen_uid == 7
    assert updated.enabled is True
    assert updated.consecutive_failures == 0
    assert updated.last_error is None
    # decided by the kill, not by the search draining (2**40 steps)
    assert elapsed < 15.0
    assert any("subject filter timed out" in record.getMessage() for record in caplog.records)
    # the loop was never frozen: the ticker got at least 60% of its share
    # (loose on purpose: child startup is ~15ms of loop work, and macOS
    # timer granularity eats a few ticks per second)
    assert ticks >= 0.6 * (elapsed / 0.01)


async def test_poll_kills_an_unbounded_validation_probe(store, monkeypatch):
    """The gate itself used to be unbounded: (a+)+\\d+$ against the "a"*30
    probe never returns (measured: still running after 15s), so the poll
    loop froze BEFORE a single mail was read. Validation now runs in the
    killable child: it comes back as a rejection on the existing
    poll-failure/3-strike path instead of a frozen process.
    """
    monkeypatch.setattr("papra_suite.mail.SUBJECT_VALIDATION_TIMEOUT_S", 1.0)
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=r"(a+)+\d+$",
    )
    session = FakeSession(raws=[(1, build_raw(subject="Rechnung 1", message_id="<x@x>"))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    start = time.monotonic()
    result = await poll_mailbox_once(
        store, StubClient(), store.get_mailbox(mailbox_id), store.get_org(org_id)
    )
    elapsed = time.monotonic() - start

    assert result.error is not None and "unsafe" in result.error
    mailbox = store.get_mailbox(mailbox_id)
    assert mailbox.consecutive_failures == 1
    assert "unsafe" in (mailbox.last_error or "")
    # bounded by the kill: the sync gate hung indefinitely
    assert elapsed < 20.0
    # rejected before connect/processing, exactly like the ratio-gate class
    assert result.messages_scanned == 0
    assert session.seen == []
    assert session.closed is True


class StubMatcher:
    """A matcher that answers every search with one fixed outcome.

    Lets a test drive the poll site's decision for a single outcome (the
    child-died classes) without paying for a real catastrophic search, so the
    split between TIMEOUT (retire) and UNAVAILABLE (do not retire) can be
    asserted against the same fixture.
    """

    def __init__(self, outcome):
        import re as _re

        self._ignorecase = _re.IGNORECASE
        self.outcome = outcome
        self.searches: list[tuple[str, str]] = []
        self.validated: list[str | None] = []
        self.closed = False

    async def search(self, pattern, subject, *, timeout_s, flags=None):
        self.searches.append((pattern, subject))
        return self.outcome

    async def validate(self, pattern, *, timeout_s):
        self.validated.append(pattern)
        return None

    async def aclose(self):
        self.closed = True


async def test_poll_degrades_when_the_regex_child_cannot_run(store, monkeypatch, caplog):
    r"""CONTRACT CHANGE (round-4 review, decided deliberately): a child that
    cannot be STARTED is not a filter miss.

    The old contract degraded UNAVAILABLE to a non-match, \Seen the message and
    moved the cursor past it. Two reviewers and the orchestrator measured the
    result: a well-formed `Rechnung 4711` on a filtered mailbox ended
    `uploaded=0 error=None` with the cursor advanced and never re-fetched —
    permanent silent ingestion loss for every filtered mailbox whenever the
    child cannot run at all (venv interpreter replaced under a running daemon,
    RLIMIT_NPROC/TasksMax, fd exhaustion), and matcher.validate was skipped the
    same way. This is not a test being weakened: the assertions got stricter.
    "cannot decide" must stop the poll visibly (result.error +
    record_poll_failure + the 3-strike path) and must leave the mail re-fetchable.
    TIMEOUT still retires — that is what stops C1 re-poisoning, and its
    companion assertion below pins it in the same fixture.
    """
    import sys

    import papra_suite.mail as mail_mod
    from papra_suite.safematch import KillableMatcher, MatchOutcome

    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=".*rechnung.*",
    )
    # every child attempt dies instantly: no match can ever be answered
    monkeypatch.setattr(
        mail_mod, "KillableMatcher",
        lambda: KillableMatcher(command=[sys.executable, "-c", "import sys; sys.exit(3)"]),
    )
    session = FakeSession(raws=[(4, build_raw(subject="Rechnung 42", message_id="<m4@x>"))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    with caplog.at_level("WARNING", logger="papra_suite.mail"):
        result = await asyncio.wait_for(
            poll_mailbox_once(
                store, client, store.get_mailbox(mailbox_id), store.get_org(org_id)
            ),
            timeout=60,
        )

    mailbox = store.get_mailbox(mailbox_id)
    # the poll FAILS visibly instead of reporting an empty success
    assert result.error is not None
    assert "regex child" in result.error
    assert result.documents_uploaded == 0        # an unanswered filter never ingests
    # ...and nothing about this mail moved: no \Seen, no cursor, next poll re-fetches
    assert session.seen == []
    assert mailbox.last_seen_uid is None
    assert mailbox.consecutive_failures == 1     # failure counted toward 3-strike
    assert mailbox.last_error is not None
    assert mailbox.enabled is True               # one failure is not the 3rd strike

    # companion: TIMEOUT keeps the retired-and-advanced behaviour EXACTLY. A
    # killed per-message search is a DECIDED non-match (the budget was spent on
    # this subject), and retiring it is what stops the poison mail from being
    # re-fetched and re-armed on the next poll.
    other_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=".*rechnung.*",
    )
    timeout_session = FakeSession(
        raws=[(9, build_raw(subject="Rechnung 42", message_id="<m9@x>"))]
    )
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: timeout_session)
    timeout_result = await asyncio.wait_for(
        poll_mailbox_once(store, StubClient(), store.get_mailbox(other_id),
                          store.get_org(org_id), matcher=StubMatcher(MatchOutcome.TIMEOUT)),
        timeout=60,
    )
    assert timeout_result.error is None          # decided, so not a poll failure
    assert timeout_result.documents_uploaded == 0
    assert timeout_session.seen == [9]           # still retired
    assert store.get_mailbox(other_id).last_seen_uid == 9
    assert store.get_mailbox(other_id).consecutive_failures == 0


async def test_poll_stops_without_retiring_mail_when_a_match_cannot_be_decided(
    store, monkeypatch
):
    """UNAVAILABLE during the per-message match freezes the cursor mid-poll.

    Already-uploaded mail keeps its progress (upload + dedup + OCR job all
    happened before the undecided message); the undecided message is NOT
    \Seen and the cursor does not pass it, so the next poll re-fetches it.
    Re-delivery is safe because the dedup hash already exists.
    """
    from papra_suite.safematch import MatchOutcome

    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=".*rechnung.*",
    )
    session = FakeSession(raws=[
        (5, build_raw(subject="Rechnung 5", message_id="<m5@x>")),
        (6, build_raw(subject="Rechnung 6", message_id="<m6@x>")),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()
    matcher = StubMatcher(MatchOutcome.UNAVAILABLE)

    result = await asyncio.wait_for(
        poll_mailbox_once(store, client, store.get_mailbox(mailbox_id),
                          store.get_org(org_id), matcher=matcher),
        timeout=60,
    )

    mailbox = store.get_mailbox(mailbox_id)
    assert result.error is not None and "uid=5" in result.error
    assert result.documents_uploaded == 0        # undecided: nothing ingested
    assert session.seen == []                    # neither message was \Seen
    # the cursor never got the undecidable message's uid, so the next poll
    # re-fetches it (a uid decided before the failure keeps its progress)
    assert mailbox.last_seen_uid is None
    assert mailbox.consecutive_failures == 1


async def test_poll_keeps_earlier_uploads_when_a_later_match_cannot_be_decided(
    store, monkeypatch
):
    """A mid-poll UNAVAILABLE must not rewind progress already made: the first
    message matched and uploaded before the second one became undecided, and
    that upload (dedup hash + OCR job) survives the stopped poll. Re-delivery
    after the cursor unfreezes is then a dedup no-op, not a double ingest.
    """
    from papra_suite.safematch import MatchOutcome

    class FirstMatchesThenUnavailable:
        def __init__(self):
            self.calls = 0
            self.closed = False

        async def search(self, pattern, subject, *, timeout_s, flags=None):
            self.calls += 1
            if self.calls == 1:
                return MatchOutcome.MATCH
            return MatchOutcome.UNAVAILABLE

        async def validate(self, pattern, *, timeout_s):
            return None

        async def aclose(self):
            self.closed = True

    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=".*rechnung.*",
    )
    session = FakeSession(raws=[
        (5, build_raw(subject="Rechnung 5", message_id="<m5@x>")),
        (6, build_raw(subject="Rechnung 6", message_id="<m6@x>")),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await asyncio.wait_for(
        poll_mailbox_once(store, client, store.get_mailbox(mailbox_id),
                          store.get_org(org_id), matcher=FirstMatchesThenUnavailable()),
        timeout=60,
    )

    assert result.documents_uploaded == 1                  # the answered match stands
    assert [u[1] for u in client.uploads] == ["a.pdf"]
    assert store.dedup_exists(poll_dedup_hash(mailbox_id, "<m5@x>", "a.pdf"))
    ocr_jobs = [job for job in store.list_jobs() if job.kind == "ocr"]
    assert [job.status for job in ocr_jobs] == ["pending"]
    assert result.error is not None                        # the stopped poll is visible
    assert session.seen == []                              # nothing was \Seen
    assert store.get_mailbox(mailbox_id).last_seen_uid is None


def poll_dedup_hash(mailbox_id, message_id, filename):
    from papra_suite.mail import dedup_hash

    return dedup_hash(mailbox_id, message_id, filename, len(b"%PDF-1"))


async def test_poll_fails_when_the_filter_cannot_be_prevalidated(store, monkeypatch):
    r"""A mailbox whose filter cannot be EVALUATED must not poll "successfully".

    KillableMatcher.validate swallowed MatcherUnavailable and logged
    "pre-validation skipped", so a mailbox with a dead child polled GREEN
    forever — and the catastrophic-pattern rejection that gate exists to
    enforce stopped being enforced at all. UNAVAILABLE from the gate now runs
    into the same poll-failure path as a rejected pattern: error visible,
    nothing processed, no \Seen. Red today: the swallowed skip let the poll
    scan the message and retire it.
    """
    import sys

    import papra_suite.mail as mail_mod
    from papra_suite.safematch import KillableMatcher

    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=r"(a|aa)+b",
    )
    session = FakeSession(raws=[(1, build_raw(subject="a" * 5000, message_id="<x@x>"))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    monkeypatch.setattr(
        mail_mod, "KillableMatcher",
        lambda: KillableMatcher(command=[sys.executable, "-c", "import sys; sys.exit(3)"]),
    )

    result = await asyncio.wait_for(
        poll_mailbox_once(store, StubClient(), store.get_mailbox(mailbox_id),
                          store.get_org(org_id)),
        timeout=60,
    )

    assert result.error is not None and "regex child" in result.error
    assert result.messages_scanned == 0              # rejected before processing
    assert session.seen == []
    mailbox = store.get_mailbox(mailbox_id)
    assert mailbox.consecutive_failures == 1
    assert mailbox.last_error is not None


async def test_poll_with_unsafe_subject_filter_records_failure(store, monkeypatch):
    """Unsafe-filter pin: a subject filter whose probe inputs reach its own
    ambiguity (exponential backtracking class) must fail the poll VISIBLY —
    result.error set and a poll failure recorded toward the 3-strike disable
    — BEFORE connecting or processing any message.

    Removing the matcher.validate call from the poll does not re-freeze the
    process any more (the killable child already guarantees that); it makes
    the failure INVISIBLE instead — the mailbox would keep polling forever,
    every message burning its full match budget in a child before being
    counted as a non-match, and a permanently unusable filter would never
    reach the dashboard or the 3-strike disable. That is why the gate still
    runs up front, in the same killable child.
    """
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=r"(a|aa)+b",
    )
    session = FakeSession(raws=[(1, build_raw(subject="a" * 5000, message_id="<x@x>"))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    start = time.monotonic()
    result = await poll_mailbox_once(
        store, StubClient(), store.get_mailbox(mailbox_id), store.get_org(org_id)
    )
    elapsed = time.monotonic() - start

    # the failure is visible in the result and the persisted mailbox state
    assert result.error is not None and "unsafe" in result.error
    mailbox = store.get_mailbox(mailbox_id)
    assert mailbox.consecutive_failures == 1
    assert "unsafe" in (mailbox.last_error or "")
    # no freeze: the pre-validation probes are milliseconds, not the ~10s
    # the pattern would burn against the crafted subject
    assert elapsed < 5.0
    # nothing was processed: no connect, no fetch handling, no uploads
    assert result.messages_scanned == 0
    assert result.documents_uploaded == 0
    assert session.seen == []
    assert session.closed is True  # the failure path still closed the session


# --- what the child is actually handed -------------------------------------

class RecordingMatcher(safematch.KillableMatcher):
    """The real killable child, with every request it was asked to make kept
    for inspection — the only way to pin what the CHILD path receives, as
    opposed to what the sync `subject_matches` helper would have done."""

    instances: list = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.validated: list[tuple[str, float]] = []
        self.searches: list[tuple[str, str, float]] = []
        RecordingMatcher.instances.append(self)

    async def validate(self, pattern, *, timeout_s):
        self.validated.append((pattern, timeout_s))
        outcome = await super().validate(pattern, timeout_s=timeout_s)
        assert self.pid is not None, "validation did not run in a child"
        return outcome

    async def search(self, pattern, subject, *, timeout_s,
                     flags=re.IGNORECASE):
        # the whole point: a real child is already running when the poll site
        # asks, so this is the child path and not the sync helper
        assert self.pid is not None, "the search ran without a child"
        self.searches.append((pattern, subject, timeout_s))
        return await super().search(pattern, subject, timeout_s=timeout_s, flags=flags)


async def test_the_child_is_handed_a_capped_subject_and_never_a_missing_one(
    store, monkeypatch
):
    """The child-path half of the subject gate, which no other test reaches:

    * subject[:MAX_SUBJECT_CHARS] — dropping the cap SURVIVES every other test,
      because the sync `subject_matches` caps too and is what the
      sync-only tests exercise. Here the cap is the only thing keeping a
      hostile 4000-character subject from being handed to the child whole.
    * `subject is None -> non-match` — both ways of losing that guard were
      mutation survivors before this test: deleted outright, the slice raises
      TypeError and the message is counted as a poll failure; relaxed to
      `subject or ""` (or `str(subject)`), the child is asked to match an empty
      or a "None" subject, and an operator filter like `^$|Rechnung` then
      decides a message that has no subject at all. Either way the wrong thing
      is decided; a missing subject has to be settled in the parent, without a
      request.
    """
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=r"^Rechnung",
    )
    monkeypatch.setattr(mail, "KillableMatcher", RecordingMatcher)
    RecordingMatcher.instances.clear()
    long_subject = "Rechnung " + "1" * 4000
    # a genuinely missing Subject header (build_raw(subject=None) would put the
    # four-character string "None" in it, which is a different question)
    no_subject = re.sub(rb"^Subject:.*\r?\n", b"",
                       build_raw(message_id="<none@x>"), count=1, flags=re.M)
    session = FakeSession(raws=[
        (1, build_raw(subject=long_subject, message_id="<long@x>")),
        (2, no_subject),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(
        store, StubClient(), store.get_mailbox(mailbox_id), store.get_org(org_id)
    )

    assert result.error is None
    matcher = RecordingMatcher.instances[0]
    # one request for the long subject, NONE for the missing one, and the
    # missing-subject message was not uploaded (non-match, not "None")
    assert [subject for _pattern, subject, _t in matcher.searches] == \
        [long_subject[:mail.MAX_SUBJECT_CHARS]]
    assert len(matcher.searches[0][1]) == mail.MAX_SUBJECT_CHARS
    assert result.documents_uploaded == 1
    assert session.seen == [1, 2]           # both retired, cursor moves on
    assert store.get_mailbox(mailbox_id).last_seen_uid == 2


def test_the_regex_budgets_are_pinned():
    """Constant pin: both shipped timeout defaults survive any change to their
    value (2.0 -> 0.001 and 10 -> 3600 were both mutation-survivors), and the
    two numbers are load-bearing in opposite directions.

    SUBJECT_FILTER_TIMEOUT_S is the per-message budget for the operator's
    regex in the killable child: raise it and a poison subject delays a whole
    poll for every message in the batch (2 s x 50 messages is already 100 s of
    a mailbox's poll); lower it and legitimate long subjects on a loaded box
    start timing out into silent non-matches.
    SUBJECT_VALIDATION_TIMEOUT_S bounds the pre-validation probes, which must
    stay far BELOW the 300 s poll interval while remaining generous enough that
    a pattern which cannot finish a 30-byte probe is rejected rather than
    installed; at 3600 s a single stuck probe would outlive the poll loop's own
    interval and the mailbox would never report anything at all.

    Asserted as constants on purpose — a test that slept 10 s to observe the
    validation budget would slow the suite to measure a number that the kill
    path already proves is enforced (see
    test_poll_kills_an_unbounded_validation_probe).
    """
    assert mail.MAX_SUBJECT_CHARS == 1000          # the cap the child receives
    assert mail.SUBJECT_FILTER_TIMEOUT_S == 2.0    # per-message match budget
    assert mail.SUBJECT_VALIDATION_TIMEOUT_S == 10.0   # whole pre-validation
    # each request's self-deadline is ITS OWN budget plus this grace (so 4s for
    # a per-message search and 12s for pre-validation, independently): positive
    # grace is what keeps the parent's own kill the thing that normally ends a
    # child, and a grace of 0 would have the child shoot itself in the foot
    # every time the parent is one tick late
    assert safematch.SELF_DEADLINE_GRACE_S > 0
