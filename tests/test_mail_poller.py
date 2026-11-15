import asyncio
import imaplib
from dataclasses import dataclass, field

import pytest

from papra_suite.allowlist import PERMISSION, parse_allowlist
from papra_suite.mail import FetchBatch, MailboxSession, PollResult, poll_mailbox_once
from papra_suite.store import Mailbox, Org, Store
from tests.test_mail_session import FakeImap


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


# x.com covers the fixture's sender. Tests that care about admission build their
# own list; everything else just needs to get past the gate to reach the mail
# pipeline under test.
ALLOW = parse_allowlist("x.com")


async def poll_with_allow_list(store, client, mailbox, org):
    return await poll_mailbox_once(store, client, mailbox, org, allowlist=ALLOW)


def build_raw(subject="Invoice", message_id="<m1@x>",
              sender="Sender <sender@x.com>",
              attachments=(("a.pdf", b"%PDF-1"),)):
    from tests.test_mail_parsing import build_raw as _build
    return _build(subject=subject, message_id=message_id, sender=sender, attachments=[
        (name, content, ("application", "pdf")) for name, content in attachments
    ])


def _headers_of(raw: bytes) -> bytes:
    """Header block of a raw message, the way IMAP would hand it back."""
    for sep in (b"\r\n\r\n", b"\n\n"):
        if sep in raw:
            return raw.split(sep, 1)[0] + sep
    return raw


@dataclass
class FakeSession:
    uidvalidity: str = "111"
    raws: list = field(default_factory=list)     # [(uid, raw)]
    oversized: list = field(default_factory=list)  # uids above the per-message cap
    seen: list = field(default_factory=list)
    header_checked: list = field(default_factory=list)  # uids read as headers only
    raws_read: list = field(default_factory=list)       # uids whose body was read
    deleted: list = field(default_factory=list)
    fail_on_connect: Exception | None = None
    fail_fetch_for: list = field(default_factory=list)  # uids with an unreadable answer
    closed: bool = False

    def connect(self):
        if self.fail_on_connect:
            raise self.fail_on_connect

    def select_inbox(self):
        return self.uidvalidity

    def fetch_since(self, last_seen_uid, admit=None):
        messages = self.raws if last_seen_uid is None \
            else [(uid, raw) for uid, raw in self.raws if uid > last_seen_uid]
        oversized = self.oversized if last_seen_uid is None \
            else [uid for uid in self.oversized if uid > last_seen_uid]
        denied = []
        admitted = []
        for uid, raw in messages:
            if uid in self.fail_fetch_for:
                # an answer this client cannot read is NOT "the message is gone"
                import imaplib

                raise imaplib.IMAP4.error(
                    f"UID FETCH for {uid} returned no message data")
            if admit is None:
                admitted.append((uid, raw))
                continue
            # A transport-shaped fake: admission is answered from the header
            # bytes only, and a refused message is never handed over as a body.
            self.header_checked.append(uid)
            verdict = admit(_headers_of(raw))
            if verdict != PERMISSION:
                denied.append((uid, verdict))
                continue
            self.raws_read.append(uid)
            admitted.append((uid, raw))
        return FetchBatch(messages=admitted, oversized_uids=list(oversized),
                          denied=denied)

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

    result = await poll_with_allow_list(store, client, make_mailbox(), ORG)

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

    result = await poll_with_allow_list(store, StubClient(), make_mailbox(), ORG)

    assert result.documents_uploaded == 0
    assert result.attachments_skipped == 1
    assert session.seen == [1]              # handled, read
    assert session.deleted == []           # never deleted (not processed)
async def test_poll_dedup_prevents_double_upload(store, monkeypatch):
    raw = build_raw()
    session = FakeSession(raws=[(1, raw)])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    await poll_with_allow_list(store, client, make_mailbox(), ORG)
    # same message again (e.g. failed upload of a LATER message kept it fetchable)
    result = await poll_with_allow_list(store, client, make_mailbox(), ORG)

    assert result.documents_uploaded == 0
    assert len(client.uploads) == 1


async def test_poll_upload_failure_does_not_advance_last_seen(store, monkeypatch):
    session = FakeSession(raws=[
        (1, build_raw(message_id="<m1@x>", attachments=[("bad.pdf", b"%PDF-1")])),
        (2, build_raw(message_id="<m2@x>", attachments=[("good.pdf", b"%PDF-2")])),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(fail_for={"bad.pdf"})

    result = await poll_with_allow_list(store, client, make_mailbox(), ORG)

    assert result.documents_uploaded == 1 and result.messages_failed == 1
    assert session.seen == [2]             # only the successful message is read
    # mailbox state: failure recorded (upload failures count toward error state)
    # NOTE: mb1 is not persisted; the poller records failure only for stored
    # mailboxes — assert the result instead
    assert result.error is None            # per-message failure is not a poll error


async def test_poll_delete_after_download(store, monkeypatch):
    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    await poll_with_allow_list(store, StubClient(), make_mailbox(delete_after_download=True), ORG)

    assert session.seen == [1] and session.deleted == [1]


async def test_poll_delete_not_triggered_on_upload_failure(store, monkeypatch):
    session = FakeSession(raws=[(1, build_raw(attachments=[("bad.pdf", b"%PDF-1")]))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(fail_for={"bad.pdf"})

    await poll_with_allow_list(store, client, make_mailbox(delete_after_download=True), ORG)

    assert session.deleted == []           # stays in the mailbox for the next poll
    assert session.seen == []


async def test_poll_uidvalidity_change_rescans(store, monkeypatch):
    session = FakeSession(uidvalidity="222", raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    # mailbox claims uid 5 already seen under the OLD validity 111
    result = await poll_with_allow_list(
        store, StubClient(), make_mailbox(last_seen_uid=5, uidvalidity="111"), ORG
    )

    assert result.documents_uploaded == 1   # uid 1 re-fetched despite last_seen 5


async def test_poll_connection_failure_records_error_not_crash(store, monkeypatch):
    session = FakeSession(fail_on_connect=ConnectionRefusedError("refused"))
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_with_allow_list(store, StubClient(), make_mailbox(), ORG)

    assert result.error is not None and "refused" in result.error
    assert result.documents_uploaded == 0


async def test_poll_registers_mailbox_password_for_redaction(store, monkeypatch, capsys):
    from papra_suite.logging_setup import setup_logging

    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    import logging
    setup_logging(store)
    await poll_with_allow_list(store, StubClient(), make_mailbox(password="box-secret-pw"), ORG)
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
    await poll_with_allow_list(store, StubClient(), mailbox, org)

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
    result = await poll_with_allow_list(store, client, mailbox, org)

    # uid 1 failed: last_seen must not advance past it (uid 2's success must
    # not strand uid 1) — the empty-poll default 0 keeps uid 1 re-fetchable
    assert store.get_mailbox(mailbox_id).last_seen_uid == 0
    assert result.documents_uploaded == 1

    # a follow-up poll re-fetches uid 1 (and retries its upload)
    second = await poll_with_allow_list(
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
    await poll_with_allow_list(store, client, mailbox, org)

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

    result = await poll_with_allow_list(store, StubClient(), make_mailbox(), ORG)

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

    result = await poll_with_allow_list(store, StubClient(), mailbox, org)

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
        def fetch_since(self, last_seen_uid, admit=None):
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
    result = await poll_with_allow_list(store, client, mailbox, org)

    assert result.documents_uploaded == 3
    assert store.get_mailbox(mailbox_id).last_seen_uid == 3

    second = await poll_with_allow_list(store, client, store.get_mailbox(mailbox_id), org)

    assert second.documents_uploaded == 2
    assert len(client.uploads) == 5
    assert store.get_mailbox(mailbox_id).last_seen_uid == 5


async def test_poll_never_raises_on_internal_error(store, monkeypatch):
    def _boom(raw):
        raise RuntimeError("boom")

    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    monkeypatch.setattr("papra_suite.mail.parse_message", _boom)

    result = await poll_with_allow_list(store, StubClient(), make_mailbox(), ORG)

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
    result = await poll_with_allow_list(store, client, mailbox, org)

    # 409 = already ingested in papra: success, not a per-message failure
    assert result.messages_failed == 0 and result.error is None
    assert result.documents_uploaded == 0
    assert session.seen == [3]
    assert store.claim_next_job("ocr") is None   # no job: document pre-exists
    updated = store.get_mailbox(mailbox_id)
    assert updated.last_seen_uid == 3
    assert updated.last_error is None and updated.consecutive_failures == 0

    # dedup hash recorded: a re-poll uploads nothing and still succeeds
    second = await poll_with_allow_list(store, client, updated, org)
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
    result = await poll_with_allow_list(store, StubClient(), make_mailbox(), ORG)

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
    result = await poll_with_allow_list(store, StubClient(), make_mailbox(), ORG)

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
        result = await poll_with_allow_list(store, StubClient(), mailbox, org)

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

    result = await poll_with_allow_list(
        store, StubClient(), make_mailbox(delete_after_download=True), ORG
    )

    assert result.documents_uploaded == 0 and result.error is None
    assert session.deleted == []
    assert session.seen == [1]
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
    result = await poll_with_allow_list(store, client, mailbox, org)

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
    result = await poll_with_allow_list(store, StubClient(), mailbox, org)

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
    result = await poll_with_allow_list(store, client, mailbox, org)

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

    result = await poll_with_allow_list(store, client, make_mailbox(), ORG)

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

    result = await poll_with_allow_list(store, StubClient(), make_mailbox(), ORG)

    assert result.error is None
    assert result.oversized_skipped == 0
ANCHORED_CATASTROPHIC = r"(Rechnung|Invoice)\s*(\d+\s*)+$"
POISON_SUBJECT = "Rechnung " + "1" * 40 + "!"
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


# --- sender allow-list ------------------------------------------------------
#
# The allow-list is the only thing deciding what enters Papra now, so these
# cover the disposition, not just the parse: what happens to refused mail is
# where data loss would live.

async def test_denied_sender_is_retired_without_upload_or_poll_failure(store, monkeypatch):
    """A stranger's mail is a NORMAL outcome, not a fault.

    Three consecutive failures disable a mailbox, so if a refusal counted as a
    failure, anyone who found the address could switch the mailbox off from
    the outside.
    """
    session = FakeSession(raws=[(1, build_raw(sender="Stranger <spam@other.net>"))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(store, client, make_mailbox(), ORG,
                                     allowlist=parse_allowlist("x.com"))

    assert result.error is None
    assert client.uploads == []
    assert result.messages_scanned == 1 and result.senders_denied == 1
    assert result.documents_uploaded == 0
    assert session.seen == [1] and session.deleted == []


async def test_refused_mail_is_not_refused_again_on_every_poll(store, monkeypatch):
    """Retiring a refused message must also advance the cursor."""
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    allow = parse_allowlist("x.com")

    session = FakeSession(raws=[(1, build_raw(sender="Stranger <spam@other.net>"))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    first = await poll_mailbox_once(store, StubClient(), store.get_mailbox(mailbox_id),
                                    store.get_org(org_id), allowlist=allow)
    assert first.senders_denied == 1

    updated = store.get_mailbox(mailbox_id)
    assert updated.last_seen_uid == 1
    assert updated.enabled is True and updated.consecutive_failures == 0

    second = await poll_mailbox_once(store, StubClient(), updated,
                                     store.get_org(org_id), allowlist=allow)
    assert second.messages_scanned == 0 and second.senders_denied == 0


async def test_unresolvable_sender_is_counted_as_malformed_not_as_a_denied_sender(
    store, monkeypatch
):
    """The two verdicts must stay separate in the log and the counters: a
    header that resolves to no address or to two is attacker-shaped input, and
    reporting it as "a sender you might want to allow" teaches the operator to
    add the wrong thing."""
    session = FakeSession(raws=[(1, build_raw(sender=""))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(store, client, make_mailbox(), ORG,
                                     allowlist=parse_allowlist("x.com"))

    assert result.malformed_senders == 1 and result.senders_denied == 0
    assert result.error is None and client.uploads == []
    assert session.seen == [1]


async def test_an_unconfigured_allow_list_paused_rather_than_retiring(store, monkeypatch):
    """No usable entry must PAUSE, not consume.

    Marking mail seen while the operator is still writing PAPRA_ALLOWED_SENDERS
    would retire the inbox one message at a time and leave nothing to recover
    from the log. Nothing is fetched, nothing is marked seen, and the mailbox is
    not failed — pausing is a decision to wait, not an outage.
    """
    session = FakeSession(raws=[(1, build_raw())])  # the usual allowed sender
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(store, client, make_mailbox(), ORG,
                                     allowlist=parse_allowlist(""))

    assert result.ingestion_paused is True
    assert result.error is None and result.senders_denied == 0
    assert client.uploads == [] and session.seen == []
    assert session.closed is False  # never even connected


async def test_refused_mail_is_never_deleted_even_with_delete_after_download(
    store, monkeypatch
):
    """Refusing a sender is not a licence to destroy the operator's mail. The
    message stays on the server, which is what makes a corrected allow-list
    recoverable instead of final."""
    session = FakeSession(raws=[(1, build_raw(sender="Stranger <spam@other.net>"))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    await poll_mailbox_once(store, StubClient(), make_mailbox(delete_after_download=True),
                            ORG, allowlist=parse_allowlist("x.com"))

    assert session.deleted == []
    assert session.seen == [1]


async def test_refused_mail_does_not_let_the_cursor_pass_an_earlier_failure(
    store, monkeypatch
):
    """Retiring a refused message must not launder the message before it that
    failed to upload: the cursor stays behind the failure, or that document is
    lost with nothing left to retry it."""
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    session = FakeSession(raws=[
        (1, build_raw(message_id="<m1@x>", attachments=[("bad.pdf", b"%PDF-1")])),
        (2, build_raw(message_id="<m2@x>", sender="Stranger <spam@other.net>")),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(store, StubClient(fail_for={"bad.pdf"}),
                                     store.get_mailbox(mailbox_id), store.get_org(org_id),
                                     allowlist=parse_allowlist("x.com"))

    assert result.senders_denied == 1
    assert store.get_mailbox(mailbox_id).last_seen_uid == 0


async def test_a_forwarded_attachment_does_not_borrow_its_inner_sender(store, monkeypatch):
    """The identity is the outer forwarding message. Anyone who can mail the
    drop-box cannot get admitted by embedding a message from an allowed address
    inside it — and the inner PDF stays out too, because the outer message was
    refused before its parts were ever considered."""
    raw = (
        b"From: Stranger <spam@other.net>\r\n"
        b"Message-ID: <outer@evil>\r\n"
        b"Subject: invoice\r\n"
        b"MIME-Version: 1.0\r\n"
        b'Content-Type: multipart/mixed; boundary="b"\r\n\r\n'
        b"--b\r\n"
        b"Content-Type: message/rfc822\r\n\r\n"
        b"From: Allowed <sender@x.com>\r\n"
        b"Subject: looks trustworthy\r\n"
        b"Content-Type: application/pdf\r\n"
        b'Content-Disposition: attachment; filename="inner.pdf"\r\n'
        b"Content-Transfer-Encoding: base64\r\n\r\n"
        b"JVBERi0x\r\n"
        b"--b--\r\n"
    )
    session = FakeSession(raws=[(1, raw)])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(store, client, make_mailbox(), ORG,
                                     allowlist=parse_allowlist("x.com"))

    assert result.senders_denied == 1 and result.malformed_senders == 0
    assert client.uploads == []


# --- header-first admission (poller side) -----------------------------------

async def test_a_refused_sender_never_has_its_body_read(store, monkeypatch):
    """The claim is about ORDER, so the assertion is about order: the refusal is
    recorded, and the body is never handed over to be parsed."""
    session = FakeSession(raws=[(1, build_raw(sender="Stranger <spam@other.net>"))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_with_allow_list(store, client, make_mailbox(), ORG)

    assert session.header_checked == [1]
    assert session.raws_read == []
    assert client.uploads == [] and result.senders_denied == 1
    assert session.seen == [1]


async def test_a_refusal_before_a_failed_upload_advances_only_to_the_refusal(
    store, monkeypatch
):
    """The refusal at uid 1 is decided, so the cursor may reach 1. The upload at
    uid 2 FAILED, so it may not go past 1 — that document has to be retried."""
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    session = FakeSession(raws=[
        (1, build_raw(message_id="<m1@x>", sender="Stranger <spam@other.net>")),
        (2, build_raw(message_id="<m2@x>", attachments=[("a.pdf", b"%PDF-1")])),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    await poll_with_allow_list(store, StubClient(fail_for={"a.pdf"}),
                               store.get_mailbox(mailbox_id), store.get_org(org_id))

    assert store.get_mailbox(mailbox_id).last_seen_uid == 1


async def test_a_failure_before_a_later_refusal_keeps_the_cursor_behind_it(
    store, monkeypatch
):
    """A refusal further along must not launder the failed upload before it."""
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    session = FakeSession(raws=[
        (1, build_raw(message_id="<m1@x>", attachments=[("bad.pdf", b"%PDF-1")])),
        (2, build_raw(message_id="<m2@x>", sender="Stranger <spam@other.net>")),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    await poll_with_allow_list(store, StubClient(fail_for={"bad.pdf"}),
                               store.get_mailbox(mailbox_id), store.get_org(org_id))

    assert store.get_mailbox(mailbox_id).last_seen_uid == 0


async def test_an_unreadable_fetch_answer_cannot_be_bypassed_by_the_next_message(
    store, monkeypatch
):
    """The loss this guards: uid 1 answers unreadably, uid 2 works fine. If uid 1
    were skipped, the cursor would reach 2 and uid 1 would never be searched
    again — mail gone with no error anywhere. The poll must fail instead."""
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    session = FakeSession(
        raws=[
            (1, build_raw(message_id="<m1@x>", attachments=[("a.pdf", b"%PDF-1")])),
            (2, build_raw(message_id="<m2@x>", attachments=[("b.pdf", b"%PDF-2")])),
        ],
        fail_fetch_for=[1],
    )
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_with_allow_list(store, client, store.get_mailbox(mailbox_id),
                                        store.get_org(org_id))

    assert result.error is not None
    assert client.uploads == []                      # nothing after it was ingested
    assert session.seen == []                        # and nothing was retired
    assert store.get_mailbox(mailbox_id).last_seen_uid is None


async def test_an_oversized_message_is_still_counted_alongside_refusals(
    store, monkeypatch
):
    session = FakeSession(
        raws=[(2, build_raw(sender="Stranger <spam@other.net>"))], oversized=[1]
    )
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_with_allow_list(store, StubClient(), make_mailbox(), ORG)

    assert result.oversized_skipped == 1 and result.senders_denied == 1
    assert result.messages_scanned == 2


async def test_malformed_metadata_on_a_low_uid_cannot_be_bypassed_by_a_good_one(
    store, monkeypatch
):
    """Through the REAL MailboxSession, not a fake that volunteers an exception:
    uid 1 answers its metadata unreadably, uid 2 is fine. If the unreadable
    answer were read as "gone", the cursor would reach 2 and uid 1 would be lost
    without an error, a refusal or a log line."""

    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    org = store.get_org(org_id)
    good = build_raw(message_id="<m2@x>", attachments=[("b.pdf", b"%PDF-2")])
    imap = FakeImap(
        search_uids=(1, 2),
        bodies={1: build_raw(message_id="<m1@x>", attachments=[("a.pdf", b"%PDF-1")]),
                2: good},
        size_responses={1: [b"1 (FLAGS (\\Seen) UID 1)"]},  # no size at all
    )
    monkeypatch.setattr("papra_suite.mail.MailboxSession",
                        lambda **kw: ScriptedSession(imap))
    client = StubClient()

    result = await poll_with_allow_list(store, client, store.get_mailbox(mailbox_id), org)

    # assert the CAUSE, not merely that something failed: a test that passes
    # because the fake lacked a method would guard nothing
    assert "RFC822.SIZE" in (result.error or "")
    assert client.uploads == []
    assert result.senders_denied == 0 and result.malformed_senders == 0
    assert store.get_mailbox(mailbox_id).last_seen_uid is None


class ScriptedSession(MailboxSession):
    """A real MailboxSession over a scripted IMAP double: only the socket is
    faked, so fetch_since, select_inbox and the metadata rules under test are
    the production ones."""

    def __init__(self, imap):
        super().__init__(host="imap.test", port=993, username="u", password="p",
                         ssl_mode="plain")
        self._imap = imap

    def connect(self):
        pass


async def test_replay_after_fixing_the_allow_list_ingests_once_and_never_twice(
    store, monkeypatch
):
    """The recovery story end to end, because "refused mail can come back" is a
    claim about three steps, not one.

    refused → operator fixes the list → rewind → ingested → rewind again →
    content dedup stops a second copy. A replay that double-ingested would be
    worse than no replay at all.
    """
    from papra_suite.store import Store

    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    org = store.get_org(org_id)
    raws = [(1, build_raw(message_id="<m1@x>", sender="Accounting <books@other.net>",
                         attachments=[("inv.pdf", b"%PDF-1")]))]

    async def poll(allow):
        session = FakeSession(raws=list(raws))
        monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
        client = StubClient()
        result = await poll_mailbox_once(store, client, store.get_mailbox(mailbox_id),
                                         org, allowlist=parse_allowlist(allow))
        return session, client, result

    # 1. refused: nothing uploaded, cursor moved past it
    _session, client, first = await poll("x.com")
    assert client.uploads == [] and first.senders_denied == 1
    assert store.get_mailbox(mailbox_id).last_seen_uid == 1

    # 2. the operator adds the sender and rewinds
    store.reset_mailbox_cursor(mailbox_id)

    # 3. ingested
    _session, client, second = await poll("x.com, other.net")
    assert [name for _org, name, _b in client.uploads] == ["inv.pdf"]
    assert second.senders_denied == 0

    # 4. a second replay must not create a second copy
    store.reset_mailbox_cursor(mailbox_id)
    _session, client, third = await poll("x.com, other.net")
    assert client.uploads == []
    assert third.documents_uploaded == 0
    assert store.get_mailbox(mailbox_id).last_seen_uid == 1


async def test_replay_refuses_to_move_the_cursor_forward(store):
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    store.record_poll_success(mailbox_id, last_seen_uid=50, uidvalidity="111")

    with pytest.raises(ValueError) as excinfo:
        store.reset_mailbox_cursor(mailbox_id, from_uid=60)

    assert "forward" in str(excinfo.value)
    assert store.get_mailbox(mailbox_id).last_seen_uid == 50


async def test_replay_re_enables_a_mailbox_disabled_by_repeated_failures(store):
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    for _ in range(3):
        store.record_poll_failure(mailbox_id, "unreadable fetch response")
    assert store.get_mailbox(mailbox_id).enabled is False

    store.reset_mailbox_cursor(mailbox_id)

    revived = store.get_mailbox(mailbox_id)
    assert revived.enabled is True
    assert revived.consecutive_failures == 0 and revived.last_error is None
