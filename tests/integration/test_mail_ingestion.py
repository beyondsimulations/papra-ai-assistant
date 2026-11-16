import pytest

from papra_suite.allowlist import parse_allowlist
from papra_suite.mail import poll_mailbox_once
from papra_suite.papra_client import PapraClient
from papra_suite.store import Store
from tests.integration.conftest import (
    GREENMAIL_HOST,
    GREENMAIL_IMAP_PORT,
    GREENMAIL_PASSWORD,
    inject_message,
    purge_greenmail_inbox,
)

# inject_message sends as sender@example.com (see conftest); this is the list an
# operator would write for their own forwarding address.
ALLOW = parse_allowlist("sender@example.com")

pytestmark = pytest.mark.integration

PDF_BYTES = b"%PDF-1.4 %" + b"body-of-a-minimal-but-plausible-pdf " * 8


@pytest.fixture
def ingestion_stack(tmp_path, papra_stack, greenmail):
    base_url, org_id, api_key = papra_stack
    gm_host, gm_imap, _, gm_user, gm_password = greenmail
    purge_greenmail_inbox()  # session-scoped GreenMail: isolate each test's INBOX
    store = Store(tmp_path / "state.db")
    store.migrate()
    local_org = store.create_org(papra_org_id=org_id, name="Integration Org")
    mailbox_id = store.create_mailbox(
        org_id=local_org, host=gm_host, port=gm_imap, username=gm_user,
        password=gm_password, ssl_mode="plain",
    )
    yield store, store.get_mailbox(mailbox_id), store.get_org(local_org), \
        PapraClient(base_url, api_key), org_id
    store.close()


async def test_pdf_mail_becomes_document_and_ocr_job(ingestion_stack):
    store, mailbox, org, client, papra_org_id = ingestion_stack
    inject_message(
        to=mailbox.username, subject="Rechnung 42",
        attachments=[("rechnung.pdf", PDF_BYTES, ("application", "pdf"))],
    )

    result = await poll_mailbox_once(store, client, mailbox, org, allowlist=ALLOW)

    assert result.error is None
    assert result.documents_uploaded == 1 and result.messages_failed == 0
    documents, _count = await client.list_documents(papra_org_id, search_query="rechnung")
    assert any(doc["name"] == "rechnung.pdf" for doc in documents)
    job = store.claim_next_job("ocr")
    assert job is not None and job.kind == "ocr"
    assert store.document_seen(papra_org_id, job.document_id)
    updated = store.get_mailbox(mailbox.id)
    assert updated.last_error is None and updated.last_seen_uid is not None

    # acceptance 3 (dedup): re-poll must not create a second document
    result2 = await poll_mailbox_once(store, client, store.get_mailbox(mailbox.id),
                                     org, allowlist=ALLOW)
    assert result2.documents_uploaded == 0
    documents_after, _ = await client.list_documents(papra_org_id, search_query="rechnung")
    assert len(documents_after) == len(documents)


async def test_non_pdf_mail_produces_no_document_and_no_error(ingestion_stack):
    store, mailbox, org, client, papra_org_id = ingestion_stack
    inject_message(
        to=mailbox.username, subject="Photos",
        attachments=[("photo.jpg", b"\xff\xd8 jpeg bytes", ("image", "jpeg"))],
    )

    result = await poll_mailbox_once(store, client, mailbox, org, allowlist=ALLOW)

    assert result.error is None
    assert result.documents_uploaded == 0
    assert result.attachments_skipped == 1
    documents, _ = await client.list_documents(papra_org_id)
    assert documents == []
    updated = store.get_mailbox(mailbox.id)
    assert updated.last_error is None and updated.enabled is True


async def test_delete_after_download_removes_message(ingestion_stack):
    store, mailbox, org, client, papra_org_id = ingestion_stack
    # enable delete for this test via a fresh mailbox row
    mailbox_id = store.create_mailbox(
        org_id=org.id, host=mailbox.host, port=mailbox.port, username=mailbox.username,
        password=mailbox.password, ssl_mode="plain", delete_after_download=True,
    )
    delete_mailbox = store.get_mailbox(mailbox_id)
    inject_message(
        to=mailbox.username, subject="To Delete",
        attachments=[("gone.pdf", PDF_BYTES, ("application", "pdf"))],
    )

    result = await poll_mailbox_once(store, client, delete_mailbox, org, allowlist=ALLOW)

    assert result.documents_uploaded == 1 and result.error is None
    import imaplib

    imap = imaplib.IMAP4(mailbox.host, mailbox.port, timeout=5)
    imap.login(mailbox.username, mailbox.password)
    imap.select("INBOX")
    status, data = imap.search(None, "ALL")
    assert data[0].split() == []          # message deleted
    imap.logout()


async def test_expunge_does_not_destroy_operator_deleted_mail(ingestion_stack):
    # C1 regression: a mailbox-wide EXPUNGE permanently destroys an
    # operator's flagged-but-unexpunged mail when an unrelated poll runs.
    # The operator flags message A \Deleted from their own client WITHOUT
    # expunging (their client expunges on folder close later); our poll of a
    # delete_after_download mailbox must remove ONLY our message B.
    store, mailbox, org, client, papra_org_id = ingestion_stack
    mailbox_id = store.create_mailbox(
        org_id=org.id, host=mailbox.host, port=mailbox.port, username=mailbox.username,
        password=mailbox.password, ssl_mode="plain", delete_after_download=True,
    )
    delete_mailbox = store.get_mailbox(mailbox_id)
    # A: jpg-only -> the poller never flags it (no PDFs -> no delete)
    inject_message(
        to=mailbox.username, subject="Operator keeps",
        attachments=[("photo.jpg", b"\xff\xd8 jpeg bytes", ("image", "jpeg"))],
    )
    # B: the PDF our delete_after_download mailbox will ingest and expunge
    inject_message(
        to=mailbox.username, subject="Suite ingests",
        attachments=[("ingested.pdf", PDF_BYTES, ("application", "pdf"))],
    )

    import imaplib

    imap = imaplib.IMAP4(mailbox.host, mailbox.port, timeout=5)
    imap.login(mailbox.username, mailbox.password)
    imap.select("INBOX")
    _status, data = imap.uid("SEARCH", None, "ALL")
    uids = sorted(int(u) for u in data[0].split())
    assert len(uids) == 2
    # operator convention: flag A \Deleted WITHOUT expunging
    imap.uid("STORE", str(uids[0]), "+FLAGS", "(\\Deleted)")
    imap.logout()

    result = await poll_mailbox_once(store, client, delete_mailbox, org, allowlist=ALLOW)

    assert result.error is None and result.documents_uploaded == 1

    imap = imaplib.IMAP4(mailbox.host, mailbox.port, timeout=5)
    imap.login(mailbox.username, mailbox.password)
    imap.select("INBOX")
    _status, data = imap.uid("SEARCH", None, "ALL")
    remaining = sorted(int(u) for u in data[0].split())
    imap.logout()
    # A (operator-flagged) must STILL EXIST; B (ours) is gone
    assert remaining == [uids[0]]


async def test_resent_document_409_is_swallowed(ingestion_stack):
    store, mailbox, org, client, papra_org_id = ingestion_stack
    inject_message(
        to=mailbox.username, subject="Rechnung 42",
        attachments=[("rechnung.pdf", PDF_BYTES, ("application", "pdf"))],
    )
    result = await poll_mailbox_once(store, client, mailbox, org, allowlist=ALLOW)
    assert result.documents_uploaded == 1 and result.messages_failed == 0

    # same PDF bytes, different subject -> new message-id -> passes our
    # dedup hash, but papra 409s on its per-org content hash
    inject_message(
        to=mailbox.username, subject="FW: Rechnung 42 (resent)",
        attachments=[("rechnung.pdf", PDF_BYTES, ("application", "pdf"))],
    )
    second = await poll_mailbox_once(store, client, store.get_mailbox(mailbox.id),
                                     org, allowlist=ALLOW)

    assert second.messages_failed == 0 and second.error is None

    # the resent message is marked seen: a third poll finds nothing new
    third = await poll_mailbox_once(store, client, store.get_mailbox(mailbox.id),
                                    org, allowlist=ALLOW)
    assert third.messages_scanned == 0 and third.messages_failed == 0

    updated = store.get_mailbox(mailbox.id)
    assert updated.enabled is True and updated.last_error is None
    assert updated.consecutive_failures == 0


async def test_wrong_password_disables_mailbox_after_three_polls(ingestion_stack):
    store, mailbox, org, client, _ = ingestion_stack
    mailbox_id = store.create_mailbox(
        org_id=org.id, host=mailbox.host, port=mailbox.port, username=mailbox.username,
        password="wrong-password", ssl_mode="plain",
    )
    bad_mailbox = store.get_mailbox(mailbox_id)

    for _ in range(3):
        result = await poll_mailbox_once(store, client, bad_mailbox, org, allowlist=ALLOW)

    assert result.error is not None
    updated = store.get_mailbox(mailbox_id)
    assert updated.enabled is False and updated.consecutive_failures == 3


async def test_a_stranger_is_refused_and_replay_after_fixing_the_list_ingests_once(
    ingestion_stack
):
    """The whole design, against a real IMAP server instead of a fake.

    refused → nothing uploaded, mail retired, mailbox healthy → the operator
    widens the list and replays → ingested → replay again → no second copy.
    """
    store, mailbox, org, client, papra_org_id = ingestion_stack
    inject_message(
        to=mailbox.username, subject="Rechnung 99", sender="stranger@notallowed.test",
        attachments=[("rechnung99.pdf", PDF_BYTES, ("application", "pdf"))],
    )

    result = await poll_mailbox_once(store, client, mailbox, org, allowlist=ALLOW)

    assert result.error is None                     # a refusal is not an outage
    assert result.senders_denied == 1 and result.documents_uploaded == 0
    documents, _ = await client.list_documents(papra_org_id, search_query="rechnung99")
    assert documents == []
    after_refusal = store.get_mailbox(mailbox.id)
    assert after_refusal.last_seen_uid is not None  # retired, not retried forever
    assert after_refusal.enabled is True and after_refusal.consecutive_failures == 0

    # the operator adds the sender and rewinds the cursor
    store.reset_mailbox_cursor(mailbox.id)
    widened = parse_allowlist("sender@example.com, notallowed.test")

    replayed = await poll_mailbox_once(store, client, store.get_mailbox(mailbox.id),
                                       org, allowlist=widened)
    assert replayed.documents_uploaded == 1
    documents, _ = await client.list_documents(papra_org_id, search_query="rechnung99")
    assert any(doc["name"] == "rechnung99.pdf" for doc in documents)

    # and a second replay must not create a duplicate
    store.reset_mailbox_cursor(mailbox.id)
    again = await poll_mailbox_once(store, client, store.get_mailbox(mailbox.id), org,
                                    allowlist=widened)
    assert again.documents_uploaded == 0
    documents, _ = await client.list_documents(papra_org_id, search_query="rechnung99")
    assert len(documents) == 1


async def test_an_empty_allow_list_pauses_without_touching_the_inbox(
    ingestion_stack
):
    """Against a real server: paused means the message is still there, unread,
    for the next poll after the operator fixes the configuration."""
    import imaplib

    store, mailbox, org, client, _org_id = ingestion_stack
    inject_message(to=mailbox.username, subject="Rechnung 100",
                   attachments=[("rechnung100.pdf", PDF_BYTES, ("application", "pdf"))])

    result = await poll_mailbox_once(store, client, mailbox, org,
                                     allowlist=parse_allowlist(""))

    assert result.ingestion_paused is True and result.error is None
    assert store.get_mailbox(mailbox.id).last_seen_uid is None

    imap = imaplib.IMAP4(GREENMAIL_HOST, GREENMAIL_IMAP_PORT, timeout=10)
    try:
        imap.login(mailbox.username, mailbox.password)
        imap.select("INBOX")
        _status, unseen = imap.uid("SEARCH", None, "UNSEEN")
        assert len(unseen[0].split()) == 1          # not fetched, not marked read
    finally:
        imap.logout()


async def test_every_skipped_outcome_is_recorded_against_a_real_server(
        ingestion_stack):
    """The event rows are only worth having if they are ACTIONABLE: they quote a
    uid and a sender from a real server, not from a fake double built to agree.

    So the assertion is not "a row exists". It is that the uid in the
    sender_denied row still names the refused message on the IMAP server — the
    exact lookup an operator would perform — and that the address in the row is
    the address that was refused.
    """
    import imaplib

    store, mailbox, org, client, _ = ingestion_stack
    inject_message(
        subject="invoice with a zip", to="ingest@localhost",
        sender="sender@example.com",
        attachments=[("invoice.pdf", PDF_BYTES, ("application", "pdf")),
                     ("detail.zip", b"PK\x03\x04zip-bytes", ("application", "zip"))],
    )
    inject_message(
        subject="newsletter nobody asked for", to="ingest@localhost",
        sender="stranger@notallowed.test",
        attachments=[("issue.pdf", PDF_BYTES + b"-other", ("application", "pdf"))],
    )

    await poll_mailbox_once(store, client, mailbox, org, allowlist=ALLOW)

    by_kind = {event.kind: event for event in store.list_ingest_events()}
    assert set(by_kind) == {"attachment_unsupported", "sender_denied"}

    skipped = by_kind["attachment_unsupported"]
    assert skipped.filename == "detail.zip"
    assert "sender@example.com" in (skipped.sender or "")

    denied = by_kind["sender_denied"]
    assert "stranger@notallowed.test" in (denied.sender or "")
    assert denied.uidvalidity == store.get_mailbox(mailbox.id).uidvalidity

    # the row's uid must still name that message on the server
    imap = imaplib.IMAP4(GREENMAIL_HOST, GREENMAIL_IMAP_PORT, timeout=5)
    try:
        imap.login("ingest@localhost", GREENMAIL_PASSWORD)
        imap.select("INBOX")
        status, data = imap.uid("FETCH", str(denied.uid), "(BODY.PEEK[HEADER])")
        assert status == "OK"
        # Same shape rule the production client uses: the literal rides in a
        # tuple item, and servers differ about the items around it. Joining the
        # literals is what keeps this assertion about the message, not about
        # GreenMail's response layout.
        header = b"".join(
            item[1] for item in data
            if isinstance(item, tuple) and len(item) >= 2
            and isinstance(item[1], bytes)
        )
        assert b"stranger@notallowed.test" in header
        assert b"newsletter nobody asked for" in header
    finally:
        imap.logout()


async def test_a_refused_sender_records_one_row_and_is_not_refused_again(
        ingestion_stack):
    """Cursor semantics for the new rows, against a real server: one row, and the
    second poll decides nothing new. A second row would mean the refused uid is
    being re-fetched every poll — the bug the header-stage refusal exists to
    prevent, now visible in the table instead of only in the log."""
    store, mailbox, org, client, _ = ingestion_stack
    inject_message(
        subject="junk", to="ingest@localhost", sender="stranger@notallowed.test",
        attachments=[("junk.pdf", PDF_BYTES + b"-junk", ("application", "pdf"))],
    )

    first = await poll_mailbox_once(store, client, mailbox, org, allowlist=ALLOW)
    second = await poll_mailbox_once(store, client, store.get_mailbox(mailbox.id),
                                     org, allowlist=ALLOW)

    assert first.senders_denied == 1 and second.messages_scanned == 0
    assert len(store.list_ingest_events()) == 1
