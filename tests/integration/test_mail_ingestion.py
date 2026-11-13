import pytest

from papra_suite.mail import poll_mailbox_once
from papra_suite.papra_client import PapraClient
from papra_suite.store import Store
from tests.integration.conftest import inject_message, purge_greenmail_inbox

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

    result = await poll_mailbox_once(store, client, mailbox, org)

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
    result2 = await poll_mailbox_once(store, client, store.get_mailbox(mailbox.id), org)
    assert result2.documents_uploaded == 0
    documents_after, _ = await client.list_documents(papra_org_id, search_query="rechnung")
    assert len(documents_after) == len(documents)


async def test_non_pdf_mail_produces_no_document_and_no_error(ingestion_stack):
    store, mailbox, org, client, papra_org_id = ingestion_stack
    inject_message(
        to=mailbox.username, subject="Photos",
        attachments=[("photo.jpg", b"\xff\xd8 jpeg bytes", ("image", "jpeg"))],
    )

    result = await poll_mailbox_once(store, client, mailbox, org)

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

    result = await poll_mailbox_once(store, client, delete_mailbox, org)

    assert result.documents_uploaded == 1 and result.error is None
    import imaplib

    imap = imaplib.IMAP4(mailbox.host, mailbox.port)
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

    imap = imaplib.IMAP4(mailbox.host, mailbox.port)
    imap.login(mailbox.username, mailbox.password)
    imap.select("INBOX")
    _status, data = imap.uid("SEARCH", None, "ALL")
    uids = sorted(int(u) for u in data[0].split())
    assert len(uids) == 2
    # operator convention: flag A \Deleted WITHOUT expunging
    imap.uid("STORE", str(uids[0]), "+FLAGS", "(\\Deleted)")
    imap.logout()

    result = await poll_mailbox_once(store, client, delete_mailbox, org)

    assert result.error is None and result.documents_uploaded == 1

    imap = imaplib.IMAP4(mailbox.host, mailbox.port)
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
    result = await poll_mailbox_once(store, client, mailbox, org)
    assert result.documents_uploaded == 1 and result.messages_failed == 0

    # same PDF bytes, different subject -> new message-id -> passes our
    # dedup hash, but papra 409s on its per-org content hash
    inject_message(
        to=mailbox.username, subject="FW: Rechnung 42 (resent)",
        attachments=[("rechnung.pdf", PDF_BYTES, ("application", "pdf"))],
    )
    second = await poll_mailbox_once(store, client, store.get_mailbox(mailbox.id), org)

    assert second.messages_failed == 0 and second.error is None

    # the resent message is marked seen: a third poll finds nothing new
    third = await poll_mailbox_once(store, client, store.get_mailbox(mailbox.id), org)
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
        result = await poll_mailbox_once(store, client, bad_mailbox, org)

    assert result.error is not None
    updated = store.get_mailbox(mailbox_id)
    assert updated.enabled is False and updated.consecutive_failures == 3
