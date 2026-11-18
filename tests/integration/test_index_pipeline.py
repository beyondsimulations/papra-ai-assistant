"""End-to-end index against the real Papra and a real Kreuzakt.

Papra's bytes go in, a Kreuzakt id comes back, the ledger records which index that
integer came from — and Papra's own record is byte-identical afterwards, which is
the whole point of this phase: the bridge can be wrong here and the record still
cannot be hurt.

What is asserted is structure: ids, statuses, ledger rows, generations, and the
bytes Papra still holds. Extraction *wording* is never asserted — that is a model's
behaviour, and a test that pinned a sentence would blame our code for a prompt
change. The one content claim made below is "not empty", plus a marker that was put
in the PDF's own text layer, which is a property of the file rather than of a
decoder.

Kreuzakt's URL comes from `PAPRA_KREUZAKT_URL` (the test compose publishes it on
127.0.0.1:12400, the only place it is ever published). When it is not there the
tests skip, unless `PAPRA_REQUIRE_STACK=1` says the stack is supposed to be up —
then an absent index is a failure, not a green run that tested nothing.
"""

import asyncio
import os
import time
from dataclasses import dataclass

import fitz
import httpx
import pytest

from papra_suite.allowlist import parse_allowlist
from papra_suite.indexing import index_upload_name, run_index_job
from papra_suite.kreuzakt_client import KreuzaktClient
from papra_suite.mail import poll_mailbox_once
from papra_suite.papra_client import PapraClient
from papra_suite.pipeline import enqueue_index_job, sweep_once
from papra_suite.store import Store
from tests.integration.conftest import _skip_or_fail, inject_message, purge_greenmail_inbox

pytestmark = pytest.mark.integration

KREUZAKT_URL = os.environ.get("PAPRA_KREUZAKT_URL", "http://127.0.0.1:12400")
# One marker in the PDF's own text layer. Extraction may phrase a document any way
# it likes; it cannot invent this string without reading the file it was given.
TEXT_LAYER_MARKER = "papra-index-bridge-marker-8f2c1a"
ALLOW = parse_allowlist("sender@example.com")


def _kreuzakt_is_up() -> bool:
    try:
        return httpx.get(f"{KREUZAKT_URL}/api/health", timeout=2.0).status_code == 200
    except httpx.TransportError:
        return False


class CountingPapra(PapraClient):
    """Papra, with every write this bridge attempts recorded.

    The record comparison below says the record did not change. This says something
    stronger and independent of Papra's timing: that we never asked for a change. Both
    are needed, because Papra fills a PDF's text layer in by itself and a changed
    record is therefore not by itself evidence of a write by us.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.patches: list[tuple[str, dict]] = []

    async def update_document(self, org_id, document_id, *, name=None, content=None,
                              tag_ids=None):
        self.patches.append((document_id, {"name": name, "content": content,
                                           "tag_ids": tag_ids}))
        return await super().update_document(org_id, document_id, name=name,
                                             content=content, tag_ids=tag_ids)


@dataclass
class Stack:
    store: Store
    papra: CountingPapra
    kreuzakt: KreuzaktClient
    org_id: str


def make_pdf(marker: str = TEXT_LAYER_MARKER) -> bytes:
    """A one-page PDF with a real text layer, so extraction has something to find."""
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((72, 72), "Rechnung Nr. 4711")
    page.insert_text((72, 110), f"Referenz: {marker}")
    data = pdf.tobytes()
    pdf.close()
    return data


@pytest.fixture
async def stack(tmp_path, papra_stack):
    base_url, org_id, api_key = papra_stack
    if not _kreuzakt_is_up():
        _skip_or_fail(f"Kreuzakt not reachable at {KREUZAKT_URL}; bring the bridge stack up")
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=org_id, name="Index Org")
    papra = CountingPapra(base_url, api_key)
    kreuzakt = KreuzaktClient(KREUZAKT_URL)
    yield Stack(store=store, papra=papra, kreuzakt=kreuzakt, org_id=org_id)
    await papra.aclose()
    await kreuzakt.aclose()
    store.close()


async def _papra_record(stack: Stack, document_id: str) -> dict:
    """Everything this bridge could write back, in one snapshot to diff against."""
    document = await stack.papra.get_document(stack.org_id, document_id)
    return {"name": document.get("name"), "content": document.get("content"),
            "description": document.get("description"),
            "tags": sorted(tag.get("name", "") for tag in (document.get("tags") or []))}


async def _papra_record_after_extraction(stack: Stack, document_id: str, *,
                                        timeout_s: float = 60.0) -> dict:
    """The baseline is Papra's record once PAPRA has finished writing it.

    Measured here: a freshly uploaded PDF answers `content: ""` and a moment later the
    same field carries its text layer — Papra extracted it, nobody PATCHed it. So the
    completion condition is Papra's own observable output (the text layer has landed),
    not a timer and not two reads that happen to agree. Once it has, the record must
    stop moving, or the bridge and Papra are being told apart by luck.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        document = await stack.papra.get_document(stack.org_id, document_id)
        if (document.get("content") or "").strip():
            break
        await asyncio.sleep(0.5)
    else:
        raise AssertionError(
            f"papra never extracted a text layer for {document_id} within {timeout_s}s; "
            "the 'untouched' baseline below would be measured against nothing")
    record = await _papra_record(stack, document_id)
    await asyncio.sleep(1.0)
    later = await _papra_record(stack, document_id)
    assert later == record, f"papra was still writing {document_id}: {record} -> {later}"
    return record


async def test_uploaded_pdf_is_indexed_without_touching_papra_content(stack):
    """A Papra-native upload: discovered by the sweep, indexed, record untouched."""
    pdf = make_pdf()
    document = await stack.papra.create_document(stack.org_id, "rechnung-4711.pdf", pdf)
    document_id = document["id"]
    before = await _papra_record_after_extraction(stack, document_id)

    result = await sweep_once(stack.store, stack.papra)
    assert (result.documents_checked, result.jobs_enqueued) == (1, 1)

    job = stack.store.claim_next_job("index")
    assert job is not None and job.document_id == document_id
    outcome = await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                                  poll_s=0.5, wait_timeout_s=90)
    stack.store.mark_job_done(job.id)

    ref = stack.store.ledger_get(stack.org_id, document_id)
    assert ref.kreuzakt_id == outcome.kreuzakt_id
    assert ref.delivered_generation == stack.store.get_generation()
    assert ref.papra_original_name == "rechnung-4711.pdf"   # the baseline, from Papra
    assert stack.store.ledger_needs_index(generation=stack.store.get_generation()) == []

    kz_document = await stack.kreuzakt.get_document(ref.kreuzakt_id)
    assert kz_document.content.strip(), "a PDF with a text layer must not extract empty"
    assert outcome.content_len == len(kz_document.content)
    assert not outcome.empty_content
    assert outcome.delivered is False                       # this phase writes nothing

    assert await _papra_record(stack, document_id) == before
    assert stack.papra.patches == []           # and we never even asked


async def test_the_upload_name_is_the_name_the_queue_reports(stack):
    """The synthetic name survived the server: same name back, or correlation could not work."""
    pdf = make_pdf()
    document = await stack.papra.create_document(stack.org_id, "nametest.pdf", pdf)
    document_id = document["id"]

    enqueue_index_job(stack.store, papra_org_id=stack.org_id, document_id=document_id,
                      file_hash=document["originalSha256Hash"] or "no-hash",
                      name=document["name"], mime="application/pdf",
                      ingested_via="sweep")
    job = stack.store.claim_next_job("index")
    outcome = await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                                  poll_s=0.5, wait_timeout_s=90)

    expected = index_upload_name(job)
    rows = await stack.kreuzakt.queue_page(limit=20)
    # Exactly one row of ours, byte-identical to the name we asked for: a sanitised
    # name would make this row unmatchable and the job would report absence.
    ours = [row for row in rows if row.filename == expected]
    assert len(ours) >= 1 and ours[0].status == "completed"
    assert ours[0].document_id == outcome.kreuzakt_id


async def test_a_bumped_generation_rebinds_from_papra_bytes(stack):
    """Rebuild semantics on the real server: an old id is not a fact any more.

    The eligibility half, the binding half, and one measured limitation are all
    asserted here, because the limitation is the reason the binding half is driven by
    hand: a backfill cannot re-enqueue this document through `enqueue_index_job`.
    """
    pdf = make_pdf()
    document = await stack.papra.create_document(stack.org_id, "rebuild.pdf", pdf)
    document_id = document["id"]
    enqueue_index_job(stack.store, papra_org_id=stack.org_id, document_id=document_id,
                      file_hash=document["originalSha256Hash"] or "no-hash",
                      name=document["name"], mime="application/pdf",
                      ingested_via="sweep")
    job = stack.store.claim_next_job("index")
    first = await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                                poll_s=0.5, wait_timeout_s=90)
    stack.store.mark_job_done(job.id)           # the worker's own completion step

    stack.store.bump_generation()
    assert [ref.papra_document_id
            for ref in stack.store.ledger_needs_index(generation=2)] == [document_id]

    # Measured, and reported rather than papered over: the job queue's idempotency
    # index is (kind, org, document, hash), so enqueueing this document's same bytes
    # again returns the finished job's id and there is nothing to claim. Tasks 11 and
    # 15 send rebuild work through this same call, so that is where the reopen belongs.
    assert enqueue_index_job(stack.store, papra_org_id=stack.org_id,
                             document_id=document_id, file_hash=job.content_hash,
                             name=document["name"], mime="application/pdf",
                             ingested_via="backfill") == job.id
    assert stack.store.claim_next_job("index") is None

    # The handler's half of a rebuild, driven with the row a re-claim would hand it.
    second = await run_index_job(stack.kreuzakt, stack.store, stack.store.get_job(job.id),
                                 papra=stack.papra, poll_s=0.5, wait_timeout_s=90)

    ref = stack.store.ledger_get(stack.org_id, document_id)
    assert ref.delivered_generation == 2
    assert second.kreuzakt_id == first.kreuzakt_id   # identical bytes deduplicate onto one id
    assert stack.papra.patches == []


@pytest.fixture
async def mail_stack(tmp_path, papra_stack, greenmail):
    """The intake address path: a mail attachment that becomes a Papra document."""
    base_url, org_id, api_key = papra_stack
    if not _kreuzakt_is_up():
        _skip_or_fail(f"Kreuzakt not reachable at {KREUZAKT_URL}; bring the bridge stack up")
    gm_host, gm_imap, _, gm_user, gm_password = greenmail
    purge_greenmail_inbox()
    store = Store(tmp_path / "state.db")
    store.migrate()
    local_org = store.create_org(papra_org_id=org_id, name="Index Org")
    mailbox_id = store.create_mailbox(org_id=local_org, host=gm_host, port=gm_imap,
                                      username=gm_user, password=gm_password,
                                      ssl_mode="plain")
    papra = CountingPapra(base_url, api_key)
    kreuzakt = KreuzaktClient(KREUZAKT_URL)
    yield (Stack(store=store, papra=papra, kreuzakt=kreuzakt, org_id=org_id),
           store.get_mailbox(mailbox_id), store.get_org(local_org))
    await papra.aclose()
    await kreuzakt.aclose()
    store.close()


async def test_mail_attachment_is_indexed_without_touching_papra_content(mail_stack):
    """An attachment that arrives by mail is indexed with the same code and the same silence.

    `enqueue_index_job` is called here rather than by `mail.py`: no task in the plan
    switches the mail intake's call site, so this is the intake path exercised
    directly instead of wired implicitly.
    """
    stack, mailbox, org = mail_stack
    inject_message(to=mailbox.username, subject="Rechnung 4711",
                   attachments=[("rechnung-4711.pdf", make_pdf(),
                                 ("application", "pdf"))])
    result = await poll_mailbox_once(stack.store, stack.papra, mailbox, org,
                                     allowlist=ALLOW)
    assert result.documents_uploaded == 1
    documents, _count = await stack.papra.list_documents(stack.org_id,
                                                         search_query="rechnung-4711")
    document_id = documents[0]["id"]
    before = await _papra_record_after_extraction(stack, document_id)

    enqueue_index_job(stack.store, papra_org_id=stack.org_id,
                            document_id=document_id,
                            file_hash=documents[0]["originalSha256Hash"] or "no-hash",
                            name=documents[0]["name"], mime="application/pdf",
                            ingested_via="mail")
    job = stack.store.claim_next_job("index")
    started = time.monotonic()
    outcome = await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                                  poll_s=0.5, wait_timeout_s=90)

    ref = stack.store.ledger_get(stack.org_id, document_id)
    assert ref.kreuzakt_id == outcome.kreuzakt_id
    assert ref.delivered_generation == stack.store.get_generation()
    assert (await stack.kreuzakt.get_document(ref.kreuzakt_id)).content.strip()
    assert await _papra_record(stack, document_id) == before
    assert stack.papra.patches == []
    assert time.monotonic() - started < 90      # the wait we waited is the wait we set


# --------------------------------------------------------------------------------
# Two tracked regressions, written as the requirement rather than as today's behaviour.
#
# `strict=True` is what makes them tracked rather than decorative: the suite FAILS when
# either starts passing, so a fix cannot land quietly and a marker cannot outlive its
# reason. Both are the acceptance edges the plan says this pipeline must reach; Task 6's
# report names an owner for each (§6.1 mail intake, §6.2 rebuild re-enqueue).
# --------------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=True,
    reason="mail intake still enqueues through enqueue_ocr_job, which marks the document "
           "seen in the same call; the sweep skips seen documents, so a mail-ingested PDF "
           "gets no ledger row and no index job. Owner: whoever switches mail.py's call "
           "site (report §6.1).",
)
async def test_mail_intake_itself_creates_the_ledger_row_and_the_index_job(mail_stack):
    """Spec §4.2's claim about intake email, driven the way mail actually arrives.

    The point is that nothing here calls `enqueue_index_job`: the requirement is about
    `poll_mailbox_once`, which is the only thing the intake address runs.
    """
    stack, mailbox, org = mail_stack
    inject_message(to=mailbox.username, subject="Rechnung 5000",
                   attachments=[("rechnung-5000.pdf", make_pdf(),
                                 ("application", "pdf"))])
    result = await poll_mailbox_once(stack.store, stack.papra, mailbox, org,
                                     allowlist=ALLOW)
    assert result.documents_uploaded == 1

    documents, _count = await stack.papra.list_documents(stack.org_id,
                                                         search_query="rechnung-5000")
    document_id = documents[0]["id"]
    kinds = [job.kind for job in stack.store.list_jobs()
             if job.document_id == document_id]
    assert "index" in kinds, kinds
    assert stack.store.ledger_get(stack.org_id, document_id) is not None
    assert stack.papra.patches == []


@pytest.mark.xfail(
    strict=True,
    reason="a finished index job cannot be scheduled again: jobs are idempotent on "
           "(kind, org, document, content_hash), so a backfill enqueue returns the "
           "finished row's id and nothing is claimable. Needs a Store-side decision "
           "(reopen, retire, or key the job on the generation); report §6.2, Tasks 11 "
           "and 15 depend on it.",
)
async def test_a_rebuild_can_schedule_work_for_an_already_indexed_document(stack):
    """What a backfill pass calls, and what it has to get back: a job it can claim."""
    pdf = make_pdf()
    document = await stack.papra.create_document(stack.org_id, "reschedule.pdf", pdf)
    document_id = document["id"]
    enqueue_index_job(stack.store, papra_org_id=stack.org_id, document_id=document_id,
                      file_hash=document["originalSha256Hash"] or "no-hash",
                      name=document["name"], mime="application/pdf",
                      ingested_via="sweep")
    job = stack.store.claim_next_job("index")
    await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                        poll_s=0.5, wait_timeout_s=90)
    stack.store.mark_job_done(job.id)
    stack.store.bump_generation()

    enqueue_index_job(stack.store, papra_org_id=stack.org_id, document_id=document_id,
                      file_hash=job.content_hash, name=document["name"],
                      mime="application/pdf", ingested_via="backfill")
    assert stack.store.claim_next_job("index") is not None
    assert stack.papra.patches == []
