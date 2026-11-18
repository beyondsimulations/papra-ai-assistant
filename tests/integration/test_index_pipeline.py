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
import hashlib
import os
import time
from dataclasses import dataclass

import fitz
import httpx
import pytest

from papra_suite.allowlist import parse_allowlist
from papra_suite.indexing import (
    SKIPPED_EMPTY,
    deliver_content,
    index_upload_name,
    run_index_job,
    verify_once,
)
from papra_suite.kreuzakt_client import KreuzaktClient, KzDocument
from papra_suite.ledger_cli import rebuild_index
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

    From Task 7 the stack suite has a writer, so the record is also the evidence for the
    opposite claim — that the ONLY thing a content delivery sends is `content`. Fields
    left as None are not recorded as patches, because the client omits them from the body:
    what is asserted is the body, which is the thing Papra acts on.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.patches: list[tuple[str, dict]] = []

    async def update_document(self, org_id, document_id, *, name=None, content=None):
        given = {"name": name, "content": content}
        self.patches.append((document_id, {k: v for k, v in given.items()
                                          if v is not None}))
        return await super().update_document(org_id, document_id, name=name, content=content)


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
    # The store holds one org and that org is created for this test, so "one
    # document" here is ours by construction rather than by a pristine volume — the
    # count is a statement about this document either way. What the pass WROTE is
    # asserted separately, scoped to this document, because one PDF is enrolled in two
    # queues while the legacy OCR writer is still switched on for it.
    assert (result.documents_checked, result.documents_enqueued) == (1, 1)
    written = sorted(job.kind for job in stack.store.list_jobs()
                     if job.document_id == document_id)
    assert written == ["index", "ocr"]

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
    # At least one row of ours whose name is byte-identical to the one we asked for: a
    # sanitised name would be unmatchable, and the job would report an absent document.
    # More than one is possible — a re-upload is a second row under the same name — so
    # this reads the newest rather than assuming there is only one.
    ours = [row for row in rows if row.filename == expected]
    assert len(ours) >= 1 and ours[0].status == "completed"
    assert ours[0].document_id == outcome.kreuzakt_id


async def test_a_bumped_generation_rebinds_from_papra_bytes(stack):
    """Rebuild semantics on the real server: an old id is not a fact any more.

    The eligibility half and the binding half are both asserted here. The binding half is
    driven by hand — a fresh `enqueue_index_job` for the same bytes, then the job — because
    the point here is what the *index* does with a stale id, not how the request gets
    scheduled. `test_a_rebuild_can_schedule_work_for_an_already_indexed_document` covers
    the scheduling half; `test_a_reconcile_rebuilds_every_binding_from_papra_bytes` drives
    both through the path an operator runs.
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

    The gate is shut here, which is the pre-handover configuration, so indexing runs and
    Papra's record stays byte-for-byte as Papra left it. The call site is exercised
    directly because the writer switch decides which path intake takes; the switched path
    is the test below it.
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
    outcome = await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                                  poll_s=0.5, wait_timeout_s=90)

    ref = stack.store.ledger_get(stack.org_id, document_id)
    assert ref.kreuzakt_id == outcome.kreuzakt_id
    assert ref.delivered_generation == stack.store.get_generation()
    assert (await stack.kreuzakt.get_document(ref.kreuzakt_id)).content.strip()
    assert await _papra_record(stack, document_id) == before
    assert stack.papra.patches == []


# --------------------------------------------------------------------------------
# One tracked regression, written as the requirement rather than as today's behaviour.
#
# `strict=True` is what makes it tracked rather than decorative: the suite FAILS when it
# starts passing, so a fix cannot land quietly and a marker cannot outlive its reason. The
# mail-intake handover that used to sit here was tracked the same way and was fixed by
# Task 7, which removed the marker in the same commit that flipped it and left
# `test_mail_intake_itself_creates_the_ledger_row_and_the_index_job` as the live proof.
# --------------------------------------------------------------------------------

async def test_mail_intake_itself_creates_the_ledger_row_and_the_index_job(mail_stack):
    """Spec §4.2's claim about intake email, driven the way mail actually arrives.

    The point is that nothing here calls `enqueue_index_job`: the requirement is about
    `poll_mailbox_once`, which is the only thing the intake address runs. The one thing the
    test does set is the writer switch, because intake's path is chosen by it: with
    `legacy_workers` off — the handover state, and the only state in which Papra's text is
    ours to write — a mailed PDF must produce the ledger reference and the index job.
    Before the flip it still enqueues OCR and stays out of the ledger, which
    `test_mail_intake_keeps_the_legacy_enqueue_while_the_writers_are_on` holds in place so
    the handover cannot be made by deleting the old path.
    """
    stack, mailbox, org = mail_stack
    stack.store.set_setting("legacy_workers", "false")
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
    assert "ocr" not in kinds, kinds        # nothing new enters the drained queue
    assert stack.store.ledger_get(stack.org_id, document_id) is not None
    assert stack.papra.patches == []


# --------------------------------------------------------------------------------
# Task 7: content delivery against the real Papra and the real index.
#
# Assertions stay structural. What Kreuzakt's model writes for a given scan is its own
# business — the marker in the PDF's text layer is here because a test may assert that a
# document is findable, never that a sentence was phrased a particular way — so every
# content claim below is "Papra's column equals what the index holds for that document",
# which is the transport property the design actually promises.
#
# The blank-page rule is tested here too, against a real document, and NOT by asking
# Kreuzakt to extract nothing. That distinction matters: the half of the rule that depends
# on a model returning an empty string for a text-less page is Kreuzakt's behaviour on a
# pinned image where it was never measured, and asserting it would make a stack test depend
# on a decoder we do not own. The half this task owns is ours and needs no model at all:
# given empty index-side text and a real Papra record that holds text, no PATCH leaves. So
# the document below is real and the empty text is fabricated — `deliver_content` takes the
# index's text as an argument, which is exactly the seam the rule is written at.
# --------------------------------------------------------------------------------

@pytest.fixture
async def delivered(stack):
    """One document Papra holds, the index holds, and the bridge has delivered once."""
    pdf = make_pdf()
    document = await stack.papra.create_document(stack.org_id, "rechnung-4711.pdf", pdf)
    document_id = document["id"]
    # Papra's own extraction has to have landed first: delivering into a column that is
    # still empty would prove nothing about overwriting anyone.
    before = await _papra_record_after_extraction(stack, document_id)
    assert (before["content"] or "").strip()

    stack.store.set_setting("index_content_delivery", "true")
    # Hashed from the bytes we uploaded rather than read back from the record: Papra's
    # `originalSha256Hash` is the sha256 of the file, and `run_index_job`'s download check
    # verifies that independently, so a disagreement here fails the job loudly.
    enqueue_index_job(stack.store, papra_org_id=stack.org_id, document_id=document_id,
                      file_hash=hashlib.sha256(pdf).hexdigest(),
                      name=document["name"], mime="application/pdf",
                      ingested_via="sweep")
    job = stack.store.claim_next_job("index")
    outcome = await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                                 poll_s=0.5, wait_timeout_s=90)
    stack.store.mark_job_done(job.id)
    ref = stack.store.ledger_get(stack.org_id, document_id)
    kz_doc = await stack.kreuzakt.get_document(ref.kreuzakt_id)
    # The delivery's own writes, handed out before the list is cleared: what a test asserts
    # afterwards is then unambiguously "what this pass did", not "what anyone did".
    delivery_patches = list(stack.papra.patches)
    stack.papra.patches.clear()
    return {"document_id": document_id, "ref": ref, "outcome": outcome,
            "kz_content": kz_doc.content, "papra_text": before["content"],
            "delivery_patches": delivery_patches}


async def test_a_delivery_replaces_papras_text_with_the_indexs(delivered, stack):
    """The phase-two promise, end to end: Papra's `content` becomes the index's text.
    `TEXT_LAYER_MARKER` is in both texts, so this is not a test that Papra's extraction is
    good; it is a test that the bytes Kreuzakt holds travelled into Papra intact, and that
    the ledger's `delivered_sha` describes exactly those bytes rather than something else
    the bridge happens to know.
    """
    assert delivered["outcome"].delivered is True
    document = await stack.papra.get_document(stack.org_id, delivered["document_id"])
    assert document["content"] == delivered["kz_content"]
    assert delivered["ref"].delivered_sha == hashlib.sha256(
        delivered["kz_content"].encode()).hexdigest()
    assert delivered["ref"].delivered_generation == stack.store.get_generation()
    # Content only: a delivery that also renamed the document would be Task 8's failure
    # mode, arriving as a surprise in Task 7.
    assert delivered["delivery_patches"] == [(delivered["document_id"],
                                              {"content": delivered["kz_content"]})]


async def test_an_empty_extraction_leaves_a_real_papra_record_alone(delivered, stack):
    """The blank-page rule against a real record: no PATCH, and the column byte-identical.

    The text handed to the writer is fabricated empty on purpose. Asking Kreuzakt to
    produce nothing needs a page this configuration extracts to nothing, which has never
    been measured on the pinned image — that half of the rule belongs to a model, and a
    stack test may not depend on it. What this task owns is entirely ours and needs no
    model: with empty index-side text and a Papra document that really holds text, the
    writer must refuse, must not even ask, and must leave Papra's bytes exactly as they
    were. The guard is the one thing between a blank scan and a year of a document's text,
    so it is proven against the real service and not only against a fake.

    Two blanks are run, `""` and whitespace-only, because the classification is one function
    over both and the second is the case a scanned page actually produces.
    """
    held = (await _papra_record(stack, delivered["document_id"]))["content"]
    assert held == delivered["kz_content"]     # the fixture's delivery is what is there

    for number, blank_text in enumerate(("", "   \n"), start=1):
        blank = KzDocument(id=delivered["ref"].kreuzakt_id, title=f"blank-{number}",
                           description="", content=blank_text, document_date=None,
                           original_filename="papra-blank.pdf", updated_at=None)

        result = await deliver_content(stack.papra, stack.store, delivered["ref"], blank,
                                       generation=stack.store.get_generation())

        assert result == SKIPPED_EMPTY, blank_text
        assert stack.papra.patches == [], blank_text   # not even an attempt, let alone a write
        document = await stack.papra.get_document(stack.org_id, delivered["document_id"])
        assert document["content"] == held, blank_text  # byte for byte what it held


async def test_a_late_papra_write_converges_back_on_the_next_pass(delivered, stack):
    """The one failure the pass exists for, against a Papra that really did overwrite us.

    Papra's OCR worker writes this column on upload and on `organizations:update`, so
    "we delivered it" and "Papra still holds it" are different facts and only one of them
    is observable from the ledger. The simulated late write is a PATCH of our own: what it
    has to prove is that a column we did not write is detected and re-converged.
    """
    sentinel = "Papra wrote this after our delivery"
    await stack.papra.update_document(stack.org_id, delivered["document_id"],
                                      content=sentinel)
    stack.papra.patches.clear()

    result = await verify_once(stack.store, stack.papra, stack.kreuzakt)

    assert (result.scanned, result.diverged, result.repaired) == (1, 1, 1)
    document = await stack.papra.get_document(stack.org_id, delivered["document_id"])
    assert document["content"] == delivered["kz_content"]
    repaired = list(stack.papra.patches)
    assert repaired == [(delivered["document_id"],
                         {"content": delivered["kz_content"]})]

    again = await verify_once(stack.store, stack.papra, stack.kreuzakt)
    assert (again.scanned, again.diverged, again.repaired) == (1, 0, 0)
    assert stack.papra.patches == repaired, "the second pass asked for nothing"


async def test_a_document_that_flip_flops_twice_is_left_alone_with_one_error(delivered,
                                                                            stack, caplog):
    """Two losses and the pass stops fighting, at ERROR, naming no culprit.

    Spec §8's integration edge. The flip-flop is driven by writing Papra's column from
    here, which is the only way to make a second Papra writer on demand; the property under
    test is our restraint, not Papra's behaviour.
    """
    import logging

    caplog.set_level(logging.INFO, logger="papra_suite.indexing")
    sentinel = "Papra wrote this after our delivery"
    for _ in range(3):
        await stack.papra.update_document(stack.org_id, delivered["document_id"],
                                          content=sentinel)
        result = await verify_once(stack.store, stack.papra, stack.kreuzakt)

    assert (result.diverged, result.repaired, result.suppressed) == (1, 0, 1)
    document = await stack.papra.get_document(stack.org_id, delivered["document_id"])
    assert document["content"] == sentinel        # left alone, not overwritten again
    errors = [record.getMessage() for record in caplog.records
              if record.levelno >= logging.ERROR]
    assert any("diverged more than twice in 24h" in message for message in errors), errors
    assert "reprocess" not in caplog.text


async def test_a_reconcile_rebuilds_every_binding_from_papra_bytes(stack):
    """The one operation in this milestone that destroys the whole index, on a real server.

    Everything else in this file rebuilds one binding by hand. This drives the path an
    operator actually runs — `rebuild_index`, which is `papra-suite ledger --reindex-all`: the
    health gate against the live index, one generation bump that makes every bound id in the
    corpus meaningless at once, `reconcile_generation` queueing the corpus through the intake
    path — and then drains the queue with the same `run_index_job` the worker runs. The claim
    is the end state: every reference resolves again under the new generation, from Papra's
    bytes, and Papra was never asked to change.

    The population is deliberately more than the ledger already knew. A document this suite
    marked seen and never ledgered is planted too, because that is the half a rebuild over
    references alone cannot reach, and it is the half that needs Papra to answer.

    **What this leaves unproven, said because the name is grander than the run.** It is a
    handful of documents in a throwaway store, so it measures nothing about a real corpus —
    neither the wall-clock the README refuses to invent nor the peak `extraction-empty` count.
    It does not wipe Kreuzakt's volume: the generation bump stands in for the wipe, which is
    the same fence (every old id stops resolving) but not the same server state, so a rebuilt
    Kreuzakt handing these integers out in a different order is not simulated here — only
    papered over by the fact that these bytes dedupe onto one document each. And the enrolment
    walk can only enrol documents this store *listed*: a Papra document that was never listed
    stays outside the number, as it does in production.
    """
    markers = [f"{TEXT_LAYER_MARKER}-rebuild-{n}" for n in (1, 2, 3)]
    document_ids = []
    for marker in markers:
        pdf = make_pdf(marker)
        document = await stack.papra.create_document(stack.org_id, f"{marker}.pdf", pdf)
        document_ids.append(document["id"])
        # Hashed from the bytes we uploaded, not read back: `run_index_job` verifies what it
        # downloads against this value, and a null `originalSha256Hash` on a just-created
        # record would otherwise be a permanent failure blaming our own job.
        enqueue_index_job(stack.store, papra_org_id=stack.org_id, document_id=document["id"],
                          file_hash=hashlib.sha256(pdf).hexdigest(), name=document["name"],
                          mime="application/pdf", ingested_via="sweep")
        job = stack.store.claim_next_job("index")
        await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                            poll_s=0.5, wait_timeout_s=90)
        stack.store.mark_job_done(job.id)

    # The pre-handover mail population: Papra holds it, this suite has listed it, the ledger
    # has never held it. Nothing but the enrolment walk can see it.
    unledgered = await stack.papra.create_document(stack.org_id, "mailed-before-handover.pdf",
                                                   make_pdf(f"{TEXT_LAYER_MARKER}-mailed"))
    stack.store.record_document_seen(papra_org_id=stack.org_id,
                                     papra_document_id=unledgered["id"],
                                     name=unledgered["name"], mime="application/pdf",
                                     ingested_via="mail")
    bound_before = {document_id: stack.store.ledger_get(stack.org_id, document_id).kreuzakt_id
                    for document_id in document_ids}
    assert stack.store.ledger_stats()["unledgered"] == 1

    result = await rebuild_index(stack.store, stack.kreuzakt, papra=stack.papra)

    assert (result.generation, result.stale, result.requeued) == (2, 3, 3)
    assert (result.enrolled, result.unledgered, result.complete) == (1, 0, True)

    drained = 0
    while (job := stack.store.claim_next_job("index")) is not None:
        await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                            poll_s=0.5, wait_timeout_s=90)
        stack.store.mark_job_done(job.id)
        drained += 1
    assert drained == 4

    generation = stack.store.get_generation()
    assert generation == 2 and stack.store.ledger_needs_index(generation=generation) == []
    for document_id in document_ids + [unledgered["id"]]:
        ref = stack.store.ledger_get(stack.org_id, document_id)
        assert ref.delivered_generation == generation, document_id
        # The binding answers at the authorization boundary, and the integer it names is a
        # document the live index will describe — not a number carried across the boundary.
        assert [item.papra_document_id
                for item in stack.store.ledger_resolve([ref.kreuzakt_id],
                                                       generation=generation)] == [document_id]
        assert (await stack.kreuzakt.get_document(ref.kreuzakt_id)).content.strip()
    # Identical bytes, one document: the re-upload cost a Kreuzakt round trip and no second
    # copy of anything, which is what makes a rebuild over a big corpus affordable at all.
    assert stack.store.ledger_get(stack.org_id, unledgered["id"]).kreuzakt_id \
        not in set(bound_before.values())
    assert stack.papra.patches == []


async def test_a_rebuild_can_schedule_work_for_an_already_indexed_document(stack):
    """What a backfill pass calls, and what it has to get back: a job it can claim.

    This ran as a strict `xfail` for two review rounds: the idempotency index covered the
    whole `jobs` table, so an index row that had already finished — cleanly or not — made
    every later request to index the same bytes a no-op, and a rebuild could not reach a
    document the ledger already knew about. That locked out the documents ingested during
    a Kreuzakt outage, not just rebuild items. Migration 004 narrowed the index to live
    rows, and this test is the live-server proof that the narrowing landed: the second
    `enqueue_index_job` returns a row the worker can claim, and the re-index still costs
    nothing but a Kreuzakt round trip.
    """
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
