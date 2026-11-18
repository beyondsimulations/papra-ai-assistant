"""Task 7: the content writer, the blank-page rule, and a convergence pass that sees.

Two decisions give this file its shape, and both are about what the pass is *for*.

**Verification compares Papra's live text against Kreuzakt's live text.** v1 compared
Papra's text against `delivered_sha` — the hash we recorded when we last wrote. After
Papra's own OCR overwrote a document we had delivered, that comparison still said
"delivered": the ledger's memory was right about what we sent and useless about what
Papra holds, which is the one fact the pass exists to know. Every test below therefore
sets Papra's current `content` and expects the answer to change with it.

**The emptiness guard sits before every shortcut.** Kreuzakt answering with nothing is
normal for a blank scan, and writing that nothing over Papra's existing text would be
the most destructive thing this bridge can do — it deletes the record's text using a
flag that was meant to improve it. So the guard runs before the equality shortcut, and
`test_the_emptiness_guard_outranks_the_equality_shortcut` fails if the two are
reordered, which is the failure mode a later "optimisation" would introduce.

The doubles come from `tests/test_index_job.py`: `FakePapra` records every write,
`FakeKreuzakt` is the real client behind a scripted transport. What is asserted about a
write is its recorded body, so "content only" is a fact about the request rather than
about a comment.
"""

import hashlib
import json
import logging
from pathlib import Path

import pytest

from papra_suite.indexing import (
    DeliveryDisabled,
    deliver_content,
    run_index_job,
    truncate_notes,
    verify_once,
)
from papra_suite.kreuzakt_client import KzDocument
from papra_suite.papra_client import PapraError
from papra_suite.store import EMPTY_EXTRACTION_SHA, Store
from tests.test_index_job import DOC_ID, HASH, NAME, ORG, FakeKreuzakt, FakePapra, make_job

FIXTURES = Path(__file__).parent / "fixtures"
#: What the scripted index holds for document 7 — the text a delivery writes.
KZ_TEXT = json.loads((FIXTURES / "kreuzakt_document.json").read_text())["content"]
PAPRA_TEXT = "Papra wrote this after us — its own OCR pass over the same bytes"
KZ_UPDATED_AT = "2026-11-17 20:02:42"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def kz_doc(*, content: str = KZ_TEXT, doc_id: int = 7,
           updated_at: str | None = KZ_UPDATED_AT) -> KzDocument:
    return KzDocument(id=doc_id, title="Rechnung 4711", description="",
                      content=content, document_date=None,
                      original_filename="papra-doc-1-a1b2c3d4.pdf",
                      updated_at=updated_at)


def seed_ref(store, *, org: str = ORG, document_id: str = DOC_ID, kreuzakt_id: int = 7,
             generation: int = 1, delivered_sha: str | None = None) -> None:
    """A ledger row as the index job leaves it: bound, and optionally already delivered.

    `delivered_generation` is stamped by `ledger_set_kreuzakt_id`, and
    `ledger_record_delivery` refuses to write unless the row is bound to the generation
    it names — so a "delivered" row has to be built in that order here too, or the
    fixture would be asserting against a state production cannot produce.
    """
    store.ledger_upsert_reference(papra_org_id=org, papra_document_id=document_id,
                                  file_hash=HASH, papra_original_name=NAME)
    store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=document_id,
                                 kreuzakt_id=kreuzakt_id, generation=generation)
    if delivered_sha is not None:
        store.ledger_record_delivery(papra_org_id=org, papra_document_id=document_id,
                                     delivered_at="2026-11-17 20:03:00",
                                     delivered_sha=delivered_sha, generation=generation)


def indexed(store, fake_kz, *, content: str = KZ_TEXT, kreuzakt_id: int = 7,
            delivered_sha: str | None = None) -> None:
    """One document the index holds AND the ledger is bound to.

    Both halves, always, because the pass reads both sides: a seeded ledger row whose
    Kreuzakt id nothing serves is a lookup error, not a document, and a test that
    forgot to script the index would be testing the failure path by accident.
    """
    fake_kz.document = kz_doc(content=content, doc_id=kreuzakt_id)
    seed_ref(store, kreuzakt_id=kreuzakt_id, delivered_sha=delivered_sha)


def add_papra_document(fake_papra, document_id: str, *, org: str = ORG,
                       content: str = PAPRA_TEXT) -> None:
    fake_papra.documents[(org, document_id)] = {
        "id": document_id, "organizationId": org, "name": f"{document_id}.pdf",
        "mimeType": "application/pdf", "content": content,
        "originalSha256Hash": HASH, "isDeleted": False, "deletedAt": None,
    }


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=ORG, name="Org 1")
    yield store
    store.close()


@pytest.fixture
def fake_papra():
    return FakePapra()


@pytest.fixture
async def fake_kz():
    fake = FakeKreuzakt()
    yield fake
    await fake.aclose()


@pytest.fixture
def job(store):
    return make_job(store=store)


def open_gate(store):
    """Open the one gate this whole module is about.

    A function rather than a fixture, because most tests here need the gate open AND the
    store's contents scripted through the helpers above; a fixture would have ordered the
    two implicitly, and the order is the part that matters.
    """
    store.set_setting("index_content_delivery", "true")
    return store


# --- one gate, every path ---------------------------------------------------------

async def test_delivery_gate_blocks_every_path_that_can_reach_papra(store, fake_papra,
                                                                    fake_kz, job):
    """A flag the scheduler alone obeys can be bypassed by the paths that matter most.

    Convergence, a webhook trigger and a CLI call all reach `deliver_content` without
    going through a loop that was told to be careful, so the gate is inside the writer.
    The worker path gets the opposite treatment — it reports `delivered=False` rather
    than raising, because a closed gate is not a job failure.
    """
    store.set_setting("index_content_delivery", "false")
    indexed(store, fake_kz)
    ref = store.ledger_get(ORG, DOC_ID)

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)
    assert outcome.delivered is False

    with pytest.raises(DeliveryDisabled):
        await deliver_content(fake_papra, store, ref, kz_doc(), generation=1)

    reads_before = fake_papra.get_calls
    assert (await verify_once(store, fake_papra, fake_kz)).repaired == 0
    assert fake_papra.patch_calls == 0
    # and the closed pass did not even read Papra: a gate that reads first and refuses
    # second is a gate that still puts load on the record it is not allowed to touch.
    assert fake_papra.get_calls == reads_before


async def test_the_gate_is_read_from_the_store_on_every_call(store, fake_papra, fake_kz):
    """Toggling the setting mid-process moves the writer, without a restart.

    The flag is the handover's lever, and the handover happens while the container
    runs; a gate captured at import would need a restart to become honest, which is
    exactly the window the two-writer refusal exists to close.
    """
    indexed(store, fake_kz)
    ref = store.ledger_get(ORG, DOC_ID)
    with pytest.raises(DeliveryDisabled):
        await deliver_content(fake_papra, store, ref, kz_doc(), generation=1)

    store.set_setting("index_content_delivery", "true")
    assert await deliver_content(fake_papra, store, ref, kz_doc(),
                                 generation=1) == "delivered"
    assert fake_papra.patch_calls == 1


# --- what a delivery writes -------------------------------------------------------

async def test_a_delivery_writes_content_and_nothing_else(store, fake_papra, fake_kz):
    """Task 7 owns the text column and only the text column.

    Titles, dates and notes are Task 8's, and the handover is deliberately separable:
    "the rename went wrong" and "the text went wrong" have to be distinguishable
    incidents. So the recorded body has exactly one key, and a delivery that also
    touched `name` — the one field a human edits — fails here rather than in the UI.
    """
    open_gate(store)
    indexed(store, fake_kz)
    ref = store.ledger_get(ORG, DOC_ID)

    result = await deliver_content(fake_papra, store, ref, kz_doc(),
                                   generation=store.get_generation())

    assert result == "delivered"
    assert fake_papra.patched == [(ORG, DOC_ID, {"content": KZ_TEXT})]
    assert fake_papra.document["content"] == KZ_TEXT


async def test_a_delivery_records_what_it_sent(store, fake_papra, fake_kz):
    """`delivered_sha` is the text we wrote; `delivered_at` is the index's own timestamp."""
    open_gate(store)
    indexed(store, fake_kz)
    ref = store.ledger_get(ORG, DOC_ID)

    await deliver_content(fake_papra, store, ref, kz_doc(), generation=1)

    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.delivered_sha == sha(KZ_TEXT)
    assert ref.delivered_generation == store.get_generation()
    assert ref.delivered_at == KZ_UPDATED_AT


async def test_a_write_papra_refuses_leaves_the_ledger_claiming_nothing(
        store, fake_papra, fake_kz, caplog):
    """A failed PATCH must not look like a delivery, and must stay retriability intact.

    Three lies the ledger could tell if the write were recorded before the PATCH: that
    we delivered (so `check` counts a document Papra never received), that the current
    hash is ours (so the next pass skips it), and that the job is done (the worker only
    retries what raised). So the exception travels, with its status on it, and the log
    line names the document the operator has to go and look at.
    """
    open_gate(store)
    caplog.set_level(logging.ERROR, logger="papra_suite.indexing")
    indexed(store, fake_kz)
    ref = store.ledger_get(ORG, DOC_ID)
    fake_papra.raise_on_update = PapraError("PATCH document failed: 502 bad gateway", 502)

    with pytest.raises(PapraError) as excinfo:
        await deliver_content(fake_papra, store, ref, kz_doc(), generation=1)

    assert excinfo.value.status == 502      # 5xx: the worker's backoff, not permanence
    assert fake_papra.patch_calls == 1      # the write was attempted, and refused
    after = store.ledger_get(ORG, DOC_ID)
    assert after.delivered_sha is None
    assert after.delivered_at is None
    assert fake_papra.document["content"] == ""
    assert DOC_ID in caplog.text


async def test_the_handler_lets_the_failure_through_so_the_job_stays_retriable(
        store, fake_papra, fake_kz, job):
    """The delivery is part of the job, so a refused write fails the job rather than the corpus.

    Swallowing it here would mark the job done: the document would keep Papra's own
    text, the sweep would consider it seen, and nothing but `jobs --reset` would look
    again.
    """
    open_gate(store)
    fake_papra.raise_on_update = PapraError("PATCH document failed: 502 bad gateway", 502)

    with pytest.raises(PapraError):
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.kreuzakt_id == 7                       # the binding is real and kept
    assert ref.delivered_sha is None                  # the delivery is not claimed


# --- the blank-page rule ----------------------------------------------------------

async def test_empty_kreuzakt_text_never_overwrites_non_empty_papra(store, fake_papra):
    """Mutation check (b) of the plan's gate: delete the guard and this fails.

    The scenario is a scan whose extraction came back empty while Papra's own pass did
    find text. The bridge's job in that moment is to do nothing, and "nothing" includes
    not wiping a paragraph that has been in the record for a year.
    """
    open_gate(store)
    seed_ref(store)
    ref = store.ledger_get(ORG, DOC_ID)
    fake_papra.document["content"] = "Vorhandener Text aus Papras eigener OCR"

    result = await deliver_content(fake_papra, store, ref, kz_doc(content=""),
                                   generation=1)

    assert result == "skipped-empty"
    assert fake_papra.patch_calls == 0
    assert fake_papra.document["content"] == "Vorhandener Text aus Papras eigener OCR"


async def test_whitespace_only_extraction_counts_as_empty(store, fake_papra):
    """`"   \\n "` is empty to a reader and non-empty to `== ""`.

    A VLM that returns a stray newline is the common case, not the exotic one, so the
    guard tests the string the way a human would read it.
    """
    open_gate(store)
    seed_ref(store)
    ref = store.ledger_get(ORG, DOC_ID)
    fake_papra.document["content"] = "Rechnung Nr. 4711"

    assert await deliver_content(fake_papra, store, ref,
                                 kz_doc(content="   \n\t "), generation=1) == "skipped-empty"
    assert fake_papra.patch_calls == 0


async def test_the_emptiness_guard_outranks_the_equality_shortcut(store, fake_papra):
    """The reorder this task is most likely to acquire, pinned by name.

    Both sides empty is byte-equal, so a shortcut placed first would report
    `skipped-current` and stamp the hash of the empty string. The difference is not
    cosmetic: `EMPTY_EXTRACTION_SHA` in `delivered_sha` is the only thing that makes
    `ledger_stats()["extraction_empty"]` count, and that number is the cheap signal
    that separates "a few odd pages" from "the extractor is wrong for this corpus".
    """
    open_gate(store)
    seed_ref(store)
    ref = store.ledger_get(ORG, DOC_ID)
    fake_papra.document["content"] = ""

    assert await deliver_content(fake_papra, store, ref, kz_doc(content=""),
                                 generation=1) == "skipped-empty"

    assert store.ledger_get(ORG, DOC_ID).delivered_sha == EMPTY_EXTRACTION_SHA
    assert store.ledger_stats()["extraction_empty"] == 1


async def test_the_extraction_empty_counter_moves(store, fake_papra):
    """The stat has to be produced by something, or `check` reports a convincing zero.

    Task 4 shipped `extraction_empty` counting a value nothing wrote. This is the write.
    """
    open_gate(store)
    seed_ref(store)
    ref = store.ledger_get(ORG, DOC_ID)
    assert store.ledger_stats()["extraction_empty"] == 0

    await deliver_content(fake_papra, store, ref, kz_doc(content="   "), generation=1)

    stats = store.ledger_stats()
    assert stats["extraction_empty"] == 1
    assert stats["delivered"] == 1              # the row is still of the current generation


# --- the convergence pass ---------------------------------------------------------

async def test_papra_overwriting_us_is_detected_and_repaired(store, fake_papra, fake_kz):
    """The defect v1 shipped: the ledger says delivered, and Papra's text is not ours.

    This is not a hypothetical. Papra's own OCR worker runs on upload and on
    `organizations:update` and writes the same column — measured, while our text sat
    there — which is the whole reason the handover stops it.
    """
    open_gate(store)
    indexed(store, fake_kz, delivered_sha=sha(KZ_TEXT))   # the ledger believes it is done
    fake_papra.document["content"] = PAPRA_TEXT

    result = await verify_once(store, fake_papra, fake_kz)

    assert (result.scanned, result.diverged, result.repaired) == (1, 1, 1)
    assert fake_papra.document["content"] == KZ_TEXT
    assert store.ledger_get(ORG, DOC_ID).repair_count == 1


async def test_a_converged_document_is_left_alone(store, fake_papra, fake_kz):
    """Equal text is equal text, whoever wrote it — no write, no repair counted.

    Without this the six-hourly pass would PATCH all five hundred documents every time
    and spend the suppression budget on documents that never diverged, so the number
    that is supposed to mean "this document keeps fighting us" would mean "the pass
    ran".
    """
    open_gate(store)
    indexed(store, fake_kz, delivered_sha=sha(KZ_TEXT))
    fake_papra.document["content"] = KZ_TEXT

    result = await verify_once(store, fake_papra, fake_kz)

    assert (result.diverged, result.repaired, result.suppressed) == (0, 0, 0)
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).repair_count == 0


async def test_a_row_that_never_recorded_a_delivery_records_one_without_writing(
        store, fake_papra, fake_kz):
    """Papra already holds our text but the ledger never stamped it: fix the ledger only.

    This is the state every document in the corpus reaches the moment the gate opens,
    because the documents were indexed while delivery was off and Papra's own
    extraction produced the same text. A pass that PATCHed on the ledger's memory alone
    would rewrite the archive on the first run after a flag flip.
    """
    open_gate(store)
    indexed(store, fake_kz)                     # bound; delivered_sha is NULL
    fake_papra.document["content"] = KZ_TEXT

    result = await verify_once(store, fake_papra, fake_kz)

    assert (result.diverged, result.repaired) == (0, 0)
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).delivered_sha == sha(KZ_TEXT)


async def test_both_sides_are_read_fresh_on_every_pass(store, fake_papra, fake_kz):
    """Nothing is cached across passes, on either side.

    Kreuzakt's text can change under a row — a re-index after a model change, an
    operator's reprocess — and Papra's can change an hour after we wrote it. A pass
    that trusted `delivered_sha` on either side would stop seeing exactly the drift it
    is here for.
    """
    open_gate(store)
    indexed(store, fake_kz)
    fake_papra.document["content"] = PAPRA_TEXT

    first = await verify_once(store, fake_papra, fake_kz)
    assert (first.diverged, first.repaired) == (1, 1)
    reads_after_first = fake_papra.get_calls
    assert fake_papra.document["content"] == KZ_TEXT

    fake_kz.document = kz_doc(content="Neuer Text nach erneutem Re-Index")
    second = await verify_once(store, fake_papra, fake_kz)

    assert (second.diverged, second.repaired) == (1, 1)
    assert fake_papra.document["content"] == "Neuer Text nach erneutem Re-Index"
    assert fake_papra.get_calls > reads_after_first


async def test_repair_suppressed_after_two_divergences_in_24h(store, fake_papra, fake_kz,
                                                              caplog):
    """A document that flip-flops twice in a day is somebody else's problem, loudly.

    Two writers on one column cannot be won by writing harder: the third repair of the
    same fight burns a VLM round trip and a PATCH to lose the text again. So the pass
    counts the fight, stops joining it, and says so at ERROR.

    The wording is pinned as well as the behaviour: it names the observable and the
    document, never a culprit. Pointing at Kreuzakt's reprocess endpoint from a log line
    would send the operator to restart the index over a race between two writers, which
    is the opposite of the fix.
    """
    open_gate(store)
    caplog.set_level(logging.INFO, logger="papra_suite.indexing")
    indexed(store, fake_kz)

    for _ in range(3):
        # Papra's own OCR pass lands again between every pair of ours. That re-write is
        # the scenario, not a shortcut: a document we repaired and that diverged again is
        # a fight, and the count that decides to stop fighting is a count of losses.
        fake_papra.document["content"] = PAPRA_TEXT
        result = await verify_once(store, fake_papra, fake_kz)

    assert result.suppressed == 1
    assert fake_papra.patch_calls == 2               # passes 1 and 2 repaired, 3 held back
    assert fake_papra.document["content"] == PAPRA_TEXT
    assert any("diverged more than twice in 24h" in record.getMessage()
               for record in caplog.records)
    assert "reprocess" not in caplog.text
    assert DOC_ID in caplog.text


async def test_a_suppressed_document_is_repaired_again_after_the_window(store, fake_papra,
                                                                        fake_kz):
    """The budget is a rolling day, not a lifetime ban on one unlucky document.

    `ledger_bump_repair` resets when the previous repair falls out of the window; a
    suppression that never expired would mean a document which had one bad afternoon is
    abandoned for good, and nothing would say so.
    """
    open_gate(store)
    indexed(store, fake_kz)
    for _ in range(3):
        fake_papra.document["content"] = PAPRA_TEXT      # the fight, as above
        await verify_once(store, fake_papra, fake_kz)
    assert fake_papra.patch_calls == 2

    # Age the repair record rather than the clock: this is the row the store writes,
    # and moving the wall clock would move the window with it (see test_main.py, which
    # ages rows the same way to test retention).
    with store._conn:
        store._conn.execute("UPDATE index_ledger SET last_repair_at = '2000-01-01 00:00:00'")

    result = await verify_once(store, fake_papra, fake_kz)
    assert (result.diverged, result.repaired, result.suppressed) == (1, 1, 0)


async def test_an_empty_extraction_does_not_spend_the_repair_budget(store, fake_papra,
                                                                    fake_kz):
    """A document we refuse to write is not a document we failed to repair.

    The suppression budget answers "how many times did we try and lose". A blank scan is
    a decision not to try, so it must not consume it — otherwise a document that is
    empty for a day, then gets a real extraction, meets a pass that has already decided
    to leave it alone.
    """
    open_gate(store)
    indexed(store, fake_kz, content="   ")
    fake_papra.document["content"] = "Vorhandener Text"

    for _ in range(3):
        result = await verify_once(store, fake_papra, fake_kz)

    assert (result.suppressed, result.empty) == (0, 1)
    assert result.diverged == 0
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).repair_count == 0
    assert fake_papra.document["content"] == "Vorhandener Text"
    assert store.ledger_stats()["extraction_empty"] == 1


async def test_lookup_error_fails_closed_and_is_counted(store, fake_papra, fake_kz):
    """An unreadable side is not a divergence.

    "Papra did not answer" cannot be told apart from "Papra's text is wrong" from here,
    and repairing on that reading means writing text into a document whose state we do
    not know — and spending a repair on it. So the row is counted and left alone.
    """
    open_gate(store)
    indexed(store, fake_kz)
    fake_papra.raise_on_get = PapraError("GET document failed: 500", 500)

    result = await verify_once(store, fake_papra, fake_kz)

    assert (result.scanned, result.diverged, result.repaired) == (1, 0, 0)
    assert result.lookup_errors == 1
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).repair_count == 0


async def test_a_kreuzakt_that_does_not_answer_is_counted_the_same_way(store, fake_papra,
                                                                       fake_kz):
    """The other side can fail too, and the rule is the same: no answer, no write."""
    open_gate(store)
    indexed(store, fake_kz)
    fake_kz.missing_document_ids.add(7)   # the read endpoint answers 404 for our id

    result = await verify_once(store, fake_papra, fake_kz)

    assert (result.diverged, result.repaired) == (0, 0)
    assert result.lookup_errors == 1
    assert fake_papra.patch_calls == 0


async def test_wrong_generation_row_is_not_repaired(store, fake_papra, fake_kz):
    """After a rebuild the ledger's integers describe an index that no longer exists.

    Repairing one would write text we cannot vouch for into Papra while the corpus is
    still being re-indexed, so the walk is generation-scoped and sees nothing at all.
    """
    open_gate(store)
    indexed(store, fake_kz)
    fake_papra.document["content"] = PAPRA_TEXT
    store.bump_generation()

    result = await verify_once(store, fake_papra, fake_kz)

    assert (result.scanned, result.diverged, result.repaired) == (0, 0, 0)
    assert fake_papra.patch_calls == 0


async def test_a_deleted_reference_is_not_repaired(store, fake_papra, fake_kz):
    """A document the user deleted is not ours to bring back by writing into it."""
    open_gate(store)
    indexed(store, fake_kz)
    fake_papra.document["content"] = PAPRA_TEXT
    store.ledger_mark_deleted(papra_org_id=ORG, papra_document_id=DOC_ID)

    result = await verify_once(store, fake_papra, fake_kz)

    assert (result.scanned, result.repaired) == (0, 0)
    assert fake_papra.patch_calls == 0


async def test_a_row_indexed_while_delivery_was_off_is_the_one_being_repaired(
        store, fake_papra, fake_kz):
    """The population this pass exists for, established rather than asserted of.

    The corpus reaches the gate open with rows bound to the current generation, Papra
    holding its own mangled extraction, and `delivered_sha` empty — indexed while
    delivery was off. If the walk filtered on "we wrote this one", the flip would
    converge nothing and look healthy.
    """
    indexed(store, fake_kz)                          # gate off: indexed, never delivered
    fake_papra.document["content"] = PAPRA_TEXT
    store.set_setting("index_content_delivery", "true")   # the flag flip, same rows

    result = await verify_once(store, fake_papra, fake_kz)

    assert (result.scanned, result.diverged, result.repaired) == (1, 1, 1)
    assert fake_papra.document["content"] == KZ_TEXT


async def test_the_walk_pages_the_ledger_and_sees_every_reference_once(store, fake_papra,
                                                                       fake_kz,
                                                                       monkeypatch):
    """Five hundred documents is a page-and-a-half, and a page boundary must not repeat.

    The cursor is the `(papra_org_id, papra_document_id)` key the walk already orders
    by, so a page that came back twice would repair the same documents twice, spend
    their repair budgets, and report the corpus as twice as broken as it is.
    """
    open_gate(store)
    total = 150
    for index in range(total):
        document_id = f"doc-{index:03d}"
        seed_ref(store, document_id=document_id)
        add_papra_document(fake_papra, document_id)
    fake_kz.document = kz_doc()

    cursors: list = []
    real = store.ledger_delivered_refs

    def spy(*, generation, after=None, org_ids=None, limit=100):
        cursors.append(after)
        return real(generation=generation, after=after, org_ids=org_ids, limit=limit)

    monkeypatch.setattr(store, "ledger_delivered_refs", spy)

    result = await verify_once(store, fake_papra, fake_kz)

    assert (result.scanned, result.diverged, result.repaired) == (total, total, total)
    assert fake_papra.patch_calls == total
    assert cursors[0] is None and len(cursors) == 2
    assert cursors[1] == (ORG, "doc-099")


# --- the notes cap, shipped here because Task 8 writes the field ------------------

def test_short_notes_are_returned_unchanged():
    assert truncate_notes("Rechnung über 1.234,56 €") == "Rechnung über 1.234,56 €"
    assert truncate_notes("") == ""


def test_notes_are_cut_at_the_last_newline_below_the_limit():
    """A note is prose; a cut that keeps whole lines reads like a note.

    The `…` is what tells a reader the sentence went on, so silently dropping the tail
    cannot be mistaken for the document's own ending.
    """
    line = "z" * 100
    text = "\n".join([line] * 200)

    result = truncate_notes(text, limit=2000)

    cut = len(result) - 1
    assert result.endswith("…")
    assert text[:cut] == result[:-1]              # shortened, never rewritten
    assert text[cut] == "\n"                      # the cut landed on a line boundary
    assert "\n" not in text[cut + 1:2000]         # and it was the last one under it
    assert len(result) <= 2001


def test_notes_without_newlines_are_cut_at_a_word():
    """Papra's `notes` is one field, and a cut mid-word is a broken amount or IBAN.

    "1.234,5" is not a truncated number, it is a wrong one, in a field a reader quotes.
    """
    text = "wort " * 900

    result = truncate_notes(text, limit=2000)

    assert result.endswith("wort…")               # a whole word, or none of it
    assert result[:-1].rstrip() == text[:len(result) - 1].rstrip()
    assert len(result) <= 2001


def test_notes_with_no_whitespace_at_all_stay_inside_the_cap():
    result = truncate_notes("x" * 5000, limit=2000)
    assert len(result) == 2001                    # nothing to cut on, so cut at the cap
    assert result.endswith("…")


def test_notes_never_exceed_papras_own_cap():
    """2048 is Papra's limit, and a `limit` argument cannot be allowed to exceed it.

    The field is bounded, so a caller asking for 5000 characters would turn a delivery
    into a 4xx and a failed job rather than a shorter note.
    """
    result = truncate_notes("wort " * 5000, limit=5000)
    assert len(result) <= 2048
