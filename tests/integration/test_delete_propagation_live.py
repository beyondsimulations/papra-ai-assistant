"""The three external facts Task 9's delete propagation bets on, measured live.

The unit suite proves the *decision logic* against doubles whose status behaviour the same
author scripted. What it cannot prove — and what this file is for — is the three facts about
other people's servers that the logic reads as evidence:

* **Papra's detail 404 means "this bucket has no such record".** A false 404 walks straight
  to a Kreuzakt delete, so it matters that 404 is not also what Papra answers for a malformed
  id, a wrong-scope token or a body it dislikes. Measured here, on the pinned container: a
  well-formed id the bucket never held answers `404 document.not_found`, a malformed id
  answers `400`, and a document in the trash answers `200` with its trash flags set — so
  `deleted` is the ordinary path to a mark and a 404 is the purge.
* **`documentsCount` exists and is truthful.** It is the only thing the truncation arm of the
  trust predicate checks. The walk asks for one row at a time below, so a client-side
  `len(page)` fallback could not pass for a server-reported total.
* **Kreuzakt's `DELETE` is idempotent success on an id it has already forgotten.** The cleanup
  pass treats "already gone" as the debt being paid; if the server answered an error instead,
  every retried delete would walk to `cleanup_state='failed'` and need an operator.

The shared-bytes pair is here too, because Kreuzakt files by content hash and has no tenancy: one
buffer uploaded into two different Papra organizations is one index document with two references,
and the survivor's text is exactly what a wrong reference count destroys. Dedup itself is already
pinned in `test_index_pipeline.py`; pinned below is what happens to the survivor across a delete,
through the real `search()` and `get_document()`.
"""

import os
import uuid
from dataclasses import dataclass

import httpx
import pytest

from papra_suite import indexing
from papra_suite.indexing import (
    DELETED_THERE,
    GONE,
    papra_document_is_deleted,
    reconcile_deletions,
    run_cleanup,
    run_index_job,
)
from papra_suite.kreuzakt_client import KreuzaktClient, KreuzaktError
from papra_suite.papra_client import PapraClient, PapraError
from papra_suite.pipeline import enqueue_index_job, sweep_once
from papra_suite.store import Store
from tests.integration.conftest import PAPRA_TEST_URL, _papra_is_up, _skip_or_fail
from tests.integration.test_index_pipeline import (
    KREUZAKT_URL,
    TEXT_LAYER_MARKER,
    _kreuzakt_is_up,
    make_pdf,
)

pytestmark = pytest.mark.integration

#: `documents:delete` is here for the test setup only: trashing a document through Papra's API
#: is how a reference becomes "deleted in Papra" without inventing a fake for it. The bridge
#: itself never asks for it — it has no Papra delete path at all.
PERMS = ["organizations:read", "documents:create", "documents:read", "documents:update",
         "documents:delete", "tags:create", "tags:read"]


@dataclass
class Buckets:
    """Two buckets of one user, one index, one store — the shape that shares documents."""

    store: Store
    papra: PapraClient
    kreuzakt: KreuzaktClient
    first: str
    second: str
    token: str


@pytest.fixture
async def buckets(tmp_path):
    """A user with two empty buckets, both ledgered, both against the real index.

    Its own sign-up rather than `papra_stack`: the shared-bytes hazard needs two buckets of
    one user, and Papra refuses to file identical bytes twice inside one bucket, so a
    single-bucket fixture cannot express the scenario at all.
    """
    if not _papra_is_up(readiness_timeout_s=15.0):
        _skip_or_fail("scratch Papra not running; use `make test-integration`")
    if not _kreuzakt_is_up():
        _skip_or_fail(f"Kreuzakt not reachable at {KREUZAKT_URL}; bring the bridge stack up")

    suffix = uuid.uuid4().hex[:8]
    with httpx.Client(base_url=PAPRA_TEST_URL, timeout=30.0) as http:
        sign_up = http.post("/api/auth/sign-up/email", json={
            "email": f"del-{suffix}@papra-suite.test", "password": "seed-password-123456",
            "name": "Delete User"})
        sign_up.raise_for_status()
        org_ids = []
        for name in ("First", "Second"):
            created = http.post("/api/organizations", json={"name": f"{name} {suffix}"})
            created.raise_for_status()
            org_ids.append(created.json()["organization"]["id"])
        token = http.post("/api/api-keys",
                          json={"name": f"del-{suffix}", "permissions": PERMS}).json()["token"]

    store = Store(tmp_path / "state.db")
    store.migrate()
    for org_id in org_ids:
        store.create_org(papra_org_id=org_id, name=f"Org {org_id[-4:]}")
    papra = PapraClient(PAPRA_TEST_URL, token)
    kreuzakt = KreuzaktClient(os.environ.get("PAPRA_KREUZAKT_URL", KREUZAKT_URL))
    yield Buckets(store=store, papra=papra, kreuzakt=kreuzakt, first=org_ids[0],
                  second=org_ids[1], token=token)
    await papra.aclose()
    await kreuzakt.aclose()
    store.close()


async def _paper(stack: Buckets, org_id: str, name: str, marker: str,
                 data: bytes | None = None) -> tuple[str, int]:
    """One document on both sides of the bridge: Papra holds the bytes, the index holds the id.

    Driven through the ordinary pipeline — `enqueue_index_job`, then the real `run_index_job`
    against the real server — because a reference that was not created the way production creates
    it would not be the thing the reference count is counting. `data` is for the case where two
    documents must be the same text: a PDF is not reproducible byte-for-byte (the writer stamps an
    id and a date into it), so a shared index entry needs the same bytes, not the same marker.
    """
    document = await stack.papra.create_document(org_id, name, data or make_pdf(marker))
    document_id = document["id"]
    enqueue_index_job(stack.store, papra_org_id=org_id, document_id=document_id,
                      file_hash=document["originalSha256Hash"] or "no-hash",
                      name=document["name"], mime="application/pdf", ingested_via="test")
    job = stack.store.claim_next_job("index")
    outcome = await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                                  poll_s=0.5, wait_timeout_s=90)
    stack.store.mark_job_done(job.id)
    return document_id, outcome.kreuzakt_id


async def _shelf(stack: Buckets, org_id: str, name: str, marker: str) -> str:
    """Put one document in a bucket and leave it un-indexed.

    A bucket needs a document that stays to keep its listing non-empty: with nothing else in
    the bucket, emptying it is the one shape the trust predicate refuses to read, and a test
    that passed on that refusal would have measured the breaker instead of the delete.
    """
    document = await stack.papra.create_document(org_id, name, make_pdf(marker))
    return document["id"]


def _trash(stack: Buckets, org_id: str, document_id: str) -> None:
    """Move one document to Papra's trash, as a person in Papra's UI would.

    A raw call rather than a client method: `PapraClient` has no delete endpoint, which is
    the one-way rule as a property of the surface. Setup is allowed to do what the bridge is
    not, and says so here.
    """
    with httpx.Client(base_url=PAPRA_TEST_URL, timeout=30.0,
                      headers={"Authorization": f"Bearer {stack.token}"}) as http:
        response = http.request("DELETE",
                                f"/api/organizations/{org_id}/documents/{document_id}")
        response.raise_for_status()


def _other_form_of(document_id: str) -> str:
    """An id of the same shape that no bucket can hold: one character, different record.

    Used to ask what Papra answers for a record it has never heard of, as opposed to an id it
    cannot even parse — two different answers, and only one of them is evidence of absence.
    """
    last = document_id[-1]
    return document_id[:-1] + ("b" if last != "b" else "c")


async def test_papra_answers_absence_in_the_form_the_pass_reads_it(buckets):
    """The listing's own total, and the three different answers "not here" can mean.

    The truncation arm of the trust predicate compares the rows the walk saw against
    `documentsCount`, so a total that is a page length rather than a total makes the arm
    permanently satisfied — and a client that substitutes `len(page)` would do exactly that.
    Asking for one row at a time is what separates the two: three documents in the bucket,
    one row per page, and the total has to be three or the arm has nothing to check against.

    The second half is what the pass reads as absence, because each of these three answers
    authorises something different:

    * a document in the trash is not in the listing, and its detail answers `200` with the
      trash flags — so the ordinary path to a mark is Papra saying `deleted` about the one
      record it was asked about;
    * a well-formed id this bucket never held answers `404 document.not_found`, which is the
      purge case and the only answer the pass reads as `gone`;
    * an id Papra cannot parse answers something that is NOT a 404. That is the difference
      between "this record is gone" and "that is not a record", and a corrupt id must land in
      the second one: a 404 here would be enough to destroy a live document's index entry.
    """
    ids = []
    for position in range(3):
        document = await buckets.papra.create_document(
            buckets.first, f"total-{position}.pdf", make_pdf(f"total-marker-{position}"))
        ids.append(document["id"])

    rows, total = await buckets.papra.list_documents(buckets.first, page_size=1)

    assert len(rows) == 1, "one row per page is what makes the total a statement, not a count"
    assert isinstance(total, int) and not isinstance(total, bool)
    assert total == 3, f"the endpoint reported {total!r} for three documents"

    listed = await indexing._walk_listing(buckets.papra, buckets.first, expected_refs=3)
    assert set(listed) == set(ids) and listed[ids[0]] is False

    _trash(buckets, buckets.first, ids[0])

    listed = await indexing._walk_listing(buckets.papra, buckets.first, expected_refs=3)
    assert ids[0] not in listed and len(listed) == 2, "a trashed document leaves the listing"
    assert papra_document_is_deleted(await buckets.papra.get_document(buckets.first, ids[0]))
    assert await indexing._ask_papra(buckets.papra, buckets.first, ids[0]) == DELETED_THERE

    unknown = _other_form_of(ids[1])
    assert await indexing._ask_papra(buckets.papra, buckets.first, unknown) == GONE
    with pytest.raises(PapraError) as not_found:
        await buckets.papra.get_document(buckets.first, unknown)
    assert not_found.value.status == 404

    unparsable = "papra-suite-not-a-document-id"
    assert await indexing._ask_papra(buckets.papra, buckets.first, unparsable) is None, \
        "a 404 for something Papra cannot parse would be enough to delete a live document"


async def test_one_buffer_in_two_buckets_is_one_index_entry_with_two_references(buckets):
    """The state the reference count defends, produced by the ordinary path and no other.

    Whether two Papra organizations holding identical bytes land on one Kreuzakt document is not a
    detail of our bookkeeping: it is how Kreuzakt files by content hash, and it decides whether
    "this was the last reference" can ever be wrong. If the index ever gains tenancy, or the hash
    stops being the key, this test is where that shows up — and the two tests after it depend on
    the answer, which is why they do not simply assume it.
    """
    paper = make_pdf(TEXT_LAYER_MARKER)
    first, first_id = await _paper(buckets, buckets.first, "twin-a.pdf", TEXT_LAYER_MARKER, paper)
    second, second_id = await _paper(buckets, buckets.second, "twin-b.pdf", TEXT_LAYER_MARKER,
                                     paper)

    assert first_id == second_id, f"{first_id} vs {second_id}"
    bound = buckets.store.ledger_resolve([first_id], generation=buckets.store.get_generation())
    assert {(ref.papra_org_id, ref.papra_document_id) for ref in bound} == {
        (buckets.first, first), (buckets.second, second)}


async def test_one_reference_going_away_leaves_the_survivor_searchable(buckets, monkeypatch):
    """One buffer in two buckets, one index document: one reference may not kill it.

    This is the scenario the reference count exists for, produced the way production produces it.
    Kreuzakt files by content hash and has no tenancy, so the same bytes uploaded into two
    different Papra organizations are one document in the index — and a delete fired on one row's
    behalf removes text the other row cannot rebuild, because the survivor's bytes live only in
    Papra. The buffer is built once and handed to both uploads on purpose: a PDF writer stamps an
    id and a date into every file it closes, so two calls to `make_pdf` with the same marker are
    two different documents, and an earlier version of this test drew the wrong conclusion from
    exactly that.

    Both buckets hold a document that stays. Without it, emptying a bucket is the one listing the
    trust predicate refuses to read, and a test that passed on that refusal would have measured the
    breaker instead of the delete.
    """
    monkeypatch.setattr(indexing, "LISTING_WALK_INTERVAL_S", 0)
    await _shelf(buckets, buckets.first, "keeper-first.pdf", "keeper-first-7b1c")
    await _shelf(buckets, buckets.second, "keeper-second.pdf", "keeper-second-7b1d")
    paper = make_pdf(TEXT_LAYER_MARKER)
    gone, first_id = await _paper(buckets, buckets.first, "shared-a.pdf", TEXT_LAYER_MARKER, paper)
    kept, second_id = await _paper(buckets, buckets.second, "shared-b.pdf", TEXT_LAYER_MARKER,
                                   paper)
    assert first_id == second_id, \
        f"one buffer in two buckets: {first_id} vs {second_id}"

    _trash(buckets, buckets.first, gone)

    result = await reconcile_deletions(buckets.store, buckets.papra, buckets.kreuzakt)
    cleanup = await run_cleanup(buckets.store, buckets.kreuzakt)

    assert (result.aborted, result.refs_marked, result.upstream_pending) == (False, 1, 0)
    assert cleanup.deleted == 0, "one reference of several is not a delete"
    ref = buckets.store.ledger_get(buckets.first, gone)
    assert ref.deleted_at is not None and ref.cleanup_state == "none"
    assert buckets.store.ledger_get(buckets.second, kept).deleted_at is None

    # The survivor, asked of the index itself rather than of our bookkeeping.
    document = await buckets.kreuzakt.get_document(first_id)
    assert document.content.strip(), "the survivor lost its text"
    hits, hits_total = await buckets.kreuzakt.search("papra-index-bridge-marker")
    assert first_id in {hit.id for hit in hits}, \
        f"the survivor stopped being findable: {hits_total} hit(s) for the marker"


async def test_the_index_entry_dies_with_the_last_reference_and_not_before(buckets, monkeypatch):
    """The end of the same story, through the sweep, with the index's own answers as evidence.

    Why this is a separate test: the previous one stops at "nothing was destroyed", which a
    mutation that never queues anything also satisfies. Here the second reference goes too, and
    the document has to be really gone — so a guard that refuses forever would fail this test
    exactly as a guard that deletes too early fails the one above. The two together pin the
    boundary rather than one side of it.

    The closing assertion is the third external fact: asking Kreuzakt to delete an id it has
    already forgotten is a success. The cleanup pass reads that as the debt being paid, and it
    is the reason a crash between "deleted" and "recorded" costs nothing instead of walking a
    finished delete to `cleanup_state='failed'`.
    """
    monkeypatch.setattr(indexing, "LISTING_WALK_INTERVAL_S", 0)
    await _shelf(buckets, buckets.first, "keeper-first.pdf", "keeper-first-9a2e")
    await _shelf(buckets, buckets.second, "keeper-second.pdf", "keeper-second-9a2f")
    paper = make_pdf(TEXT_LAYER_MARKER)
    first, first_id = await _paper(buckets, buckets.first, "shared-a.pdf", TEXT_LAYER_MARKER, paper)
    second, second_id = await _paper(buckets, buckets.second, "shared-b.pdf", TEXT_LAYER_MARKER,
                                     paper)
    assert first_id == second_id, "one buffer in two buckets is one index entry"

    _trash(buckets, buckets.first, first)
    one_gone = await sweep_once(buckets.store, buckets.papra, kreuzakt=buckets.kreuzakt)

    assert (one_gone.deletions_marked, one_gone.cleanup_deleted) == (1, 0)
    assert (await buckets.kreuzakt.get_document(first_id)).id == first_id

    _trash(buckets, buckets.second, second)
    both_gone = await sweep_once(buckets.store, buckets.papra, kreuzakt=buckets.kreuzakt)

    assert (both_gone.deletions_marked, both_gone.cleanup_deleted) == (1, 1)
    with pytest.raises(KreuzaktError) as missing:
        await buckets.kreuzakt.get_document(first_id)
    assert missing.value.status == 404
    assert await buckets.kreuzakt.delete_document(first_id) is None, \
        " Kreuzakt does not treat an already-gone id as done"
