"""Task 9: delete and restore propagation — reference-counted, crash-durable, one-way.

Everything in this file exists because the failure mode here is not a wrong count or a
missed log line: it is **the text of a document the user did not delete disappearing from
search**. Kreuzakt has no tenancy and deduplicates identical bytes onto one document, so
several Papra references share one Kreuzakt document, and a delete that fires while one of
them is still live removes content that the surviving reference cannot rebuild from the
index. So the ordering below is not style:

* **Reference counting is the mutation gate.** A Kreuzakt document is destroyed only when
  the last live reference of the current generation is gone, counted at delete time, and
  re-checked at execution time — a queued intent is a claim about the past, and a live
  reference that appears or comes back between the queue and the delete outvotes it.
* **Papra is never written or deleted here.** The propagation is one-way; every test in
  this module is covered by `papra_is_never_written_or_deleted`, including the success
  paths, because "we only read the record" has to be a mechanism, not an intention.
* **Absence is the weakest signal in the system and the most expensive one to get wrong.**
  A listing that is short, empty, malformed, self-contradictory or non-terminating aborts
  the whole pass rather than authorising a mark; and even a trustworthy listing only
  *proposes* candidates, each of which has to be confirmed by Papra's own detail endpoint
  before anything is marked. Every trust predicate is tested individually, and the abort
  is tested by asserting the ledger is byte-for-byte what it was.

The two counters that must never be silent — `aborted` and `capped` — are asserted in the
sweep log line and in `check`, because a pass that quietly refused to delete for a whole
week and a pass that deleted 20 of 500 documents both look identical from the ledger.
"""

import hashlib
import logging
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from papra_suite import indexing
from papra_suite.indexing import (
    CLEANUP_BACKOFF_S,
    CLEANUP_MAX_ATTEMPTS,
    MAX_DELETIONS_PER_PASS,
    CleanupResult,
    DeletionResult,
)
from papra_suite.papra_client import PapraError
from papra_suite.pipeline import sweep_once
from papra_suite.store import Store
from tests.test_index_job import FakeKreuzakt

ORG = "org-a"
OTHER_ORG = "org-b"
DOC_ID = "doc-1"
PDF_BYTES = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\ntrailer\n%%EOF"
HASH = hashlib.sha256(PDF_BYTES).hexdigest()
NAME = "scan-0001.pdf"

#: One page of a listing, kept small so a paging test can build a walk out of rows.
PAGE_SIZE = 2


# --- doubles ----------------------------------------------------------------------

class FakePapra:
    """Papra's read side, plus write counters that must stay at zero forever.

    The listing is the input this task lives on, so it is modelled as a real paginated
    endpoint: `list_documents` slices `documents` by `page_index`/`page_size` and reports a
    total like Papra's `documentsCount` does. Every way that can stop being trustworthy has
    a knob of its own, and each knob is what one abort test turns.

    `patch_calls` and `delete_calls` exist to be asserted empty. `delete_calls` in
    particular: `PapraClient` has no delete endpoint at all, so the strongest form of the
    one-way rule is that this bridge cannot even express the request — the counter is here
    so that stays a tested fact rather than a coincidence of the client's surface.
    """

    def __init__(self):
        self.documents: dict[tuple[str, str], dict] = {}
        self.patch_calls = 0
        self.delete_calls = 0
        self.get_calls = 0
        self.list_calls = 0
        self.page_sizes: list[int] = []
        # --- the distrust knobs, all off by default
        self.raise_on_list: Exception | None = None
        self.raise_on_get: dict[str, Exception] = {}
        self.count_override: int | None = None      # the server's own total disagrees
        self.total_pages: list[int | None] | None = None   # per-page declared totals
        self.malformed_page: list | None = None     # rows that are not documents
        self.endless = False                        # every page is a full page, forever
        self.conflicting_id: str | None = None      # one id, two answers, one walk
        self.stale_listing: set[str] = set()        # listed as trash, detail says live

    def add(self, document_id: str, *, org: str = ORG, deleted: bool = False,
            **over) -> dict:
        document = {"id": document_id, "organizationId": org, "name": f"{document_id}.pdf",
                    "mimeType": "application/pdf", "content": "",
                    "originalSha256Hash": HASH, "isDeleted": deleted,
                    "deletedAt": "2026-11-17T10:00:00.000Z" if deleted else None, **over}
        self.documents[(org, document_id)] = document
        return document

    @property
    def records(self) -> list[dict]:
        return [doc for (org, _), doc in self.documents.items() if org == ORG]

    async def list_documents(self, org_id: str, *, page_index: int = 0,
                             page_size: int = 100, search_query: str | None = None,
                             sort_field: str = "createdAt",
                             sort_order: str = "desc") -> tuple[list[dict], int]:
        self.list_calls += 1
        self.page_sizes.append(page_size)
        if self.raise_on_list is not None:
            raise self.raise_on_list
        if self.malformed_page is not None and page_index == 0:
            return list(self.malformed_page), len(self.malformed_page)
        if self.endless:
            rows = [{"id": f"filler-{page_index}-{n}", "isDeleted": False}
                    for n in range(page_size)]
            return rows, page_size * 1000
        rows = [doc for (org, _), doc in self.documents.items() if org == org_id]
        rows.sort(key=lambda doc: doc["id"])
        if self.conflicting_id is not None and page_index == 1:
            # The same document, answered differently, twice inside one walk: pages shift
            # under a live listing, so a duplicate is survivable and a contradiction is not.
            double = next((doc for doc in rows if doc["id"] == self.conflicting_id), None)
            if double is not None:
                page = rows[page_index * page_size:(page_index + 1) * page_size]
                flipped = {**double, "isDeleted": not double["isDeleted"],
                           "deletedAt": None if double["isDeleted"] else
                           "2026-11-17T10:00:00.000Z"}
                return [dict(doc) for doc in page] + [flipped], (
                    self.count_override if self.count_override is not None
                    else len(rows) + 1)
        page = rows[page_index * page_size:(page_index + 1) * page_size]
        total = self.count_override if self.count_override is not None else len(rows)
        if self.total_pages is not None:
            # What the endpoint declares about THIS page. `None` is a server that answers no
            # total at all; a list whose entries differ is a total that moves mid-walk. Both
            # are arms of the trust predicate, and neither can be built out of one number.
            total = self.total_pages[min(page_index, len(self.total_pages) - 1)]
        return [self._as_listed(doc) for doc in page], total

    def _as_listed(self, document: dict) -> dict:
        """How the listing answers for one document, which a stale page gets wrong.

        Separate from `get_document` on purpose: the two endpoints are separate calls to a
        server that may be serving a stale index, and the pass has to be tested against the
        case where they disagree about the same record.
        """
        if document["id"] in self.stale_listing:
            return {**document, "isDeleted": True,
                    "deletedAt": "2026-11-17T10:00:00.000Z"}
        return dict(document)

    async def get_document(self, org_id: str, document_id: str) -> dict:
        self.get_calls += 1
        if document_id in self.raise_on_get:
            raise self.raise_on_get[document_id]
        document = self.documents.get((org_id, document_id))
        if document is None:
            raise PapraError(f"GET document failed: 404 {document_id} not found", 404)
        return dict(document)

    async def update_document(self, org_id: str, document_id: str, **body) -> dict:
        self.patch_calls += 1
        raise AssertionError(f"the deletion pass wrote to Papra: {body}")

    async def update_content(self, org_id: str, document_id: str, content: str) -> None:
        self.patch_calls += 1
        raise AssertionError("the deletion pass wrote Papra's content")

    async def download_file(self, org_id: str, document_id: str) -> bytes:
        return PDF_BYTES

    async def aclose(self) -> None:
        return None


_MISSING = object()


class FakeIndex(FakeKreuzakt):
    """The real `KreuzaktClient` over a scripted server, with a flaky DELETE.

    Reused from Task 6 deliberately: `delete_document` and `get_document` then run through
    Task 5's real client, so the two status facts this pass depends on are the client's own
    — a 404 on a DELETE counts as done, and a transport failure arrives as a
    `KreuzaktError` with `status is None`, which is what distinguishes "the index said no"
    from "the index is not answering".
    """

    def __init__(self):
        super().__init__()
        self.delete_failures: dict[int, object] = {}
        self.all_deletes_fail: object = _MISSING
        self.unreachable = False
        self.unreadable: set[int] = set()   # these ids alone stop answering

    def fail_delete(self, document_id, status: int | None = 500) -> None:
        """Make a DELETE fail. `None` clears every scripted failure, as the sketch does."""
        if document_id is None:
            self.all_deletes_fail = _MISSING
            self.delete_failures.clear()
            return
        self.delete_failures[document_id] = status

    def _delete(self, document_id: int):
        status = self.delete_failures.get(document_id, self.all_deletes_fail)
        if status is _MISSING:
            return super()._delete(document_id)
        if status is None:
            raise httpx.ConnectError("connection refused")
        return self._json({"error": "server_error",
                           "message": f"cannot delete {document_id}"}, status=status)

    def serve_document(self, document_id: int) -> None:
        """Script a document at an id the upload flow would not have minted."""
        self.documents[document_id] = {**self._document_template, "id": document_id}

    def make_unreachable(self) -> None:
        """Nothing answers: every read AND every delete fails at the transport.

        Reads alone are not enough — the cleanup branch under test is a failing DELETE, whose
        shape is `KreuzaktError` with `status is None`.
        """
        self.unreachable = True
        self.all_deletes_fail = None

    def recover(self) -> None:
        """Undo `make_unreachable`: the index answers reads and deletes again.

        The outage tests need both halves. A deferred delete that is never retried proves
        nothing about a veto that is supposed to fire *after* the outage ends, which is the
        sequence the Papra re-ask exists for.
        """
        self.unreachable = False
        self.all_deletes_fail = _MISSING

    def _document(self, document_id: int):
        if document_id in self.unreadable:
            # A partial outage: this id, and only this id, stops answering. `unreachable` is the
            # whole index down; the difference matters because one unanswerable id is neither a
            # verdict about the document nor a reason to read the whole corpus again.
            raise httpx.ConnectError(f"connection refused for {document_id}")
        if self.unreachable:
            raise httpx.ConnectError("connection refused")
        if document_id not in self.documents \
                and document_id not in self.missing_document_ids:
            # Any id answers by default. These tests bind integers the upload flow never
            # minted, and a fake that 404'd them all would make the probe's verdict a
            # property of the fake's bookkeeping rather than of the scenario.
            self.serve_document(document_id)
        return super()._document(document_id)

    async def aclose(self) -> None:
        await self.client.aclose()


# --- fixtures and seeding ---------------------------------------------------------

@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=ORG, name="Org A")
    yield store
    store.close()


@pytest.fixture
def papra():
    return FakePapra()


@pytest.fixture
async def index():
    fake = FakeIndex()
    yield fake
    await fake.aclose()


@pytest.fixture(autouse=True)
def papra_is_never_written_or_deleted(papra):
    """Y3, applied to every test in this module including the successful ones.

    Papra is the record and the propagation is one-way, so an index-side delete that
    reached back and patched or deleted the archive would be the one bug in this task that
    destroys data the user never touched. Asserted here rather than per test so a new test
    cannot forget it.
    """
    yield
    assert papra.patch_calls == 0, "the deletion pass patched a Papra document"
    assert papra.delete_calls == 0, "the deletion pass deleted a Papra document"


def seed_ledger(store, *, org: str = ORG, document_id: str = DOC_ID,
                file_hash: str = HASH, name: str | None = NAME,
                kreuzakt_id: int | None = None,
                generation: int | None = None) -> None:
    """A ledger row as intake would have left it, optionally bound to a Kreuzakt id."""
    stamp = store.get_generation() if generation is None else generation
    store.ledger_upsert_reference(papra_org_id=org, papra_document_id=document_id,
                                  file_hash=file_hash, papra_original_name=name)
    if kreuzakt_id is not None:
        store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=document_id,
                                     kreuzakt_id=kreuzakt_id, generation=stamp)


def seed_reference(store, papra, *, org: str = ORG, document_id: str = DOC_ID,
                   file_hash: str = HASH, kreuzakt_id: int | None = None,
                   deleted: bool = False, generation: int | None = None) -> None:
    """One document on both sides: the ledger row and the listing entry that describes it."""
    seed_ledger(store, org=org, document_id=document_id, file_hash=file_hash,
                kreuzakt_id=kreuzakt_id, generation=generation)
    papra.add(document_id, org=org, deleted=deleted)


def seed_deleted_in_papra(store, papra, *, count: int, kreuzakt_id_start: int = 100,
                          org: str = ORG) -> list[str]:
    """`count` documents Papra says are in the trash, each with its own index document."""
    ids = []
    for n in range(count):
        document_id = f"doc-{n:03d}"
        seed_reference(store, papra, org=org, document_id=document_id,
                       file_hash=f"{n:064x}", kreuzakt_id=kreuzakt_id_start + n,
                       deleted=True)
        ids.append(document_id)
    return ids


@pytest.fixture
def small_pages(monkeypatch):
    """Force the walk to page over rows two at a time.

    The paging predicates are about the *shape* of a walk, and asserting them against the
    production page size would mean building a listing of a hundred documents per test. The
    page size is a constant of the pass, so the test moves the constant rather than faking
    the endpoint harder.
    """
    monkeypatch.setattr(indexing, "LISTING_PAGE_SIZE", 2)


@pytest.fixture(autouse=True)
def walks_are_always_due(monkeypatch):
    """Turn the listing walk's own cadence off for the rest of this module.

    Most tests here ask what the pass decides when it looks, and several look twice in the same
    instant. With the production gate in place the second look would be skipped, and a test suite
    that quietly asserts on skipped passes is worse than none. The cadence is pinned where it
    belongs — in the two tests that name it — and everything else asks its own question.
    """
    monkeypatch.setattr(indexing, "LISTING_WALK_INTERVAL_S", 0)


def later(seconds: int) -> str:
    """A timestamp `seconds` from now, for tests that need one of the clocks to turn."""
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat()


def document_reads(index) -> int:
    """How many Kreuzakt documents this fake was asked about — the cost of a walk."""
    return sum(1 for method, route in index.calls
               if method == "GET" and route.startswith("/api/documents/"))


def due(store, *, n: int = 1) -> str:
    """A timestamp past every backoff this task can produce, so a pass is due."""
    return (datetime.now(UTC) + timedelta(days=n)).isoformat()


def cleanup_state(store, document_id: str = DOC_ID, org: str = ORG) -> str:
    ref = store.ledger_get(org, document_id)
    return ref.cleanup_state if ref else "<no row>"


# --- the shared predicate (Y8) ----------------------------------------------------

@pytest.mark.parametrize("document, expected, why", [
    ({"isDeleted": True, "deletedAt": None}, True, "the trash flag alone"),
    ({"isDeleted": False, "deletedAt": "2026-11-17T10:00:00.000Z"}, True,
     "the timestamp alone, which is what Papra's listing carries"),
    ({"isDeleted": True, "deletedAt": "2026-11-17T10:00:00.000Z"}, True, "both"),
    ({"isDeleted": False, "deletedAt": None}, False, "a live document"),
    ({"isDeleted": None, "deletedAt": None}, False, "nulls are not deletions"),
    ({}, False, "a record that carries neither field"),
])
def test_papra_document_is_deleted_reads_both_signals(document, expected, why):
    """One table for a predicate four call sites share, so a mistake names itself.

    `isDeleted` and `deletedAt` are two independent ways Papra says "trash", and the index
    job, the convergence pass, metadata delivery and this pass all read the answer. Without
    a test of its own, reading only one of them would surface as four unrelated failures —
    and in the worst direction, as a write into a document the user deleted.
    """
    assert indexing.papra_document_is_deleted(document) is expected, why


# --- reference counting: the mutation gate (Y1) -----------------------------------

async def test_shared_document_survives_one_reference_being_deleted(store, papra, index):
    """Two buckets, identical bytes, one Kreuzakt document: one reference may not kill it.

    The reference count is the whole gate, so both halves of it are asserted: nothing is
    queued for the reference that went away first, and nothing is destroyed upstream. The
    first assertion is what a mutation that deletes on the first vanished reference dies
    on — an assertion about `index.deleted` alone would survive it, because the cleanup
    pass has its own revalidation and would refuse the intent it was handed.
    """
    seed_reference(store, papra, org=ORG, document_id="doc-1",
                   kreuzakt_id=7, deleted=True)
    seed_reference(store, papra, org=OTHER_ORG, document_id="doc-2",
                   kreuzakt_id=7, deleted=False)

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.refs_marked == 1
    assert store.ledger_get(ORG, "doc-1").deleted_at is not None
    assert store.ledger_get(ORG, "doc-1").cleanup_state == "none", \
        "a shared document must not even be queued for deletion"
    assert store.ledger_stats()["cleanup_pending"] == 0
    assert index.deleted == []
    assert [r.papra_document_id for r in
            store.ledger_resolve([7], generation=store.get_generation())] == ["doc-2"]


async def test_last_reference_deletion_is_queued_then_executed(store, papra, index):
    """The intent is written to the ledger, and the cleanup pass is what destroys anything."""
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)

    result = await indexing.reconcile_deletions(store, papra, index)
    assert result.upstream_pending == 1
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.cleanup_state == "pending"
    assert ref.cleanup_kreuzakt_id == 7
    assert ref.cleanup_generation == store.get_generation()
    assert index.deleted == [], "the marking pass does not destroy anything itself"

    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert cleanup == CleanupResult(attempted=1, deleted=1, deferred=0, failed=0)
    assert index.deleted == [7]
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "done"


async def test_the_cleanup_counts_account_for_everything_drained(store, papra, index):
    """One definition, one identity: every drained row lands in exactly one bucket.

    `attempted` counts the rows the pass took off the due queue and judged — not the calls
    it made — because the number an operator needs is "how many intents did this pass
    handle", and a pass that refused 40 of them is not a pass that did nothing. The identity
    is pinned so a later field cannot be smuggled in as a fourth meaning of `attempted`.
    """
    seed_reference(store, papra, document_id="doc-1", kreuzakt_id=7, deleted=True)
    seed_reference(store, papra, document_id="doc-2", file_hash="ab" * 32,
                   kreuzakt_id=8, deleted=True)
    seed_reference(store, papra, document_id="doc-3", file_hash="cd" * 32,
                   kreuzakt_id=9, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    index.fail_delete(8)
    papra.documents[(ORG, "doc-3")]["isDeleted"] = False     # restored: nothing owed
    papra.documents[(ORG, "doc-3")]["deletedAt"] = None
    await indexing.reconcile_deletions(store, papra, index)   # the ledger learns about it

    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert cleanup == CleanupResult(attempted=3, deleted=1, deferred=1, failed=1)
    assert cleanup.attempted == cleanup.deleted + cleanup.deferred + cleanup.failed
    assert index.deleted == [7], "the restored document's index entry must survive"


async def test_cleanup_refuses_a_queued_delete_once_a_reference_resolves_again(
        store, papra, index, caplog):
    """The count is taken again at execution time, because the queue is a claim about later.

    A live reference that appears after the intent was queued — restored, or re-indexed onto
    the same id because Kreuzakt deduplicates identical bytes — outvotes it. This is the
    guard that holds when the queueing side is wrong, which is why it is tested separately
    from the queueing side: two guards, two tests, or one of them can rot unnoticed.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "pending"

    # A second bucket starts holding the same bytes, and the index re-binds it to id 7.
    seed_ledger(store, org=OTHER_ORG, document_id="doc-2", kreuzakt_id=7)
    caplog.set_level(logging.INFO, logger="papra_suite.store")

    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert cleanup == CleanupResult(attempted=1, deleted=0, deferred=1, failed=0)
    assert index.deleted == []
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "skipped"
    assert any("still resolves" in record.getMessage() for record in caplog.records)


async def test_a_restore_during_the_outage_stops_the_queued_delete_from_firing(store, papra,
                                                                              index):
    """The queue's own vetoes read the ledger, and the ledger is the thing that was stale.

    The sequence this exists for, with no brake or cadence involved: the document is trashed,
    the reference is marked and the delete is queued; an index outage defers the delete; the
    person restores the document in Papra; the index comes back. Every ledger-side veto still
    says "deleted" — only Papra knows otherwise, and until this round nothing on the execution
    path asked Papra. So the delete fires against a document the user kept, and its text leaves
    search until a restore pass unwinds it, up to a day later.

    One GET per queued delete closes it independently of any cadence or brake.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "pending"

    index.make_unreachable()                        # the delete is deferred, not attempted
    deferred = await indexing.run_cleanup(store, papra, index, now=due(store))
    assert (deferred.deleted, deferred.failed) == (0, 0)
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "pending"

    papra.documents[(ORG, DOC_ID)]["isDeleted"] = False
    papra.documents[(ORG, DOC_ID)]["deletedAt"] = None   # restored in Papra, during the outage
    index.recover()                                 # ...and now the index is back

    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert cleanup.deleted == 0, "a delete fired against a document the user restored"
    assert index.deleted == [], "the index entry of a live document was destroyed"
    assert 7 in index.documents, "the survivor's index entry is gone"
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "skipped"


async def test_a_queued_delete_waits_when_papra_will_not_answer_about_the_document(store, papra,
                                                                                  index):
    """The re-ask fails closed, and a non-answer is not an attempt.

    "Papra did not reply" is not evidence about a document, so the delete waits; and spending
    the retry allowance on it would walk a corpus of queued deletes to `cleanup_state='failed'`
    through a Papra outage — the mistake this file already refuses twice elsewhere.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    papra.raise_on_get[DOC_ID] = PapraError("GET document failed: 500 upstream", 500)

    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert cleanup == CleanupResult(attempted=1, deleted=0, deferred=1, failed=0)
    assert index.deleted == []
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.cleanup_state == "pending" and ref.cleanup_attempts == 0, \
        "an outage on the read side was spent as an attempt on the delete"


async def test_the_upstream_delete_waits_for_the_last_reference_across_passes(
        store, papra, index):
    """The shared case, taken to its end: the survivor keeps its text until it too goes."""
    seed_reference(store, papra, org=ORG, document_id="doc-1", kreuzakt_id=7)
    seed_reference(store, papra, org=OTHER_ORG, document_id="doc-2", kreuzakt_id=7)
    papra.documents[(ORG, "doc-1")]["isDeleted"] = True

    await indexing.reconcile_deletions(store, papra, index)
    await indexing.run_cleanup(store, papra, index, now=due(store))
    assert index.deleted == []
    assert store.ledger_resolve([7], generation=store.get_generation())

    papra.documents[(OTHER_ORG, "doc-2")]["isDeleted"] = True
    await indexing.reconcile_deletions(store, papra, index)
    await indexing.run_cleanup(store, papra, index, now=due(store))

    assert index.deleted == [7]
    assert store.ledger_resolve([7], generation=store.get_generation()) == []


# --- the queue-side decision (Y2) -------------------------------------------------

async def test_a_stale_generation_binding_marks_the_reference_and_queues_nothing(
        store, papra, index, caplog):
    """`mark_deleted` answers `(-1, -1)` for a stale binding, and `-1` is not an id.

    The row is still soft-deleted — the reference is gone whatever index it was bound
    under — but a stale integer names nothing, so it cannot authorise anything. Queuing
    `-1` would put a bogus delete in the durable queue that passes every later
    revalidation (nothing resolves to `-1`, and the bump that made it stale makes the
    queued generation current again by the next rebuild) and spends five attempts on
    nothing. The outcome is counted separately so a corpus full of stale bindings is
    visible rather than looking like a corpus with nothing to do.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    store.bump_generation()                    # the binding is from a destroyed index
    caplog.set_level(logging.WARNING, logger="papra_suite.indexing")

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.refs_marked == 1
    assert result.upstream_pending == 0
    assert result.stale_bindings == 1
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.deleted_at is not None
    assert ref.cleanup_state == "none", "a stale binding must not reach the durable queue"
    assert store.ledger_stats()["cleanup_pending"] == 0
    assert any("stale" in record.getMessage() for record in caplog.records)


async def test_an_unbound_reference_queues_nothing(store, papra, index):
    """A reference that never reached the index leaves nothing upstream to destroy."""
    seed_reference(store, papra, kreuzakt_id=None, deleted=True)

    result = await indexing.reconcile_deletions(store, papra, index)

    assert (result.refs_marked, result.upstream_pending, result.stale_bindings) == (1, 0, 0)
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "none"


async def test_no_queued_cleanup_ever_carries_a_nonpositive_kreuzakt_id(store, papra,
                                                                        index):
    """The guard as a hard assertion: whatever the pass decides, `-1` is never queued.

    Every mark in this run is one the queueing side must refuse (stale binding, no id), so
    a guard that only happens to work fails here. Asserted on the store's own call, because
    the hazard is the row the queue ends up holding, not the decision that wrote it.
    """
    queued: list[tuple] = []
    original = store.ledger_queue_cleanup

    def spy(**kwargs):
        queued.append(kwargs)
        return original(**kwargs)

    store.ledger_queue_cleanup = spy
    seed_reference(store, papra, document_id="doc-stale", kreuzakt_id=7, deleted=True)
    seed_reference(store, papra, document_id="doc-unbound", file_hash="cd" * 32,
                   kreuzakt_id=None, deleted=True)
    seed_reference(store, papra, org=OTHER_ORG, document_id="doc-live",
                   file_hash="ef" * 32, kreuzakt_id=9, deleted=True)
    store.bump_generation()                    # doc-stale's binding is now meaningless
    # doc-live is rebound under the new index, so exactly one mark here carries a real id and
    # the pass has something to queue. The point is that the two which do not are refused, not
    # that nothing was ever queued.
    store.ledger_set_kreuzakt_id(papra_org_id=OTHER_ORG, papra_document_id="doc-live",
                                 kreuzakt_id=9, generation=store.get_generation())

    await indexing.reconcile_deletions(store, papra, index)

    assert [call["kreuzakt_id"] for call in queued], "the guard needs a call to judge"
    assert all(call["kreuzakt_id"] > 0 for call in queued), queued


# --- one-way propagation (Y3) -----------------------------------------------------

async def test_a_kreuzakt_side_deletion_never_touches_papra(store, papra, index):
    """An index-side delete makes the document unfindable, and that is the whole damage.

    Papra holds the bytes and the record, so the honest response is to stop claiming the
    document is indexed: unbind the reference so it becomes eligible for re-indexing from
    Papra's bytes. Patching or deleting in Papra because the index lost a document would be
    the record inheriting the index's mistake.
    """
    seed_reference(store, papra, kreuzakt_id=7)
    index.missing_document_ids = {7}

    await indexing.reconcile_deletions(store, papra, index)
    # The probe is the detector for a Kreuzakt-side deletion, and it is its own pass now: the
    # deletion pass has no business running it, and cannot — Papra being readable says nothing
    # about our bindings, and vice versa.
    await indexing.probe_index_liveness(store, index, store.get_generation())

    assert papra.patch_calls == 0 and papra.delete_calls == 0
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.kreuzakt_id is None, "a dead binding must not survive as a live one"
    assert store.ledger_needs_index(generation=store.get_generation())


# --- the durable intent (Y4) ------------------------------------------------------

async def test_failed_upstream_delete_retries_from_the_ledger_after_restart(store, papra,
                                                                            index):
    """Crash-durable: the intent lives in the ledger, not in a variable.

    A delete that failed is not a delete that was abandoned. The second call below stands
    in for a new process — nothing was carried over but the row.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    index.fail_delete(7)

    await indexing.reconcile_deletions(store, papra, index)
    await indexing.run_cleanup(store, papra, index, now=due(store))

    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.cleanup_state == "pending" and ref.cleanup_attempts == 1
    assert index.deleted == []

    index.fail_delete(None)                    # the outage ends; a new process starts
    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store, n=8))

    assert cleanup.deleted == 1
    assert index.deleted == [7]
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "done"


async def test_cleanup_backs_off_by_the_attempt_number(store, papra, index):
    """`defer = CLEANUP_BACKOFF_S * attempts`, so a failing delete gets quieter, not louder."""
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    index.fail_delete(7)
    first = due(store)

    await indexing.run_cleanup(store, papra, index, now=first)
    next_at = store.ledger_get(ORG, DOC_ID).cleanup_next_at

    expected = (datetime.fromisoformat(first)
                + timedelta(seconds=CLEANUP_BACKOFF_S * 1)).isoformat()
    assert next_at == expected, "a backoff that is not the store's own form sorts wrong"


async def test_cleanup_stops_erroring_after_five_attempts(store, papra, index, caplog):
    """Five attempts, then it stops and says so exactly once.

    "Still trying" repeated every pass is noise; "we have stopped trying" is the fact an
    operator has to hear once, because from that point only `papra-suite ledger --cleanup`
    will ever delete the document again.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    index.fail_delete(7)
    caplog.set_level(logging.WARNING, logger="papra_suite.store")

    for step in range(1, CLEANUP_MAX_ATTEMPTS + 2):
        result = await indexing.run_cleanup(store, papra, index, now=due(store, n=step * 8))
        assert result.attempted == (1 if step <= CLEANUP_MAX_ATTEMPTS else 0)

    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.cleanup_state == "failed" and ref.cleanup_attempts == CLEANUP_MAX_ATTEMPTS
    permanent = [record for record in caplog.records
                 if "cleanup failed permanently" in record.getMessage()]
    assert len(permanent) == 1
    assert store.ledger_stats()["cleanup_pending"] == 1, \
        "the debt stays counted when the retries stop"


async def test_an_already_deleted_upstream_document_is_a_finished_delete(store, papra,
                                                                         index):
    """A 404 from the DELETE is the wanted end state, not a failure to retry five times.

    Somebody may have cleared the document in the index by hand; burning the allowance on
    it would land a perfectly clean ledger in `failed` and put an ERROR in the log that
    describes nothing.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    index.fail_delete(7, 404)

    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert cleanup == CleanupResult(attempted=1, deleted=1, deferred=0, failed=0)
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.cleanup_state == "done" and ref.cleanup_attempts == 0


async def test_an_unreachable_index_does_not_spend_the_allowance(store, papra, index,
                                                                 caplog):
    """"There is no answer" must not be spent like "the answer is no".

    A transport failure carries no status. Treating it as an ordinary failure would let an
    index outage walk every queued delete in the corpus to `failed` in five passes, and the
    operator would come back to a debt that only a forced pass can retry. So the pass
    stops, records nothing, and says so at ERROR.
    """
    seed_reference(store, papra, document_id="doc-1", kreuzakt_id=7, deleted=True)
    seed_reference(store, papra, document_id="doc-2", file_hash="ab" * 32,
                   kreuzakt_id=8, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    index.make_unreachable()
    caplog.set_level(logging.ERROR, logger="papra_suite.indexing")

    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert cleanup == CleanupResult(attempted=2, deleted=0, deferred=2, failed=0)
    for document_id in ("doc-1", "doc-2"):
        ref = store.ledger_get(ORG, document_id)
        assert ref.cleanup_state == "pending" and ref.cleanup_attempts == 0
    assert any("not answering" in record.getMessage() for record in caplog.records)


async def test_an_intent_for_an_already_destroyed_document_closes_rather_than_retrying(
        store, papra, index):
    """The index has already forgotten that document: the debt is paid, not owed.

    Confirmed by reading the id the intent names, at the moment the intent is written. The
    alternative is a queued delete that fails five times against a 404 and lands in
    `failed`, which reads as an outage and is nothing of the sort.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    index.missing_document_ids = {7}

    result = await indexing.reconcile_deletions(store, papra, index)

    assert (result.upstream_deleted, result.upstream_pending) == (1, 0)
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "done"
    assert store.ledger_stats()["cleanup_pending"] == 0


async def test_cleanup_refuses_a_clock_that_is_not_the_stores_own_form(store, papra,
                                                                      index):
    """Backoffs are compared as text, so a naive `now` is a silently wrong schedule.

    The store refuses a `next_at` it could not have produced itself; this pins that the
    cleanup pass lets that refusal through instead of swallowing it into a pass that
    reported nothing and scheduled nothing.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    index.fail_delete(7)

    with pytest.raises(ValueError, match="now"):
        await indexing.run_cleanup(store, papra, index, now="2026-11-17 10:00:00")


# --- what makes a listing untrustworthy (Y5) --------------------------------------

async def test_a_failing_listing_aborts_the_whole_pass(store, papra, index, caplog):
    """Never infer a mass deletion from a paging error."""
    seed_reference(store, papra, kreuzakt_id=7)
    papra.raise_on_list = PapraError("GET documents failed: 503 upstream", 503)
    caplog.set_level(logging.ERROR, logger="papra_suite.indexing")

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.aborted is True and result.refs_marked == 0
    assert store.ledger_get(ORG, DOC_ID).deleted_at is None
    assert store.ledger_stats()["cleanup_pending"] == 0
    assert any("skipping deletion pass" in record.getMessage() for record in caplog.records)


async def test_a_short_page_the_servers_own_count_disputes_aborts_the_pass(store, papra,
                                                                          index):
    """Two rows back out of a total the same response calls five.

    A truncated page read as a finished walk is the shape of "everything after row two was
    deleted", which is the difference between pruning one document and pruning the index.
    """
    papra.add("doc-1")
    seed_ledger(store, document_id="doc-1", kreuzakt_id=7)
    papra.count_override = 5                    # the listing stopped early

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.aborted is True and result.refs_marked == 0
    assert store.ledger_get(ORG, DOC_ID).deleted_at is None


async def test_an_empty_listing_for_a_populated_bucket_is_never_read_as_a_purge(store, papra,
                                                                                index):
    """A bucket the ledger populated cannot answer empty and be believed.

    This is a circuit breaker, not a proof: the last document of a bucket really can be purged,
    and then that bucket's propagation stops until its listing answers again. The trade is
    deliberate — a purge of everything is exactly the shape a broken listing takes, and being
    wrong in that direction costs the bucket's whole index. How wide the stop reaches is the
    subject of the next tests; what this one pins is that the references in the unreadable bucket
    keep their bindings and their index entries whatever else the pass does.
    """
    seed_reference(store, papra, kreuzakt_id=7)
    papra.documents.clear()                     # Papra has hard-deleted it, all of it

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.refs_marked == 0
    assert store.ledger_get(ORG, DOC_ID).deleted_at is None
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id == 7, "nothing was unbound either"
    assert store.ledger_stats()["cleanup_pending"] == 0


async def test_a_malformed_listing_page_aborts_the_pass(store, papra, index):
    """Rows without an id are not a listing; they are a proxy, a login page or an error."""
    seed_reference(store, papra, kreuzakt_id=7)
    papra.malformed_page = [{"id": None}, "not-a-document"]

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.aborted is True and result.refs_marked == 0
    assert store.ledger_get(ORG, DOC_ID).deleted_at is None


async def test_a_listing_that_disagrees_with_itself_aborts_the_pass(store, papra, index,
                                                                    small_pages):
    """The same id answered two different ways inside one walk: neither answer is evidence.

    A document appearing twice is ordinary — pages shift under a live listing — so the
    signal is not the duplicate, it is the duplicate disagreeing with itself. Trusting the
    second answer would mark a live document; trusting the first would ignore a deleted one.
    """
    papra.add("doc-a")
    papra.add("doc-b", deleted=True)
    papra.count_override = 3                    # accounts for the row the walk sees twice
    seed_ledger(store, document_id="doc-a", kreuzakt_id=7)
    papra.conflicting_id = "doc-a"              # page 2 contradicts page 1 about doc-a

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.aborted is True and result.refs_marked == 0


async def test_a_listing_walk_that_never_ends_aborts_the_pass(store, papra, index):
    """An endpoint that ignores `pageIndex` must not be walked into a mass deletion."""
    seed_reference(store, papra, kreuzakt_id=7)
    papra.endless = True

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.aborted is True and result.refs_marked == 0
    assert store.ledger_get(ORG, DOC_ID).deleted_at is None


async def test_an_aborted_pass_leaves_the_ledger_exactly_as_it_was(store, papra, index):
    """`aborted` means nothing was written, not "we wrote some of it".

    The pass is read-only until every bucket has been walked, so the abort is structural
    rather than a rollback, and this asserts it on the columns the delete path owns.
    """
    seed_deleted_in_papra(store, papra, count=3)
    seed_reference(store, papra, org=OTHER_ORG, document_id="doc-other",
                   file_hash="ff" * 32, kreuzakt_id=500)
    before = {doc: store.ledger_get(ORG, doc) for doc in ("doc-000", "doc-001", "doc-002")}
    papra.raise_on_list = PapraError("GET documents failed: 500 boom", 500)

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.aborted is True
    for document_id, ref in before.items():
        after = store.ledger_get(ORG, document_id)
        assert after.deleted_at == ref.deleted_at
        assert after.cleanup_state == ref.cleanup_state == "none"
        assert after.kreuzakt_id == ref.kreuzakt_id


async def test_a_hard_deleted_document_is_marked_from_its_own_404(store, papra, index):
    """Papra can purge. Absence from the listing is a candidate; the 404 is the evidence.

    The listing alone is never enough — an id that stopped being listed is also what a
    broken listing produces — but a detail read that answers 404 for that specific document
    is Papra saying it has this one, individually.
    """
    seed_reference(store, papra, kreuzakt_id=7)
    papra.add("doc-keeper")                     # the bucket is not empty...
    papra.documents.pop((ORG, DOC_ID))          # ...but this document is gone from it

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.refs_marked == 1 and result.upstream_deleted + result.upstream_pending == 1
    assert store.ledger_get(ORG, DOC_ID).deleted_at is not None


async def test_a_stale_listing_row_is_refused_by_the_detail_read(store, papra, index,
                                                                 caplog):
    """The listing says trash, the document says live: the live answer wins.

    A restored document caught mid-flight by a stale page would otherwise be marked
    deleted, stop resolving for every token, and — for the last reference — be queued for
    the upstream delete that takes its text away.
    """
    seed_reference(store, papra, kreuzakt_id=7)
    papra.stale_listing = {DOC_ID}                    # the page still says trash
    papra.add("doc-keeper")
    caplog.set_level(logging.WARNING, logger="papra_suite.indexing")

    result = await indexing.reconcile_deletions(store, papra, index)

    assert (result.refs_marked, result.restored) == (0, 0)
    assert store.ledger_get(ORG, DOC_ID).deleted_at is None
    assert store.ledger_stats()["cleanup_pending"] == 0
    assert any("refused" in record.getMessage() for record in caplog.records)


async def test_a_detail_read_that_errors_defers_the_mark(store, papra, index):
    """A document whose detail will not answer is a document nothing is known about."""
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    papra.add("doc-keeper")
    papra.raise_on_get[DOC_ID] = PapraError("GET document failed: 503 upstream", 503)

    result = await indexing.reconcile_deletions(store, papra, index)

    assert (result.refs_marked, result.restored) == (0, 0)
    assert store.ledger_get(ORG, DOC_ID).deleted_at is None


async def test_a_refused_candidate_does_not_consume_the_deletion_budget(store, papra,
                                                                       index):
    """Probing and marking are budgeted separately, or one bad head starves the tail.

    The first candidate in sorted order still answers live, so it costs a probe and no
    mark; the document behind it has to be able to reach the cap.
    """
    seed_deleted_in_papra(store, papra, count=3, kreuzakt_id_start=10)
    papra.raise_on_get["doc-000"] = PapraError("GET document failed: 503 upstream", 503)

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.refs_marked == 2, "a candidate that answered nothing must not " \
        "cost the documents behind it their place in the budget"
    assert store.ledger_get(ORG, "doc-000").deleted_at is None


async def test_the_candidate_window_rotates_so_a_refused_head_cannot_starve_the_tail(
        store, papra, index):
    """Refusers stay candidates, so a fixed window would probe them forever.

    Enough documents refuse the detail read to fill one probe window, with two deletable
    documents behind them. Without rotation the pass would report `refs_marked=0` every
    pass, forever, and look like a corpus with nothing to do.
    """
    blocked = indexing.MAX_CANDIDATE_PROBES
    seed_deleted_in_papra(store, papra, count=blocked + 2, kreuzakt_id_start=200)
    for n in range(blocked):
        papra.raise_on_get[f"doc-{n:03d}"] = PapraError(
            "GET document failed: 503 upstream", 503)

    first = await indexing.reconcile_deletions(store, papra, index)
    second = await indexing.reconcile_deletions(store, papra, index)

    assert first.refs_marked == 0
    assert second.refs_marked == 2, "the window never moved past the refusals"
    assert [ref.papra_document_id for ref in _live_refs(store)] == [
        f"doc-{n:03d}" for n in range(blocked)]


def _live_refs(store, org: str = ORG):
    """Live references only: `ledger_org_refs` deliberately returns deleted rows too."""
    return [ref for ref in store.ledger_org_refs(org_id=org, limit=1000)
            if ref.deleted_at is None]


# --- the blast radius (Y5) --------------------------------------------------------

async def test_deletion_pass_is_capped_per_pass(store, papra, index, caplog):
    """A mass deletion is loud and bounded, never efficient."""
    seed_deleted_in_papra(store, papra, count=MAX_DELETIONS_PER_PASS + 5)
    caplog.set_level(logging.ERROR, logger="papra_suite.indexing")

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.refs_marked == MAX_DELETIONS_PER_PASS
    assert result.capped == MAX_DELETIONS_PER_PASS
    assert result.aborted is False
    assert any("deletion pass capped" in record.getMessage() for record in caplog.records)
    remaining = [ref.papra_document_id for ref in _live_refs(store)]
    assert len(remaining) == 5


async def test_the_cap_bounds_the_upstream_deletes_it_queues(store, papra, index):
    """The cap is on deletions, so the durable queue cannot be filled past it either."""
    seed_deleted_in_papra(store, papra, count=MAX_DELETIONS_PER_PASS + 5)

    result = await indexing.reconcile_deletions(store, papra, index)
    await indexing.run_cleanup(store, papra, index, now=due(store))

    assert result.upstream_deleted + result.upstream_pending <= MAX_DELETIONS_PER_PASS
    assert len(index.deleted) <= MAX_DELETIONS_PER_PASS


# --- restore (Y6, Y7, Y9) ---------------------------------------------------------

async def test_a_restore_clears_the_flag_and_cancels_the_queued_delete(store, papra, index,
                                                                      caplog):
    """A durable intent must not outlive the reason for it."""
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    await indexing.run_cleanup(store, papra, index, now=due(store))
    assert index.deleted == [7]

    papra.documents[(ORG, DOC_ID)]["isDeleted"] = False
    papra.documents[(ORG, DOC_ID)]["deletedAt"] = None
    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.restored == 1
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.deleted_at is None
    assert ref.delivered_generation != store.get_generation() or ref.kreuzakt_id is None
    assert store.ledger_needs_index(generation=store.get_generation())


async def test_a_restore_cancels_a_pending_delete_without_deleting(store, papra, index):
    """Restored before the cleanup ran: the intent is skipped, and nothing is destroyed."""
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "pending"

    papra.documents[(ORG, DOC_ID)]["isDeleted"] = False
    papra.documents[(ORG, DOC_ID)]["deletedAt"] = None
    await indexing.reconcile_deletions(store, papra, index)
    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert index.deleted == []
    assert cleanup.deleted == 0
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "skipped"


async def test_a_restore_resets_a_permanently_failed_index_job(store, papra, index):
    """A restore has to be able to ask for the work again, and the queue says no by default.

    `tests/test_metadata_delivery.py::test_a_permanently_failed_index_row_makes_the_next_enqueue_a_noop`
    pins the trap: `enqueue_job` is `INSERT OR IGNORE` against the unique idempotency index,
    so a `failed` row for these bytes turns every re-enqueue — restore, unbind, backfill —
    into a silent no-op that reports the old id. Here the restore path is shown resetting
    that row so the document is claimable again. Task 11's partial unique index is the
    durable fix; the reset is today's, and it is the reason a restore is not a lie.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    await indexing.run_cleanup(store, papra, index, now=due(store))
    job_id = store.enqueue_job(kind="index", papra_org_id=ORG, document_id=DOC_ID,
                               content_hash=HASH, payload={"file_hash": HASH})
    store.mark_job_failed(job_id, "document deleted in papra", max_attempts=1)
    assert store.get_job(job_id).status == "failed"

    papra.documents[(ORG, DOC_ID)]["isDeleted"] = False
    papra.documents[(ORG, DOC_ID)]["deletedAt"] = None
    await indexing.reconcile_deletions(store, papra, index)

    assert store.get_job(job_id).status == "pending"
    claimed = store.claim_next_job("index")
    assert claimed is not None and claimed.id == job_id


async def test_a_restore_creates_the_index_job_when_none_exists(store, papra, index):
    """The other half: a document restored after its job was long since reaped."""
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    await indexing.run_cleanup(store, papra, index, now=due(store))

    papra.documents[(ORG, DOC_ID)]["isDeleted"] = False
    papra.documents[(ORG, DOC_ID)]["deletedAt"] = None
    await indexing.reconcile_deletions(store, papra, index)

    claimed = store.claim_next_job("index")
    assert claimed is not None and claimed.document_id == DOC_ID
    assert claimed.content_hash == HASH


async def test_a_restore_after_the_upstream_delete_unbinds_the_dead_binding(store, papra,
                                                                           index):
    """The hazard Task 4 named: a restore that arrives after the delete completed.

    Left bound, the row resolves for every token and hands out a Kreuzakt id that answers
    404 — a search hit that resolves to nothing, which is worse than not being found.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    await indexing.run_cleanup(store, papra, index, now=due(store))
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "done"

    papra.documents[(ORG, DOC_ID)]["isDeleted"] = False
    papra.documents[(ORG, DOC_ID)]["deletedAt"] = None
    await indexing.reconcile_deletions(store, papra, index)

    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.kreuzakt_id is None
    assert store.ledger_resolve([7], generation=store.get_generation()) == []
    assert [r.papra_document_id for r in
            store.ledger_needs_index(generation=store.get_generation())] == [DOC_ID]


async def test_a_restore_leaves_protection_exactly_as_it_was(store, papra, index):
    """A restore is not a human edit, in either direction (Y9).

    `name_protected` records that a person took the field away from the bridge; only
    `ledger_clear_protection` — the operator's `metadata --restore-auto` — may lift it, and
    nothing may set it on a document that merely came back from the trash. A restore that
    cleared a protection would silently re-authorise renaming a document the user renamed;
    one that set a protection would leave a document permanently un-titleable for a reason
    that has nothing to do with a human edit.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    store.ledger_protect(papra_org_id=ORG, papra_document_id=DOC_ID, field="name")
    store.ledger_record_metadata(papra_org_id=ORG, papra_document_id=DOC_ID,
                                 name="Rechnung 4711")
    seed_reference(store, papra, org=OTHER_ORG, document_id="doc-plain", file_hash="ab" * 32,
                   kreuzakt_id=8, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)

    papra.documents[(ORG, DOC_ID)]["isDeleted"] = False
    papra.documents[(ORG, DOC_ID)]["deletedAt"] = None
    papra.documents[(OTHER_ORG, "doc-plain")]["isDeleted"] = False
    papra.documents[(OTHER_ORG, "doc-plain")]["deletedAt"] = None
    await indexing.reconcile_deletions(store, papra, index)

    protected = store.ledger_get(ORG, DOC_ID)
    plain = store.ledger_get(OTHER_ORG, "doc-plain")
    assert (protected.name_protected, protected.written_name) == (True, "Rechnung 4711")
    assert (plain.name_protected, plain.written_name) == (False, None)


async def test_the_delete_path_protects_nothing(store, papra, index):
    """Deleting a reference is not an argument about who owns a field."""
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)

    await indexing.reconcile_deletions(store, papra, index)

    ref = store.ledger_get(ORG, DOC_ID)
    assert (ref.name_protected, ref.date_protected, ref.notes_protected) == (False, False,
                                                                            False)


# --- the index liveness probe: the recovery for a dead binding ---------------------

async def test_the_liveness_probe_unbinds_a_dead_binding_and_readmits_the_job(store, papra,
                                                                             index):
    """A binding the index answers 404 for is not a finished document.

    Task 6 left this state pinned as a limitation: bound, stamped as delivered under the
    current generation, and permanently failed, so `ledger_needs_index` excludes it and the
    sweep, a rebuild and a backfill all walk past it. The probe is what turns that back into
    work — and it has to produce a claimable job, because an unbound row that nothing will
    enqueue is eligible in a sense nobody can act on.
    """
    seed_reference(store, papra, kreuzakt_id=7)
    job_id = store.enqueue_job(kind="index", papra_org_id=ORG, document_id=DOC_ID,
                               content_hash=HASH, payload={"file_hash": HASH})
    store.mark_job_failed(job_id, "kreuzakt document 7 answers 404", max_attempts=1)
    index.missing_document_ids = {7}
    assert store.ledger_needs_index(generation=store.get_generation()) == []

    await indexing.probe_index_liveness(store, index, store.get_generation())

    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None
    assert [r.papra_document_id for r in
            store.ledger_needs_index(generation=store.get_generation())] == [DOC_ID]
    assert store.get_job(job_id).status == "pending"
    assert store.claim_next_job("index").id == job_id


async def test_the_liveness_probe_does_not_run_twice_inside_the_interval(store, papra,
                                                                        index,
                                                                        monkeypatch):
    """One full read of the index per `LIVENESS_PROBE_INTERVAL_S`, not one per sweep.

    The probe costs a GET per distinct Kreuzakt id, so on the sweep's own cadence it would
    be the most expensive thing in the process for no extra correctness — the state it
    detects changes on the scale of hours, not minutes.
    """
    seed_reference(store, papra, document_id="doc-1", file_hash=HASH, kreuzakt_id=7)
    seed_reference(store, papra, document_id="doc-2", file_hash="ab" * 32, kreuzakt_id=9)
    index.serve_document(9)
    index.missing_document_ids = {7}

    def document_reads():
        return sum(1 for method, route in index.calls
                   if method == "GET" and route.startswith("/api/documents/"))

    def probe():
        return indexing.probe_index_liveness(store, index, store.get_generation())

    await probe()
    first_reads = document_reads()
    assert first_reads >= 2                       # both ids were looked at

    await probe()
    assert document_reads() == first_reads, "the interval gate did not hold"

    later = (datetime.now(UTC)
             + timedelta(seconds=indexing.LIVENESS_PROBE_INTERVAL_S + 1)).isoformat()
    monkeypatch.setattr(indexing, "_store_now", lambda: later)
    await probe()
    assert document_reads() > first_reads


async def test_the_probe_does_not_unbind_on_an_answer_that_is_not_404(store, papra, index):
    """An index that will not answer is not an index that has forgotten the document."""
    seed_reference(store, papra, kreuzakt_id=7)
    index.unreachable = True

    await indexing.probe_index_liveness(store, index, store.get_generation())

    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id == 7
    assert store.ledger_needs_index(generation=store.get_generation()) == []


async def test_the_probe_clears_every_reference_bound_to_a_destroyed_document(store,
                                                                             papra, index):
    """Two references, one id: the verdict was about the document, so both rows follow it.

    Clearing only the row the pass happened to look at would leave the other resolving to a
    Kreuzakt id that answers 404, which is the exact state this task exists to escape.
    """
    seed_reference(store, papra, org=ORG, document_id="doc-1", kreuzakt_id=7)
    seed_reference(store, papra, org=OTHER_ORG, document_id="doc-2", file_hash="ab" * 32,
                   kreuzakt_id=7)
    index.missing_document_ids = {7}

    await indexing.probe_index_liveness(store, index, store.get_generation())

    assert store.ledger_get(ORG, "doc-1").kreuzakt_id is None
    assert store.ledger_get(OTHER_ORG, "doc-2").kreuzakt_id is None


# --- the sweep and the operator's view (Y10) --------------------------------------

async def test_sweep_reports_the_deletion_and_cleanup_counts(store, papra, index):
    """The counts reach `SweepResult` and the pass log line, and the debt is executed."""
    seed_reference(store, papra, document_id="doc-1", file_hash=HASH, kreuzakt_id=7,
                   deleted=True)
    seed_reference(store, papra, document_id="doc-2", file_hash="ab" * 32, kreuzakt_id=8,
                   deleted=True)
    first = await indexing.reconcile_deletions(store, papra, index)
    assert (first.refs_marked, first.upstream_pending) == (2, 2)
    index.fail_delete(8)                             # one of the two is a broken index

    result = await sweep_once(store, papra, kreuzakt=index)

    assert result.deletions_marked == 0, "the debt was already marked, not re-marked"
    assert result.deletions_aborted is False
    assert result.deletions_capped == 0
    assert result.cleanup_attempted == 2 and result.cleanup_deleted == 1
    assert result.cleanup_failed == 1
    assert index.deleted == [7]


async def test_sweep_without_an_index_client_does_not_propagate_deletions(store, papra,
                                                                         caplog):
    """No client, no propagation — said out loud rather than reported as an empty pass.

    The delete pass needs both services. A sweep that quietly skipped it would look
    identical to a sweep that found nothing to delete, which is the failure this task is
    most afraid of.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    caplog.set_level(logging.WARNING, logger="papra_suite.pipeline")

    result = await sweep_once(store, papra)

    assert result.deletions_marked == 0
    assert store.ledger_get(ORG, DOC_ID).deleted_at is None
    assert any("no kreuzakt client" in record.getMessage() for record in caplog.records)


async def test_an_aborted_or_capped_pass_is_loud_in_the_sweep_log(store, papra, index,
                                                                 caplog):
    """Both signals that must never be silent, on the line the operator actually reads."""
    seed_deleted_in_papra(store, papra, count=MAX_DELETIONS_PER_PASS + 5)
    caplog.set_level(logging.INFO, logger="papra_suite.pipeline")
    await sweep_once(store, papra, kreuzakt=index)
    assert any("capped" in record.getMessage() for record in caplog.records)

    papra.raise_on_list = PapraError("GET documents failed: 503 upstream", 503)
    caplog.clear()
    caplog.set_level(logging.ERROR, logger="papra_suite.pipeline")
    await sweep_once(store, papra, kreuzakt=index)
    assert any("deletion pass aborted" in record.getMessage() for record in caplog.records)


async def test_the_pass_records_its_outcome_where_check_can_read_it(store, papra, index):
    """`check` does not run a pass, so the last pass has to be in the store.

    `aborted` and `capped` are facts about a pass that happened up to `sweep_interval_s`
    ago; keeping them only in that pass's log line would make them invisible to the command
    the README names for "why did nothing change".
    """
    seed_reference(store, papra, kreuzakt_id=7)
    papra.raise_on_list = PapraError("GET documents failed: 503 upstream", 503)

    await indexing.reconcile_deletions(store, papra, index)

    stamp = store.get_setting(indexing.LAST_DELETION_PASS_SETTING)
    assert stamp is not None and "aborted" in stamp

    papra.raise_on_list = None
    await indexing.reconcile_deletions(store, papra, index)
    assert "aborted" not in store.get_setting(indexing.LAST_DELETION_PASS_SETTING)


def test_the_deletion_constants_are_the_ones_the_plan_names():
    """The blast radius and the retry shape, pinned as values rather than as prose."""
    assert MAX_DELETIONS_PER_PASS == 20
    assert CLEANUP_MAX_ATTEMPTS == 5
    assert CLEANUP_BACKOFF_S == 300
    assert DeletionResult().aborted is False
    assert CleanupResult() == CleanupResult(0, 0, 0, 0)


async def test_cleanup_never_deletes_a_reused_integer_after_a_rebuild(store, papra, index,
                                                                      caplog):
    """The dangerous one the durable queue makes possible: id 7 meant another document before
    the rebuild than it does after.

    A queued delete is a claim about an index that existed when it was written. After a rebuild
    the integer is minted from scratch, so the row the queue names may be a live document
    nobody meant to destroy, and the delete is irreversible. The generation stored beside the id
    is what makes those two cases distinguishable at all.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "pending"

    store.bump_generation()
    seed_ledger(store, org=OTHER_ORG, document_id="doc-new", kreuzakt_id=7)  # live, new index
    caplog.set_level(logging.INFO, logger="papra_suite.store")

    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert cleanup == CleanupResult(attempted=1, deleted=0, deferred=1, failed=0)
    assert index.deleted == []
    assert store.ledger_get(ORG, DOC_ID).cleanup_state == "skipped"
    assert any("generation changed" in record.getMessage() for record in caplog.records)


async def test_a_restore_into_a_rebuilt_index_comes_back_stale_not_falsely_current(
        store, papra, index):
    """A restore cannot shortcut the re-index, whatever the row was bound to before.

    The binding predates the rebuild, so it names an index that no longer exists; stamping the
    current generation onto it would make a stale integer resolvable, which is the write this
    plan refuses in `ledger_record_delivery` for the same reason. Coming back stale — unbound,
    eligible, and needing to be indexed again — is the correct answer, and the one a restore is
    allowed to produce.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    store.bump_generation()                    # the binding is from a destroyed index
    await indexing.reconcile_deletions(store, papra, index)   # marked; a stale id queues nothing
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id == 7, "the mark leaves a stale binding"
    papra.documents[(ORG, DOC_ID)]["isDeleted"] = False
    papra.documents[(ORG, DOC_ID)]["deletedAt"] = None

    result = await indexing.reconcile_deletions(store, papra, index)

    ref = store.ledger_get(ORG, DOC_ID)
    assert result.restored == 1
    assert ref.deleted_at is None
    assert ref.kreuzakt_id is None
    assert ref.delivered_generation != store.get_generation()
    assert [r.papra_document_id for r in
            store.ledger_needs_index(generation=store.get_generation())] == [DOC_ID]


# --- review round 2: the completeness arm has to fail closed -----------------------

async def test_a_listing_that_never_reports_a_total_aborts_the_pass(store, papra, index,
                                                                    caplog):
    """A walk nobody gave a total cannot claim it saw everything, so it may not prune.

    This is the arm that used to be able to vanish. The client answered a missing
    `documentsCount` with the length of the page it had just returned, so `rows_seen <
    reported` was permanently false and the truncation guard — the strongest one in the trust
    predicate — disappeared at exactly the moment it was needed: an upstream change to the
    listing's shape. The failure was silent because the pass still reported `ok`, and the
    marks it went on to make were each "confirmed" by the same endpoint that had stopped
    listing the rest of the corpus.

    So the client reports what the server declared, which may be nothing, and the walk that
    heard nothing refuses. Nothing here is inferred from an absence the server never denied.
    """
    seed_reference(store, papra, document_id="doc-1", kreuzakt_id=7)
    papra.stale_listing = {"doc-1"}                 # a candidate the abort must protect
    papra.total_pages = [None]                      # no page declares a total at all
    caplog.set_level(logging.ERROR, logger="papra_suite.indexing")

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.aborted is True and result.refs_marked == 0
    assert store.ledger_get(ORG, "doc-1").deleted_at is None
    assert store.ledger_stats()["cleanup_pending"] == 0
    assert any("total" in record.getMessage() for record in caplog.records)


async def test_a_total_that_sinks_below_the_rows_served_aborts_the_pass(store, papra, index,
                                                                        small_pages):
    """If the server ever claimed more documents than the walk served, it did not finish.

    The completeness arm asks one question: did we see at least as many rows as the biggest
    total the endpoint volunteered? This listing says five on page one and two on page two;
    three rows were served, so the five-document claim was never satisfied and nothing may be
    inferred from the gaps. The direction ordinary churn produces, a total that GROWS, is the
    next test, and it does not abort.
    """
    seed_reference(store, papra, document_id="doc-1", kreuzakt_id=7)
    seed_reference(store, papra, document_id="doc-2", file_hash="ab" * 32, kreuzakt_id=8)
    seed_reference(store, papra, document_id="doc-3", file_hash="cd" * 32, kreuzakt_id=9)
    papra.stale_listing = {"doc-1"}
    papra.total_pages = [5, 2]                      # page 1 says five, page 2 says two

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.aborted is True and result.refs_marked == 0
    for document_id in ("doc-1", "doc-2", "doc-3"):
        assert store.ledger_get(ORG, document_id).deleted_at is None


async def test_a_total_that_grows_midway_still_completes_the_walk(store, papra, index,
                                                                 small_pages):
    """`documentsCount` is a live count, so a total that moves is ordinary traffic.

    Somebody uploading or trashing one document between page 1 and page 2 makes the total move
    while the walk runs. Refusing on movement threw the whole pass away for a person touching
    their library — and an aborted pass earns no cadence stamp, so the next sweep re-pays the
    entire walk and marks nothing again. That is the amplification this round exists to remove,
    mirrored: the walk is what propagates a deletion, and it was being cancelled by the very
    traffic it is meant to notice.

    Four rows served against totals of [2, 4] satisfies the property the arm is for — we saw at
    least as many rows as the server ever claimed exist — so the walk completes and the trashed
    document is marked.
    """
    for number, document_id in enumerate(("doc-1", "doc-2", "doc-3", "doc-4")):
        seed_reference(store, papra, document_id=document_id, file_hash=f"h{number}" * 32,
                       kreuzakt_id=number + 7, deleted=(document_id == "doc-3"))
    papra.total_pages = [2, 4]                      # one document arrived mid-walk

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.aborted is False, "a total that grew discarded a walk that saw everything"
    assert result.refs_marked == 1
    assert store.ledger_get(ORG, "doc-3").deleted_at is not None
    for document_id in ("doc-1", "doc-2", "doc-4"):
        assert store.ledger_get(ORG, document_id).deleted_at is None


# --- review round 2: the empty-bucket breaker is one bucket wide -------------------

async def test_an_empty_listing_stops_its_own_bucket_and_no_other(store, papra, index, caplog):
    """A bucket that answers empty stops being readable; the corpus next door does not.

    The verdict is the same one the other arms reach — absence may not be read out of this
    listing — but only this arm describes a state a bucket can be in legitimately, and it says
    nothing whatever about the others. Aborting the whole pass for it let one emptied bucket
    freeze propagation for every bucket in the corpus, permanently, while both copies of the
    prose promised that only that bucket had stopped.

    So this bucket is quarantined and named, and the rest of the pass runs: the trashed
    document in the healthy bucket is marked exactly as it would have been if nothing was wrong
    anywhere.
    """
    seed_reference(store, papra, org=ORG, document_id="doc-stay", file_hash="ab" * 32)
    seed_reference(store, papra, org=ORG, document_id="doc-gone", file_hash="cd" * 32,
                   kreuzakt_id=7, deleted=True)
    seed_ledger(store, org=OTHER_ORG, document_id="doc-orphan", file_hash="ef" * 32,
                kreuzakt_id=9)                       # and Papra lists nothing for that bucket,
    # while the ledger still holds a LIVE reference for it: that pairing is the whole suspicion.
    caplog.set_level(logging.ERROR, logger="papra_suite.indexing")

    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.aborted is False, "one empty bucket is not a reason to stop the corpus"
    assert result.quarantined == 1
    assert store.ledger_get(ORG, "doc-gone").deleted_at is not None
    assert store.ledger_get(ORG, "doc-stay").deleted_at is None
    assert store.ledger_get(OTHER_ORG, "doc-orphan").deleted_at is None, \
        "a bucket nobody could read must not have its references pruned"
    assert OTHER_ORG in (store.get_setting(indexing.DELETION_QUARANTINE_SETTING) or "")
    assert any("quarantin" in record.getMessage() for record in caplog.records)


async def test_a_quarantined_bucket_does_not_stop_another_buckets_restore(store, papra, index):
    """The half the old behaviour lost quietly: restores were stopped by it too.

    A document that came back from the trash is still in a healthy bucket, and the damage a
    stalled restore does is permanent-looking — the row stays bound to whatever the index filed
    while it was in the trash. One unreadable bucket had no business holding that up.
    """
    seed_reference(store, papra, org=ORG, document_id="doc-back", kreuzakt_id=7, deleted=True)
    seed_ledger(store, org=OTHER_ORG, document_id="doc-orphan", file_hash="ef" * 32,
                kreuzakt_id=9)
    await indexing.reconcile_deletions(store, papra, index)      # the row goes deleted upstream
    papra.documents[(ORG, "doc-back")]["isDeleted"] = False
    papra.documents[(ORG, "doc-back")]["deletedAt"] = None       # and then it comes back

    result = await indexing.reconcile_deletions(store, papra, index)

    assert (result.quarantined, result.restored) == (1, 1)
    assert store.ledger_get(ORG, "doc-back").deleted_at is None
    assert store.ledger_get(OTHER_ORG, "doc-orphan").deleted_at is None


async def test_a_bucket_leaves_quarantine_by_answering_again(store, papra, index):
    """The record is a report, not a lock: nobody has to clear it for traffic to resume.

    An operator lifting a quarantine by hand is a second way to get it wrong, so the set is
    rewritten from what the last pass actually saw. A bucket that starts listing again is
    readable again, and the settings row says so without being touched.
    """
    seed_ledger(store, org=OTHER_ORG, document_id="doc-orphan", file_hash="ef" * 32,
                kreuzakt_id=9)
    await indexing.reconcile_deletions(store, papra, index)
    assert OTHER_ORG in (store.get_setting(indexing.DELETION_QUARANTINE_SETTING) or "")

    papra.add("doc-orphan", org=OTHER_ORG)
    result = await indexing.reconcile_deletions(store, papra, index)

    assert result.quarantined == 0
    assert OTHER_ORG not in (store.get_setting(indexing.DELETION_QUARANTINE_SETTING) or "")


async def test_a_bucket_that_is_emptied_and_cleaned_is_not_quarantined_forever(store, papra,
                                                                              index, caplog):
    """A finished bucket is not an unreadable one, and the health surface must know which.

    The breaker counts references, and ledger rows are never deleted, so the old predicate
    (`COUNT(*)` with no `deleted_at` filter) stayed true for ever: once a person emptied a
    bucket and its references were marked and cleaned, every subsequent walk quarantined that
    bucket, logged ERROR, and printed `set aside: <org>` in `check` permanently. Nothing was
    blocked — which is exactly the damage: the two mechanisms whose only defence is being loud
    were shouting about a state that is *finished*, on a corpus of four buckets, until nobody
    listens to either of them.

    So the empty-listing verdict is judged against the work here: a live reference, or cleanup
    debt still owed. Neither means nothing to read absence out of, and nothing to say.
    """
    seed_reference(store, papra, org=OTHER_ORG, document_id="doc-last", file_hash="ef" * 32,
                   kreuzakt_id=9, deleted=True)
    await indexing.reconcile_deletions(store, papra, index)
    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))
    assert (cleanup.deleted, index.deleted) == (1, [9])
    assert store.ledger_get(OTHER_ORG, "doc-last").cleanup_state == "done"

    del papra.documents[(OTHER_ORG, "doc-last")]     # Papra purges it: the bucket is now empty
    caplog.set_level(logging.ERROR, logger="papra_suite.indexing")

    for _ in range(2):                               # "forever" is the thing under test
        result = await indexing.reconcile_deletions(store, papra, index)
        assert result.quarantined == 0, "a bucket the ledger finished with was set aside"
        assert OTHER_ORG not in (store.get_setting(indexing.DELETION_QUARANTINE_SETTING) or "")
    assert not any("quarantin" in record.getMessage() for record in caplog.records), \
        "a finished bucket was shouting about itself at ERROR"


# --- review round 2: the same-pass case, and the bucket nobody registered -----------

async def test_two_references_to_one_document_vanishing_in_one_pass_queue_one_delete(
        store, papra, index):
    """The dangerous case in a single pass, which is the case the count is taken for.

    Two buckets holding identical bytes share one index document, and nothing says the two
    references have to reach the trash in different passes — a user emptying two buckets, or one
    bucket and one mail rule, produce exactly this. Both rows go soft-deleted in one walk, one
    after the other, so the second marking statement must count what the first already wrote:
    the count lives inside the same transaction as the soft-delete precisely so that the pair
    cannot both read "one left" and both decide the document is still wanted.

    Getting that wrong in the permissive direction destroys a document the user kept; getting it
    wrong the other way leaks the index entry forever. So both halves are asserted here, plus the
    single upstream delete this pass is allowed to authorise.
    """
    queued: list[dict] = []
    original = store.ledger_queue_cleanup

    def spy(**kwargs):
        queued.append(kwargs)
        return original(**kwargs)

    store.ledger_queue_cleanup = spy
    seed_reference(store, papra, org=ORG, document_id="doc-1", kreuzakt_id=7, deleted=True)
    seed_reference(store, papra, org=OTHER_ORG, document_id="doc-2",
                   file_hash=HASH, kreuzakt_id=7, deleted=True)

    result = await indexing.reconcile_deletions(store, papra, index)

    assert (result.refs_marked, result.upstream_pending) == (2, 1)
    assert [call["kreuzakt_id"] for call in queued] == [7], \
        "one shared document is one queued delete, whichever order the rows were marked in"
    assert store.ledger_get(ORG, "doc-1").cleanup_state == "none", \
        "the first reference gone still left a live one"
    assert store.ledger_get(OTHER_ORG, "doc-2").cleanup_state == "pending"

    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert (cleanup.deleted, index.deleted) == (1, [7])
    assert store.ledger_resolve([7], generation=store.get_generation()) == []


async def test_an_orphan_buckets_index_entry_is_still_reclaimed(store, papra, index):
    """A bucket that left `orgs` still owes the index its cleanup, and `orgs` cannot say so.

    The pass walks `ledger_org_ids()` rather than `list_orgs()` for exactly this: an operator can
    remove a bucket, or a store can be copied without one, and its references and their Kreuzakt
    documents are still there. Driven from `orgs`, those index entries outlive every record of
    why they exist — nothing ever looks at them again, and nothing says so. The hazard is not a
    missing count, it is an index entry nobody will ever reclaim.
    """
    orphan = "org-removed-from-orgs"
    assert [org.papra_org_id for org in store.list_orgs()] == [ORG]
    seed_reference(store, papra, org=orphan, document_id="doc-orphan", file_hash="ab" * 32,
                   kreuzakt_id=7, deleted=True)

    result = await indexing.reconcile_deletions(store, papra, index)
    cleanup = await indexing.run_cleanup(store, papra, index, now=due(store))

    assert (result.refs_marked, result.upstream_pending) == (1, 1)
    assert cleanup.deleted == 1 and index.deleted == [7]
    assert store.ledger_get(orphan, "doc-orphan").cleanup_state == "done"


# --- review round 2: the post-delete unbind must not strand a live row --------------

async def test_a_reference_bound_inside_the_delete_window_is_unbound_and_readmitted(store,
                                                                                    papra):
    """The cleanup pass may not clear a binding it did not make and then walk away.

    The four vetoes run before the request, and the request is an `await`. The index worker is a
    sibling task in the same process, so a second bucket can finish `ledger_set_kreuzakt_id` onto
    this very id after the pass last looked and before the delete lands. The unbind that follows a
    successful delete is unscoped on purpose — every row still pointing at a destroyed document is
    wrong — so it clears that row too, and a row that is live, unbound and never enqueued is the
    state this task refuses to leave elsewhere: `sweep_once` only enqueues documents it has never
    seen, so nothing will ever ask for that work again.

    So the pass names the rows it is about to clear and re-admits the live ones, exactly as the
    liveness probe already does.
    """
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)

    class BindsOnDelete(FakeIndex):
        """The index worker, finishing its own write inside the delete's await window."""

        async def delete_document(self, kreuzakt_id):
            seed_ledger(store, org=OTHER_ORG, document_id="doc-sibling", file_hash=HASH)
            store.ledger_set_kreuzakt_id(papra_org_id=OTHER_ORG,
                                         papra_document_id="doc-sibling",
                                         kreuzakt_id=kreuzakt_id,
                                         generation=store.get_generation())
            return await super().delete_document(kreuzakt_id)

    raced = BindsOnDelete()
    try:
        await indexing.reconcile_deletions(store, papra, raced)
        assert store.ledger_get(ORG, DOC_ID).cleanup_state == "pending"

        cleanup = await indexing.run_cleanup(store, papra, raced, now=due(store))
    finally:
        await raced.aclose()

    assert (cleanup.deleted, raced.deleted) == (1, [7])
    assert store.ledger_get(OTHER_ORG, "doc-sibling").kreuzakt_id is None, \
        "a live row must not keep a binding to a destroyed document"
    claimed = store.claim_next_job("index")
    assert claimed is not None and claimed.document_id == "doc-sibling", \
        "unbound but never enqueued is eligible in a sense nobody can act on"


# --- review round 2: the expensive reads have clocks of their own --------------------

async def test_the_listing_walk_is_not_repeated_on_every_sweep(store, papra, index, monkeypatch):
    """The walk costs a page per hundred documents per bucket, so it needs a clock of its own.

    A sweep defaults to five minutes, and this is the most frequent full read in the process while
    the state it looks for does not change on that timescale. The gate is one interval, and an
    operator waiting for a deletion to propagate has to be able to read which interval that is.
    """
    monkeypatch.setattr(indexing, "LISTING_WALK_INTERVAL_S", 900)
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)

    await indexing.reconcile_deletions(store, papra, index)
    listed = papra.list_calls
    assert listed >= 1

    await indexing.reconcile_deletions(store, papra, index)
    assert papra.list_calls == listed, "the corpus was re-listed inside its own interval"

    monkeypatch.setattr(indexing, "_store_now",
                        lambda: later(indexing.LISTING_WALK_INTERVAL_S + 10))
    await indexing.reconcile_deletions(store, papra, index)
    assert papra.list_calls > listed, "the walk never came back around"


async def test_a_walk_that_did_not_finish_is_not_recorded_as_done(store, papra, index,
                                                                  monkeypatch):
    """An aborted walk earns no cadence stamp, so the next sweep is free to try again.

    Stamping an unfinished walk would turn one bad page into a quarter-hour of silence about a
    corpus that was never read, which is the opposite of what the interval is for.
    """
    monkeypatch.setattr(indexing, "LISTING_WALK_INTERVAL_S", 900)
    seed_reference(store, papra, kreuzakt_id=7, deleted=True)
    papra.raise_on_list = httpx.ConnectError("papra is down")

    result = await indexing.reconcile_deletions(store, papra, index)
    assert result.aborted is True
    assert store.get_setting(indexing.LISTING_WALK_STAMP_SETTING) is None

    papra.raise_on_list = None
    result = await indexing.reconcile_deletions(store, papra, index)
    assert result.aborted is False
    assert store.get_setting(indexing.LISTING_WALK_STAMP_SETTING) is not None


async def test_a_total_outage_stamps_the_walk_and_retries_only_what_did_not_answer(
        store, papra, index, monkeypatch):
    """Cadence honesty: the interval bounds the *walk*, not whether every read succeeded.

    Withholding the stamp while any id stayed unreadable made the corpus-wide read run again
    every `LIVENESS_PROBE_RETRY_INTERVAL_S` — twenty-four times a day against a constant that
    advertises six hours — and it left the narrow retry unreachable in exactly the case it was
    written for. Two sentences have to hold at once, and they only do if they are carried by
    two different pieces of state: the stamp says the walk ran to its end, and the unanswered
    list says the corpus is not thereby known to be clean.
    """
    seed_reference(store, papra, kreuzakt_id=7)
    index.make_unreachable()
    generation = store.get_generation()

    assert await indexing.probe_index_liveness(store, index, generation) == 0
    assert store.get_setting(indexing.LIVENESS_PROBE_STAMP_SETTING) is not None, \
        "a walk that ran to its end earned no cadence stamp"
    assert store.get_setting(indexing.LIVENESS_PROBE_UNANSWERED_SETTING).endswith("|7")

    reads = document_reads(index)
    assert await indexing.probe_index_liveness(store, index, generation) == 0
    assert document_reads(index) == reads, \
        "the whole corpus went back under the knife one sweep later"

    monkeypatch.setattr(indexing, "_store_now",
                        lambda: later(indexing.LIVENESS_PROBE_RETRY_INTERVAL_S + 10))
    before = document_reads(index)
    await indexing.probe_index_liveness(store, index, generation)
    assert document_reads(index) - before == 1, \
        "the narrow retry re-read the corpus instead of the one id that never answered"


async def test_a_walk_that_finished_around_an_unreadable_id_retries_only_that_id(store, papra,
                                                                                index, monkeypatch):
    """Some reads failed: the walk still counts as completed, and the failures are retried alone.

    This is the shape the amplification came from — a partial outage that withheld the interval
    stamp and so re-read every distinct binding every sweep — and the shape the old anti-stall rule
    was protecting: ids that did not answer must not be filed as answered just because the walk
    ended.
    """
    seed_reference(store, papra, kreuzakt_id=7)
    seed_reference(store, papra, document_id="doc-two", file_hash="hash-two", kreuzakt_id=9)
    index.unreadable.add(9)
    generation = store.get_generation()

    assert await indexing.probe_index_liveness(store, index, generation) == 1
    assert store.get_setting(indexing.LIVENESS_PROBE_STAMP_SETTING) is not None
    assert store.get_setting(indexing.LIVENESS_PROBE_UNANSWERED_SETTING).endswith("|9")
    assert store.ledger_get(ORG, "doc-two").kreuzakt_id == 9, "an unanswered id is not a verdict"

    reads = document_reads(index)
    assert await indexing.probe_index_liveness(store, index, generation) == 0
    assert document_reads(index) == reads

    monkeypatch.setattr(indexing, "_store_now",
                        lambda: later(indexing.LIVENESS_PROBE_RETRY_INTERVAL_S + 10))
    before = document_reads(index)
    await indexing.probe_index_liveness(store, index, generation)
    assert document_reads(index) - before == 1, \
        "the retry re-read the corpus instead of the failure"


# --- review round 2/3: a run of marks is a signal, and it gates marks only ----------

async def test_the_marks_brake_gates_marks_and_not_the_pass(store, papra, index, monkeypatch):
    """Marks that keep coming are a bad signal about the pipeline, not work to grind through.

    The per-pass cap bounds one pass and is silent about the day: twenty references every
    quarter-hour is nineteen hundred and twenty a day with every individual pass looking
    ordinary. So the second brake is a fraction of the references the ledger holds — "is this
    normal" has no answer that is not about the size of the corpus — and the floor is one full
    pass, lowered here because ten references is not a corpus the production floor is meant for.

    What the brake is allowed to stop is the marking loop, and that is all. Round 2 refused the
    pass before reading, which froze the non-destructive half of the pass — the walk, the
    restores, the probe — while `run_cleanup`, the only destructive half, kept running from
    `sweep_once` regardless: the safe work stopped and the dangerous work continued. And the
    stated rationale does not reach `_apply_restore`, which can unbind and re-admit and cannot
    destroy anything.
    """
    monkeypatch.setattr(indexing, "_marks_rolling_floor", lambda: 2)
    for number in (1, 2, 3):
        seed_reference(store, papra, document_id=f"doc-gone-{number}", file_hash=f"{number}" * 64,
                       kreuzakt_id=number, deleted=True)

    first = await indexing.reconcile_deletions(store, papra, index)
    assert first.refs_marked == 2, "one reference in ten is the allowance for ten references"
    assert (first.brake_tripped, first.aborted) == (True, False)

    # The window is spent. The corpus still holds the deletion the brake turned down, and now it
    # holds a restore as well.
    listed = papra.list_calls
    papra.documents[(ORG, "doc-gone-1")]["isDeleted"] = False
    papra.documents[(ORG, "doc-gone-1")]["deletedAt"] = None

    second = await indexing.reconcile_deletions(store, papra, index)

    assert second.aborted is False, "the brake gates marks, not the pass"
    assert (second.refs_marked, second.brake_tripped) == (0, True)
    assert second.restored == 1, "a restore cannot destroy anything, so the brake may not hold it"
    assert papra.list_calls > listed, "the braked pass stopped reading the corpus"
    assert store.ledger_get(ORG, "doc-gone-3").deleted_at is None, "new marks are what it holds"
    assert store.ledger_get(ORG, "doc-gone-1").deleted_at is None
    assert "brake_held=1" in store.get_setting(indexing.LAST_DELETION_PASS_SETTING)

    # Not a ratchet: an operator who has established that the run was real clears one row.
    store.set_setting(indexing.MARKS_ROLLING_SETTING, "")
    third = await indexing.reconcile_deletions(store, papra, index)
    assert third.aborted is False and third.refs_marked == 1


async def test_the_liveness_probe_runs_even_when_papra_cannot_be_read(store, papra, index):
    """The probe repairs our side; Papra's listing being unreadable is not its business.

    Inside the pass it sat behind two early returns, so while the listing was untrustworthy —
    precisely when a delete would be wrong — the one mechanism that recovers a document the
    index has forgotten did not run at all. It has its own clock, so it has its own call site.
    """
    seed_reference(store, papra, kreuzakt_id=7)
    index.missing_document_ids = {7}
    papra.raise_on_list = PapraError("GET documents failed: 503 upstream", 503)

    result = await sweep_once(store, papra, kreuzakt=index)

    assert result.deletions_aborted is True
    assert store.ledger_get(ORG, DOC_ID).deleted_at is None, "an unreadable listing marked"
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None, \
        "the probe never ran while Papra's listing was untrustworthy"


async def test_the_marks_brake_is_announced_as_a_brake_and_still_lets_the_probe_run(store,
                                                                                   papra, index,
                                                                                   monkeypatch,
                                                                                   caplog):
    """A brake trip must read as a brake trip, and it must not stop the Kreuzakt-side repair.

    Two defects in one place. The sweep's line tested `aborted` before `brake`, and the pre-pass
    refusal set `aborted=True`, so a marks-brake stop shouted about a listing that could not be
    trusted — sending the operator to Papra for a fault the marks counter named. And the probe
    sat inside the pass, so it froze with everything else while `run_cleanup` carried on.
    """
    monkeypatch.setattr(indexing, "_marks_rolling_floor", lambda: 1)
    # Both rows carry the hash the fake actually serves: `ledger_upsert_reference` clears a binding
    # whose bytes changed, and the sweep's own pass re-upserts every document it lists.
    seed_reference(store, papra, document_id="doc-gone", kreuzakt_id=1, deleted=True)
    first = await sweep_once(store, papra, kreuzakt=index)
    assert (first.deletions_marked, first.cleanup_deleted) == (1, 1)

    # The window is spent, and a second document goes the same way. Its index entry has already
    # died on its own: the state the probe exists to notice and repair.
    seed_reference(store, papra, document_id="doc-next", kreuzakt_id=2, deleted=True)
    index.missing_document_ids = {2}
    store.set_setting(indexing.LIVENESS_PROBE_STAMP_SETTING, "")   # the long clock rolls over
    caplog.set_level(logging.ERROR, logger="papra_suite.pipeline")

    second = await sweep_once(store, papra, kreuzakt=index)

    assert (second.deletions_marked, second.deletions_brake, second.deletions_aborted) == \
        (0, True, False)
    assert store.ledger_get(ORG, "doc-next").kreuzakt_id is None, \
        "a brake on marks froze the repair of a Kreuzakt-side fault"
    lines = [record.getMessage() for record in caplog.records]
    assert any("marks brake" in line for line in lines), lines
    assert not any("could not be trusted" in line for line in lines), \
        "a brake trip was announced as an untrustworthy listing"


def test_the_probe_retry_list_is_capped_and_aged(store):
    """An id the index will never answer for must not grow the settings row for ever.

    The unanswered list is a schedule rather than a record. Two bounds follow from that: it
    holds at most a fixed number of ids, and an id that has stayed unreadable for a whole day
    leaves it — the full walk re-reads every binding on its own clock, so aging costs a delay
    and nothing else, while an unbounded list is a settings row that grows with an outage.
    """
    many = list(range(indexing.LIVENESS_PROBE_UNANSWERED_LIMIT + 25))
    indexing._remember_unanswered(store, many, now=indexing._store_now())
    assert len(indexing._unanswered_ids(store)) == indexing.LIVENESS_PROBE_UNANSWERED_LIMIT
    assert indexing.pending_probe_rereads(store) == indexing.LIVENESS_PROBE_UNANSWERED_LIMIT

    aged = later(-indexing.LIVENESS_PROBE_UNANSWERED_MAX_AGE_S - 60)
    store.set_setting(indexing.LIVENESS_PROBE_UNANSWERED_SETTING, f"{aged}|7,9")
    assert indexing._unanswered_ids(store) == [], "an aged-out id is the full walk's business"
    assert indexing.pending_probe_rereads(store) == 0

    # A value written by an earlier build (bare ids, no age) reads as nothing rather than
    # crashing: the next full walk asks about every id again anyway.
    store.set_setting(indexing.LIVENESS_PROBE_UNANSWERED_SETTING, "7,9")
    assert indexing._unanswered_ids(store) == []
    assert indexing.pending_probe_rereads(store) == 0

    # Re-writing the SAME failing set must not re-age it, or an id that never answers stays
    # permanently young and the cut-off never arrives: the full walk re-reads it every six hours
    # and each of those walks would otherwise hand it a fresh day of short-clock retries.
    first_stamp = indexing._store_now()
    indexing._remember_unanswered(store, [7, 9], now=first_stamp)
    again = later(600)
    indexing._remember_unanswered(store, [7], now=again)
    assert store.get_setting(indexing.LIVENESS_PROBE_UNANSWERED_SETTING) == f"{first_stamp}|7"
    # A NEW failure is a new vintage, and re-ages the list: coarse on purpose.
    indexing._remember_unanswered(store, [7, 11], now=again)
    assert store.get_setting(indexing.LIVENESS_PROBE_UNANSWERED_SETTING) == f"{again}|7,11"


async def test_a_gated_walk_says_when_it_comes_back(store, papra, index, monkeypatch):
    """`check` must not present a pass that is being skipped as one that just ran.

    A gated pass writes nothing at all, so the line the operator reads can be a quarter-hour
    old and say `ok marked=0` — which, next to a document they trashed five minutes ago, reads
    as propagation being broken. The cadence knows when the walk may run again, so it says so.
    """
    monkeypatch.setattr(indexing, "LISTING_WALK_INTERVAL_S", 900)
    seed_reference(store, papra, kreuzakt_id=7)

    await indexing.reconcile_deletions(store, papra, index)      # walks, and stamps
    gate = indexing.listing_walk_gate(store)
    assert gate is not None, "a pass inside its own interval reported nothing outstanding"
    assert gate == indexing._shift(store.get_setting(indexing.LISTING_WALK_STAMP_SETTING),
                                   seconds=900)

    listed = papra.list_calls
    await indexing.reconcile_deletions(store, papra, index)      # gated: reads nothing
    assert papra.list_calls == listed
    assert indexing.listing_walk_gate(store) == gate

    monkeypatch.setattr(indexing, "LISTING_WALK_INTERVAL_S", 0)  # the gate has let go
    assert indexing.listing_walk_gate(store) is None


async def test_the_probe_no_longer_runs_ahead_of_the_queued_deletes(store, papra, index):
    """One sweep: the delete goes first, then the probe's read of an unrelated id.

    The probe's own clock is the argument for its own call site, and the same argument decides
    where in the sweep it belongs. It used to run between the marks and `run_cleanup`, so a slow
    but answering index pushed every queued delete to the next sweep: a delete aged by a repair
    that gained nothing by going first, since all the probe does is unbind rows whose document is
    already gone.
    """
    seed_reference(store, papra, document_id="doc-gone", kreuzakt_id=1, deleted=True)
    seed_reference(store, papra, document_id="doc-live", kreuzakt_id=2)
    index.missing_document_ids = {2}                 # our side forgot it; Papra did not

    result = await sweep_once(store, papra, kreuzakt=index)

    assert (result.cleanup_deleted, index.deleted) == (1, [1])
    assert store.ledger_get(ORG, "doc-live").kreuzakt_id is None, "the probe did not run"
    calls = list(index.calls)
    delete_at = next(i for i, (method, _) in enumerate(calls) if method == "DELETE")
    probe_at = next(i for i, (method, route) in enumerate(calls)
                    if method == "GET" and route.endswith("/api/documents/2"))
    assert delete_at < probe_at, "the probe ran ahead of the queued delete again"
