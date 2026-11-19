"""Task 13: `search_documents` and `list_recent_documents`, the authorization boundary.

Everything an agent may learn about this archive comes through these two tools, so this file is not
a search test that happens to mention permissions — it is a permissions test that happens to use
search to make the difference visible. Four rules govern it, and each is a different failure:

* **The ledger is the scope filter.** One `ledger_resolve` per page of hits, at the current
  generation. A Kreuzakt hit with no ledger row, a row under a stale generation, a row in a bucket
  this token was not given: all invisible, and none of them an error. Kreuzakt is a corpus-wide
  index of *everything*, so without this filter one bucket's credential reads the whole archive.
* **Fail closed where being ambiguous is cheap.** "Unknown" never means "show it": a Papra lookup
  that errors drops the item and is counted, a deleted document is dropped and counted.
* **A page is complete or it says so.** `partial` is true whenever the answer could be missing
  results — the scan budget ran out, the window cannot be proven complete, a Papra read failed, or
  the fan-out bound meant a bucket in scope was never consulted. Never a short answer that reads
  like a small corpus.
* **Papra and Kreuzakt internals stop here.** The JSON an agent gets carries Papra ids and nothing
  else: no Kreuzakt integer id, no `/api/documents/…`, no thumbnail link, no `content`, no `score`.

The bounds are refusals, not clamps: a 513-character query and a `page_size` of 51 are answered
with `invalid-request`, because a silently-clamped request is an agent that believes it searched
for something it did not search for.

One deviation from the plan's Step 2 sketch, and it is load-bearing rather than cosmetic: the
sketch reads `store.ledger_resolve(...)` off a `Store`, and the shipped listener cannot have one
(containment option C). It is `LedgerReader`'s method now, which is the same query object.
"""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from mcp.server.mcpserver.exceptions import ToolError
from starlette.testclient import TestClient

from papra_suite.kreuzakt_client import KreuzaktError
from papra_suite.mcp_server import Unauthenticated, build_mcp_app, require_principal
from papra_suite.mcp_tokens import MAX_ORG_IDS, TokenContext, mint_token, resolve_token
from papra_suite.mcp_tools import (
    DEFAULT_LIMITS,
    KREUZAKT_PAGE_ROWS,
    MAX_BUCKETS_PER_CALL,
    MAX_CONCURRENT_LOOKUPS,
    list_recent_documents,
    search_documents,
)
from papra_suite.readers import (
    FORBIDDEN_WRITE_NAMES,
    REQUIRED_READS,
    KreuzaktReader,
    LedgerReader,
    PapraReader,
)
from papra_suite.store import Store
from tests.fakes.ledger import close_views, view_for

FIXTURES = Path(__file__).parent / "fixtures"
ORG_A = "org-a"
ORG_B = "org-b"
ORG_C = "org-c"
ORG_D = "org-d"
#: A bucket whose name *begins* with a granted one: the corpus the prefix-match mutant needs.
ORG_AB = "org-ab"
KREUZAKT_URL = "http://kreuzakt.test"
PAPRA_URL = "http://papra.test"


def load(name):
    return json.loads((FIXTURES / name).read_text())


# --- the two upstreams, driven through the real readers ---------------------------------------


def kz_row(kreuzakt_id, **overrides):
    """One `/api/search` row, in the shape Task 3 captured — including the fields that must not
    survive: `content`, `snippet` and `thumbnail_url` are all present upstream, and `KzHit` is what
    drops them. A hand-written dict without them would test a thinner server than the one we run."""
    row = {
        "id": kreuzakt_id,
        "title": f"Rechnung {kreuzakt_id}",
        "description": "",
        "document_date": None,
        "added_at": f"2026-11-17T10:{kreuzakt_id % 60:02d}:00.000Z",
        "original_filename": f"kz-{kreuzakt_id}.pdf",
        "mime_type": "application/pdf",
        "content": "PAPRA-INTERNAL-TEXT that must never reach a tool result",
        "thumbnail_url": f"/api/documents/{kreuzakt_id}/thumbnail",
        "snippet": "PAPRA-INTERNAL-TEXT <mark>Rechnung</mark>",
    }
    row.update(overrides)
    return row


def kz_document_row(kreuzakt_id, **overrides):
    """One `/api/documents` row: the listing envelope's shape, no `content` field on it."""
    row = {
        "id": kreuzakt_id,
        "title": f"Rechnung {kreuzakt_id}",
        "description": "",
        "document_date": None,
        "added_at": f"2026-11-17T10:{kreuzakt_id % 60:02d}:00.000Z",
        "original_filename": f"kz-{kreuzakt_id}.pdf",
        "mime_type": "application/pdf",
        "thumbnail_url": f"/api/documents/{kreuzakt_id}/thumbnail",
        "stored_filename": "kreuzakt-internal-storage-name.pdf",
    }
    row.update(overrides)
    return row


class ScriptedIndex:
    """Kreuzakt as a transport, so every test parses the real envelope through `KreuzaktReader`.

    Deliberately not a fake with a `search` method returning prepared objects: the envelope checks
    (1-based `page`, `limit` clamped to 100, `offset` ignored, the required `total`, and the row
    narrowing that removes `content`/`snippet`/`thumbnail_url`) are what the tools sit on top
    of, and a double that skipped them would test a server that does not exist.

    Knobs: `hits` and `rows` are the scripted corpora; `total` overrides the reported match count
    (that is how "the window cannot be proven complete" is produced); `error` makes every read fail;
    `search_calls` records the `(page, limit)` of each request, which is how "paged by `page`, never
    by `offset`" is asserted.
    """

    def __init__(self, *, hits=None, rows=None, total=None, error=None, error_status=None,
                 error_body=None):
        self.hits = list(hits or [])
        self.rows = list(rows or [])
        self.total = total
        self.error = error
        # `error` alone answers 503 with a generic body, which is what an outage looks like from the
        # outside. `error_status`/`error_body` let a test put a *specific* body on the wire, so
        # that "the body did not reach the client" asserts about something that was there.
        self.error_status = error_status
        self.error_body = error_body
        self.error_hits = 0
        self.search_calls = []
        self.list_calls = []
        # The full query string of every request, because "we page by `page` and never send
        # `offset`" is a claim about the wire, not about a tuple of two integers.
        self.search_params = []
        self.list_params = []
        self.served_raw = None

    def reader(self) -> KreuzaktReader:
        return KreuzaktReader(KREUZAKT_URL, transport=httpx.MockTransport(self._handle))

    async def _handle(self, request):
        if self.error is not None:
            # The request is recorded before the failure, so a test can prove the scripted index was
            # the one that answered: an assertion that a secret is absent is worth nothing if the
            # dependency holding it was never consulted.
            self.error_hits += 1
            params = dict(request.url.params)
            if request.url.path == "/api/search":
                self.search_calls.append((int(params.get("page", 1)),
                                          int(params.get("limit", 20))))
            elif request.url.path == "/api/documents":
                self.list_calls.append((int(params.get("page", 1)),
                                        int(params.get("limit", 100)), params.get("since")))
            return httpx.Response(self.error_status or 503,
                                  json=self.error_body or {"detail": "kreuzakt is down"})
        path = request.url.path
        params = dict(request.url.params)
        if path == "/api/search":
            return httpx.Response(200, json=self._search_page(params))
        if path == "/api/documents":
            return httpx.Response(200, json=self._documents_page(params))
        if path == "/api/health":
            return httpx.Response(200, json={"status": "ok", "documents": len(self.hits)})
        if path == "/api/queue":
            return httpx.Response(200, json={"entries": [], "counts": {"pending": 0}})
        return httpx.Response(404, json={"detail": f"no such endpoint: {path}"})

    def _search_page(self, params):
        self.search_calls.append((int(params.get("page", 1)), int(params.get("limit", 20))))
        self.search_params.append(params)
        if self.served_raw is not None:
            # The captured payload, byte for byte, whatever the paging asked for.
            return dict(self.served_raw)
        limit = int(params.get("limit", 20))
        page = int(params.get("page", 1))
        start = (page - 1) * limit
        rows = self.hits[start:start + limit]
        return {"results": rows, "total": self.total if self.total is not None else len(self.hits),
                "page": page, "limit": limit, "query": params.get("q", "")}

    def _documents_page(self, params):
        self.list_calls.append((int(params.get("page", 1)), int(params.get("limit", 100)),
                               params.get("since")))
        self.list_params.append(params)
        limit = int(params.get("limit", 100))
        page = int(params.get("page", 1))
        start = (page - 1) * limit
        rows = self.rows[start:start + limit]
        return {"documents": rows,
                "total": self.total if self.total is not None else len(self.rows),
                "page": page, "limit": limit}


def papra_doc(document_id, *, org=ORG_A, **overrides):
    doc = {
        "id": document_id,
        "organizationId": org,
        "name": f"Papra name of {document_id}",
        "mimeType": "application/pdf",
        "content": "PAPRA-INTERNAL-BODY",
        "documentDate": None,
        "createdAt": "2026-11-01T09:00:00.000Z",
        "updatedAt": "2026-11-17T09:00:00.000Z",
        "isDeleted": False,
        "deletedAt": None,
        "originalSize": 1234,
        "tags": [],
    }
    doc.update(overrides)
    return doc


class FakePapra:
    """Papra's live document state, counted.

    `get_calls` is the assertion behind "checked only for items being returned": it must track the
    documents a page hands back, not the rows Kreuzakt scanned. `peak_concurrent` is the fan-out
    bound's witness — the client tracks how many lookups were in flight at once.
    """

    def __init__(self, documents=None):
        self.documents = dict(documents or {})
        # Two buckets may hold the same Papra *id*, so `documents` (keyed by id alone) cannot
        # express it. Entries added with `add_for` win over it, which is how the per-bucket
        # granularity of the liveness cache is tested.
        self.by_org = dict()
        self.get_calls = 0
        self.peak_concurrent = 0
        self.in_flight = 0
        self.raise_on_get = False
        self.delay_s = 0.0
        self.requested = []

    def add(self, document_id, **overrides):
        self.documents[document_id] = papra_doc(document_id, **overrides)
        return self.documents[document_id]

    def add_for(self, org_id, document_id, **overrides):
        """The document of one *bucket*, addressed by `(org_id, document_id)`.

        Papra ids are per organisation, so the same id can be two different documents; a fake that
        keys by id alone cannot say so, and the cache-key mutant needs it to.
        """
        self.by_org[(org_id, document_id)] = papra_doc(document_id, org=org_id, **overrides)
        return self.by_org[(org_id, document_id)]

    async def get_document(self, org_id, document_id):
        self.get_calls += 1
        self.requested.append((org_id, document_id))
        self.in_flight += 1
        self.peak_concurrent = max(self.peak_concurrent, self.in_flight)
        try:
            if self.delay_s:
                await asyncio.sleep(self.delay_s)
            if self.raise_on_get:
                raise KreuzaktError("papra refused the read")     # any failure: same handling
            doc = self.by_org.get((org_id, document_id)) or self.documents.get(document_id)
            if doc is None or doc.get("organizationId") != org_id:
                raise KreuzaktError(f"404 for {document_id}")
            return dict(doc)
        finally:
            self.in_flight -= 1

    async def list_documents(self, org_id, **kwargs):
        raise AssertionError("these tools page kreuzakt, not papra")

    async def download_file(self, org_id, document_id):
        raise AssertionError("these tools never hand out bytes")

    async def list_tags(self, org_id):
        raise AssertionError("these tools never list tags")

    async def whoami(self):
        raise AssertionError("these tools never ask who the key belongs to")


# --- seeding ----------------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


def bind(store, org, document_id, kreuzakt_id, *, generation=None, name=None):
    """One ledger reference: this Papra document's bytes are Kreuzakt row `kreuzakt_id`."""
    store.ledger_upsert_reference(papra_org_id=org, papra_document_id=document_id,
                                  file_hash=f"{kreuzakt_id:0>2x}" + "0" * 62,
                                  papra_original_name=name or f"{document_id}.pdf")
    store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=document_id,
                                 kreuzakt_id=kreuzakt_id,
                                 generation=generation or store.get_generation())


def ctx(*orgs, scope_all=False):
    return TokenContext(label="test", org_ids=frozenset(orgs), scope_all=scope_all)


def stats_of(page):
    return page.stats


# --- request bounds: refused, never clamped ----------------------------------------------------


async def test_oversized_query_and_page_bounds_are_refused(store):
    """`invalid-request`, and no silent clamp in any direction.

    A clamp is the failure this test exists for: a caller that asked for `page_size=51` and got 50
    believes it received 51, and a 900-character query quietly truncated to 512 answers a question
    nobody asked. All four shapes go through the same refusal.
    """
    index = ScriptedIndex(hits=[kz_row(1)])
    papra = FakePapra({"doc-1": papra_doc("doc-1")})
    bind(store, ORG_A, "doc-1", 1)
    for kwargs in ({"query": "x" * (DEFAULT_LIMITS.query_max_chars + 1)},
                   {"query": "   "}, {"query": ""},
                   {"query": "a", "page_size": 51}, {"query": "a", "page_size": 0},
                   {"query": "a", "page": 0}, {"query": "a", "page": -3}):
        with pytest.raises(ToolError, match="invalid-request") as caught:
            await search_documents(ctx(ORG_A), store, papra, index.reader(), **kwargs)
        assert str(caught.value).startswith("invalid-request")


async def test_the_query_that_is_measured_is_the_query_that_leaves(store):
    """`PAPRA_INDEX_QUERY_MAX_CHARS` bounds what the index is sent, not a copy of it.

    Nine hundred spaces and an `x` is a one-character question. Measuring the stripped copy while
    forwarding the raw one let 901 characters past a 512-character bound, so the bound did not bound
    the request at all; and a caller who padded a genuinely over-long query must still be refused,
    with the length they are being refused for. Both halves are asserted against the recorded query
    string, because the claim is about the request, not about a local variable.
    """
    papra = own_corpus(store, 1)
    index = ScriptedIndex(hits=[kz_row(1)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), " " * 900 + "x")
    assert [item.document_id for item in page.items] == ["doc-1"]
    assert [params["q"] for params in index.search_params] == ["x"]

    over = " " * 900 + "x" * (DEFAULT_LIMITS.query_max_chars + 1)
    before = len(index.search_calls)
    with pytest.raises(ToolError, match="invalid-request") as caught:
        await search_documents(ctx(ORG_A), store, papra, index.reader(), over)
    assert str(DEFAULT_LIMITS.query_max_chars + 1) in str(caught.value)
    assert len(index.search_calls) == before        # refused before the index was asked


async def test_recent_list_limit_is_refused_rather_than_clamped(store):
    index = ScriptedIndex(rows=[kz_document_row(1)])
    papra = FakePapra({"doc-1": papra_doc("doc-1")})
    bind(store, ORG_A, "doc-1", 1)
    for limit in (0, -1, 51):
        with pytest.raises(ToolError, match="invalid-request"):
            await list_recent_documents(ctx(ORG_A), store, papra, index.reader(), limit=limit)


# --- the ledger is the scope filter -----------------------------------------------------------


def four_buckets(store):
    """Four buckets, one document each, one Kreuzakt row each — the smallest corpus where a
    missing scope filter is visible at all."""
    for index, org in enumerate((ORG_A, ORG_B, ORG_C, ORG_D), start=1):
        bind(store, org, f"doc-{org}", index)
    return ScriptedIndex(hits=[kz_row(i) for i in (1, 2, 3, 4)]), FakePapra({
        "doc-org-a": papra_doc("doc-org-a", org=ORG_A),
        "doc-org-b": papra_doc("doc-org-b", org=ORG_B),
        "doc-org-c": papra_doc("doc-org-c", org=ORG_C),
        "doc-org-d": papra_doc("doc-org-d", org=ORG_D),
    })


async def test_four_buckets_four_tokens_see_nothing_foreign(store):
    """Mutation check (a): replace the scope filter with `if True:` and this dies first.

    Every token is granted exactly one bucket, the index answers all four rows to every query, and
    the ledger is the only thing between one bucket's credential and the other three documents. The
    assertion is on both the set of buckets *and* the ids, because a filter that kept the right
    bucket count but picked the wrong documents would pass a bucket-set assertion.
    """
    index, papra = four_buckets(store)
    reader = index.reader()
    allowed = {ORG_A: {"doc-org-a"}, ORG_B: {"doc-org-b"}, ORG_C: {"doc-org-c"},
               ORG_D: {"doc-org-d"}}
    for org, documents in allowed.items():
        page = await search_documents(ctx(org), store, papra, reader, "rechnung")
        assert {item.org_id for item in page.items} == {org}
        assert {item.document_id for item in page.items} == documents
        assert page.items[0].org_id == org


async def test_an_all_buckets_token_sees_all_four(store):
    """The other half of the same filter: `scope_all` widens to the corpus, and only that way.

    `--all-buckets` is a real grant with a real meaning, and it is exercised here so that "the
    filter is not just a bug that hides everything" is asserted by the same fixture.
    """
    index, papra = four_buckets(store)
    page = await search_documents(ctx(scope_all=True), store, papra, index.reader(), "rechnung")
    assert [item.org_id for item in page.items] == [ORG_A, ORG_B, ORG_C, ORG_D]
    assert page.partial is False


async def test_a_hit_with_no_ledger_row_is_invisible(store):
    """Kreuzakt knows a document this suite never uploaded, or already forgot. Fails closed.

    No error, no stat that reads like a failure: `dropped_unmatched` counts it, and the row is gone.
    The alternative — showing a hit whose reference the ledger cannot name — would hand an agent a
    document with no bucket and no Papra id, i.e. one it cannot open and this suite cannot account
    for. The drift question ("why does the index hold this?") belongs to `papra-suite check`.
    """
    index, papra = four_buckets(store)
    page = await search_documents(ctx(scope_all=True), store, papra, index.reader(), "rechnung")
    # …then the same query with a row the ledger has never heard of.
    index.hits.append(kz_row(4242))
    after = await search_documents(ctx(scope_all=True), store, papra, index.reader(), "rechnung")
    assert len(after.items) == len(page.items)
    assert after.stats.dropped_unmatched == 1
    assert after.partial is False          # nothing was withheld: the row is not ours to show


async def test_a_hit_on_the_wrong_generation_is_invisible(store):
    """After a rebuild, an id from the destroyed index resolves to nothing.

    `ledger_resolve` filters on `delivered_generation`, so a `bump_generation` is the whole
    invalidation — the tools inherit it rather than deciding about generations themselves. Showing
    them anyway would attribute a document to text extracted from an index that no longer exists.
    """
    index, papra = four_buckets(store)
    before = await search_documents(ctx(scope_all=True), store, papra, index.reader(), "rechnung")
    assert len(before.items) == 4
    store.bump_generation()
    after = await search_documents(ctx(scope_all=True), store, papra, index.reader(), "rechnung")
    assert after.items == []
    assert after.stats.dropped_unmatched == 4


async def test_one_hit_two_authorized_references_yields_two_items(store):
    """Identical bytes in two buckets of one grant: one Kreuzakt hit, two Papra documents.

    This is why resolution returns references rather than documents, and why `expanded` counts
    *extra items*: a hit that resolves to two authorised references produces two results, in hit
    order, and the second is not a duplicate of the first — different bucket, different Papra id.
    """
    bind(store, ORG_A, "doc-1", 1)
    bind(store, ORG_B, "doc-2", 1)
    index = ScriptedIndex(hits=[kz_row(1)])
    papra = FakePapra({"doc-1": papra_doc("doc-1", org=ORG_A),
                       "doc-2": papra_doc("doc-2", org=ORG_B)})
    page = await search_documents(ctx(ORG_A, ORG_B), store, papra, index.reader(), "rechnung")
    assert [item.document_id for item in page.items] == ["doc-1", "doc-2"]
    assert [item.org_id for item in page.items] == [ORG_A, ORG_B]
    assert page.stats.expanded == 1
    assert page.stats.kept == 2


# --- I-1: per-reference authorization, the case nothing else in this file contains ------------


def shared_hit(store, granted, foreign, *, kreuzakt_id=1):
    """One Kreuzakt row that is two buckets' documents, only one of them this token's.

    The shape is not exotic: two buckets hold byte-identical uploads, the content-addressed index
    stores one row, and `store.ledger_resolve` answers it with both references ordered by
    `papra_org_id` (`store.py:1748-1752`). That ordering is what makes a per-*hit* filter dangerous:
    whichever bucket sorts first decides the whole hit's fate — so the helper is used with the
    granted bucket in both positions.
    """
    bind(store, granted, f"doc-{granted}", kreuzakt_id)
    bind(store, foreign, f"doc-{foreign}", kreuzakt_id)
    papra = FakePapra()
    papra.add_for(granted, f"doc-{granted}")
    papra.add_for(foreign, f"doc-{foreign}")
    return ScriptedIndex(hits=[kz_row(kreuzakt_id)]), papra


@pytest.mark.parametrize("granted,foreign", [(ORG_A, ORG_B), (ORG_B, ORG_A)])
async def test_a_hit_shared_with_a_foreign_bucket_shows_only_the_granted_reference(store, granted,
                                                                                   foreign):
    """Authorization is per `LedgerRef`, and both reference orderings are asserted.

    This is the corpus that distinguishes "filter every reference by its own bucket" from "admit a
    hit if its first reference's bucket is granted", which the suite could not: `ledger_resolve`
    orders by `papra_org_id`, so a granted `org-a` sorts first and a per-hit filter hands it
    `org-b`'s id, title and filename, while a granted `org-b` sorts last and the same filter
    silently withholds a document that *is* the caller's. Both directions are here because each
    kills a different half of that mistake.

    The load-bearing assertion is the second one: `papra.requested` names only the granted
    reference, so the foreign document is not merely absent from the answer — it is never asked
    about at all.
    """
    index, papra = shared_hit(store, granted, foreign)
    page = await search_documents(ctx(granted), store, papra, index.reader(), "rechnung")

    assert [(item.org_id, item.document_id) for item in page.items] == [(granted, f"doc-{granted}")]
    assert papra.requested == [(granted, f"doc-{granted}")]
    assert papra.get_calls == 1
    assert page.stats.kept == 1 and page.stats.expanded == 0
    assert page.buckets_reached == [granted] and page.buckets_skipped == []
    # The hit survived, so a withheld *reference* is not a withheld hit, and nothing here is
    # uncertain: `partial` stays false and `expanded` stays 0 for the same reason.
    assert page.partial is False


async def test_a_bucket_whose_name_only_begins_like_a_granted_one_is_not_granted(store):
    """`org-a` does not grant `org-ab`: the comparison on a bucket id is exact, not a prefix.

    A prefix match (`ref.papra_org_id.startswith(o)`) reads as harmless because bucket ids are short
    and distinct in practice, and it over-grants every bucket whose id extends a granted one. Both
    shapes are asserted because they fail differently: on a shared hit nothing is even counted, and
    on two separate rows the counter tells the operator something.
    """
    index, papra = shared_hit(store, ORG_A, ORG_AB)
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung")
    assert [(item.org_id, item.document_id) for item in page.items] == [(ORG_A, "doc-org-a")]
    assert papra.requested == [(ORG_A, "doc-org-a")]

    # …and the foreign bucket as its own index row, which a prefix match would also have kept.
    bind(store, ORG_AB, "doc-ab-2", 2)
    other = ScriptedIndex(hits=[kz_row(1), kz_row(2)])
    fresh = FakePapra()
    fresh.add_for(ORG_A, "doc-org-a")
    fresh.add_for(ORG_AB, "doc-ab-2")
    second = await search_documents(ctx(ORG_A), store, fresh, other.reader(), "rechnung")
    assert [item.org_id for item in second.items] == [ORG_A]
    assert second.stats.kept == 1
    assert fresh.requested == [(ORG_A, "doc-org-a")]


async def test_the_same_papra_id_in_two_granted_buckets_is_two_items(store):
    """Deduplication is keyed by `(org_id, document_id)`, because a Papra id is per bucket.

    Two organisations can hold documents whose ids happen to be equal. Dropping the bucket from the
    emitted key turns the second one's result into a duplicate of the first's and quietly halves the
    answer, which no assertion on `document_id` alone can see.
    """
    bind(store, ORG_A, "doc-shared", 1)
    bind(store, ORG_B, "doc-shared", 1)
    index = ScriptedIndex(hits=[kz_row(1)])
    papra = FakePapra()
    papra.add_for(ORG_A, "doc-shared")
    papra.add_for(ORG_B, "doc-shared")
    page = await search_documents(ctx(ORG_A, ORG_B), store, papra, index.reader(), "rechnung")
    assert [(item.org_id, item.document_id) for item in page.items] == [(ORG_A, "doc-shared"),
                                                                        (ORG_B, "doc-shared")]
    assert papra.requested == [(ORG_A, "doc-shared"), (ORG_B, "doc-shared")]
    assert page.stats.kept == 2 and page.stats.expanded == 1


async def test_the_liveness_answer_is_cached_per_bucket_not_per_document_id(store):
    """The cache key is the reference, so one bucket's `isDeleted` never answers for another's.

    The two documents share an id and disagree about being in the trash. A cache keyed by document
    id alone lets the first lookup answer for the second, which would show a deleted document — the
    one stale answer an agent could act on by opening it — and count nothing.
    """
    bind(store, ORG_A, "doc-shared", 1)
    bind(store, ORG_B, "doc-shared", 1)
    index = ScriptedIndex(hits=[kz_row(1)])
    papra = FakePapra()
    papra.add_for(ORG_A, "doc-shared")
    papra.add_for(ORG_B, "doc-shared", isDeleted=True)
    page = await search_documents(ctx(ORG_A, ORG_B), store, papra, index.reader(), "rechnung")
    assert [(item.org_id, item.document_id) for item in page.items] == [(ORG_A, "doc-shared")]
    assert papra.requested == [(ORG_A, "doc-shared"), (ORG_B, "doc-shared")]
    assert papra.get_calls == 2                      # both buckets asked, once each
    assert page.stats.kept == 1 and page.stats.dropped_deleted == 1


async def test_the_wire_hides_the_foreign_reference_of_a_shared_hit(store):
    """The mixed-bucket hit over the real transport: bearer gate → scope → per-reference filter.

    The in-process version above could be satisfied by a helper called with a convenient context;
    this one goes through the registered tool with a minted token, which is where a scope that
    quietly widened would actually appear.
    """
    index, papra = shared_hit(store, ORG_A, ORG_B)
    token = mint_token(store, label="scoped", org_ids=[ORG_A])
    _response, message = wire_call(store, token, "search_documents", {"query": "rechnung"},
                                   kz=index.reader(), papra=papra)
    payload = message["result"]["structuredContent"]
    assert [(i["org_id"], i["document_id"]) for i in payload["items"]] == [(ORG_A, "doc-org-a")]
    assert papra.requested == [(ORG_A, "doc-org-a")]
    assert ORG_B not in json.dumps(message)


async def test_the_recent_listing_hides_the_foreign_reference_of_a_shared_hit(store):
    """The same hit through `list_recent_documents`, which shares `resolve_hits` and must share it.

    Two resolvers would be two scope rules, and a listing that filtered differently from a search
    would leak the bucket the search hid.
    """
    index, papra = shared_hit(store, ORG_A, ORG_B)
    index.rows = [kz_document_row(1)]        # the same row, in the listing envelope
    page = await list_recent_documents(ctx(ORG_A), store, papra, index.reader(), limit=10)
    assert [(item.org_id, item.document_id) for item in page.items] == [(ORG_A, "doc-org-a")]
    assert papra.requested == [(ORG_A, "doc-org-a")]
    assert page.stats.kept == 1 and page.stats.expanded == 0


async def test_the_same_document_twice_in_one_page_is_emitted_once(store):
    """Deduplicated by `(org_id, document_id)`, which is unique per reference by construction.

    The realistic shape is Kreuzakt's ordering shifting between two page fetches of a live index:
    the same document appearing in both windows must not occupy two slots of the caller's page.
    """
    bind(store, ORG_A, "doc-1", 1)
    index = ScriptedIndex(hits=[kz_row(1), kz_row(1)])
    papra = FakePapra({"doc-1": papra_doc("doc-1")})
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung")
    assert [item.document_id for item in page.items] == ["doc-1"]
    assert papra.get_calls == 1


async def test_a_hit_in_a_foreign_bucket_and_a_granted_one_is_kept_for_the_granted_only(store):
    """`dropped_unauthorized` is a bucket decision, not a missing-row decision.

    Both counters matter: they are the difference between "your token does not cover that" and "our
    ledger has never held that", and an operator reading an empty result needs to know which of the
    two happened. The item list is the same either way, which is exactly why the counters exist.
    """
    bind(store, ORG_A, "doc-own", 1)
    bind(store, ORG_B, "doc-other", 2)
    index = ScriptedIndex(hits=[kz_row(2), kz_row(1)])
    papra = FakePapra({"doc-own": papra_doc("doc-own"), "doc-other": papra_doc("doc-other",
                                                                               org=ORG_B)})
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung")
    assert [item.document_id for item in page.items] == ["doc-own"]
    assert page.stats.dropped_unauthorized == 1
    assert page.stats.dropped_unmatched == 0


async def test_a_deleted_papra_document_is_dropped_and_counted(store):
    """The ledger says a reference is live; Papra says the document is in the trash.

    Papra's live state wins, because the promise is that a result can be opened. The drop is counted
    rather than silent, and it does not make the page `partial`: nothing was left uncertain, the
    archive answered, and the answer was "deleted".
    """
    bind(store, ORG_A, "doc-1", 1)
    index = ScriptedIndex(hits=[kz_row(1)])
    papra = FakePapra({"doc-1": papra_doc("doc-1", isDeleted=True)})
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung")
    assert page.items == []
    assert stats_of(page).dropped_deleted == 1
    assert page.partial is False


async def test_a_papra_lookup_error_drops_the_item_and_is_counted(store):
    """Fail closed, but say the answer may be incomplete.

    A lookup that errored is not a document that does not exist — it is a document we could not
    check — so it is dropped *and* the page is `partial`. Reporting it as a hard error would be
    wrong too: the search itself succeeded, and one unreachable document is not a reason to answer
    nothing. The stat plus the flag is the honest pair.
    """
    bind(store, ORG_A, "doc-1", 1)
    index = ScriptedIndex(hits=[kz_row(1)])
    papra = FakePapra()
    papra.raise_on_get = True
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung")
    assert page.items == []
    assert stats_of(page).dropped_lookup_error == 1
    assert page.partial is True


async def test_papra_state_is_checked_only_for_items_being_returned(store):
    """The cost of the fail-closed rule, bounded: one liveness read per candidate, none per hit.

    A page of 100 hits of which this token may see 5 costs 5 Papra reads, not 100 — the mutation
    pins is "check everything Kreuzakt returned, then filter", which turns a search into 100
    and a rate limit into a search failure. The `+1` is the tolerance the plan's own assertion
    the honest number is 5 and that is what is asserted here.
    """
    for index in range(1, 6):
        bind(store, ORG_A, f"doc-{index}", index)
    foreign = [kz_row(500 + i) for i in range(95)]
    index = ScriptedIndex(hits=foreign + [kz_row(i) for i in range(1, 6)])
    papra = FakePapra({f"doc-{i}": papra_doc(f"doc-{i}") for i in range(1, 6)})
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung",
                                  page_size=5)
    returned = [item.document_id for item in page.items]
    assert returned == [f"doc-{i}" for i in range(1, 6)]
    assert papra.get_calls == len(returned)
    assert papra.get_calls <= len(returned) + 1


async def test_one_ledger_query_per_page_never_one_per_hit(store):
    """The spec's §4.5 cost rule, asserted as a call count.

    Authorization is what makes a Kreuzakt answer usable, so it must not cost a round trip per row:
    five fetched pages, five `ledger_resolve` calls, whatever the hits inside them. This is
    the difference between a search that reads the ledger once per page and one that reads it 500
    times while a single-writer WAL database holds everyone else waiting.
    """
    index = ScriptedIndex(hits=[kz_row(i) for i in range(1, 901)])
    papra = FakePapra()
    calls = []

    class CountingStore:
        """The store as the tools see it, with the authorization read counted."""

        def __init__(self, inner):
            self._inner = inner

        def get_generation(self):
            return self._inner.get_generation()

        def ledger_resolve(self, kreuzakt_ids, *, generation):
            calls.append(len(list(kreuzakt_ids)))
            return self._inner.ledger_resolve(kreuzakt_ids, generation=generation)

    page = await search_documents(ctx(scope_all=True), CountingStore(store), papra,
                                  index.reader(), "versicherung", page_size=20)
    assert len(calls) == len(index.search_calls) == page.scanned // KREUZAKT_PAGE_ROWS
    assert all(count <= KREUZAKT_PAGE_ROWS for count in calls)


# --- the page: filled, exhausted, or honest about which --------------------------------------


def own_corpus(store, count, *, start=1, org=ORG_A):
    """`count` documents of one bucket, one Kreuzakt row each, all live in Papra."""
    for n in range(start, start + count):
        bind(store, org, f"doc-{n}", n)
    return FakePapra({f"doc-{n}": papra_doc(f"doc-{n}", org=org)
                      for n in range(start, start + count)})


async def test_a_later_page_continues_where_the_first_one_stopped(store):
    """`page` counts *returned* items, so page 2 starts where page 1 ended, in index order."""
    papra = own_corpus(store, 30)
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 31)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung",
                                  page=2, page_size=5)
    assert [item.document_id for item in page.items] == [f"doc-{n}" for n in range(6, 11)]
    assert page.page == 2 and page.page_size == 5
    assert page.partial is False


async def test_paging_is_by_page_and_never_sends_the_ignored_offset(store):
    """`offset` demonstrably does nothing upstream, so a request that carried it would lie.

    Asserted on the query string rather than on the recorded `(page, limit)` pair: the whole stop
    condition depends on paging being real, and a client-side `offset` that Kreuzakt ignores is a
    page that silently repeats the first hundred rows.
    """
    papra = own_corpus(store, 250)
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 251)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung",
                                  page=3, page_size=20)
    assert [item.document_id for item in page.items] == [f"doc-{n}" for n in range(41, 61)]
    assert all("offset" not in params for params in index.search_params)
    assert {int(params["page"]) for params in index.search_params} == {1}


async def test_page_size_of_100_rows_stops_at_exhaustion_not_budget(store):
    """Upstream serves 100 rows a request; one request fills a 50-item page and the scan ends."""
    papra = own_corpus(store, 100)
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 101)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "miete",
                                  page_size=50)
    assert page.partial is False
    assert page.scanned == 100
    assert index.search_calls == [(1, KREUZAKT_PAGE_ROWS)]


async def test_a_short_page_whose_total_still_claims_rows_is_partial(store):
    """An index that does not honour its own `total` gets "I may have missed something".

    This is the case a completeness flag exists for: the answer is short, nothing errored, and the
    only honest statement is that the window cannot be proven complete. Silence here would read as
    a small corpus forever.
    """
    own_corpus(store, 3)
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 11)], total=40)
    page = await search_documents(ctx(ORG_A), store, FakePapra(), index.reader(), "miete")
    assert page.scanned == 10
    assert page.partial is True
    assert "short page" in (page.partial_reason or "")


async def test_budget_exhaustion_reports_partial_and_counts_scanned(store):
    """A scan that spends the row budget says so instead of claiming a small corpus."""
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 901)])
    page = await search_documents(ctx(ORG_A), store, FakePapra(), index.reader(), "versicherung",
                                  page=1, page_size=20)
    assert page.items == [] and page.partial is True
    assert page.scanned == DEFAULT_LIMITS.scan_rows
    assert "budget" in (page.partial_reason or "")
    assert index.search_calls == [(n, KREUZAKT_PAGE_ROWS) for n in range(1, 6)]


async def test_page_past_the_end_is_an_empty_page_not_an_error(store):
    """Page 9 of a three-document archive is a short answer that is *not* incomplete."""
    papra = own_corpus(store, 3)
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 4)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "miete",
                                  page=9, page_size=20)
    assert page.items == [] and page.page == 9
    assert page.partial is False


async def test_papra_reads_are_bounded_by_the_requested_page_not_the_scanned_window(store):
    """The cost half of the liveness rule: 500 scanned rows do not cost 500 Papra requests.

    The window is scanned because a caller may ask for page 9, but the liveness check is only owed
    to the items that can be returned — and it is the checking that costs a request. A page of 5
    out of 100 live hits is five reads, and the row budget is what bounds a call, not the number of
    documents a search happened to look at.
    """
    papra = own_corpus(store, 100)
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 101)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung",
                                  page_size=5)
    assert len(page.items) == 5
    assert papra.get_calls == 5


# --- the fan-out bound, enforced here rather than at mint time --------------------------------


def twelve_buckets(store):
    """One document in each of twelve buckets — more than `MAX_BUCKETS_PER_CALL` grants per call.

    This corpus exists because of how tokens are minted: `mcp_tokens.MAX_ORG_IDS` refuses a ninth
    bucket on a *scoped* token, but `--all-buckets` is exempt from that ceiling by design, and
    `--all-buckets` is how an operator's own credential is minted. A bound that only bites at mint
    time therefore bounds the tokens that could never exceed it, and leaves the one token that can
    unbounded.
    """
    orgs = [f"org-{n:02d}" for n in range(1, 13)]
    documents = {}
    for n, org in enumerate(orgs, start=1):
        bind(store, org, f"doc-{org}", n)
        documents[f"doc-{org}"] = papra_doc(f"doc-{org}", org=org)
    return orgs, ScriptedIndex(hits=[kz_row(n) for n in range(1, 13)]), FakePapra(documents)


async def test_an_all_buckets_call_consults_at_most_eight_buckets(store):
    orgs, index, papra = twelve_buckets(store)
    page = await search_documents(ctx(scope_all=True), store, papra, index.reader(), "rechnung",
                                  page_size=50)
    assert len(page.buckets_reached) == MAX_BUCKETS_PER_CALL
    assert len(page.items) == MAX_BUCKETS_PER_CALL
    assert len(page.buckets_skipped) == len(orgs) - MAX_BUCKETS_PER_CALL
    assert set(page.buckets_skipped).isdisjoint(page.buckets_reached)
    assert page.stats.dropped_unreached == len(orgs) - MAX_BUCKETS_PER_CALL


async def test_a_wider_scope_is_partial_and_names_the_buckets_it_never_read(store):
    """The bound truncates, so it must announce itself — silently-short is the failure mode.

    Naming the skipped buckets is what makes "your answer covers eight of twelve" checkable instead
    of merely flagged: the only token that can hit the bound is one that is already allowed to see
    every bucket, so the honest report is which ones this call did not look at.
    """
    orgs, index, papra = twelve_buckets(store)
    page = await search_documents(ctx(scope_all=True), store, papra, index.reader(), "rechnung",
                                  page_size=50)
    assert page.partial is True
    assert "bucket" in (page.partial_reason or "")
    for org in page.buckets_skipped:
        assert org in orgs
        assert f"doc-{org}" not in [item.document_id for item in page.items]


async def test_a_scoped_token_never_hits_the_fanout_bound(store):
    """Eight granted buckets is inside the bound: a scoped token is never reported partial for this.

    The bound is an operational ceiling on fan-out, and `MAX_ORG_IDS` is the same number, so a
    scoped credential cannot exceed it. If this ever became `partial`, every scoped search on a full
    grant would arrive flagged for a reason the operator cannot act on.
    """
    orgs, index, papra = twelve_buckets(store)
    granted = orgs[:MAX_ORG_IDS]
    page = await search_documents(ctx(*granted), store, papra, index.reader(), "rechnung",
                                  page_size=50)
    assert len(page.items) == MAX_ORG_IDS
    assert page.buckets_skipped == [] and page.partial is False


async def test_lookups_run_at_most_four_at_a_time(store):
    """The spec's fan-out concurrency, measured on the fake rather than asserted in prose."""
    orgs, index, papra = twelve_buckets(store)
    papra.delay_s = 0.01
    await search_documents(ctx(scope_all=True), store, papra, index.reader(), "rechnung",
                           page_size=50)
    assert papra.peak_concurrent <= MAX_CONCURRENT_LOOKUPS
    assert papra.peak_concurrent > 1          # and it really is concurrent, not serial
    assert papra.get_calls == MAX_BUCKETS_PER_CALL


async def test_a_papra_document_is_asked_about_once_per_call_and_again_on_the_next(store):
    """The liveness cache is per call: honest within an answer, and not a session.

    The claim a result makes is "live as of this answer". A cache that outlived the call would
    answer a second search with the first search's knowledge — including a document somebody deleted
    in between, which is the one stale answer an agent could act on by opening it.
    """
    papra = own_corpus(store, 3)
    reader = ScriptedIndex(hits=[kz_row(n) for n in range(1, 4)]).reader()
    first = await search_documents(ctx(ORG_A), store, papra, reader, "rechnung")
    assert papra.get_calls == 3 and first.scanned == 3
    assert len(papra.requested) == len(set(papra.requested))     # no document asked twice
    second = await search_documents(ctx(ORG_A), store, papra, reader, "rechnung")
    assert second.scanned == 3
    assert papra.get_calls == 6                                  # asked again, deliberately


# --- nothing upstream leaves here -------------------------------------------------------------

#: Every string the plan forbids on the client-visible surface, plus the two bodies the captured
#: payload carries. A hit arrives with `content`, `snippet` and `thumbnail_url` on it; the answer
#: must carry none of them.
LEAKAGE_NEEDLES = ("/api/documents/", "/documents/", "/thumbnail", "/original", "kreuzakt",
                   "Kreuzakt", "PAPRA-INTERNAL-TEXT", "stored_filename", "thumbnail_url",
                   "snippet", "content")


async def test_the_search_answers_from_the_captured_real_payload(store):
    """Byte-exact against Task 3's capture, not against a hand-written dict of what it looked like.

    The captured row is the only evidence there is that `KzHit` picks the fields up from the names
    the server actually sends: `original_filename` and `added_at` are read here out of the fixture,
    so a rename upstream breaks this test rather than silently blanking an agent's result list. The
    fixture's hit is Kreuzakt id 1, which is bound to `doc-1` below.
    """
    bind(store, ORG_A, "doc-1", 1)
    papra = FakePapra({"doc-1": papra_doc("doc-1", name="The name Papra holds")})
    index = ScriptedIndex()
    index.served_raw = load("kreuzakt_search_page.json")
    page = await search_documents(ctx(scope_all=True), store, papra, index.reader(), "Ziphoid")

    assert [item.document_id for item in page.items] == ["doc-1"]
    assert page.items[0].original_filename == "papra-search0001-11111111.pdf"
    assert page.items[0].added_at == "2026-11-17T22:28:04.906Z"
    assert page.items[0].title == "The name Papra holds"     # live Papra wins over the index
    assert page.items[0].document_date is None
    assert page.scanned == 1 and page.partial is False
    assert page.stats.kept == 1


async def test_the_json_an_agent_gets_names_no_kreuzakt_path_or_integer_id(store):
    """The identifiers on the wire are Papra's, and that is a rule, not a formatting choice.

    Every needle is something the upstream response genuinely carries in this test's own fixture —
    the row builder puts `content`, `snippet` and `thumbnail_url` on every hit, and the Kreuzakt
    document rows carry a `stored_filename`. So a leak is a failure here rather than a hope that the
    row happened not to contain it. An agent that was handed `/api/documents/7/thumbnail` would take
    it as a URL to give the user, and there is nothing behind it the user may reach: Kreuzakt has no
    buckets and is not published.
    """
    papra = own_corpus(store, 4)
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 5)])
    blob = (await search_documents(ctx(scope_all=True), store, papra, index.reader(),
                                   "rechnung")).model_dump_json()
    for needle in LEAKAGE_NEEDLES:
        assert needle not in blob, f"{needle!r} reached the client"
    # And the ids it *does* carry are the ones `get_document` can be called with.
    assert "doc-1" in blob and ORG_A in blob


async def test_a_recent_listing_leaks_no_upstream_path_or_storage_name(store):
    """`/api/documents` rows carry `thumbnail_url` and `stored_filename`; neither survives."""
    papra = own_corpus(store, 3)
    index = ScriptedIndex(rows=[kz_document_row(n) for n in range(1, 4)])
    blob = (await list_recent_documents(ctx(scope_all=True), store, papra, index.reader(),
                                        limit=3)).model_dump_json()
    for needle in LEAKAGE_NEEDLES:
        assert needle not in blob, f"{needle!r} reached the client"
    assert "kreuzakt-internal-storage-name.pdf" not in blob


# --- `list_recent_documents`: Kreuzakt's order, Kreuzakt's `since` ----------------------------


async def test_the_recent_listing_keeps_the_indexes_own_order(store):
    """No local sort: the order `/api/documents` answered is the order the agent gets.

    The rows here are deliberately *not* in `added_at` order — newest is not first — because a sort
    would make that test pass. Sorting by date locally would be a second, worse index: this suite
    has no timestamp that means *indexed* and no comparable score across buckets, so an order it
    invented would be a claim about the archive that only the archive can make. What Kreuzakt
    returns is what it says it returned, and passing that through is the honest thing.
    """
    papra = own_corpus(store, 4)
    rows = [kz_document_row(n, added_at=f"2026-11-{10 + n:02d} 09:00:00") for n in (3, 1, 4, 2)]
    index = ScriptedIndex(rows=rows)
    page = await list_recent_documents(ctx(scope_all=True), store, papra, index.reader(), limit=4)
    assert [item.document_id for item in page.items] == ["doc-3", "doc-1", "doc-4", "doc-2"]
    assert [item.added_at for item in page.items] == ["2026-11-13 09:00:00", "2026-11-11 09:00:00",
                                                      "2026-11-14 09:00:00", "2026-11-12 09:00:00"]
    assert page.page == 1 and page.page_size == 4 and page.partial is False


async def test_the_recent_listing_is_filtered_by_the_ledger_too(store):
    """The same resolver, so a listing cannot leak a bucket the search would have hidden."""
    for index, org in enumerate((ORG_A, ORG_B), start=1):
        bind(store, org, f"doc-{org}", index)
    kz_index = ScriptedIndex(rows=[kz_document_row(1), kz_document_row(2)])
    papra = FakePapra({"doc-org-a": papra_doc("doc-org-a", org=ORG_A),
                       "doc-org-b": papra_doc("doc-org-b", org=ORG_B)})
    page = await list_recent_documents(ctx(ORG_A), store, papra, kz_index.reader(), limit=10)
    assert [item.org_id for item in page.items] == [ORG_A]
    assert page.stats.dropped_unauthorized == 1


async def test_the_recent_listing_pages_by_page_under_the_same_budget(store):
    """100 rows a request, the scan budget as the ceiling, and `partial` when it runs out."""
    papra = own_corpus(store, 30)
    index = ScriptedIndex(rows=[kz_document_row(n) for n in range(1, 31)])
    page = await list_recent_documents(ctx(ORG_A), store, papra, index.reader(), limit=5)
    assert page.scanned == 30                       # the rows it actually looked at, 5 returned
    assert len(page.items) == 5 and page.partial is False
    assert index.list_calls == [(1, KREUZAKT_PAGE_ROWS, None)]
    assert all("offset" not in params for params in index.list_params)


async def test_a_recent_listing_that_spends_the_budget_is_partial(store):
    """A scan that stops mid-corpus never reports "these are the newest" without saying so."""
    index = ScriptedIndex(rows=[kz_document_row(n) for n in range(1, 901)])
    page = await list_recent_documents(ctx(ORG_A), store, FakePapra(), index.reader(), limit=5)
    assert page.items == []
    assert page.scanned == DEFAULT_LIMITS.scan_rows
    assert page.partial is True and "budget" in (page.partial_reason or "")
    assert len(index.list_calls) == DEFAULT_LIMITS.scan_rows // KREUZAKT_PAGE_ROWS


async def test_since_goes_upstream_in_kreuzakts_own_form(store):
    """The value that filters is `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`, unchanged.

    Measured against the pinned container: `"2026-11-17 22:28:05"` filters, while the same instant
    as `2026-11-17T22:28:05Z` answers 200 with **no** rows and an epoch integer answers it with
    **every** row. Both wrong shapes look like a working request, which is why a wrong one is a
    refusal here instead of a silent empty listing forever.
    """
    papra = own_corpus(store, 2)
    index = ScriptedIndex(rows=[kz_document_row(1), kz_document_row(2)])
    await list_recent_documents(ctx(ORG_A), store, papra, index.reader(), limit=5,
                                since="2026-11-17 22:28:05")
    assert index.list_calls[0][2] == "2026-11-17 22:28:05"


@pytest.mark.parametrize("since", ["2026-01-01", " 2026-01-01 ", "2026-01-01\n",
                                   "2026-11-17 22:28:05", "\t2026-11-17 22:28:05 "])
def test_every_since_this_tool_accepts_the_wire_validator_also_accepts(since):
    """One value, checked twice, agreed on both times.

    The whole of I-3 in one line: the tool validated a stripped copy while the client validated and
    sent the raw one, so a watermark could clear the first check and fail the second — and the
    failure came back to the caller as an index outage. Asserting the two validators agree on the
    forwarded value is cheaper and steadier than asserting it about any particular message.
    """
    from papra_suite.kreuzakt_client import _require_documents_since
    from papra_suite.mcp_tools import check_recent_request

    forwarded = check_recent_request(5, since, limits=DEFAULT_LIMITS)
    assert _require_documents_since(forwarded) == forwarded.strip()


@pytest.mark.parametrize("since", ["2026-11-17T22:28:05Z", "2026-11-17T22:28:05", "1700000000",
                                   "17.11.2026", "yesterday", "",
                                   # the shapes `str.strip()` cannot rescue: a second line, a suffix,
                                   # and a time the two-field form cannot mean
                                   "2026-01-01\n2026-01-02", "2026-01-01 22:28:05 UTC",
                                   "2026-01-01 1:02:03"])
async def test_a_since_the_index_would_ignore_is_refused(store, since):
    """`invalid-request`, not a conversion: rewriting it would pick a timezone and hide that.

    The column Kreuzakt compares against has no documented timezone (Task 3 could not establish it),
    so `2026-11-17T22:28:05Z` is not a value this program can turn into a watermark without
    inventing a meaning for the instant — and an agent that got a converted answer would have no way
    to know which day it had actually been asked about.
    """
    papra = own_corpus(store, 1)
    index = ScriptedIndex(rows=[kz_document_row(1)])
    with pytest.raises(ToolError, match="invalid-request") as caught:
        await list_recent_documents(ctx(ORG_A), store, papra, index.reader(), limit=5, since=since)
    assert "YYYY-MM-DD" in str(caught.value)
    assert index.list_calls == []          # refused before anything was asked of the index


@pytest.mark.parametrize("since", [" 2026-01-01 ", "2026-01-01\n", "\t2026-11-17 22:28:05"])
async def test_a_padded_since_is_stripped_before_it_leaves(store, since):
    """A watermark padded with whitespace means the day the caller meant, and leaves stripped.

    Measured before the fix: `" 2026-01-01 "` was checked stripped, forwarded raw, rejected by the
    client's own validator, and reported to the agent as **`index_unavailable`: the index would not
    answer this request** — an outage for the caller's own typo, which invites an agent to retry a
    request that can never succeed. `"2026-01-01\n"` was worse: it cleared both validators (Python's
    `$` matches before a trailing newline) and reached the wire with the newline on it, which is the
    exact shape this file says it refuses. One stripped value is checked and sent, so neither
    happens: padding is padding, and a `\n` in the middle is a refusal, not a mystery filter.
    """
    papra = own_corpus(store, 1)
    index = ScriptedIndex(rows=[kz_document_row(1)])
    page = await list_recent_documents(ctx(ORG_A), store, papra, index.reader(), limit=5,
                                       since=since)
    forwarded = index.list_params[0]["since"]
    assert forwarded == since.strip()
    assert forwarded == forwarded.strip()          # nothing whitespace-shaped left to confuse anyone
    assert not any(c in forwarded for c in "\n\t")
    assert [item.document_id for item in page.items] == ["doc-1"]


# --- the principal is the only scope, and it comes from the bearer gate -----------------------

PUBLIC_URL = "https://papra-mcp.test"
HOST = "papra-mcp.test"


@pytest.fixture(autouse=True)
def _close_ledger_views():
    yield
    close_views()


def wire_call(store, token, name, arguments=None, *, kz=None, papra=None, limits=None):
    """One `tools/call` against the real app, over the SDK's own Streamable HTTP transport.

    Built on a `LedgerReader` over the fixture's database, a `KreuzaktReader` on the scripted index
    and the `FakePapra` — so what is under test is the registered tool, its published schema, the
    principal resolution and the views, not a call into `mcp_tools` in a comfortable shape.
    """
    app = build_mcp_app(view_for(store), papra or FakePapra(), kz or ScriptedIndex().reader(),
                        url=PUBLIC_URL, limits=limits)
    payload = {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
               "params": {"name": name, "arguments": arguments or {}}}
    with TestClient(app) as client:
        headers = {"host": HOST, "content-type": "application/json",
                   "accept": "application/json, text/event-stream",
                   "authorization": f"Bearer {token}"}
        initialised = client.post("/mcp", headers=headers, json={
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-11-25", "capabilities": {},
                       "clientInfo": {"name": "pytest", "version": "0"}}})
        headers["mcp-session-id"] = initialised.headers.get("mcp-session-id")
        client.post("/mcp", headers=headers,
                    json={"jsonrpc": "2.0", "method": "notifications/initialized"})
        response = client.post("/mcp", headers=headers, json=payload)
    frames = [json.loads(line[6:]) for line in response.text.splitlines()
              if line.startswith("data: ")]
    return response, frames[0] if frames else {}


def tool_error_text(message):
    result = message.get("result") or {}
    return " ".join(part.get("text", "") for part in result.get("content", [])
                    if part.get("type") == "text")


def registered_tools(store, token):
    """The tool list the server publishes, as the client sees it."""
    with TestClient(build_mcp_app(view_for(store), FakePapra(), ScriptedIndex().reader(),
                                  url=PUBLIC_URL)) as client:
        headers = {"host": HOST, "content-type": "application/json",
                   "accept": "application/json, text/event-stream",
                   "authorization": f"Bearer {token}"}
        initialised = client.post("/mcp", headers=headers, json={
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-11-25", "capabilities": {},
                       "clientInfo": {"name": "pytest", "version": "0"}}})
        headers["mcp-session-id"] = initialised.headers.get("mcp-session-id")
        client.post("/mcp", headers=headers,
                    json={"jsonrpc": "2.0", "method": "notifications/initialized"})
        response = client.post("/mcp", headers=headers,
                               json={"jsonrpc": "2.0", "id": 2, "method": "tools/list",
                                     "arguments": {}})
    frames = [json.loads(line[6:]) for line in response.text.splitlines()
              if line.startswith("data: ")]
    return {tool["name"]: tool for tool in frames[0]["result"]["tools"]}


async def test_only_the_two_search_tools_are_registered(store):
    """Task 14 owns `get_document`, `get_document_content` and `open_document`: none is here yet.

    A tool that is registered early is a tool whose authorization path has not been reviewed, and
    the write flag this milestone gates on is switched on by what the surface can do.
    """
    token = mint_token(store, label="wire", org_ids=[ORG_A])
    assert set(registered_tools(store, token)) == {
        "index_status", "search_documents", "list_recent_documents"}


async def test_the_listener_serves_one_route_and_no_other(store):
    """Condition 3, pinned: there is no second door.

    `RequireAuthMiddleware` wraps the `/mcp` endpoint, so a tool exposed on a hand-written
    Starlette route would be a tool with no bearer gate in front of it. The route table is asserted
    instead of the intent because the mistake is additive: nothing breaks when somebody adds a
    `custom_starlette_routes` entry that "just" serves a health page, and if it ever reads the store
    it reads it unauthenticated.
    """
    app = build_mcp_app(view_for(store), FakePapra(), ScriptedIndex().reader(), url=PUBLIC_URL)
    assert [getattr(route, "path", None) for route in app.routes] == ["/mcp"]


async def test_no_search_tool_takes_an_org_list_a_scope_or_a_token(store):
    """The scope is not an argument, so it cannot be widened by one.

    Asserted against the schema the server publishes, because that is the surface a client is told
    to use: a `org_ids`, `all_buckets` or `scope` parameter would be a way for the caller to state
    its own authorization, and `token_context_of` would stop being the only source of a principal.
    """
    token = mint_token(store, label="wire", org_ids=[ORG_A])
    tools = registered_tools(store, token)
    assert set(tools["search_documents"]["inputSchema"]["properties"]) == {
        "query", "page", "page_size"}
    assert set(tools["list_recent_documents"]["inputSchema"]["properties"]) == {"limit", "since"}
    for tool in tools.values():
        for name in tool["inputSchema"]["properties"]:
            assert not any(word in name.lower() for word in ("org", "scope", "token", "bucket",
                                                            "principal")), name


async def test_the_wire_search_answers_only_what_the_token_was_granted(store):
    """The whole path at once: bearer token → `TokenContext` → ledger filter → items."""
    for index, org in enumerate((ORG_A, ORG_B), start=1):
        bind(store, org, f"doc-{org}", index)
    index = ScriptedIndex(hits=[kz_row(1), kz_row(2)])
    papra = FakePapra({"doc-org-a": papra_doc("doc-org-a", org=ORG_A),
                       "doc-org-b": papra_doc("doc-org-b", org=ORG_B)})
    token = mint_token(store, label="scoped", org_ids=[ORG_A])
    _response, message = wire_call(store, token, "search_documents", {"query": "rechnung"},
                                   kz=index.reader(), papra=papra)
    payload = message["result"]["structuredContent"]
    assert [item["org_id"] for item in payload["items"]] == [ORG_A]
    assert payload["stats"]["dropped_unauthorized"] == 1
    assert payload["partial"] is False


async def test_a_request_with_no_principal_reads_nothing(store, monkeypatch):
    """The fail-closed default, pinned by what it refuses to do rather than by a message.

    With the gate bypassed there is no bucket list, and the two possible readings of "no scope" are
    "nothing" and "everything". This asserts the read never happens: an implementation that defaults
    a missing context to the corpus, or to an empty grant that some later filter happens to honour,
    touches the index and dies here.
    """
    import papra_suite.mcp_server as mcp_server

    monkeypatch.setattr(mcp_server, "token_context_of", lambda request: None)
    for index, org in enumerate((ORG_A, ORG_B), start=1):
        bind(store, org, f"doc-{org}", index)
    index = ScriptedIndex(hits=[kz_row(1), kz_row(2)])
    papra = FakePapra({"doc-org-a": papra_doc("doc-org-a", org=ORG_A)})
    token = mint_token(store, label="scoped", org_ids=[ORG_A])
    _response, message = wire_call(store, token, "search_documents", {"query": "rechnung"},
                                   kz=index.reader(), papra=papra)

    assert message["result"]["isError"] is True
    assert "unauthenticated" in tool_error_text(message)
    assert index.search_calls == []          # refused before the index was consulted
    assert papra.get_calls == 0              # and before any document was looked up


def test_require_principal_refuses_rather_than_inventing_a_scope():
    """The unit-level half of the same rule: no claims, no context, no default.

    `token_context_of` returning a `TokenContext` with an empty `org_ids` would pass a "did it
    return None?" test and still leave the widening question to chance; this pins the refusal at the
    one function every tool calls.
    """
    for request in (None, object(), SimpleNamespace(scope=None)):
        with pytest.raises(Unauthenticated, match="unauthenticated"):
            require_principal(request)


async def test_a_grant_with_no_buckets_sees_nothing(store):
    """An empty scope is no access, not unrestricted access — the boolean's other failure mode.

    Inverting the scope test (`ref.papra_org_id in ctx.org_ids or ctx.scope_all` with the operands
    swapped, or `if ctx.org_ids and ...`) still passes a test that only checks the granted case;
    this is the one that dies.
    """
    own_corpus(store, 3, org=ORG_A)
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 4)])
    page = await search_documents(ctx(), store, FakePapra(), index.reader(), "rechnung")
    assert page.items == []
    assert page.stats.dropped_unauthorized == 3
    assert page.stats.kept == 0


# --- what a call that could not find out says, and what it must not say -----------------------


async def test_a_dead_index_is_a_refusal_and_never_an_empty_page(store):
    """"The index is down" and "nothing matches" are different facts, and only one is a lie.

    The plan's global constraint is a hard error here: an empty page under a dead index is the
    confident wrong answer, and an agent that hears "no results" stops looking. The bounded half is
    the upstream body: `KreuzaktError` interpolates it, so the message carries the exception class
    and the status, never the text someone else's server wrote.
    """
    own_corpus(store, 2)
    index = ScriptedIndex(hits=[kz_row(1)], error=KreuzaktError(
        'HTTP 503 /api/search: {"detail": "UPSTREAM INTERNAL BODY", "rows": 9000}', status=503))
    with pytest.raises(ToolError, match="index_unavailable") as caught:
        await search_documents(ctx(ORG_A), store, FakePapra(), index.reader(), "rechnung")
    message = str(caught.value)
    assert "UPSTREAM INTERNAL BODY" not in message and "9000" not in message
    assert "503" in message                       # what an operator can act on
    assert len(message) <= 300


async def test_a_recent_listing_on_a_dead_index_refuses_too(store):
    index = ScriptedIndex(rows=[kz_document_row(1)], error=KreuzaktError("connection refused"))
    with pytest.raises(ToolError, match="index_unavailable"):
        await list_recent_documents(ctx(ORG_A), store, FakePapra(), index.reader(), limit=5)


async def test_a_ledger_that_cannot_name_a_generation_refuses_rather_than_resolving(store):
    """No current generation means every reference is unresolvable, which is a refusal.

    `get_generation` raises rather than defaulting for exactly this reason; answering "no documents
    match" from a database whose epoch nobody can identify would be that refusal laundered into an
    empty result. The client gets the exception class, not the SQL or the settings key.
    """
    from papra_suite.store import INDEX_GENERATION_KEY

    own_corpus(store, 2)
    store.set_setting(INDEX_GENERATION_KEY, "not-a-number")
    index = ScriptedIndex(hits=[kz_row(1), kz_row(2)])
    with pytest.raises(ToolError, match="index_unavailable") as caught:
        await search_documents(ctx(scope_all=True), store, FakePapra(), index.reader(), "rechnung")
    assert "SELECT" not in str(caught.value)
    assert INDEX_GENERATION_KEY not in str(caught.value)


async def test_a_call_that_overruns_its_budget_refuses_instead_of_answering_late(store):
    """The spec's 15 s per-call deadline, on the two tools that can actually exceed it.

    `index_status` makes two Kreuzakt reads; a search pages five times and then looks documents up,
    on clients that wait 30 s each. The SDK has no timeout of its own, so a call that overruns is a
    call that answers nothing — refused here at its own budget, with the reason, rather than
    delivered after the client has gone.
    """
    papra = own_corpus(store, 8)
    papra.delay_s = 0.05
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 9)])
    with pytest.raises(ToolError, match="index_unavailable") as caught:
        await search_documents(ctx(scope_all=True), store, papra, index.reader(), "rechnung",
                               page_size=8, deadline_s=0.01)
    assert "budget" in str(caught.value)


async def test_the_bounds_are_the_ones_the_environment_configures(store):
    """`PAPRA_INDEX_QUERY_MAX_CHARS` and `PAPRA_INDEX_SEARCH_SCAN_ROWS` mean the same thing here.

    The defaults are compared against `load_config`'s own rather than written down twice: a default
    that moves in `config.py` and not here would leave the listener honouring a bound nobody
    configured, and the unit tests would still be green.
    """
    from papra_suite.config import load_config
    from papra_suite.mcp_tools import SearchLimits
    from tests.test_config import BASE_ENV

    config = load_config({**BASE_ENV})
    assert DEFAULT_LIMITS.query_max_chars == config.index_query_max_chars
    assert DEFAULT_LIMITS.scan_rows == config.index_search_scan_rows
    configured = SearchLimits.from_config(load_config({**BASE_ENV,
                                                       "PAPRA_INDEX_QUERY_MAX_CHARS": "40",
                                                       "PAPRA_INDEX_SEARCH_SCAN_ROWS": "120"}))
    assert (configured.query_max_chars, configured.scan_rows) == (40, 120)

    papra = own_corpus(store, 2)
    index = ScriptedIndex(hits=[kz_row(1), kz_row(2)])
    with pytest.raises(ToolError, match="invalid-request"):
        await search_documents(ctx(ORG_A), store, papra, index.reader(), "x" * 41,
                               limits=configured)
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "x" * 40,
                                  limits=configured)
    assert page.partial is False and page.scanned == 2


# --- the containment these tools stand on ------------------------------------------------------

#: Every upstream read the two tools perform, by role. A tool that grows a read without growing the
#: view underneath it works in the unit tests and raises `AttributeError` in production, so this
#: list is checked against both the shipped views and `REQUIRED_READS` — the set
#: `build_mcp_app`/`StoreTokenVerifier` refuse to build without.
TOOL_READS = {
    "store": ("get_generation", "ledger_resolve"),
    "papra": ("get_document",),
    "kreuzakt": ("search", "list_documents"),
}

VIEWS = {
    "store": LedgerReader,
    "papra": PapraReader,
    "kreuzakt": KreuzaktReader,
}


def test_every_read_these_tools_perform_is_a_read_the_view_offers():
    """The tools reach the upstreams through the three views, and through nothing else.

    Two assertions, because they fail differently. A read missing from the *view* is a listener that
    crashes on the first search. A read the tools perform but that is absent from `REQUIRED_READS`
    is subtler: `require_read_only` would let a double through that cannot answer it, so a test
    double could hide a capability the real view never had — which is how a scope filter ends up
    written twice, once against a fake and once against the database.
    """
    for role, names in TOOL_READS.items():
        view = VIEWS[role]
        for name in names:
            assert hasattr(view, name), f"{view.__name__} cannot {name}"
        unwarranted = set(names) - REQUIRED_READS[role]
        assert not unwarranted, f"{role}: {sorted(unwarranted)} no containment check insists on"


def test_growing_a_read_means_growing_the_view_and_the_requirement_together():
    """`REQUIRED_READS` is a floor the tools sit on, not a description of one of them."""
    assert REQUIRED_READS["store"] >= set(TOOL_READS["store"])
    assert REQUIRED_READS["papra"] == {"get_document"}
    assert set(TOOL_READS["kreuzakt"]) <= REQUIRED_READS["kreuzakt"]


def test_the_views_the_tools_stand_on_still_cannot_write():
    """Task 12's containment, re-asserted now that these two tools actually use all three views."""
    for role, view in VIEWS.items():
        for name in sorted(FORBIDDEN_WRITE_NAMES):
            assert not hasattr(view, name), f"the {role} view can {name}"


# --- why the bound cannot live at mint time ----------------------------------------------------


def test_an_all_buckets_token_carries_more_buckets_than_one_call_may_read(store):
    """The premise of the read-time bound, asserted where it is decided.

    `mcp_tokens.MAX_ORG_IDS` is the ceiling on a *scoped* token; `--all-buckets` is exempt, because
    the operator's own credential has to be able to cover an archive with more buckets than eight
    and naming them one by one is not a grant, it is a maintenance task. That exemption is what
    makes a mint-time-only bound vacuous for the one token that can use it — so the ceiling these
    tests assert on lives on the read path, and this test is the reason it may not move back.
    """
    token = mint_token(store, label="operator", org_ids=[], scope_all=True)
    context = resolve_token(store, token)
    assert context.scope_all is True and context.org_ids == frozenset()
    # and the scoped ceiling is real, so the bound really is a per-call read bound, not a mint one:
    with pytest.raises(Exception, match="at most"):
        mint_token(store, label="too-wide", org_ids=[f"org-{n:02d}"
                                                     for n in range(1, MAX_ORG_IDS + 2)])


# --- what a page is allowed to cost, and what it is owed ---------------------------------------


async def test_deleted_documents_do_not_eat_the_callers_page(store):
    """Five rows in the Papra trash followed by five live ones: page 1 is still five documents.

    The liveness check is what the ledger's `need` bound stops at, and a candidate that fails it is
    not an item. Stopping at exactly five candidates would answer this page with nothing and
    `partial=False` — the first five rows were scanned, checked and refused — while five live
    documents sat beside them in the same page. So candidates are drained in batches until the page
    is full or the page's candidates run out: five live results still cost five reads, and a page of
    deleted documents costs what it has to and returns the results that exist.
    """
    for n in range(1, 11):
        bind(store, ORG_A, f"doc-{n}", n)
    papra = FakePapra({f"doc-{n}": papra_doc(f"doc-{n}", isDeleted=(n <= 5))
                       for n in range(1, 11)})
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 11)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung",
                                  page_size=5)
    assert [item.document_id for item in page.items] == [f"doc-{n}" for n in range(6, 11)]
    assert page.stats.dropped_deleted == 5
    assert page.stats.kept == 5
    assert page.partial is False                 # the archive answered; nothing was left uncertain
    assert papra.get_calls == 10                 # the six it had to look at, not the ten it scanned


async def test_a_later_page_fills_from_the_live_ones_in_order(store):
    """Page 2 is Kreuzakt's ranking of the live documents, not a window over the raw rows."""
    for n in range(1, 11):
        bind(store, ORG_A, f"doc-{n}", n)
    papra = FakePapra({f"doc-{n}": papra_doc(f"doc-{n}", isDeleted=(n <= 5))
                       for n in range(1, 11)})
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 11)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung",
                                  page=2, page_size=3)
    assert [item.document_id for item in page.items] == ["doc-9", "doc-10"]
    assert page.partial is False                 # exhausted: five live documents exist, no more


async def test_a_ragged_budget_is_spent_in_pages_that_divide_it(store):
    """120 rows of allowance buys 60 + 60, so the document on row 105 is reached and counted.

    The page size is chosen per scan, not fixed at 100, because the rows are addressed as
    `(page - 1) * limit`: a second request shrunk to `page=2&limit=20` to fit the leftover budget
    would be rows 21-40 — twenty rows scanned twice and eighty never scanned. A divisor spends the
    allowance and tiles it exactly, which is what makes `scanned` a number that can be added up.
    """
    from papra_suite.mcp_tools import SearchLimits

    bind(store, ORG_A, "doc-far", 105)
    papra = FakePapra({"doc-far": papra_doc("doc-far")})
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 251)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung",
                                  page_size=5, limits=SearchLimits(scan_rows=120))
    assert index.search_calls == [(1, 60), (2, 60)]
    assert [item.document_id for item in page.items] == ["doc-far"]
    assert page.scanned == 120                      # the whole allowance, tiled with no overlap
    assert page.partial is True and "budget" in (page.partial_reason or "")


def test_the_scan_never_mixes_page_sizes_in_one_sweep():
    """One sweep, one `limit` — and never a dribble of one-row requests to fit a prime budget.

    `page_rows_for` has two jobs: divide the allowance where a divisor exists, and refuse to be
    clever where none does. 101 has no divisor below 100, so it takes a full page and reports the
    row it could not spend; a function optimising for exactness there would issue a hundred and one
    one-row reads against a server the caller is trying not to burden.
    """
    from papra_suite.mcp_tools import page_rows_for

    assert page_rows_for(500) == KREUZAKT_PAGE_ROWS      # the default: unchanged, one full page
    assert page_rows_for(120) == 60
    assert page_rows_for(250) == 50
    assert page_rows_for(37) == 37                       # a budget under a page is one page
    assert page_rows_for(997) == KREUZAKT_PAGE_ROWS      # 97 rows stranded: see the test below
    for scan_rows in (37, 99, 100, 101, 120, 250, 500, 997, 1024):
        rows = page_rows_for(scan_rows)
        assert rows <= KREUZAKT_PAGE_ROWS
        assert rows >= min(scan_rows, 25)
        assert scan_rows % rows == 0 or rows == KREUZAKT_PAGE_ROWS


async def test_an_allowance_with_no_usable_divisor_names_the_rows_it_cannot_spend(store):
    """The fallback is not about primes: 997 leaves 97 rows stranded after nine full pages.

    What matters is that the report says *which* rows it did not look at, rather than describing the
    budget as spent. `page_rows_for` stops before exceeding the ceiling; the remainder is a fact the
    caller is owed, because `scanned: 900` beside `budget: 997` otherwise reads as arithmetic that
    has not finished yet.
    """
    from papra_suite.mcp_tools import SearchLimits

    bind(store, ORG_A, "doc-far", 105)
    papra = FakePapra({"doc-far": papra_doc("doc-far")})
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 1201)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung",
                                  page_size=5, limits=SearchLimits(scan_rows=997))
    assert len(index.search_calls) == 9 and {n for n, _ in index.search_calls} == set(range(1, 10))
    assert {limit for _, limit in index.search_calls} == {KREUZAKT_PAGE_ROWS}
    assert [i.document_id for i in page.items] == ["doc-far"]
    assert page.scanned == 900
    assert page.partial is True and "less than one page's worth" in (page.partial_reason or "")
    assert "97 rows of allowance remain" in (page.partial_reason or "")


async def test_the_page_size_is_asked_for_the_same_way_every_time(store):
    """The other half of the same rule: a sweep that mixes limits would not be a sweep of rows.

    The document this token may see sits past the first page. Any answer that reached it with two
    different `limit` values would have re-read some rows and never read others, and reported a
    `scanned` that no longer means the rows examined.
    """
    from papra_suite.mcp_tools import SearchLimits

    bind(store, ORG_A, "doc-far", 105)
    papra = FakePapra({"doc-far": papra_doc("doc-far")})
    index = ScriptedIndex(hits=[kz_row(n) for n in range(1, 251)])
    page = await search_documents(ctx(ORG_A), store, papra, index.reader(), "rechnung",
                                  page_size=5, limits=SearchLimits(scan_rows=250))
    assert [item.document_id for item in page.items] == ["doc-far"]
    limits = {limit for (_page, limit) in index.search_calls}
    assert limits == {50} and len(index.search_calls) == 5
    assert [page_num for (page_num, _limit) in index.search_calls] == [1, 2, 3, 4, 5]
    assert page.scanned == 250                       # rows 1-50, 51-100, …: nothing twice
    assert page.partial is False                     # the corpus ended; nothing is owed


# --- the shape an agent is handed --------------------------------------------------------------


async def test_the_result_shape_is_a_closed_list_of_fields(store):
    """A whitelist, asserted as a set, because a new field is a new leak surface.

    The substring test above proves what the *fixture* did not carry; this one is what no response
    can carry: `content`, `snippet`, `score`, `thumbnail_url` and every Kreuzakt-side name are not
    fields of these models, so they cannot be serialised by a code path that forgot to strip them.
    `SearchItem` is what `KzHit` refuses to be — a Papra id, a bucket, and four display strings.
    """
    from papra_suite.mcp_tools import ResolveStats, SearchItem, SearchPage

    assert set(SearchItem.model_fields) == {"document_id", "org_id", "title", "document_date",
                                            "added_at", "original_filename"}
    assert set(SearchPage.model_fields) == {"items", "page", "page_size", "partial", "scanned",
                                            "stats", "buckets_reached", "buckets_skipped",
                                            "partial_reason"}
    assert set(ResolveStats.model_fields) == {"kept", "dropped_unauthorized", "dropped_unmatched",
                                              "dropped_deleted", "dropped_lookup_error",
                                              "dropped_unreached", "expanded"}


async def test_an_index_outage_reaches_the_client_as_one_code_and_no_body(store):
    """The error the agent sees, serialised the way the SDK serialises it.

    Not a `pytest.raises`: an exception the SDK renders is a different surface from an exception the
    test catches, and this one has to be both stable (`index_unavailable`, retryable) and empty of
    the upstream body — a Kreuzakt error message interpolates its own response, and that body is the
    one text on this path that neither this program nor the token holder wrote. The scripted index
    is therefore built to put a *specific* body on the wire and to record that it was the one asked,
    because "the secret is absent" is worth nothing if the dependency holding it was never read.
    """
    own_corpus(store, 2)
    secret = {"detail": "SECRET UPSTREAM BODY", "documents": 9000, "path": "/api/documents/7"}
    index = ScriptedIndex(hits=[kz_row(1)], error=KreuzaktError("boom", status=500),
                          error_status=500, error_body=secret)
    token = mint_token(store, label="wire", org_ids=[ORG_A])
    _response, message = wire_call(store, token, "search_documents", {"query": "rechnung"},
                                   kz=index.reader())
    assert index.error_hits == 1 and index.search_calls == [(1, KREUZAKT_PAGE_ROWS)]
    result = message["result"]
    assert result["isError"] is True
    assert "index_unavailable" in tool_error_text(message)   # the SDK's prefix is all before it
    serialised = json.dumps(message, ensure_ascii=False).lower()
    # Every needle is checked against the whole envelope. One could not be: the SDK's own result
    # field is called `content`, so scanning for that word would report a leak in every successful
    # response too. The useful claim there is that no *item* carries a text field, which is what the
    # closed-field-set test pins by name.
    for needle in ("secret upstream body", "9000", "/api/documents/", "/documents/", "/thumbnail",
                   "/original", "kreuzakt", "kreuzakterror", "httpx", "sqlite", "operationalerror",
                   "snippet", "canary"):
        assert needle not in serialised, needle
    assert not result.get("structuredContent")      # no half-an-answer payload beside the refusal
