"""`search_documents` and `list_recent_documents`: the two tools that decide what an agent may see.

These are the authorization boundary of the whole MCP surface. `index_status` reports counts; these
two return documents, and Kreuzakt — the thing that actually answers the query — is a corpus-wide
index of **every** bucket with no notion of a token in it. So the index's answer is not the agent's
answer: the ledger is what turns "these 400 rows matched" into "these 3 of yours matched", in one
query per page rather than one per hit (the 2026-11-16 spec's §4.5 rule, and the difference between
a search that costs one read and one that costs five hundred).

Four decisions, each because the obvious version fails quietly:

**The ledger is the filter, and it is the store's own query.** `store.ledger_resolve(ids,
generation=…)` is the one place that decides what a reference means: it hides soft-deleted rows, it
hides rows bound under a generation that no longer means what it said, and it returns nothing at all
for an id it does not know. A Kreuzakt hit with no ledger row is therefore invisible rather than an
error — "the index has a document we never uploaded" is a drift question for `papra-suite check`,
not a reason to show an agent something the ledger cannot account for.

**Fail closed wherever "unknown" could be read as "show it".** A Papra lookup that errors drops the
item and counts it; a deleted document drops and counts. Neither is reported as a failure of the
search, because the search succeeded — but both make the page `partial`, because an item that could
not be checked is an item that might have belonged in the answer.

**Completeness is claimed only when it is proven.** `partial` is true when the scan budget ran out,
when upstream stopped cooperating with its own `total`, when a Papra read failed, or when the
fan-out bound meant a bucket in scope was never consulted. The last one is why the bound lives here
rather than at `mcp-token --create`: an `--all-buckets` token is exempt from `MAX_ORG_IDS` by
design, so mint time cannot bound it, and a mint-time-only bound bounds only the tokens that could
never exceed it.

**Nothing upstream leaves here.** `SearchItem` has Papra's id and Papra's bucket, and no Kreuzakt
integer id, no `/api/documents/…`, no thumbnail path, no `content`, no `score`. That is not
formatting: `KzHit` is built without those fields, so the values never reach this module at all.

Query handling stays Kreuzakt's: the query goes to the tokeniser upstream, and the tool description
says what that means (prefix matches on complete tokens, diacritics kept, an empty result is not
proof of absence).
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
from dataclasses import dataclass, field

from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel

from papra_suite.kreuzakt_client import KreuzaktError, KzHit, _hit_from_row

# `_hit_from_row` is imported rather than re-written: it is the single check that a Kreuzakt row
# carries the fields the bridge depends on, and the house precedent for borrowing a private
# single-implementation is `from papra_suite.store import _now` in `mcp_tokens.py`. A row parsed
# here by hand would be a second rule about `added_at`, and the drift would look like a document
# that is older than everything.
from papra_suite.store import sanitize_event_text

logger = logging.getLogger("papra_suite.mcp_tools")

#: Rows fetched per Kreuzakt request. Measured (Task 3, Task 5): `/api/search` and `/api/documents`
#: honour `limit` up to 100 and ignore `offset` entirely, so 100 is one round trip's worth and the
#: only way past the first page is `page`.
KREUZAKT_PAGE_ROWS = 100

#: The spec's fan-out ceiling, enforced **here** rather than at mint time: at most this many
#: buckets' live state is consulted for one tool call. `mcp_tokens.MAX_ORG_IDS` refuses a ninth
#: bucket on a scoped token, but `--all-buckets` is exempt from it by design — and `--all-buckets`
#: is how both operator tokens are minted. Without a read-time bound, the ceiling constrains only
#: the tokens that could never exceed it.
MAX_BUCKETS_PER_CALL = 8

#: …and at most this many Papra lookups in flight at once, per the spec's `Content size` section.
#: The bound is what keeps a 50-item page across 8 buckets from being 50 simultaneous requests to a
#: single-user Papra instance.
MAX_CONCURRENT_LOOKUPS = 4

#: Display bounds for the two free-text fields. Truncating a title is a display decision; truncating
#: an id would be a document that cannot be fetched, which is why ids are never touched here.
TEXT_FIELD_MAX_CHARS = 200


#: How long one of these two calls may take before it is refused. The spec's `Content size` section
#: puts the per-call deadline at 15 s, and `index_status` already honours that number; a search that
#: pages five times and checks fifty documents can exceed it on a sick index, and an answer that
#: arrives after the client gave up is not an answer.
TOOL_DEADLINE_S = 15.0

#: A client-visible message, bounded. Same number as `mcp_server.CLIENT_MESSAGE_LIMIT`, re-declared
#: here because the import runs the other way: `mcp_server` registers these tools, so nothing in
#: this module may import from it.
CLIENT_MESSAGE_LIMIT = 240

#: The two shapes `/api/documents` demonstrably filters on, copied from `kreuzakt_client`'s own
#: validator (which refuses the others). Kept here so the refusal reaches the caller as
#: `invalid-request` — their mistake — rather than as an upstream error.
_SINCE_FORM = re.compile(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$")


class IndexUnavailable(ToolError):
    """`index_unavailable: …` — the index or the ledger did not answer, so there is no result.

    The same class `index_status` raises: it lives here because both files need it and the import
    only runs one way, and `mcp_server` imports it back (so `from papra_suite.mcp_server import
    IndexUnavailable` still resolves). One condition, one code: a client that branches on
    `index_unavailable` must not have to learn a second spelling for the search path.

    What it refuses is the empty list. "The index is down" and "nothing in your archive matches"
    are different facts, and the second is a claim about the archive that this call cannot make;
    an empty list under a dead index is the confident wrong answer this project keeps designing
    against.
    """

    def __init__(self, reason: str):
        bounded = sanitize_event_text(reason, limit=CLIENT_MESSAGE_LIMIT) or "unknown"
        super().__init__(f"index_unavailable: {bounded}")


class InvalidRequest(ToolError):
    """A request this tool will not attempt, prefixed with the stable code `invalid-request`.

    Refusing rather than clamping is the whole point. A clamp turns "give me 51 per page" into an
    answer that looks like it delivered 51, and a truncated query answers a question the caller did
    not ask — both are the confident wrong answer, just smaller. The message names the bound that
    was crossed so an agent can correct the request instead of guessing.
    """

    def __init__(self, reason: str):
        super().__init__(f"invalid-request: {reason}")




# --- what an agent receives -------------------------------------------------------------------


class SearchItem(BaseModel):
    """One document, in the only terms an agent can act on: a Papra id and its bucket.

    There is no Kreuzakt integer id on this model, and no field where one could be put. That is the
    containment for the "no upstream identifier" rule: `KzHit` never carries a path or a link, and
    this model never carries an int, so a leak would need somebody to add a field to one of them and
    a test that says `kreuzakt` must not appear in the serialised JSON is the backstop.

    `title` and `document_date` come from Papra's live record when it has one, and from the index
    otherwise: the agent's next move is `get_document(document_id)`, and a title the operator has
    since changed in Papra would be a small confident wrong answer. `original_filename` and
    `added_at` are the index's, because they describe what was indexed and when. `document_id` and
    `org_id` are never truncated, whatever the length: a cut id names a document nobody can fetch.
    """

    document_id: str
    org_id: str
    title: str
    document_date: str | None
    added_at: str
    original_filename: str


class ResolveStats(BaseModel):
    """What the resolver did with the rows Kreuzakt returned, including what it threw away.

    Every counter is a different reason, because they have different owners:
    `dropped_unauthorized` is a scope decision (the row exists, this token was not given that
    bucket), `dropped_unmatched` is a ledger gap (no row at the current generation — never uploaded,
    already forgotten, or invalidated by a rebuild), `dropped_deleted` is the archive's own answer,
    `dropped_lookup_error` is Papra failing to answer, and `dropped_unreached` is the fan-out bound
    leaving a bucket unconsulted. An empty result with no numbers is a dead end for its reader.
    """

    kept: int = 0
    dropped_unauthorized: int = 0
    dropped_unmatched: int = 0
    dropped_deleted: int = 0
    dropped_lookup_error: int = 0
    dropped_unreached: int = 0
    #: Items beyond the first, for hits that resolved to more than one authorized reference. Two
    #: buckets holding identical bytes share one Kreuzakt id, and the second result is the
    #: interesting fact: it says one row of this index is two documents of this archive.
    expanded: int = 0


class SearchPage(BaseModel):
    """A page of results, and an honest statement of how much of the archive it proves anything
    about.

    `partial` is the load-bearing field: true whenever the answer could be missing something, and
    `partial_reason` names the cause in one line so a client does not have to infer it from
    `scanned`. It is false in exactly two situations: the caller's page was filled from rows this
    tool scanned, or the scan proved upstream had nothing more to give. A full page that also
    exhausted the corpus is `partial=False` — being short is not the same as being incomplete.

    `scanned` is Kreuzakt rows examined, not items returned: it is what the scan budget was
    spent on, and it is how an operator can tell "no matches" from "we stopped looking".

    `buckets_reached` and `buckets_skipped` say which buckets were consulted. Skipped ones are named
    because the only way to skip a bucket is to hold an `--all-buckets` token, which is already
    allowed to see every bucket — and because "your answer covers these seven of twelve" must be
    checkable, not merely flagged.
    """

    items: list[SearchItem]
    page: int
    page_size: int
    partial: bool
    scanned: int
    stats: ResolveStats
    buckets_reached: list[str] = []
    buckets_skipped: list[str] = []
    partial_reason: str | None = None


# --- the budgets -------------------------------------------------------------------------------


@dataclass(frozen=True)
class SearchLimits:
    """One tool call's budgets, in one place, so the refusal and the budget cannot drift apart.

    The two numbers that come from configuration travel as a `from_config` of the real thing, so
    `PAPRA_INDEX_QUERY_MAX_CHARS` and `PAPRA_INDEX_SEARCH_SCAN_ROWS` mean what they say on the MCP
    path as well as on the CLI's. The defaults are asserted against `load_config`'s own defaults in
    `tests/test_mcp_search.py`: a default that moves in `config.py` without moving here is a
    listener that quietly stops honouring the environment.
    """

    query_max_chars: int = 512
    scan_rows: int = 500
    page_size_min: int = 1
    page_size_max: int = 50
    page_size_default: int = 20
    max_buckets: int = MAX_BUCKETS_PER_CALL
    max_concurrent: int = MAX_CONCURRENT_LOOKUPS

    @classmethod
    def from_config(cls, config) -> SearchLimits:
        """The two configured bounds, from whatever config object the caller was handed.

        `getattr` with the dataclass default, the way `resolve_bind` reads `mcp_host`: a config
        object that predates these fields (a test's `SimpleNamespace`, a caller that assembles one
        by hand) then gets the documented default rather than an `AttributeError` from inside a
        listener start-up. `tests/test_mcp_search.py` pins that `load_config` does set both, so the
        fallback is for assembled configs and cannot silently replace the environment's values.
        """
        return cls(
            query_max_chars=getattr(config, "index_query_max_chars", cls.query_max_chars),
            scan_rows=getattr(config, "index_search_scan_rows", cls.scan_rows))


DEFAULT_LIMITS = SearchLimits()


def check_request(query: str, page: int, page_size: int, *, limits: SearchLimits,
                  what: str) -> None:
    """The bounds, checked before anything is fetched. Raises `InvalidRequest`; never clamps."""
    if not isinstance(query, str) or not query.strip():
        raise InvalidRequest(f"{what} needs a non-empty query")
    if len(query.strip()) > limits.query_max_chars:
        raise InvalidRequest(f"the query is {len(query.strip())} characters, and this tool accepts "
                             f"at most {limits.query_max_chars}")
    _require_page(page, page_size, limits=limits)


def check_recent_request(limit: int, since: object, *, limits: SearchLimits) -> None:
    """The bounds of `list_recent_documents`: a bounded `limit`, and a `since` that filters."""
    _require_page(1, limit, limits=limits)
    if since is not None and (not isinstance(since, str)
                              or not _SINCE_FORM.match(since.strip())):
        raise InvalidRequest(
            f"since must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS', got {since!r}. A value in ISO "
            "`…Z` form is refused rather than rewritten: measured against the pinned container it "
            "answers 200 with no rows at all, and the column it would be compared against has no "
            "documented timezone, so converting it would pick a meaning for the instant and hide "
            "that choice from whoever asked for yesterday")


def _require_page(page: int, page_size: int, *, limits: SearchLimits) -> None:
    """The paging bounds both tools share, stated once."""
    if not isinstance(page, int) or isinstance(page, bool) or page < 1:
        raise InvalidRequest(f"page must be an integer >= 1, got {page!r}")
    if (not isinstance(page_size, int) or isinstance(page_size, bool)
            or not limits.page_size_min <= page_size <= limits.page_size_max):
        raise InvalidRequest(f"page_size must be between {limits.page_size_min} and "
                             f"{limits.page_size_max}, got {page_size!r}")


# --- one tool call's shared state --------------------------------------------------------------


@dataclass
class CallScope:
    """What one tool call remembers across the pages it fetches.

    Three things have to be per call rather than per page, and each is a different bug otherwise:

    * **The Papra liveness cache.** The authorization claim is "this document is live as of this
      answer", and a cache scoped to one request keeps that true without fifty round trips for the
      same document. It is not a session cache: the next call asks again.
    * **The buckets consulted.** The fan-out ceiling is per tool call — a call that paged across
      five Kreuzakt pages and touched eight buckets each time would be forty buckets with a ceiling
      of eight.
    * **The emitted set.** Deduplication by `(org_id, document_id)` across pages, so a document that
      slides into a second window cannot take two slots of the caller's page.
    """

    limits: SearchLimits = DEFAULT_LIMITS
    live: dict = field(default_factory=dict)
    reached: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    emitted: set = field(default_factory=set)
    stats: ResolveStats = field(default_factory=ResolveStats)
    _semaphore: asyncio.Semaphore | None = None

    def semaphore(self) -> asyncio.Semaphore:
        """The concurrency bound, created once per call and shared by every page it fetches."""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.limits.max_concurrent)
        return self._semaphore

    def consults(self, org_id: str) -> bool:
        """Whether this bucket may be looked up at all — the read-time fan-out bound.

        Buckets are admitted in the order the index surfaced them, which for `search_documents` is
        relevance order and for `list_recent_documents` is newest-first: when a bound has to cut
        something off, it cuts the least relevant part of the answer, not an alphabetical lottery.
        A bucket that is refused is named, once, and the page that results from it is `partial`.
        """
        if org_id in self.reached:
            return True
        if len(self.reached) >= self.limits.max_buckets:
            if org_id not in self.skipped:
                self.skipped.append(org_id)
            return False
        self.reached.append(org_id)
        return True


def _bounded_text(value: object, limit: int = TEXT_FIELD_MAX_CHARS) -> str:
    """Display text, length-bounded, with the cut visible. Ids never go through here."""
    text = "" if value is None else str(value)
    if len(text) <= limit:
        return text
    return text[:limit - 1] + "…"


def _item(hit: KzHit, ref, live: dict | None) -> SearchItem:
    """The one place a Kreuzakt hit becomes something an agent may hold.

    `live` is Papra's current record — already fetched, already checked for deletion — and it is
       consulted for the two fields the operator can edit in Papra. Everything else describes the
       index.
    """
    document_date = (live or {}).get("documentDate") or hit.document_date
    if isinstance(document_date, str) and len(document_date) > 10 and document_date[10] in "T ":
        document_date = document_date[:10]      # Papra stores a bare date as an instant
    return SearchItem(
        document_id=ref.papra_document_id,
        org_id=ref.papra_org_id,
        title=_bounded_text((live or {}).get("name") or hit.title),
        document_date=_bounded_text(document_date, 40) or None,
        added_at=hit.added_at,
        original_filename=_bounded_text(hit.original_filename),
    )



#: Sentinel for "Papra did not answer", distinct from "Papra answered and there is nothing there".
#: Both mean *do not show it*; keeping them apart is what lets `dropped_lookup_error` count the
#: failures and only the failures.
UNKNOWN = object()


async def _live_document(papra, call: CallScope, ref) -> dict | None | object:
    """Papra's current record for one reference, cached for this call, or `UNKNOWN` if unanswered.

    The cache lives on the call, not on the module: the claim a result makes is "live as of this
    answer", and a cache that outlives a call would answer a second search with the first search's
    knowledge of a document somebody may have deleted in between. Bounded by the number of documents
    one call can return, because a call is bounded.

    Any exception at all is `UNKNOWN`. Not distinguishing a 404 from a timeout is deliberate — both
    mean "cannot be shown", and the difference is Papra's internals rather than something the caller
    may act on. It is counted, so an outage is a number rather than a quietly short page.
    """
    key = (ref.papra_org_id, ref.papra_document_id)
    if key in call.live:
        return call.live[key]
    async with call.semaphore():
        try:
            record = await papra.get_document(ref.papra_org_id, ref.papra_document_id)
        # Any failure at all is UNKNOWN, i.e. "do not show this one". Broad on purpose: the caller's
        # question is whether the document may be shown, and Papra refusing to say is one answer
        # whether it arrived as a 404, a timeout, or a body this client cannot read.
        except Exception:  # noqa: BLE001
            record = UNKNOWN
    call.live[key] = record
    return record


async def resolve_hits(ctx, store, papra, hits, *, generation: int, call: CallScope,
                       need: int | None = None):
    """The authorization boundary, in one function, at one ledger query per page of hits.

    Why it is shared by both tools rather than inlined in each: the scope rule is the security
    property of this milestone, and two copies of it is two things to keep correct. `index_status`
    gets its scoping from `ledger_stats(org_ids=…)`, this from `ledger_resolve(ids, generation=…)` —
    both of them the store's own queries, so these tools and `papra-suite ledger` cannot disagree
    what a reference means.

    The order of operations is the whole design:

    1. **One query for the page.** `ledger_resolve` takes every Kreuzakt id the page carried.
       Per-hit resolution would be one read per row against a single-writer WAL database shared
       with the delivery workers, i.e. a search that stalls the pipeline (the spec's §4.5 rule).
    2. **The token's bucket list is applied to the references**, not to the query: two buckets can
       share one Kreuzakt id, so the filter runs per reference and a hit can survive it more than
       once.
    3. **Papra is asked only about what survived.** A scanned hit that was never a candidate costs
       no round trip; a candidate costs at most one, cached across the call, capped at
       `call.limits.max_concurrent` in flight.
    4. **Kreuzakt's order is kept.** Filtering happens before the caller's slice is taken, so a
       document's rank is the index's, never a side effect of which bucket it lives in.

    `generation` is read once by the caller and passed down, so every page of one call is judged
    against the same index epoch: a rebuild that lands mid-scan must not make half a page resolve
    under a generation the other half did not.

    Returns the items in hit order plus the call's running stats (`call.stats`), so a caller that
    resolves several pages reports one set of numbers rather than five.
    """
    if not hits:
        return [], call.stats
    ids = sorted({hit.id for hit in hits})
    # Off the event loop: this is a correlated read on a database `serve` writes to, and a request
    # path that blocks the loop blocks the OCR workers.
    refs = await asyncio.to_thread(store.ledger_resolve, ids, generation=generation)
    resolved = {ref.kreuzakt_id for ref in refs}
    authorized: dict[int, list] = {}
    for ref in refs:
        if ctx.scope_all or ref.papra_org_id in ctx.org_ids:
            authorized.setdefault(ref.kreuzakt_id, []).append(ref)

    # Candidates first, liveness checks second: the count of Papra reads is the count of documents
    # this call still needs, not the count of rows the index looked at.
    candidates = []
    for hit in hits:
        matches = authorized.get(hit.id)
        if not matches:
            # Every scanned row is still counted, including the ones past the page: the numbers
            # describe the window that was looked at, which is what an empty result needs.
            if hit.id in resolved:
                call.stats.dropped_unauthorized += 1
            else:
                call.stats.dropped_unmatched += 1
            continue
        for ref in matches:
            key = (ref.papra_org_id, ref.papra_document_id)
            if key in call.emitted:
                continue
            call.emitted.add(key)
            if not call.consults(ref.papra_org_id):
                call.stats.dropped_unreached += 1
                continue
            candidates.append((hit, ref))

    # Drained in bounded batches: `need` is what the caller's page is still short by, so the common
    # case is one batch of `need` reads. Batches rather than a truncation because a candidate that
    # fails its liveness check is not an item: taking exactly `need` candidates and stopping would
    # answer a page of 5 with 0 items and `partial=False` when the first 5 were deleted in Papra and
    # 25 live ones sat beside them in the same page. Checking every candidate of a page is the
    # ceiling; checking nothing that was never a candidate is the floor, and both are asserted.
    items = []
    seen_for_hit: dict[int, int] = {}
    pending = candidates
    while pending and (need is None or len(items) < need):
        batch = pending if need is None else pending[:need - len(items)]
        pending = pending[len(batch):]
        records = await asyncio.gather(
            *(_live_document(papra, call, ref) for _, ref in batch))
        for (hit, ref), record in zip(batch, records, strict=True):
            if record is UNKNOWN:
                call.stats.dropped_lookup_error += 1
                continue
            if record.get("isDeleted"):
                call.stats.dropped_deleted += 1
                continue
            seen_for_hit[hit.id] = seen_for_hit.get(hit.id, 0) + 1
            if seen_for_hit[hit.id] > 1:
                call.stats.expanded += 1
            call.stats.kept += 1
            items.append(_item(hit, ref, record))
    return items, call.stats


# --- the paging loop ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Sweep:
    """What one paging loop proved, before anyone reads it as an answer.

    The three endings are three different facts, and collapsing them is how a short page becomes a
    lie:

    * `complete` — upstream demonstrably had nothing more (`scanned >= total`, or a page shorter
      than the one asked for, where no count is offered). Short, and honest about it.
    * `budget_spent` — `config.index_search_scan_rows` ran out. The page may be full and still be
      `partial`, because a full page from a scan that stopped is not the whole answer.
    * `stalled` — upstream answered a page shorter than it was asked for while its own `total` still
      claimed rows. That is an index not honouring its own count, which is not exhaustion, and the
      only honest report is "I may have missed something".
    """

    items: list
    scanned: int
    complete: bool
    budget_spent: bool
    stalled: bool

    def reasons(self, call: CallScope, *, scan_rows: int) -> list[str]:
        """Why this answer could be missing something. Empty means it proves itself complete."""
        reasons = []
        if self.budget_spent:
            spare = scan_rows - self.scanned
            if spare > 0:
                reasons.append(f"the scan budget of {scan_rows} rows allowed no further whole page "
                               f"(scanned {self.scanned}, {spare} row(s) short of one)")
            else:
                reasons.append(f"the scan budget of {scan_rows} rows was spent after "
                               f"{self.scanned} rows")
        if self.stalled:
            reasons.append("the index returned a short page while its own total claimed more rows")
        if call.stats.dropped_lookup_error:
            reasons.append(f"{call.stats.dropped_lookup_error} papra read(s) failed, so those "
                           "documents could not be checked")
        if call.skipped:
            reasons.append(f"{len(call.skipped)} bucket(s) in scope were beyond this call's bound "
                           f"of {call.limits.max_buckets} buckets")
        return reasons


def page_rows_for(scan_rows: int) -> int:
    """One page size for a whole scan: the largest that fits the allowance and divides it exactly.

    Two constraints pull against each other. Kreuzakt addresses rows as `(page - 1) * limit`, so
    every request in a scan has to ask for the same `limit` — shrinking the last one to fit a budget
    that is not a multiple of 100 moves the window instead of trimming it: with a budget of 120 a
    second request of `page=2&limit=20` is rows 21-40: twenty rows scanned twice, eighty never
    scanned. But a fixed 100 also throws away the tail of a ragged allowance, which the advisor was
    right to call out as a budget not actually spent. A page size that divides the allowance
    satisfies both: a budget of 120 scans 60+60 and reaches every row it paid for.

    A prime allowance (101) has no divisor at all, so the floor of 25 keeps one pathological config
    from turning a scan into a hundred-and-one one-row requests: it falls back to a full page, stops
    with a row unusable, and `Sweep.reasons` says so rather than claiming the budget ran out.
    """
    if scan_rows <= KREUZAKT_PAGE_ROWS:
        return scan_rows                    # one page, and the ceiling is the whole allowance
    for rows in range(KREUZAKT_PAGE_ROWS, 0, -1):
        if scan_rows % rows == 0:
            return rows if rows >= 25 else KREUZAKT_PAGE_ROWS
    return KREUZAKT_PAGE_ROWS


async def _collect(fetch, resolve, *, want: int, scan_rows: int) -> Sweep:
    """Pull pages until the caller's page is full, upstream is done, or the budget is spent.

    `complete` means "upstream demonstrably had nothing more", and only that — or a filled page —
    may produce `partial=False`. A short page whose `total` still claims more rows is **not**
    complete: that is an upstream that stopped cooperating with its own count, and the honest answer
    is "I may have missed something".

    `fetch(page, limit)` returns `(rows, total_or_None)`. Paging is by 1-based `page` because
    `/api/search` and `/api/documents` honour it and ignore `offset` (measured, Task 3), and `limit`
    is the same on every request of one scan for the reason given below.
    """
    items: list = []
    scanned = 0
    complete = False
    budget_spent = False
    stalled = False
    upstream_page = 1
    # One page size for the whole scan. Kreuzakt addresses rows as `(page - 1) * limit`, so
    # shrinking the last request to fit a budget that is not a multiple of 100 moves the window
    # instead of trimming it: with a budget of 120, a second request of `page=2&limit=20` is rows
    # 21-40 — twenty rows scanned twice and eighty that were never scanned. The allowance is
    # One page size for the whole scan, chosen to divide the allowance: see `page_rows_for`. The
    # allowance stays a ceiling on rows scanned, and `reasons()` reports the shortfall honestly
    # when a prime allowance leaves a row or two unusable.
    page_rows = page_rows_for(scan_rows)
    pages_allowed = max(1, scan_rows // page_rows)
    while upstream_page <= pages_allowed:
        rows, total = await fetch(upstream_page, page_rows)
        scanned += len(rows)
        items.extend(await resolve(rows, want - len(items)))
        if len(items) >= want:
            break                   # the caller's page is full; nothing further is owed
        if total is None:
            # `/api/documents` sends no usable count through this client, so the only exhaustion
            # signal there is a page shorter than the one asked for.
            complete = len(rows) < page_rows
            if complete:
                break
        elif scanned >= total:
            complete = True
            break
        elif len(rows) < page_rows:
            stalled = True          # a short page whose own `total` still claims more
            break
        upstream_page += 1
    else:
        # Ended on the page allowance rather than on a `break`: the row budget is spent.
        budget_spent = True
    return Sweep(items=items, scanned=scanned, complete=complete, budget_spent=budget_spent,
                 stalled=stalled)


async def _guarded(what: str, body, *, deadline_s: float | None):
    """Await one tool call's work, and turn every "could not find out" into one stable code.

    Three things happen here and each is a leak or a lie prevented:

    **Upstream prose stops here.** `KreuzaktError` interpolates the response body into its message,
    and the SDK hands `str(exc)` straight to the agent. So the client gets the exception's class and
    the HTTP status it can act on, never the body — the same rule `mcp_server`'s outage path
    follows, applied here without quoting any of the text.

    **A ledger that will not answer is a refusal, not an empty page.** `get_generation` raises
    `ValueError` when `index_generation` names nothing, and a `sqlite3.Error` means the file would
    not answer. Both become `index_unavailable`; answering "no documents match" from a database this
    call could not read would be the confident wrong answer with a `partial=False` stamp on it.

    **The deadline is honoured.** The SDK has no timeout of its own and the HTTP clients wait 30 s
    per call, so without this a sick index answers nothing at 30 s instead of refusing at 15 s. The
    worker thread a cancelled `to_thread` left behind finishes in the background and is dropped —
    the same trade `index_status_for` documents.
    """
    try:
        return await asyncio.wait_for(body, timeout=deadline_s)
    except ToolError:
        raise                      # a refusal this module chose is already the right code
    except TimeoutError:
        raise IndexUnavailable(
            f"{what} did not finish within its {deadline_s:g} s budget, so nothing is reported as "
            "an answer") from None
    except KreuzaktError as error:
        status = getattr(error, "status", None)
        logger.warning("mcp: %s could not read the index (%s): %s", what, type(error).__name__,
                       sanitize_event_text(str(error), limit=CLIENT_MESSAGE_LIMIT))
        detail = f" (http {status})" if isinstance(status, int) else " without answering at all"
        raise IndexUnavailable(f"the index would not answer this request{detail}") from error
    except (ValueError, sqlite3.Error) as error:
        # The class name goes to the log, not to the client: `OperationalError` names our storage
        # engine to an agent that can do nothing with it, and a class name is the kind of internal
        # that a leak test should not have to learn to forgive.
        logger.warning("mcp: %s could not read the ledger (%s)", what, type(error).__name__)
        raise IndexUnavailable("the ledger would not answer this request, so no reference could "
                               "be resolved and nothing can be shown") from error


async def search_documents(ctx, store, papra, kz, query: str, *, page: int = 1,
                           page_size: int = DEFAULT_LIMITS.page_size_default,
                           limits: SearchLimits = DEFAULT_LIMITS,
                           deadline_s: float | None = TOOL_DEADLINE_S) -> SearchPage:
    """Search the index for this principal, and answer only with what the ledger lets them see.

    The loop is in `_collect` and the filter is in `resolve_hits`; what happens here is the parts
    that belong to one tool call: the request bounds, one generation for the whole call, the scan
    budget, and the slice.

    **The slice is taken at the end.** `items` holds every item this call resolved, in Kreuzakt's
    order, so `[start:start + page_size]` is Kreuzakt's ranking with unauthorized rows removed —
    never a page that shifted because a foreign bucket was skipped, and never a page that came out
    short because a document of yours is in the Papra trash (the next live one is pulled up into
    that slot, which is what "the archive answered and the answer was no" means for a listing).
    """
    check_request(query, page, page_size, limits=limits, what="search_documents")
    call = CallScope(limits=limits)

    async def sweep() -> Sweep:
        generation = await asyncio.to_thread(store.get_generation)

        async def fetch(upstream_page: int, limit: int):
            return await kz.search(query, page=upstream_page, limit=limit)

        async def resolve(hits, need):
            resolved, _stats = await resolve_hits(ctx, store, papra, hits,
                                                  generation=generation, call=call, need=need)
            return resolved

        return await _collect(fetch, resolve, want=page * page_size, scan_rows=limits.scan_rows)

    swept = await _guarded("search_documents", sweep(), deadline_s=deadline_s)
    start = (page - 1) * page_size
    reasons = swept.reasons(call, scan_rows=limits.scan_rows)
    return _page(swept, call, items=swept.items[start:start + page_size], page=page,
                 page_size=page_size, reasons=reasons)


async def list_recent_documents(ctx, store, papra, kz, *, limit: int = 20, since: str | None = None,
                                limits: SearchLimits = DEFAULT_LIMITS,
                                deadline_s: float | None = TOOL_DEADLINE_S) -> SearchPage:
    """The newest documents this principal may see, in Kreuzakt's own `added_at` order.

    "Newest" is Kreuzakt's ordering of `/api/documents`, and nothing here re-sorts it. A local sort
    would be a second, worse index: this suite has no comparable score across buckets and no
    timestamp of its own that means *indexed*, so any ordering it invented would be a claim about
    the archive that only the archive can make. Under the same rule, the page carries no `since`
    watermark either: which column Kreuzakt filters with `since` was never established (Task 3), so
    a returned watermark would read as "everything older than this is scanned" and be false.

    It answers a `SearchPage` with `page=1` rather than the bare list the plan's sketch showed:
    `limit` is a request, the scan budget applies, and a scan that stops early has to say so — a
    `list` has nowhere to put that. Same fields, same `partial` rule as a search.
    """
    check_recent_request(limit, since, limits=limits)
    call = CallScope(limits=limits)

    async def sweep() -> Sweep:
        generation = await asyncio.to_thread(store.get_generation)

        async def fetch(upstream_page: int, row_limit: int):
            # `since` is passed through untouched: it is Kreuzakt's own format, checked by the
            # client that refuses the shapes it demonstrably does not filter on.
            rows = await kz.list_documents(page=upstream_page, limit=row_limit, since=since)
            # The same required-fields check `/api/search` rows go through: a listing row missing
            # `added_at` is an error naming the row, not `""`, which would sort as the oldest
            # document in the archive.
            return [_hit_from_row(row) for row in rows], None

        async def resolve(hits, need):
            resolved, _stats = await resolve_hits(ctx, store, papra, hits,
                                                  generation=generation, call=call, need=need)
            return resolved

        return await _collect(fetch, resolve, want=limit, scan_rows=limits.scan_rows)

    swept = await _guarded("list_recent_documents", sweep(), deadline_s=deadline_s)
    reasons = swept.reasons(call, scan_rows=limits.scan_rows)
    return _page(swept, call, items=swept.items[:limit], page=1, page_size=limit, reasons=reasons)


def _page(swept: Sweep, call: CallScope, *, items: list[SearchItem], page: int, page_size: int,
          reasons: list[str]) -> SearchPage:
    """The one place `partial` is decided, so both tools answer it from the same rule.

    `partial` is **not** "the page is short": a page that ran out because the archive has nothing
    more is complete. It is "this answer could be missing something", which is true when the scan
    stopped early (`_collect`), when Papra could not be asked about something that might have been
    shown, or when the fan-out bound left a bucket in scope unconsulted. Rows the ledger refused, or
    that Papra reports deleted, are not in that list: those were checked, and the archive's answer
    was that they are not this caller's or not there any more.
    """
    return SearchPage(
        items=items,
        page=page,
        page_size=page_size,
        partial=bool(reasons),
        scanned=swept.scanned,
        stats=call.stats,
        buckets_reached=list(call.reached),
        buckets_skipped=list(call.skipped),
        partial_reason="; ".join(reasons) or None,
    )
