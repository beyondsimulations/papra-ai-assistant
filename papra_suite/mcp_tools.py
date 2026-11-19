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
import secrets
import sqlite3
from dataclasses import dataclass, field
from urllib.parse import quote, urlparse

from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel

from papra_suite.kreuzakt_client import KreuzaktError, KzDocument, KzHit, _hit_from_row

# `_hit_from_row` is imported rather than re-written: it is the single check that a Kreuzakt row
# carries the fields the bridge depends on, and the house precedent for borrowing a private
# single-implementation is `from papra_suite.store import _now` in `mcp_tokens.py`. A row parsed
# here by hand would be a second rule about `added_at`, and the drift would look like a document
# that is older than everything.
from papra_suite.store import sanitize_event_text
from papra_suite.untrusted_text import strip_unsafe_text

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
#: `invalid-request` — their mistake — rather than as an upstream error. Matched with `fullmatch`
#: below: with `match`, the trailing `$` still permitted a newline, so `2026-01-01\n` passed this
#: check and a shape nobody chose reached the wire.
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


#: The bands a *scoped* token is told a withheld count in, and the whole information content of
#: those two fields for such a token: "none", "one", "a few", "many".
WITHHELD_NONE = "0"
WITHHELD_ONE = "1"
#: At and above this many withheld rows, a scoped token is told only "10+".
WITHHELD_MANY = 10


def withheld_band(count: int) -> str:
    """An exact count of withheld rows, collapsed to "0", "1", "2-9" or "10+".

    Why a band and not the number is in `ResolveStats`: for a token that is not `--all-buckets`, the
    exact figure states how many of the top `scan_rows` rows matching *the caller's own query*
    belong to buckets they were not given, and it is quotable per query. That is a census of other
    people's archive, and the caller can do nothing with it — the number is the denial, not a
    diagnostic they can act on. A band keeps the useful half ("your query did match elsewhere, and
    roughly how much") and drops the census. The exact value is logged for the operator.
    """
    if count <= 0:
        return WITHHELD_NONE
    if count == 1:
        return WITHHELD_ONE
    if count < WITHHELD_MANY:
        return "2-9"
    return "10+"


class ResolveStats(BaseModel):
    """What the resolver did with the rows Kreuzakt returned, including what it threw away.

    Every counter is a different reason, because they have different owners:
    `dropped_unauthorized` is a scope decision (the row exists, this token was not given that
    bucket), `dropped_unmatched` is a ledger gap (no row at the current generation — never uploaded,
    already forgotten, or invalidated by a rebuild), `dropped_deleted` is the archive's own answer,
    `dropped_lookup_error` is Papra failing to answer, and `dropped_unreached` is the fan-out bound
    leaving a bucket unconsulted. An empty result with no numbers is a dead end for its reader.

    **Two of them are bands for a scoped token, not counts.** `dropped_unauthorized` and
    `dropped_unmatched` are strings — `"0"`, `"1"`, `"2-9"`, `"10+"` — unless the caller holds an
    `--all-buckets` token, in which case they are exact decimal strings. The reason is a disclosure,
    not a formatting taste: for a scoped token `dropped_unauthorized` is *always* zero when it would
    otherwise be informative-free (an all-buckets token is never denied), so the field's only
    possible content is "N of the rows matching your query belong to buckets you were not granted".
    With a caller-controlled query, that is a counting oracle over the archive, and the only tokens
    that can produce a non-zero value are the per-device ones that were scoped precisely to see
    nothing of the rest. `dropped_unmatched` is the same category: it can be drift inside a bucket
    the caller may not be granted, and it tells them nothing they can act on. The exact counts stay
    in the server log; the README's MCP section says plainly that a scoped token can learn *that*
    other buckets have matches, which no field-level change can take away.

    The rest are exact for everybody, because they are the caller's own business: `kept` and
    `expanded` describe the answer, `dropped_deleted` is the archive's answer about documents in
    scope, `dropped_lookup_error` is Papra failing, `dropped_unreached` is this call's own bound.
    """

    kept: int = 0
    dropped_unauthorized: str = WITHHELD_NONE
    dropped_unmatched: str = WITHHELD_NONE
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
                  what: str) -> str:
    """The bounds, checked before anything is fetched. Raises `InvalidRequest`; never clamps.

    **It returns the value that is then forwarded**, and that is the point of its signature. A check
    that measures `query.strip()` while the tool sends `query` bounds one string and ships another:
    nine hundred spaces plus an `x` cleared the 512-character bound and 901 characters went
    upstream. Validating the stripped copy and sending the stripped copy is one value, so the bound
    means what an operator reading `PAPRA_INDEX_QUERY_MAX_CHARS` would assume it means. Stripping
    changes no meaning here — the tokeniser is upstream and leading whitespace is not a term.
    """
    if not isinstance(query, str) or not query.strip():
        raise InvalidRequest(f"{what} needs a non-empty query")
    asked = query.strip()
    if len(asked) > limits.query_max_chars:
        raise InvalidRequest(f"the query is {len(asked)} characters, and this tool accepts "
                             f"at most {limits.query_max_chars}")
    _require_page(page, page_size, limits=limits)
    return asked


def check_recent_request(limit: int, since: object, *, limits: SearchLimits) -> str | None:
    """The bounds of `list_recent_documents`, returning the `since` to forward (`None` = no bound).

    Same rule as `check_request`: the validated value is the forwarded one. A padded
    `" 2026-01-01 "` used to clear this check and then fail the client's own validator, which the
    caller heard as **`index_unavailable`: the index would not answer this request** — an outage
    reported for a typo in their own argument, and an invitation to retry something that can never
    succeed. It is `invalid-request` (or, once stripped, a valid request) and never the other.
    """
    _require_page(1, limit, limits=limits)
    if since is None:
        return None
    if not isinstance(since, str) or _SINCE_FORM.fullmatch(since.strip()) is None:
        raise InvalidRequest(
            f"since must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS', got {since!r}. A value in ISO "
            "`…Z` form is refused rather than rewritten: measured against the pinned container it "
            "answers 200 with no rows at all, and the column it would be compared against has no "
            "documented timezone, so converting it would pick a meaning for the instant and hide "
            "that choice from whoever asked for yesterday")
    return since.strip()


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
class DropCounts:
    """The exact counters, internal to one call, before anyone is told about them.

    Kept apart from `ResolveStats` on purpose: the wire model is what a caller may know, and two of
    these numbers are not in it for a scoped token. Splitting the two means the narrowing happens
    in one place (`_wire_stats`) rather than wherever a counter happens to be incremented, and the
    exact values are still there to log.
    """

    kept: int = 0
    dropped_unauthorized: int = 0
    dropped_unmatched: int = 0
    dropped_deleted: int = 0
    dropped_lookup_error: int = 0
    dropped_unreached: int = 0
    expanded: int = 0
    #: Exception type names seen by `_live_document`, so one failing Papra can be described in the
    #: log without logging one line per document.
    lookup_error_types: list[str] = field(default_factory=list)


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
    stats: DropCounts = field(default_factory=DropCounts)
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

    **It is defensive today, and the test that pins it says so.** `resolve_hits` admits each
    `(org_id, document_id)` to the candidate list once per call (`call.emitted`), so no tool path
    reachable right now asks this function the same question twice — which is why deleting the cache
    changes no observable behaviour of `search_documents`. The contract is still worth pinning, at
    the helper, because it is the helper that would be reused the first time that stops being true.

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
        except Exception as error:  # noqa: BLE001
            record = UNKNOWN
            # The type name only, bounded and deduplicated: `_log_what_the_caller_was_not_told`
            # writes one line for the whole call, so a 50-document outage is one log line that says
            # what kind of failure it was — and not Papra's message, which is prose from another
            # process and would be in the log anyway with a traceback if we let one escape.
            name = type(error).__name__
            if name not in call.stats.lookup_error_types and len(
                    call.stats.lookup_error_types) < 3:
                call.stats.lookup_error_types.append(name)
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
                reasons.append(f"the scan budget of {scan_rows} rows allowed no further whole "
                               f"page ({self.scanned} rows scanned; {spare} rows of allowance "
                               f"remain, less than one page's worth)")
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

    An allowance with no divisor at or above the floor does not divide: 101 is prime, and 997 after
    nine 100-row pages still leaves 97 rows that no further page can spend. Both take a full page
    and
    stop before exceeding the ceiling, and `Sweep.reasons` names the remainder rather than claiming
    the budget ran out. The floor of 25 is what keeps a pathological config from turning a scan into
    a hundred-and-one one-row requests.
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

    Four things happen here and each is a leak or a lie prevented:

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

    **And the unexpected is still a refusal.** Anything else that escapes — a Papra record that is
    not a mapping, a view whose method set moved — becomes `index_unavailable` too, with the type
    name in the log. Without that last branch the client gets the SDK's generic "Error executing
    tool", which is a fourth undocumented shape to code against, and this module's own rule is that
    a call which could not find out says so with the one code it already has.
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
    except Exception as error:
        # The catch-all this function did not have. Measured: a Papra record that is not a mapping
        # made `record.get("isDeleted")` raise `AttributeError`, which escaped `mcp_tools` whole —
        # and while the SDK kept the text off the wire (the client read only `Error executing tool
        # search_documents`), that is a fourth, undocumented error shape for a client that
        # branches on codes, and "this call could not find out" already has a code for it. The
        # answer is a refusal, exactly like every other failure to find out.
        logger.warning("mcp: %s failed unexpectedly (%s)", what, type(error).__name__)
        raise IndexUnavailable("this request could not be answered, so nothing is reported as an "
                               "answer") from error


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
    asked = check_request(query, page, page_size, limits=limits, what="search_documents")
    call = CallScope(limits=limits)

    async def sweep() -> Sweep:
        generation = await asyncio.to_thread(store.get_generation)

        async def fetch(upstream_page: int, limit: int):
            # `asked`, not `query`: the value that cleared the bound is the value that leaves.
            return await kz.search(asked, page=upstream_page, limit=limit)

        async def resolve(hits, need):
            resolved, _stats = await resolve_hits(ctx, store, papra, hits,
                                                  generation=generation, call=call, need=need)
            return resolved

        return await _collect(fetch, resolve, want=page * page_size, scan_rows=limits.scan_rows)

    swept = await _guarded("search_documents", sweep(), deadline_s=deadline_s)
    start = (page - 1) * page_size
    reasons = swept.reasons(call, scan_rows=limits.scan_rows)
    return _page(swept, call, items=swept.items[start:start + page_size], page=page,
                 page_size=page_size, reasons=reasons, scope_all=bool(ctx.scope_all),
                 what="search_documents")


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
    watermark = check_recent_request(limit, since, limits=limits)
    call = CallScope(limits=limits)

    async def sweep() -> Sweep:
        generation = await asyncio.to_thread(store.get_generation)

        async def fetch(upstream_page: int, row_limit: int):
            # `watermark` is the `since` that cleared `check_recent_request`, stripped: what the
            # caller wrote is validated and forwarded as one value, so no padded shape can reach
            # the client's own validator and be reported to the caller as an index outage.
            rows = await kz.list_documents(page=upstream_page, limit=row_limit, since=watermark)
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
    return _page(swept, call, items=swept.items[:limit], page=1, page_size=limit,
                 reasons=reasons, scope_all=bool(ctx.scope_all), what="list_recent_documents")


def _wire_stats(counts: DropCounts, *, scope_all: bool) -> ResolveStats:
    """The exact counters, in the form this caller is allowed to have.

    One place, because it is the only thing standing between a per-device token and a census of the
    buckets it was not given: an all-buckets caller gets the numbers (`dropped_unauthorized` is zero
    for them by construction — nothing is ever withheld from a token that covers everything), a
    scoped caller gets `withheld_band`. Everything else travels exact.
    """
    exact = (lambda n: str(n)) if scope_all else withheld_band
    return ResolveStats(
        kept=counts.kept,
        dropped_unauthorized=exact(counts.dropped_unauthorized),
        dropped_unmatched=exact(counts.dropped_unmatched),
        dropped_deleted=counts.dropped_deleted,
        dropped_lookup_error=counts.dropped_lookup_error,
        dropped_unreached=counts.dropped_unreached,
        expanded=counts.expanded,
    )


def _log_what_the_caller_was_not_told(what: str, swept: Sweep, call: CallScope,
                                     *, scope_all: bool) -> None:
    """The two facts the caller's JSON does not carry, written once per call for the operator.

    The banded counts, because the number is still worth having when somebody is debugging an empty
    result — and a Papra outage, because `dropped_lookup_error` inside a caller's payload is
    otherwise the only witness that this program could not read the archive: nothing else logged a
    line, and an outage nobody can see in a log is an outage nobody knows about. One line per call,
    with a count and the exception type names (bounded, deduplicated), the way `_guarded` reports a
    ledger failure: the type is the actionable part, and the messages are Papra's prose.
    """
    counts = call.stats
    if not scope_all and (counts.dropped_unauthorized or counts.dropped_unmatched):
        logger.info("mcp: %s banded its withheld counts for a scoped token; exact: "
                    "dropped_unauthorized=%d dropped_unmatched=%d rows_scanned=%d",
                    what, counts.dropped_unauthorized, counts.dropped_unmatched, swept.scanned)
    if counts.dropped_lookup_error:
        logger.warning("mcp: %s could not check %d document(s) in papra (%s), so they were dropped "
                       "and the page is partial", what, counts.dropped_lookup_error,
                       ", ".join(counts.lookup_error_types) or "no type recorded")


def _page(swept: Sweep, call: CallScope, *, items: list[SearchItem], page: int, page_size: int,
          reasons: list[str], scope_all: bool, what: str) -> SearchPage:
    """The one place `partial` is decided, so both tools answer it from the same rule.

    `partial` is **not** "the page is short": a page that ran out because the archive has nothing
    more is complete. It is "this answer could be missing something", which is true when the scan
    stopped early (`_collect`), when Papra could not be asked about something that might have been
    shown, or when the fan-out bound left a bucket in scope unconsulted. Rows the ledger refused, or
    that Papra reports deleted, are not in that list: those were checked, and the archive's answer
    was that they are not this caller's or not there any more.

    It is also the one place the stats leave this module, so the narrowing (`_wire_stats`) and the
    operator's log line happen for both tools or for neither.
    """
    _log_what_the_caller_was_not_told(what, swept, call, scope_all=scope_all)
    return SearchPage(
        items=items,
        page=page,
        page_size=page_size,
        partial=bool(reasons),
        scanned=swept.scanned,
        stats=_wire_stats(call.stats, scope_all=scope_all),
        buckets_reached=list(call.reached),
        buckets_skipped=list(call.skipped),
        partial_reason="; ".join(reasons) or None,
    )


# --- the three document tools ------------------------------------------------------------------

#: The 2026-11-16 spec's `Content size` ceiling for one document read, and the bound of the
#: `max_chars` argument. The realistic failure it protects against is not an exploit but a 400-page
#: bank statement, so the ceiling is a refusal a caller can see and work around (`truncated`,
#: `chars_total`, and `open_document` for the rest of it) rather than a clamp nobody notices.
MAX_CONTENT_CHARS = 20_000

#: What a `DocumentView` says about where the document lives, and the only value it ever says. A
#: constant rather than a per-call observation because it is a fact about this deployment's identity
#: model: everything a caller can act on is a Papra address. Naming a second system here — even a
#: truthful "kreuzakt" — would tell an agent about a service it has no business addressing and no
#: credential for.
OPENED_FROM = "papra"

#: The refusal for "this id is not a document the caller may read, and here is what that does and
#: does not mean". The negative half is not decoration: `not-found` is what a stale generation, a
# : soft-deleted row and a never-indexed document all answer, and a caller who reads it as "the
# archive
#: has no such document" will tell a user their invoice was never uploaded.
_NOT_FOUND_MESSAGE = (
    "no live reference to that document id exists in the part of this archive this token may read. "
    "That covers a document that was never indexed, one that is deleted, and one whose index entry "
    "belongs to a generation that has since been rebuilt; it is not a claim that the archive does "
    "not hold the document")

#: …and the refusal for "it exists, elsewhere". `not-in-scope` is the plan's code, and it is an
#: existence oracle about buckets this caller was not given: with a guessable id, `not-in-scope` and
#: `not-found` are two answers where the safe deployment would give one. Two things narrow it as far
#: as the code allows — it fires only for a reference that is live and bound to the current
# : generation (a foreign row that is deleted or stale answers `not-found`, like everything else),
# and
# : it never names the bucket or the document's title — and the argument for retiring it is written
# up
#: in the task report rather than settled here.
_NOT_IN_SCOPE_MESSAGE = (
    "a live reference to that document id exists in a bucket this token was not given, so nothing "
    "about it is readable here. The bucket is not named, and no other detail is returned")

# : The shape a Kreuzakt rowid has: digits, and nothing else. Papra's ids are prefixed, entropy-
# bearing
# : strings (`doc_bm1plswq9gnov2u9jvvc9s6r` in the 2026-11-17 webhook capture), so this never
# shadows a
#: real id — it matches the thing that must not be a lookup.
_KREUZAKT_ROWID_FORM = re.compile(r"\d+")


class DocumentNotFound(ToolError):
    """`not-found: …` — this caller may not read anything about that id.

    One code for four causes (absent, deleted, stale generation, and a Kreuzakt rowid presented as a
    document id) because the four are indistinguishable from the outside and one of them is not an
    answer the caller should be able to distinguish at all: if `"7"` said anything different from
    `"doc_zzz"`, the first probe would tell a caller whether the index has seven rows.
    """

    def __init__(self, reason: str = _NOT_FOUND_MESSAGE):
        super().__init__(f"not-found: {reason}")


class NotInScope(ToolError):
    """`not-in-scope: …` — a live reference exists outside this token's buckets.

    See `_NOT_IN_SCOPE_MESSAGE`: this is the one place on the MCP surface where a scoped token is
    told that something exists that it cannot have. The alternative — answering `not-found` — was
    the spec's own rule (`not_found_or_not_accessible`: "distinguishing them leaks existence across
    the allow-list boundary"), and the plan for this milestone overrode it so that an operator
    debugging an empty result can tell "wrong token" from "no document". Both are true, which is
    why the code says as little as it can and the report argues the rest.
    """

    def __init__(self, reason: str = _NOT_IN_SCOPE_MESSAGE):
        super().__init__(f"not-in-scope: {reason}")


class DocumentView(BaseModel):
    """One document, in the same terms a `SearchItem` uses — plus the two fields Papra does not
    hold.

    **Where each field comes from**, because "the archive" is two systems and they disagree:

    * `document_id`, `org_id` — the ledger's, i.e. Papra's. These are the only identifiers.
    * `title`, `document_date` — Papra's live record when it has them, the index otherwise. Both are
      fields the operator edits in Papra, and the rule is `_item`'s: a view that showed the index's
      stale copy of a renamed document would be a small confident wrong answer.
    * `mime_type` — Papra's, for the same reason.
    * `description`, `original_filename`, `page_count` — the index's, because that is where they
      exist. `page_count` is null when the index does not know, never `0`.

    No Kreuzakt integer, no Kreuzakt path: `KzDocument` never carried them and this model has no
    field that could.
    """

    document_id: str
    org_id: str
    title: str
    description: str
    document_date: str | None
    original_filename: str
    mime_type: str | None
    page_count: int | None
    opened_from: str


class ContentResult(BaseModel):
    """The document's text, bounded, sanitised, and labelled for what it is.

    `chars_total` is the length of the **sanitised** text this tool could have returned, so
    `chars_total - len(body)` is exactly what was withheld and `chars_total` never counts a
    character
    the caller was never offered. `truncated` is the plan's flag; the spec's `chars_omitted` is
    derivable from these two and is not carried twice. `sanitized` says whether the character rule
    changed anything, so "this document contains no control or format characters" is a fact the
    caller can read rather than an assumption about a tool it cannot see inside.

    `text` is the whole message: the untrusted-content wrapper is part of the text an agent reads,
    and an agent that receives the field receives the label with it.
    """

    text: str
    truncated: bool
    chars_total: int
    sanitized: bool


class OpenResult(BaseModel):
    """Two Papra URLs, built from nothing but the configured base and the reference's own ids.

    They are the answer to "I need the rest of it" and to "show me the file": the omitted tail is
    unreachable through MCP by the spec's v1 rule, so the honest offering is a place a human can
    open. Both are UI routes rather than API routes, measured against the pinned stack:
    `/organizations/{org}/documents/{id}` and its `/file` answer 200, while
    `/api/organizations/{org}/documents/{id}/file` answers 401 to anything with no key — a URL an
    agent cannot authenticate with is not a fallback, it is a dead end that looks like one.
    """

    viewer_url: str
    download_url: str


def _require_document_id(document_id: object, *, what: str) -> str:
    """The id bounds, returning the value that is then used. Never a Kreuzakt rowid.

    Two refusals, in the order that matters. An empty or whitespace id is `invalid-request`: nothing
    was asked of the archive, so nothing may be answered about it. A digits-only id is `not-found`
    and is refused **before any read at all** — the shape is the whole test, so a probe of the
    index's rowid space costs a ledger query and an index read for the answer, and gets neither.
    Papra ids are prefixed and entropy-bearing, so no real document is refused by this rule;
    `"0000007"` included.
    """
    if not isinstance(document_id, str) or not document_id.strip():
        raise InvalidRequest(f"{what} needs a papra document id, got {document_id!r}")
    asked = document_id.strip()
    if _KREUZAKT_ROWID_FORM.fullmatch(asked):
        raise DocumentNotFound()
    return asked


def _resolve_reference(ctx, store, document_id: str, *, what: str):
    """The one ledger read these tools make, and the whole authorization decision.

    Blocking by design, so it runs under `asyncio.to_thread` like every other store touch here. The
    order is the one the plan's Step 2 gives, and each step is a different lie prevented:

    1. `get_generation` first. A reference bound under an epoch that has been rebuilt points at an
       id that may now be some other document's bytes, so it is not a reference at all.
    2. `ledger_refs_for_document` — every row carrying the id, deleted and stale ones included,
       because the distinction between "nowhere", "not yours" and "not any more" is what the three
       refusals are made of.
    3. The scope test, against the **token**, on the **reference**: nothing here asks either API
       whether the document may be shown, so a scope answer costs one query and no network — which
       is what keeps "is this mine?" from becoming a latency and enumeration question at the index.
    4. More than one live in-scope row is `invalid-request`, not a pick. Papra ids are per
       organisation and global uniqueness is unverified; the 2026-11-16 spec's answer to one id in
       two allowed orgs is `multiple_matches` naming both, and since this tool takes no bucket
       argument the request itself is under-specified. Both names are safe to print here: the caller
       is granted every bucket the message lists.
    """
    generation = store.get_generation()
    refs = store.ledger_refs_for_document(document_id)
    live = [ref for ref in refs
            if ref.deleted_at is None and ref.delivered_generation == generation]
    mine = [ref for ref in live if ctx.scope_all or ref.papra_org_id in ctx.org_ids]
    if len(mine) > 1:
        buckets = ", ".join(sorted(ref.papra_org_id for ref in mine))
        raise InvalidRequest(
            f"{what}: {document_id} is a live document in {len(mine)} buckets ({buckets}), and "
            "this tool takes no bucket argument, so it cannot answer which one you mean. Ask for "
            "the document through one bucket's own credential, or open it in papra")
    if mine:
        return mine[0]
    if live:
        raise NotInScope()
    raise DocumentNotFound()


async def _live_record(papra, ref, *, what: str) -> dict:
    """Papra's current record for the one document the ledger already handed over.

    Two answers are refused here rather than absorbed, and both are the same mistake:

    **A read that errors is the outage code, not `not-found`.** In a search page an unchecked hit is
    dropped and counted, because the page has `partial` to say "this may be missing something". Here
    the document *is* the answer, and `not-found` would be a claim about the archive this call did
    not get to make — the failure the spec's error model exists to prevent, in its smallest possible
    form. Logged with the exception's class and not its message, which is another process's prose.

    **`isDeleted` is the archive's own no**, and it is asked before the index is touched: a document
    in the Papra trash still has a live ledger row until the delete pass gets to it, and Kreuzakt
    still
    serves its text. Papra is the archive, so the answer is `not-found` and the index never sees the
    request.

    A record that is not a mapping raises on `record.get`, and `_guarded` turns that into the outage
    code — the same treatment a malformed hit gets on the search path.
    """
    try:
        record = await papra.get_document(ref.papra_org_id, ref.papra_document_id)
    except Exception as error:  # noqa: BLE001 - "could not find out" is one answer here
        logger.warning("mcp: %s could not read papra's live record for one document (%s), so it is "
                       "refused rather than reported present or absent", what,
                       type(error).__name__)
        raise IndexUnavailable(
            "papra would not say whether this document is still there, so it can be reported as "
            "neither") from error
    if record.get("isDeleted"):
        raise DocumentNotFound(
            "papra reports this document as deleted, so it is not shown even though the index "
            "still holds a row for it")
    return record


def _document_view(ref, record: dict, kz_doc: KzDocument) -> DocumentView:
    """The one place a Papra record and an index body become a view. See `DocumentView` for which
    field comes from which, and why a field's owner is a question with two answers here."""
    document_date = record.get("documentDate") or kz_doc.document_date
    if isinstance(document_date, str) and len(document_date) > 10 and document_date[10] in "T ":
        document_date = document_date[:10]      # Papra stores a bare date as an instant
    return DocumentView(
        document_id=ref.papra_document_id,
        org_id=ref.papra_org_id,
        title=_bounded_text(record.get("name") or kz_doc.title),
        description=_bounded_text(kz_doc.description),
        document_date=_bounded_text(document_date, 40) or None,
        original_filename=_bounded_text(kz_doc.original_filename),
        mime_type=_bounded_text(record.get("mimeType"), 100) or None,
        page_count=kz_doc.page_count,
        opened_from=OPENED_FROM,
    )


def _content_result(ref, raw: str, *, max_chars: int) -> ContentResult:
    """Sanitise, cap, wrap — in that order, because each step's arithmetic depends on the one
    before.

    **Sanitise first**, so `chars_total` counts characters the caller could actually have been
    given,
    and so a truncated tail cannot end inside an escape sequence that a partial removal produced.

    **Then cap**, and the cap applies to the document text, never to the message: cutting the
    wrapper in half leaves a reader unable to tell where the document stops, which is the one thing
    the wrapper is for.

    **Then wrap**, in a delimiter carrying a per-call nonce. A fixed `</document-content>` is a tag
    any document can print, and a fence that a payload can close from the inside is worse than no
    fence, because it reads like a boundary that holds. The document's own text is **not** rewritten
    to defuse it: an invoice that contains that tag keeps saying it, and the only tag that ends the
    block is the one this call generated. `secrets` rather than a counter or the clock, because the
    reader this defends against has seen the previous results.

    The header carries the two ids percent-encoded: an id that contained `>` would otherwise end the
    opening tag inside its own attribute, and an attribute is exactly where an id-shaped string has
    no business having structure.
    """
    text = strip_unsafe_text(raw)
    truncated = len(text) > max_chars
    fence = f"document-content-{secrets.token_hex(4)}"
    header = (f'<{fence} document_id="{quote(ref.papra_document_id, safe="")}" '
              f'org_id="{quote(ref.papra_org_id, safe="")}" untrusted="true">')
    return ContentResult(
        text=f"{header}{text[:max_chars]}</{fence}>",
        truncated=truncated,
        chars_total=len(text),
        # The rule always runs; this says whether it had anything to remove, so a clean document is
        # reported as one instead of inheriting a warning it did not earn.
        sanitized=text != raw,
    )


def _papra_origin(base_url: object) -> str:
    """The configured Papra origin, or a refusal that quotes nothing back.

    An URL is operator configuration, and this one is about to be printed into a link an agent may
    click. So: http(s) and nothing else (an `ftp://`, `file://` or javascript-shaped base is a
    refusal, not a string concatenation), credentials refused rather than carried forward, and the
    query and fragment dropped — a base that arrived with `?token=…` would otherwise hand that
    token to whoever opened the link. Neither the value nor a piece of it reaches the client's
    message or the log line: `PAPRA_BASE_URL` is the address of the archive, and a bearer of a read
    token is not owed the deployment's internal hostnames.
    """
    unusable = IndexUnavailable(
        "this server has no usable papra address configured, so it cannot offer a link. That is an "
        "operator setting (PAPRA_BASE_URL), not something this request can change")
    try:
        parsed = urlparse(base_url) if isinstance(base_url, str) else None
        port = parsed.port if parsed is not None else None
    except ValueError:
        parsed, port = None, None
    if (parsed is None or parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username or parsed.password):
        logger.warning("mcp: open_document refused: the configured papra base url is not a usable "
                       "http(s) origin without credentials")
        raise unusable
    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"                    # an IPv6 literal needs its brackets kept intact
    netloc = f"{host}:{port}" if port else host
    return f"{parsed.scheme}://{netloc}{parsed.path.rstrip('/')}"


async def get_document(ctx, store, papra, kz, document_id: str, *,
                       deadline_s: float | None = TOOL_DEADLINE_S) -> DocumentView:
    """One document's metadata, by its Papra id, for this principal.

    The three steps are the whole design and they run in this order for cost reasons and for
    security ones at the same time: the **ledger** decides whose it is (one query, no network),
    **Papra** says whether it is still there (one GET), and **Kreuzakt** supplies what only the
    index knows (the page count, the index's own description and filename). A document that fails
    the first step never causes a request to either service, which is what makes "a disallowed
    document is never fetched" true of this tool rather than merely intended.

    An index outage is refused here too, even though the fields that would have come from it are
    metadata. A view that quietly omitted `page_count` and `description` during an outage would be a
    different document than the one served on a healthy day, and the caller has no way to notice.
    """
    asked = _require_document_id(document_id, what="get_document")

    async def body() -> DocumentView:
        ref = await asyncio.to_thread(_resolve_reference, ctx, store, asked, what="get_document")
        record = await _live_record(papra, ref, what="get_document")
        kz_doc = await kz.get_document(ref.kreuzakt_id)
        return _document_view(ref, record, kz_doc)

    return await _guarded("get_document", body(), deadline_s=deadline_s)


async def get_document_content(ctx, store, papra, kz, document_id: str, *,
                               max_chars: int = MAX_CONTENT_CHARS,
                               deadline_s: float | None = TOOL_DEADLINE_S) -> ContentResult:
    """One document's text, bounded by `max_chars`, sanitised, and wrapped as data.

    The text comes from the **index** and from nowhere else. Papra's detail payload carries a
    `content`
    field too — the copy this suite delivered — and falling back to it during an index outage would
    answer with a copy that may be older, or blank where the index has text, with nothing in the
    result to say which. So an outage is `index_unavailable`, and the worse copy stays unread.

    `max_chars` is bounded by `MAX_CONTENT_CHARS` and refused outside it rather than clamped, for
    the reason every other bound here is refused: a caller that asked for 200 000 characters and got
    20 000
    believes it got 200 000. `truncated` and `chars_total` together say what was withheld, and
    `open_document` is where the rest of it lives.
    """
    if (not isinstance(max_chars, int) or isinstance(max_chars, bool)
            or not 1 <= max_chars <= MAX_CONTENT_CHARS):
        raise InvalidRequest(
            f"max_chars must be an integer between 1 and {MAX_CONTENT_CHARS}, got {max_chars!r} — "
            "the bound is refused rather than clamped, so you are never handed a shorter document "
            "than the one you asked for")
    asked = _require_document_id(document_id, what="get_document_content")

    async def body() -> ContentResult:
        ref = await asyncio.to_thread(_resolve_reference, ctx, store, asked,
                                      what="get_document_content")
        # Checked, not read: this is the call that refuses a document Papra has
        # trashed (and an index outage that must not be answered from Papra's copy).
        await _live_record(papra, ref, what="get_document_content")
        kz_doc = await kz.get_document(ref.kreuzakt_id)
        return _content_result(ref, kz_doc.content, max_chars=max_chars)

    return await _guarded("get_document_content", body(), deadline_s=deadline_s)


async def open_document(ctx, store, document_id: str, *, papra_base_url: str | None,
                        deadline_s: float | None = TOOL_DEADLINE_S) -> OpenResult:
    """Where this document lives in Papra: two URLs, no other service, and no network call at all.

    The scope rule is the same as the other two tools — same ledger read, same three refusals — and
    then this stops. That is deliberate, and it is worth stating what it buys and what it costs:

    * It buys a tool that still works while the index is down (which is when an operator most wants
      the link), and one whose answer cannot name a Kreuzakt path, id or host, because it never
      speaks to Kreuzakt.
    * It costs the live Papra check. A document deleted in Papra a moment ago, whose ledger row the
      delete pass has not marked yet, still gets a URL. No content is disclosed by that — the caller
      already holds the id, and the link goes to the archive's own UI, which is the authority on
      whether the document is there and says so itself. The two tools that return text do check, and
      they are where the disclosure would be.

    Path segments are percent-encoded with `safe=""`, so an id that reads `../../admin?x=1#y` cannot
    walk out of the route it belongs in: the encoding belongs to the URL, and the id is reported
    verbatim everywhere else.
    """
    asked = _require_document_id(document_id, what="open_document")
    origin = _papra_origin(papra_base_url)

    async def body() -> OpenResult:
        ref = await asyncio.to_thread(_resolve_reference, ctx, store, asked,
                                      what="open_document")
        path = (f"/organizations/{quote(ref.papra_org_id, safe='')}"
                f"/documents/{quote(ref.papra_document_id, safe='')}")
        return OpenResult(viewer_url=f"{origin}{path}", download_url=f"{origin}{path}/file")

    return await _guarded("open_document", body(), deadline_s=deadline_s)
