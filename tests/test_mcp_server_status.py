"""Task 12, part 3: `index_status`, and the rule that a scope is a boundary.

The brief's mutation target is one sentence: *global queue counts reaching a one-bucket token is a
leak.* That is the shape of every test here. A corpus-wide number does not have to be a secret to be
a leak — it tells a reader how big the archive is, how much of it is missing, and how much delete
debt it carries, and none of that belongs to a credential that was minted for one bucket.

So the scoping rule is applied field by field, and tested field by field: one test per field,
because a single "the whole response is scoped" assertion is one assertion, and a mutation that
leaks one number out of nine passes it. The same discipline gave this task its `unledgered` and
`delivery-blocked` fields: a warning in a README is not a control, a number is — and the number is
only a control if the person who is allowed to see it is the person who sees it.

Two deliberate asymmetries:

* **Queue depth is privileged.** It is a corpus-wide aggregate with no per-bucket form, so it is
  `None` for anything but an `--all-buckets` token.
* **An outage is an error, never a Papra-backed answer.** This suite is a bridge; answering from
  Papra while Kreuzakt is down would be a second index with worse search and no warning.
"""

import asyncio
import itertools
import json
import logging
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest
from starlette.testclient import TestClient

from papra_suite.indexing import (
    DELETION_QUARANTINE_SETTING,
    LISTING_WALK_INTERVAL_S,
    LISTING_WALK_STAMP_SETTING,
    LIVENESS_PROBE_UNANSWERED_SETTING,
    MARKS_ROLLING_SETTING,
    MARKS_ROLLING_WINDOW_S,
)
from papra_suite.kreuzakt_client import KreuzaktError
from papra_suite.mcp_server import (
    IndexStatus,
    IndexUnavailable,
    build_mcp_app,
    index_status_for,
    token_context_of,
)
from papra_suite.mcp_tokens import mint_token, resolve_token
from papra_suite.readers import LedgerReader
from papra_suite.store import EMPTY_EXTRACTION_SHA, Store
from tests.fakes.ledger import close_views, remember, view_for

PUBLIC_URL = "https://papra-mcp.ts.net"
HOST = "papra-mcp.ts.net"
SCOPED = "org-1"
OTHER = "org-2"


class FakeKreuzakt:
    """`health` and `queue_counts`, counted, plus the two content reads named so the app can build.

    Everything `index_status` is allowed to ask is scripted; the two reads that belong to
    `search_documents` and `list_recent_documents` raise, because a status test that answered a
    search from an unscripted fake would report an archive it never seeded. The search tests in
    `tests/test_mcp_search.py` script those two themselves.
    """

    def __init__(self, *, counts=None, health_body=None, error=None):
        self.counts = {"pending": 40, "completed": 900} if counts is None else counts
        self.health_body = {"status": "ok"} if health_body is None else health_body
        self.error = error
        self.health_calls = 0
        self.queue_calls = 0

    async def health(self):
        self.health_calls += 1
        if self.error is not None:
            raise self.error
        return dict(self.health_body)

    async def queue_counts(self):
        self.queue_calls += 1
        if self.error is not None:
            raise self.error
        return dict(self.counts)

    async def search(self, query, *, page=1, limit=100):
        raise AssertionError("an index_status test asked the index for search results")

    async def list_documents(self, *, page=1, limit=100, since=None):
        raise AssertionError("an index_status test asked the index for a document listing")


class NoPapra:
    """No tool these tests register reads Papra, so a call to it is a failure, not a stub.

    The five names are written out rather than fabricated by `__getattr__`, because containment
    option C refuses to build an app on an object whose capability surface cannot be enumerated: a
    proxy fake answers `update_document` as readily as `list_tags`, which is precisely what
    `readers.require_read_only` must not wave through. Same promise, one that can be checked.

    `index_status` still never reads Papra; `search_documents` does, and those tests use their own
    scripted double in `tests/test_mcp_search.py`.
    """

    async def get_document(self, org_id, document_id):
        raise AssertionError("index_status must not read papra")

    async def list_documents(self, org_id, **kwargs):
        raise AssertionError("index_status must not read papra")

    async def download_file(self, org_id, document_id):
        raise AssertionError("index_status must not read papra")

    async def list_tags(self, org_id):
        raise AssertionError("index_status must not read papra")

    async def whoami(self):
        raise AssertionError("index_status must not read papra")

    async def aclose(self):
        raise AssertionError("the listener owns the views it opened, not this one")


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


@pytest.fixture(autouse=True)
def _close_ledger_views():
    """The wire tests open a read-only connection each; this is where they are closed."""
    yield
    close_views()


def seed_bucket(store, org, *, refs=0, indexed=0, empty=0, blocked=0,
                cleanup_pending=0, cleanup_failed=0, unledgered=0):
    """One bucket, with each population built through the store's own API.

    The knobs are nested rather than independent, because the real counts are: `empty` and `blocked`
    are subsets of `indexed` (a refusal is charged to a binding that exists, and so is an empty
    delivery), and `cleanup_failed` is a subset of `cleanup_pending`. Anything else would describe a
    ledger the passes could not have produced.

    `unledgered` is the population Task 7's review found invisible: `documents` says this suite saw
    it, `index_ledger` never held it. It is built by the seen-record write alone, which is exactly
    how the pre-handover mail path left it.
    """
    generation = store.get_generation()
    ids = []
    for index in range(refs):
        document_id = f"{org}-doc-{index}"
        store.ledger_upsert_reference(papra_org_id=org, papra_document_id=document_id,
                                      file_hash=f"{index:02x}" * 32,
                                      papra_original_name=f"{document_id}.pdf")
        ids.append(document_id)
    for index, document_id in enumerate(ids[:indexed]):
        store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=document_id,
                                     kreuzakt_id=index + 1, generation=generation)
    for document_id in ids[:empty]:
        store.ledger_record_delivery(papra_org_id=org, papra_document_id=document_id,
                                     delivered_at="2026-11-19T10:00:00+00:00",
                                     delivered_sha=EMPTY_EXTRACTION_SHA, generation=generation)
    for index, document_id in enumerate(ids):
        if not empty <= index < empty + blocked:
            continue
        for _ in range(5):
            store.ledger_record_delivery_failure(
                papra_org_id=org, papra_document_id=document_id,
                expected_kreuzakt_id=index + 1, generation=generation,
                reason="413 too large")
    for document_id in ids[:cleanup_pending]:
        store.ledger_queue_cleanup(papra_org_id=org, papra_document_id=document_id,
                                   due_at="2026-11-19T10:00:00+00:00", kreuzakt_id=1,
                                   generation=generation)
    if cleanup_failed:
        with store._conn:
            store._conn.execute(
                "UPDATE index_ledger SET cleanup_state = 'failed' WHERE papra_org_id = ? "
                "AND papra_document_id IN (" + ",".join("?" * cleanup_failed) + ")",
                [org, *ids[:cleanup_failed]])
    for index in range(unledgered):
        store.record_document_seen(papra_org_id=org, papra_document_id=f"{org}-orphan-{index}",
                                   name=f"{org}-orphan-{index}.pdf", mime="application/pdf",
                                   ingested_via="mail")
    return generation


_minted = itertools.count()


def scoped_token(store, *orgs):
    # A unique label per mint: a test that asks for two tokens on the same bucket must get two, and
    # `mint_token` refuses a duplicate active label on purpose.
    return mint_token(store, label=f"scoped-{next(_minted)}", org_ids=list(orgs))


def all_token(store):
    return mint_token(store, label=f"operator-{next(_minted)}", org_ids=[], scope_all=True)


async def status_for(store, kz, token):
    from papra_suite.mcp_tokens import resolve_token

    context = resolve_token(store, token)
    assert context is not None
    return await index_status_for(store, kz, context)


# --- the fields a scoped token may see, one test each --------------------------------------
#
# In scope: `org-1` — 4 references, 2 of them indexed, 1 of those delivered an empty extraction and
# 1 blocked after five refusals, 1 owed delete, plus 1 document Papra has and the ledger never held.
# Out of scope: `org-2`, larger in every dimension. Every assertion below is therefore a different
# mutation: dropping the bucket restriction turns each of these numbers into the corpus total, and
# each test dies on its own field.

IN_SCOPE = dict(refs=4, indexed=2, empty=1, blocked=1, cleanup_pending=1, unledgered=1)
OUT_OF_SCOPE = dict(refs=5, indexed=3, empty=2, blocked=1, cleanup_pending=3, cleanup_failed=2,
                    unledgered=4)


@pytest.fixture
def seeded(store):
    seed_bucket(store, SCOPED, **IN_SCOPE)
    seed_bucket(store, OTHER, **OUT_OF_SCOPE)
    # Both scheduling brakes are armed in the shared fixture, stamped against the real clock because
    # they are read against it: a scoped token must still see the gate (it is timing, not counting),
    # and the probe list is asserted to be privileged below.
    moment = datetime.now(UTC).isoformat()
    store.set_setting(LIVENESS_PROBE_UNANSWERED_SETTING, f"{moment}|11,12,13")
    store.set_setting(LISTING_WALK_STAMP_SETTING, moment)
    return store


async def bucket_status(store, kz, token, org=SCOPED):
    status = await status_for(store, kz, token)
    matching = [bucket for bucket in status.buckets if bucket.org_id == org]
    assert len(matching) == 1, status
    return matching[0]


async def test_a_scoped_token_sees_only_its_own_references(seeded):
    kz = FakeKreuzakt()
    bucket = await bucket_status(seeded, kz, scoped_token(seeded, SCOPED))
    assert bucket.references == IN_SCOPE["refs"]


async def test_a_scoped_token_sees_only_its_own_indexed_count(seeded):
    bucket = await bucket_status(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    assert bucket.indexed_current == IN_SCOPE["indexed"]


async def test_a_scoped_token_sees_only_its_own_stale_count(seeded):
    bucket = await bucket_status(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    assert bucket.stale == IN_SCOPE["refs"] - IN_SCOPE["indexed"]


async def test_a_scoped_token_sees_only_its_own_unledgered_count(seeded):
    """The number Task 7's review demanded, per bucket, and never corpus-wide.

    A token minted for one bucket is told how much of *that* bucket never reached the ledger — which
    is what makes it actionable — and learns nothing about the rest of the corpus.
    """
    bucket = await bucket_status(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    assert bucket.unledgered == IN_SCOPE["unledgered"]


async def test_a_scoped_token_sees_only_its_own_delivery_blocked_count(seeded):
    bucket = await bucket_status(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    assert bucket.delivery_blocked == IN_SCOPE["blocked"]


async def test_a_scoped_token_sees_only_its_own_empty_extractions(seeded):
    bucket = await bucket_status(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    assert bucket.extraction_empty == IN_SCOPE["empty"]


async def test_a_scoped_token_sees_only_its_own_cleanup_debt(seeded):
    bucket = await bucket_status(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    assert bucket.cleanup_pending == IN_SCOPE["cleanup_pending"]
    assert bucket.cleanup_failed == 0


async def test_a_scoped_token_sees_only_its_own_set_aside_state(seeded):
    """Quarantine is per bucket, and so is what a scoped token is allowed to learn about it."""
    seeded.set_setting(DELETION_QUARANTINE_SETTING, f"{SCOPED},{OTHER}")
    mine = await bucket_status(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    theirs = await bucket_status(seeded, FakeKreuzakt(), scoped_token(seeded, OTHER), org=OTHER)
    assert mine.set_aside is True and theirs.set_aside is True

    only_other = await status_for(seeded, FakeKreuzakt(), scoped_token(seeded, OTHER))
    assert [bucket.org_id for bucket in only_other.buckets] == [OTHER]
    assert only_other.set_aside == [OTHER]


async def test_the_response_carries_nothing_about_an_ungranted_bucket(seeded):
    """Not a number, not a name, not a count of buckets the token does not have."""
    status = await status_for(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    assert [bucket.org_id for bucket in status.buckets] == [SCOPED]
    assert OTHER not in status.model_dump_json()


# --- the top line: sums over what is in scope -------------------------------------

async def test_the_totals_are_the_in_scope_sums_not_the_corpus(seeded):
    status = await status_for(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    assert status.indexed_for_scope == IN_SCOPE["indexed"]
    assert status.stale == IN_SCOPE["refs"] - IN_SCOPE["indexed"]
    assert status.unledgered == IN_SCOPE["unledgered"]
    assert status.delivery_blocked == IN_SCOPE["blocked"]
    assert status.scope == "buckets"


async def test_an_all_bucket_token_sees_the_corpus_totals(seeded):
    status = await status_for(seeded, FakeKreuzakt(), all_token(seeded))
    assert status.scope == "all"
    assert status.indexed_for_scope == IN_SCOPE["indexed"] + OUT_OF_SCOPE["indexed"]
    assert status.unledgered == IN_SCOPE["unledgered"] + OUT_OF_SCOPE["unledgered"]
    assert status.delivery_blocked == IN_SCOPE["blocked"] + OUT_OF_SCOPE["blocked"]


async def test_an_all_bucket_token_covers_a_bucket_with_only_unledgered_documents(seeded):
    """The population has to be reachable by name, not only by a corpus number.

    Enumerating buckets from the ledger alone would show every count for `org-3` as absent while the
    total said otherwise — the exact shape that hides a whole bucket outside the index.
    """
    seed_bucket(seeded, "org-3", unledgered=3)
    status = await status_for(seeded, FakeKreuzakt(), all_token(seeded))
    assert [bucket.org_id for bucket in status.buckets] == sorted([OTHER, SCOPED, "org-3"])
    assert status.buckets[-1].unledgered == 3


async def test_a_token_with_no_grant_sees_no_buckets_and_no_counts(seeded):
    status = await status_for(seeded, FakeKreuzakt(), scoped_token(seeded))
    assert status.scope == "none"
    assert status.buckets == [] and status.queue is None
    assert status.indexed_for_scope == 0 and status.unledgered == 0
    assert status.delivery_blocked == 0 and status.marks is None
    assert status.probe_rereads is None


# --- what only an all-buckets token may see ---------------------------------------

async def test_index_status_for_a_scoped_token_leaks_no_global_counts(seeded):
    """The brief's mutation target, in one assertion pair.

    Queue depth is a corpus-wide aggregate: Kreuzakt answers "how much work is there", and that is a
    statement about every bucket. A one-bucket token gets `None`, and the tool says so rather than
    inventing a zero — `pending: 0` would read as "your documents are indexed".
    """
    kz = FakeKreuzakt(counts={"pending": 40, "completed": 900})
    status = await status_for(seeded, kz, scoped_token(seeded, SCOPED))
    assert status.queue is None and status.indexed_for_scope == IN_SCOPE["indexed"]
    assert kz.queue_calls == 0, "a scoped token must not even ask for the aggregate"


async def test_index_status_for_an_all_bucket_token_includes_queue(seeded):
    kz = FakeKreuzakt(counts={"pending": 40, "completed": 900})
    status = await status_for(seeded, kz, all_token(seeded))
    assert status.queue["pending"] == 40 and status.queue["completed"] == 900
    assert kz.queue_calls == 1


async def test_the_marks_brake_counts_are_privileged(seeded):
    """The window counter is a corpus-wide count of deletion marks; the allowance is a constant.

    Both are `None` for a scoped token: printing the allowance without the count would be a hint
    about what the hidden number is being judged against, and printing the count is the leak.

    The window is stamped half a window ago, not with a literal: this counter is read against the
    real clock (`MARKS_ROLLING_WINDOW_S`, a day), so a fixed stamp says “34” for one day and then
    “0” for the rest of the project — the same trap the cadence test above fell into.
    """
    half_window = MARKS_ROLLING_WINDOW_S / 2
    stamp = (datetime.now(UTC) - timedelta(seconds=half_window)).isoformat()
    seeded.set_setting(MARKS_ROLLING_SETTING, f"{stamp}|34")
    scoped = await status_for(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    everything = await status_for(seeded, FakeKreuzakt(), all_token(seeded))
    assert scoped.marks is None
    assert everything.marks is not None and everything.marks.counted == 34
    assert everything.marks.allowance > 0


async def test_the_pending_probe_list_is_privileged(seeded):
    """The size of the retry list is a corpus fact, so a bucket grant does not unlock it.

    `11,12,13` are Kreuzakt document ids, and how many of them the index will not answer for says
    how unhealthy the corpus is overall — including the buckets this token was not given. The count
    is the leak being tested, not the ids.
    """
    scoped = await status_for(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    everything = await status_for(seeded, FakeKreuzakt(), all_token(seeded))
    assert scoped.probe_rereads is None
    assert everything.probe_rereads == 3


async def test_the_cadence_gate_is_shown_to_every_token(seeded):
    """When the next deletion pass may run is timing, not a count, and it explains an idle ledger.

    Task 9's review found an operator comparing a stale-looking `check` to a fresh trash and
    concluding propagation was broken. That reading is corrected by a "gated until" the scoped token
    can also see — and it names no other bucket.
    """
    # Relative to the real clock: half an interval ago, so the gate is genuinely open for another
    # half-interval whenever the suite runs. This used a literal, so it
    # passed only until that instant arrived — red from 09:00 UTC on and never green again. That
    # is how a cadence regression ends up “fixed” by deleting an assertion nobody trusts.
    half = LISTING_WALK_INTERVAL_S / 2
    seeded.set_setting(LISTING_WALK_STAMP_SETTING,
                       (datetime.now(UTC) - timedelta(seconds=half)).isoformat())
    scoped = await status_for(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    assert scoped.gated_until, "a pass that ran half an interval ago is still gated"
    gate = datetime.fromisoformat(scoped.gated_until)
    assert gate > datetime.now(UTC), f"the gate is printed as already open: {scoped.gated_until}"


async def test_an_expired_cadence_prints_nothing_rather_than_a_stale_date(seeded):
    """The other arm, next to the first: a cadence that ran out has **no** date to show.

    A `gated_until` in the past is the confident wrong answer this field exists to prevent — an
    operator reads “until 09:00” at 14:00 as “the pass is late” when the truth is “it may run on the
    next sweep”. And without this arm, a mutation that made stamps never expire would keep the test
    above green: it only ever looks at the open case.
    """
    stale = datetime.now(UTC) - timedelta(seconds=LISTING_WALK_INTERVAL_S * 2)
    seeded.set_setting(LISTING_WALK_STAMP_SETTING, stale.isoformat())

    scoped = await status_for(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))

    assert scoped.gated_until is None, f"an expired cadence printed a date: {scoped.gated_until}"


# --- Kreuzakt down is an error, not an answer --------------------------------------

async def test_index_status_raises_index_unavailable_when_kreuzakt_is_down(store):
    """No Papra-backed fallback anywhere: an outage says "index unavailable"."""
    token = scoped_token(store, SCOPED)
    kz = FakeKreuzakt(error=KreuzaktError("connection refused"))
    with pytest.raises(IndexUnavailable, match="index_unavailable"):
        await status_for(store, kz, token)


async def test_a_health_body_that_is_not_ok_is_also_unavailable(store):
    token = scoped_token(store, SCOPED)
    kz = FakeKreuzakt(health_body={"status": "degraded"})
    with pytest.raises(IndexUnavailable, match="degraded"):
        await status_for(store, kz, token)


async def test_a_refused_queue_read_is_unavailable_even_for_a_scoped_token(store, seeded):
    """The scoped token never asks for the queue, so the refusal must come from the health probe.

    Otherwise a degraded index would answer a scoped token with confident per-bucket counts, which
    is the same wrong answer the all-buckets token is refused.
    """
    token = scoped_token(seeded, SCOPED)
    kz = FakeKreuzakt(error=KreuzaktError("502"))
    with pytest.raises(IndexUnavailable):
        await status_for(seeded, kz, token)
    assert kz.queue_calls == 0


async def test_the_outage_message_carries_no_counts_and_no_upstream_body(store, seeded):
    token = scoped_token(seeded, SCOPED)
    kz = FakeKreuzakt(error=KreuzaktError(
        'upstream said {"documents":9000,"secret":"internal body"} after 30s'))
    with pytest.raises(IndexUnavailable) as caught:
        await status_for(seeded, kz, token)
    message = str(caught.value)
    assert "9000" not in message and "internal body" not in message
    assert message.startswith("index_unavailable")
    assert len(message) <= 300, "an upstream message is echoed bounded"


async def test_a_plain_text_upstream_body_is_not_echoed_either(store, seeded):
    """The body that gets past a sanitizer which only knows about JSON.

    An earlier version of `_upstream_reason` cut the upstream message at the first brace or bracket,
    which stops `{"documents":9000}` and nothing else: a proxy's own error line names its software,
    its config path and its row counts in plain text, and every word of it reached the client. The
    status is the only upstream fact a caller is given now, and this is the case the old split let
    through whole.
    """
    token = scoped_token(seeded, SCOPED)
    kz = FakeKreuzakt(error=KreuzaktError(
        "502 Bad Gateway from nginx/1.27.4: upstream /var/lib/kreuzakt/index.db is locked "
        "with 9000 rows pending", status=502))
    with pytest.raises(IndexUnavailable) as caught:
        await status_for(seeded, kz, token)
    message = str(caught.value)
    for needle in ("Bad Gateway", "nginx", "/var/lib/kreuzakt", "locked", "9000", "pending"):
        assert needle not in message, needle
    assert "http 502" in message          # the actionable part, and the only part


async def test_a_refused_health_read_reaches_the_wire_with_no_upstream_prose(store, seeded):
    """The same claim through `tools/call`, because the SDK is what composes the client's text.

    A refusal asserted on a raised exception can still be relayed differently once the tool wrapper
    serialises it, and this is the path an agent actually reads.
    """
    token = scoped_token(seeded, SCOPED)
    kz = FakeKreuzakt(error=KreuzaktError(
        "500 Internal Server Error: /var/lib/kreuzakt/index.db, 9000 documents", status=500))
    _response, message = wire_call(seeded, token, kz=kz)
    assert message["result"]["isError"] is True
    serialised = json.dumps(message, ensure_ascii=False).lower()
    # The needles are the ones the plan actually bars: upstream prose, internal paths, counts that
    # are not the caller's, and our own class names. Not the word "kreuzakt" — naming the component
    # is how `index_status` has always described the outage, and the containment rule is about
    # identifiers and bodies, not vocabulary.
    for needle in ("internal server error", "/var/lib/kreuzakt", "9000", "documents\":",
                   "kreuzakterror", "operationalerror"):
        assert needle not in serialised, needle
    assert "index_unavailable" in serialised and "http 500" in serialised


# --- the tool, over the wire ---------------------------------------------------------------

def wire_call(store, token, name="index_status", arguments=None, kz=None):
    """Drive the real MCP handshake against the in-process app and return the tool result.

    `store` is the fixture's store, or a read-only view of it when the test wants a view that
    misbehaves (`BlockingStore`): either way the app is built on a `LedgerReader`, never on `Store`.
    `kz` lets an outage test hand the app an index that fails in a specific, scripted way.
    """
    view = store if isinstance(store, LedgerReader) else view_for(store)
    app = build_mcp_app(view, NoPapra(), kz or FakeKreuzakt(), url=PUBLIC_URL)
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
        session = initialised.headers.get("mcp-session-id")
        headers["mcp-session-id"] = session
        client.post("/mcp", headers=headers,
                    json={"jsonrpc": "2.0", "method": "notifications/initialized"})
        response = client.post("/mcp", headers=headers, json=payload)
    frames = [json.loads(line[6:]) for line in response.text.splitlines()
              if line.startswith("data: ")]
    return response, frames[0] if frames else {}


def structured(message):
    return message.get("result", {}).get("structuredContent")


def test_the_tool_answers_the_scope_it_authenticated(store, seeded):
    """The authorization path end to end: the context comes from the bearer gate, not an argument.

    There is no `org_id` argument to pass and no way to ask for a bucket the token does not carry,
    which is what makes "the agent may narrow, never widen" true in the schema rather than in a
    comment.
    """
    response, message = wire_call(seeded, scoped_token(seeded, SCOPED))
    payload = structured(message)
    assert response.status_code == 200
    assert payload["scope"] == "buckets"
    assert [bucket["org_id"] for bucket in payload["buckets"]] == [SCOPED]
    assert payload["queue"] is None
    assert payload["buckets"][0]["unledgered"] == IN_SCOPE["unledgered"]
    assert payload["buckets"][0]["delivery_blocked"] == IN_SCOPE["blocked"]


def test_the_tool_returns_a_pydantic_model(store, seeded):
    """N7: structured output, so the client parses rather than pattern-matches prose."""
    response, message = wire_call(seeded, all_token(seeded))
    payload = structured(message)
    assert IndexStatus.model_validate(payload).scope == "all"
    assert set(payload) >= {"scope", "buckets", "indexed_for_scope", "unledgered",
                            "delivery_blocked", "queue", "set_aside", "gated_until"}


def test_the_tool_reports_the_outage_as_a_tool_error(store, monkeypatch):
    """A hard error, and not one number in place of it."""
    import papra_suite.mcp_server as mcp_server

    token = mint_token(store, label="pi", org_ids=[SCOPED])
    seed_bucket(store, SCOPED, refs=1, indexed=1)
    broken = FakeKreuzakt(error=KreuzaktError("connection refused"))
    app = mcp_server.build_mcp_app(view_for(store), NoPapra(), broken, url=PUBLIC_URL)
    with TestClient(app) as client:
        headers = {"host": HOST, "content-type": "application/json",
                   "accept": "application/json, text/event-stream",
                   "authorization": f"Bearer {token}"}
        client.post("/mcp", headers=headers, json={
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-11-25", "capabilities": {},
                       "clientInfo": {"name": "pytest", "version": "0"}}})
        session = client.post("/mcp", headers=headers, json={
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-11-25", "capabilities": {},
                       "clientInfo": {"name": "pytest", "version": "0"}}}).headers.get(
                           "mcp-session-id")
        headers["mcp-session-id"] = session
        response = client.post("/mcp", headers=headers, json={
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "index_status", "arguments": {}}})
    frames = [json.loads(line[6:]) for line in response.text.splitlines()
              if line.startswith("data: ")]
    result = frames[0]["result"]
    assert result["isError"] is True
    assert "index_unavailable" in result["content"][0]["text"]
    assert "refs" not in result["content"][0]["text"]


def test_index_status_writes_nothing_to_the_ledger_or_the_queue(store, seeded):
    """N7: no tool in this task mutates Papra, Kreuzakt, or the ledger."""
    generation_before = seeded.get_generation()
    jobs_before = len(seeded.list_jobs(limit=500))
    token = scoped_token(seeded, SCOPED)
    for _ in range(2):
        wire_call(seeded, token)
    assert seeded.get_generation() == generation_before
    assert len(seeded.list_jobs(limit=500)) == jobs_before
    assert seeded.ledger_stats()["references"] == IN_SCOPE["refs"] + OUT_OF_SCOPE["refs"]


def test_token_context_of_refuses_a_request_with_no_principal():
    """The tool's own guard: a context that did not authenticate gets nothing, not a wide scope."""
    assert token_context_of(None) is None
    assert token_context_of(object()) is None


# --- what an unreadable store says, and where the reads run (review I5, I6) -----------------


class BlockingStore(LedgerReader):
    """A read-only ledger view whose counts scan blocks the **thread** they run in.

    `ledger_stats` is a correlated `NOT EXISTS` scan, and this listener shares `serve`'s event loop
    with the pollers, the OCR workers and the sweep, against one single-writer connection. Run on
    the loop, one `index_status` call stalls the whole pipeline behind a delivery write; the house
    answer is `asyncio.to_thread` (`mail.py`, `pipeline.py`). If the reads go back onto the loop,
    the heartbeat below stops ticking and this class is what notices.

    It is a `LedgerReader` and not a wrapper around `Store`, for the same reason the shipped code
    is: the handler's dependency must not be able to write, and a wrapper with a catch-all
    `__getattr__` cannot be shown not to.
    """

    def __init__(self, store, *, delay_s=0.2, error=None):
        super().__init__(store.db_path)
        remember(self)
        self._store = store
        self.delay_s = delay_s
        self.error = error
        self.blocking_calls = 0

    def ledger_stats(self, *, org_ids=None):
        self.blocking_calls += 1
        if self.error is not None:
            raise self.error
        time.sleep(self.delay_s)
        return self._store.ledger_stats(org_ids=org_ids)


class ThreadRecordingStore:
    """A store that remembers which OS thread every read happened on.

    `to_thread` is the whole of the fix for a blocking read, and a test that only checks the answer
    is correct cannot tell whether it was used. This asks the mechanical question instead: did any
    store call run on the thread that owns the event loop? The error path matters most, because an
    error handler written after the fact is where a `store.get_generation()` sneaks back onto the
    loop — one indexed read is enough to stall a poller behind a delivery write.
    """

    def __init__(self, store):
        self._store = store
        self.reads: list[tuple[str, int]] = []

    def __getattr__(self, name):
        attribute = getattr(self._store, name)
        if not callable(attribute):
            return attribute

        def recorded(*args, **kwargs):
            self.reads.append((name, threading.get_ident()))
            return attribute(*args, **kwargs)

        return recorded


class SlowKreuzakt:
    """An index that never answers, at longer than the tool's own budget."""

    def __init__(self, delay_s=10.0):
        self.delay_s = delay_s

    async def health(self):
        await asyncio.sleep(self.delay_s)
        return {"status": "ok"}

    async def queue_counts(self):
        await asyncio.sleep(self.delay_s)
        return {"pending": 0}


async def test_the_store_reads_do_not_block_the_event_loop(seeded):
    """The loop has to keep breathing while a bucket is counted (review I6).

    Twenty milliseconds of sleep per bucket against a heartbeat that ticks every ten: off-loop, the
    heartbeat runs about twenty times; on the loop, it runs once.
    """
    slow = BlockingStore(seeded, delay_s=0.2)
    ticks = 0

    async def heartbeat():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.01)

    beat = asyncio.create_task(heartbeat())
    try:
        await status_for(slow, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    finally:
        beat.cancel()

    assert slow.blocking_calls > 0, "the ledger was never read, so this test proved nothing"
    assert ticks > 3, f"the event loop ticked only {ticks} times during a blocking store read"


async def test_the_call_is_bounded_by_its_own_deadline(store):
    """The spec's per-call deadline, asserted on the call rather than trusted in prose (I6).

    Two sequential Kreuzakt reads at the client's own 30 s timeout would otherwise blow the 15 s
    budget for an all-buckets token, which is the one token an operator uses.
    """
    from papra_suite.mcp_tokens import resolve_token

    context = resolve_token(store, all_token(store))
    with pytest.raises(IndexUnavailable) as caught:
        await index_status_for(store, SlowKreuzakt(), context, deadline_s=0.05)

    message = str(caught.value)
    assert message.startswith("index_unavailable")
    assert "15" not in message or "0.05" in message   # names the budget it missed, not a bare retry
    assert len(message) <= 300


async def test_a_corrupt_generation_is_a_stable_code_and_not_an_internal_error(seeded, caplog):
    """The state `_index_line` already treats as expected-and-repairable (review I5).

    A corrupt `index_generation` makes every generation-scoped count a guess, and the store says so
    with a `ValueError` whose text is store internals. The agent gets the code and the repair; the
    internals stay in this process, and a *warning* — this is not a crash.
    """
    seeded.set_setting("index_generation", "rebuild-2026-11-17")
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(IndexUnavailable) as caught:
            await status_for(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))

    message = str(caught.value)
    assert message.startswith("index_unavailable")
    assert "ledger --invalidate" in message, "the repair has to be named, as `check` names it"
    assert "SELECT" not in message.upper() and "index_ledger" not in message
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR], \
        "an expected, repairable state must not arrive as a traceback"


async def test_even_the_error_path_reads_the_store_off_the_event_loop(seeded):
    """The `to_thread` rule has to hold on the path that runs *after* the deadline expired.

    `index_status_for` classifies a `ValueError` by asking the store whether the generation is
    corrupt, and that question is itself a store read — made from the handler, outside
    `wait_for`'s budget, on whatever thread the handler happens to run on, which is the event loop.
    A tool that refuses for being slow and then blocks the loop to work out why has fixed the wrong
    thing: the deadline protects the caller, the thread protects everyone else in the process.
    """
    loop_thread = threading.get_ident()
    # Resolved before the spy is fitted: the credential read is the auth path's business, and it has
    # its own test. This one is about the counts.
    context = resolve_token(seeded, scoped_token(seeded, SCOPED))
    assert context is not None
    spy = ThreadRecordingStore(seeded)
    seeded.set_setting("index_generation", "rebuild-2026-11-17")   # the expected, repairable state

    with pytest.raises(IndexUnavailable):
        await index_status_for(spy, FakeKreuzakt(), context)

    assert spy.reads, "the spy saw no read at all, so it would pass for the wrong reason"
    on_loop = sorted({name for name, ident in spy.reads if ident == loop_thread})
    assert not on_loop, f"these store reads ran on the event loop: {on_loop}"


async def test_a_store_error_names_the_type_and_no_query(seeded):
    """`sqlite3` messages quote SQL and column names; the client gets neither."""
    broken = BlockingStore(seeded, error=sqlite3.OperationalError(
        "database is locked: SELECT count(*) FROM index_ledger WHERE secret_note = 1"))
    with pytest.raises(IndexUnavailable) as caught:
        await status_for(broken, FakeKreuzakt(), scoped_token(seeded, SCOPED))

    message = str(caught.value)
    assert "OperationalError" in message
    assert "database is locked" not in message and "index_ledger" not in message


async def test_an_unexpected_error_never_reaches_the_agent_verbatim(seeded, caplog):
    """The SDK returns `str(exc)` as tool content, so anything unhandled is a message to the agent.

    The operator still gets the traceback — a swallowed programming error is a worse outage than a
    leaky one — but the client gets a bounded, stable code.
    """
    broken = BlockingStore(seeded, error=RuntimeError(
        "no such column: documents.secret_excerpt for org-9"))
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(IndexUnavailable) as caught:
            await status_for(broken, FakeKreuzakt(), scoped_token(seeded, SCOPED))

    message = str(caught.value)
    assert message.startswith("index_unavailable")
    assert "secret_excerpt" not in message and "org-9" not in message
    assert "RuntimeError" in message, "the class name is the bounded part an operator can grep"
    assert len(message) <= 300
    assert [r for r in caplog.records if r.levelno >= logging.ERROR], \
        "a bug nobody logs is a bug nobody fixes"


def test_the_wire_carries_the_stable_code_and_not_the_store(seeded):
    """The same promise, over the tool boundary: `isError`, one code, no internals."""
    token = scoped_token(seeded, SCOPED)
    broken = BlockingStore(seeded, error=sqlite3.OperationalError("database is locked"))
    response, message = wire_call(broken, token)

    result = message["result"]
    text = result["content"][0]["text"]
    assert response.status_code == 200
    assert result["isError"] is True
    assert "index_unavailable" in text
    assert "database is locked" not in text and "SELECT" not in text.upper()



# --- the oversized field: the reason an operator cannot get from the ledger ---------------


def seed_oversized(store, *documents, size=157286400, limit=104857600, org=OTHER):
    """Refusals written the way a pass writes them, in the bucket the scoped token cannot see."""
    from papra_suite.oversized import record_oversized_skip

    for index, document_id in enumerate(documents):
        record_oversized_skip(store, papra_org_id=org, document_id=document_id,
                              size=size + index, limit=limit,
                              now="2026-11-19T10:00:00+00:00")
    return store


async def test_index_status_carries_the_three_numbers(seeded):
    """Count, largest size, and the ceiling that refused them — the same three `check` prints.

    A document refused for size has no ledger row, so no bucket count can describe it: this field is
    the only way the reason reaches an agent at all. There is no dashboard yet, so `check` and here
    are the two surfaces, and they read one record through one function.
    """
    seed_oversized(seeded, "doc-huge")

    status = await status_for(seeded, FakeKreuzakt(), all_token(seeded))

    assert status.oversized is not None
    assert status.oversized.seen == 1
    assert status.oversized.largest == 157286400
    assert status.oversized.limit == 104857600
    assert status.oversized.seen_since == "2026-11-19T10:00:00+00:00"


async def test_the_oversized_field_dumps_no_documents(seeded):
    """Bounded on purpose: this goes to a model, and 400 refused scans are not a status.

    The count is the fact an agent can act on ("this corpus has material outside the index"); the
    ids are for the operator: `papra-suite check` and `backfill --dry-run` name them.
    """
    import json

    seed_oversized(seeded, *(f"doc-huge-{index}" for index in range(5)))

    status = await status_for(seeded, FakeKreuzakt(), all_token(seeded))

    payload = json.dumps(status.model_dump(mode="json"))
    assert "doc-huge" not in payload
    assert status.oversized.seen == 5


async def test_the_oversized_field_is_privileged(seeded):
    """The count is corpus-wide and read through a cap, so a bucket grant gets nothing.

    `None` rather than `0` for the same reason the queue depth is: `seen: 0` would read as "your
    buckets have no oversized documents", and the record cannot answer that — it does not know which
    buckets the refusals belonged to once the cap has evicted them.
    """
    seed_oversized(seeded, "doc-huge")

    scoped = await status_for(seeded, FakeKreuzakt(), scoped_token(seeded, SCOPED))
    everything = await status_for(seeded, FakeKreuzakt(), all_token(seeded))

    assert scoped.oversized is None
    assert everything.oversized is not None and everything.oversized.seen == 1


async def test_no_refusals_is_a_zero_count_with_no_ceiling(seeded):
    """`limit: null` because nothing was refused, so no ceiling was ever applied to anything."""
    status = await status_for(seeded, FakeKreuzakt(), all_token(seeded))

    assert status.oversized is not None
    assert status.oversized.seen == 0 and status.oversized.limit is None
    assert status.oversized.seen_since is None and status.oversized.largest == 0


async def test_the_oversized_field_survives_a_corrupt_record(seeded):
    """`index_status` answers, with zeros, rather than refusing the whole status over a report.

    The corrupt row is the store's problem to notice and `check`'s to show; a tool that will not
    answer counts because its diagnostic row is unparseable is a diagnostic that caused an outage.
    """
    from papra_suite.oversized import OVERSIZED_SETTING

    seeded.set_setting(OVERSIZED_SETTING, "{not json")

    status = await status_for(seeded, FakeKreuzakt(), all_token(seeded))

    assert status.oversized is not None and status.oversized.seen == 0
    assert status.buckets, "the rest of the status is still answered"
