"""The record of documents this suite refused because they were too big.

**Why a record has to exist at all.** A document over `PAPRA_MAX_DOCUMENT_BYTES` is refused *before
it is enrolled*: no `index_ledger` row, no job, no `documents` sighting that means anything (the
sweep marks it seen precisely so it will not be retried). There is therefore nothing for a count to
read — the population is invisible by construction, which is how the pre-handover corpus could sit
outside the index with nothing in any status line saying so. This module is that missing row: it is
written where the skip happens, so the reason survives the pass that made it.

Three bounds, because a "helpful" unbounded diagnostic is its own outage:

* **Capped listing, oldest-first eviction.** One settings row, at most `OVERSIZED_RECORD_LIMIT`
  documents in it. A corpus of 400 scans must not grow a settings value without end or put 400
  document ids on an operator's line.
* **The count is not the listing.** `count` counts the refusals; `documents` lists the ones still
  held. Deriving the number from a capped list is how a report can only ever shorten, which reads as
  "the problem got better" when the list merely filled up.
* **It is a floor, never a census.** "N oversized seen since <pass>" is what the passes walked.
  Every string here and in its callers keeps that wording: a line saying "your corpus has N"
  claims knowledge of documents nobody listed, and this suite has no catalogue of the corpus except
  the walks it performed.

**Why the entry leaves at enrollment.** The operator's remedy for a 400 MB scan is to compress it
and re-upload to Papra, or to raise `PAPRA_MAX_DOCUMENT_BYTES` and re-run the backfill. Either way
the document is enrolled and indexed, and from that moment it is not oversized. `enqueue_index_job`
— the intake path every writer shares — removes the entry, because a reason that outlives its
problem is worse than no reason: it points an operator at a document that is already fixed.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from papra_suite.store import _now as _store_now
from papra_suite.store import sanitize_event_text

logger = logging.getLogger(__name__)

#: The `settings` row this whole record lives in.
OVERSIZED_SETTING = "oversized_seen"

#: How many documents the row lists. Deliberately not "all of them": the row is read by `check` and
#: by the MCP `index_status` tool, so its size is paid for on every one of those reads, and a total
#: outage of uploads (a scan backlog) would otherwise be an unbounded settings value that an agent
#: can ask for. 50 is comfortably more than an operator can act on in one sitting and far below the
#: point where the row costs anything to read.
OVERSIZED_RECORD_LIMIT = 50

#: Document and bucket ids come from Papra's listing, which is somebody else's text. Bounded and
#: control-screened with the house sanitizer before they are stored, so the cap above is a bound on
#: bytes and not merely on entries.
_ID_MAX_CHARS = 128

#: What an id that sanitizes to nothing is stored as. Empty would collide with a real empty id, and
#: the record is the only place an operator can still see which document this was.
_UNKNOWN_ID = "?"


@dataclass(frozen=True)
class OversizedDoc:
    """One refused document: which bucket, which document, how big it was measured."""

    org_id: str
    document_id: str
    size: int


@dataclass(frozen=True)
class OversizedSeen:
    """What the record holds, exactly as it was recorded.

    `since` is when this record began — the first refusal since it was last empty — and it is what
    makes `count` an honest number: "N seen since then", not "N in your corpus".

    `limit` is the ceiling that refused them, and it is `None` only when nothing has been refused.
    It is the ceiling in force *at the time of the refusals*: `record_oversized_skip` starts a new
    record when the limit changes, so the entries and the number printed beside them can never
    describe two different rules (150 MB "oversized" beside a 500 MB ceiling reads as a bug, and it
    would be one — those documents are not oversized any more).

    `largest` is the biggest refusal, and it is a running figure rather than `max(size for ...)`
    over `documents`: the largest document is exactly the one most likely to have been evicted from
    the capped listing, and "largest size seen" that quietly reports a smaller number is the
    confident wrong answer.
    """

    since: str | None
    limit: int | None
    count: int
    largest: int
    documents: tuple[OversizedDoc, ...]

    @property
    def evicted(self) -> int:
        """How many refused documents the cap dropped from the listing (none from the count)."""
        return max(0, self.count - len(self.documents))


EMPTY = OversizedSeen(since=None, limit=None, count=0, largest=0, documents=())


def _clean_id(value: object) -> str:
    return sanitize_event_text(value, limit=_ID_MAX_CHARS) or _UNKNOWN_ID


def _entries(document: dict) -> tuple[OversizedDoc, ...] | None:
    """The `docs` list, or None when any element of it is not one.

    All-or-nothing on purpose, and the caller drops the whole record on None: a count printed beside
    a listing that failed to parse is two numbers that disagree, and this record has no authority to
    guess which part of a broken row was meant to be real.
    """
    entries = []
    for item in document["docs"]:
        if (not isinstance(item, list) or len(item) != 3
                or not isinstance(item[0], str) or not isinstance(item[1], str)
                or isinstance(item[2], bool) or not isinstance(item[2], int)):
            return None
        entries.append(OversizedDoc(org_id=item[0], document_id=item[1], size=item[2]))
    return tuple(entries)


def oversized_seen(store) -> OversizedSeen:
    """What has been refused, read from the record. Never raises, never invents.

    Takes anything with `get_setting` — `Store` in the writer paths, `LedgerReader` on the MCP
    request path — which is how `check` and `index_status` read the same row through the same code
    and cannot disagree about a number. The reader exposes `get_setting` already, so this needs no
    new door on the containment surface.

    A row that is not JSON, or is JSON of the wrong shape, is reported as no record with a warning
    naming the key. This is a report, not a gate: nothing decides anything with it, so the honest
    reading of an unreadable row is "I have no record" rather than a crash in the command the
    operator runs to find out what is wrong.
    """
    raw = store.get_setting(OVERSIZED_SETTING)
    if not raw:
        return EMPTY
    try:
        document = json.loads(raw)
    except ValueError:
        logger.warning("%s is not readable and is reported as no record at all", OVERSIZED_SETTING)
        return EMPTY
    if not isinstance(document, dict) or set(document) != {"since", "limit", "largest", "count",
                                                           "docs"}:
        logger.warning("%s is not a record this build writes, so it is reported as no record",
                       OVERSIZED_SETTING)
        return EMPTY
    since, limit, largest, count, docs = (document["since"], document["limit"],
                                          document["largest"], document["count"], document["docs"])
    for value in (largest, count):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            logger.warning("%s carries a count this program cannot have written; reported as no "
                           "record", OVERSIZED_SETTING)
            return EMPTY
    if not isinstance(docs, list):
        logger.warning("%s carries a listing that is not a list; reported as no record",
                       OVERSIZED_SETTING)
        return EMPTY
    if (since is not None and not isinstance(since, str)) or limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int)):
        logger.warning("%s names a ceiling or a start this program cannot have written; reported "
                       "as no record", OVERSIZED_SETTING)
        return EMPTY
    documents = _entries(document)
    if documents is None:
        logger.warning("%s carries a listing this program cannot read; reported as no record",
                       OVERSIZED_SETTING)
        return EMPTY
    return OversizedSeen(since=since, limit=limit, count=count, largest=largest,
                         documents=documents)


def _write(store, record: OversizedSeen) -> None:
    store.set_setting(OVERSIZED_SETTING, json.dumps(
        {"since": record.since, "limit": record.limit, "largest": record.largest,
         "count": record.count,
         "docs": [[doc.org_id, doc.document_id, doc.size] for doc in record.documents]},
        separators=(",", ":"), sort_keys=True))


def record_oversized_skip(store, *, papra_org_id: str, document_id: str, size: int, limit: int,
                          now: str | None = None) -> None:
    """Say that one document was refused, and by which ceiling.

    Called at the moment of the skip, from each of the three enrollment paths (the sweep, the
    backfill walk, `ledger` reconcile), and nowhere else: a record rebuilt at read time would need
    the ledger rows these documents do not have.

    A repeat sighting of a document already listed updates its size rather than counting it twice —
    the sweep, a backfill and a reconcile can each walk the same scan, and three counts of one
    document would triple the only number `check` prints. The size is updated because Papra's number
    is the measurement and the newest one is the truest.

    A different `limit` starts the record again. Those are different claims about the corpus, and
    mixing them is how "3 oversized" comes to mean "3 documents that were too big at some point".
    """
    org = _clean_id(papra_org_id)
    document = _clean_id(document_id)
    size = int(size)
    limit = int(limit)
    existing = oversized_seen(store)
    if existing.limit is not None and existing.limit != limit:
        logger.info("the document ceiling moved from %d to %d bytes, so the record of refused "
                    "documents starts again: what is left of the old list was refused under the "
                    "ceiling that no longer applies", existing.limit, limit)
        existing = EMPTY
    key = (org, document)
    listed = [doc for doc in existing.documents if (doc.org_id, doc.document_id) != key]
    repeated = len(listed) != len(existing.documents)
    listed = listed + [OversizedDoc(org_id=org, document_id=document, size=size)]
    kept = listed[-OVERSIZED_RECORD_LIMIT:]
    if len(listed) > len(kept):
        # The sound of a backlog: documents are still being counted, they are just not listed. This
        # is logged because the alternative is a report that goes quiet while the problem grows.
        logger.warning("%s lists at most %d refused document(s), so %d older refusal(s) "
                       "are counted "
                       "but not named", OVERSIZED_SETTING, OVERSIZED_RECORD_LIMIT,
                       len(listed) - len(kept))
    _write(store, OversizedSeen(
        since=existing.since or (now or _store_now()),
        limit=limit,
        largest=max(existing.largest, size),
        # A repeat of a document that was already evicted from the listing counts again: the record
        # cannot know it is the same document, and this is a count of refusals seen, not a census.
        count=existing.count if repeated else existing.count + 1,
        documents=tuple(kept)))


def clear_oversized_skip(store, *, papra_org_id: str, document_id: str) -> bool:
    """Drop one document from the record because it has been enrolled. False when it was not listed.

    False is the ordinary answer — every document that was never refused for size takes this path on
    its way through `enqueue_index_job` — so it must not write, and it does not: an untouched row is
    the difference between "this enrollment changed the report" and a settings write per document.

    When the last entry leaves, the row is deleted rather than zeroed, and deleted with
    `clear_setting(expected=...)`: a skip recorded between the read above and this write is a
    document that is still oversized, and clobbering it would erase a live reason.
    """
    raw = store.get_setting(OVERSIZED_SETTING)
    if not raw:
        return False
    record = oversized_seen(store)
    org, document = _clean_id(papra_org_id), _clean_id(document_id)
    kept = tuple(doc for doc in record.documents
                 if (doc.org_id, doc.document_id) != (org, document))
    if len(kept) == len(record.documents):
        return False
    count = max(0, record.count - 1)
    if not kept and count == 0:
        store.clear_setting(OVERSIZED_SETTING, expected=raw)
        return True
    _write(store, OversizedSeen(since=record.since, limit=record.limit, largest=record.largest,
                                count=count, documents=kept))
    return True
