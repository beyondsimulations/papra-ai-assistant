"""`papra-suite backfill` — the staged corpus enqueue, with the ledger as the referee.

This is the command that enrols the documents the sweep will never visit. `documents` says this
suite ingested them, `index_ledger` never held them, and since every count, cadence and eligibility
rule in the system reads the ledger, that population is invisible from every other angle: the PDFs
emailed before the handover, and the oversized skips the sweep recorded as seen and never indexed.
It is also the command that slices a corpus into the index on purpose — `--limit 20`, read `check`,
then more — which is how the atlas run is meant to be done.

**Eligibility is the ledger's, never `document_seen`.** The seen flag records *ingestion*, so after
a generation bump every document in the corpus would look already-done and the command would report
a finished backfill over an index holding nothing. A document is work when the current generation
has no usable reference for it — the same predicate `ledger_needs_index` implements — and the flag
`--all-stale` walks that predicate from the ledger itself, for references Papra's listing does not
hand this walk.

**Enrolling obeys the intake ordering, because it is the same call.** `enqueue_index_job` writes
the ledger reference *before* the job becomes visible, then the index job (and the legacy `ocr`
job while content delivery is off), then the seen record last. A queued job with no row behind it
is work no
sweep, rebuild or later backfill can see — the document would be enqueued once and stay invisible
forever — so this command does not implement its own enqueue.

**A remainder is printed, never implied.** `--limit 5` over 12 candidates prints `7 remaining`, and
the walk keeps listing after the cap so that number is measured rather than guessed. Stopping at the
cap would leave the rest unknown, and an unknown remainder printed as silence is how a half-enrolled
corpus reads as a finished one. Non-PDFs, Papra's trash and documents over the size limit are
counted
and named in their own buckets: they are not remaining work, and they are not a failure either.

**Enqueuing is the whole extent of it.** No Papra write, no Kreuzakt call: the work is done by the
worker that claims the row, under the generation fence. That is also what makes it safe to run while
a service is unhappy — with the exception of the listing itself, which is read from Papra and whose
refusal is reported as an unfinished walk rather than as an empty corpus.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import sqlite3
import sys
from dataclasses import dataclass, field

from papra_suite.indexing import papra_document_is_deleted
from papra_suite.jobs_cli import LIVE_STATUSES
from papra_suite.papra_client import PapraClient
from papra_suite.pipeline import (
    OCR_MAX_DOCUMENT_BYTES,
    _is_pdf,
    enqueue_index_job,
    legacy_writers_enabled,
)
from papra_suite.store import Store

logger = logging.getLogger("papra_suite.backfill_cli")

#: Exit codes, with the meaning the other operator commands give them: `0` the walk finished,
#: `1` it stopped or could not read part of the corpus, `2` it would not start at all.
EXIT_OK = 0
EXIT_INCOMPLETE = 1
EXIT_REFUSED = 2

#: Rows per listing page. The sweep's cadence, so a backfill and a sweep ask Papra the same
#: question the same way.
LISTING_PAGE_SIZE = 100
# Queue rows one document can hold in one kind since migration 004. Read as a yes/no, not a
# page: a document with more live rows than this is a queue nobody is backfilling anyway.
DOC_ROW_LIMIT = 100

PDF_MIME = "application/pdf"

#: Provenance written into `documents.ingested_via` for a document this command enrols. When the
#: question is "why is this in the index?", the answer is the command that put it there.
BACKFILL_INGESTED_VIA = "backfill"

#: The four verdicts the ledger can give about one document, mirroring `ledger_needs_index` exactly.
CURRENT = "current"
STALE = "stale"
UNLEDGERED = "unledgered"
DELETED = "deleted"


@dataclass
class BackfillResult:
    """What one backfill run queued, left out, and could not read.

    Field by field rather than as totals, because the populations are different decisions the
    operator has to be able to tell apart: `enrolled` is the invisible population this command
    exists for, `skipped_non_pdf` is a design boundary, `unreadable` is an instruction to run it
    again, and `remaining` is the reason to run it again at all. Collapsing them into "processed N"
    is how a half-enrolled corpus gets recorded as a backfilled one.

    * `listed` — documents Papra returned, of any type: the walk's reach.
    * `candidates` — the PDFs among them that are neither trashed nor non-PDF: what "of M" counts.
    * `enqueued` — documents this run asked for: `new_rows` plus `already_queued`. A document a live
      row already carried was still asked for, so it belongs to this population even though it
      changed nothing. The invariant is `enqueued == new_rows + already_queued`, so the two are
      equal only when nothing was absorbed — were they equal always, a run over a stalled queue
      would print the same pair as a run over an empty corpus.
    * `new_rows` — of those, the ones that took a fresh queue row: the only number `--limit` caps,
      and the total `enrolled` and `stale_queued` partition.
    * `already_queued` — of those, the ones a live row already carried, so nothing changed. Counted
      apart because an absorbed request is not progress: were it to spend the cap, a stalled worker
      would make every limited run re-report the same head of the queue, and the rest would never be
      reached.
    * `enrolled` — of those, the ones that had no ledger reference at all before this run.
    * `stale_queued` — of those, the ones the ledger held under a generation that is not current.
    * `remaining` — eligible documents this run did not queue because of `--limit`. The number that
      says whether to run again, measured by walking the rest of the corpus rather than assumed.
    * `unreadable` — buckets Papra would not list, which makes the walk incomplete.
    """

    generation: int
    orgs_queued: set[str] = field(default_factory=set)
    listed: int = 0
    candidates: int = 0
    enqueued: int = 0
    new_rows: int = 0
    already_queued: int = 0
    enrolled: int = 0
    stale_queued: int = 0
    already_current: int = 0
    skipped_non_pdf: int = 0
    skipped_deleted: int = 0
    skipped_oversized: int = 0
    needs_download: int = 0
    remaining: int = 0
    unreadable: list[str] = field(default_factory=list)
    read_errors: list[str] = field(default_factory=list)
    aborted: str | None = None

    @property
    def complete(self) -> bool:
        """May the operator record this corpus as backfilled?

        Stopping at the cap does not make a run incomplete — that is what `--limit` asks for — but a
        bucket nobody read does, because its documents are neither queued nor counted.
        """
        return self.aborted is None and not self.unreadable


def _classify(store: Store, org: str, document_id: str, generation: int) -> str:
    """One document's standing with the ledger, using `ledger_needs_index`'s own rule.

    The predicate lives in the store and every pass — sweep, rebuild, verification — reads it from
    there, so a backfill that invented its own eligibility would be a second definition of "done"
    and the two would diverge in production, silently. A reference bound under the current
    generation is finished work; anything else is work, and a soft-deleted reference belongs to the
    deletion pass rather than to anybody here.
    """
    ref = store.ledger_get(org, document_id)
    if ref is None:
        return UNLEDGERED
    if ref.deleted_at is not None:
        return DELETED
    if ref.kreuzakt_id is None or ref.delivered_generation != generation:
        return STALE
    return CURRENT


def add_subparser(subparsers) -> None:
    """Register `backfill` on the entry point's subparser table."""
    parser = subparsers.add_parser(
        "backfill",
        help="queue documents the index does not currently hold: the enrolment path for pdfs the "
             "sweep will never visit, and the bounded way to re-queue a corpus",
    )
    parser.add_argument("--org-id", default=None,
                        help="only this papra organization (default: every organization this "
                             "store knows, from `orgs` and from the ledger)")
    parser.add_argument("--limit", type=int, default=None,
                        help="take at most this many new queue rows in one run — a document a "
                             "live row already carries is absorbed and does not spend the cap — "
                             "and print how many are still eligible (default: no cap — the walk "
                             "still counts them)")
    parser.add_argument("--dry-run", action="store_true",
                        help="count what would be queued and change nothing: no job, no ledger "
                             "row, no seen record. Prints the cost before it is paid")
    parser.add_argument("--all-stale", dest="all_stale", action="store_true",
                        help="also queue every reference the current generation calls un-indexed, "
                             "walked from the ledger instead of from papra's listing — for "
                             "references the listing does not hand this command")


@dataclass
class _Budget:
    """How many documents this run may still queue.

    One object rather than a bare counter, because the same decision — "is there room for this
    one?" — is taken by the listing walk and by the ledger walk, and a cap that the second walk
    forgot to honour would queue more than the operator asked for.
    """

    limit: int | None
    used: int = 0

    def allows(self) -> bool:
        return self.limit is None or self.used < self.limit

    def spend(self) -> None:
        self.used += 1


def _moved(store: Store, generation: int) -> str | None:
    """Has somebody reopened the index underneath this walk?

    A page read under a generation that is no longer current is a page about a corpus somebody else
    has re-opened: queueing it would spend the new index's work allowance on the old one's verdicts.
    Checked once per page — the same cadence the rebuild walks at — because a per-document read
    would cost a query per document on a path that already costs a listing per hundred.
    """
    current = store.get_generation()
    if current == generation:
        return None
    return (f"the index generation moved from {generation} to {current} mid-walk; "
            "re-run this command to queue what is left under the new generation")


def _already_queued(store: Store, document_id: str, content_hash: str) -> bool:
    """Is this exact work sitting in the queue right now?

    Read before the enqueue, because afterwards it is always true: `enqueue_job` names the live row
    that already carries the key instead of taking a second one. The answer decides whether this
    document is progress or a repeat, which decides whether it spends the `--limit` cap.
    """
    rows = store.list_jobs(kind="index", document_id=document_id, limit=DOC_ROW_LIMIT)
    return any(row.status in LIVE_STATUSES and row.content_hash == content_hash for row in rows)


def _queue(store: Store, org: str, document_id: str, *, file_hash: str, name: str,
           standing: str, budget: _Budget, result: BackfillResult, dry_run: bool) -> None:
    """Queue one document through the intake path — the only enqueue this command has.

    `enqueue_index_job` is the whole point of calling it: reference first, then the index job (and
    the legacy `ocr` job while the flags say Papra's text still needs that writer), then the seen
    record. A backfill with its own enqueue would be a second intake path, and the ordering rule it
    skipped is the one that keeps an enrolled document visible afterwards.
    """
    if _already_queued(store, document_id, file_hash):
        # Asked for, and already carried: it counts in `enqueued` beside the documents that took a
        # row, while `new_rows` and the cap stay where they were — an absorbed request is not
        # progress.
        result.enqueued += 1
        result.already_queued += 1
        logger.debug("backfill: %s/%s is already queued, nothing changed", org, document_id)
        return
    if dry_run:
        result.enqueued += 1
        result.new_rows += 1
        result.orgs_queued.add(org)
        budget.spend()        # the rehearsal caps itself the way the run does
        if standing == UNLEDGERED:
            result.enrolled += 1
        else:
            result.stale_queued += 1
        return
    job_id = enqueue_index_job(store, papra_org_id=org, document_id=document_id,
                               file_hash=file_hash, name=name, mime=PDF_MIME,
                               ingested_via=BACKFILL_INGESTED_VIA)
    if not job_id:
        # `_is_pdf` accepted the name while the intake path refuses the mime: the two rules
        # disagree about this document, and a silent shortfall is the failure this command exists to
        # prevent. Counted where it can be printed.
        result.skipped_non_pdf += 1
        logger.warning("backfill: %s/%s was listed as a pdf but the intake path refused it, "
                       "left out of this run", org, document_id)
        return
    budget.spend()
    result.enqueued += 1
    result.new_rows += 1
    result.orgs_queued.add(org)
    if standing == UNLEDGERED:
        result.enrolled += 1
    else:
        result.stale_queued += 1
    logger.info("backfill: queued %s/%s (%s) against generation %d", org, document_id, standing,
                result.generation)


async def _consider(store: Store, papra, org: str, document: dict, generation: int,
                    budget: _Budget, decided: set, result: BackfillResult, *,
                    dry_run: bool) -> None:
    """One listing row, one verdict, counted in the bucket that says why.

    The order of the tests is the order of their cost: Papra's trash and non-PDFs are decided from
    the row itself, the ledger answers for the rest, and the only download in this function is the
    fallback for a server that does not publish `originalSha256Hash` — the sweep's fallback, for the
    sweep's reason (the hash on the job has to be Papra's fact about its bytes, or the job fails
    permanently the moment it verifies the download).
    """
    document_id = document.get("id")
    if not document_id:
        result.skipped_non_pdf += 1
        logger.warning("backfill: a listing row in %s carried no document id, skipped", org)
        return
    result.listed += 1
    key = (org, document_id)
    if key in decided:
        return
    decided.add(key)
    if papra_document_is_deleted(document):
        result.skipped_deleted += 1
        logger.info("backfill: %s/%s is in papra's trash, left to the deletion pass", org,
                    document_id)
        return
    if not _is_pdf(document):
        result.skipped_non_pdf += 1
        logger.debug("backfill: %s/%s is %s, outside the pdf-only index", org, document_id,
                     document.get("mimeType") or "an unknown type")
        return

    result.candidates += 1
    standing = _classify(store, org, document_id, generation)
    if standing == DELETED:
        result.skipped_deleted += 1
        logger.info("backfill: %s/%s is queued for deletion in the ledger, left alone", org,
                    document_id)
        return
    if standing == CURRENT:
        result.already_current += 1
        return
    declared = document.get("originalSize")
    if declared is not None and declared > OCR_MAX_DOCUMENT_BYTES:
        # Refused on the size Papra declared, without fetching the body: this limit is the reason
        # the population was invisible in the first place, and paying the transfer to rediscover it
        # is how a backfill becomes the incident it was meant to fix.
        result.skipped_oversized += 1
        logger.warning("backfill: %s/%s is %s bytes, over the %s byte limit, not queued", org,
                       document_id, declared, OCR_MAX_DOCUMENT_BYTES)
        return

    file_hash = document.get("originalSha256Hash")
    if not file_hash:
        result.needs_download += 1
        if not dry_run and budget.allows():
            body = await papra.download_file(org, document_id)
            if len(body) > OCR_MAX_DOCUMENT_BYTES:
                result.skipped_oversized += 1
                logger.warning("backfill: %s/%s downloaded %s bytes, over the %s byte limit, not "
                               "queued", org, document_id, len(body), OCR_MAX_DOCUMENT_BYTES)
                return
            file_hash = hashlib.sha256(body).hexdigest()
    if not budget.allows():
        result.remaining += 1
        return
    _queue(store, org, document_id, file_hash=file_hash or "",
           name=document.get("name") or document_id, standing=standing, budget=budget,
           result=result, dry_run=dry_run)


async def _walk_listing(store: Store, papra, org: str, generation: int, budget: _Budget,
                        decided: set, result: BackfillResult, *, page: int,
                        dry_run: bool) -> str | None:
    """Page through one bucket until a short page ends it. Returns why it stopped early.

    Pagination is by page index because enqueuing never creates a Papra document, so the listing is
    stable underneath the walk — the same assumption the sweep makes, and the reason a document
    enrolled here does not shift the pages the way a rebuild's keyset walk has to avoid.
    """
    page_index = 0
    while True:
        try:
            documents, _declared_total = await papra.list_documents(
                org, page_index=page_index, page_size=page,
                sort_field="createdAt", sort_order="desc")
        except Exception as error:  # noqa: BLE001 - one bucket must not end the corpus
            result.unreadable.append(org)
            result.read_errors.append(f"{org}: {type(error).__name__}: {error}")
            logger.error("backfill: papra would not list %s (%s), so that bucket was not walked — "
                         "run the command again once it answers", org, error)
            return None
        moved = _moved(store, generation)
        if moved:
            return moved
        for document in documents:
            await _consider(store, papra, org, document, generation, budget, decided, result,
                            dry_run=dry_run)
        if len(documents) < page:
            return None
        page_index += 1


def _queue_stale_references(store: Store, org: str, generation: int, budget: _Budget,
                            decided: set, result: BackfillResult, *, page: int,
                            dry_run: bool) -> str | None:
    """`--all-stale`: the ledger's own worklist, read from the ledger.

    The population is references that are un-indexed for the current generation but that Papra's
    listing did not hand this walk — a bucket whose listing changed underneath the paging, a
    reference enrolled by a restore, a document enrolled by an earlier run whose job then failed.
    Their hash comes from the reference, so Papra is not asked at all: the job that runs verifies
    the bytes it downloads against that hash, which makes a fresh read a guess the job then checks.

    Keyset paging is load-bearing here and not in the listing walk: enqueuing does not make a
    reference stop needing an index, so an offset walk would re-read the page it just queued and
    never reach the ones after it.
    """
    cursor: tuple[str, str] | None = None
    while True:
        refs = store.ledger_needs_index(generation=generation, org_id=org, after=cursor,
                                        limit=max(1, min(page, 1000)))
        moved = _moved(store, generation)
        if moved:
            return moved
        if not refs:
            return None
        for ref in refs:
            key = (ref.papra_org_id, ref.papra_document_id)
            if key in decided:
                continue        # the listing walk already counted and queued this one
            decided.add(key)
            result.candidates += 1
            if not budget.allows():
                result.remaining += 1
                continue
            _queue(store, ref.papra_org_id, ref.papra_document_id, file_hash=ref.file_hash,
                   name=ref.papra_original_name or ref.papra_document_id, standing=STALE,
                   budget=budget, result=result, dry_run=dry_run)
        cursor = (refs[-1].papra_org_id, refs[-1].papra_document_id)


async def backfill_corpus(store: Store, papra, *, orgs: list[str], limit: int | None = None,
                          all_stale: bool = False, page: int = LISTING_PAGE_SIZE,
                          dry_run: bool = False) -> BackfillResult:
    """Queue everything the index should hold and does not, in the buckets this store knows.

    Two walks per bucket, because there are two ways to be missing: Papra's listing (the documents
    this suite has never enrolled, which is where the pre-handover mailings live) and the ledger
    (the references that are enrolled but un-indexed for this generation, which `--all-stale`
    walks). `decided` is what keeps them one run: a document the listing walk counted is not
    counted a second time by the ledger walk, and a remainder printed twice would be a remainder
    that never empties.

    `dry_run` performs neither walk's writes: no job, no reference, no seen record, and no
    downloads either — a rehearsal that transfers the corpus to count it is not a rehearsal.
    """
    generation = store.get_generation()
    result = BackfillResult(generation=generation)
    budget = _Budget(limit)
    decided: set[tuple[str, str]] = set()
    for org in orgs:
        stopped = await _walk_listing(store, papra, org, generation, budget, decided, result,
                                      page=page, dry_run=dry_run)
        if stopped:
            result.aborted = stopped
            return result
        if all_stale:
            stopped = _queue_stale_references(store, org, generation, budget, decided, result,
                                              page=page, dry_run=dry_run)
            if stopped:
                result.aborted = stopped
                return result
    return result


def _known_orgs(store: Store) -> list[str]:
    """Every bucket this store has ever heard of: the configured ones plus the ledger's.

    A bucket can leave `orgs` — an operator removing it, a store copied without it — while its
    references and their Kreuzakt documents remain, and a scope built only from configured mailboxes
    would leave those documents un-back-filled and silent. Papra is not asked for a bucket list: the
    API key is scoped to what it may read, and `--org-id` is the way to name one this store forgot.
    """
    known = {org.papra_org_id for org in store.list_orgs()}
    known.update(store.ledger_org_ids())
    return sorted(known)


def _cost_line(store: Store) -> str:
    """What the queued work will cost, read from the same two flags `serve` seeds.

    Said out loud before the operator commits to it, because it is money and it is the difference
    between this command and `ledger --requeue`: while the bridge does not own Papra's `content`
    column, the intake path also fills the legacy `ocr` queue, which is one paid VLM extraction per
    document. That is the point when Papra's text was extracted with the wrong OCR languages, and it
    is not the point when it was not.
    """
    if store.settings_flag("index_content_delivery"):
        return ("cost: one kreuzakt queue entry per document — no vlm extraction, index content "
                "delivery is on, so the bridge writes papra's text itself")
    if not legacy_writers_enabled(store):
        return ("cost: one kreuzakt queue entry per document — no legacy `ocr` job is queued, the "
                "legacy workers are off, so papra's text will NOT change")
    return ("cost: one kreuzakt queue entry AND one legacy `ocr` job per document — that is a paid "
            "vlm extraction each, and it is what rewrites papra's text. `ledger --requeue` is the "
            "command that queues the index without it")


def _report(store: Store, args, result: BackfillResult) -> int:
    """Print what one run queued, left out and could not read, and turn it into an exit code.

    The remainder line comes last on purpose: it is the sentence that decides what the operator does
    next, and a summary that ends with its own successes reads as finished.
    """
    verb = "would enqueue" if args.dry_run else "enqueued"
    # Two different numbers belong on this line, so each is named by what it counts. The leading one
    # is what the run changed (new queue rows: the only thing `--limit` caps, the only thing an
    # operator can act on), because leading with the asked-for count is how a run over a stalled
    # queue reads as progress. `asked for` is `enqueued` — new rows plus what a live row had already
    # taken — decomposed in the parenthesis so the totals add up under the eye.
    head = (f"backfill: {verb} {result.new_rows} new row(s) across "
            f"{len(result.orgs_queued)} bucket(s); asked for {result.enqueued} of "
            f"{result.candidates} candidate(s)"
            + (f" ({result.already_queued} already queued, unchanged)"
               if result.already_queued else "")
            + f"; {result.remaining} remaining")
    print(head)
    print(f"  {result.listed} document(s) listed; {result.already_current} already current in the "
          f"ledger, {result.skipped_non_pdf} not a pdf, {result.skipped_deleted} in papra's trash "
          "or queued for deletion, "
          f"{result.skipped_oversized} over the {OCR_MAX_DOCUMENT_BYTES} byte limit")
    if result.enrolled or result.stale_queued:
        # `new_rows`, not `enqueued`: `enrolled` and `stale_queued` partition the rows this run
        # took, and an absorbed document belongs to neither bucket.
        print(f"  of those {result.new_rows} new row(s): {result.enrolled} had no ledger reference "
              f"at all and are enrolled now, {result.stale_queued} were references the current "
              "generation calls un-indexed")
    if result.needs_download:
        print(f"  {result.needs_download} candidate(s) carry no content hash in the listing: each "
              "is downloaded and hashed before it is queued, and refused if it is over the limit"
              + (" (a dry run does not download them)" if args.dry_run else ""))
    print("  " + _cost_line(store))
    if args.dry_run:
        print("  dry run: nothing was queued, no ledger row was written and no document was marked "
              "seen")
    if result.already_queued:
        print(f"  {result.already_queued} candidate(s) were already queued under a live row and "
              "were left alone — the cap counts new rows, so this run went on to the next "
              "documents instead of spending itself on work already in the queue")
    if result.remaining:
        capped = (f"this run stopped at --limit {args.limit}" if args.limit is not None
                  else "this run stopped early")
        print(f"{result.remaining} candidate(s) are still eligible — {capped}. Run "
              "`papra-suite backfill` again for the next slice: the ledger decides what is left, "
              "so repeating it cannot duplicate the work")
    for line in result.read_errors:
        print(f"could not read: {line}", file=sys.stderr)
    if result.aborted:
        print(f"stopped early: {result.aborted}", file=sys.stderr)
        return EXIT_INCOMPLETE
    return EXIT_OK if result.complete else EXIT_INCOMPLETE


async def _execute(store: Store, papra, args, orgs, to_close) -> int:
    """Walk, report, then close the clients on the loop that used them.

    The `finally` is inside the coroutine, so it runs before `asyncio.run` closes the loop — the
    only place an `httpx` transport may be closed — and its own failures are swallowed into a
    WARNING.
    That is a decision: a socket that would not shut down cleanly says nothing about whether the
    corpus was queued, and letting it propagate would put a `finally` between the operator and the
    answer they asked for, which is how a run that really did enqueue the work reports failure.
    """
    try:
        result = await backfill_corpus(store, papra, orgs=orgs, limit=args.limit,
                                       all_stale=args.all_stale, dry_run=args.dry_run)
        return _report(store, args, result)
    finally:
        for client in to_close:
            try:
                await client.aclose()
            except Exception as error:  # noqa: BLE001 - the command's result stands
                logger.warning("backfill: a client did not close cleanly (%s: %s) — the command's "
                               "result stands; the socket goes with the process",
                               type(error).__name__, error)


def run_backfill(config, args, *, store: Store | None = None, papra=None) -> int:
    """Do what was asked of the corpus. Returns the process exit code.

    `store` and `papra` are injectable for the same reason the passes take their clients: the
    behaviour that matters here is what the command does when Papra will not answer, or when the
    walk is interrupted, and neither is something to stage by unplugging a container.

    Unlike `jobs`, this command migrates: it writes ledger references, jobs and seen records, and a
    store left behind by an older build would otherwise fail halfway through a corpus. `--dry-run`
    migrates too, exactly as `ledger --dry-run` does — what a rehearsal promises is that no queue,
    ledger or seen state changes, not that no statement is ever executed.

    Kreuzakt is never contacted: this command queues work and the worker that claims it talks to the
    index. That is what makes it safe to run during an index outage, which is when a backfill is
    most often wanted.
    """
    if args.limit is not None and args.limit < 1:
        print(f"--limit wants at least 1 document, got {args.limit}", file=sys.stderr)
        print("queue nothing by hand instead: there is no run that enqueues zero and means it",
              file=sys.stderr)
        return EXIT_REFUSED

    owns_store = store is None
    if owns_store:
        # Before `Store()` is constructed, because constructing it creates the file: an operator
        # pointing at the wrong `DATA_DIR` came to find out why the corpus is not indexed, and an
        # empty database on the way past turns "wrong path" into "nothing to do".
        db_path = config.data_dir / "state.db"
        if not db_path.exists():
            print(f"no database at {db_path}", file=sys.stderr)
            print("run `papra-suite check` first; nothing to backfill", file=sys.stderr)
            return EXIT_REFUSED
        store = Store(db_path)
    try:
        store.migrate()
        orgs = [args.org_id] if args.org_id else _known_orgs(store)
        if not orgs:
            print("no organization to walk: this store holds no `orgs` row and no `index_ledger` "
                  "row, so it does not know of a bucket to list", file=sys.stderr)
            print("name one: `papra-suite backfill --org-id ID` (the id papra prints in its own "
                  "URL), or run `papra-suite check` against the deployment that knows them",
                  file=sys.stderr)
            return EXIT_REFUSED
        owns_papra = papra is None
        if owns_papra:
            papra = PapraClient(config.papra_base_url, config.papra_api_key)
        to_close = [papra] if owns_papra and getattr(papra, "aclose", None) is not None else []
        return asyncio.run(_execute(store, papra, args, orgs, to_close))
    except (ValueError, sqlite3.Error) as error:
        # The store's own integrity refusals (`get_generation` on a corrupt `index_generation`) and
        # a database that is locked or busy. "This will not start" is a different report from "I
        # walked part of the corpus and one bucket did not read", and it must not be counted as one.
        print(f"backfill refused: {error}", file=sys.stderr)
        return EXIT_REFUSED
    except KeyboardInterrupt:
        # Ctrl-C in the middle of a corpus is the expected way to stop, and everything already
        # queued stays queued: the reference-before-job ordering means no document was enrolled
        # halfway. The instruction that follows an interruption is therefore the same one the
        # remainder line already prints.
        print("backfill stopped: interrupted. Whatever was queued stays queued and is safe to "
              "leave; run the same command again and the ledger decides what is left",
              file=sys.stderr)
        return EXIT_INCOMPLETE
    finally:
        if owns_store:
            store.close()
