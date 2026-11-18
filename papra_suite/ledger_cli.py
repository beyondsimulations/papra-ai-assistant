"""`papra-suite ledger` — the operator's door to the index ledger.

Five commands, and the reason they exist as commands rather than as SQLite incantations is
the same for all five: each one changes a state that other code derives its idea of "done"
from, and each one has a wrong way to run it that a person at a terminal can reach.

* `--status` reads: generation, references, current, stale, unledgered, delivery-blocked,
  cleanup debt, protection counts — the ledger part of `check`, on its own.
* `--reindex-all` (`--invalidate` is the same command) declares the current Kreuzakt ids
  meaningless and queues the corpus again through the intake path.
* `--requeue` is that queueing on its own, without the declaration: how an interrupted rebuild
  is finished, and how work a spent queue row used to swallow is asked for again.
* `--cleanup` settles the upstream deletes the retries gave up on.
* `--clear-marks` releases the rolling marks brake once the listing has been confirmed
  trustworthy again.

**What a rebuild is, stated once because every command here depends on it.** The ids it throws
away are integers in an index that has been wiped, and a rebuilt Kreuzakt numbers its
documents from 1, so the old integers are not stale facts about the same corpus — they are
facts about a different one. So nothing here copies an id forward, nothing here writes to
Papra, and nothing here concludes from `documents` that a reference is unnecessary. The
rebuild queues work into the same intake queue the sweep uses; the job that runs it re-derives
the id from Papra's bytes under the generation fence.

**The rebuild is a schedule, not a repair.** `reconcile_generation` does not index anything, it
enqueues. That is what makes it safe to interrupt, and therefore safe to run at all: the
generation is bumped before the work is queued, so a pass that dies halfway leaves the rest of
the corpus stale, already marked seen, and invisible to the sweep that would otherwise have
found it. Everything here therefore pages by keyset, re-runs on the same generation without
another bump, and refuses to report a half-walk as finished.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import sys
from dataclasses import dataclass

from papra_suite.indexing import (
    MARKS_ROLLING_SETTING,
    IndexFailed,
    papra_document_is_deleted,
    run_cleanup,
)
from papra_suite.kreuzakt_client import KreuzaktClient, KreuzaktError
from papra_suite.papra_client import PapraClient, PapraError
from papra_suite.pipeline import OCR_MAX_DOCUMENT_BYTES, _is_pdf, enqueue_index_job
from papra_suite.store import DELIVERY_MAX_FAILURES, Store
from papra_suite.store import _now as _store_now

logger = logging.getLogger("papra_suite.ledger_cli")

#: Exit codes, with the meaning `check`, `replay` and `metadata` already give them: `0` the
#: command did what was asked, `1` it stopped partway or refused an unconfirmed act, `2` it
#: would not start at all.
EXIT_OK = 0
EXIT_INCOMPLETE = 1
EXIT_REFUSED = 2

#: Rows per page for both rebuild walks. The number is arbitrary; the paging is not — see the
#: module docstring for why a rebuild that read the whole corpus at once could not resume.
RECONCILE_PAGE_SIZE = 100

PDF_MIME = "application/pdf"

#: What the enrolling walk writes into `documents.ingested_via` when the seen row carried
#: nothing usable: the command that created the reference, which is the provenance an operator
#: wants when the question is "why is this one in the index?".
RECONCILE_INGESTED_VIA = "reconcile"

@dataclass
class RebuildResult:
    """What one reconcile pass queued, left out, and could not finish.

    Field by field rather than as a pair of totals, because the populations are different
    decisions: `skipped_non_pdf` is a design boundary the operator has to be told about,
    `read_errors` is an instruction to run the command again, and `aborted` means somebody else
    started a newer index. Collapsing them into "processed N" is how a half-rebuilt corpus gets
    recorded as a rebuilt one.

    * `generation` — the epoch this pass scheduled against: the epoch every job it queued will
      bind under, and the epoch that stopped being current if `aborted` is set.
    * `stale` — references it found needing the current generation; `requeued` — how many it
      managed to queue. Equal unless the pass was interrupted.
    * `enrolled` — documents that had no reference at all and now have one, queued as well.
    * `unledgered` — what the anti-join still holds when the walk finished. This is the number
      the plan demands reach zero, and the three below are the reasons it legitimately may not.
    * `skipped_deleted` / `skipped_non_pdf` / `skipped_oversized` — candidates deliberately left
      out: Papra's trash belongs to the deletion pass, images are outside a PDF-only index, and
      a body over the resource limit is the reason this population was invisible to begin with.
    * `read_errors` — candidates Papra would not describe. Not a verdict about the document.
    * `enrolment_attempted` — whether there was a Papra client to ask at all. Without one the
      unledgered population is untouched, and that has to show in the result.
    * `aborted` — why the walk stopped early, or None.
    """

    generation: int
    stale: int = 0
    requeued: int = 0
    enrolled: int = 0
    unledgered: int = 0
    skipped_deleted: int = 0
    skipped_non_pdf: int = 0
    skipped_oversized: int = 0
    read_errors: int = 0
    enrolment_attempted: bool = False
    aborted: str | None = None

    @property
    def complete(self) -> bool:
        """May the operator record this corpus as rebuilt?

        Three things stop it: stopping early, a candidate that was never read, and — where there
        was no Papra client — a non-empty unledgered population. Documents left out on purpose do
        *not* make it incomplete: they are counted, named and explained, and `check` keeps
        printing them. Demanding zero there would either enrol PNGs in a PDF index or make an
        honest pass report failure forever.
        """
        if self.aborted is not None or self.read_errors:
            return False
        return not (self.unledgered and not self.enrolment_attempted)


def _enqueue_index(store: Store, *, org: str, document_id: str, file_hash: str, name: str,
                   mime: str, ingested_via: str) -> str:
    """Queue one document through the intake path, index job only.

    `legacy_ocr=False` is why this wrapper exists rather than calling `enqueue_index_job`
    directly. Reusing the intake path is the plan's rule — one path, so a rebuild cannot skip the
    reference write, the seen record, or the rename baseline — but that path also fills the legacy
    `ocr` queue while content delivery is off, and a rebuild is the one occasion where that is
    plainly wrong: now that spent rows no longer absorb repeats behind migration 004, a
    full-corpus rebuild would order one paid VLM extraction per document in order to rewrite a
    Papra column Papra already holds. The rebuild asks the index to re-derive the id; it does not
    ask for the PDFs to be read out a second time.
    """
    return enqueue_index_job(store, papra_org_id=org, document_id=document_id,
                             file_hash=file_hash, name=name, mime=mime,
                             ingested_via=ingested_via, legacy_ocr=False)

async def reconcile_generation(store: Store, *, papra=None,
                               page: int = RECONCILE_PAGE_SIZE) -> RebuildResult:
    """Queue everything the index should hold and does not, without writing to anything.

    Two walks, because there are two ways for a document to be missing and only one of them has
    ever had a name.

    **The stale references.** `ledger_needs_index` is the same predicate the sweep uses, so a
    rebuild asks for exactly the work the automatic passes would ask for — which is the point of
    the operation: it exists because that predicate was *true* for rows the queue would not
    accept (a terminal job holding the idempotency key), not because eligibility was misdefined.
    The hashes come from the ledger and Papra is not read at all here: the job verifies the bytes
    it downloads against this hash anyway, so reading Papra now would be a guess the job then
    checks, at a few hundred requests per corpus, on a path operators run while a service is
    unhappy.

    **The references that never existed.** `documents` says this suite ingested a document;
    `index_ledger` has never held it; every eligibility rule, count and cadence derives from the
    ledger, so that population is invisible from every other angle in the system — and the sweep
    skips it by name, because it skips documents it has already seen. Enrolling it means asking
    Papra what the document is, because the seen table stores no hash and a hash invented here
    fails permanently the moment the job downloads and verifies. So this is the walk that needs a
    Papra client, and without one it reports what it could not reach instead of pretending.

    Both walks page by keyset, and for the second one that is load-bearing in a way it is not for
    the others: enrolling a document *removes* it from the result set. An offset walk would skip
    the documents that slid into the gap its own work had opened, and finish green.

    `kz` is deliberately absent from this signature even though the plan's sketch had it: neither
    walk touches Kreuzakt — the rebuild's whole premise is that a Kreuzakt id is not a fact worth
    consulting — and a health gate here would be worse than useless, because resuming an
    interrupted queueing pass during an index outage is exactly when it is most needed. The gate
    belongs to the command that bumps the generation, and it is in `rebuild_index`.
    """
    generation = store.get_generation()
    result = RebuildResult(generation=generation)

    cursor: tuple[str, str] | None = None
    while True:
        refs = store.ledger_needs_index(generation=generation, after=cursor, limit=page)
        # Checked after the read rather than before the next one: a page fetched under a
        # generation that has already moved is a page about a corpus somebody else has reopened,
        # and queueing it would put work for the old epoch into the new one.
        moved = store.get_generation()
        if moved != generation:
            result.aborted = (
                f"the index generation moved from {generation} to {moved} mid-walk; "
                f"{result.requeued} of {result.stale} queued so far, re-run to finish")
            return _finish(store, result)
        if not refs:
            break
        result.stale += len(refs)
        for ref in refs:
            job_id = _enqueue_index(
                store, org=ref.papra_org_id, document_id=ref.papra_document_id,
                file_hash=ref.file_hash, name=ref.papra_original_name or ref.papra_document_id,
                mime=PDF_MIME, ingested_via=RECONCILE_INGESTED_VIA)
            if job_id:
                result.requeued += 1
            else:
                # A reference means this was enrolled as a PDF once, so `enqueue_index_job`
                # answering "" for a non-PDF can only mean the reference and the intake rule
                # disagree. Counted as left out, and named: a silent shortfall is the failure this
                # file exists to prevent.
                result.skipped_non_pdf += 1
                logger.warning("ledger reconcile: reference %s/%s carries no queueable pdf, "
                               "skipped", ref.papra_org_id, ref.papra_document_id)
        cursor = (refs[-1].papra_org_id, refs[-1].papra_document_id)

    if papra is None:
        # Not an error, and not silence either: the stale half of the rebuild has been queued,
        # and this says the other half was not, in the same breath as the count of it.
        logger.error("ledger reconcile: no papra client, so the unledgered population was not "
                     "enrolled — run `papra-suite ledger --reindex-all` with papra reachable "
                     "to finish it")
        return _finish(store, result)
    result.enrolment_attempted = True

    cursor = None
    while True:
        candidates = store.documents_without_reference(after=cursor, limit=page)
        moved = store.get_generation()
        if moved != generation:
            result.aborted = (
                f"the index generation moved from {generation} to {moved} while enrolling "
                f"unledgered documents; {result.enrolled} enrolled so far, re-run to finish")
            return _finish(store, result)
        if not candidates:
            break
        for row in candidates:
            await _enrol_one(store, papra, row, generation, result)
        cursor = (candidates[-1].papra_org_id, candidates[-1].papra_document_id)
    return _finish(store, result)


def _finish(store: Store, result: RebuildResult) -> RebuildResult:
    """Attach the remainder the operator is going to ask about, measured rather than inferred.

    `unledgered` is read back from the store instead of tallied from the walk's own buckets: the
    walk knows what it touched, only the anti-join knows what is left, and the number that has to
    be comparable with `check`'s line has to come from the same query `check` uses.
    """
    result.unledgered = store.ledger_stats()["unledgered"]
    return result

async def _enrol_one(store: Store, papra, row, generation: int,
                     result: RebuildResult) -> None:
    """Turn one seen-but-unledgered document into a reference plus queued work.

    Every early return is a document this pass is leaving out on purpose, and each one increments
    a bucket the CLI prints. The alternatives — enrolling anyway on a guessed hash, or counting
    the skip as enrolled to make the number tidy — trade a visible remainder for an invisible
    wrong one.

    The rename baseline comes from Papra's live name, which is right here and would be wrong for
    a stale reference: this document has no history to protect, so what Papra calls it now *is*
    its first-sighting name.
    """
    where = f"{row.papra_org_id}/{row.papra_document_id}"
    try:
        record = await papra.get_document(row.papra_org_id, row.papra_document_id)
    except PapraError as error:
        result.read_errors += 1
        logger.warning("ledger reconcile: papra would not describe %s (%s), left unledgered",
                       where, error)
        return
    if papra_document_is_deleted(record):
        result.skipped_deleted += 1
        logger.info("ledger reconcile: %s is in papra's trash, left to the deletion pass", where)
        return
    if not _is_pdf(record):
        result.skipped_non_pdf += 1
        logger.info("ledger reconcile: %s is %s, outside the pdf-only index",
                    where, record.get("mimeType") or row.mime or "an unknown type")
        return
    declared = record.get("originalSize")
    if declared is not None and declared > OCR_MAX_DOCUMENT_BYTES:
        # Refused on the size Papra declared, without fetching the body. The resource limit is
        # the reason this population was invisible in the first place, and paying the transfer to
        # rediscover it is how a rebuild becomes the incident it was meant to fix.
        result.skipped_oversized += 1
        logger.warning("ledger reconcile: %s is %s bytes, over the %s byte limit, not enrolled",
                       where, declared, OCR_MAX_DOCUMENT_BYTES)
        return
    file_hash = record.get("originalSha256Hash")
    if not file_hash:
        body = await papra.download_file(row.papra_org_id, row.papra_document_id)
        if len(body) > OCR_MAX_DOCUMENT_BYTES:
            result.skipped_oversized += 1
            logger.warning("ledger reconcile: %s downloaded %s bytes, over the %s byte limit, "
                           "not enrolled", where, len(body), OCR_MAX_DOCUMENT_BYTES)
            return
        file_hash = hashlib.sha256(body).hexdigest()
    job_id = _enqueue_index(
        store, org=row.papra_org_id, document_id=row.papra_document_id, file_hash=file_hash,
        name=record.get("name") or row.name or row.papra_document_id,
        mime=record.get("mimeType") or PDF_MIME,
        ingested_via=row.ingested_via or RECONCILE_INGESTED_VIA)
    if not job_id:
        result.skipped_non_pdf += 1
        return
    result.enrolled += 1
    logger.info("ledger reconcile: enrolled %s (generation %d) and queued its index job",
                where, generation)


async def rebuild_index(store: Store, kz: KreuzaktClient, *, papra=None,
                        page: int = RECONCILE_PAGE_SIZE) -> RebuildResult:
    """Throw the current ids away, then queue everything that needs new ones.

    **The health gate comes first, before any state changes.** `bump_generation` is a one-row
    UPDATE that makes every bound id in the corpus stale at once, and while Kreuzakt is not
    answering that does not look like a rebuild, it looks like an outage of search: the work gets
    queued, the queue drains into a dead index, and `check` reports the whole corpus as backlog
    while an operator is looking for the thing that broke. Refusing costs one retry after the
    index is back, and the queueing pass is resumable anyway.

    **One bump, then the queueing.** Bumping before queueing is what makes the operation atomic
    in the only sense available here — no half of the corpus can be judged against the old epoch
    while the other half is judged against the new — and the price is that the corpus stays stale
    and invisible to the sweep until the queueing finishes. That price is why the queueing is a
    separate re-runnable call on the same generation rather than something that has to work in
    one attempt.
    """
    try:
        health = await kz.health()
    except KreuzaktError as error:
        raise RuntimeError(
            f"kreuzakt is unreachable ({error}), so this would invalidate the index against "
            "nothing: bring the index back first, then re-run") from error
    status = health.get("status")
    if status != "ok":
        raise RuntimeError(
            f"kreuzakt reports status {status!r}, not 'ok': invalidating the index now turns a "
            "bad moment into an outage of search, because the re-queued work has nowhere to go")

    store.bump_generation()
    return await reconcile_generation(store, papra=papra, page=page)

def add_subparser(subparsers) -> None:
    """Register `ledger` on the entry point's subparser table."""
    parser = subparsers.add_parser(
        "ledger",
        help="read the index ledger, rebuild the index from papra's bytes (--reindex-all), "
             "settle failed upstream deletes (--cleanup), or release the marks brake "
             "(--clear-marks)",
    )
    intent = parser.add_mutually_exclusive_group(required=True)
    intent.add_argument("--status", action="store_true",
                        help="print the ledger's counts: generation, references, current, "
                             "stale, unledgered, delivery-blocked, cleanup debt, protection")
    intent.add_argument("--reindex-all", "--invalidate", dest="reindex_all",
                        action="store_true",
                        help="declare the current kreuzakt ids meaningless (bump the generation) "
                             "and queue the whole corpus again from papra's bytes. Needs "
                             "kreuzakt healthy and --yes-i-mean-it; interrupting it is safe, "
                             "re-running it resumes")
    intent.add_argument("--requeue", action="store_true",
                        help="queue what the current generation considers un-indexed, without "
                             "bumping the generation: resumes the queueing half of an "
                             "interrupted --reindex-all. Enqueues only")
    intent.add_argument("--cleanup", action="store_true",
                        help="settle the upstream deletes the retries gave up on: re-queue each "
                             "one with the kreuzakt id and generation it was originally queued "
                             "under, then run one cleanup pass. Needs papra and kreuzakt")
    intent.add_argument("--clear-marks", dest="clear_marks", action="store_true",
                        help=f"clear {MARKS_ROLLING_SETTING}, the rolling counter that can hold "
                             "deletion marking for a day. Ledger-only: needs neither service, so "
                             "it works while the index is down. Needs --yes-i-mean-it")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would change and change nothing")
    parser.add_argument("--yes-i-mean-it", dest="yes_i_mean_it", action="store_true",
                        help="confirm a command that releases a safety or discards history")


def run_ledger(config, args, *, store: Store | None = None, papra=None,
               kreuzakt=None) -> int:
    """Do what was asked, and name the state it touched. Returns the process exit code.

    `store`, `papra` and `kreuzakt` are injectable for the same reason the passes take their
    clients: the behaviour that matters here is what a command does when a service is
    unreachable, and that is not something to stage by unplugging a container.

    Only three of the five commands need the network at all, and which ones they are is the
    point: `--status` and `--clear-marks` work with both services down, because they are the
    commands an operator reaches for *during* an incident. The ones that need a service are the
    ones that would do something irreversible with its answer.
    """
    owns_store = store is None
    if owns_store:
        config.data_dir.mkdir(parents=True, exist_ok=True)
        store = Store(config.data_dir / "state.db")
    try:
        store.migrate()
        if args.status:
            return _status(store)
        if args.clear_marks:
            return _clear_marks(store, args)
        own_clients = papra is None or kreuzakt is None
        if own_clients:
            papra = papra or PapraClient(config.papra_base_url, config.papra_api_key)
            kreuzakt = kreuzakt or KreuzaktClient(config.kreuzakt_base_url)
        try:
            if args.requeue:
                return asyncio.run(_requeue(store, papra, args))
            if args.reindex_all:
                return asyncio.run(_reindex_all(store, kreuzakt, papra, args))
            return asyncio.run(_cleanup(store, papra, kreuzakt, args))
        finally:
            # Clients this function created are closed here, inside the loop that used them: a
            # transport belongs to the event loop its connections were made on, and `asyncio.run`
            # has closed that loop by the time it returns.
            if own_clients:
                asyncio.run(_aclose_both(papra, kreuzakt))
    except (ValueError, KreuzaktError, PapraError, IndexFailed) as error:
        # `get_generation`'s refusal, a store that will not answer, a client that could not be
        # built. "This will not start" is a different report from "I walked the corpus and one row
        # did not read", and it must not be counted as one of those.
        print(f"ledger refused: {error}", file=sys.stderr)
        return EXIT_REFUSED
    finally:
        if owns_store:
            store.close()


async def _aclose_both(papra, kreuzakt) -> None:
    for client in (papra, kreuzakt):
        close = getattr(client, "aclose", None)
        if close is not None:
            await close()


def _status(store: Store) -> int:
    """Print the ledger's own numbers. Never fails, because it decides nothing.

    Every line is a count the store answers exactly, from the same queries `check` and the passes
    use, so a number here cannot disagree with the same number there. `stale` is printed beside
    `references` deliberately: after a generation bump it equals the corpus, and the operator has
    to learn that that reading is correct before the rebuild, not during it.
    """
    stats = store.ledger_stats()
    print(f"generation: {store.get_generation()}")
    print(f"references: {stats['references']}")
    print(f"indexed current: {stats['indexed_current']}")
    print(f"stale: {stats['stale']}")
    print(f"unledgered: {stats['unledgered']}"
          + ("  (seen by this suite, never enrolled — --reindex-all enrolls them)"
             if stats["unledgered"] else ""))
    print(f"delivered: {stats['delivered']}")
    print(f"extraction empty: {stats['extraction_empty']}")
    print(f"delivery blocked: {stats['delivery_blocked']}"
          + (f"  (papra refused {DELIVERY_MAX_FAILURES} writes in a row; the log names them)"
             if stats["delivery_blocked"] else ""))
    print(f"repairs (24h): {stats['repairs_24h']}")
    print(f"cleanup pending: {stats['cleanup_pending']}")
    print(f"cleanup failed: {stats['cleanup_failed']}"
          + ("  (--cleanup settles these)" if stats["cleanup_failed"] else ""))
    print("protected: " + ", ".join(f"{stats[f'protected_{field}']} {field}"
                                    for field in ("name", "date", "notes")))
    marks = store.get_setting(MARKS_ROLLING_SETTING)
    print(f"{MARKS_ROLLING_SETTING}: {marks if marks else 'not set'}")
    return EXIT_OK

def _clear_marks(store: Store, args) -> int:
    """Release the rolling marks brake, naming the counter it clears.

    The brake exists because one bad listing read as an empty bucket would mark the corpus
    deleted: marks inside a rolling window above the allowance stop the pass. Clearing it is
    therefore releasing a safety, so the command prints what it releases — the window, the count,
    the allowance — and asks for `--yes-i-mean-it`, because the alternative is a safety people
    disable by typing whatever the help said without reading the number.

    The clear is a compare-and-swap against the value just read. Another pass may have rewritten
    the counter in between, and clearing whatever arrived instead of what was shown would reset
    the count of marks nobody has looked at.
    """
    value = store.get_setting(MARKS_ROLLING_SETTING)
    if value is None:
        print(f"{MARKS_ROLLING_SETTING}: not set, so nothing is holding deletion marking")
        return EXIT_OK
    print(f"{MARKS_ROLLING_SETTING} holds {value}")
    if args.dry_run:
        print("dry run: nothing cleared")
        return EXIT_OK
    if not args.yes_i_mean_it:
        print("this releases a safety: marks made inside this window stop counting toward the "
              "brake that halts deletion marking after a listing nobody has confirmed. Run with "
              "--yes-i-mean-it if the listing is trustworthy again")
        return EXIT_INCOMPLETE
    if not store.clear_setting(MARKS_ROLLING_SETTING, expected=value):
        print(f"nothing cleared: {MARKS_ROLLING_SETTING} changed under this command, so the value "
              "above is no longer the one that was there. Read it again", file=sys.stderr)
        return EXIT_INCOMPLETE
    print(f"cleared {MARKS_ROLLING_SETTING}: the window restarts and marks count against the "
          "allowance from zero")
    return EXIT_OK


def _report(result: RebuildResult) -> int:
    """Print one walk's verdict and turn it into an exit code.

    Shared because `--reindex-all` and `--requeue` must not tell two stories about the same walk:
    the remainder buckets are the point of the output, and a command that reported only its own
    successes is how a half-enrolled corpus gets recorded as a rebuilt one.
    """
    if result.stale or result.enrolled:
        print(f"generation {result.generation}: {result.requeued} of {result.stale} stale "
              f"reference(s) queued, {result.enrolled} unledgered document(s) enrolled")
    print("left out of the index on purpose: "
          f"{result.skipped_deleted} in papra's trash, {result.skipped_non_pdf} not a pdf, "
          f"{result.skipped_oversized} over the {OCR_MAX_DOCUMENT_BYTES} byte limit")
    if result.unledgered:
        print(f"still unledgered: {result.unledgered} — the populations above, plus anything "
              "papra would not describe")
    if result.read_errors:
        print(f"{result.read_errors} document(s) papra would not describe: run the command again",
              file=sys.stderr)
    if result.aborted:
        print(f"stopped early: {result.aborted}", file=sys.stderr)
        return EXIT_INCOMPLETE
    return EXIT_OK if result.complete else EXIT_INCOMPLETE


async def _requeue(store: Store, papra, args) -> int:
    """Queue what the current generation calls un-indexed, and bump nothing.

    The other half of `--reindex-all`, and it has to be a separate command because the two differ
    in what they destroy: `--reindex-all` declares every bound id meaningless, this one declares
    nothing and only asks the queue. That distinction is what makes an interrupted rebuild
    recoverable — the bump happened once, the work behind it did not all get queued, and re-running
    the invalidation to finish the job would invalidate the part that already succeeded.

    It is also the answer to the older complaint this task came from: work that was asked for and
    swallowed by a terminal row holding the idempotency key. With nothing stale it queues nothing
    and says so, which is the reply an operator needs rather than silence.
    """
    stats = store.ledger_stats()
    generation = store.get_generation()
    if args.dry_run:
        print(f"dry run: generation {generation} would be left alone; would queue "
              f"{stats['stale']} stale reference(s) and try {stats['unledgered']} unledgered "
              "document(s)")
        return EXIT_OK
    result = await reconcile_generation(store, papra=papra)
    if not result.stale and not result.enrolled:
        print(f"generation {result.generation}: nothing is stale, so nothing was queued "
              f"({stats['indexed_current']} of {stats['references']} reference(s) are indexed "
              "under it)")
    return _report(result)


async def _reindex_all(store: Store, kreuzakt: KreuzaktClient, papra, args) -> int:
    """Confirm, gate, bump, queue. In that order, because each step is dearer than the last.

    The confirmation names the cost in the currency the operator is spending: queue entries, and,
    if the index no longer holds any of these documents, one extraction per PDF. It does not
    promise the rebuild is quick, because nobody knows how quick it is until the runbook entry is
    measured — see the README's rebuild section.
    """
    generation = store.get_generation()
    stats = store.ledger_stats()
    scope = (f"{stats['references']} reference(s) and {stats['unledgered']} unledgered "
             "document(s)")
    if args.dry_run:
        print(f"dry run: would check kreuzakt health, bump the generation from {generation}, and "
              f"re-queue {scope} through the intake queue")
        return EXIT_OK
    if not args.yes_i_mean_it:
        print(f"this declares the {scope} of generation {generation} no longer indexed: every "
              "kreuzakt id in the ledger stops resolving, and the corpus is re-indexed from "
              "papra's bytes. It costs one kreuzakt queue entry per document — and one text "
              "extraction per pdf the rebuilt index does not already hold. Interrupting it is "
              "safe: `papra-suite ledger --requeue` finishes the queueing without a second bump. "
              "Run with --yes-i-mean-it")
        return EXIT_INCOMPLETE
    try:
        result = await rebuild_index(store, kreuzakt, papra=papra)
    except RuntimeError as error:
        print(f"ledger refused: {error}", file=sys.stderr)
        return EXIT_REFUSED
    return _report(result)


async def _cleanup(store: Store, papra, kreuzakt: KreuzaktClient, args) -> int:
    """Settle the deletes the retries stopped on, then run one pass over the debt.

    Two halves, and the first is the part the daemon's own pass cannot do: a row in
    `cleanup_state='failed'` is not due, so no automatic pass will look at it again, and the
    operator's only remaining lever would be an UPDATE by hand. Re-queueing does two things the
    daemon cannot do for itself — it refills the attempt allowance, which is what the `failed`
    state was earned by spending, and it does so per row, under that row's own
    `cleanup_kreuzakt_id` and `cleanup_generation`.

    **Re-queued under the generation it was recorded in, never the current one.** After a rebuild
    the same integer may name different content, and re-stamping it would convert an old id into
    permission to delete whatever holds that number now. The pass's own revalidation decides from
    there, and for a stale-generation intent it decides to skip — which settles the row as a
    recorded skip rather than as debt, and is reported here as such.

    `expected_state='failed'` is the fence on the reset: a row that stopped being `failed` between
    the read and the write — the daemon's pass took it, a restore closed it — is not the row this
    command was answering, and a second helping of attempts has to be refused rather than granted.
    """
    settled = 0
    stale_generation = 0
    cursor: tuple[str, str] | None = None
    while True:
        page = store.ledger_cleanup_failed(after=cursor, limit=RECONCILE_PAGE_SIZE)
        if not page:
            break
        for ref in page:
            cursor = (ref.papra_org_id, ref.papra_document_id)
            if ref.cleanup_kreuzakt_id is None:
                # Queued with no id to delete, which `ledger_queue_cleanup` refuses to store and
                # the pass would skip anyway. Reported rather than re-queued: there is no intent
                # here to retry, only a row to explain. Printed in both modes: a rehearsal that
                # quietly omitted a row the real run would refuse is a rehearsal nobody trusts.
                print(f"cleanup: {ref.papra_org_id}/{ref.papra_document_id} names no kreuzakt id "
                      "— nothing to retry")
                continue
            if ref.cleanup_generation != store.get_generation():
                stale_generation += 1
            if args.dry_run:
                # Counted, not written. A `--dry-run` that re-queued would leave the row pending
                # with a full allowance and print a sentence about what it had not done, which is
                # the one thing a rehearsal is not allowed to be: a run.
                settled += 1
                continue
            if store.ledger_queue_cleanup(
                    papra_org_id=ref.papra_org_id, papra_document_id=ref.papra_document_id,
                    due_at=_store_now(), kreuzakt_id=ref.cleanup_kreuzakt_id,
                    generation=ref.cleanup_generation, reset_attempts=True,
                    expected_state="failed"):
                settled += 1
        cursor = (page[-1].papra_org_id, page[-1].papra_document_id)

    if args.dry_run:
        print(f"dry run: {settled} delete(s) in cleanup_state 'failed' would be re-queued with "
              "their original kreuzakt id and generation, then one cleanup pass would run"
              + (f"; {stale_generation} of them belong to an older generation and would be "
                 "settled as skipped rather than deleted" if stale_generation else ""))
        return EXIT_OK
    print(f"cleanup: re-queued {settled} delete(s) from cleanup_state 'failed', each with its "
          "original kreuzakt id and generation"
          + (f"; {stale_generation} of them belong to an older generation, so the pass settles "
             "them as skipped rather than deleting" if stale_generation else ""))
    result = await run_cleanup(store, papra, kreuzakt)
    print(f"cleanup pass: {result.deleted} deleted, {result.deferred} skipped or deferred, "
          f"{result.failed} failed again ({result.attempted} judged)")
    return EXIT_INCOMPLETE if result.failed else EXIT_OK
