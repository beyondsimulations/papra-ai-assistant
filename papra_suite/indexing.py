"""The `index` job kind: put Papra's bytes into Kreuzakt, correlate, record the id.

This module is the whole of phase one, and phase one deliberately writes nothing
back into Papra. The upload, the correlation and the ledger binding are exercised
for a full phase before a single `PATCH` exists, so a bug here can only cost a
wasted upload — never a corrupted record. Content delivery arrives in Task 7
(behind a gate inside `deliver_content`, not only here) and metadata in Task 8.

Three measured rules shape the code below; all three come from
`docs/superpowers/probes/2026-11-17-kreuzakt-contracts/capture-queue.json`, and
each is a case where the obvious implementation is wrong:

* **Correlation is serial and immediate.** The queue window is the newest 100 rows
  with no pagination, so the mark is taken before the upload and the row is waited
  for before the next upload starts. A batched worker is a bet that nobody else
  wrote 90 rows in between.
* **`status == "completed"` is the only completion signal.** `document_id` being
  non-null proves nothing — `reindex-all` creates its rows with the id already
  filled in, having processed nothing.
* **A completed row with a null `document_id` is a vanished document**, not a
  transient anomaly: deleting a Kreuzakt document leaves its queue rows
  `completed` with the id nulled. Re-running the row is not even possible
  (`retry` answers `Queue entry 31 is not in failed status`), so the branch has to
  read Papra's own state to decide what to do.

The upload name is synthetic — `papra-<documentId>-<hash8>.pdf`, alphabet
`[a-z0-9.-]` — because it is the join key between a queue row and a Papra
document, and it must survive Kreuzakt's filename sanitising byte-identically.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from dataclasses import dataclass

from papra_suite.kreuzakt_client import (
    AmbiguousCorrelation,
    CorrelationWindowFull,
    KreuzaktError,
    KzDocument,
)
from papra_suite.store import Job, LedgerRef, Store

logger = logging.getLogger(__name__)

# Characters Kreuzakt stores verbatim. Measured: anything else comes back
# sanitised (`papra:doc0005<…>.pdf` → `papra_doc0005_…_.pdf`), and the client
# matches queue rows by exact equality on purpose, so a renamed upload is a
# document we can no longer find in the queue.
_UNSAFE = re.compile(r"[^a-z0-9.-]+")
_SEPARATORS = re.compile(r"[-.]{2,}")

# Longest stem we will put in a filename verbatim. A filename is one path
# component and Kreuzakt writes it to disk, so a document id long enough to push
# past a filesystem's component limit would fail the upload rather than the
# correlation — and silently truncating would let two documents that share a
# prefix collide onto one name. Past this length the id is hashed instead:
# deterministic, and unique by construction rather than by luck.
_MAX_VERBATIM_STEM = 96
_HASH_STEM = 24


def _stem(value: str) -> str:
    """The safe, bounded form of one identifier for the upload name."""
    text = (value or "").lower()
    collapsed = _SEPARATORS.sub("-", _UNSAFE.sub("-", text)).strip("-.")
    if len(collapsed) > _MAX_VERBATIM_STEM or not collapsed:
        # Empty falls here too on purpose: `papra--a1b2c3d4.pdf` would be a name
        # that cannot be traced back to any document.
        collapsed = "h" + hashlib.sha256(text.encode()).hexdigest()[:_HASH_STEM]
    return collapsed


def index_upload_name(job: Job) -> str:
    """The name this job's bytes are uploaded under: `papra-<docId>-<hash8>.pdf`.

    Deterministic, so a re-upload of the same document lands on the same name and
    Kreuzakt's dedup keeps one document, and self-describing, so a row in the
    queue can be traced to a Papra document by eye. The alphabet is `[a-z0-9.-]`
    plus `.pdf` and nothing else — see `_stem`.
    """
    stem = f"papra-{_stem(job.document_id)}-{_stem(job.content_hash)[:8]}"
    return f"{stem}.pdf"


# --- the handler ------------------------------------------------------------------

# Whether THIS build can write Kreuzakt's content into Papra. Task 7 writes
# `deliver_content` and sets this True; until that function exists, a build that
# claimed the flag would stop the one component producing Papra's text today and
# put nothing in its place. `serve` reads this when it seeds the setting, so the
# flag cannot be turned on ahead of the code that honours it.
CONTENT_DELIVERY_IMPLEMENTED = False

DEFAULT_POLL_S = 2.0
DEFAULT_WAIT_TIMEOUT_S = 900.0
#: The ceiling on attempts at the JOB, across runs, which is what
#: `PAPRA_INDEX_MAX_ATTEMPTS` means and what `Store.mark_job_failed` spends to decide
#: when a row becomes `failed` for good. `worker_loop` takes it as its own parameter
#: and the handler takes it to know when the budget is spent; the number of recovery
#: actions one run may take is `DEFAULT_INDEX_RECOVERY_ACTIONS`, and passing one as the
#: other is how a configured 5 bought three attempts while a configured 1 bought three.
DEFAULT_INDEX_MAX_ATTEMPTS = 3
#: How many recovery actions one run of a job may take: re-uploads and row re-runs
#: together. Bounded by the shape of the repair rather than by taste — every recovery
#: action here ends in an upload or a re-run, so a row that stays broken costs three
#: round trips and then reports itself. That is what keeps a permanent error from being
#: paid for three times inside one job attempt.
DEFAULT_INDEX_RECOVERY_ACTIONS = 3

# Task 5's client reports a completed row whose `document_id` was nulled as a plain
# `KreuzaktError`, not a type of its own, so recognising the vanished-document case
# means recognising its wording. That is a real coupling, so it is a named constant
# and a test (`test_the_vanished_row_error_is_recognised_from_the_real_client`) that
# drives the real client: if the wording moves, that test fails rather than this
# branch quietly stopping ever firing.
_VANISHED_ROW_MARKER = "completed without document_id"


@dataclass(frozen=True)
class IndexOutcome:
    """What one index attempt produced. `delivered` is always False in this phase.

    `empty_content` distinguishes "Kreuzakt extracted nothing" from the reference
    existing at all: the binding is recorded either way, because the document is
    indexed and findable by id, and the emptiness is reported rather than
    interpreted — only its owner can tell a blank scan from an empty document.
    """

    kreuzakt_id: int
    content_len: int
    empty_content: bool
    delivered: bool


class IndexFailed(Exception):
    """This job cannot proceed. `permanent` says a retry cannot help either.

    The worker reads the attribute the same way it reads a client's 4xx status, so
    a job whose document has been deleted in Papra or whose source bytes moved on
    fails once instead of riding the backoff three times for the same answer.
    """

    def __init__(self, message: str, *, permanent: bool = False) -> None:
        super().__init__(message)
        self.permanent = permanent


class IndexTimeout(Exception):
    """This job's correlation budget ran out while it still had an attempt left.

    `wait_timeout_s` is one cumulative budget for the job's upload-and-correlate
    sequence rather than one per attempt, and it covers the bytes fetch that precedes
    each upload. Two limits on what it claims:

    * it is a budget checked between operations, not a cancellation of one. An upload
      already in flight can overrun it; the job stops at the next checkpoint rather
      than mid-request. Individual requests are bounded separately, by the client's own
      transport timeout.
    * it is not a handler-wide deadline. The opening Papra read and the closing
      `get_document` stay under that same transport bound.

    Distinct from the client's "no queue row within Ns": that one is about a single
    wait and is the worker's to back off. This one exists because attempts multiply a
    wait — 3 against a 900 s timeout would hold the only index worker for 45 minutes on
    one document while every other document waited behind it. Retriable, like a
    timeout.
    """


def _clock() -> float:
    """Monotonic time behind a name, so the wall-clock budget is testable without waiting."""
    return time.monotonic()


def _is_vanished_row_error(error: KreuzaktError) -> bool:
    """Is this the client's "completed, but the document is gone" error?"""
    return _VANISHED_ROW_MARKER in str(error)


async def _read_binding(kz, document_id: int) -> KzDocument | None:
    """Read the document a binding names, with a 404 answered as "nothing there".

    A binding is a claim about one index, and that index can stop honouring it:
    measured, a document DELETE leaves our ledger row pointing at an integer that
    answers 404, and a read landing while the index is mid-restart answers the same
    way for a document that is perfectly there. Neither is the fact `worker_loop`'s
    4xx rule reads — "a retry cannot help" — and letting the client's error through
    would leave the row bound under the current generation, which is exactly the state
    `ledger_needs_index` excludes: the document would never be indexed again while it
    sits in Papra looking handled. So the 404 comes back as None and the caller
    decides, which is the only place that knows whether it is re-uploading or
    re-queueing.
    """
    try:
        return await kz.get_document(document_id)
    except KreuzaktError as error:
        if error.status != 404:
            raise
        return None


def _outcome(store: Store, job: Job, kreuzakt_id: int, kz_doc: KzDocument) -> IndexOutcome:
    """Report what the index holds, and refuse out loud when a switch has no code."""
    empty = not kz_doc.content.strip()
    if empty:
        # Reported, not interpreted: "extraction-empty" is a fact about the index,
        # "legitimately blank" is a fact about the document, and nothing here can
        # establish the second one.
        logger.info("extraction-empty papra=%s kreuzakt=%s", job.document_id, kreuzakt_id)
    delivered = False
    if store.settings_flag("index_content_delivery"):
        # Task 7 writes `deliver_content`, gate included. Until that function
        # exists the honest answer to an enabled flag is a loud refusal: saying
        # nothing and returning `delivered=True` would report a write we did not
        # make, and saying nothing silently would leave an operator believing the
        # switch had been thrown.
        logger.warning(
            "content delivery is enabled but this build has no delivery path yet: "
            "papra=%s kreuzakt=%s left untouched", job.document_id, kreuzakt_id)
    return IndexOutcome(kreuzakt_id=kreuzakt_id, content_len=len(kz_doc.content),
                        empty_content=empty, delivered=delivered)


def _require_budget(deadline: float, wait_timeout_s: float, job: Job, attempts_done: int,
                    reason: Exception | None) -> None:
    """Stop before starting work this job no longer has time to finish.

    Checked twice per attempt: before it starts, so a job with no budget left does not
    upload a document it will not correlate, and again after the upload, because the
    download in between is the part whose duration nobody controls.

    Exhaustion has two distinct faces, on purpose. This one is `IndexTimeout`: the job's
    own budget is gone, a fact about the job. A wait that runs out mid-flight is the
    client's `KreuzaktError` about a queue row, a fact about Kreuzakt, and the worker
    backs off from both without either one being described as the other.
    """
    if deadline - _clock() > 0:
        return
    raise IndexTimeout(
        f"index wait budget of {wait_timeout_s}s spent after {attempts_done} "
        f"attempt(s) on {job.document_id}"
        + (f": last reason was {reason}" if reason is not None else "")
        + "; retriable, the job is re-queued with backoff")


async def download_original(papra, job: Job) -> bytes:
    """Papra's own bytes, always, and always the ones this job was queued for.

    There is deliberately no "reuse what Kreuzakt has" branch: the index may be
    wiped, restored from an older backup, or holding a different document under
    this name, and Papra is the record. That is also what makes a re-upload free of
    side effects — the bytes are the same bytes, so dedup lands on one document.

    The bytes are hashed here rather than trusted from the record read at the start
    of the job, because the two reads are not the same moment: the metadata the job
    was queued on is a fact from job start, and the body arrives later. A document
    replaced in between would otherwise have its NEW bytes indexed while the ledger
    stamps the OLD `content_hash` — a wrong document behind a right-looking row that
    never heals, since the id is bound under the current generation and the document
    counts as seen. A transfer that stopped short is the same defect with a different
    cause, and it gets the same permanent failure — but that is a fail-closed choice,
    not a claim about the transport: some short transfers would succeed on a second
    try. What decides it is that the alternative is uploading bytes we cannot name, and
    a retry here downloads from the same body we just refused. Recovery is a fresh job
    for the bytes Papra actually holds — `jobs --reset` today, and Task 11's
    re-schedulable queue after it — not this job.
    """
    blob = await papra.download_file(job.papra_org_id, job.document_id)
    digest = hashlib.sha256(blob).hexdigest()
    if digest != job.content_hash:
        raise IndexFailed(
            f"papra://{job.papra_org_id}/{job.document_id} sent {len(blob)} bytes that "
            f"hash to {digest}, not the {job.content_hash} this job was queued for: the "
            "document moved under the job or the transfer stopped short, and none of it "
            "went up",
            permanent=True)
    return blob


async def _papra_holds_document(papra, job: Job) -> bool:
    """Does the record still hold this document? Papra answers, we do not infer.

    A 404 is an answer. A 5xx or a timeout is not: concluding "the user deleted it"
    from an unreachable service would hand a live document to the cleanup path.
    """
    from papra_suite.papra_client import PapraError

    try:
        document = await papra.get_document(job.papra_org_id, job.document_id)
    except PapraError as error:
        if error.status == 404:
            return False
        raise
    return not (document.get("isDeleted") or document.get("deletedAt"))


def _first_sight(store: Store, job: Job, document: dict) -> LedgerRef:
    """Create the ledger reference at the first moment we hold Papra's document.

    `papra_original_name` is the baseline the rename rule (Task 8) measures
    ownership against, and it can only be captured now: later, a name we wrote and
    a name the user typed are indistinguishable. An absent name stays None — with
    `""` as the baseline, "the current name equals what Papra had at first sight"
    would be true of any document renamed away from a blank, and we would take
    that as licence to rename a document we never named.
    """
    store.ledger_upsert_reference(
        papra_org_id=job.papra_org_id, papra_document_id=job.document_id,
        file_hash=job.content_hash, papra_original_name=document.get("name") or None,
    )
    ref = store.ledger_get(job.papra_org_id, job.document_id)
    if ref is None:
        raise IndexFailed(f"ledger reference for {job.document_id} could not be created")
    return ref


async def run_index_job(kz, store: Store, job: Job, *, papra,
                        poll_s: float = DEFAULT_POLL_S,
                        wait_timeout_s: float = DEFAULT_WAIT_TIMEOUT_S,
                        max_attempts: int = DEFAULT_INDEX_MAX_ATTEMPTS,
                        max_recovery_actions: int = DEFAULT_INDEX_RECOVERY_ACTIONS
                        ) -> IndexOutcome:
    """Upload one Papra document to Kreuzakt, find its queue row, record the id.

    `generation` is read once, here, and everything this job binds is stamped and
    fenced with that one value: a rebuild that lands mid-flight has to win, because
    an id from the destroyed index is an integer that resolves to nothing.

    Eligibility is the ledger's, never the seen-table's: an id bound under the
    current generation means done, anything else is work. `document_seen` records
    that we ingested something, which is exactly the fact a rebuild invalidates.

    A binding the index answers 404 for is neither of those. While the job has attempts
    left, both places we can meet it end in work rather than a verdict — see
    `_read_binding`; the one attempt that has no attempt behind it is the documented
    residual in `_upload_and_correlate`'s caller.
    """
    generation = store.get_generation()

    document = await papra.get_document(job.papra_org_id, job.document_id)
    if document.get("isDeleted") or document.get("deletedAt"):
        raise IndexFailed(f"document deleted in papra: {job.document_id}", permanent=True)
    current_hash = document.get("originalSha256Hash") or job.content_hash
    if current_hash != job.content_hash:
        # The bytes we would upload are not the bytes this job was queued for, and a
        # newer job exists for the newer content. Uploading anyway would index a
        # version of the record that no longer exists.
        raise IndexFailed(
            f"source hash changed; job abandoned: papra holds {current_hash} for "
            f"{job.document_id}, this job carries {job.content_hash}", permanent=True)

    ref = store.ledger_get(job.papra_org_id, job.document_id) or _first_sight(store, job,
                                                                              document)

    if ref.kreuzakt_id is not None and ref.delivered_generation == generation:
        kreuzakt_id = ref.kreuzakt_id
        bound = await _read_binding(kz, kreuzakt_id)
        if bound is not None:
            return _outcome(store, job, kreuzakt_id, bound)     # nothing to upload
        # A binding that answers 404 is not a finished document, and stopping here
        # would say it was: the row stays bound under the current generation, which is
        # the state `ledger_needs_index` excludes, so the sweep would never look again
        # while Papra holds a document nothing has indexed. Kreuzakt deduplicates on
        # content, so the fix is the upload we already know how to do — it replaces the
        # id in place and costs one request for bytes the index already digested.
        logger.warning(
            "kreuzakt document %s is gone behind the binding for %s: re-uploading and "
            "rebinding in place", kreuzakt_id, job.document_id)

    kreuzakt_id = await _upload_and_correlate(
        kz, store, job, papra=papra, generation=generation, poll_s=poll_s,
        wait_timeout_s=wait_timeout_s, max_attempts=max_attempts,
        max_recovery_actions=max_recovery_actions)

    bound = await _read_binding(kz, kreuzakt_id)
    if bound is None:
        # The bind is kept and the job is re-queued: while attempts remain, the next
        # run either reads the document (a restart that has passed) or takes the
        # stale-binding path above and uploads again. A 404 straight off our own upload
        # is therefore a spent attempt rather than a verdict, which is what the client's
        # error, with its status on it, would have said.
        #
        # What that does NOT buy, stated because the ledger is where it shows: on the
        # attempt that exhausts the ceiling, the row is left bound under the current
        # generation without the document ever having been read, and
        # `ledger_needs_index` excludes exactly that state. Nothing in the sweep or a
        # rebuild will look at it again; the recovery is `jobs --reset`, and Task 11's
        # migration is what turns that into a path rather than an intervention. A
        # binding that was never verified is not worth a new Store API in a follow-up
        # round, but it is worth a named limit — see
        # `test_a_fresh_404_that_outlives_the_attempts_leaves_the_row_stuck`.
        raise IndexFailed(
            f"kreuzakt document {kreuzakt_id} answers 404 for {job.document_id} right "
            "after being bound: the binding is kept and the job retries, because either "
            "answer about the index makes a retry useful")
    return _outcome(store, job, kreuzakt_id, bound)


async def _upload_and_correlate(kz, store: Store, job: Job, *, papra, generation: int,
                                poll_s: float, wait_timeout_s: float,
                                max_attempts: int,
                                max_recovery_actions: int) -> int:
    """One upload, correlated, with a bounded recovery — and a reason on failure.

    The loop has two recoveries, and they are different actions because the failures
    are different facts:

    * a window that saturated or an ambiguous window means our row is not readable
      *yet* — the fix is a fresh upload, whose deduplicated document costs nothing
      and whose row is the newest thing in the window;
    * a `failed` row is a finished row, and re-running *that row* is the only thing
      `retry` accepts, so it is re-run rather than re-uploaded.

    Both budgets show up here and neither stands in for the other. The recovery bound
    (`max_recovery_actions`) is this run's: how many re-uploads and row re-runs one
    attempt may spend before it reports itself. `max_attempts` is the job's ceiling
    across runs, which matters at the failed-row branch — on what would be the last
    attempt there is nobody left to re-run the row, so the attempt says so instead of
    leaving a finished row nobody will revisit. The failure that follows names the
    condition. Falling through as "nothing to do" is the outcome this function exists
    to make impossible: the job would be marked done with no id recorded.
    """
    filename = index_upload_name(job)
    deadline = _clock() + wait_timeout_s
    reason: Exception | None = None
    vanished = False
    requeue_row: int | None = None       # a `failed` row to re-run instead of re-uploading
    stored_name = filename
    after_id = 0
    last_error_text: str | None = None

    for attempt in range(1, max_recovery_actions + 1):
        _require_budget(deadline, wait_timeout_s, job, attempt - 1, reason)

        if requeue_row is None:
            after_id = await kz.queue_high_water_mark()   # before the upload, always
            # The name we asked for is not the name the server stored, and the
            # stored name is what the queue reports: correlate on the response.
            stored_name = await kz.upload(filename, await download_original(papra, job))
        else:
            await kz.retry_queue(requeue_row)
            requeue_row = None

        # Recomputed here rather than reused from the top of the loop: the high-water
        # read, the download of Papra's bytes and the upload all cost time, and a wait
        # started on that stale allowance is a wait that outlives the budget — on a
        # 40 MB document the download alone can be the whole of it.
        remaining = deadline - _clock()
        _require_budget(deadline, wait_timeout_s, job, attempt, reason)
        try:
            entry = await kz.wait_for_queue_row(stored_name, after_id=after_id,
                                                poll_s=poll_s,
                                                timeout_s=min(remaining, wait_timeout_s))
        except (CorrelationWindowFull, AmbiguousCorrelation) as error:
            reason = error
            logger.warning(
                "index correlation incomplete (%s) for %s above id %s: re-uploading, "
                "recovery action %d of %d", type(error).__name__, job.document_id,
                after_id, attempt, max_recovery_actions)
            continue
        except KreuzaktError as error:
            if not _is_vanished_row_error(error):
                # A wait that timed out, a transport failure, a body we could not
                # read: none of them is ours to reinterpret. The worker backs off
                # and tries again, and a document that keeps failing gets its
                # attempts spent with the real error recorded.
                raise
            # Measured: this is what deleting the document leaves behind — a
            # completed row with a null id, which cannot be re-run. Papra decides.
            reason = error
            if not await _papra_holds_document(papra, job):
                raise IndexFailed(
                    f"index document vanished: Kreuzakt completed a row for {filename} "
                    f"with no document id and Papra no longer holds {job.document_id} "
                    f"(the deletion pass owns this document)", permanent=True) from error
            vanished = True
            logger.warning(
                "kreuzakt completed a row for %s with no document id (recovery action "
                "%d of %d) and papra still holds it: re-uploading", job.document_id,
                attempt, max_recovery_actions)
            continue

        if entry.status == "failed":
            text = entry.error or "kreuzakt reported no error text"
            deterministic = text == last_error_text
            last_error_text = text
            if deterministic:
                # The probe re-ran the corrupt-PDF row and got byte-identical text
                # every time. Identical text is how that shows up from in here, and
                # one more attempt is one more VLM extraction of a file that will
                # never parse.
                raise IndexFailed(f"kreuzakt queue error for {job.document_id}: {text}")
            if job.attempts + 1 >= max_attempts:
                raise IndexFailed(
                    f"kreuzakt queue error for {job.document_id}: {text} "
                    f"(job attempt budget of {max_attempts} spent)")
            requeue_row = entry.id
            logger.warning(
                "kreuzakt queue row %s failed for %s (%s): re-running the row, "
                "job attempt %d of %d", entry.id, job.document_id, text,
                job.attempts + 1, max_attempts)
            continue
        if entry.status == "completed" and entry.document_id is not None:
            bound = store.ledger_set_kreuzakt_id(
                papra_org_id=job.papra_org_id, papra_document_id=job.document_id,
                kreuzakt_id=entry.document_id, generation=generation,
                only_if_generation=generation)
            if not bound:
                if store.get_generation() != generation:
                    logger.warning(
                        "generation changed mid-flight for %s: kreuzakt id %s from "
                        "generation %d discarded, the job retries against the new index",
                        job.document_id, entry.document_id, generation)
                    raise IndexFailed("generation changed mid-flight; result discarded")
                raise IndexFailed(
                    f"ledger reference for {job.document_id} was not updated: the row "
                    f"for {job.papra_org_id}/{job.document_id} is gone, so kreuzakt id "
                    f"{entry.document_id} would be an id nothing resolves")
            return entry.document_id

        # `wait_for_queue_row` only ever returns a terminal row, and a completed row
        # with a null id reaches us as an error, so this is the shape we cannot read
        # as either completion or re-run. Say so rather than return nothing.
        #
        # Belt, and a known-unkillable one: under the client's contract today no row
        # can arrive here, so a mutation that deletes this raise survives the suite —
        # the same equivalence as the "completed means completed" check in the branch
        # above. It stays because the thing it protects is the return value: falling
        # out of the loop with no id would mark a job done with nothing recorded, and
        # that is cheaper to survive than to rediscover if the client's rule changes.
        raise IndexFailed(
            f"kreuzakt queue row {entry.id} for {job.document_id} ended {entry.status} "
            f"with document_id={entry.document_id}: not a completion we may bind and not "
            "a failure we may re-run")

    if vanished:
        raise IndexFailed(
            f"index document vanished: Kreuzakt completed a row for {filename} with no "
            f"document id {max_recovery_actions} times while papra still holds "
            f"{job.document_id}") from reason
    if reason is None:      # unreachable under Task 5's client, see the note below
        raise IndexFailed(f"index of {job.document_id} gave up after "
                          f"{max_recovery_actions} recovery actions with no recorded "
                          "reason")
    raise reason
