"""The `index` job kind: put Papra's bytes into Kreuzakt, correlate, record the id.

Phase one was upload, correlate, record, and write nothing back into Papra. Task 7 adds
the second half: `deliver_content`, which puts Kreuzakt's text into Papra's `content`, and
`verify_once`, the pass that notices when Papra has overwritten it. Both sit behind the
`index_content_delivery` gate read from the store, checked HERE as well as in the
scheduler, because the convergence pass, an inbound trigger and a CLI call all reach this
module without passing a loop that was told to be careful. Metadata — title, date, note —
arrives in Task 8 and stays a separate, independently reversible switch.

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
from datetime import date, datetime, timedelta

from papra_suite.kreuzakt_client import (
    AmbiguousCorrelation,
    CorrelationWindowFull,
    KreuzaktError,
    KzDocument,
)
from papra_suite.papra_client import PapraError
from papra_suite.store import (
    CLEANUP_MAX_ATTEMPTS as CLEANUP_MAX_ATTEMPTS,
)
from papra_suite.store import (
    EMPTY_EXTRACTION_SHA,
    PROTECTION_FIELDS,
    Job,
    LedgerRef,
    Store,
    _iso,
    _require_store_utc_iso,
)
from papra_suite.store import (
    _now as _store_now,
)

# Two names come in through the door marked private, and both because a second definition of
# the same rule would be the bug rather than a shortcut. The timestamp form is compared as
# *text* inside the ledger, so code that schedules a backoff has to produce and validate it
# with the store's own helpers or a `Z` suffix silently reschedules a delete.
# `CLEANUP_MAX_ATTEMPTS` is re-exported under the `import X as X` convention — that spelling
# is how a name declares itself part of this module's surface rather than a forgotten import:
# the ceiling lives in the store, which is what enforces it, but it belongs to the retry shape
# `CLEANUP_BACKOFF_S` below also describes, and restating a number in two places is how a
# configured 5 becomes a realised 3.

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


def synthetic_upload_name(document_id: str, file_hash: str) -> str:
    """The name this bridge uploads a document under: `papra-<docId>-<hash8>.pdf`.

    Deterministic, so a re-upload of the same document lands on the same name and
    Kreuzakt's dedup keeps one document, and self-describing, so a row in the queue can be
    traced to a Papra document by eye. The alphabet is `[a-z0-9.-]` plus `.pdf` and nothing
    else — see `_stem`.

    Named for what it is to the user, not only for how it is built: this string is the
    reason `is_synthetic_upload_name` exists. A title derived from it is our own text
    arriving back at us, and delivering it into Papra's human-visible `name` renames the
    corpus to garbage. See `deliver_metadata`.
    """
    return f"papra-{_stem(document_id)}-{_stem(file_hash)[:8]}.pdf"


def index_upload_name(job: Job) -> str:
    """The upload name for one job's bytes — `synthetic_upload_name` seen from a job."""
    return synthetic_upload_name(job.document_id, job.content_hash)


_COLLISION_SUFFIX = re.compile(r"\s*\(\d+\)$")
_SEPARATORS_OUT = re.compile(r"[\s_.\-]+")
#: Our own shape with any document id and any hash: `papra-<id>-<8 hex>`. The id may
#: contain separators of its own (Papra's are uuids), so the only reliable anchor is the
#: prefix and the eight hex digits at the end.
_SYNTHETIC_SHAPE = re.compile(r"^papra-.+-[0-9a-f]{8}$", re.IGNORECASE)


def _upload_stem(name: str) -> str:
    """A name reduced to the form our upload stem arrives in, for comparison only.

    Two transformations have to be undone before a title can be compared with our upload
    name at all. Both servers append ` (1)`, ` (2)`… when a name collides, and they put it
    BEFORE the extension (`papra-doc-1-a1b2c3d4 (1).pdf`), so the extension comes off first
    and the suffix second — the other order leaves the suffix in and the guard misses every
    second copy of a document, which is the case a synthetic name is most likely to be
    found in. And the title a keyless Kreuzakt derives from a filename is the *prettified*
    filename: measured on the pinned container, `papra-doc-1-a1b2c3d4.pdf` comes back as
    `Papra Doc 1 A1b2c3d4`. So separators — space, dot, underscore, hyphen — collapse to one
    form, and case is folded away at the call site because prettifying capitalises words.
    """
    stem = (name or "").strip()
    if stem.lower().endswith(".pdf"):
        stem = stem[:-4]
    stem = _COLLISION_SUFFIX.sub("", stem)
    return _SEPARATORS_OUT.sub("-", stem).strip("-")


def is_synthetic_upload_name(name: str, *, document_id: str, file_hash: str) -> bool:
    """Is `name` our own upload name for THIS document, in any form it reaches us in?

    Exact, with or without `.pdf`, with a ` (N)` collision suffix, or prettified into a
    title. All four are the same string by the time the servers are done with it, and a
    guard that caught only one of them would be a guard an operator could step over by
    uploading twice.
    """
    if not (name or "").strip():
        return False
    ours = _upload_stem(synthetic_upload_name(document_id, file_hash))
    return _upload_stem(name).casefold() == ours.casefold()


def looks_like_synthetic_upload_name(name: str) -> bool:
    """Does `name` have our shape, whatever document it belongs to?

    The pair to `is_synthetic_upload_name`, and needed because the two answers differ in
    the direction that matters. A reference can point at a Papra document whose name is
    our upload name for a *different* hash — a document re-ingested after its bytes moved,
    or a row whose ledger hash is behind — and that string is no more ours to deliver as a
    title than our own is.

    It errs the other way on purpose: a genuine name that happens to end in eight
    hex-looking characters (`Papra-Rechnung-20250101`) is refused as a rename target, which
    costs a title, while the reverse error costs a corpus-wide rename. Refusing is the
    cheap side of that trade.
    """
    return bool(_SYNTHETIC_SHAPE.match(_upload_stem(name)))


# --- the handler ------------------------------------------------------------------

# Whether THIS build can write Kreuzakt's content into Papra. Task 7 wrote
# `deliver_content` and set this True; had it stayed False, a build that claimed the flag
# would stop the one component producing Papra's text today and put nothing in its place.
# It is a fact about the build rather than about the operator, which is why `serve` reads
# it when it seeds the setting — and why it is read as an attribute OF THIS MODULE (never
# `from papra_suite.indexing import CONTENT_DELIVERY_IMPLEMENTED`), so the documented way
# of exercising a build without the path still works from a test.
CONTENT_DELIVERY_IMPLEMENTED = True

#: Whether THIS build can write Kreuzakt's title, date and note into Papra. The same seam
#: as above and a sharper need: `PAPRA_INDEX_METADATA_DELIVERY` was advertised in the
#: README as configurable while nothing in the build read it, which is the `OCR_LANGUAGES`
#: shape — an operator sets a flag, sees nothing happen, and has nothing to blame but the
#: documentation. Seeding the setting from this attribute is what stops that repeating: a
#: build without the path keeps the legacy title writer in charge and says so in the log,
#: rather than promising renames and delivering silence. Read as an attribute OF THIS
#: MODULE, never imported by value, so the documented way to exercise such a build works.
METADATA_DELIVERY_IMPLEMENTED = True

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
    #: What this job delivered into `name`/`documentDate`/`notes`, or None when the metadata
    #: gate was shut. Kept apart from `delivered` because the columns have separate gates:
    #: `delivered=True, metadata=None` is a normal configuration (the text ours, the titles
    #: not), and collapsing the two would make the metadata flip unauditable from the log.
    metadata: MetadataResult | None = None


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


class DeliveryDisabled(Exception):
    """A delivery gate is shut, so this write is refused.

    Raised by both writers — `index_content_delivery` for `content`, `index_metadata_delivery`
    for `name`/`documentDate`/`notes` — because both are asked the same question by the same
    callers.

    An exception rather than a `False`: the callers that reach this with the gate shut are
    the ones that were not told to check it — the convergence pass, an inbound trigger, a
    CLI call — and for those, being refused is news. A returned `False` is a value a
    caller can drop on the floor, and the failure it would hide is the silent kind: an
    operator who switched the writer on and got text nowhere.

    The index worker is the one caller that does not raise, and it does not need to: for a
    worker a shut gate is the normal configuration rather than an incident, so it reports
    `delivered=False` and no metadata result instead of failing a job nobody asked to have
    failed.
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


# --- the content writer -----------------------------------------------------------

#: Page size for the verification walk. The corpus is a few hundred documents, so this is
#: two to five queries; the number bounds memory and, more usefully, bounds how many
#: references can change between two pages of one walk. It is not a tuning knob.
VERIFY_PAGE_SIZE = 100
#: How many repairs one reference may lose inside `store.REPAIR_WINDOW_H` before the pass
#: stops joining the fight and says so at ERROR. Two writers on one column cannot be won by
#: writing harder; the third attempt costs a PATCH and a VLM round trip to lose the text
#: again. This is the number of repairs ALREADY SPENT that stops the pass, and a repair
#: counts only when the write landed — `Store.ledger_repairs_in_window` is the read that
#: answers it, precisely so that a write Papra refused cannot spend its way to suppression.
MAX_REPAIRS_IN_WINDOW = 2
#: Papra's own ceiling on the `notes` field. Asking for more does not produce a longer
#: note, it produces a 4xx and a failed job.
PAPRA_NOTES_CAP = 2048
DEFAULT_NOTES_LIMIT = 2000
TRUNCATION_MARKER = "…"

#: What `deliver_content` did. Strings rather than booleans because "it wrote" and "it
#: correctly refused" have to be told apart by three callers, and `False` cannot say which.
DELIVERED = "delivered"
SKIPPED_EMPTY = "skipped-empty"
SKIPPED_CURRENT = "skipped-current"


def papra_document_is_deleted(document: dict) -> bool:
    """Is this Papra record in the trash? Papra answers; we do not infer.

    One definition, used by the index job, the vanished-row branch, the convergence pass
    and the writer itself, because the consequence of the two readings disagreeing is a
    write into a document the user deleted: Papra resurrects it when the column changes,
    so a "helpful" repair un-deletes something on purpose. `reconcile_deletions` is what
    sets the ledger's own `deleted_at`, and it asks this question (or Papra's 404) before it
    does — so for every soft-deleted Papra document the ledger still considers live, the
    writer is the only thing standing between the pass and the trash.
    """
    return bool(document.get("isDeleted") or document.get("deletedAt"))


def sha_of(text: str) -> str:
    """The hash of a piece of text, the way the ledger stores it in `delivered_sha`."""
    return hashlib.sha256(text.encode()).hexdigest()


def extraction_is_empty(text: str) -> bool:
    """One definition of "the index has no text for this document".

    It lives here, named, because two decisions read it: whether to refuse a write, and
    whether a reference counts as extraction-empty in `check`. Two definitions of empty
    is how a document ends up empty to one of them and not to the other, and the
    whitespace-only page is the common case rather than the exotic one.
    """
    return not (text or "").strip()


def truncate_notes(text: str, limit: int = DEFAULT_NOTES_LIMIT) -> str:
    """Shorten `text` for Papra's `notes`, cutting at a boundary a reader can live with.

    Task 8 writes the field; the cap ships with the content writer because both are the
    same question — how much of Kreuzakt's text may be put into Papra — and because a
    delivery that exceeded Papra's own bound would fail the job rather than shorten the
    note. Three rules, in order: cut at the last newline below the limit, because a note
    is prose; otherwise cut at the last space, because "1.234,5" is not a truncated
    amount, it is a wrong one; and mark the cut with an ellipsis, so a reader knows the
    sentence went on rather than ending there.
    """
    cap = max(1, min(int(limit), PAPRA_NOTES_CAP - len(TRUNCATION_MARKER)))
    if len(text) <= cap:
        return text
    head = text[:cap]
    cut = max(head.rfind("\n"), head.rfind(" "))
    if cut > 0:
        head = head[:cut]
    return head.rstrip() + TRUNCATION_MARKER


def _record_extraction_empty(store: Store, ref: LedgerRef, kz_doc: KzDocument,
                             generation: int) -> None:
    """Stamp "the extraction produced nothing" onto the reference, and write nothing else.

    `EMPTY_EXTRACTION_SHA` in `delivered_sha` is the only thing that makes
    `ledger_stats()["extraction_empty"]` count anything, and that count is the cheap
    signal during a backfill that separates "a few odd pages" from "this extractor is
    wrong for this corpus". Until Task 7 nothing wrote it, so the number sat at a
    convincing zero.

    It is written by both the worker and the verification pass through this one function:
    an emptiness fact recorded two different ways is a corpus where the count depends on
    which pass happened to look first.
    """
    if ref.delivered_sha == EMPTY_EXTRACTION_SHA:
        return                      # already recorded; a six-hourly pass need not rewrite it
    store.ledger_record_delivery(papra_org_id=ref.papra_org_id,
                                 papra_document_id=ref.papra_document_id,
                                 delivered_at=kz_doc.updated_at or "",
                                 delivered_sha=EMPTY_EXTRACTION_SHA,
                                 generation=generation)


async def deliver_content(papra, store: Store, ref: LedgerRef, kz_doc: KzDocument, *,
                          generation: int) -> str:
    """Write Kreuzakt's text into Papra, unless doing so would lose text.

    The flag is checked here as well as in the scheduler: convergence, an inbound trigger
    and the CLI all reach this function, and a gate that lives only in the loop can be
    bypassed by the paths that matter most during an incident.

    Order also matters within the body, and it is load-bearing rather than stylistic. The
    emptiness guard runs BEFORE the Papra read and before any equality shortcut, because a
    guard placed after a cheap comparison can be reordered into a wipe by someone
    optimising this function later — and the thing it prevents is the one unrecoverable
    act available here: overwriting a year of a document's text with a blank page.

    `delivered_sha` records what we sent, not what Papra holds: Papra's OCR worker can and
    does overwrite us afterwards, which is what `verify_once` is for. The write and the
    record are deliberately in that order — a refusal from Papra must leave the ledger
    claiming nothing.

    A document Papra says it has deleted is refused here as well as upstream, and
    permanently: this is the shared write boundary, so the caller that reaches it with a
    stale read — the convergence pass, an inbound trigger, anything added later — gets the
    same answer the index job gives. A retry cannot help, because the trash is not going
    to un-delete itself.
    """
    if not store.settings_flag("index_content_delivery"):
        raise DeliveryDisabled(
            f"content delivery is off, so papra://{ref.papra_org_id}/"
            f"{ref.papra_document_id} was not written")

    if extraction_is_empty(kz_doc.content):
        logger.info("extraction-empty, not writing papra=%s kreuzakt=%s",
                    ref.papra_document_id, kz_doc.id)
        _record_extraction_empty(store, ref, kz_doc, generation)
        return SKIPPED_EMPTY

    sha = sha_of(kz_doc.content)
    current = await papra.get_document(ref.papra_org_id, ref.papra_document_id)
    if papra_document_is_deleted(current):
        raise IndexFailed(
            f"document deleted in papra, not writing: {ref.papra_document_id}",
            permanent=True)
    papra_content = current.get("content") or ""
    if sha == sha_of(papra_content):
        if ref.delivered_sha != sha:
            # Someone else — Papra's own pass, an earlier run of this very function — put
            # our text there. The column is converged and only the ledger's memory is
            # behind, so record it without a PATCH: rewriting the archive on the strength
            # of a null column is how a flag flip rewrites five hundred documents.
            store.ledger_record_delivery(papra_org_id=ref.papra_org_id,
                                         papra_document_id=ref.papra_document_id,
                                         delivered_at=kz_doc.updated_at or "",
                                         delivered_sha=sha, generation=generation)
        return SKIPPED_CURRENT

    try:
        await papra.update_content(ref.papra_org_id, ref.papra_document_id, kz_doc.content)
    except Exception as error:  # noqa: BLE001 - re-raised; this line is the operator's record
        # The write is not recorded, so nothing claims a delivery: the row stays as it was,
        # the exception travels with its status so `worker_loop` can tell a retry from a
        # permanent no, and this is the line that says which document to go and look at.
        logger.error("content delivery to papra=%s failed: %s", ref.papra_document_id, error)
        raise
    store.ledger_record_delivery(papra_org_id=ref.papra_org_id,
                                 papra_document_id=ref.papra_document_id,
                                 delivered_at=kz_doc.updated_at or "",
                                 delivered_sha=sha, generation=generation)
    logger.info("content delivered papra=%s kreuzakt=%s chars=%d",
                ref.papra_document_id, kz_doc.id, len(kz_doc.content))
    return DELIVERED


# --- metadata delivery -------------------------------------------------------------

#: The three fields this pass may write, which are exactly the three that can be
#: protected. One list, imported from the store rather than repeated here: a fourth
#: writable column would otherwise have to be added in two places, and the second one is a
#: `ValueError` from `ledger_protect` far away from the cause.
METADATA_FIELDS = PROTECTION_FIELDS

#: What one field's verdict was. Four, because three different nothings have to be told
#: apart by whoever reads the CLI's output: `protected` is "a human owns this now",
#: `skipped` is "the index has nothing to say", `unchanged` is "Papra already holds it".
METADATA_WRITTEN = "written"
METADATA_PROTECTED = "protected"
METADATA_UNCHANGED = "unchanged"
METADATA_SKIPPED = "skipped"

#: Where each field lives, in the three vocabularies it passes through: Papra's record as
#: read, `PapraClient.update_document`'s keyword, and Papra's request body. The middle one
#: differs on exactly one field (`document_date` → `documentDate`), which is why all three
#: are written down rather than derived by guesswork.
_PAPRA_FIELD = {"name": "name", "date": "documentDate", "notes": "notes"}
_CLIENT_FIELD = {"name": "name", "date": "document_date", "notes": "notes"}

_ISO_DATE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


def normalize_date(value: str | None) -> str | None:
    """The index's date as Papra's `documentDate` input: ISO `YYYY-MM-DD`, or None.

    Nothing else is accepted, and the reason is a measured 400 rather than taste. Sending
    `1. Oktober 2025` does not lose the date, it fails the whole PATCH — and the date
    shares that request with the title and the note, so one free-text date from the model
    would cost the other two fields their delivery and fail the job. So:

    * `2025-10-01` (with surrounding whitespace tolerated) → `2025-10-01`;
    * a real-looking impossibility (`2025-02-30`, `2025-13-01`) → None, because Papra would
      answer 400 for those too;
    * empty, missing, or prose → None.

    None means *skip the field*, which is not the same as protecting it: the model's
    silence says nothing about who owns the column, and a pass that protected fields it had
    nothing to write would freeze the whole corpus on the first document with no date.

    Papra's own stored form (`2025-10-01T00:00:00.000Z`) is deliberately not accepted here:
    this reads what the index holds. What Papra holds is read by `_stored_date`.
    """
    text = (value or "").strip()
    match = _ISO_DATE.fullmatch(text)
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    try:
        date(year, month, day)
    except ValueError:
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def _stored_date(value) -> str | None:
    """Papra's `documentDate` as a bare ISO day, or None when it is not a day we recognise.

    Measured on the pinned container: a date we send as `2025-10-01` is stored and read
    back as `2025-10-01T00:00:00.000Z`. Comparing raw strings would therefore call our own
    write a human edit, and protection is permanent — one pass would protect `date` on
    every document in the corpus and never write it again. That is worse than the error it
    guards against, so the comparison happens at day resolution.

    A value that is not a recognisable day comes back None as well, and the caller treats
    that as *protected*: an unparseable date is still a value somebody put there, and this
    pass has no business overwriting it.
    """
    text = str(value or "").strip()
    return normalize_date(text[:10]) if len(text) >= 10 else None


def _comparable(field: str, value) -> str:
    """One field's value in the form used for "did anything change?".

    Dates compare as days for the reason in `_stored_date`; text compares with surrounding
    whitespace gone, because a trailing space in a title is not a human edit and Pretend
    it is protects a field over a difference nobody can see.
    """
    if field == "date":
        return _stored_date(value) or str(value or "").strip()
    return str(value or "").strip()


@dataclass(frozen=True)
class OwnershipVerdict:
    """Whether a field is ours to write, and if not, which rule says so.

    `reason` names the rule rather than carrying prose, because the log line and the CLI's
    count both need to distinguish "a human renamed this" from "Papra had already inferred
    a date" — those two send an operator to different places.
    """

    owned: bool
    reason: str


def ownership(ref, *, field: str, current) -> OwnershipVerdict:
    """Is this field ours to write? A heuristic, not a compare-and-swap.

    Papra offers no conditional write, so a rename landing between our read and our write
    is overwritten; the worst case is one lost rename, never lost text. Given that, the
    order below is a ranking of what this pass is willing to be wrong about, and each
    refusal names itself:

    * `protected` — a human took this field once, and the flag is durable. Nothing further
      down can override it; `papra-suite metadata --restore-auto` is the way back.
    * `synthetic-name` — our own upload name in Papra's `name` proves nothing about a human
      leaving it alone, so it is not ours to overwrite either. Checked before the baseline
      deliberately: a reference whose `papra_original_name` came in looking like our upload
      name would otherwise read as "Papra still holds what we first saw".
    * `not-ours` — we wrote a value here and Papra no longer holds it.
    * the baseline, per field. For `name` that is `papra_original_name`, which is Papra's
      own name for the document at the moment we first saw it. **Kreuzakt's
      `original_filename` is not consulted anywhere in this function**: it holds our
      `papra-<documentId>-<hash8>.pdf` for every document in the corpus, because we named
      the file when we uploaded it, so measuring a human's rename against it measures
      nothing at all. For `date` and `notes` there is no baseline to have, so the first
      write requires an empty Papra value: `empty-required` says a value we cannot attribute
      might be a human's, and Papra does run its own date detection.

    `current` is Papra's value as read, in whatever form Papra stores it.
    """
    if getattr(ref, f"{field}_protected"):
        return OwnershipVerdict(False, "protected")
    if field == "name" and (
        is_synthetic_upload_name(current or "", document_id=ref.papra_document_id,
                                 file_hash=ref.file_hash)
        or looks_like_synthetic_upload_name(current or "")
    ):
        return OwnershipVerdict(False, "synthetic-name")
    ours = getattr(ref, f"written_{field}", None)
    if ours is not None:
        if _comparable(field, current) == _comparable(field, ours):
            return OwnershipVerdict(True, "")
        return OwnershipVerdict(False, "not-ours")
    if field == "name":
        if _comparable(field, current) == _comparable(field, ref.papra_original_name):
            return OwnershipVerdict(True, "")
        return OwnershipVerdict(False, "no-baseline")
    return OwnershipVerdict(not _comparable(field, current),
                            "" if not _comparable(field, current) else "empty-required")


def _metadata_candidate(field: str, kz_doc: KzDocument) -> str | None:
    """What the index has to say about one field, or None when it has nothing to say.

    None means *skip*, and it is a different answer from the empty string: silence about a
    field says nothing about who owns it, while a value Papra holds is a claim somebody
    made. Conflating them is how a document with no date ends up with its date protected.

    `notes` is truncated here rather than at the write, so the comparison, the request body
    and the ledger record are all the same string. Truncating only when sending would leave
    `written_notes` holding text Papra never received, and the next pass would then read a
    human's shorter note as an edit — the truncation itself would look like vandalism.
    """
    if field == "name":
        return (kz_doc.title or "").strip() or None
    if field == "date":
        return normalize_date(kz_doc.document_date)
    return truncate_notes((kz_doc.description or "").strip()) or None


@dataclass(frozen=True)
class MetadataResult:
    """What one metadata delivery did, as one verdict per field.

    Per field rather than one boolean, because the three fields have three different
    histories on the same document — a renamed file, a date Papra inferred, a note the user
    typed — and an operator deciding whether to run `--restore-auto` needs to know which
    one the pass refused and why. `skipped` on all three is a different situation from
    `protected` on all three, and only the field-level view can say which.
    """

    name_action: str
    date_action: str
    notes_action: str


def _result(actions: dict) -> MetadataResult:
    """Build the result from the per-field verdicts this pass collected.

    A function for one line, because the keys are the field names while the dataclass's are
    `name_action`, `date_action`, `notes_action`, and `MetadataResult(**actions)` is a
    `TypeError` at the end of a delivery that already wrote to Papra. Deriving the suffix
    once here keeps the four verdict constants and the three field names as the only two
    vocabularies in this module.
    """
    return MetadataResult(**{f"{field}_action": actions[field]
                             for field in METADATA_FIELDS})


async def deliver_metadata(papra, store: Store, ref: LedgerRef, kz_doc: KzDocument, *,
                           fields=None, force: bool = False,
                           dry_run: bool = False) -> MetadataResult:
    """Write Kreuzakt's title, date and note into Papra, unless a human owns the field.

    The gate is checked here, in the writer, for the same reason `deliver_content` checks
    its own: the scheduler is not the only door, and a flag that only the loop consults is
    bypassed by the CLI, the convergence pass and whatever path gets added next. It is
    checked before the document is read, so a refused call leaves no GET on a record it was
    not allowed to touch.

    **A synthetic title is refused before the ownership question is asked.** A keyless
    Kreuzakt derives a title from the filename, and the filename is always ours, so the
    model's "title" for such a deployment is `papra-doc-1-a1b2c3d4` — and Papra's prettified
    version of it, `Papra Doc 1 A1b2c3d4`, is what a real pass would read back as the
    current name. Delivering that renames every document in the corpus to our own upload
    garbage and then records it in `written_name` as authoritative, so every later pass
    defends the garbage. The verdict is `skipped`: not a human edit, so it must not protect,
    and not a write, so it must not record.

    **The ledger row is the authority on ownership, not the reference this caller is holding.**
    `deliver_metadata` re-reads it, and refuses outright when no row answers: every caller
    obtains its reference from that table, so a row that has disappeared means the history
    saying which fields are ours was removed on purpose. A reference is an address here, not a
    record of the past.

    One PATCH for all the fields that changed, and nothing else in its body: `content` is
    Task 7's column with its own gate, and a request that mixed them would make a title
    change cost a text rewrite. The write is recorded only after Papra accepts it, so a 400
    or a 500 claims nothing and every field stays retriable. `written_*` is then stamped with
    the string this pass sent — and, for a field that needed no write, with the identical
    string it checked and endorses. Never with the model's longer pre-truncation original:
    the column exists to answer "is Papra still holding what we sent?", and a value Papra
    never received cannot answer that — recording it would make our own truncation look like
    a human edit on the next pass.

    `fields` scopes the pass (the CLI's `--field`); fields outside the scope are reported
    `skipped` and are neither compared nor written. `force` overrides protection for this run
    only — the durable flag stays set, because `--restore-auto` is the command that means
    "forever". `dry_run` answers with the verdicts and changes neither Papra nor the ledger,
    including the protection flags: a dry run that sets a flag is not a rehearsal.
    """
    if not store.settings_flag("index_metadata_delivery"):
        raise DeliveryDisabled(
            f"metadata delivery is off (PAPRA_INDEX_METADATA_DELIVERY), so papra://"
            f"{ref.papra_org_id}/{ref.papra_document_id} was not touched")

    scope = METADATA_FIELDS if fields is None else tuple(fields)
    unknown = sorted(set(scope) - set(METADATA_FIELDS))
    if unknown:
        raise ValueError(
            f"not metadata fields: {', '.join(unknown)}; this pass writes only "
            f"{', '.join(METADATA_FIELDS)}")

    document = await papra.get_document(ref.papra_org_id, ref.papra_document_id)
    if papra_document_is_deleted(document):
        # The same containment `deliver_content` applies, from the same single definition of
        # "deleted": Papra resurrects a trashed document when a column changes, so a
        # metadata write into one un-deletes it deliberately. The ledger still calls this
        # reference live until Task 9's pass owns deletes, so nothing upstream would have
        # noticed.
        raise IndexFailed(
            f"document deleted in papra, not writing metadata: {ref.papra_document_id}",
            permanent=True)

    # The ledger row, read now, is the authority on this reference's field history — not the
    # snapshot the caller is holding. The worker's `ref` was read when its job started and the
    # CLI's may come from another process entirely, and staleness cuts both ways, which is why
    # this is a re-read rather than a conservative merge:
    #
    # * a protection flag the caller does not know about must stop the write, because
    #   overwriting a human's value is the one unrecoverable act available here;
    # * a `written_*` the caller still believes in may already have been superseded by another
    #   delivery, and Papra holding that older value is then a change nobody here accounted
    #   for — possibly a human putting a previous title back. Trusting the stale column would
    #   call it ours and overwrite it.
    #
    # So the row wins in both directions, and a `--restore-auto` that cleared a protection in
    # another process is honoured rather than second-guessed.
    #
    # And when no row answers, this refuses rather than falling back to the snapshot. Every
    # caller reads its reference out of this same table, so a row that has gone away means
    # someone deleted the reference — the cleanup pass, or an operator — and the history that
    # authorises a write went with it. Writing on from the caller's `ref` at that moment would
    # be writing on the strength of history that was deliberately removed.
    current_ref = store.ledger_get(ref.papra_org_id, ref.papra_document_id)
    if current_ref is None:
        raise IndexFailed(
            f"no ledger reference for papra://{ref.papra_org_id}/{ref.papra_document_id}: "
            "refusing to deliver metadata from a caller's snapshot, because the record of "
            "which fields are ours is gone. Permanent, so the index job for these bytes ends "
            "failed, and a failed index row absorbs every later enqueue for the same hash "
            "until that row is reset — by `jobs --reset` once Task 15 ships it, by a hand edit "
            "of the `jobs` table until then — or by the queue being made re-schedulable",
            permanent=True)

    actions: dict[str, str] = {}
    body: dict[str, str] = {}
    recorded: dict[str, str] = {}

    for field in METADATA_FIELDS:
        candidate = _metadata_candidate(field, kz_doc) if field in scope else None
        if candidate is None:
            # Either nothing to deliver or out of scope; both are "not this pass's business".
            actions[field] = METADATA_SKIPPED
            continue
        if field == "name" and (
            is_synthetic_upload_name(candidate, document_id=ref.papra_document_id,
                                    file_hash=ref.file_hash)
            or looks_like_synthetic_upload_name(candidate)
        ):
            logger.info("not delivering our own upload name as a title: papra=%s title=%s",
                        ref.papra_document_id, candidate)
            actions[field] = METADATA_SKIPPED
            continue

        held = document.get(_PAPRA_FIELD[field])
        verdict = ownership(current_ref, field=field, current=held)
        if not verdict.owned and not force:
            actions[field] = METADATA_PROTECTED
            if not dry_run and not getattr(current_ref, f"{field}_protected"):
                # Set the flag first, then say so: the flag is the memory, so "once" is
                # structural rather than a promise about log volume. Three passes over the
                # same human rename cost one line each the first time and nothing after.
                store.ledger_protect(papra_org_id=ref.papra_org_id,
                                     papra_document_id=ref.papra_document_id, field=field)
                logger.warning("protecting %s on papra=%s kreuzakt=%s: %s (papra holds %r)",
                               field, ref.papra_document_id, kz_doc.id, verdict.reason,
                               str(held or "")[:80])
            continue

        if _comparable(field, held) == _comparable(field, candidate):
            # Papra already holds this exact value. Only the ledger's memory can be behind,
            # so it is caught up without a PATCH — the same reasoning that keeps a flag flip
            # from rewriting five hundred documents.
            actions[field] = METADATA_UNCHANGED
            recorded[field] = candidate
            continue

        actions[field] = METADATA_WRITTEN
        body[_CLIENT_FIELD[field]] = candidate
        recorded[field] = candidate

    if dry_run:
        return _result(actions)

    if body:
        try:
            await papra.update_document(ref.papra_org_id, ref.papra_document_id, **body)
        except Exception as error:  # noqa: BLE001 - re-raised; this line is the operator's record
            # Nothing is recorded, so the ledger keeps claiming only what Papra has: every
            # field in that refused body stays retriable, and none of them is remembered as
            # ours. `verify_once` cannot converge a title either, so this line is the only
            # trace the attempt left.
            logger.error("metadata delivery to papra=%s failed: %s",
                         ref.papra_document_id, error)
            raise
    if recorded:
        try:
            store.ledger_record_metadata(papra_org_id=ref.papra_org_id,
                                         papra_document_id=ref.papra_document_id,
                                         **recorded)
        except Exception as error:  # noqa: BLE001 - re-raised; this line is the recovery
            logger.error(
                "metadata landed on papra://%s/%s but the ledger could not record it (%s): "
                "the next pass reads Papra's new value as somebody else's and protects the "
                "field. Put the history back by hand with these exact values: %s",
                ref.papra_org_id, ref.papra_document_id, error,
                ", ".join(f"{field}={value!r}" for field, value in sorted(recorded.items())))
            raise
    if body:
        logger.info("metadata delivered papra=%s kreuzakt=%s fields=%s",
                    ref.papra_document_id, kz_doc.id, ", ".join(sorted(body)))
    return _result(actions)


@dataclass(frozen=True)
class VerifyResult:
    """What one convergence pass did, counted in references rather than in queries.

    `diverged` counts references where the two sides disagreed AND the index had text to
    give: a blank extraction that differs from Papra's text is reported in `empty`, not
    as a divergence we cannot repair. `suppressed` counts the references left alone because
    they had already reached the ceiling of landed repairs inside the window — a count of
    references, not a diagnosis of why the text keeps changing; the ERROR line beside each
    one carries the document and the count, and nothing here can name the hand that
    overwrote us. `lookup_errors` counts references this pass could not finish, for either
    reason: a side that would not answer, or a repair Papra refused mid-write. Both mean
    "this document was not converged by this pass", which is the only decision the loop
    makes with it.
    """

    scanned: int = 0
    diverged: int = 0
    repaired: int = 0
    suppressed: int = 0
    empty: int = 0
    lookup_errors: int = 0


async def verify_once(store: Store, papra, kz) -> VerifyResult:
    """Compare Papra's current text with the index's, and repair what disagrees.

    This is the second half of the fix for v1's blind spot. v1 compared Papra's text
    against `delivered_sha` — what we last sent — so after Papra's own OCR worker
    overwrote a document we had delivered, the pass still reported it converged. The
    ledger's memory is a fact about us; the only fact worth asking about is what the two
    services hold now, so both sides are read fresh on every pass and `delivered_sha` is
    written, never consulted.

    Reads cost one GET per side per reference, which at a few hundred documents is seconds
    every six hours — the design's whole reason a full pass is affordable. `deliver_content`
    re-reads Papra on the repair path rather than trusting the value read here, because it
    is the shared write boundary and a caller's read is not its read; one extra GET on a
    document that already diverged is the cheaper side of that.

    Two failures this pass must not commit, both found in review and both about what the
    numbers it reports mean. It must not end because one document was refused: a
    `PapraError` escaping here loses the whole `VerifyResult` and every reference after the
    refused one, which turns one bad document into a corpus-wide blind spot. And a write
    that never landed must not spend the repair budget: the ceiling answers "how many times
    did we write this column and lose it again", and a refusal wrote nothing.
    """
    if not store.settings_flag("index_content_delivery"):
        # Not an error, and not a read either: a gate that queries first and refuses second
        # still puts load on the record it is not allowed to touch.
        return VerifyResult()

    generation = store.get_generation()
    counts = {"scanned": 0, "diverged": 0, "repaired": 0, "suppressed": 0, "empty": 0,
              "lookup_errors": 0}
    after: tuple[str, str] | None = None
    while True:
        refs = store.ledger_delivered_refs(generation=generation, after=after,
                                           limit=VERIFY_PAGE_SIZE)
        if not refs:
            break
        for ref in refs:
            counts["scanned"] += 1
            try:
                papra_document = await papra.get_document(ref.papra_org_id,
                                                          ref.papra_document_id)
                kz_doc = await kz.get_document(ref.kreuzakt_id)
            except (PapraError, KreuzaktError) as error:
                # Either side can be unreachable, and "it did not answer" is not evidence
                # that its text is wrong: no write, and no repair spent.
                logger.warning("verification could not read papra=%s kreuzakt=%s: %s",
                               ref.papra_document_id, ref.kreuzakt_id, error)
                counts["lookup_errors"] += 1
                continue

            if papra_document_is_deleted(papra_document):
                # Not a divergence, and not ours to repair. The ledger row is still live
                # because nothing on this branch sets `deleted_at` — Task 9 owns
                # `reconcile_deletions` — so every soft-deleted Papra document the ledger
                # still considers live would otherwise be written into twice and then
                # reported as a fight, spending `repairs(24h)` on the trash.
                logger.info("papra=%s is deleted there, not repairing: the deletion pass "
                            "owns this reference", ref.papra_document_id)
                continue

            if extraction_is_empty(kz_doc.content):
                counts["empty"] += 1
                # Refused, not failed: spending the suppression budget here would mean a
                # document that was blank for a day meets a pass that has already decided
                # to leave it alone, on the day its extraction finally arrives.
                _record_extraction_empty(store, ref, kz_doc, generation)
                continue

            papra_content = papra_document.get("content") or ""
            kz_sha = sha_of(kz_doc.content)
            if kz_sha == sha_of(papra_content):
                if ref.delivered_sha != kz_sha:
                    store.ledger_record_delivery(
                        papra_org_id=ref.papra_org_id, papra_document_id=ref.papra_document_id,
                        delivered_at=kz_doc.updated_at or "",
                        delivered_sha=kz_sha, generation=generation)
                continue

            counts["diverged"] += 1
            # READ before the repair, spent after it lands. The ceiling is about repairs
            # rather than about successes — a repair that diverged again an hour later is
            # the second loss whether or not the PATCH was pleasant about it — but a write
            # Papra refused landed nothing, and charging it walked persistently-refused
            # documents into suppression while blaming a second writer that was never
            # there. `store.ledger_repairs_in_window` is the read-only half of that.
            repairs = store.ledger_repairs_in_window(papra_org_id=ref.papra_org_id,
                                                     papra_document_id=ref.papra_document_id)
            if repairs >= MAX_REPAIRS_IN_WINDOW:
                counts["suppressed"] += 1
                # The observable and the document, never a verdict: a second writer, this
                # suite's own OCR landing after us, and a delivery that raced a job all
                # reach this line, and the first is not more likely than the others.
                logger.error(
                    "content diverged more than twice in 24h, leaving papra=%s alone "
                    "(%d repairs this window); Papra no longer holds what we sent, and "
                    "this pass cannot say who wrote it",
                    ref.papra_document_id, repairs)
                continue

            try:
                outcome = await deliver_content(papra, store, ref, kz_doc,
                                                generation=generation)
            except IndexFailed as error:
                # The document reached the trash between this pass's read and the writer's
                # own, and the writer refused it permanently. `diverged` already counted it,
                # and that stays: at the moment we read it, the two sides really did
                # disagree. What it does not cost is a repair unit or a lookup-error claim,
                # and the walk moves on — which is the whole point of containing the repair
                # rather than letting one document end the pass.
                logger.info("papra=%s was deleted behind the pass: %s",
                            ref.papra_document_id, error)
                continue
            except (PapraError, KreuzaktError) as error:
                # The repair itself can fail after both sides answered — Papra refuses the
                # PATCH, or one side is gone by the time the writer re-reads it. That is
                # the same fact as a lookup error and gets the same treatment: counted, no
                # budget spent, and the walk goes on to the next reference. `DeliveryDisabled`
                # deliberately travels: the gate changed under a pass that had already
                # promised not to write, which is news about the pass, not about a document.
                logger.warning("verification could not repair papra=%s kreuzakt=%s: %s",
                               ref.papra_document_id, ref.kreuzakt_id, error)
                counts["lookup_errors"] += 1
                continue

            if outcome == DELIVERED:
                # Spent here, after the write landed and was recorded: the only event that
                # deserves the unit, and the reason a refusal cannot reach suppression.
                store.ledger_bump_repair(papra_org_id=ref.papra_org_id,
                                         papra_document_id=ref.papra_document_id)
                counts["repaired"] += 1
            elif outcome == SKIPPED_EMPTY:
                # Unreachable under the check above, kept because the two definitions of
                # empty must not be able to combine into a write.
                counts["empty"] += 1
            elif outcome == SKIPPED_CURRENT:
                pass    # raced: Papra holds our text now, which is the wanted end state
        after = (refs[-1].papra_org_id, refs[-1].papra_document_id)
        if len(refs) < VERIFY_PAGE_SIZE:
            break

    return VerifyResult(**counts)


async def _outcome(papra, store: Store, job: Job, ref: LedgerRef, kreuzakt_id: int,
                   kz_doc: KzDocument, generation: int) -> IndexOutcome:
    """Report what the index holds, and hand the text and the metadata over when the gates
    are open.

    Two gates, two calls, text first: the text is the expensive column and the one whose
    loss the user cannot reconstruct, and a job that fails on it should fail before it has
    renamed anything. Two PATCHes when both are open is the price of keeping the gates
    separate, and it is the right price — see `deliver_metadata`.
    """
    empty = extraction_is_empty(kz_doc.content)
    delivering = store.settings_flag("index_content_delivery")
    if empty:
        # Recorded whatever the gate says. "extraction-empty" is a fact about the index and
        # a line in the ledger, not a delivery, and gating it is how the count stayed a
        # convincing zero in the one configuration every deployment is in until the
        # handover: `check` advertises it as the number to look at when a document should
        # have been OCR'd and wasn't, while steps 1-2 of the handover keep the gate shut for
        # days. `deliver_content` stamps the same sentinel when it refuses for this reason,
        # so both paths write the one value.
        _record_extraction_empty(store, ref, kz_doc, generation)
        if not delivering:
            # Reported, not interpreted: "legitimately blank" is a fact about the document,
            # and nothing here can establish it. While the gate is open `deliver_content`
            # logs the same fact as the reason it wrote nothing, and two lines per document
            # is noise.
            logger.info("extraction-empty papra=%s kreuzakt=%s", job.document_id, kreuzakt_id)
    delivered = False
    if delivering:
        outcome = await deliver_content(papra, store, ref, kz_doc, generation=generation)
        # "delivered" is a claim about Papra's column, which is why a refusal to write an
        # empty extraction is not one: `check` reads this on the index worker's output.
        delivered = outcome in (DELIVERED, SKIPPED_CURRENT)
    metadata = None
    if store.settings_flag("index_metadata_delivery"):
        # Read from the store, like the content gate above, because this is the path that
        # runs unattended for days at a stretch. A flag honoured only by the CLI is a
        # setting that renames nothing while the daemon renames everything, which is the
        # complaint this flag used to deserve. A shut gate is not an incident for a worker,
        # so the setting is tested rather than the exception caught: `metadata=None` means
        # "not this job's business", which is exactly what a shut gate means here.
        metadata = await deliver_metadata(papra, store, ref, kz_doc)
    return IndexOutcome(kreuzakt_id=kreuzakt_id, content_len=len(kz_doc.content),
                        empty_content=empty, delivered=delivered, metadata=metadata)


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
    return not papra_document_is_deleted(document)


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
    if papra_document_is_deleted(document):
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
            # nothing to upload
            return await _outcome(papra, store, job, ref, kreuzakt_id, bound, generation)
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
    return await _outcome(papra, store, job, ref, kreuzakt_id, bound, generation)


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


# --- delete and restore propagation ------------------------------------------------
#
# Task 9: Papra's trash, propagating one way. The rules below exist because the mistake
# available here is not a wrong count — it is a document the user kept losing its index
# entry, or a document the user deleted leaving its text in the index forever. So the pass
# reads everything before it writes anything, treats absence as the weakest signal it is
# given, and never destroys anything itself.
#
# The trust predicate for a listing, spelled out because it is the difference between
# pruning one reference and pruning the index. A bucket's listing is untrustworthy, and the
# whole pass is discarded, when:
#
#   1. `list_documents` raises at all — a paging error, a 5xx, a timeout, a transport failure.
#      An error is not an empty bucket.
#   2. A page contains a row that is not a document, or a document with no usable id. That is
#      a proxy, a login redirect or an error body wearing a 200.
#   3. The same document id is answered two different ways inside one walk. Pages shift under
#      a live listing, so a duplicate is ordinary; a duplicate that contradicts itself means
#      neither answer can be believed.
#   4. The walk ends with fewer rows than the endpoint's own total. The walk is only
#      exhaustive if the server agrees it was exhaustive; a truncated page read as a finished
#      walk is what "everything after row two was deleted" looks like from in here.
#   5. The listing answers empty for a bucket the ledger holds references for. This is a
#      circuit breaker, not a proof: the last document of a bucket genuinely can be purged,
#      and then propagation for that bucket stops until an operator looks at it. The trade is
#      deliberate, because "empty" is also exactly what a broken listing looks like, and the
#      cost of being wrong in that direction is every reference in the corpus.
#   6. The walk reaches `MAX_LISTING_PAGES_PER_ORG` without ever going short. An endpoint that
#      ignores `pageIndex` must not be walked into a mass deletion.
#
# And even a trustworthy listing only *proposes* candidates. Nothing is marked until Papra's
# own detail endpoint confirms that individual document — `deleted` or a genuine 404 — so one
# stale page, a partial listing or a correlated endpoint failure cannot delete by itself. What
# that trust is worth is stated where it is used: it rests on Papra's 404 meaning "this
# document does not exist", which is measured behaviour of the pinned container but is still
# Papra's answer and not a fact we can verify.

#: How many references one pass may mark deleted. A mass deletion is either a user emptying a
#: bucket or a listing that has stopped being trustworthy, and this number is the only thing
#: that decides which one becomes real downstream. It bounds the damage per pass, NOT in
#: total: a persistent fault still drains the corpus at this rate, pass after pass, which is
#: why every mark also needs Papra's own detail confirmation and the aborts above.
MAX_DELETIONS_PER_PASS = 20
#: How many candidates one pass may put a question to Papra about. Deliberately larger than
#: the cap above and separately counted: a candidate Papra's detail read refuses is not a
#: deletion, and if the two shared one budget a handful of unreachable documents could hold
#: every deletion behind them.
MAX_CANDIDATE_PROBES = 50
#: Page size of the listing walk, and how far that walk may go before it is treated as a
#: broken endpoint rather than a large bucket.
LISTING_PAGE_SIZE = 100
MAX_LISTING_PAGES_PER_ORG = 100
#: How often the index is asked whether the documents our bindings name still exist. One full
#: read costs a GET per distinct Kreuzakt id — the same profile as `verify_once` — so it runs
#: on the scale of hours, which is the scale on which the state it looks for changes.
LIVENESS_PROBE_INTERVAL_S = 21600
#: How long a queued upstream delete waits between attempts, and how many it gets. The
#: attempts ceiling is the store's (`CLEANUP_MAX_ATTEMPTS`); the interval is this pass's
#: because it owns the schedule it writes into `cleanup_next_at`.
CLEANUP_BACKOFF_S = 300
#: Rows one cleanup pass drains, so a backlog cannot monopolise the sweep.
CLEANUP_DUE_LIMIT = 50
#: Settings keys. The probe stamp and the candidate cursor are pass state that has to
#: outlive the pass; the last-pass summary is what `check` prints.
LIVENESS_PROBE_STAMP_SETTING = "index_liveness_probe_at"
DELETION_CURSOR_SETTING = "index_deletion_cursor"
LAST_DELETION_PASS_SETTING = "index_last_deletion_pass"

#: What Papra's detail read said about one candidate. `GONE` is a 404 — Papra hard-deletes, so
#: absence from a listing plus a 404 for that specific id is the record's own answer.
DELETED_THERE = "deleted"
GONE = "gone"
LIVE_THERE = "live"


# --- delete and restore propagation ------------------------------------------------
#
# Task 9: Papra's trash, propagating one way. The mistake available here is not a wrong
# count — it is a document the user kept losing its index entry, or a document the user
# deleted leaving its text in the index forever. So this pass reads everything before it
# writes anything, treats absence as the weakest signal it is given, and never destroys
# anything itself: `reconcile_deletions` marks and queues, `run_cleanup` executes.
#
# The trust predicate for a listing, spelled out because it is the difference between
# pruning one reference and pruning the index. A bucket's listing is untrustworthy, and the
# whole pass is discarded, when:
#
#   1. `list_documents` raises at all — a paging error, a 5xx, a timeout, a transport
#      failure. An error is not an empty bucket.
#   2. A page contains a row that is not a document, or a document with no usable id. That
#      is a proxy, a login redirect, or an error body wearing a 200.
#   3. The same document id is answered two different ways inside one walk. Pages shift
#      under a live listing, so a duplicate is ordinary; a duplicate that contradicts
#      itself means neither answer can be believed.
#   4. The walk ends with fewer rows than the endpoint's own total. The walk is only
#      exhaustive if the server agrees it was; a truncated page read as a finished walk is
#      what "everything after row two was deleted" looks like from in here.
#   5. The listing answers empty for a bucket the ledger holds references for. This is a
#      circuit breaker, not a proof: the last document of a bucket genuinely can be purged,
#      and then propagation for that bucket stops until an operator looks. Deliberate,
#      because "empty" is also exactly what a broken listing looks like, and being wrong in
#      that direction costs every reference in the corpus.
#   6. The walk reaches `MAX_LISTING_PAGES_PER_ORG` without ever going short. An endpoint
#      that ignores `pageIndex` must not be walked into a mass deletion.
#
# And even a trustworthy listing only *proposes* candidates. Nothing is marked until Papra's
# own detail endpoint confirms that individual document — deleted, or a genuine 404 — so a
# stale page, a partial listing or a correlated endpoint failure cannot delete by itself. What
# that trust is worth is stated where it is used: it rests on Papra's 404 meaning "this
# document does not exist", which is measured behaviour of the pinned container but is still
# Papra's answer and not a fact we can verify from here.

#: How many references one pass may mark deleted. A mass deletion is either a user emptying a
#: bucket or a listing that has stopped being trustworthy, and this number is the only thing
#: that decides which one becomes real downstream. It bounds the damage per pass, NOT in
#: total: a persistent fault still drains the corpus at this rate, pass after pass, which is
#: why every mark also needs Papra's own detail confirmation and the aborts above.
MAX_DELETIONS_PER_PASS = 20
#: How many candidates one pass may put a question to Papra about. Deliberately larger than
#: the cap above, and counted separately: a candidate Papra's detail read refuses is not a
#: deletion, and if the two shared one budget a handful of unreachable documents could hold
#: every deletion behind them.
MAX_CANDIDATE_PROBES = 50
#: Page size of the listing walk, and how far that walk may go before it is treated as a
#: broken endpoint rather than a large bucket.
LISTING_PAGE_SIZE = 100
MAX_LISTING_PAGES_PER_ORG = 100
#: How often the index is asked whether the documents our bindings name still exist. One full
#: read costs a GET per distinct Kreuzakt id — the same profile as `verify_once` — so it runs
#: on the scale of hours, which is the scale on which the state it looks for changes.
LIVENESS_PROBE_INTERVAL_S = 21600
#: How long a queued upstream delete waits between attempts, and how many rows one pass
#: drains. The attempt ceiling is the store's (`CLEANUP_MAX_ATTEMPTS`, imported at the top of
#: this module rather than restated, because `ledger_cleanup_result` is what enforces it); the
#: interval is this pass's, because this pass owns the schedule it writes into
#: `cleanup_next_at`. Two definitions of one ceiling is how a configured 5 becomes a realised
#: 3, which is the mistake the index job already had to fix.
CLEANUP_BACKOFF_S = 300
CLEANUP_DUE_LIMIT = 50
#: Settings keys. The probe stamp and the candidate cursor are pass state that has to outlive
#: the pass; the last-pass summary is the line `check` prints.
LIVENESS_PROBE_STAMP_SETTING = "index_liveness_probe_at"
DELETION_CURSOR_SETTING = "index_deletion_cursor"
LAST_DELETION_PASS_SETTING = "index_last_deletion_pass"

#: What Papra's detail read said about one candidate. `GONE` is a 404: Papra hard-deletes, so
#: absence from a listing plus a 404 for that specific id is the record's own answer.
DELETED_THERE = "deleted"
GONE = "gone"
LIVE_THERE = "live"


@dataclass(frozen=True)
class DeletionResult:
    """What one delete/restore pass did, counted in references.

    `aborted` and `capped` are not metrics, they are alarms, and neither may be silent: a
    pass that refused to touch anything for a week and a pass that deleted twenty of five
    hundred documents both look like a quiet corpus from the ledger alone. So the sweep logs
    them at ERROR and `check` prints the last pass's outcome, which is why this pass stamps
    its result into `settings` instead of only returning it.

    Three fields carry meanings worth stating rather than assuming:

    * `upstream_pending` is the durable queue — the intent written, nothing destroyed. The
      marking pass never deletes anything itself; `run_cleanup` does, after revalidating.
    * `upstream_deleted` counts the references whose upstream end state this pass settled
      *without* a delete call: the index no longer has that document at all, so the debt is
      paid rather than owed. Without that check the intent would fail five times against a
      404 and land in `failed`, which reads as an outage and is nothing of the sort.
    * `stale_bindings` is the `(-1, -1)` answer from `ledger_mark_deleted` counted as its own
      outcome. A reference bound to a superseded index is still marked deleted — the
      reference is gone whatever index it pointed at — but it queues nothing, and a corpus
      full of those has to be visible as itself rather than as a corpus with nothing to do.
    """

    refs_marked: int = 0
    upstream_deleted: int = 0
    upstream_pending: int = 0
    restored: int = 0
    aborted: bool = False
    capped: int = 0
    stale_bindings: int = 0


@dataclass(frozen=True)
class CleanupResult:
    """What one cleanup pass did, with one arithmetic identity pinned by a test.

    `attempted` is every queued intent the pass took off the due list and judged — not the
    calls it made — because the number an operator needs is "how much of the debt did this
    pass handle", and a pass that refused forty of them is not a pass that did nothing. Then
    `attempted == deleted + deferred + failed`, exactly one bucket per intent, so a fourth
    outcome cannot arrive without breaking the identity.

    `deferred` is "we looked, and we did not delete": the reference is no longer deleted, the
    generation moved, the intent names no id, a live reference still resolves to it, or the
    index was not answering and the pass stopped rather than spend anyone's retries. Most of
    those are terminal for the row (`skipped`), and each writes its own INFO line, so the
    count never stands in for the reason.
    """

    attempted: int = 0
    deleted: int = 0
    deferred: int = 0
    failed: int = 0


class _ListingUntrusted(Exception):
    """One bucket's listing did not answer in a shape absence may be read from.

    Private and never surfaced: the caller's contract is `aborted=True` plus one ERROR line
    naming the bucket and the page, which is what an operator acts on.
    """

    def __init__(self, message: str, *, org_id: str, page: int) -> None:
        super().__init__(f"{message} (org={org_id} page={page})")
        self.message = message
        self.org_id = org_id
        self.page = page


def _shift(now: str, *, seconds: int) -> str:
    """`now` plus a delay, rendered in the store's own text form.

    Every timestamp this module writes is compared as *text* inside the ledger —
    `cleanup_next_at <= now` — which is chronological only while every stored value carries the
    same zero offset. So a delay is computed on parsed values and rendered with the store's own
    formatter rather than a local `strftime`: a `Z` suffix sorts after every offset, a naive
    value is silently hours away from what it means, and the only symptom is a delete that fired
    early or a row that never came due. The write boundary validates as well
    (`_require_store_utc_iso`, reached through `ledger_cleanup_result`), because a guard nobody
    can see is a guard that gets stepped on.
    """
    return _iso(datetime.fromisoformat(now) + timedelta(seconds=seconds))


def _stamp_expired(stamp: str | None, *, interval_s: int, now: str) -> bool:
    """Has the last full probe fallen out of its interval?

    A corrupt stamp counts as expired. It is a schedule rather than a record: the cost of
    doubting it is one extra read of the index, while the cost of trusting garbage is a corpus
    whose dead bindings nobody ever re-checks.
    """
    if not stamp:
        return True
    try:
        return _shift(stamp, seconds=interval_s) <= now
    except (TypeError, ValueError):
        return True


async def _walk_listing(papra, org_id: str, *, expected_refs: int) -> dict[str, bool]:
    """Every document Papra lists for one bucket, as `{document_id: deleted_there}`.

    Raises `_ListingUntrusted` for any of the six conditions named at the top of this
    section; the caller drops the whole pass on one. The return value is the only place the
    walk's verdict exists, and it is deliberately a *complete* set of ids: a document missing
    from the dict is what the caller reads as "Papra did not list this one", which is a
    candidate, and it must not be confusable with "the walk never got this far" — that case
    does not return.

    `expected_refs` is what the ledger holds for this bucket, and it is what makes an empty
    answer suspicious rather than merely surprising. It counts every reference, deleted ones
    included, because the question is not "is there work here" but "could this bucket really
    be empty" — and the answer to that is no while the ledger still carries rows for it.
    """
    listed: dict[str, bool] = {}
    rows_seen = 0
    reported: int | None = None
    page_index = 0
    while True:
        try:
            documents, count = await papra.list_documents(
                org_id, page_index=page_index, page_size=LISTING_PAGE_SIZE,
                sort_field="createdAt", sort_order="desc")
        except Exception as error:
            raise _ListingUntrusted(
                f"listing failed: {type(error).__name__}: {error}",
                org_id=org_id, page=page_index) from error
        rows = list(documents or ())
        rows_seen += len(rows)
        if isinstance(count, int):
            reported = count
        for document in rows:
            if not isinstance(document, dict):
                raise _ListingUntrusted(
                    f"a listing row is {type(document).__name__}, not a document",
                    org_id=org_id, page=page_index)
            document_id = document.get("id")
            if not isinstance(document_id, str) or not document_id:
                raise _ListingUntrusted(
                    f"a listing row carries no usable id: {document_id!r}",
                    org_id=org_id, page=page_index)
            deleted_there = papra_document_is_deleted(document)
            if document_id in listed and listed[document_id] != deleted_there:
                raise _ListingUntrusted(
                    f"document {document_id} answered "
                    f"{'deleted' if deleted_there else 'live'} after answering "
                    f"{'deleted' if listed[document_id] else 'live'}",
                    org_id=org_id, page=page_index)
            listed[document_id] = deleted_there
        if len(rows) < LISTING_PAGE_SIZE:
            break
        page_index += 1
        if page_index >= MAX_LISTING_PAGES_PER_ORG:
            raise _ListingUntrusted(
                f"{MAX_LISTING_PAGES_PER_ORG} pages in a row came back full, so the endpoint "
                "is not paging", org_id=org_id, page=page_index)
    if reported is not None and rows_seen < reported:
        raise _ListingUntrusted(
            f"the endpoint reported {reported} documents and served {rows_seen}",
            org_id=org_id, page=page_index)
    if not listed and expected_refs:
        raise _ListingUntrusted(
            f"the listing answered empty while the ledger holds {expected_refs} "
            "reference(s) for this bucket", org_id=org_id, page=0)
    return listed


async def _ask_papra(papra, org_id: str, document_id: str) -> str | None:
    """Ask Papra's detail endpoint about one candidate. `None` means it did not answer.

    This is the second and stronger question behind the listing, and it is the only thing that
    turns a candidate into a mark. `GONE` is a 404 *for this document id*, which is Papra
    saying it has never heard of the one record we are asking about — a different kind of
    answer from a listing that did not include it, because a broken listing stops listing
    everything while a 404 is about exactly one thing. Any other failure answers `None`, and
    `None` marks nothing: a document whose detail will not load is a document nothing is known
    about, and an outage on the read side must not be spent as evidence of a deletion.
    """
    try:
        document = await papra.get_document(org_id, document_id)
    except PapraError as error:
        if error.status == 404:
            return GONE
        logger.warning("deletion candidate papra=%s/%s could not be read: %s",
                       org_id, document_id, error)
        return None
    except Exception as error:
        logger.warning("deletion candidate papra=%s/%s could not be read: %s: %s",
                       org_id, document_id, type(error).__name__, error)
        return None
    return DELETED_THERE if papra_document_is_deleted(document) else LIVE_THERE


def _rotate(candidates: list[tuple[str, str]], store) -> list[tuple[str, str]]:
    """The next `MAX_CANDIDATE_PROBES` candidates, starting where the last pass stopped.

    Rotation is not a nicety. A candidate that will not confirm — its detail read errors, or
    Papra says live while the listing said trash — stays a candidate on the next pass too, so
    a fixed window anchored at the start of the sorted list probes the same refusals forever
    and never reaches the documents behind them. The pass would report `refs_marked=0` every
    pass and look exactly like a corpus with nothing to do.

    The cursor is modulo the list, so it stays valid when the candidate set shrinks (a mark
    takes a candidate out of the population) and when it grows. It is pass state in the
    settings table rather than a local, because the pass runs once per sweep in a process
    expected to restart.
    """
    if not candidates:
        store.set_setting(DELETION_CURSOR_SETTING, "0")
        return []
    try:
        start = int(store.get_setting(DELETION_CURSOR_SETTING) or "0")
    except ValueError:
        start = 0                      # a corrupt cursor must not wedge the pass
    start %= len(candidates)
    ordered = candidates[start:] + candidates[:start]
    window = ordered[:MAX_CANDIDATE_PROBES]
    store.set_setting(DELETION_CURSOR_SETTING,
                      str((start + len(window)) % len(candidates)))
    return window


async def _index_lacks_document(kz, kreuzakt_id: int) -> bool | None:
    """Does the index answer 404 for this document? `None` means it did not answer at all.

    Three answers, not two, because the difference between "the index has forgotten it" and
    "the index is down" is the difference between a repair and a corpus of wrongly dropped
    bindings. A caller that treats `None` as `True` would unbind every reference in the
    ledger during an outage, and unbinding is the expensive mistake: it makes a live document
    unfindable, and its rows become backlog that has to be re-uploaded to be found again.
    """
    try:
        document = await kz.get_document(kreuzakt_id)
    except KreuzaktError as error:
        if error.status == 404:
            return True
        logger.warning("kreuzakt=%s could not be read: %s", kreuzakt_id, error)
        return None
    except Exception as error:
        logger.warning("kreuzakt=%s could not be read: %s: %s",
                       kreuzakt_id, type(error).__name__, error)
        return None
    return document is None


def _readmit_index_job(store: Store, ref: LedgerRef, *, reason: str) -> None:
    """Ask for this document to be indexed again, and mean it.

    The trap is in the queue, not here: `enqueue_job` is `INSERT OR IGNORE` against a unique
    idempotency index, so an already-`failed` row for these bytes makes every re-enqueue a
    silent no-op that returns the old id — pinned in
    `tests/test_metadata_delivery.py::test_a_permanently_failed_index_row_makes_the_next_enqueue_a_noop`.
    A restore or an unbind that only called `enqueue_job` would therefore report success and
    change nothing, and the document would stay missing from search while the log said it was
    being indexed. So the pass reads the row back and resets it when it is spent: that is the
    only way this call can honestly be called a re-admission.

    The durable fix is Task 11's partial unique index (unique only over rows that can still
    run), after which a terminal row stops shadowing a fresh request and this reset becomes
    redundant rather than load-bearing. Until then it is the mechanism, and it logs, because
    "we reset a job the queue had given up on" is the sentence that explains a document that
    came back. `worker_loop` already shouts about this trap when a job dies; resetting it here
    is the automated counterpart of the `jobs --reset` that message tells an operator to run.

    It does not check whether the row is *eligible* — `ledger_needs_index` is the eligibility
    rule, and it is the caller's job to have made the row eligible first (an unbound row, or a
    binding that is not current). A spurious extra index job costs one idempotent upload; a
    missing one costs a document that is never found.
    """
    job_id = store.enqueue_job(
        kind="index", papra_org_id=ref.papra_org_id, document_id=ref.papra_document_id,
        content_hash=ref.file_hash,
        payload={"file_hash": ref.file_hash, "name": ref.papra_original_name or "",
                 "ingested_via": reason})
    job = store.get_job(job_id)
    if job is not None and job.status in ("failed", "done"):
        store.reset_job(job_id)
        logger.info(
            "re-admitted index job %s for papra=%s/%s (%s): the queue's idempotency index "
            "had it pinned as %s, so a plain enqueue would have been a no-op",
            job_id, ref.papra_org_id, ref.papra_document_id, reason, job.status)


async def _apply_mark(store: Store, kz, ref: LedgerRef, *,
                      generation: int) -> str:
    """Mark one reference deleted, queue the upstream delete if nothing else needs it.

    Returns `queued` | `closed` | `shared` | `unbound` | `stale`, which is the same
    distinction `DeletionResult` counts — with `shared` and `unbound` deliberately counted
    nowhere, because "a sibling still needs this document" and "this was never indexed" are
    both ordinary outcomes of an ordinary corpus, and a counter that includes them cannot be
    the alarm the two real anomalies need.

    The order is the point, and it is an order the store forces: `ledger_mark_deleted`
    soft-deletes the row and counts the surviving references in one immediate transaction, so
    the number below is the count *at the moment the row became deleted* rather than a number
    read a statement earlier. It is also only the queue-side half of the guard — the cleanup
    pass takes the count again before it destroys anything, because a queued intent is a claim
    about the future and a reference that comes back between the two is right and the intent
    is wrong.

    Nothing is deleted here. The last reference's intent goes into `cleanup_state`, and
    `run_cleanup` performs it, so a crash between the two leaves a debt the next process can
    see.
    """
    kreuzakt_id, remaining = store.ledger_mark_deleted(
        papra_org_id=ref.papra_org_id, papra_document_id=ref.papra_document_id)
    if ref.kreuzakt_id is None:
        # Never indexed, so nothing upstream was ever created for it. The mark is the whole
        # story — and it is not an anomaly. A corpus with many never-indexed documents is
        # backlog, not damage, and warning about each one would bury the case below, which is.
        logger.info("marked papra=%s/%s deleted with no binding: never indexed, so nothing is "
                    "owed upstream", ref.papra_org_id, ref.papra_document_id)
        return "unbound"
    if kreuzakt_id is None or kreuzakt_id <= 0:
        # Bound, but under an index that no longer exists: `mark_deleted` answered `(-1, -1)`
        # for a row that carries an id. The row still got its `deleted_at` — the reference is
        # gone either way — it just queues nothing, because a stale integer names no document
        # we may destroy, and the same number may belong to different content now. The row as
        # it was read before the mark is the only place the two histories differ, so the
        # distinction is taken there and counted separately.
        logger.warning(
            "marked papra=%s/%s deleted with a stale binding to a superseded generation: "
            "nothing queued, because that integer names nothing we may destroy",
            ref.papra_org_id, ref.papra_document_id)
        return "stale"
    outcome = "shared"
    if remaining == 0:
        # The last live reference of the current generation. Ask the index whether the
        # document is even still there before booking the debt: if it is gone, the intent
        # would fail five times against a 404 and land in `failed`, which reads as an outage.
        gone = await _index_lacks_document(kz, kreuzakt_id)
        store.ledger_queue_cleanup(papra_org_id=ref.papra_org_id,
                                   papra_document_id=ref.papra_document_id,
                                   due_at=_store_now(), kreuzakt_id=kreuzakt_id,
                                   generation=generation)
        if gone is True:
            store.ledger_cleanup_result(papra_org_id=ref.papra_org_id,
                                        papra_document_id=ref.papra_document_id, ok=True)
            outcome = "closed"
        else:
            outcome = "queued"
    # Stop the dead reference occupying the id. Scoped to this row: a sibling in another
    # bucket may still be live and still needs the binding, and clearing it would unfind a
    # document nobody deleted.
    store.ledger_unbind(
        expected_kreuzakt_id=kreuzakt_id, generation=generation,
        reason=f"papra deleted {ref.papra_org_id}/{ref.papra_document_id}",
        papra_org_id=ref.papra_org_id, papra_document_id=ref.papra_document_id)
    return outcome


def _apply_restore(store: Store, ref: LedgerRef) -> None:
    """Bring one reference back, and refuse to assume the index still holds it.

    `ledger_mark_restored` clears `deleted_at` and nothing else, which is correct as far as it
    goes — a restore is not a human edit, so it sets no protection, and it leaves a queued
    upstream delete for the cleanup pass to revalidate and skip rather than rewriting the
    state machine from here. On its own it is not enough, for two reasons this pass has to
    finish:

    * **The binding cannot be trusted.** If the cleanup pass already deleted the document, the
      row is bound to an id that answers 404, and — because the row is live again — it now
      *resolves*, so every token that matches hands out a Kreuzakt document that does not
      exist. That is worse than the row being missing, and it is the state Task 9 exists to
      escape. Deciding per-row whether the upstream delete actually ran would mean reading
      `cleanup_state` and trusting it to describe Kreuzakt; instead the binding is dropped
      unconditionally and the work is re-admitted. The cost when the document survived is one
      idempotent re-upload, because Kreuzakt deduplicates identical bytes; the cost when it
      did not is a permanently broken hit.
    * **Nothing will ask for the work again.** So the row is made eligible (unbound) and the
      index job is re-admitted, reset if the queue had pinned it as spent.

    A row that was bound under a superseded generation is unbound the same way: its binding is
    already meaningless, and dropping it is what makes the row visible to this generation's
    passes instead of invisible to all of them.
    """
    store.ledger_mark_restored(papra_org_id=ref.papra_org_id,
                               papra_document_id=ref.papra_document_id)
    after = store.ledger_get(ref.papra_org_id, ref.papra_document_id)
    if after is None:
        return
    if after.kreuzakt_id is not None:
        store.ledger_unbind(
            expected_kreuzakt_id=after.kreuzakt_id,
            generation=after.delivered_generation if after.delivered_generation is not None
            else store.get_generation(),
            reason="restored in papra: the index entry cannot be assumed to have survived "
                   "the trash",
            papra_org_id=after.papra_org_id, papra_document_id=after.papra_document_id)
    _readmit_index_job(store, after, reason="restored in papra")


async def probe_index_liveness(store: Store, kz, generation: int) -> int:
    """Ask the index whether the documents our bindings still name; drop the ones that do not.

    Returns documents read, and runs at most once per `LIVENESS_PROBE_INTERVAL_S` — the gate
    is a stamp in `settings`, because this is a GET per distinct Kreuzakt id and the sweep that
    calls it runs every few minutes. The state it looks for changes on the scale of hours, so a
    tighter cadence buys nothing and would make this the most expensive thing in the process.

    What it is for: Task 6 left a state pinned as a known limitation. A row can end up bound,
    stamped as delivered under the *current* generation, with its index job permanently failed
    because Kreuzakt answered 404 for the id. `ledger_needs_index` excludes exactly that
    combination, so the sweep, a rebuild and a backfill all walk past it, and the row still
    resolves — a search hit that leads nowhere, forever. Nothing in this codebase detects it,
    because nothing else asks Kreuzakt about an id we already believe we delivered. This probe
    is the detection, and the unbind plus `_readmit_index_job` are the recovery: unbound, the
    row is eligible again, and the reset job is what makes "eligible" mean a worker will claim
    it.

    The verdict is a 404 and nothing else. `None` — the index not answering — leaves every
    binding alone, and it also leaves the stamp unwritten, so the next pass tries again instead
    of waiting out the interval on the strength of a walk that read nothing. Where a read
    failed, the pass reports how many references it could not judge rather than reporting a
    clean corpus.
    """
    now = _store_now()
    stamp = store.get_setting(LIVENESS_PROBE_STAMP_SETTING)
    if not _stamp_expired(stamp, interval_s=LIVENESS_PROBE_INTERVAL_S, now=now):
        return 0
    probed = 0
    unbound = 0
    unreadable = 0
    seen: set[int] = set()
    after: tuple[str, str] | None = None
    while True:
        refs = store.ledger_delivered_refs(generation=generation, after=after,
                                           limit=VERIFY_PAGE_SIZE)
        if not refs:
            break
        for ref in refs:
            kreuzakt_id = ref.kreuzakt_id
            if kreuzakt_id is None or kreuzakt_id in seen:
                continue           # one document, however many references point at it
            seen.add(kreuzakt_id)
            verdict = await _index_lacks_document(kz, kreuzakt_id)
            if verdict is None:
                unreadable += 1
                continue
            probed += 1
            if verdict is not True:
                continue
            # The verdict is about the document, so every reference to it follows: leaving one
            # bound would leave it resolving to an id that answers 404.
            affected = store.ledger_resolve([kreuzakt_id], generation=generation)
            unbound += store.ledger_unbind(
                expected_kreuzakt_id=kreuzakt_id, generation=generation,
                reason=f"kreuzakt document {kreuzakt_id} answers 404 for a current binding")
            for item in affected:
                _readmit_index_job(store, item, reason="liveness probe found the document gone")
        after = (refs[-1].papra_org_id, refs[-1].papra_document_id)
        if len(refs) < VERIFY_PAGE_SIZE:
            break
    if not unreadable:
        store.set_setting(LIVENESS_PROBE_STAMP_SETTING, now)
    if unbound:
        logger.warning(
            "unbound %d reference(s) whose kreuzakt document answers 404 and re-admitted "
            "their index work; they were resolving to nothing", unbound)
    if unreadable:
        logger.warning(
            "liveness probe could not judge %d bound document(s) because kreuzakt did not "
            "answer; no binding was dropped and the next pass will look again", unreadable)
    return probed


async def reconcile_deletions(store: Store, papra, kz) -> DeletionResult:
    """Propagate Papra's trash into the index, and restore back the other way.

    Three phases, in this order, and the order is the safety property:

    1. **Read every bucket.** Each one's listing is walked to exhaustion and judged against the
       trust predicate at the top of this section. Any bucket that fails it discards the whole
       pass — `aborted=True`, nothing written — because the pass's central inference is
       *absence*, and an inference from absence is only as good as the completeness of the
       listing it came from. Half a corpus's verdicts cannot be kept while the other half is
       doubted: the marks that were already applied are the ones nobody questioned.
    2. **Confirm each candidate individually.** A listing that is trustworthy enough to propose
       is not evidence about one document, so each candidate is put to Papra's detail endpoint
       (bounded by `MAX_CANDIDATE_PROBES`, rotated so refusals cannot starve the tail). Only
       `deleted` or a 404 marks anything; a live answer vetoes the mark and, if the ledger was
       already marked, becomes a restore.
    3. **Then write.** Marks, restores, the liveness probe, and one summary stamp.
       The probe is inside that ordering, so an aborted pass does not run it either: a pass
       that could not read the record does not spend the interval on repair work, and the
       stamp it leaves says `aborted` instead of recording a probe that never happened.

    Candidates are the ledger compared against the listing in both directions: a live row whose
    document Papra says is in the trash, a live row whose document Papra no longer lists at all
    (a purge — the listing cannot prove it, so the 404 has to), and a deleted row whose
    document Papra lists live again. Deleted rows are read from `ledger_org_refs`, the one walk
    that hides no state: `ledger_resolve` hides the soft-deleted rows that are exactly the
    restore population.

    Nothing here destroys anything. `MAX_DELETIONS_PER_PASS` bounds how many references are
    marked per pass, and the upstream delete waits for `run_cleanup`.
    """
    generation = store.get_generation()
    counts = {"refs_marked": 0, "upstream_deleted": 0, "upstream_pending": 0,
              "restored": 0, "capped": 0, "stale_bindings": 0}
    proposed: list[tuple[str, str]] = []
    for org_id in store.ledger_org_ids():
        expected_refs = store.ledger_stats(org_ids=[org_id])["references"]
        try:
            listed = await _walk_listing(papra, org_id, expected_refs=expected_refs)
        except _ListingUntrusted as error:
            logger.error(
                "listing untrustworthy, skipping deletion pass entirely org=%s page=%s: %s",
                error.org_id, error.page, error.message)
            store.set_setting(LAST_DELETION_PASS_SETTING,
                              f"aborted: {error.org_id} page {error.page}: {error.message}")
            return DeletionResult(aborted=True)
        for ref in _walk_org_refs(store, org_id):
            deleted_there = listed.get(ref.papra_document_id)
            if ref.deleted_at is None and deleted_there is True:
                proposed.append((org_id, ref.papra_document_id))
            elif ref.deleted_at is None and deleted_there is None:
                # Not listed: the strongest-looking absence in the whole pass, and the reason a
                # detail read is mandatory. An id that stopped being listed is also what a
                # broken listing, a filtered listing and a bucket rename all produce.
                proposed.append((org_id, ref.papra_document_id))
            elif ref.deleted_at is not None and deleted_there is False:
                proposed.append((org_id, ref.papra_document_id))
    marks: list[LedgerRef] = []
    restores: list[LedgerRef] = []
    for org_id, document_id in _rotate(sorted(proposed), store):
        verdict = await _ask_papra(papra, org_id, document_id)
        if verdict is None:
            continue
        ref = store.ledger_get(org_id, document_id)
        if ref is None:
            continue
        if verdict in (DELETED_THERE, GONE):
            if ref.deleted_at is None:
                marks.append(ref)
            continue
        if ref.deleted_at is not None:
            restores.append(ref)
        else:
            # The listing proposed a deletion and the document itself denies it. The live answer
            # wins, loudly: this is the pair of Papra endpoints disagreeing about one record,
            # and which one is stale decides whether a document keeps its index entry.
            logger.warning(
                "deletion candidate papra=%s/%s refused: the listing did not say deleted but "
                "the detail read says live, so nothing was marked", org_id, document_id)
    marks.sort(key=lambda ref: (ref.papra_org_id, ref.papra_document_id))
    if len(marks) > MAX_DELETIONS_PER_PASS:
        counts["capped"] = MAX_DELETIONS_PER_PASS
        logger.error(
            "deletion pass capped at %d references this pass, %d left for the next: if this "
            "repeats without anyone deleting anything, suspect the listing",
            MAX_DELETIONS_PER_PASS, len(marks) - MAX_DELETIONS_PER_PASS)
        marks = marks[:MAX_DELETIONS_PER_PASS]
    for ref in marks:
        outcome = await _apply_mark(store, kz, ref, generation=generation)
        counts["refs_marked"] += 1
        if outcome == "queued":
            counts["upstream_pending"] += 1
        elif outcome == "closed":
            counts["upstream_deleted"] += 1
        elif outcome == "stale":
            counts["stale_bindings"] += 1
    for ref in restores:
        _apply_restore(store, ref)
        counts["restored"] += 1
    probed = await probe_index_liveness(store, kz, generation)
    store.set_setting(LAST_DELETION_PASS_SETTING,
                      "ok marked={refs_marked} queued={upstream_pending} closed="
                      "{upstream_deleted} restored={restored} stale={stale_bindings} "
                      "capped={capped} probed={probed}".format(probed=probed, **counts))
    return DeletionResult(**counts)


def _walk_org_refs(store: Store, org_id: str) -> list[LedgerRef]:
    """Every reference of one bucket, pages and duplicates collapsed.

    Keyset paging, so a row that changes under the walk cannot shift the window and hide a
    neighbour — the failure the other walks in `store` use a keyset for, and the reason the
    pass that infers from absence must not be the one that walks with an offset.

    The `seen` guard is not paranoia about the store: it is protection against a caller whose
    `after` tuple repeats a row, which with an offset-free keyset would loop forever on a
    bucket large enough to page. A candidate appearing twice costs one wasted detail read, so
    duplicates are dropped here rather than treated as a reason to abort.
    """
    refs: list[LedgerRef] = []
    seen: set[tuple[str, str]] = set()
    after: tuple[str, str] | None = None
    while True:
        page = store.ledger_org_refs(org_id=org_id, after=after, limit=VERIFY_PAGE_SIZE)
        if not page:
            break
        for ref in page:
            key = (ref.papra_org_id, ref.papra_document_id)
            if key in seen:
                continue
            seen.add(key)
            refs.append(ref)
        after = (page[-1].papra_org_id, page[-1].papra_document_id)
        if len(page) < VERIFY_PAGE_SIZE:
            break
    return refs


async def run_cleanup(store: Store, kz, *, now: str | None = None) -> CleanupResult:
    """Perform the upstream deletes the ledger says are owed. This is the only place anything
    in Task 9 destroys anything, and even here only what the record has already let go of.

    Every row taken off the due list is *judged* before it is acted on, because the queue is a
    claim about a moment that has already passed. Four checks, each of which can veto the delete
    on its own, in the order a wrong answer is cheapest to catch:

    1. the reference is still deleted in the ledger — if it is not, the user restored it and the
       intent is stale;
    2. the intent names a real Kreuzakt id — `ledger_queue_cleanup` refuses to store anything
       else, so a `-1` here would be a corrupt row, not a decision;
    3. `cleanup_generation` still equals the current generation — after a rebuild the same
       integer may name different content, and the queued id says nothing about what it points
       at now;
    4. **no live reference of the current generation resolves to that id** — the count taken at
       queue time, taken again. This is the one that holds when the queueing side was wrong: a
       document restored between the mark and the delete, or a second bucket that arrived in
       the meantime and was indexed onto the same id because Kreuzakt deduplicates identical
       bytes. Deleting then removes content the survivor cannot rebuild from the index.

    A transport failure stops the pass for the rows it has not reached. It is not an attempt:
    "there is no answer" spent like "the answer is no" would walk every queued delete in the
    corpus to `failed` in five passes, and an operator would come back to a debt only a forced
    pass can retry. So nothing is recorded, the rows stay pending with their attempts intact,
    and the pass says so at ERROR. A 404 *is* an answer, and the wanted one — Kreuzakt has
    already forgotten the document, which `KreuzaktClient.delete_document` reports as success.

    `now` exists for the schedule to be testable rather than for production use; it is validated
    against the store's own timestamp form and the refusal is allowed to propagate, because a
    naive `now` would produce a `cleanup_next_at` that sorts wrong — a delete firing early or a
    row never coming due, with no error anywhere.
    """
    moment = _store_now() if now is None else _require_store_utc_iso(now, "now")
    current = store.get_generation()
    due = store.ledger_cleanup_due(now=moment, limit=CLEANUP_DUE_LIMIT)
    attempted = len(due)
    deleted = deferred = failed = 0
    for position, ref in enumerate(due):
        where = {"papra_org_id": ref.papra_org_id, "papra_document_id": ref.papra_document_id}
        kreuzakt_id = ref.cleanup_kreuzakt_id
        if ref.deleted_at is None:
            store.ledger_skip_cleanup(**where,
                                      reason="the papra reference is no longer deleted")
            deferred += 1
            continue
        if kreuzakt_id is None or kreuzakt_id <= 0:
            store.ledger_skip_cleanup(**where,
                                      reason="the queued intent names no kreuzakt id")
            deferred += 1
            continue
        if ref.cleanup_generation != current:
            store.ledger_skip_cleanup(
                **where,
                reason=f"generation changed: queued under {ref.cleanup_generation}, the index "
                       f"is now {current}, so the queued id may name other content")
            deferred += 1
            continue
        live = store.ledger_resolve([kreuzakt_id], generation=current)
        if live:
            store.ledger_skip_cleanup(
                **where,
                reason=f"a live reference still resolves to kreuzakt={kreuzakt_id}: "
                       + ", ".join(f"{item.papra_org_id}/{item.papra_document_id}"
                                   for item in live))
            deferred += 1
            continue
        try:
            await kz.delete_document(kreuzakt_id)
        except KreuzaktError as error:
            if error.status is None:
                logger.error(
                    "kreuzakt is not answering, stopping the cleanup pass before it spends "
                    "the retry allowance of %d queued delete(s): %s",
                    len(due) - position, error)
                deferred += len(due) - position
                break
            failed += 1
            store.ledger_cleanup_result(**where, ok=False, reason=str(error),
                                        next_at=_shift(moment,
                                                       seconds=CLEANUP_BACKOFF_S
                                                       * (ref.cleanup_attempts + 1)))
            continue
        store.ledger_cleanup_result(**where, ok=True)
        deleted += 1
        # The document is gone, so no reference may still claim it is indexed. Unscoped on
        # purpose: the last live reference was the condition for getting here, and any row
        # still bound to this id is bound to something that no longer exists.
        store.ledger_unbind(
            expected_kreuzakt_id=kreuzakt_id,
            generation=ref.cleanup_generation if ref.cleanup_generation is not None
            else current,
            reason=f"kreuzakt document {kreuzakt_id} deleted by the cleanup pass")
    return CleanupResult(attempted=attempted, deleted=deleted, deferred=deferred,
                         failed=failed)
