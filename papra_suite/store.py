import hashlib
import json
import logging
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def _require_store_utc_iso(value: str, field: str) -> str:
    """Reject a timestamp the store could not have produced itself.

    Backoffs are compared as text — `cleanup_next_at <= ?` — which is
    chronological only while every stored value carries the same `+00:00` offset.
    A `Z` suffix sorts after every offset, a naive value and a `+01:00` one are
    silently hours away from what they mean, and the only symptom is a delete that
    fired early or a row that never came due: a mis-timed backoff with no error
    anywhere. The write boundary is therefore the honest place to guard, and it is
    cheaper than moving every comparison in this file onto timestamp arithmetic.

    The accepted form is exactly what `_now()`/`_iso()` produce: the offset must be
    zero and `fromisoformat(...).isoformat()` must reproduce the input, which
    refuses `Z`, a space separator, a missing offset and a non-zero offset.
    """
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{field} must be a UTC ISO-8601 timestamp like {_now()}; got {value!r}"
        ) from exc
    if (parsed.tzinfo is None or parsed.utcoffset() != timedelta(0)
            or parsed.isoformat() != value):
        raise ValueError(
            f"{field} must be the store's own UTC form — 'T' separator, +00:00"
            f" offset — like {_now()}; got {value!r}"
        )
    return value


LOG_RING_BUFFER_SIZE = 1000

VALID_SSL_MODES = ("ssl", "starttls", "plain")

# How long a "this is why nothing arrived" row stays worth keeping. Ninety days
# outlives every realistic "where is my invoice?" question while keeping the
# table small enough to read; older than that, the answer belongs to the logs or
# to `replay`, not to this window.
INGEST_EVENT_RETENTION_DAYS = 90

# -- index ledger constants ---------------------------------------------------

# Kreuzakt ids are only meaningful inside one index generation. This is the
# settings key that holds the current one; it starts at 1 and is bumped when
# Kreuzakt is wiped or restored from a different backup, after which every
# existing id may name a different document.
INDEX_GENERATION_KEY = "index_generation"

DEFAULT_GENERATION = 1

# How long a repair counts toward "this document keeps diverging". Two repairs
# inside this window is the suppression threshold Task 7 acts on, so the window
# and the counter have to agree about what "recent" means.
REPAIR_WINDOW_H = 24

# How many failed upstream deletes `ledger_cleanup_result` tolerates before it
# stops retrying and leaves the row in `cleanup_state='failed'`, which no automatic
# pass will touch again. Five is "the outage is not a blip" without being "we gave up
# on the operator's data".
CLEANUP_MAX_ATTEMPTS = 5

# How many refused content writes a reference accumulates before the convergence pass
# stops asking. It is a ceiling on *refusals*, and it is deliberately not
# `MAX_REPAIRS_IN_WINDOW`: that one counts writes that landed and were overwritten again,
# and a PATCH Papra refused landed nothing, so the two budgets answer different questions
# and a document can be at one ceiling while nowhere near the other. Five is the same
# "not a blip" judgement as the cleanup cap, made for the same reason: below it an outage
# is ordinary, above it a six-hourly pass is shouting at a wall for years.
DELIVERY_MAX_FAILURES = 5

# The three fields we may write into Papra, and therefore the only fields that
# can be protected. A field name outside this set is a programming error, never
# a column name to interpolate.
PROTECTION_FIELDS = ("name", "date", "notes")

# Spellings an operator types into a settings row that mean "on". Anything else —
# including a missing key and a typo like `ture` — is off: a delivery flag that
# fails open would let the bridge write into Papra while the old writers still
# own those fields, which is the one state this milestone is designed to prevent.
TRUTHY_SETTING_VALUES = ("1", "true", "yes", "on")

# sha256 of the empty string. Kreuzakt returning no text is a *fact about the
# document*, and `extraction_empty` in `ledger_stats` is the operator's only
# window onto how often that happens (§9's quality signal for the extractor).
# The ledger cannot observe Kreuzakt's content, so a caller that saw an empty
# extraction records it by delivering against this hash; nothing here invents it.
EMPTY_EXTRACTION_SHA = hashlib.sha256(b"").hexdigest()

# `ledger_stats`' keys and the SQL aliases that carry them. The pairs are spelled
# out because `references` is a SQL keyword: its alias is `n_references`, so the
# mapping has to exist somewhere rather than be assumed to be the identity.
LEDGER_STAT_KEYS = (("references", "n_references"), ("indexed_current", "indexed_current"),
                    ("delivered", "delivered"), ("extraction_empty", "extraction_empty"),
                    ("protected_name", "protected_name"),
                    ("protected_date", "protected_date"),
                    ("protected_notes", "protected_notes"),
                    ("repairs_24h", "repairs_24h"),
                    ("cleanup_pending", "cleanup_pending"),
                    ("cleanup_failed", "cleanup_failed"),
                    ("delivery_blocked", "delivery_blocked"),
                    ("stale", "stale"), ("unledgered", "unledgered"))


def _migration_001_initial(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE orgs (
            id TEXT PRIMARY KEY,
            papra_org_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE mailboxes (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL REFERENCES orgs(id),
            host TEXT NOT NULL,
            port INTEGER NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            ssl_mode TEXT NOT NULL DEFAULT 'ssl',
            poll_interval_s INTEGER NOT NULL DEFAULT 300,
            subject_filter TEXT,
            delete_after_download INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            last_seen_uid INTEGER,
            uidvalidity TEXT,
            last_poll_at TEXT,
            last_error TEXT,
            consecutive_failures INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            papra_org_id TEXT NOT NULL,
            document_id TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            payload TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT NOT NULL,
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX idx_jobs_idempotency
            ON jobs(kind, papra_org_id, document_id, content_hash)
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_jobs_claim ON jobs(kind, status, next_attempt_at)
        """
    )
    conn.execute(
        """
        CREATE TABLE documents (
            papra_org_id TEXT NOT NULL,
            papra_document_id TEXT NOT NULL,
            name TEXT NOT NULL,
            mime TEXT NOT NULL,
            ingested_via TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            PRIMARY KEY (papra_org_id, papra_document_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE dedup_hashes (
            hash TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE log_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            line TEXT NOT NULL
        )
        """
    )


def _migration_002_ingest_events(conn: sqlite3.Connection) -> None:
    """Why a message or attachment was not ingested.

    Additive: nothing that exists is touched, and existing databases open
    unchanged and are bumped from user_version 1 to 2. Rows are dispositions —
    one per message-or-attachment per poll that decided about it — and they are
    retained for INGEST_EVENT_RETENTION_DAYS, so this is a window, not an
    archive. See docs/superpowers/plans/2026-11-16-ingest-visibility.md.
    """
    conn.execute(
        """
        CREATE TABLE ingest_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            mailbox_id TEXT NOT NULL,
            uid INTEGER NOT NULL,
            uidvalidity TEXT,
            papra_org_id TEXT,
            kind TEXT NOT NULL,
            filename TEXT,
            sender TEXT,
            detail TEXT
        )
        """
    )
    conn.execute("CREATE INDEX idx_ingest_events_ts ON ingest_events(ts)")
    conn.execute(
        "CREATE INDEX idx_ingest_events_mailbox ON ingest_events(mailbox_id, ts)"
    )


def _migration_003_index_ledger(conn: sqlite3.Connection) -> None:
    """One row per PAPRA document: a reference, not a pairing.

    Kreuzakt dedupes by file hash, so identical bytes in two buckets produce one
    Kreuzakt document and two rows sharing kreuzakt_id. No unique index on
    kreuzakt_id: resolution returns a set and deletes are reference-counted.

    kreuzakt ids are only meaningful within an index generation. A wiped or
    restored Kreuzakt restarts its ids at 1, so bumping the generation makes
    every stale row unresolvable and eligible for re-index from Papra's bytes —
    which is why nothing here matches by id or filename prefix.

    Every statement is applied with its own `conn.execute`, like 001 and 002, and
    that is load-bearing rather than stylistic. `migrate()` runs each migration
    inside a `BEGIN IMMEDIATE` and rolls back on failure; `executescript` commits
    that transaction before it starts and releases the write lock, so a later
    statement failing would leave the earlier `CREATE TABLE` permanently applied at
    the old `user_version` — an unretryable upgrade, since 003 has no
    `IF NOT EXISTS` — and the racing-migrator guard in `migrate()` would lose its
    lock mid-flight. Statements run one at a time, inside the transaction, so a
    failure anywhere leaves the file exactly as it was and the next start retries.
    """
    conn.execute(
        """
        CREATE TABLE index_ledger (
            papra_org_id        TEXT NOT NULL,
            papra_document_id   TEXT NOT NULL,
            kreuzakt_id         INTEGER,
            file_hash           TEXT NOT NULL,
            papra_original_name TEXT,
            delivered_at        TEXT,
            delivered_sha       TEXT,
            delivered_generation INTEGER,
            written_name        TEXT,
            name_protected      INTEGER NOT NULL DEFAULT 0,
            written_date        TEXT,
            date_protected      INTEGER NOT NULL DEFAULT 0,
            written_notes       TEXT,
            notes_protected     INTEGER NOT NULL DEFAULT 0,
            repair_count        INTEGER NOT NULL DEFAULT 0,
            last_repair_at      TEXT,
            deleted_at          TEXT,
            cleanup_state       TEXT NOT NULL DEFAULT 'none',
            cleanup_kreuzakt_id INTEGER,
            cleanup_generation  INTEGER,
            cleanup_attempts    INTEGER NOT NULL DEFAULT 0,
            cleanup_next_at     TEXT,
            PRIMARY KEY (papra_org_id, papra_document_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX idx_index_ledger_kreuzakt ON index_ledger (kreuzakt_id)"
        " WHERE kreuzakt_id IS NOT NULL AND deleted_at IS NULL"
    )
    conn.execute("CREATE INDEX idx_index_ledger_hash ON index_ledger (file_hash)")
    conn.execute(
        "CREATE INDEX idx_index_ledger_cleanup ON index_ledger"
        " (cleanup_state, cleanup_next_at) WHERE cleanup_state = 'pending'"
    )


def _migration_005_delivery_refusals(conn) -> None:
    """005: remember the content writes Papra refused, so refusing can eventually stop.

    Nothing bounded them. A document whose PATCH Papra rejects for a reason that does not
    heal (413 over the upload limit, 403 on a column a human locked, an endpoint that will
    not take these bytes) was re-read, re-PATCHed and re-ERROR-logged by every convergence
    pass for the life of the deployment. The suppression ceiling could not see it, because
    that ceiling counts repairs that *landed* and a refusal lands nothing — so the one
    state the pass can never resolve by itself was also the one it never stopped paying
    for, and the only evidence was a log line repeating every six hours.

    Two columns, because "how many in a row" and "when were we last refused" answer
    different questions and only the first one can be acted on: the count is what retires
    a reference from the retry loop and what `check` will count, the timestamp is what tells
    an operator whether the refusals are this afternoon's or from before the last upgrade.

    `NOT NULL DEFAULT 0` on the counter is not decoration: a NULL default would make
    `delivery_failures >= 5` false for every row that ever existed, which is a silent
    no-op migration — the counter would read as healthy precisely because nothing could
    ever be counted. Applied statement by statement, inside `migrate()`'s transaction, for
    the reason 003 and 004 spell out: half an `ALTER TABLE` pair at the old
    `user_version` cannot be repaired by the retry, it can only be opened once.
    """
    conn.execute(
        "ALTER TABLE index_ledger ADD COLUMN delivery_failures INTEGER NOT NULL DEFAULT 0"
    )
    conn.execute("ALTER TABLE index_ledger ADD COLUMN last_delivery_error_at TEXT")


# The statuses that mean "this piece of work still exists", as one SQL fragment.
# Migration 004's partial index, `enqueue_job`'s read-back and `reset_job`'s refusal
# all have to agree on it, and three hand-written copies of a predicate is how a
# queue ends up with a row that is live to one query and invisible to another.
LIVE_JOB_STATUS_SQL = "status IN ('pending','running')"


def _migration_004_jobs_live_idempotency(conn) -> None:
    """004: the jobs idempotency key constrains live rows only.

    A job row is a *scheduling record*, and 001 constrained the key over the whole
    table — so a row that reached `done` or `failed` kept absorbing every later
    `INSERT OR IGNORE` for the same (kind, org, document, content hash) forever. The
    bridge's own recovery paths died there: a document whose Kreuzakt copy was
    deleted could not be re-indexed, because the completed `index` row was still
    sitting on its bytes, and the sweep only ever saw it as `done`. Making the
    constraint partial turns a terminal row into history rather than a lock, which
    is what lets a rebuild re-derive a document from Papra's bytes at all.

    Narrowing the constraint can only *admit* rows the old one admitted fewer of, so
    no existing row can violate the new index and no de-duplication pass is needed:
    the old constraint was strictly stronger, so at most one row per key exists and
    it either is live (and stays the only live one) or is not (and is now free).

    Two statements, each with its own `conn.execute`, for the reason 003 learned:
    `executescript` commits `migrate()`'s pending `BEGIN IMMEDIATE` before it runs.
    Here the shape of the damage is worse than a lost table — a `DROP INDEX` that
    committed on its own leaves the queue with *no* unique index while
    `user_version` still reads 3, so the retry does not fail loudly, it silently
    stops de-duplicating and the first symptom is duplicate upstream work.
    """
    conn.execute("DROP INDEX idx_jobs_idempotency")
    conn.execute(
        f"""
        CREATE UNIQUE INDEX idx_jobs_idempotency
            ON jobs(kind, papra_org_id, document_id, content_hash)
        WHERE {LIVE_JOB_STATUS_SQL}
        """
    )


MIGRATIONS = [
    _migration_001_initial,
    _migration_002_ingest_events,
    _migration_003_index_ledger,
    _migration_004_jobs_live_idempotency,
    _migration_005_delivery_refusals,
]

# The closed set of outcomes worth recording. A sixth value must be added here
# and written at a call site, never invented by a call site: `record_ingest_event`
# refuses anything else, so a typo at a call site fails in the developer's test
# run instead of quietly becoming an outcome nobody renders.
INGEST_EVENT_KINDS = (
    "attachment_unsupported",   # a document format this pipeline does not take
    "sender_denied",            # the allow-list refused the sender
    "sender_malformed",         # From: did not resolve to exactly one sender
    "message_oversized",        # over MAX_MESSAGE_BYTES, body never fetched
    "upload_failed",            # Papra refused the upload (non-409)
)

# Field budgets for attacker-supplied text. These strings are evidence about a
# message, not documents to store in full: a 5 MB MIME filename must not become
# a 5 MB row, and the row has to stay readable on one CLI line.
EVENT_TEXT_LIMITS = {"filename": 200, "sender": 254, "detail": 300, "uidvalidity": 64}


def sanitize_event_text(value: object, *, limit: int) -> str | None:
    """Bound attacker-supplied text for an ingest_events column.

    Every value here comes from a message someone else sent: a filename from a
    MIME part, a sender from a header, a classification derived from an upstream
    error. Three things happen and nothing else:

    - control characters (``ord(ch) < 32``, plus DEL) become a **space**, never
      removed. Deleting them would splice two words into one and destroy the
      evidence; a space keeps the row honest and keeps one event on one line,
      which is what protects ``papra-suite events`` from a filename that
      contains a newline.
    - runs of whitespace collapse and the ends strip, for the same reason.
    - the result is truncated to ``limit`` **inclusive**, with a trailing "…"
      when characters were lost. A truncation that overflows its own budget is
      the bug this rule exists to prevent.

    What is deliberately NOT done: no HTML escaping and no quote stripping. This
    is SQLite text bound by parameter, and mangling the bytes would corrupt the
    record while buying no safety. Whatever renders these rows in a web page
    escapes at render time — that is that renderer's job, and this is the reason
    the raw value is kept.

    Returns ``None`` for empty or whitespace-only input, so an absent field
    stays absent instead of becoming an empty string.
    """
    if value is None:
        return None
    text = "".join(" " if (ord(ch) < 32 or ord(ch) == 127) else ch for ch in str(value))
    text = " ".join(text.split())
    if not text:
        return None
    if len(text) > limit:
        return text[: limit - 1] + "…"
    return text


@dataclass
class Org:
    id: str
    papra_org_id: str
    name: str


@dataclass
class Mailbox:
    id: str
    org_id: str
    host: str
    port: int
    username: str
    password: str
    ssl_mode: str
    poll_interval_s: int
    subject_filter: str | None
    delete_after_download: bool
    enabled: bool
    last_seen_uid: int | None
    uidvalidity: str | None
    last_poll_at: str | None
    last_error: str | None
    consecutive_failures: int


@dataclass
class Job:
    id: str
    kind: str
    papra_org_id: str
    document_id: str
    content_hash: str
    payload: dict
    status: str
    attempts: int
    next_attempt_at: str
    error: str | None
    created_at: str
    updated_at: str


@dataclass
class LedgerRef:
    """One Papra document's row in `index_ledger`.

    A *reference*, not a pairing: two Papra buckets holding identical bytes share
    one `kreuzakt_id`, and `delivered_generation` says which index that integer
    came from. Every field here is either something Kreuzakt can be asked again
    (`kreuzakt_id`, `file_hash`) or something only this table remembers — the
    Papra-side baseline `papra_original_name`, what we last wrote (`written_*`),
    and whether a human has taken the field away from us (`*_protected`).

    The three `*_protected` flags are `bool`, not the stored `int`: the delivery
    code asks `if ref.name_protected:`, and handing it a raw 1 would turn
    "protected" into a value that behaves as true everywhere and as `True`
    nowhere.
    """

    papra_org_id: str
    papra_document_id: str
    kreuzakt_id: int | None
    file_hash: str
    papra_original_name: str | None
    delivered_at: str | None
    delivered_sha: str | None
    delivered_generation: int | None
    written_name: str | None
    name_protected: bool
    written_date: str | None
    date_protected: bool
    written_notes: str | None
    notes_protected: bool
    repair_count: int
    last_repair_at: str | None
    deleted_at: str | None
    cleanup_state: str
    cleanup_attempts: int
    cleanup_next_at: str | None
    # Carried last, with defaults: the delete-propagation pass revalidates a
    # queued upstream delete from these two columns after a restart, and it reads
    # them off the refs `ledger_cleanup_due` hands back. Keeping them behind the
    # contract's field list means a caller that builds a LedgerRef from a dict of
    # defaults does not have to know they exist.
    cleanup_kreuzakt_id: int | None = None
    cleanup_generation: int | None = None
    # Also defaulted, and also load-bearing for a pass rather than for a writer:
    # `verify_once` reads `delivery_failures` to decide whether to attempt a repair at
    # all, so a reference whose refusals are unknown has to read as "none", not as None.
    delivery_failures: int = 0
    last_delivery_error_at: str | None = None


@dataclass
class SeenDocument:
    """One row of `documents`: what the poller or the sweep saw, with no reference behind it.

    A *sighting*, not a reference — it has no `kreuzakt_id`, no `file_hash` and no ownership
    columns, because nothing here has ever promised anything about this document to the
    index. Which is exactly why enrolling one needs Papra: the hash that keys the work is
    Papra's fact about its own bytes, and this table never stored one.
    """

    papra_org_id: str
    papra_document_id: str
    name: str
    mime: str
    ingested_via: str


@dataclass
class IngestEvent:
    """One recorded decision NOT to ingest something.

    `uidvalidity` travels with `uid` because an IMAP uid is only meaningful
    inside one UIDVALIDITY epoch: after a mailbox is rebuilt the uids restart
    and can name a different message, so a row quoting a bare uid would point
    at the wrong mail — and the recovery documentation tells operators to act on
    that uid. The poller holds the value from `select_inbox` on every poll, so
    recording it is free and recovering it later is not possible.
    """

    id: int
    ts: str
    mailbox_id: str
    uid: int
    uidvalidity: str | None
    papra_org_id: str | None
    kind: str
    filename: str | None
    sender: str | None
    detail: str | None


def _row_to_ingest_event(row: sqlite3.Row) -> IngestEvent:
    return IngestEvent(
        id=row["id"], ts=row["ts"], mailbox_id=row["mailbox_id"], uid=row["uid"],
        uidvalidity=row["uidvalidity"], papra_org_id=row["papra_org_id"],
        kind=row["kind"], filename=row["filename"], sender=row["sender"],
        detail=row["detail"],
    )


def _row_to_mailbox(row: sqlite3.Row) -> Mailbox:
    return Mailbox(
        id=row["id"], org_id=row["org_id"], host=row["host"], port=row["port"],
        username=row["username"], password=row["password"], ssl_mode=row["ssl_mode"],
        poll_interval_s=row["poll_interval_s"], subject_filter=row["subject_filter"],
        delete_after_download=bool(row["delete_after_download"]),
        enabled=bool(row["enabled"]), last_seen_uid=row["last_seen_uid"],
        uidvalidity=row["uidvalidity"], last_poll_at=row["last_poll_at"],
        last_error=row["last_error"], consecutive_failures=row["consecutive_failures"],
    )


def _row_to_ledger_ref(row: sqlite3.Row) -> LedgerRef:
    return LedgerRef(
        papra_org_id=row["papra_org_id"], papra_document_id=row["papra_document_id"],
        kreuzakt_id=row["kreuzakt_id"], file_hash=row["file_hash"],
        papra_original_name=row["papra_original_name"], delivered_at=row["delivered_at"],
        delivered_sha=row["delivered_sha"], delivered_generation=row["delivered_generation"],
        written_name=row["written_name"], name_protected=bool(row["name_protected"]),
        written_date=row["written_date"], date_protected=bool(row["date_protected"]),
        written_notes=row["written_notes"], notes_protected=bool(row["notes_protected"]),
        repair_count=row["repair_count"], last_repair_at=row["last_repair_at"],
        deleted_at=row["deleted_at"], cleanup_state=row["cleanup_state"],
        cleanup_attempts=row["cleanup_attempts"], cleanup_next_at=row["cleanup_next_at"],
        cleanup_kreuzakt_id=row["cleanup_kreuzakt_id"],
        cleanup_generation=row["cleanup_generation"],
        delivery_failures=row["delivery_failures"],
        last_delivery_error_at=row["last_delivery_error_at"],
    )


def _row_to_job(row: sqlite3.Row) -> Job:
    return Job(
        id=row["id"],
        kind=row["kind"],
        papra_org_id=row["papra_org_id"],
        document_id=row["document_id"],
        content_hash=row["content_hash"],
        payload=json.loads(row["payload"]),
        status=row["status"],
        attempts=row["attempts"],
        next_attempt_at=row["next_attempt_at"],
        error=row["error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class Store:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        # Timestamp of the last recover_running_jobs() pass on this instance;
        # None until the first recovery, which reclaims every running job.
        self._last_recover_at: str | None = None
        # Journal-mode changes do NOT honor busy_timeout: on a fresh (non-WAL)
        # database, if another process holds a write lock, the WAL pragma
        # fails immediately with "database is locked". Retry within a bounded
        # window so racing constructors converge on WAL instead of dying
        # with a raw traceback before migrate()'s race guard can run.
        deadline = time.monotonic() + 30.0
        while True:
            try:
                self._conn.execute("PRAGMA journal_mode=WAL")
                break
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "locked" not in message and "busy" not in message:
                    raise
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        # Ordinary (non-journal-mode) contention honors busy_timeout.
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def migrate(self) -> None:
        with self._lock:
            current = self._conn.execute("PRAGMA user_version").fetchone()[0]
            for index in range(current, len(MIGRATIONS)):
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    # Re-read inside the transaction: another process may have
                    # migrated (and bumped user_version) between our pre-lock
                    # read and the BEGIN IMMEDIATE lock. Skip anything already
                    # applied so concurrent migrators cannot double-apply.
                    applied = self._conn.execute("PRAGMA user_version").fetchone()[0]
                    if applied > index:
                        self._conn.rollback()
                        continue
                    MIGRATIONS[index](self._conn)
                    self._conn.execute(f"PRAGMA user_version = {index + 1}")
                    self._conn.commit()
                except BaseException:
                    self._conn.rollback()
                    raise

    # -- settings ---------------------------------------------------------

    def get_setting(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def set_setting(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def settings_flag(self, name: str) -> bool:
        """A settings row read as a boolean, missing key meaning off.

        The delivery flags decide whether this program is allowed to write into
        Papra at all, so the read fails closed: only `TRUTHY_SETTING_VALUES` mean
        on, and everything else (absent, empty, unrecognised) is off. A helper
        rather than `get_setting(...) == "1"` at each call site because the CLI, the
        daemon and the worker loops all ask the same question and must not be able
        to disagree about what the operator typed.
        """
        value = self.get_setting(name)
        return value is not None and value.strip().lower() in TRUTHY_SETTING_VALUES

    def clear_setting(self, key: str, *, expected: str) -> bool:
        """Delete a settings row only if it still holds the value that was read.

        `set_setting` has no fence because its callers mean "make it so". This one is the
        opposite act: an operator clearing a piece of pass state they were *shown*, on a row
        that some background pass may have rewritten in the meantime. Read-then-delete would
        clear whatever arrived since — which for `index_marks_rolling`, the counter that
        brakes mass deletion, means re-arming the brake on the strength of a stale glance.
        So the value read is the condition, and False means "it moved, nothing was cleared".

        The row is deleted rather than blanked: an empty settings row is a value like any
        other, and a reader that tests truthiness reads it the same way as an absent one
        while anyone reading the table sees a row that means nothing.
        """
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "DELETE FROM settings WHERE key = ? AND value = ?", (key, expected))
        return cursor.rowcount > 0

    def _read_generation_or_none(self) -> int | None:
        """The stored index generation, or None when the value cannot be trusted.

        **Caller must already hold `self._lock`** — `threading.Lock` is not
        reentrant, so `get_generation()` cannot be called from inside a locked
        block.

        An absent key is `DEFAULT_GENERATION`. A value that is not a positive
        integer is None, and the caller decides what refusing means; what it must
        not mean is "1". Generation 1 is the namespace the very first index bound
        into, so reading a corrupt value as the default would silently re-authorise
        every row a rebuild was supposed to retire — a fail-open in the one place
        whose whole job is to fail closed.
        """
        row = self._conn.execute(
            "SELECT value FROM settings WHERE key = ?", (INDEX_GENERATION_KEY,)
        ).fetchone()
        if row is None:
            return DEFAULT_GENERATION
        try:
            generation = int(row["value"])
        except (TypeError, ValueError):
            return None
        return generation if generation >= DEFAULT_GENERATION else None

    def _read_generation(self) -> int:
        """`_read_generation_or_none`, refusing loudly rather than returning None.

        An uncaught ValueError out of a worker pass is the desired shape here: the
        alternative is a service that confidently answers from an index whose epoch
        nobody can identify.
        """
        generation = self._read_generation_or_none()
        if generation is None:
            raise ValueError(
                f"{INDEX_GENERATION_KEY} is not a positive integer, so no generation is current; "
                "refusing to resolve or bind index ids. The state is that one row of the settings "
                "table. `papra-suite ledger --reindex-all` bumps the generation above every "
                "epoch the ledger still carries, and it refuses to run while Kreuzakt is "
                "unreachable, so fixing the row by hand is still the route when the index is "
                "down: set it to a positive integer one above the highest generation the ledger "
                "still carries, when the point is to invalidate what is bound to epochs nobody "
                "can identify."
            )
        return generation

    def get_generation(self) -> int:
        """The generation newly bound kreuzakt ids belong to (default 1)."""
        with self._lock:
            return self._read_generation()

    def bump_generation(self) -> int:
        """Declare the current kreuzakt ids meaningless; return the new generation.

        Called after Kreuzakt is wiped or restored from another backup. Every row
        that still carries the old `delivered_generation` stops resolving and
        becomes eligible for re-index from Papra's bytes, which is the whole
        rebuild story — nothing has to be deleted here for it to work.
        """
        with self._lock:
            # BEGIN IMMEDIATE, not the usual `with self._conn`: two processes both
            # reading generation 1 and both writing 2 would leave stale rows looking
            # current after a rebuild, which is the one outcome a generation must
            # never allow. Taking the write lock first makes read-then-write atomic
            # across processes.
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                current = self._read_generation_or_none()
                if current is None:
                    # The settings row is unusable and this command is how the
                    # operator repairs it, so the next value comes from the ledger:
                    # one above the highest generation any row still carries is
                    # strictly new, which a blind `DEFAULT_GENERATION + 1` is not.
                    bounds = self._conn.execute(
                        "SELECT COALESCE(MAX(delivered_generation), 0) AS bound,"
                        " COALESCE(MAX(cleanup_generation), 0) AS queued FROM index_ledger"
                    ).fetchone()
                    current = max(bounds["bound"], bounds["queued"], DEFAULT_GENERATION)
                next_generation = current + 1
                self._conn.execute(
                    "INSERT INTO settings (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (INDEX_GENERATION_KEY, str(next_generation)),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return next_generation

    # -- ingest events -------------------------------------------------------

    def table_columns(self, table: str) -> list[str]:
        """The columns a table actually has, or [] when the table is absent.

        Lets a reader print what the database knows instead of assuming its own
        schema: a newer CLI reading an older store should drop the column, not
        print a lie or raise. `PRAGMA table_info` takes its argument as text and
        cannot be parameter-bound, so the name is interpolated — hence the
        identifier guard, which keeps this usable for names this program knows
        and useless as an injection point for a name it does not.
        """
        if not table.isidentifier():
            raise ValueError(f"not a table name: {table!r}")
        with self._lock:
            rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [row["name"] for row in rows]

    def record_ingest_event(
        self, *, mailbox_id: str, uid: int, kind: str, uidvalidity: str | None = None,
        papra_org_id: str | None = None, filename: str | None = None,
        sender: str | None = None, detail: str | None = None,
    ) -> None:
        """Record one decision NOT to ingest something.

        `kind` is validated against `INGEST_EVENT_KINDS` here rather than at the
        call sites, so an unknown outcome is a programming error raised now, not
        a row some future renderer has to guess about. Every attacker-supplied
        string goes through `sanitize_event_text` on the way in: the columns hold
        a MIME filename, a header value and an upstream classification, and a
        writer must not be able to forget that.
        """
        if kind not in INGEST_EVENT_KINDS:
            raise ValueError(f"unknown ingest event kind: {kind!r}")
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO ingest_events (ts, mailbox_id, uid, uidvalidity,"
                " papra_org_id, kind, filename, sender, detail)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    _now(), mailbox_id, uid,
                    sanitize_event_text(uidvalidity, limit=EVENT_TEXT_LIMITS["uidvalidity"]),
                    sanitize_event_text(papra_org_id, limit=64),
                    kind,
                    sanitize_event_text(filename, limit=EVENT_TEXT_LIMITS["filename"]),
                    sanitize_event_text(sender, limit=EVENT_TEXT_LIMITS["sender"]),
                    sanitize_event_text(detail, limit=EVENT_TEXT_LIMITS["detail"]),
                ),
            )

    def list_ingest_events(
        self, *, mailbox_id: str | None = None, kinds: list[str] | None = None,
        since: str | None = None, limit: int = 200,
    ) -> list[IngestEvent]:
        """Newest first. `ts DESC, id DESC` because two events can share a
        millisecond and an unstable order would make 'the last 200' mean
        something different on every read. `kinds=[]` is an empty answer, not
        'no filter' — an empty filter set selecting everything would turn a
        caller's empty list into the whole table."""
        bounded = max(1, min(int(limit), 1000))
        sql = "SELECT * FROM ingest_events"
        clauses: list[str] = []
        params: list[object] = []
        if mailbox_id is not None:
            clauses.append("mailbox_id = ?")
            params.append(mailbox_id)
        if kinds is not None:
            if not kinds:
                return []
            clauses.append(f"kind IN ({', '.join('?' * len(kinds))})")
            params.extend(kinds)
        if since is not None:
            clauses.append("ts >= ?")
            params.append(since)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY ts DESC, id DESC LIMIT ?"
        params.append(bounded)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_ingest_event(row) for row in rows]

    def count_ingest_events_by_kind(self, *, since: str | None = None) -> dict[str, int]:
        """How many events of each kind, optionally only at or after `since`.

        Counted in SQL rather than by listing rows: this backs `check`'s summary
        line, and a listing cap would silently turn a bad day into a normal-looking
        one. Kinds with no rows are absent rather than zero — the caller prints what
        happened, not the closed set.
        """
        sql = "SELECT kind, COUNT(*) AS n FROM ingest_events"
        params: list[object] = []
        if since is not None:
            sql += " WHERE ts >= ?"
            params.append(since)
        sql += " GROUP BY kind"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return {row["kind"]: row["n"] for row in rows}

    def prune_ingest_events(self, retention_days: int = INGEST_EVENT_RETENTION_DAYS) -> int:
        """Drop events older than the retention window; return how many went.

        The comparison is lexical on purpose: `_now()` and `_iso()` both produce
        a zero-padded UTC ISO timestamp ending "+00:00", and for that fixed shape
        lexical order equals chronological order (the "+00:00" separator sorts
        before the "." of a fractional second, so a whole second still sorts
        first). If `_now()`'s format ever changes — a different offset, a space
        instead of a "T", a naive timestamp — this comparison silently stops
        meaning what it says, and this is the sentence that says so.
        """
        cutoff = _iso(datetime.now(UTC) - timedelta(days=retention_days))
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "DELETE FROM ingest_events WHERE ts < ?", (cutoff,)
            )
        return cursor.rowcount

    # -- jobs ---------------------------------------------------------------

    def enqueue_job(self, *, kind: str, papra_org_id: str, document_id: str,
                    content_hash: str, payload: dict) -> str:
        """Queue this work, or name the live job that already carries it.

        The idempotency key is `(kind, papra_org_id, document_id, content_hash)`, and
        since migration 004 it constrains live rows only: a `pending` or `running` row
        absorbs the insert, a `done` or `failed` one does not. So the same key can now
        hold several rows, and the read-back has to say which one this call is about.
        It prefers the live row — the insert above can only have been ignored because
        a live row exists — and falls back to the newest row by rowid, which keeps the
        return value a real row for the key instead of `None` if another writer settled
        it between the two statements.

        Handing back a terminal row would be a silent wrong answer:
        `indexing._readmit_index_job` reads the returned id back and acts on it, and
        `papra-suite jobs --reset JOB_ID` hands an id straight to `reset_job`.
        """
        job_id = uuid.uuid4().hex
        now = _now()
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO jobs "
                "(id, kind, papra_org_id, document_id, content_hash, payload, status,"
                " attempts, next_attempt_at, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)",
                (job_id, kind, papra_org_id, document_id, content_hash,
                 json.dumps(payload), now, now, now),
            )
            row = self._conn.execute(
                f"SELECT id FROM jobs WHERE kind = ? AND papra_org_id = ?"
                f" AND document_id = ? AND content_hash = ?"
                f" ORDER BY CASE WHEN {LIVE_JOB_STATUS_SQL} THEN 0 ELSE 1 END,"
                " rowid DESC LIMIT 1",
                (kind, papra_org_id, document_id, content_hash),
            ).fetchone()
        return row["id"]

    def claim_next_job(self, kind: str) -> Job | None:
        now = _now()
        with self._lock, self._conn:
            while True:
                row = self._conn.execute(
                    "SELECT * FROM jobs WHERE kind = ? AND status = 'pending' "
                    "AND next_attempt_at <= ? ORDER BY created_at LIMIT 1",
                    (kind, now),
                ).fetchone()
                if row is None:
                    return None
                cursor = self._conn.execute(
                    "UPDATE jobs SET status = 'running', updated_at = ? "
                    "WHERE id = ? AND status = 'pending'",
                    (now, row["id"]),
                )
                if cursor.rowcount == 1:
                    row = self._conn.execute(
                        "SELECT * FROM jobs WHERE id = ?", (row["id"],)
                    ).fetchone()
                    return _row_to_job(row)
                # lost the race against another instance: try the next candidate

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return _row_to_job(row) if row else None

    def mark_job_done(self, job_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE jobs SET status = 'done', error = NULL, updated_at = ? WHERE id = ?",
                (_now(), job_id),
            )

    def mark_job_failed(self, job_id: str, error: str, *, max_attempts: int = 3,
                        backoff_base_s: int = 30) -> None:
        now = _now()
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT attempts FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown job: {job_id}")
            attempts = row["attempts"] + 1
            if attempts >= max_attempts:
                self._conn.execute(
                    "UPDATE jobs SET status = 'failed', attempts = ?, error = ?, updated_at = ? "
                    "WHERE id = ?",
                    (attempts, error, now, job_id),
                )
            else:
                delay_s = backoff_base_s * 4 ** (attempts - 1)
                next_at = _iso(datetime.now(UTC) + timedelta(seconds=delay_s))
                self._conn.execute(
                    "UPDATE jobs SET status = 'pending', attempts = ?, error = ?, "
                    "next_attempt_at = ?, updated_at = ? WHERE id = ?",
                    (attempts, error, next_at, now, job_id),
                )

    def reset_job(self, job_id: str) -> None:
        """Put a settled job back at the front of the queue.

        Since migration 004 a terminal row no longer *blocks* anything: asking for the
        same work again takes a fresh row, which is the recovery path the sweep and the
        rebuild use. This method is the other path — an operator naming one row
        (`papra-suite jobs --reset JOB_ID`), which keeps that row's identity and history in one
        place instead of growing a second one.

        It refuses when the two paths would collide. Reviving a terminal row whose key
        already has a live row would violate the partial unique index, so SQLite would
        raise `IntegrityError` out of a CLI command; the honest answer is that the work
        is already queued, in words, with the rows left untouched.
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT kind, papra_org_id, document_id, content_hash, status FROM jobs"
                " WHERE id = ?", (job_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown job: {job_id}")
            live = self._conn.execute(
                f"SELECT id FROM jobs WHERE kind = ? AND papra_org_id = ?"
                f" AND document_id = ? AND content_hash = ? AND id != ?"
                f" AND {LIVE_JOB_STATUS_SQL} LIMIT 1",
                (row["kind"], row["papra_org_id"], row["document_id"],
                 row["content_hash"], job_id),
            ).fetchone()
            if live is not None:
                raise ValueError(
                    f"job {job_id}: the same work is already queued as {live['id']}"
                )
            if row["status"] == "running":
                raise ValueError(f"job {job_id}: it is running, a worker is holding it")
            # Guarded again in the write itself: a worker can claim the row between the read above
            # and this statement, and a reset that lands then puts a second claimer on a document
            # somebody is uploading. The condition is evaluated where SQLite holds the write lock,
            # so the rowcount is the only answer that can be trusted.
            moved = self._conn.execute(
                "UPDATE jobs SET status = 'pending', attempts = 0, error = NULL, "
                "next_attempt_at = ?, updated_at = ? WHERE id = ? AND status != 'running'",
                (_now(), _now(), job_id),
            )
            if moved.rowcount != 1:
                raise ValueError(f"job {job_id}: it is running, a worker is holding it")

    def list_jobs(self, *, kind: str | None = None, status: str | None = None,
                  document_id: str | None = None, limit: int = 100) -> list[Job]:
        """The queue, newest first. Every filter is applied in SQL, never afterwards.

        That is about the limit, not tidiness: filtering rows after `LIMIT n` answers
        `jobs --kind ocr` with nothing while fifty `index` rows sit ahead of them, which is the
        reading that makes an operator believe a queue has drained. `document_id` exists because
        "what is happening to this document?" is the question the `jobs` CLI was written to
        answer, and a filter that silently does nothing answers it with the whole queue — Task 6
        met exactly that behind a passing `xfailed`.

        Since migration 004 one `(kind, papra_org_id, document_id, content_hash)` can hold
        several rows — the live one plus the superseded history — so a caller must not read a row
        count as a document count. `rowid DESC` breaks ties between rows created in the same
        instant, without which one document's history could reorder between two reads of an
        unchanged table.
        """
        query = "SELECT * FROM jobs"
        clauses: list[str] = []
        params: list[object] = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if document_id:
            clauses.append("document_id = ?")
            params.append(document_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC, rowid DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [_row_to_job(row) for row in rows]

    def count_jobs(self, kind: str | None = None, status: str | None = None, *,
                   document_id: str | None = None) -> int:
        """How many job ROWS match, counted by SQLite.

        `jobs --drain` is the middle step of the `content` handover, where the price of a wrong
        reading is two writers on one column, so the number has to be exact rather than "as many
        rows as a listing was willing to carry": a count derived from a capped listing can report
        a quiet queue with rows still behind the cap. Same filters as `list_jobs`, same meaning.

        It counts rows and not documents, deliberately: since migration 004 a document can own a
        settled row and a live one at the same time, and the thing the drain has to be sure about
        is that no row is waiting to be claimed or being worked.
        """
        clauses: list[str] = []
        params: list[object] = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if document_id:
            clauses.append("document_id = ?")
            params.append(document_id)
        query = "SELECT COUNT(*) FROM jobs"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        with self._lock:
            row = self._conn.execute(query, params).fetchone()
        return int(row[0])

    def count_jobs_by_status(self, kind: str | None = None, *,
                             document_id: str | None = None) -> dict[str, int]:
        """Every status count for a scope, from one statement.

        `jobs --drain` is the middle step of the `content` handover and its wrong answer is two
        writers on one column. Asked for `pending` and `running` as two queries, SQLite may answer
        from two instants: a worker restart runs `recover_running_jobs`, which moves a `running`
        row back to `pending`, so a first count of `pending` and a second of `running` can both
        come home 0 above a queue that holds work. One GROUP BY cannot be split that way, and a
        status missing from the result means zero rather than an unknown.
        """
        query = "SELECT status, COUNT(*) FROM jobs"
        params: list[object] = []
        clauses: list[str] = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if document_id:
            clauses.append("document_id = ?")
            params.append(document_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " GROUP BY status"
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return {str(row[0]): int(row[1]) for row in rows}

    # -- orgs ---------------------------------------------------------------

    def create_org(self, *, papra_org_id: str, name: str) -> str:
        org_id = uuid.uuid4().hex
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO orgs (id, papra_org_id, name) VALUES (?, ?, ?)",
                (org_id, papra_org_id, name),
            )
            row = self._conn.execute(
                "SELECT id FROM orgs WHERE papra_org_id = ?", (papra_org_id,)
            ).fetchone()
        return row["id"]

    def get_org(self, org_id: str) -> Org | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM orgs WHERE id = ?", (org_id,)).fetchone()
        return Org(row["id"], row["papra_org_id"], row["name"]) if row else None

    def list_orgs(self) -> list[Org]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM orgs ORDER BY papra_org_id").fetchall()
        return [Org(row["id"], row["papra_org_id"], row["name"]) for row in rows]

    def get_org_by_papra_id(self, papra_org_id: str) -> Org | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM orgs WHERE papra_org_id = ?", (papra_org_id,)
            ).fetchone()
        return Org(row["id"], row["papra_org_id"], row["name"]) if row else None

    # -- mailboxes ------------------------------------------------------------

    def create_mailbox(self, *, org_id: str, host: str, port: int, username: str,
                       password: str, ssl_mode: str = "ssl", poll_interval_s: int = 300,
                       subject_filter: str | None = None,
                       delete_after_download: bool = False) -> str:
        # Fail-closed validation: an unrecognized ssl_mode must never be
        # persisted — MailboxSession.connect would otherwise fail-open to
        # plaintext IMAP and leak the credentials in cleartext.
        if ssl_mode not in VALID_SSL_MODES:
            raise ValueError(
                f"ssl_mode must be one of {', '.join(VALID_SSL_MODES)}; got {ssl_mode!r}"
            )
        mailbox_id = uuid.uuid4().hex
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO mailboxes (id, org_id, host, port, username, password, "
                "ssl_mode, poll_interval_s, subject_filter, delete_after_download) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (mailbox_id, org_id, host, port, username, password, ssl_mode,
                 poll_interval_s, subject_filter, int(delete_after_download)),
            )
        return mailbox_id

    def get_mailbox(self, mailbox_id: str) -> Mailbox | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM mailboxes WHERE id = ?", (mailbox_id,)
            ).fetchone()
        return _row_to_mailbox(row) if row else None

    def list_mailboxes(self, *, enabled_only: bool = False) -> list[Mailbox]:
        query = "SELECT * FROM mailboxes"
        if enabled_only:
            query += " WHERE enabled = 1"
        query += " ORDER BY id"
        with self._lock:
            rows = self._conn.execute(query).fetchall()
        return [_row_to_mailbox(row) for row in rows]

    def delete_mailbox(self, mailbox_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM mailboxes WHERE id = ?", (mailbox_id,))

    def record_poll_success(self, mailbox_id: str, *, last_seen_uid: int,
                            uidvalidity: str | None) -> None:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT 1 FROM mailboxes WHERE id = ?", (mailbox_id,)
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown mailbox: {mailbox_id}")
            self._conn.execute(
                "UPDATE mailboxes SET last_seen_uid = ?, uidvalidity = ?, last_poll_at = ?, "
                "last_error = NULL, consecutive_failures = 0 WHERE id = ?",
                (last_seen_uid, uidvalidity, _now(), mailbox_id),
            )

    def reset_mailbox_cursor(self, mailbox_id: str, *,
                             from_uid: int | None = None,
                             reenable: bool = True) -> int | None:
        """Rewind the poll cursor so mail is read again; returns the old cursor.

        This is the recovery path for a corrected sender allow-list: refused
        messages were retired (marked seen) but never deleted and never
        uploaded, so they are still on the server. Rewinding re-reads them and
        the content dedup keeps already-ingested documents from being uploaded
        twice. Failure state is cleared too, because the point is to try again.
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT last_seen_uid, enabled FROM mailboxes WHERE id = ?",
                (mailbox_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown mailbox: {mailbox_id}")
            previous, was_enabled = row[0], bool(row[1])
            if from_uid is not None and from_uid < 0:
                raise ValueError(f"from_uid must not be negative, got {from_uid}")
            if previous is not None and from_uid is not None and from_uid > previous:
                # This is a rewind. Moving the cursor forward would retire mail
                # the operator never asked to skip — the mistake here is exactly
                # the silent loss the fetch code spent this milestone removing.
                raise ValueError(
                    f"refusing to move the cursor forward ({previous} -> {from_uid}); "
                    "replay only rewinds")
            self._conn.execute(
                "UPDATE mailboxes SET last_seen_uid = ?, last_error = NULL, "
                "consecutive_failures = 0, enabled = ? WHERE id = ?",
                (from_uid, int(reenable), mailbox_id),
            )
            # A mailbox disabled by three failed polls would otherwise stay
            # disabled after a successful replay: the counters were cleared but
            # the poll loop exits when the row says enabled = 0, so the operator
            # would rewind and still see nothing happen.
            self._replayed = (previous, was_enabled)
            return previous

    def record_poll_failure(self, mailbox_id: str, error: str, *,
                            disable_after: int = 3) -> None:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT consecutive_failures FROM mailboxes WHERE id = ?", (mailbox_id,)
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown mailbox: {mailbox_id}")
            failures = row["consecutive_failures"] + 1
            self._conn.execute(
                "UPDATE mailboxes SET consecutive_failures = ?, last_error = ?, "
                "last_poll_at = ?, enabled = ? WHERE id = ?",
                (failures, error[:500], _now(), 0 if failures >= disable_after else 1,
                 mailbox_id),
            )

    # -- job safety ------------------------------------------------------------

    def recover_running_jobs(self) -> int:
        """Reset stale running jobs to pending (restart survival).

        The first call on a Store instance recovers every running job (these
        can only be orphans from a previous process). Later calls only recover
        jobs whose running state predates the previous recovery pass, so a
        job legitimately claimed after it is never clobbered.
        """
        now = _now()
        with self._lock, self._conn:
            query = (
                "UPDATE jobs SET status = 'pending', next_attempt_at = ?, updated_at = ? "
                "WHERE status = 'running'"
            )
            params: tuple = (now, now)
            if self._last_recover_at is not None:
                query += " AND updated_at < ?"
                params = params + (self._last_recover_at,)
            cursor = self._conn.execute(query, params)
            self._last_recover_at = now
            return cursor.rowcount

    # -- documents seen-table ------------------------------------------------

    def record_document_seen(self, *, papra_org_id: str, papra_document_id: str,
                             name: str, mime: str, ingested_via: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO documents "
                "(papra_org_id, papra_document_id, name, mime, ingested_via, first_seen_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (papra_org_id, papra_document_id, name, mime, ingested_via, _now()),
            )

    def document_seen(self, papra_org_id: str, papra_document_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM documents WHERE papra_org_id = ? AND papra_document_id = ?",
                (papra_org_id, papra_document_id),
            ).fetchone()
        return row is not None

    # -- mail dedup -----------------------------------------------------------

    def dedup_exists(self, dedup_hash: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM dedup_hashes WHERE hash = ?", (dedup_hash,)
            ).fetchone()
        return row is not None

    def add_dedup(self, dedup_hash: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO dedup_hashes (hash, created_at) VALUES (?, ?)",
                (dedup_hash, _now()),
            )

    # -- log ring buffer ----------------------------------------------------

    def append_log_line(self, ts: str, level: str, line: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO log_lines (ts, level, line) VALUES (?, ?, ?)", (ts, level, line)
            )
            self._conn.execute(
                "DELETE FROM log_lines WHERE id <= "
                "(SELECT COALESCE(MAX(id), 0) - ? FROM log_lines)",
                (LOG_RING_BUFFER_SIZE,),
            )

    def recent_log_lines(self, limit: int = 500) -> list[tuple[str, str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts, level, line FROM log_lines ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [(row["ts"], row["level"], row["line"]) for row in reversed(rows)]

    # -- index ledger ---------------------------------------------------------
    #
    # Everything the MCP layer is allowed to answer about a Kreuzakt document is
    # derived from these rows, so the read paths below are authorization code.
    # Two rules hold across all of them and are the reason they look fussy:
    #
    #   * a row resolves only while `deleted_at IS NULL` AND its
    #     `delivered_generation` equals the generation asked about. A Kreuzakt id
    #     is an integer in *some* index; after a rebuild the same integer names a
    #     different document, so "wrong generation" must be no answer at all.
    #   * a write never overwrites a value it did not come to write. Upserts keep
    #     protection, delivery state and the first-seen Papra name, because each
    #     of those is either a human's decision or the baseline a later decision
    #     is measured against.

    def ledger_upsert_reference(
        self, *, papra_org_id: str, papra_document_id: str, file_hash: str,
        papra_original_name: str | None = None,
    ) -> None:
        """Record that this Papra document exists, without disturbing anything else.

        `file_hash` follows the bytes, and taking those bytes away from a bound row
        takes the binding with it: Kreuzakt indexed the old content, so keeping
        `kreuzakt_id` would leave the row resolving to text extracted from a file
        that no longer exists *and* keep it out of `ledger_needs_index`, which is a
        wrong answer the sweep could never repair. Protection flags, the written
        values and the delivery columns are not mentioned in the UPDATE at all, so a
        re-ingest cannot clear them by construction — a human's decision is about
        the document, not about which bytes were indexed.

        `papra_original_name` keeps the first value it was given: that column is the
        ownership baseline the metadata delivery compares Papra's current name
        against, and moving it under the comparison would either protect a field
        forever or authorise a rename we did not make.
        """
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO index_ledger (papra_org_id, papra_document_id, file_hash,"
                " papra_original_name) VALUES (?, ?, ?, ?)"
                " ON CONFLICT(papra_org_id, papra_document_id) DO UPDATE SET"
                " file_hash = excluded.file_hash,"
                " papra_original_name = COALESCE(index_ledger.papra_original_name,"
                " excluded.papra_original_name),"
                " kreuzakt_id = CASE"
                " WHEN index_ledger.file_hash = excluded.file_hash"
                " THEN index_ledger.kreuzakt_id END,"
                " delivered_generation = CASE"
                " WHEN index_ledger.file_hash = excluded.file_hash"
                " THEN index_ledger.delivered_generation END",
                (papra_org_id, papra_document_id, file_hash, papra_original_name),
            )

    def ledger_set_kreuzakt_id(
        self, *, papra_org_id: str, papra_document_id: str, kreuzakt_id: int,
        generation: int, only_if_generation: int | None = None,
    ) -> bool:
        """Bind a kreuzakt id; report whether the row now holds it.

        With `only_if_generation` the write is fenced: it happens only while the
        generation in `settings` still equals that value, and returns False
        otherwise, leaving the row exactly as it was. That is the conditional
        accept the index job needs — a rebuild that lands between the upload and
        this write has to win, because binding an id from the destroyed index
        under the new generation would point a live reference at a meaningless
        integer. `False` must be a real answer: a `None`-returning version would
        make a refused bind look like a successful one.

        The read of the generation and the UPDATE happen in one `BEGIN IMMEDIATE`
        transaction, so a `bump_generation()` from another thread or process
        cannot slip between them.

        `generation` and `only_if_generation` have to name the same index when
        both are given. Passing a stale stamp with a fresh fence would return True
        while writing a `delivered_generation` that no backfill can ever match:
        the row stops resolving, stops looking like work to the sweep, and reports
        success — so the mismatch is refused as the caller bug it is.

        Binding also retires the refused-write history. `delivery_failures` records what
        Papra said about the text this binding pointed at; a new binding is a new extraction
        the document has not been refused for yet, and carrying the old verdict over would
        make the ceiling a permanent exclusion — the thing it exists to avoid being.
        """
        if only_if_generation is not None and generation != only_if_generation:
            raise ValueError(
                f"generation ({generation}) and only_if_generation "
                f"({only_if_generation}) disagree: the stamp and the fence describe"
                " one index, or the row would be bound to a generation that cannot "
                "resolve it"
            )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if (only_if_generation is not None
                        and self._read_generation() != only_if_generation):
                    self._conn.rollback()
                    return False
                cursor = self._conn.execute(
                    "UPDATE index_ledger SET kreuzakt_id = ?, delivered_generation = ?,"
                    " delivery_failures = 0, last_delivery_error_at = NULL"
                    " WHERE papra_org_id = ? AND papra_document_id = ?",
                    (kreuzakt_id, generation, papra_org_id, papra_document_id),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return cursor.rowcount > 0

    def ledger_get(self, papra_org_id: str, papra_document_id: str) -> LedgerRef | None:
        """The row for one Papra document, deleted or stale included.

        Deliberately unfiltered: the callers that need `deleted_at` and
        `delivered_generation` themselves (the delete pass, the CLI, the re-index
        decision) are the reason this is not `ledger_resolve` by another name.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM index_ledger WHERE papra_org_id = ? AND papra_document_id = ?",
                (papra_org_id, papra_document_id),
            ).fetchone()
        return _row_to_ledger_ref(row) if row else None

    def ledger_org_ids(self) -> list[str]:
        """Every bucket the ledger holds references for, whatever the `orgs` table says.

        The delete pass walks these rather than `list_orgs()`. A bucket can leave the
        `orgs` table — an operator removing it, a store copied without it — while its
        references and their Kreuzakt documents remain, and a reconciliation driven by
        `orgs` would never look at them again: index entries that outlive every record of
        why they exist. Naming them is also what makes an orphan bucket loud in the delete
        pass's own error line instead of invisible.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT papra_org_id FROM index_ledger ORDER BY papra_org_id"
            ).fetchall()
        return [row["papra_org_id"] for row in rows]

    def ledger_org_refs(
        self, *, org_id: str, after: tuple[str, str] | None = None, limit: int = 100,
    ) -> list[LedgerRef]:
        """Every reference of one bucket, deleted and stale ones included, one page at a time.

        The only walk here that filters on nothing but the bucket, and that is the point:
        every other one excludes a state on purpose, and the delete pass is the caller that
        has to see all of them.

        * `ledger_resolve` is the authorization boundary, so it hides soft-deleted and stale
          rows — which is precisely the population a restore has to be found in.
        * `ledger_needs_index` is work for the indexer, so it hides deleted rows entirely; a
          document the user threw away is not work.
        * `ledger_delivered_refs` needs a current binding, so it hides the unbound and the
          never-indexed, which is where a hard-deleted document's reference usually sits.

        Driven one bucket at a time because its caller is walking Papra's listing for that
        bucket and comparing. `after` is the keyset `(papra_org_id, papra_document_id)` the
        previous page ended with, for the same reason `ledger_delivered_refs` uses one: a row
        changing under an offset would skip or repeat a reference mid-pass, and the row it
        skipped is the one whose index entry survives or does not.

        `limit` is bounded like the other walks: this pass runs inside the sweep, and one
        bucket's whole table must not be the reason a poll cycle stalls.
        """
        bounded = max(1, min(int(limit), 1000))
        sql = "SELECT * FROM index_ledger WHERE papra_org_id = ?"
        params: list[object] = [org_id]
        if after is not None:
            sql += " AND (papra_org_id > ? OR (papra_org_id = ? AND papra_document_id > ?))"
            params.extend([after[0], after[0], after[1]])
        sql += " ORDER BY papra_org_id, papra_document_id LIMIT ?"
        params.append(bounded)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_ledger_ref(row) for row in rows]

    def ledger_resolve(self, kreuzakt_ids, *, generation: int) -> list[LedgerRef]:
        """Active references of `generation` that point at these Kreuzakt ids.

        This is the authorization boundary. Anything not returned does not exist
        as far as the MCP server is concerned: an unknown id, a soft-deleted row,
        a row bound under an older generation. An empty input is an empty answer,
        never "no filter" — the alternative would turn a caller with nothing
        resolved into a caller with the whole table.

        Rows come back per reference, not per Kreuzakt document, because two
        buckets can share one id and each has to be filtered against its own
        token scope afterwards.
        """
        ids = list(kreuzakt_ids)
        if not ids:
            return []
        placeholders = ", ".join("?" * len(ids))
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM index_ledger WHERE kreuzakt_id IN (" + placeholders
                + ") AND deleted_at IS NULL AND delivered_generation = ?"
                " ORDER BY papra_org_id, papra_document_id",
                (*ids, generation),
            ).fetchall()
        return [_row_to_ledger_ref(row) for row in rows]

    def ledger_needs_index(
        self, *, generation: int, org_id: str | None = None,
        after: tuple[str, str] | None = None, limit: int = 100,
    ) -> list[LedgerRef]:
        """Live references that have no usable id in `generation`.

        Either never indexed, or bound under a generation that no longer means
        what it said. This is the eligibility rule the sweep and the rebuild use,
        and it is derived from the ledger alone — never from `document_seen`,
        which records ingestion history and would make a rebuild look already
        done. Soft-deleted documents are excluded: the delete pass owns those.

        `after` is the keyset `(papra_org_id, papra_document_id)` the previous page ended
        with, and the rebuild is the caller that needs it. Queueing index work does not make
        a reference stop needing index work — the row stays in this result until a job binds
        an id under `generation` — so a walk that re-asked for the same first page would sit
        on it forever, and a walk that paged by offset would be counting rows it had already
        queued while later rows slid into the gap it had skipped. Only a keyset both
        terminates and covers.
        """
        bounded = max(1, min(int(limit), 1000))
        sql = (
            "SELECT * FROM index_ledger WHERE deleted_at IS NULL"
            " AND (kreuzakt_id IS NULL OR delivered_generation IS NULL"
            " OR delivered_generation != ?)"
        )
        params: list[object] = [generation]
        if org_id is not None:
            sql += " AND papra_org_id = ?"
            params.append(org_id)
        if after is not None:
            sql += " AND (papra_org_id > ? OR (papra_org_id = ? AND papra_document_id > ?))"
            params.extend([after[0], after[0], after[1]])
        sql += " ORDER BY papra_org_id, papra_document_id LIMIT ?"
        params.append(bounded)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_ledger_ref(row) for row in rows]

    def documents_without_reference(
        self, *, after: tuple[str, str] | None = None, limit: int = 100,
    ) -> list[SeenDocument]:
        """Documents this store marked seen that the ledger has never held.

        The anti-join between the two tables, which is the only way to see the population
        every other number here misses. Ingestion records a document in `documents`; the
        index bridge records a reference in `index_ledger`; until this walk existed nothing
        joined them, so a document seen by a build that had no ledger — everything emailed
        before the content handover, and every document the intake path declined to enrol —
        was invisible to every eligibility rule and every count, all of which derive from
        the ledger. `check` can only report what these two tables jointly know, and a Papra
        document that was never listed at all is not knowable from here at all; that is the
        limit of the number, not a bug in it.

        Non-PDF rows are included, because the caller has to be able to say *who* is left
        out rather than hand an operator a bare count: the index is PDF-only by design, and
        the two populations need different sentences.

        `after` is a keyset for the reason the other walks have one, with a sharper edge
        here: this walk's own work removes rows from the result. An offset would skip the
        documents that slid into the gap the enrolled rows left, and the walk would report
        having seen everything it was asked to look at.
        """
        bounded = max(1, min(int(limit), 1000))
        sql = (
            "SELECT d.papra_org_id, d.papra_document_id, d.name, d.mime, d.ingested_via"
            " FROM documents d WHERE NOT EXISTS ("
            "  SELECT 1 FROM index_ledger l"
            "  WHERE l.papra_org_id = d.papra_org_id"
            "   AND l.papra_document_id = d.papra_document_id)"
        )
        params: list[object] = []
        if after is not None:
            sql += " AND (d.papra_org_id > ? OR (d.papra_org_id = ? AND d.papra_document_id > ?))"
            params.extend([after[0], after[0], after[1]])
        sql += " ORDER BY d.papra_org_id, d.papra_document_id LIMIT ?"
        params.append(bounded)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [SeenDocument(papra_org_id=row["papra_org_id"],
                             papra_document_id=row["papra_document_id"],
                             name=row["name"], mime=row["mime"],
                             ingested_via=row["ingested_via"]) for row in rows]

    def ledger_cleanup_failed(
        self, *, after: tuple[str, str] | None = None, limit: int = 100,
    ) -> list[LedgerRef]:
        """References whose upstream delete the retries have stopped on.

        `ledger_cleanup_due` hides these on purpose — a permanently failed intent is not due
        — which is the same invisibility that made the debt un-findable from the CLI. The
        rows carry `cleanup_kreuzakt_id` and `cleanup_generation` because whoever settles
        them has to re-queue *that* intent: the id is only meaningful in the generation it
        was recorded in, and re-stamping it with the current one would turn an old integer
        into permission to delete whatever holds it now.
        """
        bounded = max(1, min(int(limit), 1000))
        sql = "SELECT * FROM index_ledger WHERE cleanup_state = 'failed'"
        params: list[object] = []
        if after is not None:
            sql += " AND (papra_org_id > ? OR (papra_org_id = ? AND papra_document_id > ?))"
            params.extend([after[0], after[0], after[1]])
        sql += " ORDER BY papra_org_id, papra_document_id LIMIT ?"
        params.append(bounded)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_ledger_ref(row) for row in rows]

    def ledger_delivered_refs(
        self, *, generation: int, after: tuple[str, str] | None = None,
        org_ids: list[str] | None = None, limit: int = 100,
    ) -> list[LedgerRef]:
        """Live references of `generation`, in a stable order, one page at a time.

        The name says "delivered" and the filter does not mean "we wrote this one", and
        that gap is deliberate enough to be worth the sentence it costs. The verification
        pass exists to converge Papra's text onto the index's, and the population it must
        reach is precisely the rows that were **never** delivered: every document indexed
        during phase one, holding Papra's own extraction, with `delivered_at` and
        `delivered_sha` both null. Filter on "delivered" and the pass converges nothing on
        the day the flag flips, while reporting a clean corpus — the one silent failure
        mode this task cannot afford. So eligibility is what it has always been here:
        bound to `generation`, and not deleted.

        What it does exclude, and why each one must not be written to:

        * no `kreuzakt_id` — there is no indexed text to compare against, so a pass could
          only guess; those rows are `ledger_needs_index`'s work, not this pass's.
        * a `delivered_generation` other than `generation` — that integer names an index
          that no longer exists, and the rule that stops the MCP server resolving it is
          the same rule that stops us PATCHing over a document we cannot identify.
        * `deleted_at` set — the user took the reference away; writing into the Papra
          document to "fix" its text would be resurrecting it by side effect.

        `after` is the `(papra_org_id, papra_document_id)` the previous page ended with.
        Keyset rather than offset because the walk takes minutes and a row can change
        under it: an offset would skip or repeat a reference whenever one became eligible
        mid-pass, and a repeated reference spends its repair budget twice, which is what
        decides whether the pass keeps trying to fix it.

        `org_ids=[]` is "no buckets", not "no filter", matching `ledger_resolve` and
        `ledger_stats`.
        """
        bounded = max(1, min(int(limit), 1000))
        sql = (
            "SELECT * FROM index_ledger WHERE deleted_at IS NULL"
            " AND kreuzakt_id IS NOT NULL AND delivered_generation = ?"
        )
        params: list[object] = [generation]
        if after is not None:
            sql += " AND (papra_org_id > ? OR (papra_org_id = ? AND papra_document_id > ?))"
            params.extend([after[0], after[0], after[1]])
        if org_ids is not None:
            if not org_ids:
                return []
            sql += " AND papra_org_id IN (" + ", ".join("?" * len(org_ids)) + ")"
            params.extend(org_ids)
        sql += " ORDER BY papra_org_id, papra_document_id LIMIT ?"
        params.append(bounded)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_ledger_ref(row) for row in rows]

    def ledger_mark_deleted(
        self, *, papra_org_id: str, papra_document_id: str,
    ) -> tuple[int | None, int]:
        """Soft-delete one reference; return `(kreuzakt_id, remaining_active_same_generation)`.

        `(-1, -1)` means the row has no Kreuzakt id (or does not exist), i.e.
        there is nothing upstream to delete — a different answer from `(7, 0)`,
        which means "this was the last live reference to Kreuzakt document 7" and
        is what authorises the upstream delete.

        A row still bound under a superseded generation also answers `(-1, -1)`,
        and that is the answer that has to be in the returned *id* and not just in
        the count: a stale integer means nothing, so it cannot authorise anything,
        and handing it back as though it did would point the caller's delete at
        whatever a restore has since filed under that number. The row is still
        soft-deleted — the reference is gone either way — it just queues nothing.

        The count is taken in the same transaction as the UPDATE, so two
        concurrent deletes of the two shared references cannot both see "one left"
        and both skip the upstream delete, leaving an orphan in Kreuzakt. Only
        rows of the *current* generation count as remaining: a stale sibling
        resolves to nothing and therefore protects nothing. When no generation is
        current (a corrupt `index_generation`) this raises and writes nothing — a
        reference marked deleted against an unknown index would be dropped without
        anything being queued for it.

        Scope of that guarantee: it makes the *count* correct at the moment the row
        is marked, nothing more. The upstream delete that a `(id, 0)` answer
        authorises happens later, in another pass, and has to revalidate against the
        generation and any surviving reference for itself — being owed a delete is
        not the same as being allowed to perform one.
        """
        with self._lock:
            # BEGIN IMMEDIATE: the reading below and the `deleted_at` write have to
            # be one atomic step. `with self._conn` alone does not open a
            # transaction until the first DML, so the generation and the existing
            # binding would be read outside it — and a `bump_generation()` from
            # another process landing in that gap makes the returned count describe
            # an index that had already been superseded. The caller acts on that
            # count by deleting a Kreuzakt document, so "stale but confident" is
            # the one answer it must not get.
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                generation = self._read_generation()
                now = _now()
                row = self._conn.execute(
                    "SELECT kreuzakt_id, delivered_generation FROM index_ledger"
                    " WHERE papra_org_id = ? AND papra_document_id = ?",
                    (papra_org_id, papra_document_id),
                ).fetchone()
                self._conn.execute(
                    "UPDATE index_ledger SET deleted_at = ?"
                    " WHERE papra_org_id = ? AND papra_document_id = ?",
                    (now, papra_org_id, papra_document_id),
                )
                if row is None or row["kreuzakt_id"] is None:
                    self._conn.commit()
                    return (-1, -1)
                if row["delivered_generation"] != generation:
                    # The binding is from an index that no longer exists. `remaining`
                    # would be a truthful 0 and `(id, 0)` reads as permission, and the
                    # cleanup pass downstream cannot recover it: its own generation
                    # check compares against the generation the row is *queued* under,
                    # which a bump makes current, and `ledger_resolve` is empty only
                    # because the backfill has not rebound anything yet. So: no id,
                    # no permission. The re-index will rebind and re-derive it.
                    self._conn.commit()
                    return (-1, -1)
                remaining = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM index_ledger WHERE kreuzakt_id = ?"
                    " AND deleted_at IS NULL AND delivered_generation = ?",
                    (row["kreuzakt_id"], generation),
                ).fetchone()["n"]
                self._conn.commit()
            except BaseException:
                # A refusal (an unusable generation, a locked database) leaves the
                # row untouched: a half-applied delete would report "marked, nothing
                # left to delete" and lose the reference without queueing anything.
                self._conn.rollback()
                raise
        return (row["kreuzakt_id"], remaining)

    def ledger_unbind(
        self, *, expected_kreuzakt_id: int, generation: int, reason: str,
        papra_org_id: str | None = None, papra_document_id: str | None = None,
    ) -> int:
        """Drop a binding the caller can name; report how many references it cleared.

        Clearing `kreuzakt_id` is what turns a reference back into work: with no id it is a
        `ledger_needs_index` candidate again, so the sweep, a rebuild and a backfill can all
        reach it. The state this exists to escape is the opposite one — a row still bound,
        under the generation that is current, to a Kreuzakt document that answers 404. That
        row still resolves, so the search path hands out a hit that leads nowhere, and it
        looks finished to every eligibility rule in this file, so nothing short of an
        operator resets it.

        Two fences, because clearing a binding destroys nothing and therefore gets away with
        being wrong right up until it does not:

        * `expected_kreuzakt_id` — the caller states which document it believes is gone. A
          bare `SET kreuzakt_id = NULL` aimed at a row or a bucket would be the unconditional
          write this plan refuses everywhere else, and the id is the one thing that names
          which claim is being retracted.
        * `generation` — an id from a superseded index resolves to nothing already, so
          unbinding it is not a repair but a guess about which row meant what. The same fence
          refuses to clear a row a rebuild has since rebound to a live document, which is the
          worst thing this call could do.

        Scope: pass `papra_org_id` and `papra_document_id` to retract one reference's
        binding, omit both to retract every reference to that document. Both forms are
        needed and they are not interchangeable. When a reference goes away in Papra, its own
        binding is what stops occupying the id, and a sibling in another bucket is still live
        and still needs it — clear that one and the search path stops finding a document
        nobody deleted. When the index itself says the document is gone, the verdict was
        about the document, so every reference pointing at it follows together.

        Returns references cleared, and logs one INFO line with the reason: "we deliberately
        stopped claiming this document is indexed" is the answer to a later "why is this
        document missing from search?", and it has to be findable. A non-positive
        `expected_kreuzakt_id` is refused rather than ignored — nothing is bound to `-1`, so
        a caller that arrived there has lost its place, and an unbind that reports success on
        the strength of a non-answer is how a stale integer earns trust.
        """
        if expected_kreuzakt_id is None or int(expected_kreuzakt_id) <= 0:
            raise ValueError(
                f"expected_kreuzakt_id must be a real kreuzakt id, got {expected_kreuzakt_id!r}:"
                " no reference is bound to a non-positive id, so there is nothing to retract"
                " and the unbind would be a guess")
        if not (reason or "").strip():
            raise ValueError("reason is required: an unbind with no reason is unauditable")
        if (papra_org_id is None) != (papra_document_id is None):
            raise ValueError(
                "scope is a pair: pass both papra_org_id and papra_document_id, or neither")
        scoped = papra_org_id is not None
        sql = ("UPDATE index_ledger SET kreuzakt_id = NULL, delivered_generation = NULL"
               " WHERE kreuzakt_id = ? AND delivered_generation = ?")
        params: list[object] = [int(expected_kreuzakt_id), generation]
        if scoped:
            sql += " AND papra_org_id = ? AND papra_document_id = ?"
            params.extend([papra_org_id, papra_document_id])
        with self._lock, self._conn:
            cursor = self._conn.execute(sql, params)
            cleared = cursor.rowcount
        if cleared:
            logger.info(
                "unbound kreuzakt=%s generation=%s references=%d: %s%s",
                expected_kreuzakt_id, generation, cleared, reason,
                f" papra={papra_org_id}/{papra_document_id}" if scoped else "")
        return cleared

    def ledger_mark_restored(self, *, papra_org_id: str, papra_document_id: str) -> None:
        """Clear `deleted_at`, which is the whole of a restore here.

        Nothing else is touched, deliberately. The generation is left alone, so a
        document deleted before a rebuild comes back stale rather than falsely
        current — stale means the sweep re-indexes it, which is the correct answer
        and not a state a restore can shortcut. A queued upstream delete is left
        pending for the cleanup pass to revalidate and skip: the revalidation is
        what proves the pass still checks, and a restore that quietly rewrote the
        cleanup state would hide that it never did.
        """
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE index_ledger SET deleted_at = NULL"
                " WHERE papra_org_id = ? AND papra_document_id = ?",
                (papra_org_id, papra_document_id),
            )

    def ledger_record_delivery_failure(
        self, *, papra_org_id: str, papra_document_id: str, expected_kreuzakt_id: int,
        generation: int, reason: str,
    ) -> int | None:
        """Charge one refused content write to the binding that was refused.

        Called from the write boundary, never from a pass: the counter means "a PATCH of
        ours was attempted against this reference and Papra said no", so a caller that
        could not reach Papra, could not read it, or never got as far as composing a write
        must not reach this method at all. Charging read failures would retire a corpus from
        convergence on a bad afternoon, silently, because the retired references are the
        ones the pass then stops writing.

        The write is fenced twice, for the reason `ledger_set_kreuzakt_id` fences for: a
        refusal is a fact about the binding that was tried and about the index it was tried
        against. If a re-index has replaced that binding while the write was in flight, or a
        rebuild has moved the generation underneath it, the answer about those bytes says
        nothing about the row that exists now, and charging it would bring a document that
        has never been tried straight to the ceiling that retires it. `None` means exactly
        that: not charged. The fence and the write share one `BEGIN IMMEDIATE`, so a
        `bump_generation()` from another thread cannot slip between them.

        The fifth one is the interesting row, so the fifth one is announced: after it the
        convergence pass stops attempting this reference, and a state that only shows up by
        not happening is a state nobody finds. The line carries `reason` because the store
        is the last place that still has it on the way to the operator.
        """
        if not isinstance(expected_kreuzakt_id, int) or isinstance(expected_kreuzakt_id, bool) \
                or expected_kreuzakt_id <= 0:
            raise ValueError(
                f"expected_kreuzakt_id must be a positive kreuzakt id, got "
                f"{expected_kreuzakt_id!r}: a refusal is a fact about the binding whose write "
                "papra refused, and `kreuzakt_id = NULL` matches no row, so a None here would "
                "make this method silently never charge anything")
        now = _now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if self._read_generation() != generation:
                    self._conn.rollback()
                    return None
                cursor = self._conn.execute(
                    "UPDATE index_ledger SET delivery_failures = delivery_failures + 1,"
                    " last_delivery_error_at = ?"
                    " WHERE papra_org_id = ? AND papra_document_id = ?"
                    " AND kreuzakt_id = ? AND delivered_generation = ?",
                    (now, papra_org_id, papra_document_id, expected_kreuzakt_id, generation),
                )
                if cursor.rowcount == 0:
                    self._conn.rollback()
                    return None
                row = self._conn.execute(
                    "SELECT delivery_failures FROM index_ledger"
                    " WHERE papra_org_id = ? AND papra_document_id = ?",
                    (papra_org_id, papra_document_id),
                ).fetchone()
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        count = int(row["delivery_failures"])
        if count == DELIVERY_MAX_FAILURES:
            logger.error(
                "papra=%s/%s has refused %d content writes in a row (%s): the convergence "
                "pass will not attempt this text again. Papra does not hold what the index "
                "holds; a re-index of the document retires this count, and nothing else "
                "will",
                papra_org_id, papra_document_id, count, reason,
            )
        return count

    def ledger_record_delivery(
        self, *, papra_org_id: str, papra_document_id: str, delivered_at: str,
        delivered_sha: str, generation: int,
    ) -> None:
        """Record what we last wrote into Papra, for the generation bound now.

        `delivered_sha` is the hash of the text we delivered — what the convergence pass
        records and reports, NOT what it compares against. v1 compared Papra's live text
        to this value and so could not see Papra overwriting us, which is the one failure
        the pass exists to catch (Task 7): the ledger's memory is a fact about us, and the
        pass asks the two services what they hold now. `EMPTY_EXTRACTION_SHA` is how
        "extraction produced nothing" becomes countable.

        This is an authorization write, not a receipt, because it stamps
        `delivered_generation` — the column that makes a binding resolve. So the
        write is conditional on the row being bound to `generation`: stamping the
        current generation onto a row still bound to a destroyed index would make
        that stale integer resolvable again, and a late worker restamping a rebound
        row would retire a live one. A refused write logs, because a tripwire
        nobody can see is a tripwire that will be stepped on: the return contract
        is `None` either way, so the row's state and this line are the evidence.

        A delivery that lands also retires the refused-write history, and only a delivery
        that lands does. The counter answers "has Papra been taking our text for this
        reference", and a write that arrived is the only answer that resets the question —
        so the count means *consecutive* refusals rather than a tally of bad days. The
        `EMPTY_EXTRACTION_SHA` record is the exception worth naming, because it goes through
        this same statement: stamping "the index produced no text" is not evidence of
        anything Papra did or did not accept, and letting it clear the history would let a
        blank scan disarm the one ceiling on re-writing a document Papra has refused.
        """
        text_delivered = delivered_sha != EMPTY_EXTRACTION_SHA
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "UPDATE index_ledger SET delivered_at = ?, delivered_sha = ?,"
                " delivered_generation = ?"
                # Built as two statements rather than a `CASE` because the columns being
                # written are the point of the difference, and a reader of the trace sees
                # exactly which of the two happened.
                + (", delivery_failures = 0, last_delivery_error_at = NULL"
                   if text_delivered else "")
                + " WHERE papra_org_id = ? AND papra_document_id = ?"
                " AND kreuzakt_id IS NOT NULL AND delivered_generation = ?",
                (delivered_at, delivered_sha, generation, papra_org_id, papra_document_id,
                 generation),
            )
            refused = cursor.rowcount == 0
        if refused:
            logger.warning(
                "delivery refused papra=%s/%s: no reference is bound to generation %s,"
                " so %s was not recorded",
                papra_org_id, papra_document_id, generation, delivered_sha,
            )

    def ledger_record_metadata(
        self, *, papra_org_id: str, papra_document_id: str, name: str | None = None,
        date: str | None = None, notes: str | None = None,
    ) -> None:
        """Record exactly the values we sent to Papra.

        Only the arguments that were given are written: an omitted field keeps its
        previous value, because "we did not touch `notes` this time" must not
        erase the record of what we wrote last time. Protection is not touched —
        lifting a protection is an operator decision with its own call, and a
        delivery that could clear one would un-protect a human's rename as a side
        effect of writing.
        """
        columns = {"name": name, "date": date, "notes": notes}
        given = [field for field, value in columns.items() if value is not None]
        if not given:
            return
        assignments = ", ".join(f"written_{field} = ?" for field in given)
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE index_ledger SET {assignments}"
                " WHERE papra_org_id = ? AND papra_document_id = ?",
                (*[columns[field] for field in given], papra_org_id, papra_document_id),
            )

    def ledger_protect(
        self, *, papra_org_id: str, papra_document_id: str, field: str,
    ) -> None:
        """Say "hands off this field" for one reference, permanently.

        A human edit makes the field theirs for the life of this reference; the
        only way back is `ledger_clear_protection`. `field` is checked against
        `PROTECTION_FIELDS` because it becomes part of a column name — a typo must
        be an exception here, not a protection that silently protects nothing.
        """
        if field not in PROTECTION_FIELDS:
            raise ValueError(
                f"field must be one of {', '.join(PROTECTION_FIELDS)}; got {field!r}"
            )
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE index_ledger SET {field}_protected = 1"
                " WHERE papra_org_id = ? AND papra_document_id = ?",
                (papra_org_id, papra_document_id),
            )

    def ledger_clear_protection(
        self, *, field: str | None = None, org_id: str | None = None,
        document_id: str | None = None,
    ) -> int:
        """Operator-only un-protect; return how many references were changed.

        `field=None` clears all three fields on each matched reference, and the
        count is references rather than flags: the CLI has to answer "how many
        documents did this change?", and counting flags would report three times
        the blast radius for the same documents.

        Every argument narrows the scope, so calling this with nothing at all
        clears protection corpus-wide. That is intended to be the operator typing
        a deliberate command, and the return value is what makes that radius
        legible in the CLI instead of a silent success.
        """
        if field is not None and field not in PROTECTION_FIELDS:
            raise ValueError(
                f"field must be one of {', '.join(PROTECTION_FIELDS)}; got {field!r}"
            )
        fields = PROTECTION_FIELDS if field is None else (field,)
        sql = "UPDATE index_ledger SET " + ", ".join(f"{f}_protected = 0" for f in fields)
        clauses: list[str] = []
        params: list[object] = []
        if org_id is not None:
            clauses.append("papra_org_id = ?")
            params.append(org_id)
        if document_id is not None:
            clauses.append("papra_document_id = ?")
            params.append(document_id)
        # SQLite counts every row the WHERE matched, protection or not, so without
        # this the return value would be "references in scope", and a repeat of the
        # command would report the same number while changing nothing.
        clauses.append("(" + " OR ".join(f"{f}_protected = 1" for f in fields) + ")")
        sql += " WHERE " + " AND ".join(clauses)
        with self._lock, self._conn:
            cursor = self._conn.execute(sql, params)
        return cursor.rowcount

    def _read_repair_row(self, papra_org_id: str, papra_document_id: str) -> sqlite3.Row | None:
        """The repair columns of one reference.

        **Caller must already hold `self._lock` and the write transaction.** The
        same reason `_read_cleanup_row` is a method rather than two inline
        statements: what makes the counter correct is that this read happens while
        the write lock is held, and that is not observable from the outside unless
        the read sits somewhere a test can stand.
        """
        return self._conn.execute(
            "SELECT repair_count, last_repair_at FROM index_ledger"
            " WHERE papra_org_id = ? AND papra_document_id = ?",
            (papra_org_id, papra_document_id),
        ).fetchone()

    def ledger_bump_repair(
        self, *, papra_org_id: str, papra_document_id: str,
    ) -> int:
        """Count a repair against this reference in a rolling 24 h window.

        Returns the new count, or 1 when the previous repair is older than the
        window — which is what makes "diverged more than twice in a day" a
        per-document judgement rather than a lifetime one. A permanently counted
        repair would suppress every future repair on a document that had one bad
        week.

        Raises for a reference that does not exist: repairing a document the ledger
        never saw is a caller bug, and answering 1 would let the caller proceed to
        write to Papra on a document it cannot vouch for.
        """
        now = _now()
        cutoff = _iso(datetime.now(UTC) - timedelta(hours=REPAIR_WINDOW_H))
        with self._lock:
            # BEGIN IMMEDIATE, like the other read-modify-writes here (the delete
            # count, the cleanup counter): `with self._conn` opens the transaction
            # at the first DML, so the SELECT would run outside it and two repairs
            # — the verification pass and an operator's forced one — could both read
            # 1 and both write 2. The lost increment is not cosmetic: Task 7
            # suppresses on two repairs inside this window, so the document that
            # keeps diverging is the one that never gets suppressed.
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_repair_row(papra_org_id, papra_document_id)
                if row is None:
                    self._conn.rollback()
                    raise ValueError(
                        f"unknown ledger reference: {papra_org_id}/{papra_document_id}"
                    )
                in_window = (row["last_repair_at"] is not None
                             and row["last_repair_at"] >= cutoff)
                count = row["repair_count"] + 1 if in_window else 1
                self._conn.execute(
                    "UPDATE index_ledger SET repair_count = ?, last_repair_at = ?"
                    " WHERE papra_org_id = ? AND papra_document_id = ?",
                    (count, now, papra_org_id, papra_document_id),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return count

    def ledger_repairs_in_window(
        self, *, papra_org_id: str, papra_document_id: str,
    ) -> int:
        """How many repairs this reference has ALREADY spent inside the rolling window.

        The read-only twin of `ledger_bump_repair`, and the reason it exists is a Task 7
        review finding: when the convergence pass bumped before it wrote, a PATCH Papra
        refused still cost a unit. A document whose write Papra persistently rejects (413,
        403, a persistent 5xx) therefore walked itself into suppression without ever
        having repaired once, and the suppression line then told the operator that two
        writers were competing for the column — sending them to look for a writer that was
        never there, because the real fault was a write that never landed. It is sticky
        too: the bump refreshes `last_repair_at`, so every pass renewed the window it was
        supposed to be escaping.

        So the ceiling is answered here and spent by `ledger_bump_repair` only once the
        text has actually been written. The ceiling still counts attempts rather than
        successes in the sense the plan wanted — a repair that lands and diverges again an
        hour later is the second loss whether or not Papra's answer was pleasant — but an
        attempt that wrote nothing is not a loss, it is a refusal, and it is reported as
        one.

        Answers one less than `ledger_bump_repair` would in the same state (0 where the
        bump returns 1), because the caller's repair is not in the number yet. Raises for
        a reference that does not exist, on the same rule as the bump: the row is the only
        thing that makes a repair countable, and answering 0 for a row we never saw would
        let a caller write to a document it cannot vouch for.
        """
        cutoff = _iso(datetime.now(UTC) - timedelta(hours=REPAIR_WINDOW_H))
        with self._lock:
            row = self._conn.execute(
                "SELECT repair_count, last_repair_at FROM index_ledger"
                " WHERE papra_org_id = ? AND papra_document_id = ?",
                (papra_org_id, papra_document_id),
            ).fetchone()
        if row is None:
            raise ValueError(f"unknown ledger reference: {papra_org_id}/{papra_document_id}")
        if row["last_repair_at"] is None or row["last_repair_at"] < cutoff:
            return 0
        return int(row["repair_count"])

    def ledger_stats(self, *, org_ids: list[str] | None = None) -> dict[str, int]:
        """Corpus counts for `check` and `index_status`.

        Counted in SQL, not by listing rows: a listing cap would turn a bad day
        into a normal-looking one.

        * `references` — every row, deleted or stale included.
        * `indexed_current` — has an id in the current generation. Everything that
          answers "is the index healthy?" is generation-scoped, because after a
          rebuild a stale row is not indexed, it is backlog.
        * `delivered` — we recorded a write for it in the current generation, blank
          extraction included. It is a receipt for what we say we sent, not a statement
          about what Papra's column holds now; only the convergence pass can say that.
          Counted by `delivered_at IS NOT NULL`, which is why an `EMPTY_EXTRACTION_SHA`
          row is inside it: `ledger_record_delivery` is the stamp for both.

        `indexed_current` and `delivered` count soft-deleted rows too: both answer
        "does a binding with this meaning exist in the index?", which a row still
        carrying it does, queued delete or not. That is a defensible reading, and
        it is not what the key's name suggests — a corpus with deletions in flight
        shows `indexed_current` above the number of live references. `references`
        is the denominator to read them against; nothing here filters on
        `deleted_at`.

        * `extraction_empty` — `delivered_sha` is `EMPTY_EXTRACTION_SHA`.
        * `protected_*` — fields a human took away from us.
        * `repairs_24h` — repairs inside the rolling window, summed.
        * `cleanup_pending` — upstream deletes still owed, any generation, whether
          still retrying or given up. The count is deliberately the whole debt: on
          the fifth failure a row leaves `pending`, and a counter that watched only
          `pending` would fall to 0 exactly when automatic retry stops — the
          operator's number going quiet while the debt turns permanent.
        * `cleanup_failed` — the subset of `cleanup_pending` the retries have
          stopped on. `papra-suite ledger --cleanup` settles them: it re-queues each
          row with the Kreuzakt id and generation it was queued under, so a delete
          whose index no longer exists is reported rather than re-armed, and drives
          one pass there and then. `cleanup_pending - cleanup_failed` are still
          being retried. Rows `skipped` by the cleanup pass are in neither: nothing
          is owed on them.
        * `delivery_blocked` — references the convergence pass has stopped writing to,
          because Papra refused `DELIVERY_MAX_FAILURES` content writes in a row. It counts
          references, not refusals: one refusal is an ordinary bad hour and belongs in the
          log, while this number is the set that will never converge on its own. It is not
          the same population as `repairs_24h`, which counts writes that landed.
        * `stale` — live references with no id in the current generation, which is
          `ledger_needs_index`'s predicate counted: backlog in the same currency the sweep
          spends work in. After a generation bump this is the whole corpus, and that is the
          correct reading — it is all waiting to be re-derived, not broken.
        * `unledgered` — documents this store marked seen that the ledger has never held.
          The count of the gap between ingestion history and index history, and the only
          number that can see that population at all. It is what these two tables jointly
          know: a Papra document this suite never listed is not knowable from here, so
          `unledgered == 0` means "nothing we have seen is missing", not "Papra and the
          ledger agree". Non-PDF documents are in this count and stay in it, because the
          index is PDF-only by design and the operator still has to be told who is left out.

        `org_ids=[]` is "no buckets", not "no filter": a token with nothing
        granted must not receive corpus-wide counts.
        """
        keys = LEDGER_STAT_KEYS
        if org_ids is not None and not org_ids:
            return {key: 0 for key, _ in keys}
        generation = self.get_generation()
        cutoff = _iso(datetime.now(UTC) - timedelta(hours=REPAIR_WINDOW_H))
        sql = (
            "SELECT COUNT(*) AS n_references,"
            " SUM(CASE WHEN kreuzakt_id IS NOT NULL AND delivered_generation = ?"
            " THEN 1 ELSE 0 END) AS indexed_current,"
            " SUM(CASE WHEN delivered_at IS NOT NULL AND delivered_generation = ?"
            " THEN 1 ELSE 0 END) AS delivered,"
            " SUM(CASE WHEN delivered_sha = ? AND delivered_generation = ?"
            " THEN 1 ELSE 0 END) AS extraction_empty,"
            " SUM(name_protected) AS protected_name,"
            " SUM(date_protected) AS protected_date,"
            " SUM(notes_protected) AS protected_notes,"
            " SUM(CASE WHEN last_repair_at IS NOT NULL AND last_repair_at >= ?"
            " THEN repair_count ELSE 0 END) AS repairs_24h,"
            " SUM(CASE WHEN cleanup_state IN ('pending', 'failed')"
            " THEN 1 ELSE 0 END) AS cleanup_pending,"
            " SUM(CASE WHEN cleanup_state = 'failed' THEN 1 ELSE 0 END) AS cleanup_failed,"
            " SUM(CASE WHEN delivery_failures >= ? THEN 1 ELSE 0 END) AS delivery_blocked,"
            # The same predicate `ledger_needs_index` applies, so the number an operator
            # compares against `references` answers "how much of my corpus is not in this
            # index?" in the same currency the sweep spends work in.
            " SUM(CASE WHEN deleted_at IS NULL AND (kreuzakt_id IS NULL"
            " OR delivered_generation IS NULL OR delivered_generation != ?)"
            " THEN 1 ELSE 0 END) AS stale,"
            # The anti-join, counted rather than walked: this is the number that used to be
            # structurally invisible, and `ledger --status` prints it beside `stale` so the
            # two ways a document can be missing from the index are not confused.
            " (SELECT COUNT(*) FROM documents d WHERE NOT EXISTS"
            "  (SELECT 1 FROM index_ledger l WHERE l.papra_org_id = d.papra_org_id"
            "   AND l.papra_document_id = d.papra_document_id)"
            + (" AND d.papra_org_id IN (" + ", ".join("?" * len(org_ids)) + ")"
               if org_ids is not None else "")
            + ") AS unledgered"
            " FROM index_ledger"
        )
        params: list[object] = [generation, generation, EMPTY_EXTRACTION_SHA, generation,
                                cutoff, DELIVERY_MAX_FAILURES, generation]
        if org_ids is not None:
            params.extend(org_ids)
        if org_ids is not None:
            sql += " WHERE papra_org_id IN (" + ", ".join("?" * len(org_ids)) + ")"
            params.extend(org_ids)
        with self._lock:
            row = self._conn.execute(sql, params).fetchone()
        return {key: int(row[alias] or 0) for key, alias in keys}

    # -- ledger cleanup queue -------------------------------------------------

    def ledger_queue_cleanup(
        self, *, papra_org_id: str, papra_document_id: str, due_at: str,
        kreuzakt_id: int, generation: int, reset_attempts: bool = False,
        expected_state: str | None = None,
    ) -> bool:
        """Make "delete Kreuzakt document N" outlive this process; report whether it is queued.

        The intent has to live in the ledger: a crash between marking a reference
        deleted and deleting upstream must not lose the orphan. `cleanup_kreuzakt_id`
        and `cleanup_generation` are recorded together because the id is only
        correct for that generation — the cleanup pass refuses to act when they no
        longer agree with the store, which is what stops a rebuilt index's reused
        integer from being deleted.

        `cleanup_attempts` is deliberately not reset, and that default is what
        keeps the cap meaning anything: re-queueing happens on ordinary traffic (a
        document deleted, restored, deleted again), and if every re-queue refilled
        the allowance a permanently failing delete would get five fresh attempts
        forever and never reach `failed`. So the counter survives by default, and
        `reset_attempts=True` exists for the one caller entitled to refill it: an
        operator saying "the outage is over, try again", which is
        `papra-suite ledger --cleanup`.

        That caller also passes `expected_state="failed"`, and that is the fence on the
        reset. It re-queues a row it read as given-up, and a row that stopped being
        given-up in between — the daemon's own pass took it, or a restore closed it — is not
        the row the operator was looking at. Refusing on the mismatch returns False and
        writes nothing, which is the difference between a reset and a guess: without it,
        `--cleanup` could refill the allowance of a delete that had already succeeded and
        set it running again. `False` has to be the answer rather than a silent no-op in
        the `None` sense, because the CLI reports how many rows it settled.

        Note what the fence is *not*: `generation` is written as given, so a caller settling
        a `failed` row passes the row's own `cleanup_generation` back. Re-stamping the
        current one would authorise a delete whose Kreuzakt id belongs to a destroyed index.
        """
        _require_store_utc_iso(due_at, "due_at")
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "UPDATE index_ledger SET cleanup_state = 'pending', cleanup_next_at = ?,"
                " cleanup_kreuzakt_id = ?, cleanup_generation = ?,"
                " cleanup_attempts = CASE WHEN ? THEN 0 ELSE cleanup_attempts END"
                " WHERE papra_org_id = ? AND papra_document_id = ?"
                + (" AND cleanup_state = ?" if expected_state is not None else ""),
                (due_at, kreuzakt_id, generation, 1 if reset_attempts else 0,
                 papra_org_id, papra_document_id,
                 *([expected_state] if expected_state is not None else [])),
            )
        return cursor.rowcount > 0

    def ledger_cleanup_due(self, *, now: str, limit: int = 50) -> list[LedgerRef]:
        """Queued upstream deletes that have come due, earliest first.

        `done`, `failed` and `skipped` rows are excluded by state, so a permanently
        failed delete stops being retried forever instead of being re-attempted by
        every pass, and one the cleanup pass decided against stays decided; a
        future `cleanup_next_at` is the backoff. A `pending` row with no
        `cleanup_next_at` at all is due immediately — nothing scheduled it away,
        and `NULL <= now` is NULL, which would otherwise strand the delete out of
        sight of every later pass. The caller still has to revalidate each one —
        being due is not the same as being allowed.
        """
        bounded = max(1, min(int(limit), 1000))
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM index_ledger WHERE cleanup_state = 'pending'"
                " AND (cleanup_next_at IS NULL OR cleanup_next_at <= ?)"
                " ORDER BY cleanup_next_at, papra_document_id LIMIT ?",
                (now, bounded),
            ).fetchall()
        return [_row_to_ledger_ref(row) for row in rows]

    def ledger_skip_cleanup(
        self, *, papra_org_id: str, papra_document_id: str, reason: str | None = None,
    ) -> None:
        """Record "this queued delete is not ours to perform" as its own outcome.

        The cleanup pass has three honest reasons to reach for it instead of
        success: the Papra document was restored, the generation moved so the
        queued id names different content now, or a live reference still resolves
        to that id. None of them is a success (nothing was deleted upstream) and
        none is a failure (nothing is retrying), so the state machine carries a
        fourth outcome rather than making the pass hand-write an UPDATE against the
        table the MCP server authorizes from.

        `skipped` is terminal and silent in the counts: not due again, not in
        `cleanup_pending` or `cleanup_failed`, and not retried by a forced pass — a
        row that becomes owed again is re-queued explicitly by
        `ledger_queue_cleanup`, which is the only thing that moves it back to
        `pending`. The one line it writes is INFO, because "we deliberately did not
        delete this" is the answer to "why is that still in Kreuzakt?" and must not
        read as a failing delete. Raises for a reference the ledger never saw, like
        the other cleanup writers.
        """
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "UPDATE index_ledger SET cleanup_state = 'skipped', cleanup_next_at = NULL"
                " WHERE papra_org_id = ? AND papra_document_id = ?",
                (papra_org_id, papra_document_id),
            )
            unknown = cursor.rowcount == 0
        if unknown:
            raise ValueError(f"unknown ledger reference: {papra_org_id}/{papra_document_id}")
        logger.info("cleanup skipped papra=%s/%s: %s",
                    papra_org_id, papra_document_id, reason)

    def _read_cleanup_row(self, papra_org_id: str, papra_document_id: str) -> sqlite3.Row | None:
        """The cleanup columns of one reference.

        **Caller must already hold `self._lock` and the write transaction.** It is a
        method rather than an inline statement so the transaction's scope is
        testable: what makes the attempt counter correct is that this read happens
        while the write lock is held, and that is not observable from the outside
        unless the read is somewhere a test can stand.
        """
        return self._conn.execute(
            "SELECT cleanup_state, cleanup_attempts, cleanup_kreuzakt_id FROM index_ledger"
            " WHERE papra_org_id = ? AND papra_document_id = ?",
            (papra_org_id, papra_document_id),
        ).fetchone()

    def ledger_cleanup_result(
        self, *, papra_org_id: str, papra_document_id: str, ok: bool,
        next_at: str | None = None, reason: str | None = None,
    ) -> None:
        """Record one upstream delete attempt and move the state machine.

        Success closes the row (`done`, no next time) and says nothing: the caller
        logs its per-pass tally, and a completion line written in the same voice as
        the failure lines only makes them harder to read. Failure keeps the row
        `pending` with the caller's backoff — or, once `CLEANUP_MAX_ATTEMPTS` is
        reached, marks it `failed` and logs one ERROR at that transition, because
        "we are still trying" repeated every pass is noise while "we have stopped
        trying" is the fact an operator has to hear exactly once. There is no reason
        column in the schema, so the upstream error travels in that log line.

        The fourth outcome, `skipped`, is `ledger_skip_cleanup`'s: it is a decision
        about whether the delete should happen at all, not an attempt that failed.
        """
        if next_at is not None:
            _require_store_utc_iso(next_at, "next_at")
        permanent_now = False
        was_failed = False
        cleanup_kreuzakt_id: int | None = None
        attempts = 0
        with self._lock:
            # BEGIN IMMEDIATE: reading `cleanup_attempts` and `cleanup_state` and
            # writing them back is one decision. Outside the write lock, the
            # daemon's cleanup pass and a second writer of the same row — a second
            # process, or `papra-suite ledger --cleanup` refilling the same row — can both read
            # attempt 4, both write 5, lose an increment, and announce the permanent failure
            # twice — which breaks the one thing that log line is for: being
            # countable.
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_cleanup_row(papra_org_id, papra_document_id)
                if row is None:
                    self._conn.rollback()
                    raise ValueError(
                        f"unknown ledger reference: {papra_org_id}/{papra_document_id}"
                    )
                if ok:
                    self._conn.execute(
                        "UPDATE index_ledger SET cleanup_state = 'done',"
                        " cleanup_next_at = NULL"
                        " WHERE papra_org_id = ? AND papra_document_id = ?",
                        (papra_org_id, papra_document_id),
                    )
                    self._conn.commit()
                    return
                was_failed = row["cleanup_state"] == "failed"
                attempts = row["cleanup_attempts"] + 1
                permanent_now = attempts >= CLEANUP_MAX_ATTEMPTS
                self._conn.execute(
                    "UPDATE index_ledger SET cleanup_state = ?, cleanup_attempts = ?,"
                    " cleanup_next_at = ?"
                    " WHERE papra_org_id = ? AND papra_document_id = ?",
                    ("failed" if permanent_now else "pending", attempts,
                     next_at if next_at is not None else _now(),
                     papra_org_id, papra_document_id),
                )
                cleanup_kreuzakt_id = row["cleanup_kreuzakt_id"]
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        # Success is silent: the caller logs its per-pass tally, and a completion
        # line written next to the failure lines would only make them harder to
        # read. The ERROR belongs to the transition into `failed`, not to every
        # later report about a row that already is — an operator scraping logs for
        # "we have stopped trying" has to be able to count the times it happened.
        if permanent_now and not was_failed:
            logger.error(
                "cleanup failed permanently papra=%s/%s kreuzakt=%s after %s attempts: %s",
                papra_org_id, papra_document_id, cleanup_kreuzakt_id, attempts, reason,
            )
        elif was_failed:
            logger.warning(
                "cleanup still failing after permanent failure papra=%s/%s"
                " kreuzakt=%s attempt=%s: %s",
                papra_org_id, papra_document_id, cleanup_kreuzakt_id, attempts, reason,
            )
        else:
            logger.warning(
                "cleanup failed, will retry papra=%s/%s kreuzakt=%s attempt=%s: %s",
                papra_org_id, papra_document_id, cleanup_kreuzakt_id, attempts, reason,
            )
