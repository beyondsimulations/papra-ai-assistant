import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


LOG_RING_BUFFER_SIZE = 1000

VALID_SSL_MODES = ("ssl", "starttls", "plain")

# How long a "this is why nothing arrived" row stays worth keeping. Ninety days
# outlives every realistic "where is my invoice?" question while keeping the
# table small enough to read; older than that, the answer belongs to the logs or
# to `replay`, not to this window.
INGEST_EVENT_RETENTION_DAYS = 90


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


MIGRATIONS = [_migration_001_initial, _migration_002_ingest_events]

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
                "SELECT id FROM jobs WHERE kind = ? AND papra_org_id = ? "
                "AND document_id = ? AND content_hash = ?",
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
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE jobs SET status = 'pending', attempts = 0, error = NULL, "
                "next_attempt_at = ?, updated_at = ? WHERE id = ?",
                (_now(), _now(), job_id),
            )

    def list_jobs(self, *, status: str | None = None, limit: int = 100) -> list[Job]:
        query = "SELECT * FROM jobs"
        params: tuple = ()
        if status:
            query += " WHERE status = ?"
            params = (status,)
        query += " ORDER BY created_at DESC LIMIT ?"
        params = params + (limit,)
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [_row_to_job(row) for row in rows]

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
