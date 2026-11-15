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


MIGRATIONS = [_migration_001_initial]


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
