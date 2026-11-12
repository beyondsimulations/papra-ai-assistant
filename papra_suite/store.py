import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


LOG_RING_BUFFER_SIZE = 1000


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
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def migrate(self) -> None:
        with self._lock:
            current = self._conn.execute("PRAGMA user_version").fetchone()[0]
            for index in range(current, len(MIGRATIONS)):
                with self._conn:
                    MIGRATIONS[index](self._conn)
                    self._conn.execute(f"PRAGMA user_version = {index + 1}")

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
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE kind = ? AND status = 'pending' "
                "AND next_attempt_at <= ? ORDER BY created_at LIMIT 1",
                (kind, now),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE jobs SET status = 'running', updated_at = ? WHERE id = ?",
                (now, row["id"]),
            )
            row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
        return _row_to_job(row)

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
