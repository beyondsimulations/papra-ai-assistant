# Papra Suite — Milestone 1: Foundation (scaffold, config, store, logging, Papra client) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the M1 foundation of the Papra Suite service: project scaffold, env config, SQLite store with migrations, JSON logging with secret redaction, and an async Papra REST client — proven by an integration test round-trip against a scratch Papra container.

**Architecture:** Single asyncio Python process (this milestone delivers libraries, not yet the server). `config.py` parses/validates env at startup; `store.py` wraps a plain sqlite3 connection with `PRAGMA user_version` migrations; `logging.py` provides JSON-lines stdout output plus a DB ring buffer, both behind one shared secret-redaction filter; `papra_client.py` is a thin async httpx wrapper with a single retry policy. The CLI gains a `check` command that validates config, opens the store, and migrates — the milestone's "run" deliverable.

**Tech Stack:** Python 3.12, httpx, stdlib sqlite3/logging/argparse, pytest + pytest-asyncio, ruff, Docker Compose (test stack: `papra/papra:latest`).

**Spec:** `spec.md` (build-to spec) + `docs/superpowers/specs/2026-11-12-papra-suite-implementation-decisions-design.md` (implementation decisions; wins on conflict). Read both before starting.

## Global Constraints

- Python 3.12. Runtime deps: stdlib + httpx only in this milestone (fastapi, jinja2, itsdangerous, pymupdf, pillow, fastmcp arrive in later milestones; PyMuPDF is used in the integration test fixture, so it is installed as a dev dep here and moves to runtime in M3).
- No ORM — plain `sqlite3`. No Celery, no Redis, no message broker.
- `requirements.txt` is pinned (exact `==` versions).
- Logging: JSON lines to stdout; secrets (IMAP passwords, API keys, admin password) redacted by a single shared filter, not per-callsite discipline.
- AGPL hygiene: read Papra upstream for facts only (API paths, payload shapes); never copy its code into this repo.
- Test suite runnable with one command (`make test`); unit tests must not need Docker.
- Every task ends green: `ruff check .` clean, `pytest` green, `python -m papra_suite check` runs.
- Commit after every task; commit messages in the form shown in each task.

---

### Task 1: Scaffold + config module + `check` command

**Files:**
- Create: `papra_suite/__init__.py`, `papra_suite/config.py`, `papra_suite/__main__.py`
- Create: `requirements.txt`, `requirements-dev.txt`, `pyproject.toml`, `Makefile`, `.gitignore`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: `AppConfig` dataclass and `load_config(env)` / `ConfigError` in `papra_suite.config` — every later module receives an `AppConfig`. Exact fields:

```python
@dataclass(frozen=True)
class AppConfig:
    papra_base_url: str      # no trailing slash
    papra_api_key: str
    llm_base_url: str        # no trailing slash
    llm_api_key: str
    title_model: str
    ocr_model: str
    admin_password: str
    dashboard_host: str      # parsed from DASHBOARD_BIND
    dashboard_port: int
    data_dir: Path
    ocr_max_pages: int
    ocr_page_pixel_budget: int
    ocr_dpi: int
    ocr_concurrency: int    # default 3, hard-capped at 3
    sweep_interval_s: int    # default 300, 0 disables the sweep
```

- [ ] **Step 1: Create the scaffold files**

`requirements.txt`:

```
httpx==0.28.1
```

`requirements-dev.txt`:

```
-r requirements.txt
pytest==8.3.4
pytest-asyncio==0.25.2
ruff==0.8.6
pymupdf==1.25.1
```

`pyproject.toml`:

```toml
[project]
name = "papra-suite"
version = "0.1.0"
requires-python = ">=3.12"

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
markers = ["integration: requires the scratch Papra compose stack"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]
```

`Makefile`:

```make
.PHONY: install lint test-unit test

install:
	pip install -r requirements-dev.txt

lint:
	ruff check .

test-unit:
	pytest -m "not integration"
```

`.gitignore`:

```
.env
__pycache__/
.pytest_cache/
.ruff_cache/
.venv/
data/
*.db
```

`papra_suite/__init__.py`:

```python
__version__ = "0.1.0"
```

- [ ] **Step 2: Write the failing config tests**

`tests/test_config.py`:

```python
import pytest

from papra_suite.config import ConfigError, load_config

BASE_ENV = {
    "PAPRA_BASE_URL": "http://papra:1221",
    "PAPRA_API_KEY": "ppapi_" + "x" * 64,
    "LLM_BASE_URL": "http://llm:8000",
    "LLM_API_KEY": "sk-test",
    "TITLE_MODEL": "qwen3-4b",
    "OCR_MODEL": "qwen3-vl-8b",
    "ADMIN_PASSWORD": "correct-horse-battery-staple",
}


def test_load_config_applies_defaults(tmp_path):
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})

    assert config.dashboard_host == "127.0.0.1"
    assert config.dashboard_port == 8080
    assert config.ocr_max_pages == 50
    assert config.ocr_page_pixel_budget == 2_000_000
    assert config.ocr_dpi == 150
    assert config.ocr_concurrency == 3
    assert config.sweep_interval_s == 300
    assert config.papra_base_url == "http://papra:1221"


def test_load_config_strips_trailing_slashes():
    config = load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221/"})

    assert config.papra_base_url == "http://papra:1221"


def test_missing_required_var_names_the_var():
    env = dict(BASE_ENV)
    del env["PAPRA_API_KEY"]

    with pytest.raises(ConfigError, match="PAPRA_API_KEY"):
        load_config(env)


def test_ocr_concurrency_hard_capped_at_3():
    config = load_config({**BASE_ENV, "OCR_CONCURRENCY": "10"})

    assert config.ocr_concurrency == 3


def test_dashboard_bind_parse():
    config = load_config({**BASE_ENV, "DASHBOARD_BIND": "0.0.0.0:9999"})

    assert config.dashboard_host == "0.0.0.0"
    assert config.dashboard_port == 9999


def test_bad_dashboard_bind_raises():
    with pytest.raises(ConfigError, match="DASHBOARD_BIND"):
        load_config({**BASE_ENV, "DASHBOARD_BIND": "nope"})
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'papra_suite'` (or collection error)

- [ ] **Step 4: Implement `papra_suite/config.py`**

```python
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class AppConfig:
    papra_base_url: str
    papra_api_key: str
    llm_base_url: str
    llm_api_key: str
    title_model: str
    ocr_model: str
    admin_password: str
    dashboard_host: str
    dashboard_port: int
    data_dir: Path
    ocr_max_pages: int
    ocr_page_pixel_budget: int
    ocr_dpi: int
    ocr_concurrency: int
    sweep_interval_s: int


REQUIRED = (
    "PAPRA_BASE_URL",
    "PAPRA_API_KEY",
    "LLM_BASE_URL",
    "LLM_API_KEY",
    "TITLE_MODEL",
    "OCR_MODEL",
    "ADMIN_PASSWORD",
)

DEFAULT_BIND = "127.0.0.1:8080"


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _int_env(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got: {raw!r}") from None


def _parse_bind(raw: str) -> tuple[str, int]:
    parsed = urlparse(f"//{raw}")
    if not parsed.hostname or parsed.port is None:
        raise ConfigError(f"DASHBOARD_BIND must be 'host:port', got: {raw!r}")
    return parsed.hostname, parsed.port


def load_config(env: Mapping[str, str] | None = None) -> AppConfig:
    env = os.environ if env is None else env

    host, port = _parse_bind(env.get("DASHBOARD_BIND", "").strip() or DEFAULT_BIND)

    return AppConfig(
        papra_base_url=_require(env, "PAPRA_BASE_URL").rstrip("/"),
        papra_api_key=_require(env, "PAPRA_API_KEY"),
        llm_base_url=_require(env, "LLM_BASE_URL").rstrip("/"),
        llm_api_key=_require(env, "LLM_API_KEY"),
        title_model=_require(env, "TITLE_MODEL"),
        ocr_model=_require(env, "OCR_MODEL"),
        admin_password=_require(env, "ADMIN_PASSWORD"),
        dashboard_host=host,
        dashboard_port=port,
        data_dir=Path(env.get("DATA_DIR", "").strip() or "/app/data"),
        ocr_max_pages=_int_env(env, "OCR_MAX_PAGES", 50),
        ocr_page_pixel_budget=_int_env(env, "OCR_PAGE_PIXEL_BUDGET", 2_000_000),
        ocr_dpi=_int_env(env, "OCR_DPI", 150),
        ocr_concurrency=min(_int_env(env, "OCR_CONCURRENCY", 3), 3),
        sweep_interval_s=_int_env(env, "SWEEP_INTERVAL_S", 300),
    )
```

- [ ] **Step 5: Implement `papra_suite/__main__.py` (the `check` command)**

```python
import argparse
import logging
import sys

from papra_suite import __version__
from papra_suite.config import ConfigError, load_config

logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(prog="papra-suite")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="validate config, open and migrate the store, exit")

    args = parser.parse_args()

    try:
        config = load_config()
    except ConfigError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2

    if args.command == "check":
        config.data_dir.mkdir(parents=True, exist_ok=True)
        print(f"config ok; data dir: {config.data_dir}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 6: Run tests and lint; verify `check` runs**

Run: `pytest tests/test_config.py -v`
Expected: all PASS

Run: `ruff check .`
Expected: clean

Run: `PAPRA_BASE_URL=http://papra:1221 PAPRA_API_KEY=k LLM_BASE_URL=http://l LLM_API_KEY=k TITLE_MODEL=t OCR_MODEL=o ADMIN_PASSWORD=p python -m papra_suite check`
Expected: prints `config ok; data dir: data` and exit code 0

- [ ] **Step 7: Commit**

```bash
git add papra_suite/ tests/test_config.py requirements.txt requirements-dev.txt pyproject.toml Makefile .gitignore
git commit -m "feat(m1): project scaffold, env config validation, check command"
```

---

### Task 2: SQLite store — schema, migrations, helpers

**Files:**
- Create: `papra_suite/store.py`
- Modify: `papra_suite/__main__.py` (`check` now migrates the store)
- Test: `tests/test_store.py`

**Interfaces:**
- Consumes: `AppConfig.data_dir` from Task 1.
- Produces: `papra_suite.store.Store` and `papra_suite.store.Job`. Later milestones (pipeline, mail, sweep, dashboard) rely on these exact signatures:

```python
class Store:
    def __init__(self, db_path: Path) -> None            # creates parent dirs, opens WAL sqlite3 conn
    def migrate(self) -> None                            # applies MIGRATIONS via PRAGMA user_version
    def close(self) -> None
    # settings
    def get_setting(self, key: str) -> str | None
    def set_setting(self, key: str, value: str) -> None
    # jobs
    def enqueue_job(self, *, kind: str, papra_org_id: str, document_id: str,
                    content_hash: str, payload: dict) -> str   # idempotent; returns existing id on conflict
    def claim_next_job(self, kind: str) -> Job | None    # pending & due -> running, attempts stays
    def get_job(self, job_id: str) -> Job | None
    def mark_job_done(self, job_id: str) -> None
    def mark_job_failed(self, job_id: str, error: str, *, max_attempts: int = 3,
                         backoff_base_s: int = 30) -> None  # re-queues with backoff, or 'failed' at max
    def reset_job(self, job_id: str) -> None              # retry button: pending, attempts=0, no error
    def list_jobs(self, *, status: str | None = None, limit: int = 100) -> list[Job]
    # documents seen-table (sweep + mail)
    def record_document_seen(self, *, papra_org_id: str, papra_document_id: str,
                              name: str, mime: str, ingested_via: str) -> None
    def document_seen(self, papra_org_id: str, papra_document_id: str) -> bool
    # mail dedup (separate from job idempotency)
    def dedup_exists(self, dedup_hash: str) -> bool
    def add_dedup(self, dedup_hash: str) -> None
    # log ring buffer
    def append_log_line(self, ts: str, level: str, line: str) -> None
    def recent_log_lines(self, limit: int = 500) -> list[tuple[str, str, str]]  # (ts, level, line)
```

```python
@dataclass
class Job:
    id: str
    kind: str
    papra_org_id: str
    document_id: str
    content_hash: str
    payload: dict
    status: str        # 'pending' | 'running' | 'done' | 'failed'
    attempts: int
    next_attempt_at: str   # ISO-8601 UTC
    error: str | None
    created_at: str
    updated_at: str
```

- [ ] **Step 1: Write the failing store tests**

`tests/test_store.py`:

```python
import pytest

from papra_suite.store import Store


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


def test_migrate_is_idempotent(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.migrate()
    store.close()


def test_settings_roundtrip(store):
    assert store.get_setting("missing") is None
    store.set_setting("key", "value")
    assert store.get_setting("key") == "value"


def test_enqueue_job_is_idempotent(store):
    first = store.enqueue_job(
        kind="ocr", papra_org_id="org_1", document_id="doc_1",
        content_hash="abc", payload={"page_count": 2},
    )
    second = store.enqueue_job(
        kind="ocr", papra_org_id="org_1", document_id="doc_1",
        content_hash="abc", payload={"page_count": 2},
    )
    assert first == second
    assert len(store.list_jobs()) == 1


def test_different_content_hash_creates_new_job(store):
    a = store.enqueue_job(kind="title", papra_org_id="org_1", document_id="doc_1",
                           content_hash="h1", payload={})
    b = store.enqueue_job(kind="title", papra_org_id="org_1", document_id="doc_1",
                           content_hash="h2", payload={})
    assert a != b
    assert len(store.list_jobs()) == 2


def test_claim_next_job_transitions_to_running(store):
    store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                      content_hash="abc", payload={})
    job = store.claim_next_job("ocr")
    assert job is not None and job.status == "running"
    # claimed job is not claimable again
    assert store.claim_next_job("ocr") is None


def test_mark_job_failed_requeues_with_backoff_then_fails(store):
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                               content_hash="abc", payload={})
    job = store.claim_next_job("ocr")

    store.mark_job_failed(job.id, "boom", backoff_base_s=0)
    assert store.get_job(job_id).status == "pending"
    assert store.get_job(job_id).attempts == 1

    store.claim_next_job("ocr")
    store.mark_job_failed(job_id, "boom", backoff_base_s=0)
    store.claim_next_job("ocr")
    store.mark_job_failed(job_id, "boom", backoff_base_s=0)
    assert store.get_job(job_id).status == "failed"
    assert store.get_job(job_id).attempts == 3
    assert "boom" in store.get_job(job_id).error


def test_reset_job_revives_failed_job(store):
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                               content_hash="abc", payload={})
    for _ in range(3):
        job = store.claim_next_job("ocr")
        store.mark_job_failed(job.id, "boom", backoff_base_s=0)
    assert store.get_job(job_id).status == "failed"

    store.reset_job(job_id)
    revived = store.get_job(job_id)
    assert revived.status == "pending" and revived.attempts == 0 and revived.error is None


def test_document_seen_roundtrip(store):
    assert not store.document_seen("org_1", "doc_1")
    store.record_document_seen(papra_org_id="org_1", papra_document_id="doc_1",
                               name="bill.pdf", mime="application/pdf", ingested_via="mail")
    assert store.document_seen("org_1", "doc_1")


def test_dedup_roundtrip(store):
    assert not store.dedup_exists("hash-1")
    store.add_dedup("hash-1")
    assert store.dedup_exists("hash-1")


def test_log_ring_buffer_capped(store):
    for i in range(1200):
        store.append_log_line(ts=f"2026-11-12T00:00:{i % 60:02d}Z", level="INFO", line=f"line {i}")
    lines = store.recent_log_lines(limit=500)
    assert len(lines) == 500
    assert lines[-1][2] == "line 1199"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_store.py -v`
Expected: FAIL with `ModuleNotFoundError` / `ImportError`

- [ ] **Step 3: Implement `papra_suite/store.py`**

```python
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


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


LOG_RING_BUFFER_SIZE = 1000


def _migration_001_initial(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE orgs (
            id TEXT PRIMARY KEY,
            papra_org_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL
        );

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
        );

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
        );
        CREATE UNIQUE INDEX idx_jobs_idempotency
            ON jobs(kind, papra_org_id, document_id, content_hash);
        CREATE INDEX idx_jobs_claim ON jobs(kind, status, next_attempt_at);

        CREATE TABLE documents (
            papra_org_id TEXT NOT NULL,
            papra_document_id TEXT NOT NULL,
            name TEXT NOT NULL,
            mime TEXT NOT NULL,
            ingested_via TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            PRIMARY KEY (papra_org_id, papra_document_id)
        );

        CREATE TABLE dedup_hashes (
            hash TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        );

        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE log_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            line TEXT NOT NULL
        );
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
                next_at = _iso(datetime.now(UTC) + timedelta(seconds=backoff_base_s * 4 ** (attempts - 1)))
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
```

Note: `_parse_iso` is not used yet — remove it if ruff flags it, or keep it only once used; prefer removing.

- [ ] **Step 4: Wire the store into `__main__.py check`**

Replace the `if args.command == "check":` block in `papra_suite/__main__.py` with:

```python
    if args.command == "check":
        from papra_suite.store import Store

        config.data_dir.mkdir(parents=True, exist_ok=True)
        store = Store(config.data_dir / "state.db")
        try:
            store.migrate()
        finally:
            store.close()
        print(f"config ok; store migrated; data dir: {config.data_dir}")
        return 0
```

- [ ] **Step 5: Run tests and lint; verify `check` migrates**

Run: `pytest tests/test_store.py tests/test_config.py -v`
Expected: all PASS

Run: `ruff check .`
Expected: clean

Run: `PAPRA_BASE_URL=http://p PAPRA_API_KEY=k LLM_BASE_URL=http://l LLM_API_KEY=k TITLE_MODEL=t OCR_MODEL=o ADMIN_PASSWORD=p DATA_DIR=/tmp/papra-check python -m papra_suite check && rm -rf /tmp/papra-check`
Expected: prints `config ok; store migrated; ...` and exit code 0

- [ ] **Step 6: Commit**

```bash
git add papra_suite/store.py papra_suite/__main__.py tests/test_store.py
git commit -m "feat(m1): sqlite store with user_version migrations, jobs, dedup, ring buffer"
```

---

### Task 3: Logging — JSON lines, shared secret redaction, DB ring buffer

**Files:**
- Create: `papra_suite/logging_setup.py`
- Test: `tests/test_logging_setup.py`

**Interfaces:**
- Consumes: `Store.append_log_line` / `recent_log_lines` from Task 2.
- Produces: `setup_logging(store: Store | None = None) -> None` and `SECRET_REGISTRY` (a `SecretRegistry` singleton with `.add(value: str)`). Later milestones call `SECRET_REGISTRY.add(...)` for every secret they learn (config at startup, mailbox passwords on load) and `setup_logging(store)` once at process start. Module is named `logging_setup` to avoid shadowing stdlib `logging` in imports.

- [ ] **Step 1: Write the failing logging tests**

`tests/test_logging_setup.py`:

```python
import json
import logging

import pytest

from papra_suite.logging_setup import SECRET_REGISTRY, setup_logging
from papra_suite.store import Store


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


def test_output_is_json_lines_with_expected_keys(store, capsys):
    setup_logging(store)
    logging.getLogger("test").info("hello %s", "world")
    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["level"] == "INFO"
    assert payload["logger"] == "test"
    assert payload["message"] == "hello world"
    assert "ts" in payload


def test_secrets_are_redacted_everywhere(store, capsys):
    SECRET_REGISTRY.add("s3cret-password")
    setup_logging(store)
    logging.getLogger("test").info("login failed for s3cret-password at imap.example.com")
    line = capsys.readouterr().out.strip().splitlines()[-1]
    assert "s3cret-password" not in line
    assert "[REDACTED]" in json.loads(line)["message"]


def test_ring_buffer_receives_lines(store):
    setup_logging(store)
    logging.getLogger("test").warning("watch out")
    lines = store.recent_log_lines(limit=10)
    assert any("watch out" in line for _, _, line in lines)


def test_ring_buffer_lines_are_redacted(store):
    SECRET_REGISTRY.add("hunter2-hunter2")
    setup_logging(store)
    logging.getLogger("test").error("auth error, password=hunter2-hunter2")
    lines = store.recent_log_lines(limit=10)
    assert all("hunter2-hunter2" not in line for _, _, line in lines)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_logging_setup.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `papra_suite/logging_setup.py`**

```python
import json
import logging
import sys
from datetime import UTC, datetime

REDACTED = "[REDACTED]"


class SecretRegistry:
    """Single shared registry of secret values; one filter consults it."""

    def __init__(self) -> None:
        self._secrets: set[str] = set()

    def add(self, value: str | None) -> None:
        if value and len(value) >= 4:
            self._secrets.add(value)

    def redact(self, text: str) -> str:
        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, REDACTED)
        return text


SECRET_REGISTRY = SecretRegistry()


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        record.msg = SECRET_REGISTRY.redact(message)
        record.args = None
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class RingBufferHandler(logging.Handler):
    def __init__(self, store) -> None:
        super().__init__()
        self._store = store

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._store.append_log_line(
                ts=datetime.now(UTC).isoformat(),
                level=record.levelname,
                line=self.format(record),
            )
        except Exception:  # noqa: BLE001 - logging must never crash the app
            self.handleError(record)


def setup_logging(store=None) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(JsonFormatter())
    stream.addFilter(RedactingFilter())
    root.addHandler(stream)

    if store is not None:
        ring = RingBufferHandler(store)
        ring.setFormatter(logging.Formatter("%(name)s: %(message)s"))
        ring.addFilter(RedactingFilter())
        root.addHandler(ring)
```

- [ ] **Step 4: Run tests and lint**

Run: `pytest tests/test_logging_setup.py -v`
Expected: all PASS

Run: `ruff check .`
Expected: clean

- [ ] **Step 5: Commit**

```bash
git add papra_suite/logging_setup.py tests/test_logging_setup.py
git commit -m "feat(m1): json-lines logging with shared secret redaction and db ring buffer"
```

---

### Task 4: Papra REST client with retry policy

**Files:**
- Create: `papra_suite/papra_client.py`
- Test: `tests/test_papra_client.py`

**Interfaces:**
- Consumes: nothing from Tasks 1–3 (takes base_url + api_key directly, so the MCP entrypoint can reuse it with different env later).
- Produces: `PapraClient` and `PapraError` used by pipeline, dashboard, and MCP in later milestones. Exact API surface (paths verified against docs.papra.app/resources/api-endpoints and the upstream repo on 2026-11-12):

```python
class PapraError(Exception):
    status: int | None          # None for network failures after retries

class PapraClient:
    def __init__(self, base_url: str, api_key: str, *, timeout_s: float = 30.0,
                 backoff_base_s: float = 0.5, transport: httpx.AsyncBaseTransport | None = None)
    async def create_document(self, org_id: str, filename: str, content: bytes) -> dict
    async def get_document(self, org_id: str, document_id: str) -> dict
    async def update_document(self, org_id: str, document_id: str, *,
                              name: str | None = None, content: str | None = None) -> dict
    async def list_documents(self, org_id: str, *, page_index: int = 0, page_size: int = 100,
                             search_query: str | None = None, sort_field: str = "createdAt",
                             sort_order: str = "desc") -> tuple[list[dict], int]
    async def download_file(self, org_id: str, document_id: str) -> bytes
    async def list_tags(self, org_id: str) -> list[dict]
    async def add_tag(self, org_id: str, document_id: str, tag_id: str) -> None
    async def whoami(self) -> dict                 # GET /api/api-keys/current (reachability check)
    async def aclose(self) -> None
```

Retry policy (spec §5.2): 3 attempts total, exponential backoff (`backoff_base_s * 4**attempt`), retry on all 5xx, 429, and network errors; any other 4xx raises `PapraError` immediately with the response body excerpt — no auto-retry.

- [ ] **Step 1: Write the failing client tests**

`tests/test_papra_client.py`:

```python
import httpx
import pytest

from papra_suite.papra_client import PapraClient, PapraError

ORG = "org_123"
DOC = {"id": "doc_1", "name": "bill.pdf", "mimeType": "application/pdf"}


def make_client(handler, **kwargs):
    return PapraClient(
        "http://papra.test", "ppapi_test", backoff_base_s=0.0,
        transport=httpx.MockTransport(handler), **kwargs,
    )


async def test_bearer_auth_header_is_sent():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("Authorization")
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        await client.get_document(ORG, "doc_1")
    assert seen["auth"] == "Bearer ppapi_test"


async def test_retries_5xx_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(500)
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        document = await client.get_document(ORG, "doc_1")
    assert document["id"] == "doc_1" and calls["n"] == 3


async def test_4xx_raises_immediately_without_retry():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404, json={"code": "document.not_found"})

    async with make_client(handler) as client:
        with pytest.raises(PapraError) as excinfo:
            await client.get_document(ORG, "doc_1")
    assert excinfo.value.status == 404
    assert calls["n"] == 1


async def test_429_is_retried():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429)
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        await client.get_document(ORG, "doc_1")
    assert calls["n"] == 2


async def test_network_error_retried_then_raises():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("connection refused")

    async with make_client(handler) as client:
        with pytest.raises(PapraError, match="connection refused"):
            await client.get_document(ORG, "doc_1")
    assert calls["n"] == 3


async def test_update_document_sends_only_provided_fields():
    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.read())
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        await client.update_document(ORG, "doc_1", name="New title")
        await client.update_document(ORG, "doc_1", content="# markdown")
    import json

    assert json.loads(bodies[0]) == {"name": "New title"}
    assert json.loads(bodies[1]) == {"content": "# markdown"}


async def test_list_documents_passes_search_query():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json={"documents": [DOC], "documentsCount": 1})

    async with make_client(handler) as client:
        documents, count = await client.list_documents(ORG, search_query="invoice")
    assert documents == [DOC] and count == 1
    assert seen["params"]["searchQuery"] == "invoice"


async def test_download_file_returns_bytes():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"%PDF-1.4 fake")

    async with make_client(handler) as client:
        content = await client.download_file(ORG, "doc_1")
    assert content == b"%PDF-1.4 fake"


async def test_add_tag_posts_tag_id():
    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.read())
        return httpx.Response(204)

    async with make_client(handler) as client:
        await client.add_tag(ORG, "doc_1", "tag_9")
    import json

    assert json.loads(bodies[0]) == {"tagId": "tag_9"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_papra_client.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `papra_suite/papra_client.py`**

```python
import asyncio

import httpx

MAX_ATTEMPTS = 3
RETRY_STATUS = {429, 500, 502, 503, 504}


class PapraError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class PapraClient:
    def __init__(self, base_url: str, api_key: str, *, timeout_s: float = 30.0,
                 backoff_base_s: float = 0.5,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout_s,
            transport=transport,
        )
        self._backoff_base_s = backoff_base_s

    async def __aenter__(self) -> "PapraClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        last_error = ""
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = await self._client.request(method, path, **kwargs)
            except httpx.TransportError as error:
                last_error = str(error)
                if attempt == MAX_ATTEMPTS - 1:
                    break
                await asyncio.sleep(self._backoff_base_s * 4**attempt)
                continue
            if response.status_code in RETRY_STATUS and attempt < MAX_ATTEMPTS - 1:
                await asyncio.sleep(self._backoff_base_s * 4**attempt)
                continue
            if response.is_success:
                return response
            raise PapraError(
                f"{method} {path} failed: {response.status_code} {response.text[:200]}",
                response.status_code,
            )
        raise PapraError(f"{method} {path} failed after {MAX_ATTEMPTS} attempts: {last_error}")

    async def create_document(self, org_id: str, filename: str, content: bytes) -> dict:
        response = await self._request(
            "POST", f"/api/organizations/{org_id}/documents",
            files={"file": (filename, content, "application/pdf")},
        )
        return response.json()["document"]

    async def get_document(self, org_id: str, document_id: str) -> dict:
        response = await self._request("GET", f"/api/organizations/{org_id}/documents/{document_id}")
        return response.json()["document"]

    async def update_document(self, org_id: str, document_id: str, *,
                              name: str | None = None, content: str | None = None) -> dict:
        body = {key: value for key, value in {"name": name, "content": content}.items()
                if value is not None}
        response = await self._request(
            "PATCH", f"/api/organizations/{org_id}/documents/{document_id}", json=body
        )
        return response.json()["document"]

    async def list_documents(self, org_id: str, *, page_index: int = 0, page_size: int = 100,
                             search_query: str | None = None, sort_field: str = "createdAt",
                             sort_order: str = "desc") -> tuple[list[dict], int]:
        params = {
            "pageIndex": page_index, "pageSize": page_size,
            "sortField": sort_field, "sortOrder": sort_order,
        }
        if search_query is not None:
            params["searchQuery"] = search_query
        response = await self._request(
            "GET", f"/api/organizations/{org_id}/documents", params=params
        )
        data = response.json()
        return data["documents"], data.get("documentsCount", len(data["documents"]))

    async def download_file(self, org_id: str, document_id: str) -> bytes:
        response = await self._request(
            "GET", f"/api/organizations/{org_id}/documents/{document_id}/file"
        )
        return response.content

    async def list_tags(self, org_id: str) -> list[dict]:
        response = await self._request("GET", f"/api/organizations/{org_id}/tags")
        return response.json()["tags"]

    async def add_tag(self, org_id: str, document_id: str, tag_id: str) -> None:
        await self._request(
            "POST", f"/api/organizations/{org_id}/documents/{document_id}/tags",
            json={"tagId": tag_id},
        )

    async def whoami(self) -> dict:
        response = await self._request("GET", "/api/api-keys/current")
        return response.json()["apiKey"]
```

- [ ] **Step 4: Run tests and lint**

Run: `pytest tests/test_papra_client.py -v`
Expected: all PASS

Run: `ruff check .`
Expected: clean

- [ ] **Step 5: Commit**

```bash
git add papra_suite/papra_client.py tests/test_papra_client.py
git commit -m "feat(m1): async papra rest client with retry policy"
```

---

### Task 5: Integration test against a scratch Papra container

**Files:**
- Create: `docker-compose.test.yml`, `tests/integration/conftest.py`, `tests/integration/test_papra_integration.py`
- Modify: `Makefile` (add `test` target)

**Interfaces:**
- Consumes: `PapraClient` from Task 4.
- Produces: the reusable test stack. `make test` brings up a scratch Papra, runs the full suite (unit + integration), tears it down. `tests/integration/conftest.py` exposes a session fixture `papra_stack` yielding `(base_url, org_id, api_key)` — later milestones' integration tests (mail poller, OCR pipeline) reuse it unchanged.

Seeding facts (verified against upstream source on 2026-11-12; re-verify live in Step 1 before encoding):
- Papra uses better-auth with email/password enabled: sign-up is `POST /api/auth/sign-up/email` with `{"email", "password", "name"}`, response sets a session cookie. Registration is enabled by default; email verification is off by default; the first user becomes admin.
- `POST /api/organizations` with the session cookie creates an org (`{"organization": {"id": ...}}`).
- `POST /api/api-keys` with the session cookie takes `{"name", "permissions": [...]}` where permissions is a flat array (e.g. `"documents:create"`); it returns `{"apiKey": ..., "token": ...}` — `token` is the `ppapi_...` key.

- [ ] **Step 1: Create the compose stack and bring it up**

`docker-compose.test.yml`:

```yaml
services:
  papra:
    image: papra/papra:latest
    environment:
      AUTH_SECRET: "papra-suite-test-secret-not-for-prod-0123456789"
      AUTH_IS_EMAIL_VERIFICATION_REQUIRED: "false"
      AUTH_IS_REGISTRATION_ENABLED: "true"
    ports:
      - "127.0.0.1:12210:1221"
```

Run: `docker compose -f docker-compose.test.yml up -d --wait`
Expected: healthy container. Then verify the sign-up route live before encoding it:

Run: `curl -s -i -X POST http://127.0.0.1:12210/api/auth/sign-up/email -H 'Content-Type: application/json' -d '{"email":"probe@seed.test","password":"probe-password-123","name":"Probe"}'`
Expected: 200/201 with a JSON body containing a user object and a `set-cookie` header. If the path differs, inspect the container's OpenAPI document (`curl http://127.0.0.1:12210/api-reference` or `/openapi.json` / `/doc`) and record the real path — do not guess (spec §5.2). Adjust the conftest in Step 2 accordingly.

- [ ] **Step 2: Write the seeding conftest**

`tests/integration/conftest.py`:

```python
import uuid

import httpx
import pytest

PAPRA_TEST_URL = "http://127.0.0.1:12210"
API_KEY_PERMISSIONS = [
    "organizations:read",
    "documents:create",
    "documents:read",
    "documents:update",
    "tags:create",
    "tags:read",
]


def _papra_is_up() -> bool:
    try:
        response = httpx.get(f"{PAPRA_TEST_URL}/api/api-keys/current", timeout=2.0)
        return response.status_code in (200, 401)
    except httpx.TransportError:
        return False


@pytest.fixture(scope="session")
def papra_stack():
    if not _papra_is_up():
        pytest.skip("scratch Papra not running; use `make test`")

    # unique per session so a reused container cannot collide
    suffix = uuid.uuid4().hex[:8]
    with httpx.Client(base_url=PAPRA_TEST_URL) as http:
        sign_up = http.post(
            "/api/auth/sign-up/email",
            json={
                "email": f"seed-{suffix}@papra-suite.test",
                "password": "seed-password-123456",
                "name": "Seed User",
            },
        )
        sign_up.raise_for_status()

        org = http.post("/api/organizations", json={"name": f"Test Org {suffix}"})
        org.raise_for_status()
        org_id = org.json()["organization"]["id"]

        api_key = http.post(
            "/api/api-keys",
            json={"name": f"suite-{suffix}", "permissions": API_KEY_PERMISSIONS},
        )
        api_key.raise_for_status()
        token = api_key.json()["token"]

    yield PAPRA_TEST_URL, org_id, token
```

- [ ] **Step 3: Write the failing integration test**

`tests/integration/test_papra_integration.py`:

```python
import pytest

from papra_suite.papra_client import PapraClient

pytestmark = pytest.mark.integration

SEARCH_PHRASE = "quokka-marmalade-protocol"  # occurs only in content, never in the filename


async def test_full_document_round_trip(papra_stack):
    base_url, org_id, api_key = papra_stack
    import fitz  # dev dep in M1; runtime dep from M3 on

    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((72, 72), "Rechnung 42")
    pdf_bytes = pdf.tobytes()
    pdf.close()

    async with PapraClient(base_url, api_key) as client:
        # upload
        document = await client.create_document(org_id, "rechnung-42.pdf", pdf_bytes)
        document_id = document["id"]
        assert document["name"] == "rechnung-42.pdf"

        # metadata
        fetched = await client.get_document(org_id, document_id)
        assert fetched["id"] == document_id

        # write back name + content (one endpoint serves both)
        updated = await client.update_document(
            org_id, document_id, name="Rechnung 42 – Titel", content=f"# Rechnung 42\n\n{SEARCH_PHRASE}"
        )
        assert updated["name"] == "Rechnung 42 – Titel"

        # search must find the content-only phrase
        documents, count = await client.list_documents(org_id, search_query=SEARCH_PHRASE)
        assert count >= 1
        assert document_id in {doc["id"] for doc in documents}

        # original file survives content write-back
        downloaded = await client.download_file(org_id, document_id)
        assert downloaded == pdf_bytes

        # tags: create via form-data endpoint, then attach
        import httpx

        async with httpx.AsyncClient(base_url=base_url) as http:
            tag = await http.post(
                f"/api/organizations/{org_id}/tags",
                data={"name": "suite-test", "color": "#000000"},
                headers={"Authorization": f"Bearer {api_key}"},
            )
            tag.raise_for_status()
            tag_id = tag.json()["tag"]["id"]

        tags = await client.list_tags(org_id)
        assert tag_id in {tag["id"] for tag in tags}
        await client.add_tag(org_id, document_id, tag_id)
```

- [ ] **Step 4: Run the integration test to verify it fails or reveals API drift**

Run: `docker compose -f docker-compose.test.yml up -d --wait && pytest tests/integration -v; docker compose -f docker-compose.test.yml down -v`
Expected: the test exercises the real API. If any endpoint shape differs from what Task 4 encoded (e.g. `documents` vs `document` keys, tag form-data fields), fix `papra_client.py` to match the live API — that is the point of this task (spec §5.2: encode what the instance actually exposes). Iterate until PASS.

- [ ] **Step 5: Add the one-command test target**

Append to `Makefile` (replace the `test-unit` block so both exist):

```make
test:
	docker compose -f docker-compose.test.yml up -d --wait
	pytest; status=$$?; docker compose -f docker-compose.test.yml down -v; exit $$status
```

Run: `make test`
Expected: full suite green (unit tests run too; integration tests run against the live container)

- [ ] **Step 6: Run lint and the full suite once more**

Run: `ruff check . && make test`
Expected: clean lint, all tests PASS

- [ ] **Step 7: Commit**

```bash
git add docker-compose.test.yml tests/integration/ Makefile papra_suite/papra_client.py
git commit -m "feat(m1): scratch papra compose stack, seeding fixture, integration round-trip"
```

---

## Self-Review (completed during planning)

**Spec coverage (M1 scope only):** scaffold (§4 tree, §12 deliverables) → Task 1; config validation at startup (§5, §6 + decisions §9 env additions) → Task 1; store §5.1 + decisions §6 job model + documents table (§4 of decisions) → Task 2; logging §12 (JSON lines, shared redaction filter, DB ring buffer) → Task 3; Papra client §5.2 (real endpoints, one retry policy) → Tasks 4–5; milestone 1 acceptance ("integration test against a scratch Papra container; open the running instance's OpenAPI document and encode the real paths") → Task 5 with the live-verify step. Mailboxes/orgs tables exist in the schema (Task 2) but their CRUD helpers and UI are M2+ scope — schema-first avoids a second migration later.

**Placeholders:** none; every code step contains the actual code.

**Type consistency:** `Store`/`Job` signatures in Task 2 match the interfaces block; `PapraClient` method signatures in Task 4 match Task 5's usage; `SECRET_REGISTRY.add`/`setup_logging(store)` names align between Task 3's interface block and implementation.

Later milestones (not this plan): M2 mail poller + upload pipeline; M3 OCR/title workers + sweep; M4 dashboard; M5 MCP; M6 Dockerfile/compose/README + acceptance run.
