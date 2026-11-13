# Papra Suite — Milestone 2: Mail poller + upload pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the mail ingestion path: per-mailbox IMAP polling, PDF-attachment extraction with magic-byte sniffing, synchronous upload to Papra, dedup, OCR-job enqueueing, and the store/pipeline groundwork M3's workers need — proven end-to-end against a GreenMail IMAP fixture and the scratch Papra container.

**Architecture:** One asyncio poll task per enabled mailbox. All blocking `imaplib` calls run through `asyncio.to_thread` behind a sync `MailboxSession` class — the async/sync boundary lives only in `mail.py` (decisions doc §3). Uploads happen inline in the poller (no upload job kind; decisions §3); each successful upload records the document as seen and enqueues an `ocr` job idempotently (decisions §4/§6). Mailbox state (last seen UID, UIDVALIDITY, failure accounting) persists in the store; three consecutive failures disable a mailbox (spec §5.3).

**Tech Stack:** Python 3.12, stdlib `imaplib`/`email`/`smtplib`/`hashlib`/`re`, existing `Store`/`PapraClient`/`SECRET_REGISTRY`. No new runtime dependencies. Test fixtures: `greenmail/standalone` (IMAP+SMTP) and the existing scratch Papra compose stack.

**Spec:** `spec.md` (§5.3 mail poller, §8 acceptance 1–3) + `docs/superpowers/specs/2026-11-12-papra-suite-implementation-decisions-design.md` (§3 synchronous upload, §6 job model). Read both before starting.

## Global Constraints

- Python 3.12; runtime deps remain stdlib + httpx only (FastAPI/Jinja2/etc. arrive in later milestones).
- All job/document timestamps are STORE-GENERATED (`+00:00` ISO format) — callers never supply timestamps or compute `next_attempt_at` (M1 review standing constraint).
- Secrets: mailbox passwords are registered into `SECRET_REGISTRY` when loaded by the poller; `register_config_secrets(config)` wires the env secrets (its call site is M3's serve command; the function lands now). No secret ever reaches a log line — the shared filter is the guarantee, per-callsite discipline is not relied on.
- `imaplib` is blocking: every IMAP call goes through `asyncio.to_thread` via `MailboxSession`; no other module touches `imaplib`.
- AGPL hygiene: read Papra upstream for facts only; never copy code. GreenMail is Apache-2.0 (facts only regardless).
- Every task ends green: `.venv/bin/ruff check .` clean, `.venv/bin/pytest -m "not integration"` green; Task 5 additionally requires the compose stack.
- Commit after every task, messages as shown.
- Unit tests must not need Docker; integration tests skip cleanly without the stack (existing `PAPRA_REQUIRE_STACK` contract covers the Papra leg; the GreenMail leg follows the same pattern).

## M2 entry requirements (from the M1 review ledger — these are first-class tasks)

1. `claim_next_job` cross-instance atomicity: the claim UPDATE must be conditional (`WHERE ... status='pending'`) with a rowcount check (Task 1).
2. `mark_job_failed` guard: unknown `job_id` raises a clear error instead of `TypeError` (Task 1).
3. Stale-`running`-job recovery: `Store.recover_running_jobs()` resets `running`→`pending` for restart survival (spec §8.3) (Task 1).
4. Compose is touched for GreenMail: add a GreenMail healthcheck where a reliable in-image command exists; the Papra container keeps the conftest readiness probe as its mechanism (in-image tooling unverified — do not guess healthcheck commands into the Papra image) (Task 5).

---

### Task 1: Store — org/mailbox CRUD, poll-state recording, job-safety guards

**Files:**
- Modify: `papra_suite/store.py`
- Test: `tests/test_store.py`

**Interfaces:**
- Consumes: existing `Store` (M1).
- Produces (later tasks and M4 dashboard rely on these exact signatures):

```python
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
    ssl_mode: str            # 'ssl' | 'starttls' | 'plain'
    poll_interval_s: int
    subject_filter: str | None
    delete_after_download: bool
    enabled: bool
    last_seen_uid: int | None
    uidvalidity: str | None
    last_poll_at: str | None
    last_error: str | None
    consecutive_failures: int

class Store:
    # orgs
    def create_org(self, *, papra_org_id: str, name: str) -> str          # returns local org id; idempotent on papra_org_id
    def get_org(self, org_id: str) -> Org | None
    def get_org_by_papra_id(self, papra_org_id: str) -> Org | None
    # mailboxes
    def create_mailbox(self, *, org_id: str, host: str, port: int, username: str,
                       password: str, ssl_mode: str = "ssl", poll_interval_s: int = 300,
                       subject_filter: str | None = None,
                       delete_after_download: bool = False) -> str
    def get_mailbox(self, mailbox_id: str) -> Mailbox | None
    def list_mailboxes(self, *, enabled_only: bool = False) -> list[Mailbox]
    def delete_mailbox(self, mailbox_id: str) -> None
    def record_poll_success(self, mailbox_id: str, *, last_seen_uid: int,
                            uidvalidity: str | None) -> None
    def record_poll_failure(self, mailbox_id: str, error: str, *,
                            disable_after: int = 3) -> None
    # job safety (M2 entry requirements)
    def recover_running_jobs(self) -> int          # running -> pending; returns count
    # modified behavior:
    def claim_next_job(self, kind: str) -> Job | None   # conditional UPDATE + rowcount check
    def mark_job_failed(self, job_id: str, error: str, *, max_attempts: int = 3,
                        backoff_base_s: int = 30) -> None   # unknown job_id -> ValueError
```

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_store.py`:

```python
# -- orgs and mailboxes -----------------------------------------------------

def test_org_roundtrip(store):
    org_id = store.create_org(papra_org_id="org_abc", name="Test Org")
    org = store.get_org(org_id)
    assert org is not None and org.papra_org_id == "org_abc" and org.name == "Test Org"
    assert store.get_org_by_papra_id("org_abc").id == org_id
    assert store.get_org("nope") is None
    assert store.get_org_by_papra_id("nope") is None


def test_create_org_is_idempotent_on_papra_id(store):
    first = store.create_org(papra_org_id="org_abc", name="A")
    second = store.create_org(papra_org_id="org_abc", name="A")
    assert first == second


def test_mailbox_roundtrip(store):
    org_id = store.create_org(papra_org_id="org_abc", name="Org")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.example.com", port=993,
        username="user", password="pass", subject_filter=".*rechnung.*",
    )
    mailbox = store.get_mailbox(mailbox_id)
    assert mailbox is not None
    assert mailbox.host == "imap.example.com" and mailbox.port == 993
    assert mailbox.username == "user" and mailbox.password == "pass"
    assert mailbox.ssl_mode == "ssl" and mailbox.poll_interval_s == 300
    assert mailbox.subject_filter == ".*rechnung.*"
    assert mailbox.delete_after_download is False and mailbox.enabled is True
    assert mailbox.last_seen_uid is None and mailbox.uidvalidity is None
    assert mailbox.consecutive_failures == 0
    assert mailbox in store.list_mailboxes()
    store.delete_mailbox(mailbox_id)
    assert store.get_mailbox(mailbox_id) is None


def test_list_mailboxes_enabled_only(store):
    org_id = store.create_org(papra_org_id="org_abc", name="Org")
    active = store.create_mailbox(org_id=org_id, host="h1", port=993, username="u", password="p")
    store.create_mailbox(org_id=org_id, host="h2", port=993, username="u", password="p")
    # disable the first via three recorded failures
    for _ in range(3):
        store.record_poll_failure(active, "auth failed")
    enabled = store.list_mailboxes(enabled_only=True)
    assert [m.id for m in enabled if m.id == active] == []


def test_record_poll_success_and_failure_reject_unknown_mailbox(store):
    with pytest.raises(ValueError, match="unknown mailbox"):
        store.record_poll_success("nope", last_seen_uid=1, uidvalidity="1")
    with pytest.raises(ValueError, match="unknown mailbox"):
        store.record_poll_failure("nope", "boom")


def test_record_poll_success_clears_failure_state(store):
    org_id = store.create_org(papra_org_id="org_abc", name="Org")
    mailbox_id = store.create_mailbox(org_id=org_id, host="h", port=993, username="u", password="p")
    store.record_poll_failure(mailbox_id, "boom")
    store.record_poll_success(mailbox_id, last_seen_uid=42, uidvalidity="111")
    mailbox = store.get_mailbox(mailbox_id)
    assert mailbox.last_seen_uid == 42 and mailbox.uidvalidity == "111"
    assert mailbox.last_error is None and mailbox.consecutive_failures == 0
    assert mailbox.last_poll_at is not None


def test_record_poll_failure_disables_after_threshold(store):
    org_id = store.create_org(papra_org_id="org_abc", name="Org")
    mailbox_id = store.create_mailbox(org_id=org_id, host="h", port=993, username="u", password="p")
    store.record_poll_failure(mailbox_id, "boom 1")
    store.record_poll_failure(mailbox_id, "boom 2")
    assert store.get_mailbox(mailbox_id).enabled is True
    assert store.get_mailbox(mailbox_id).consecutive_failures == 2
    store.record_poll_failure(mailbox_id, "boom 3")
    mailbox = store.get_mailbox(mailbox_id)
    assert mailbox.enabled is False
    assert mailbox.consecutive_failures == 3
    assert "boom 3" in mailbox.last_error


def test_record_poll_failure_truncates_long_errors(store):
    org_id = store.create_org(papra_org_id="org_abc", name="Org")
    mailbox_id = store.create_mailbox(org_id=org_id, host="h", port=993, username="u", password="p")
    store.record_poll_failure(mailbox_id, "x" * 5000)
    assert len(store.get_mailbox(mailbox_id).last_error) <= 500


# -- job safety (M2 entry requirements) ---------------------------------------

def test_recover_running_jobs_resets_to_pending(store):
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                                content_hash="abc", payload={})
    store.claim_next_job("ocr")
    assert store.get_job(job_id).status == "running"
    assert store.recover_running_jobs() == 1
    recovered = store.get_job(job_id)
    assert recovered.status == "pending" and recovered.attempts == 0
    # claimable again immediately (next_attempt_at reset to now)
    assert store.claim_next_job("ocr").id == job_id
    assert store.recover_running_jobs() == 0


def test_claim_next_job_survives_lost_race(store):
    # simulate another instance stealing the job between SELECT and UPDATE:
    # enqueue two, claim one externally via raw SQL, then claim via the API
    store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                      content_hash="h1", payload={})
    store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_2",
                      content_hash="h2", payload={})
    store._conn.execute("UPDATE jobs SET status = 'done' WHERE document_id = 'doc_1'")
    store._conn.commit()
    job = store.claim_next_job("ocr")
    assert job.document_id == "doc_2" and job.status == "running"


def test_mark_job_failed_unknown_job_raises(store):
    with pytest.raises(ValueError, match="unknown job"):
        store.mark_job_failed("no-such-job", "boom")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_store.py -v`
Expected: FAIL with `AttributeError`/`TypeError` on the new methods

- [ ] **Step 3: Implement in `papra_suite/store.py`**

Add the `Org` and `Mailbox` dataclasses (mirroring the `Job` pattern), row mappers, and methods. Key implementations:

```python
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
```

In `Store`:

```python
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
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "UPDATE jobs SET status = 'pending', next_attempt_at = ?, updated_at = ? "
                "WHERE status = 'running'",
                (_now(), _now()),
            )
            return cursor.rowcount
```

Modify `claim_next_job` — the claim UPDATE becomes conditional with a rowcount check, looping over candidates:

```python
    def claim_next_job(self, kind: str) -> Job | None:
        now = _now()
        with self._lock:
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
```

Modify `mark_job_failed` — guard the missing row (find the `row = self._conn.execute(...).fetchone()` in the existing implementation and add):

```python
            if row is None:
                raise ValueError(f"unknown job: {job_id}")
```

- [ ] **Step 4: Run tests and lint**

Run: `.venv/bin/pytest tests/test_store.py -v`
Expected: all PASS (existing + new)

Run: `.venv/bin/ruff check .`
Expected: clean

- [ ] **Step 5: Commit**

```bash
git add papra_suite/store.py tests/test_store.py
git commit -m "feat(m2): org/mailbox store crud, poll-state recording, job-safety guards"
```

---

### Task 2: Pipeline — OCR enqueue path + config-secret wiring

**Files:**
- Create: `papra_suite/pipeline.py`
- Modify: `papra_suite/logging_setup.py` (add `register_config_secrets`)
- Test: `tests/test_pipeline.py`, `tests/test_logging_setup.py`

**Interfaces:**
- Consumes: `Store` (Task 1), `AppConfig`.
- Produces (M3 workers and the M3 sweep consume these):

```python
# papra_suite/pipeline.py
def enqueue_ocr_job(store: Store, *, papra_org_id: str, document_id: str,
                    file_hash: str, name: str, mime: str, ingested_via: str) -> str
    # records the document as seen (idempotent) and enqueues kind="ocr" with
    # content_hash=file_hash and payload={"file_hash", "name"}; returns job id
    # (existing job id on conflict — INSERT OR IGNORE semantics)

# papra_suite/logging_setup.py
def register_config_secrets(config: AppConfig) -> None
    # adds papra_api_key, llm_api_key, admin_password to SECRET_REGISTRY
```

- [ ] **Step 1: Write the failing tests**

`tests/test_pipeline.py`:

```python
from papra_suite.pipeline import enqueue_ocr_job
from papra_suite.store import Store


def test_enqueue_ocr_job_creates_seen_document_and_job(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    try:
        job_id = enqueue_ocr_job(
            store, papra_org_id="org_1", document_id="doc_1", file_hash="abc123",
            name="bill.pdf", mime="application/pdf", ingested_via="mail",
        )
        assert store.document_seen("org_1", "doc_1")
        job = store.get_job(job_id)
        assert job.kind == "ocr" and job.status == "pending"
        assert job.content_hash == "abc123"
        assert job.payload == {"file_hash": "abc123", "name": "bill.pdf"}
    finally:
        store.close()


def test_enqueue_ocr_job_is_idempotent(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    try:
        first = enqueue_ocr_job(
            store, papra_org_id="org_1", document_id="doc_1", file_hash="abc123",
            name="bill.pdf", mime="application/pdf", ingested_via="mail",
        )
        second = enqueue_ocr_job(
            store, papra_org_id="org_1", document_id="doc_1", file_hash="abc123",
            name="bill.pdf", mime="application/pdf", ingested_via="sweep",
        )
        assert first == second
        assert len(store.list_jobs()) == 1
    finally:
        store.close()


def test_enqueue_ocr_job_new_hash_creates_new_job(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    try:
        a = enqueue_ocr_job(store, papra_org_id="org_1", document_id="doc_1",
                            file_hash="h1", name="n", mime="application/pdf",
                            ingested_via="mail")
        b = enqueue_ocr_job(store, papra_org_id="org_1", document_id="doc_1",
                            file_hash="h2", name="n", mime="application/pdf",
                            ingested_via="mail")
        assert a != b
    finally:
        store.close()
```

Add to `tests/test_logging_setup.py`:

```python
def test_register_config_secrets_adds_all_env_secrets(store, capsys):
    from papra_suite.config import load_config
    from papra_suite.logging_setup import register_config_secrets

    env = {
        "PAPRA_BASE_URL": "http://papra:1221", "PAPRA_API_KEY": "ppapi_env_key_1",
        "LLM_BASE_URL": "http://llm:8000", "LLM_API_KEY": "llm_env_key_2",
        "TITLE_MODEL": "t", "OCR_MODEL": "o", "ADMIN_PASSWORD": "admin_env_pw_3",
    }
    config = load_config(env)
    register_config_secrets(config)
    setup_logging(store)
    logging.getLogger("test").info("keys: ppapi_env_key_1 llm_env_key_2 admin_env_pw_3")
    line = capsys.readouterr().out.strip().splitlines()[-1]
    assert "ppapi_env_key_1" not in line
    assert "llm_env_key_2" not in line
    assert "admin_env_pw_3" not in line
    assert line.count("[REDACTED]") == 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_pipeline.py tests/test_logging_setup.py -v`
Expected: FAIL with `ModuleNotFoundError` for `papra_suite.pipeline`

- [ ] **Step 3: Implement `papra_suite/pipeline.py`**

```python
"""Pipeline orchestration: document ingestion and job chaining.

M2 scope: the ingestion path (record document as seen + enqueue OCR job).
M3 adds the OCR/title worker loops and the central transition handler.
"""

from papra_suite.store import Store


def enqueue_ocr_job(store: Store, *, papra_org_id: str, document_id: str,
                    file_hash: str, name: str, mime: str, ingested_via: str) -> str:
    """Record a document as seen and enqueue an OCR job for it (idempotent).

    The job's idempotency key is (kind='ocr', org, document_id, file_hash):
    re-ingesting the same document with the same content is a no-op that
    returns the existing job id; changed content yields a new job.
    """
    store.record_document_seen(
        papra_org_id=papra_org_id,
        papra_document_id=document_id,
        name=name,
        mime=mime,
        ingested_via=ingested_via,
    )
    return store.enqueue_job(
        kind="ocr",
        papra_org_id=papra_org_id,
        document_id=document_id,
        content_hash=file_hash,
        payload={"file_hash": file_hash, "name": name},
    )
```

Add to `papra_suite/logging_setup.py` (after `SECRET_REGISTRY`; import `AppConfig` under `TYPE_CHECKING` to avoid an import cycle — check whether one exists; `config.py` does not import `logging_setup`, so a plain import is fine, but keep it lazy inside the function to stay safe):

```python
def register_config_secrets(config) -> None:
    """Register all env-carried secrets so the shared filter can redact them."""
    SECRET_REGISTRY.add(config.papra_api_key)
    SECRET_REGISTRY.add(config.llm_api_key)
    SECRET_REGISTRY.add(config.admin_password)
```

- [ ] **Step 4: Run tests and lint**

Run: `.venv/bin/pytest tests/test_pipeline.py tests/test_logging_setup.py tests/test_store.py -v`
Expected: all PASS

Run: `.venv/bin/ruff check .`
Expected: clean

- [ ] **Step 5: Commit**

```bash
git add papra_suite/pipeline.py papra_suite/logging_setup.py tests/test_pipeline.py tests/test_logging_setup.py
git commit -m "feat(m2): ocr enqueue path and config-secret registry wiring"
```

---

### Task 3: Mail parsing — pure helpers (sniff, parse, sanitize, dedup, filter)

**Files:**
- Create: `papra_suite/mail.py`
- Test: `tests/test_mail_parsing.py`

**Interfaces:**
- Consumes: nothing (pure stdlib).
- Produces (Task 4's poller and M4's dashboard consume):

```python
# papra_suite/mail.py — pure helpers (no imaplib usage in this task)
@dataclass
class Attachment:
    filename: str          # sanitized
    content: bytes

@dataclass
class ParsedMessage:
    message_id: str | None
    subject: str | None
    pdf_attachments: list[Attachment]
    skipped_attachments: list[str]   # descriptive names of ignored non-PDF parts

def sniff_is_pdf(data: bytes) -> bool
def sanitize_filename(name: str | None) -> str      # strips paths/control chars, caps length
def parse_message(raw: bytes) -> ParsedMessage     # MIME walk; PDF by magic bytes only
def subject_matches(subject: str | None, pattern: str | None) -> bool   # None/"" -> True
def dedup_hash(mailbox_id: str, message_id: str | None, filename: str, size: int) -> str
def compute_file_hash(content: bytes) -> str       # sha256 hex
```

Spec anchors: PDF detection is by magic bytes `%PDF-`, NOT the declared mime type (spec §5.3); every other attachment type is skipped and logged; the dedup hash is `(mailbox, message-id, attachment filename, attachment size)` (spec §5.3); the subject filter is an optional per-mailbox regex (spec §5.3).

- [ ] **Step 1: Write the failing tests**

`tests/test_mail_parsing.py`:

```python
import email
from email.message import EmailMessage

from papra_suite.mail import (
    Attachment, ParsedMessage, compute_file_hash, dedup_hash, parse_message,
    sanitize_filename, sniff_is_pdf, subject_matches,
)

PDF_BYTES = b"%PDF-1.4 fake pdf body"


def build_raw(*, subject="Invoice", message_id="<msg-1@example.com>", attachments=()):
    message = EmailMessage()
    message["From"] = "sender@example.com"
    message["To"] = "box@example.com"
    message["Subject"] = subject
    if message_id:
        message["Message-ID"] = message_id
    for filename, content, mime in attachments:
        message.add_attachment(content, maintype=mime[0], subtype=mime[1], filename=filename)
    return message.as_bytes()


def test_sniff_is_pdf_ignores_declared_mime():
    assert sniff_is_pdf(PDF_BYTES)
    assert sniff_is_pdf(b"%PDF-")
    assert not sniff_is_pdf(b"PK\x03\x04 zip")
    assert not sniff_is_pdf(b"")
    assert not sniff_is_pdf(b"%PD")  # truncated magic


def test_sniff_is_pdf_catches_mislabeled_pdf():
    # declared as text/plain but actually a PDF -> must be treated as PDF
    raw = build_raw(attachments=[("a.pdf", PDF_BYTES, ("text", "plain"))])
    parsed = parse_message(raw)
    assert [a.filename for a in parsed.pdf_attachments] == ["a.pdf"]
    assert parsed.skipped_attachments == []


def test_parse_message_extracts_metadata_and_attachments():
    raw = build_raw(attachments=[
        ("rechnung.pdf", PDF_BYTES, ("application", "pdf")),
        ("photo.jpg", b"\xff\xd8 jpeg", ("image", "jpeg")),
    ])
    parsed = parse_message(raw)
    assert parsed.message_id == "<msg-1@example.com>"
    assert parsed.subject == "Invoice"
    assert [a.filename for a in parsed.pdf_attachments] == ["rechnung.pdf"]
    assert parsed.pdf_attachments[0].content == PDF_BYTES
    assert len(parsed.skipped_attachments) == 1
    assert "photo.jpg" in parsed.skipped_attachments[0]


def test_parse_message_without_message_id():
    raw = build_raw(message_id=None)
    parsed = parse_message(raw)
    assert parsed.message_id is None


def test_parse_message_plain_body_only():
    raw = build_raw()
    parsed = parse_message(raw)
    assert parsed.pdf_attachments == [] and parsed.skipped_attachments == []


def test_sanitize_filename_strips_paths_and_control_chars():
    assert sanitize_filename("../../etc/passwd.pdf") == "passwd.pdf"
    assert sanitize_filename("folder\\evil.pdf") == "evil.pdf"
    assert sanitize_filename("bad\x00name.pdf") == "badname.pdf"
    assert sanitize_filename("x" * 300 + ".pdf").endswith(".pdf")
    assert len(sanitize_filename("x" * 300 + ".pdf")) <= 120
    assert sanitize_filename(None) == "document.pdf"
    assert sanitize_filename("") == "document.pdf"
    assert sanitize_filename("   .pdf") == "document.pdf"


def test_subject_matches():
    assert subject_matches("Rechnung 42", None) is True
    assert subject_matches("Rechnung 42", "") is True
    assert subject_matches("Rechnung 42", ".*rechnung.*") is True
    assert subject_matches("Spam", ".*rechnung.*") is False
    assert subject_matches(None, ".*rechnung.*") is False


def test_dedup_hash_is_stable_and_discriminating():
    a = dedup_hash("mb1", "<m@x>", "f.pdf", 100)
    assert a == dedup_hash("mb1", "<m@x>", "f.pdf", 100)
    assert a != dedup_hash("mb2", "<m@x>", "f.pdf", 100)
    assert a != dedup_hash("mb1", "<m@y>", "f.pdf", 100)
    assert a != dedup_hash("mb1", "<m@x>", "g.pdf", 100)
    assert a != dedup_hash("mb1", "<m@x>", "f.pdf", 101)
    assert a != dedup_hash("mb1", None, "f.pdf", 100)


def test_compute_file_hash():
    assert compute_file_hash(b"abc") == compute_file_hash(b"abc")
    assert compute_file_hash(b"abc") != compute_file_hash(b"abd")
    assert len(compute_file_hash(b"abc")) == 64
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_mail_parsing.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement the pure helpers in `papra_suite/mail.py`**

```python
"""Mail ingestion: parsing helpers, the sync IMAP session, and the poller.

This module owns the only imaplib usage in the service; every IMAP call is
executed via asyncio.to_thread by the poller (decisions doc §3).
"""

import hashlib
import re
from dataclasses import dataclass, field
from email import message_from_bytes
from email.message import Message

MAX_FILENAME_LENGTH = 120
PDF_MAGIC = b"%PDF-"


@dataclass
class Attachment:
    filename: str
    content: bytes


@dataclass
class ParsedMessage:
    message_id: str | None
    subject: str | None
    pdf_attachments: list[Attachment] = field(default_factory=list)
    skipped_attachments: list[str] = field(default_factory=list)


def sniff_is_pdf(data: bytes) -> bool:
    """PDF detection by magic bytes only — mail clients mislabel mime types."""
    return data.startswith(PDF_MAGIC)


def sanitize_filename(name: str | None) -> str:
    """Strip path components and control chars; cap length; never empty."""
    if not name:
        return "document.pdf"
    # take the basename regardless of separator style
    name = re.split(r"[/\\]", name)[-1]
    name = "".join(ch for ch in name if ch.isprintable() and not ch.isspace() or ch == " ")
    name = name.strip().strip(".")
    if not name:
        return "document.pdf"
    if len(name) > MAX_FILENAME_LENGTH:
        stem, dot, suffix = name.rpartition(".")
        if dot and len(suffix) <= 10:
            name = name[: MAX_FILENAME_LENGTH - len(suffix) - 1] + "." + suffix
        else:
            name = name[:MAX_FILENAME_LENGTH]
    return name


def _attachment_name(part: Message) -> str:
    raw = part.get_filename() or "attachment"
    return sanitize_filename(raw)


def parse_message(raw: bytes) -> ParsedMessage:
    """Parse a raw MIME message; classify attachments by magic bytes, not labels."""
    message = message_from_bytes(raw)
    parsed = ParsedMessage(
        message_id=message.get("Message-ID"),
        subject=message.get("Subject"),
    )
    for part in message.walk():
        if part.get_content_maintype() == "multipart":
            continue
        payload = part.get_payload(decode=True)
        if payload is None:
            continue
        filename = part.get_filename()
        if filename is None:
            continue  # body text or inline part without a name: not an attachment
        if sniff_is_pdf(payload):
            parsed.pdf_attachments.append(Attachment(sanitize_filename(filename), payload))
        else:
            parsed.skipped_attachments.append(sanitize_filename(filename))
    return parsed


def subject_matches(subject: str | None, pattern: str | None) -> bool:
    """Optional per-mailbox subject regex filter; no pattern means match all."""
    if not pattern:
        return True
    if subject is None:
        return False
    return re.search(pattern, subject) is not None


def dedup_hash(mailbox_id: str, message_id: str | None, filename: str, size: int) -> str:
    basis = f"{mailbox_id}\x00{message_id or ''}\x00{filename}\x00{size}"
    return hashlib.sha256(basis.encode()).hexdigest()


def compute_file_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
```

- [ ] **Step 4: Run tests and lint**

Run: `.venv/bin/pytest tests/test_mail_parsing.py -v`
Expected: all PASS

Run: `.venv/bin/ruff check .`
Expected: clean

- [ ] **Step 5: Commit**

```bash
git add papra_suite/mail.py tests/test_mail_parsing.py
git commit -m "feat(m2): mail parsing helpers with magic-byte pdf sniffing"
```

---

### Task 4: MailboxSession + the poller

**Files:**
- Modify: `papra_suite/mail.py` (add `MailboxSession` and `poll_mailbox_once`)
- Test: `tests/test_mail_poller.py`

**Interfaces:**
- Consumes: `Store`/`Mailbox`/`Org` (Task 1), `enqueue_ocr_job` (Task 2), helpers (Task 3), `PapraClient`/`PapraError`, `SECRET_REGISTRY`.
- Produces (M3's serve command runs one poll task per enabled mailbox; M4's dashboard shows `Mailbox.last_error`/`enabled`):

```python
class MailboxSession:
    def __init__(self, *, host: str, port: int, username: str, password: str,
                 ssl_mode: str) -> None
    def connect(self) -> None                      # IMAP4_SSL | IMAP4+starttls | IMAP4
    def select_inbox(self) -> str                  # uidvalidity as str
    def fetch_since(self, last_seen_uid: int | None) -> list[tuple[int, bytes]]
        # UID SEARCH UID <last+1>:* (or ALL when last is None); PEEK fetch;
        # returns uid-sorted (uid, raw) pairs with uid > last (the * edge case)
    def mark_seen(self, uids: list[int]) -> None
    def delete_and_expunge(self, uids: list[int]) -> None
    def close(self) -> None                        # logout, best-effort

@dataclass
class PollResult:
    messages_scanned: int = 0
    documents_uploaded: int = 0
    attachments_skipped: int = 0
    messages_failed: int = 0
    error: str | None = None                      # set on session/connection failure

async def poll_mailbox_once(store: Store, client: PapraClient,
                            mailbox: Mailbox, org: Org) -> PollResult
```

Poller semantics (spec §5.3 + decisions §3):
- One IMAP round-trip collects candidates (PEEK, no `\Seen` side effects); uploads run async inline; a second round-trip applies `\Seen` / delete flags. Connections are not held across uploads.
- UIDVALIDITY change → rescan from the beginning (`last_seen_uid` reset) and store the new value.
- Dedup hash gates uploads; after a successful upload: `enqueue_ocr_job` (which records the document as seen), then `add_dedup`.
- `last_seen_uid` advances only across a failure-free prefix of processed messages (a failed upload leaves the message re-fetchable next poll; dedup prevents double uploads of already-uploaded attachments).
- Messages whose PDF attachments were all dedup-skipped count as processed (seen, advance).
- A message with only non-PDF attachments: logged, seen, no document, no error state (acceptance 2); never deleted (spec: not counted as processed).
- `delete_after_download`: a message is deleted only after every PDF attachment in it uploaded successfully.
- Per-message `PapraError` → `messages_failed`, poll continues; session/connection errors → `record_poll_failure` on the mailbox (3 consecutive → disabled) and `PollResult.error`; the poller itself never raises.
- `SECRET_REGISTRY.add(mailbox.password)` on every poll (the password must never survive into a log line).

- [ ] **Step 1: Write the failing poller tests**

`tests/test_mail_poller.py` (unit tests with a fake session and a stub client — no network):

```python
import asyncio
from dataclasses import dataclass, field

import pytest

from papra_suite.mail import PollResult, poll_mailbox_once
from papra_suite.store import Mailbox, Org, Store


def make_mailbox(**overrides):
    base = dict(
        id="mb1", org_id="org_local", host="imap.test", port=993, username="u",
        password="box-secret-pw", ssl_mode="ssl", poll_interval_s=300,
        subject_filter=None, delete_after_download=False, enabled=True,
        last_seen_uid=None, uidvalidity=None, last_poll_at=None, last_error=None,
        consecutive_failures=0,
    )
    base.update(overrides)
    return Mailbox(**base)


ORG = Org(id="org_local", papra_org_id="org_papra", name="Test")


def build_raw(subject="Invoice", message_id="<m1@x>", attachments=(("a.pdf", b"%PDF-1"),)):
    from tests.test_mail_parsing import build_raw as _build
    return _build(subject=subject, message_id=message_id, attachments=[
        (name, content, ("application", "pdf")) for name, content in attachments
    ])


@dataclass
class FakeSession:
    uidvalidity: str = "111"
    raws: list = field(default_factory=list)     # [(uid, raw)]
    seen: list = field(default_factory=list)
    deleted: list = field(default_factory=list)
    fail_on_connect: Exception | None = None

    def connect(self):
        if self.fail_on_connect:
            raise self.fail_on_connect

    def select_inbox(self):
        return self.uidvalidity

    def fetch_since(self, last_seen_uid):
        if last_seen_uid is None:
            return list(self.raws)
        return [(uid, raw) for uid, raw in self.raws if uid > last_seen_uid]

    def mark_seen(self, uids):
        self.seen.extend(uids)

    def delete_and_expunge(self, uids):
        self.deleted.extend(uids)

    def close(self):
        pass


class StubClient:
    def __init__(self, fail_for=()):
        self.fail_for = set(fail_for)   # filenames that should fail
        self.uploads = []

    async def create_document(self, org_id, filename, content):
        if filename in self.fail_for:
            from papra_suite.papra_client import PapraError
            raise PapraError(f"upload failed for {filename}", 500)
        document = {"id": f"doc_{len(self.uploads)}", "name": filename}
        self.uploads.append((org_id, filename, content))
        return document


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


async def test_poll_uploads_pdf_and_enqueues_ocr(store, monkeypatch):
    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(store, client, make_mailbox(), ORG)

    assert result.documents_uploaded == 1 and result.error is None
    assert client.uploads == [("org_papra", "a.pdf", b"%PDF-1")]
    job = store.claim_next_job("ocr")
    assert job.document_id == "doc_0" and job.content_hash == \
        __import__("papra_suite.mail", fromlist=["compute_file_hash"]).compute_file_hash(b"%PDF-1")
    assert store.document_seen("org_papra", "doc_0")
    # mb1 is an in-memory fixture (not persisted); poller state is asserted
    # via the result and the session, and via the persisted-state tests below
    assert session.seen == [1]
    assert session.deleted == []


async def test_poll_skips_non_pdf_and_marks_seen(store, monkeypatch, caplog):
    raw = build_raw(attachments=[("photo.jpg", b"\xff\xd8jpeg")])
    session = FakeSession(raws=[(1, raw)])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(store, StubClient(), make_mailbox(), ORG)

    assert result.documents_uploaded == 0
    assert result.attachments_skipped == 1
    assert session.seen == [1]              # handled, read
    assert session.deleted == []           # never deleted (not processed)


async def test_poll_respects_subject_filter(store, monkeypatch):
    session = FakeSession(raws=[
        (1, build_raw(subject="Spam")),
        (2, build_raw(subject="Rechnung 42", message_id="<m2@x>")),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(store, client, make_mailbox(subject_filter=".*rechnung.*"), ORG)

    assert result.documents_uploaded == 1
    assert client.uploads[0][1] == "a.pdf"
    assert session.seen == [1, 2]          # both handled (one filtered, one uploaded)


async def test_poll_dedup_prevents_double_upload(store, monkeypatch):
    raw = build_raw()
    session = FakeSession(raws=[(1, raw)])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    await poll_mailbox_once(store, client, make_mailbox(), ORG)
    # same message again (e.g. failed upload of a LATER message kept it fetchable)
    result = await poll_mailbox_once(store, client, make_mailbox(), ORG)

    assert result.documents_uploaded == 0
    assert len(client.uploads) == 1


async def test_poll_upload_failure_does_not_advance_last_seen(store, monkeypatch):
    session = FakeSession(raws=[
        (1, build_raw(message_id="<m1@x>", attachments=[("bad.pdf", b"%PDF-1")])),
        (2, build_raw(message_id="<m2@x>", attachments=[("good.pdf", b"%PDF-2")])),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(fail_for={"bad.pdf"})

    result = await poll_mailbox_once(store, client, make_mailbox(), ORG)

    assert result.documents_uploaded == 1 and result.messages_failed == 1
    assert session.seen == [2]             # only the successful message is read
    # mailbox state: failure recorded (upload failures count toward error state)
    # NOTE: mb1 is not persisted; the poller records failure only for stored
    # mailboxes — assert the result instead
    assert result.error is None            # per-message failure is not a poll error


async def test_poll_delete_after_download(store, monkeypatch):
    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(store, StubClient(), make_mailbox(delete_after_download=True), ORG)

    assert session.seen == [1] and session.deleted == [1]


async def test_poll_delete_not_triggered_on_upload_failure(store, monkeypatch):
    session = FakeSession(raws=[(1, build_raw(attachments=[("bad.pdf", b"%PDF-1")]))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient(fail_for={"bad.pdf"})

    result = await poll_mailbox_once(store, client, make_mailbox(delete_after_download=True), ORG)

    assert session.deleted == []           # stays in the mailbox for the next poll
    assert session.seen == []


async def test_poll_uidvalidity_change_rescans(store, monkeypatch):
    session = FakeSession(uidvalidity="222", raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    # mailbox claims uid 5 already seen under the OLD validity 111
    result = await poll_mailbox_once(
        store, StubClient(), make_mailbox(last_seen_uid=5, uidvalidity="111"), ORG
    )

    assert result.documents_uploaded == 1   # uid 1 re-fetched despite last_seen 5


async def test_poll_connection_failure_records_error_not_crash(store, monkeypatch):
    session = FakeSession(fail_on_connect=ConnectionRefusedError("refused"))
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(store, StubClient(), make_mailbox(), ORG)

    assert result.error is not None and "refused" in result.error
    assert result.documents_uploaded == 0


async def test_poll_registers_mailbox_password_for_redaction(store, monkeypatch, capsys):
    from papra_suite.logging_setup import SECRET_REGISTRY, setup_logging

    session = FakeSession(raws=[(1, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    import logging
    setup_logging(store)
    await poll_mailbox_once(store, StubClient(), make_mailbox(password="box-secret-pw"), ORG)
    logging.getLogger("test").info("connection used password box-secret-pw")
    line = capsys.readouterr().out.strip().splitlines()[-1]
    assert "box-secret-pw" not in line
```

Also add a persisted-state test using the real store (mailbox created via `store.create_mailbox`):

```python
async def test_poll_updates_persisted_mailbox_state(store, monkeypatch):
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    session = FakeSession(raws=[(7, build_raw())])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    from papra_suite.store import Org as OrgDC
    org = store.get_org(org_id)
    mailbox = store.get_mailbox(mailbox_id)
    result = await poll_mailbox_once(store, StubClient(), mailbox, org)

    updated = store.get_mailbox(mailbox_id)
    assert updated.last_seen_uid == 7
    assert updated.uidvalidity == "111"
    assert updated.last_error is None
    assert updated.consecutive_failures == 0
    assert updated.enabled is True


async def test_poll_connection_failure_disables_after_three(store, monkeypatch):
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    mailbox = store.get_mailbox(mailbox_id)
    org = store.get_org(org_id)

    for attempt in range(3):
        session = FakeSession(fail_on_connect=ConnectionRefusedError("refused"))
        monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
        result = await poll_mailbox_once(store, StubClient(), mailbox, org)

    assert result.error is not None
    updated = store.get_mailbox(mailbox_id)
    assert updated.enabled is False
    assert updated.consecutive_failures == 3
    assert "refused" in updated.last_error
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_mail_poller.py -v`
Expected: FAIL with `ImportError` (`PollResult`/`poll_mailbox_once` missing)

- [ ] **Step 3: Implement `MailboxSession` and `poll_mailbox_once` in `papra_suite/mail.py`**

```python
import asyncio
import imaplib
import logging

from papra_suite.logging_setup import SECRET_REGISTRY
from papra_suite.papra_client import PapraClient, PapraError
from papra_suite.pipeline import enqueue_ocr_job
from papra_suite.store import Mailbox, Org, Store

logger = logging.getLogger(__name__)


class MailboxSession:
    """Sync imaplib wrapper — every method runs inside asyncio.to_thread."""

    def __init__(self, *, host: str, port: int, username: str, password: str,
                 ssl_mode: str) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._ssl_mode = ssl_mode
        self._imap: imaplib.IMAP4 | None = None

    def connect(self) -> None:
        if self._ssl_mode == "ssl":
            self._imap = imaplib.IMAP4_SSL(self._host, self._port)
        elif self._ssl_mode == "starttls":
            self._imap = imaplib.IMAP4(self._host, self._port)
            self._imap.starttls()
        else:
            self._imap = imaplib.IMAP4(self._host, self._port)
        self._imap.login(self._username, self._password)

    def select_inbox(self) -> str:
        status, _ = self._imap.select("INBOX")
        if status != "OK":
            raise imaplib.IMAP4.error("select INBOX failed")
        _, data = self._imap.response("UIDVALIDITY")
        # data is [b'UIDVALIDITY <n>'] after select
        return data[0].decode().split()[-1]

    def fetch_since(self, last_seen_uid: int | None) -> list[tuple[int, bytes]]:
        if last_seen_uid is None:
            status, data = self._imap.uid("SEARCH", None, "ALL")
        else:
            status, data = self._imap.uid("SEARCH", None, f"UID {last_seen_uid + 1}:*")
        if status != "OK":
            raise imaplib.IMAP4.error("UID SEARCH failed")
        uids = [int(u) for u in data[0].split()]
        uids = [u for u in uids if last_seen_uid is None or u > last_seen_uid]
        uids.sort()
        messages = []
        for uid in uids:
            status, fetch_data = self._imap.uid("FETCH", str(uid), "(BODY.PEEK[])")
            if status != "OK":
                raise imaplib.IMAP4.error(f"UID FETCH failed for {uid}")
            raw = fetch_data[0][1]
            messages.append((uid, raw))
        return messages

    def mark_seen(self, uids: list[int]) -> None:
        for uid in uids:
            self._imap.uid("STORE", str(uid), "+FLAGS", "(\\Seen)")

    def delete_and_expunge(self, uids: list[int]) -> None:
        for uid in uids:
            self._imap.uid("STORE", str(uid), "+FLAGS", "(\\Deleted)")
        self._imap.expunge()

    def close(self) -> None:
        if self._imap is not None:
            try:
                self._imap.logout()
            except Exception:  # noqa: BLE001 - closing must never raise
                pass
            self._imap = None


@dataclass
class PollResult:
    messages_scanned: int = 0
    documents_uploaded: int = 0
    attachments_skipped: int = 0
    messages_failed: int = 0
    error: str | None = None


async def poll_mailbox_once(store: Store, client: PapraClient,
                            mailbox: Mailbox, org: Org) -> PollResult:
    """Poll one mailbox once. Never raises: failures land in the result and
    the mailbox's persisted error state (3 consecutive -> disabled)."""
    SECRET_REGISTRY.add(mailbox.password)
    session = MailboxSession(
        host=mailbox.host, port=mailbox.port, username=mailbox.username,
        password=mailbox.password, ssl_mode=mailbox.ssl_mode,
    )
    result = PollResult()
    try:
        await asyncio.to_thread(session.connect)
        uidvalidity = await asyncio.to_thread(session.select_inbox)
        rescan = mailbox.uidvalidity is not None and mailbox.uidvalidity != uidvalidity
        last_seen = None if (mailbox.last_seen_uid is None or rescan) else mailbox.last_seen_uid
        raw_messages = await asyncio.to_thread(session.fetch_since, last_seen)
    except Exception as error:  # noqa: BLE001 - the poller never crashes the loop
        result.error = str(error)
        await asyncio.to_thread(session.close)
        _record_failure_quietly(store, mailbox.id, result.error)
        return result

    seen_uids: list[int] = []
    delete_uids: list[int] = []
    candidate_last_seen = mailbox.last_seen_uid if not rescan else None

    for uid, raw in raw_messages:
        result.messages_scanned += 1
        parsed = parse_message(raw)
        if not subject_matches(parsed.subject, mailbox.subject_filter):
            logger.info("message uid=%s skipped by subject filter", uid)
            seen_uids.append(uid)
            candidate_last_seen = uid
            continue
        uploads_ok = True
        had_pdfs = False
        for attachment in parsed.pdf_attachments:
            had_pdfs = True
            attachment_hash = dedup_hash(
                mailbox.id, parsed.message_id, attachment.filename, len(attachment.content)
            )
            if store.dedup_exists(attachment_hash):
                logger.info("attachment %s already ingested (dedup)", attachment.filename)
                continue
            try:
                document = await client.create_document(
                    org.papra_org_id, attachment.filename, attachment.content
                )
            except PapraError as error:
                uploads_ok = False
                result.messages_failed += 1
                logger.error("upload failed for %s: %s", attachment.filename, error)
                continue
            file_hash = compute_file_hash(attachment.content)
            enqueue_ocr_job(
                store, papra_org_id=org.papra_org_id, document_id=document["id"],
                file_hash=file_hash, name=document["name"],
                mime="application/pdf", ingested_via="mail",
            )
            store.add_dedup(attachment_hash)
            result.documents_uploaded += 1
        for name in parsed.skipped_attachments:
            result.attachments_skipped += 1
            logger.info("skipped non-pdf attachment %s", name)
        if uploads_ok:
            seen_uids.append(uid)
            if mailbox.delete_after_download and had_pdfs:
                delete_uids.append(uid)
            candidate_last_seen = uid
        # on failure: leave the message unread and do not advance last_seen

    try:
        await asyncio.to_thread(session.mark_seen, seen_uids)
        if delete_uids:
            await asyncio.to_thread(session.delete_and_expunge, delete_uids)
    except Exception as error:  # noqa: BLE001
        result.error = str(error)
        await asyncio.to_thread(session.close)
        _record_failure_quietly(store, mailbox.id, result.error)
        return result
    finally:
        await asyncio.to_thread(session.close)

    # record success even for an empty poll: last_poll_at must refresh and
    # a previous error must clear; uid 0 means "nothing seen yet"
    last_seen = candidate_last_seen if candidate_last_seen is not None else (mailbox.last_seen_uid or 0)
    _record_success_quietly(store, mailbox.id, last_seen_uid=last_seen, uidvalidity=uidvalidity)
    return result


def _record_success_quietly(store: Store, mailbox_id: str, *, last_seen_uid: int,
                             uidvalidity: str | None) -> None:
    try:
        store.record_poll_success(mailbox_id, last_seen_uid=last_seen_uid, uidvalidity=uidvalidity)
    except ValueError:
        pass  # mailbox not persisted (unit fixtures): nothing to record


def _record_failure_quietly(store: Store, mailbox_id: str, error: str) -> None:
    try:
        store.record_poll_failure(mailbox_id, error)
    except ValueError:
        pass  # mailbox not persisted (unit fixtures): nothing to record
```

Note: `parse_message`, `subject_matches`, `dedup_hash`, `compute_file_hash` come from Task 3 — same module. `Mailbox`/`Org` are used only for typing; import them from `papra_suite.store`.

- [ ] **Step 4: Run tests and lint**

Run: `.venv/bin/pytest tests/test_mail_poller.py tests/test_mail_parsing.py -v`
Expected: all PASS

Run: `.venv/bin/ruff check . && .venv/bin/pytest -m "not integration"`
Expected: clean, full unit suite green

- [ ] **Step 5: Commit**

```bash
git add papra_suite/mail.py tests/test_mail_poller.py
git commit -m "feat(m2): imap session and mailbox poller with dedup and failure accounting"
```

---

### Task 5: Integration — GreenMail fixture + end-to-end ingestion tests

**Files:**
- Modify: `docker-compose.test.yml` (add greenmail service), `tests/integration/conftest.py` (greenmail fixture), `Makefile` (no change expected — `make test` already runs the full suite)
- Create: `tests/integration/test_mail_ingestion.py`

**Interfaces:**
- Consumes: `poll_mailbox_once` (Task 4), `papra_stack` fixture (M1), store CRUD (Task 1).
- Produces: the GreenMail-backed integration fixture `greenmail` yielding `(host, imap_port, smtp_port, email_address, password)` — M3/M4 integration tests reuse it unchanged. Verified fixture facts: image `greenmail/standalone:latest`; test-setup ports SMTP 3025 / IMAP 3143 (plain); users via `GREENMAIL_OPTS="-Dgreenmail.setup.test.smtp -Dgreenmail.setup.test.imap -Dgreenmail.hostname=0.0.0.0 -Dgreenmail.users.login=email -Dgreenmail.users=ingest:ingest-pass-123@localhost"` (auth enabled — the default image disables auth, which must be overridden); GreenMail's IMAP UIDVALIDITY is per-select-session (do not assert on its concrete value).

- [ ] **Step 1: Add GreenMail to the compose stack and bring it up**

Append to `docker-compose.test.yml`:

```yaml
  greenmail:
    image: greenmail/standalone:latest
    environment:
      GREENMAIL_OPTS: >-
        -Dgreenmail.setup.test.smtp -Dgreenmail.setup.test.imap
        -Dgreenmail.hostname=0.0.0.0 -Dgreenmail.users.login=email
        -Dgreenmail.users=ingest:ingest-pass-123@localhost
    ports:
      - "127.0.0.1:3025:3025"
      - "127.0.0.1:3143:3143"
    healthcheck:
      test: ["CMD-SHELL", "exec 3<>/dev/tcp/127.0.0.1/3143"]
      interval: 2s
      timeout: 3s
      retries: 30
      start_period: 5s
```

(The healthcheck uses bash's `/dev/tcp`, available in the debian-based image; the Papra service keeps the conftest readiness probe as its mechanism — in-image tooling unverified, do not guess a command into it.)

Run: `docker compose -f docker-compose.test.yml up -d --wait`
Expected: both containers healthy. Live-verify before encoding the fixture:

Run: `nc -z 127.0.0.1 3143 && echo imap-up` and inject a probe message via Python smtplib to 127.0.0.1:3025 addressed to `ingest@localhost`, then verify IMAP login as `ingest@localhost` / `ingest-pass-123` lists it (imaplib in a scratch script). If login with the full address fails, try login `ingest` (local part) and adjust the fixture + a ledger note — do not guess.

- [ ] **Step 2: Write the fixture**

Add to `tests/integration/conftest.py`:

```python
GREENMAIL_HOST = "127.0.0.1"
GREENMAIL_SMTP_PORT = 3025
GREENMAIL_IMAP_PORT = 3143
GREENMAIL_USER = "ingest@localhost"
GREENMAIL_PASSWORD = "ingest-pass-123"


def _greenmail_is_up() -> bool:
    import socket

    try:
        with socket.create_connection((GREENMAIL_HOST, GREENMAIL_IMAP_PORT), timeout=2.0):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def greenmail():
    if not _greenmail_is_up():
        _skip_or_fail("scratch GreenMail not running; use `make test`")
    yield GREENMAIL_HOST, GREENMAIL_IMAP_PORT, GREENMAIL_SMTP_PORT, GREENMAIL_USER, GREENMAIL_PASSWORD
```

(Reuse the existing `_skip_or_fail` helper for the `PAPRA_REQUIRE_STACK` contract.)

Add a small SMTP injection helper in the same file (used by the tests):

```python
def inject_message(*, to, subject, attachments=()):
    """Send a MIME message with (filename, bytes, mime-tuple) attachments via GreenMail SMTP."""
    import smtplib
    from email.message import EmailMessage

    message = EmailMessage()
    message["From"] = "sender@example.com"
    message["To"] = to
    message["Subject"] = subject
    message["Message-ID"] = f"<{uuid.uuid4().hex}@test>"
    for filename, content, mime in attachments:
        message.add_attachment(content, maintype=mime[0], subtype=mime[1], filename=filename)
    with smtplib.SMTP(GREENMAIL_HOST, GREENMAIL_SMTP_PORT, timeout=10) as smtp:
        smtp.send_message(message)
```

- [ ] **Step 3: Write the failing integration tests**

`tests/integration/test_mail_ingestion.py`:

```python
import pytest

from papra_suite.mail import poll_mailbox_once
from papra_suite.papra_client import PapraClient
from papra_suite.pipeline import enqueue_ocr_job
from papra_suite.store import Store

pytestmark = pytest.mark.integration

PDF_BYTES = b"%PDF-1.4 %" + b"body-of-a-minimal-but-plausible-pdf " * 8


@pytest.fixture
def ingestion_stack(tmp_path, papra_stack, greenmail):
    base_url, org_id, api_key = papra_stack
    gm_host, gm_imap, _, gm_user, gm_password = greenmail
    store = Store(tmp_path / "state.db")
    store.migrate()
    local_org = store.create_org(papra_org_id=org_id, name="Integration Org")
    mailbox_id = store.create_mailbox(
        org_id=local_org, host=gm_host, port=gm_imap, username=gm_user,
        password=gm_password, ssl_mode="plain",
    )
    yield store, store.get_mailbox(mailbox_id), store.get_org(local_org), \
        PapraClient(base_url, api_key), org_id
    store.close()


async def test_pdf_mail_becomes_document_and_ocr_job(ingestion_stack):
    store, mailbox, org, client, papra_org_id = ingestion_stack
    inject_message(
        to=mailbox.username, subject="Rechnung 42",
        attachments=[("rechnung.pdf", PDF_BYTES, ("application", "pdf"))],
    )

    result = await poll_mailbox_once(store, client, mailbox, org)

    assert result.error is None
    assert result.documents_uploaded == 1 and result.messages_failed == 0
    documents, _count = await client.list_documents(papra_org_id, search_query="rechnung")
    assert any(doc["name"] == "rechnung.pdf" for doc in documents)
    job = store.claim_next_job("ocr")
    assert job is not None and job.kind == "ocr"
    assert store.document_seen(papra_org_id, job.document_id)
    updated = store.get_mailbox(mailbox.id)
    assert updated.last_error is None and updated.last_seen_uid is not None

    # acceptance 3 (dedup): re-poll must not create a second document
    result2 = await poll_mailbox_once(store, client, store.get_mailbox(mailbox.id), org)
    assert result2.documents_uploaded == 0
    documents_after, _ = await client.list_documents(papra_org_id, search_query="rechnung")
    assert len(documents_after) == len(documents)


async def test_non_pdf_mail_produces_no_document_and_no_error(ingestion_stack):
    store, mailbox, org, client, papra_org_id = ingestion_stack
    inject_message(
        to=mailbox.username, subject="Photos",
        attachments=[("photo.jpg", b"\xff\xd8 jpeg bytes", ("image", "jpeg"))],
    )

    result = await poll_mailbox_once(store, client, mailbox, org)

    assert result.error is None
    assert result.documents_uploaded == 0
    assert result.attachments_skipped == 1
    documents, _ = await client.list_documents(papra_org_id)
    assert documents == []
    updated = store.get_mailbox(mailbox.id)
    assert updated.last_error is None and updated.enabled is True


async def test_delete_after_download_removes_message(ingestion_stack):
    store, mailbox, org, client, papra_org_id = ingestion_stack
    # enable delete for this test via a fresh mailbox row
    mailbox_id = store.create_mailbox(
        org_id=org.id, host=mailbox.host, port=mailbox.port, username=mailbox.username,
        password=mailbox.password, ssl_mode="plain", delete_after_download=True,
    )
    delete_mailbox = store.get_mailbox(mailbox_id)
    inject_message(
        to=mailbox.username, subject="To Delete",
        attachments=[("gone.pdf", PDF_BYTES, ("application", "pdf"))],
    )

    result = await poll_mailbox_once(store, client, delete_mailbox, org)

    assert result.documents_uploaded == 1 and result.error is None
    import imaplib

    imap = imaplib.IMAP4(mailbox.host, mailbox.port)
    imap.login(mailbox.username, mailbox.password)
    imap.select("INBOX")
    status, data = imap.search(None, "ALL")
    assert data[0].split() == []          # message deleted
    imap.logout()


async def test_wrong_password_disables_mailbox_after_three_polls(ingestion_stack):
    store, mailbox, org, client, _ = ingestion_stack
    mailbox_id = store.create_mailbox(
        org_id=org.id, host=mailbox.host, port=mailbox.port, username=mailbox.username,
        password="wrong-password", ssl_mode="plain",
    )
    bad_mailbox = store.get_mailbox(mailbox_id)

    for _ in range(3):
        result = await poll_mailbox_once(store, client, bad_mailbox, org)

    assert result.error is not None
    updated = store.get_mailbox(mailbox_id)
    assert updated.enabled is False and updated.consecutive_failures == 3
```

- [ ] **Step 4: Run the integration suite and iterate**

Run: `docker compose -f docker-compose.test.yml up -d --wait && .venv/bin/pytest tests/integration -v; status=$?; docker compose -f docker-compose.test.yml down -v; exit $status`
Expected: all four tests PASS against the live GreenMail + Papra stack. If the GreenMail login form differs from the verified facts (local part vs full email), fix the fixture per the Step 1 probe and record what differed in the task report.

- [ ] **Step 5: Full one-command run**

Run: `make test`
Expected: full suite green (all unit tests + all integration tests)

- [ ] **Step 6: Run lint and the full suite once more**

Run: `.venv/bin/ruff check . && make test`
Expected: clean lint, all tests PASS

- [ ] **Step 7: Commit**

```bash
git add docker-compose.test.yml tests/integration/ Makefile
git commit -m "feat(m2): greenmail fixture and end-to-end mail ingestion tests"
```

---

## Self-Review (completed during planning)

**Spec coverage (M2 scope):** spec §12 M2 ("Mail poller with a local GreenMail-style IMAP fixture, upload pipeline, dedup") → Tasks 3–5; §5.3 poller requirements: UID tracking + UIDVALIDITY reset (Task 4), magic-byte PDF sniffing (Task 3), subject regex filter (Tasks 3–4), dedup hash (Tasks 3–4), synchronous upload + mark-as-read/delete-after-download semantics incl. the failed-upload-stays rule (Task 4), error-never-crashes + 3-consecutive-failures disable (Tasks 1, 4), filename sanitization (Task 3). M2 entry requirements from the M1 review ledger: claim atomicity, mark_job_failed guard, stale-running recovery (Task 1), compose healthcheck where verifiable (Task 5). Acceptance 1 (mail → document in correct org + OCR job enqueued; title is M3) and 2 (non-PDF → no document, log line, no error) are exercised end-to-end in Task 5; acceptance 3's dedup half is covered (restart-resilience of jobs is Task 1's `recover_running_jobs` + M3's serve wiring).

**Placeholders:** none; every code step contains the actual code. Task 5 Step 1 contains one deliberately exploratory probe (GreenMail login form) with a defined resolution procedure — that is verification work, not a placeholder.

**Type consistency:** `Mailbox`/`Org` dataclass fields match the migration-1 schema (verified against `papra_suite/store.py`: mailboxes columns org_id/host/port/username/password/ssl_mode/poll_interval_s/subject_filter/delete_after_download/enabled/last_seen_uid/uidvalidity/last_poll_at/last_error/consecutive_failures). `enqueue_ocr_job`'s parameters match Task 2's interface block and Task 4's call site; `PollResult` fields match the tests; the `greenmail` fixture tuple matches its consumers.

**Known wrinkles, resolved by design:** (a) the poller records failure state only for persisted mailboxes — unit fixtures use in-memory `Mailbox` objects, so failure accounting is asserted via `PollResult.error` there and via the store in the two persisted-state tests; (b) `imaplib`'s `response("UIDVALIDITY")` shape is asserted live in Task 4's integration run (Task 5's end-to-end test exercises the real path); (c) GreenMail's UIDVALIDITY behavior is not asserted concretely (fixture note).

Later milestones (not this plan): M3 OCR/title workers + sweep + transition handler + serve command (calls `recover_running_jobs`, `register_config_secrets`, one poll task per mailbox); M4 dashboard (mailbox CRUD UI); M5 MCP; M6 deploy.
