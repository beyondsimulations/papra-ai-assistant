import os
import sqlite3
import stat
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest

import papra_suite.store as store_module
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


def test_get_job_unknown_returns_none(store):
    # Pins the None guard in get_job: removing it would raise TypeError on
    # unknown ids instead of honouring the advertised Job | None contract.
    assert store.get_job("nonexistent") is None


def test_set_setting_upserts(store):
    # Pins the ON CONFLICT(key) DO UPDATE branch: a plain INSERT would raise
    # an IntegrityError on the second call instead of updating.
    store.set_setting("key", "v1")
    store.set_setting("key", "v2")
    assert store.get_setting("key") == "v2"


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


def test_concurrent_constructor_waits_for_wal(tmp_path):
    """A racing constructor must not crash on the WAL pragma while another
    connection holds a write lock on a fresh (delete-journal) database: the
    pragma does not honor busy_timeout, so __init__ retries within a bounded
    window until the lock is released."""
    db_path = tmp_path / "state.db"
    # Fresh db file in default delete-journal mode (no Store constructed yet).
    holder = sqlite3.connect(db_path, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE")
    holder.execute("CREATE TABLE lock_holder (id INTEGER PRIMARY KEY)")

    def release_later():
        time.sleep(1.0)
        holder.commit()
        holder.close()

    releaser = threading.Thread(target=release_later)
    releaser.start()
    try:
        store = Store(db_path)  # must wait for the lock, not raise
    finally:
        releaser.join(timeout=35.0)
    mode = store._conn.execute("PRAGMA journal_mode").fetchone()[0]
    store.close()
    assert mode == "wal"


def test_claim_next_job_is_fifo(store):
    """The oldest pending job must be claimed first (ORDER BY created_at ASC);
    a DESC ordering would starve the oldest job."""
    store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                      content_hash="h1", payload={})
    time.sleep(0.01)
    store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_2",
                      content_hash="h2", payload={})

    first = store.claim_next_job("ocr")
    assert first is not None and first.document_id == "doc_1"
    second = store.claim_next_job("ocr")
    assert second is not None and second.document_id == "doc_2"


def test_concurrent_migrate_no_double_apply(tmp_path):
    """Two migrators race on one file: B's pre-lock user_version read is stale
    after A commits; the in-transaction re-read must skip already-applied
    migrations instead of crashing with 'table orgs already exists'."""
    db_path = tmp_path / "state.db"
    store_a = Store(db_path)
    store_b = Store(db_path)

    class _StaleVersionConn:
        """Proxy forcing the first (pre-lock) user_version read to return the
        stale value 0, simulating B reading before A commits."""

        def __init__(self, conn):
            self._conn = conn
            self._reads = 0

        def execute(self, sql, *args):
            if sql == "PRAGMA user_version":
                self._reads += 1
                if self._reads == 1:
                    return self._conn.execute("SELECT 0")
            return self._conn.execute(sql, *args)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    store_a.migrate()

    real_conn = store_b._conn
    store_b._conn = _StaleVersionConn(real_conn)
    try:
        store_b.migrate()  # must no-op, not raise
    finally:
        store_b._conn = real_conn

    expected = len(store_module.MIGRATIONS)
    assert store_a._conn.execute("PRAGMA user_version").fetchone()[0] == expected
    assert store_b._conn.execute("PRAGMA user_version").fetchone()[0] == expected

    store_a.close()
    store_b.close()


def test_mark_job_done(store):
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                               content_hash="abc", payload={})
    job = store.claim_next_job("ocr")
    store.mark_job_done(job.id)
    done = store.get_job(job_id)
    assert done.status == "done"
    assert done.error is None


def test_mark_job_done_clears_stale_error(store):
    """mark_job_done must clear error = NULL: a job that previously failed
    (and was requeued) carries a stale error that must not survive into the
    done state."""
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                               content_hash="abc", payload={})
    job = store.claim_next_job("ocr")
    store.mark_job_failed(job.id, "boom", backoff_base_s=0)
    assert store.get_job(job_id).status == "pending"

    job = store.claim_next_job("ocr")
    store.mark_job_done(job.id)
    done = store.get_job(job_id)
    assert done.status == "done"
    assert done.error is None


def test_list_jobs_filters_by_status(store):
    first_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                                 content_hash="h1", payload={})
    second_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_2",
                                  content_hash="h2", payload={})
    claimed = store.claim_next_job("ocr")
    running_id = claimed.id
    pending_id = second_id if running_id == first_id else first_id

    pending_jobs = store.list_jobs(status="pending")
    assert [job.id for job in pending_jobs] == [pending_id]
    running_jobs = store.list_jobs(status="running")
    assert [job.id for job in running_jobs] == [running_id]


def test_mark_job_failed_requeues_with_backoff_then_fails(store):
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                               content_hash="abc", payload={})
    job = store.claim_next_job("ocr")

    store.mark_job_failed(job.id, "boom", backoff_base_s=0)
    assert store.get_job(job_id).status == "pending"
    assert store.get_job(job_id).attempts == 1
    # The requeue UPDATE must persist the error too, not just on final failure.
    assert store.get_job(job_id).error == "boom"

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


def test_reset_job_makes_job_claimable(store):
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                               content_hash="abc", payload={})
    for _ in range(3):
        job = store.claim_next_job("ocr")
        store.mark_job_failed(job.id, "boom", backoff_base_s=0)
    assert store.get_job(job_id).status == "failed"

    store.reset_job(job_id)

    claimed = store.claim_next_job("ocr")
    assert claimed is not None
    assert claimed.id == job_id
    assert claimed.status == "running"


def test_document_seen_roundtrip(store):
    assert not store.document_seen("org_1", "doc_1")
    store.record_document_seen(papra_org_id="org_1", papra_document_id="doc_1",
                               name="bill.pdf", mime="application/pdf", ingested_via="mail")
    assert store.document_seen("org_1", "doc_1")


def test_record_document_seen_is_idempotent(store):
    # Pins INSERT OR IGNORE: a plain INSERT would raise an IntegrityError on
    # the second call for the same (org, document) pair.
    store.record_document_seen(papra_org_id="org_1", papra_document_id="doc_1",
                                name="bill.pdf", mime="application/pdf", ingested_via="mail")
    store.record_document_seen(papra_org_id="org_1", papra_document_id="doc_1",
                                name="bill.pdf", mime="application/pdf", ingested_via="mail")
    assert store.document_seen("org_1", "doc_1")


def test_dedup_roundtrip(store):
    assert not store.dedup_exists("hash-1")
    store.add_dedup("hash-1")
    assert store.dedup_exists("hash-1")


def test_add_dedup_is_idempotent(store):
    # Pins INSERT OR IGNORE: a plain INSERT would violate the dedup_hashes
    # primary key on the second call.
    store.add_dedup("hash-1")
    store.add_dedup("hash-1")
    assert store.dedup_exists("hash-1")


def test_log_ring_buffer_capped(store):
    for i in range(1200):
        store.append_log_line(ts=f"2026-11-12T00:00:{i % 60:02d}Z", level="INFO", line=f"line {i}")
    lines = store.recent_log_lines(limit=500)
    assert len(lines) == 500
    assert lines[-1][2] == "line 1199"
    retained = store.recent_log_lines(limit=2000)
    assert len(retained) == 1000
    assert retained[0][2] == "line 200"
    assert retained[-1][2] == "line 1199"


def test_failed_migration_is_rolled_back_atomically(tmp_path, monkeypatch):
    def failing_migration(conn):
        conn.execute("CREATE TABLE scratch (id INTEGER PRIMARY KEY)")
        raise RuntimeError("boom")

    monkeypatch.setattr(store_module, "MIGRATIONS", [failing_migration])
    store = Store(tmp_path / "state.db")
    with pytest.raises(RuntimeError, match="boom"):
        store.migrate()
    store.close()

    monkeypatch.undo()
    fresh = Store(tmp_path / "state.db")
    fresh.migrate()
    # the version a clean re-migrate must land on is "every real migration", not
    # the literal 1 this assertion was written against (three since migration 003)
    assert fresh._conn.execute("PRAGMA user_version").fetchone()[0] == len(
        store_module.MIGRATIONS)
    assert fresh._conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'scratch'"
    ).fetchone() is None
    fresh.close()


def test_job_payload_roundtrip(store):
    """The payload dict must survive enqueue -> claim -> get unchanged;
    a mutation like json.dumps({}) must not pass silently."""
    payload = {"page_count": 2, "filename": "a b.pdf"}
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                               content_hash="abc", payload=payload)

    claimed = store.claim_next_job("ocr")
    assert claimed is not None
    assert claimed.payload == payload

    fetched = store.get_job(job_id)
    assert fetched is not None
    assert fetched.payload == payload


def test_list_jobs_respects_limit(store):
    """list_jobs(limit=2) must return exactly 2 jobs; dropping the LIMIT
    clause (or ignoring the limit argument) must fail this test."""
    for i in range(3):
        store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id=f"doc_{i}",
                          content_hash=f"h{i}", payload={})
        time.sleep(0.01)

    assert len(store.list_jobs(limit=2)) == 2


def test_backoff_schedule_is_exponential(store):
    """next_attempt_at must follow backoff_base_s * 4 ** (attempts - 1):
    ~30s after the first failure, ~120s after the second. A constant
    (non-exponential) backoff must fail this test."""
    store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                      content_hash="abc", payload={})
    job = store.claim_next_job("ocr")

    store.mark_job_failed(job.id, "boom")  # default backoff_base_s=30
    next_at = datetime.fromisoformat(store.get_job(job.id).next_attempt_at)
    delta = (next_at - datetime.now(UTC)).total_seconds()
    assert 29 <= delta <= 32, f"first backoff should be ~30s, got {delta}s"

    # The job is now in backoff (next_attempt_at ~ now+30s) and not
    # claimable; age it artificially without touching attempts so the
    # second claim/fail cycle can run.
    with store._lock, store._conn:
        store._conn.execute(
            "UPDATE jobs SET next_attempt_at = ? WHERE id = ?",
            ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(), job.id),
        )

    store.claim_next_job("ocr")
    store.mark_job_failed(job.id, "boom")  # attempts=2 -> 30 * 4 = 120s
    next_at = datetime.fromisoformat(store.get_job(job.id).next_attempt_at)
    delta = (next_at - datetime.now(UTC)).total_seconds()
    assert 119 <= delta <= 122, f"second backoff should be ~120s, got {delta}s"


def test_log_ring_buffer_tuple_order(store):
    """Each entry must be (ts, level, line) in that order; swapping the
    INSERT bind order (level, ts, line) must fail this test."""
    ts = "2026-11-13T00:00:00+00:00"
    store.append_log_line(ts=ts, level="WARNING", line="disk almost full")
    entries = store.recent_log_lines()
    assert len(entries) == 1
    assert entries[0] == (ts, "WARNING", "disk almost full")


def test_list_jobs_newest_first(store):
    """list_jobs must return the newest job first (ORDER BY created_at DESC);
    the dashboard queue view relies on this ordering."""
    a_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_a",
                             content_hash="h1", payload={})
    time.sleep(0.01)
    b_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_b",
                             content_hash="h2", payload={})

    assert [job.document_id for job in store.list_jobs()] == ["doc_b", "doc_a"]
    assert [job.id for job in store.list_jobs()] == [b_id, a_id]


def test_failed_job_is_not_claimable_during_backoff(store):
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                               content_hash="abc", payload={})
    job = store.claim_next_job("ocr")
    store.mark_job_failed(job.id, "boom")  # default backoff_base_s=30
    assert store.get_job(job_id).status == "pending"
    assert store.claim_next_job("ocr") is None


def test_migrate_holds_write_lock_from_begin(tmp_path, monkeypatch):
    """Pins BEGIN IMMEDIATE (vs a deferred BEGIN): the write lock must already
    be held when the migration function runs, otherwise a concurrent migrator
    can slip in between the BEGIN and the first DDL statement."""
    db_path = tmp_path / "state.db"

    def migration(conn: sqlite3.Connection) -> None:
        other = sqlite3.connect(db_path, timeout=0.1)
        try:
            other.execute("PRAGMA busy_timeout=100")
            try:
                other.execute("BEGIN IMMEDIATE")
                other.execute("CREATE TABLE sneaky (id TEXT)")
                other.commit()
            except sqlite3.OperationalError:
                pass  # locked/busy: the write lock IS held from the start
            else:
                raise AssertionError("write lock was not held from the start")
        finally:
            other.close()

    monkeypatch.setattr(store_module, "MIGRATIONS", [migration])

    store = Store(db_path)
    store.migrate()
    store.close()


def test_wal_retry_deadline_raises(tmp_path, monkeypatch):
    """A wedged database (lock never released) must make the constructor
    raise after the 30s deadline — not hang forever. The deadline check is
    pinned by faking the clock: the deadline computation sees t=0 and every
    subsequent monotonic() read is far past it, while time.sleep is a no-op
    so the test itself cannot stall on real sleeps."""
    db_path = tmp_path / "state.db"
    holder = sqlite3.connect(db_path, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE")
    holder.execute("CREATE TABLE lock_holder (id INTEGER PRIMARY KEY)")

    calls = 0

    def fake_monotonic():
        nonlocal calls
        calls += 1
        if calls > 10_000:
            raise AssertionError("WAL retry clock spun unboundedly")
        return 0.0 if calls == 1 else 1e12  # deadline = 30.0, then far past it

    sleeps = 0

    def fake_sleep(_seconds):
        # A no-op sleep, but bounded: a regression that deletes the deadline
        # raise (and with it every remaining monotonic() call) spins here —
        # fail the test instead of hanging the suite forever.
        nonlocal sleeps
        sleeps += 1
        if sleeps > 10_000:
            raise AssertionError("WAL retry looped past its deadline without raising")

    monkeypatch.setattr(store_module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(store_module.time, "sleep", fake_sleep)

    start = time.perf_counter()  # real clock for the wall-time bound
    try:
        with pytest.raises(sqlite3.OperationalError):
            Store(db_path)
    finally:
        holder.rollback()
        holder.close()
    assert time.perf_counter() - start < 5.0


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
    disabled = store.create_mailbox(org_id=org_id, host="h1", port=993, username="u", password="p")
    still_enabled = store.create_mailbox(
        org_id=org_id, host="h2", port=993, username="u", password="p"
    )
    # disable the first via three recorded failures
    for _ in range(3):
        store.record_poll_failure(disabled, "auth failed")
    # exact id-set comparison: the enabled mailbox IS returned, the disabled
    # one is not (a filter returning nothing must not pass)
    assert {m.id for m in store.list_mailboxes(enabled_only=True)} == {still_enabled}
    assert {m.id for m in store.list_mailboxes()} == {disabled, still_enabled}


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


def test_wal_retry_raises_fast_on_fatal_error(tmp_path):
    """A non-lock OperationalError at WAL-pragma time (e.g. an unwritable
    data dir: the pre-created db opens read-only, so PRAGMA journal_mode=WAL
    fails with 'attempt to write a readonly database') must fail fast via
    the non-contention screen — not spin through the full 30s retry window.
    (The db file must be pre-created while the dir is still writable so the
    failure lands on the PRAGMA, not on sqlite3.connect.)"""
    db_path = tmp_path / "state.db"
    seed = sqlite3.connect(db_path)  # create a valid, openable db file
    seed.close()
    os.chmod(tmp_path, stat.S_IRUSR | stat.S_IXUSR)  # read-only data dir
    try:
        start = time.monotonic()
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            Store(db_path)
        elapsed = time.monotonic() - start
    finally:
        os.chmod(tmp_path, stat.S_IRWXU)  # let tmp_path cleanup succeed
    assert elapsed < 5.0, f"fatal error should fail fast, took {elapsed:.1f}s"


def test_create_mailbox_rejects_unknown_ssl_mode(store):
    # fail-closed: an unrecognized ssl_mode must never be persisted — the
    # session would otherwise fail-open to plaintext IMAP (cleartext credentials)
    org_id = store.create_org(papra_org_id="org_abc", name="Org")
    for mode in ("TLS", "tls", "SSL", "STARTTLS", "", "secure"):
        with pytest.raises(ValueError, match="ssl_mode"):
            store.create_mailbox(
                org_id=org_id, host="h", port=993, username="u", password="p",
                ssl_mode=mode,
            )


def test_create_mailbox_accepts_valid_ssl_modes(store):
    org_id = store.create_org(papra_org_id="org_abc", name="Org")
    for mode in ("ssl", "starttls", "plain"):
        mailbox_id = store.create_mailbox(
            org_id=org_id, host="h", port=993, username="u", password="p",
            ssl_mode=mode,
        )
        assert store.get_mailbox(mailbox_id).ssl_mode == mode


def test_claim_next_job_retries_when_race_steals_candidate(tmp_path):
    """M2 carry-over: the conditional-claim retry branch, deterministically.

    A steal committed between the claim's SELECT and UPDATE must lead to
    rowcount == 0 and the loop claiming the NEXT pending job — with the
    stolen job untouched by our claim. The store is constructed inside the
    test (not via the fixture) so the thief can open the SAME db file.
    """
    db_path = tmp_path / "race.db"
    store = Store(db_path)
    store.migrate()
    try:
        for i in range(3):
            store.enqueue_job(kind="ocr", papra_org_id="org_1",
                              document_id=f"doc_{i}", content_hash=f"h{i}", payload={})

        class RacingConnection:
            """Delegates to the real connection; steals the first claimed job
            via a second connection between the claim's SELECT and UPDATE."""

            def __init__(self, real):
                self._real = real
                self._stolen = False

            def execute(self, sql, params=()):
                if not self._stolen and sql.startswith("UPDATE jobs SET status = 'running'"):
                    self._stolen = True
                    # the thief commits a steal BEFORE our UPDATE runs
                    thief = sqlite3.connect(db_path, timeout=5)
                    thief.execute("BEGIN IMMEDIATE")
                    thief.execute(
                        "UPDATE jobs SET status = 'running' WHERE document_id = 'doc_0'"
                    )
                    thief.commit()
                    thief.close()
                return self._real.execute(sql, params)

            def __getattr__(self, name):
                return getattr(self._real, name)

            def __enter__(self):
                self._real.__enter__()
                return self

            def __exit__(self, *exc_info):
                return self._real.__exit__(*exc_info)

        original = store._conn
        store._conn = RacingConnection(original)
        try:
            job = store.claim_next_job("ocr")
            assert job.document_id == "doc_1"  # doc_0 was stolen; loop retried
        finally:
            store._conn = original

        # the stolen job's status is untouched by our claim: it stays
        # 'running' from the thief's write, and doc_2 is still pending
        statuses = {
            row["document_id"]: row["status"]
            for row in store._conn.execute(
                "SELECT document_id, status FROM jobs"
            ).fetchall()
        }
        assert statuses == {"doc_0": "running", "doc_1": "running", "doc_2": "pending"}
    finally:
        store.close()


# -- ingest_events ---------------------------------------------------------

def _event_kinds():
    return store_module.INGEST_EVENT_KINDS


def test_migration_002_adds_ingest_events(tmp_path):
    """A database already at migration 001 is upgraded in place to 002."""
    db_path = tmp_path / "state.db"
    store = Store(db_path)
    store.migrate()
    assert store._conn.execute("PRAGMA user_version").fetchone()[0] == len(
        store_module.MIGRATIONS)
    tables = {row[0] for row in store._conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    indexes = {row[0] for row in store._conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_ingest%'")}
    assert "ingest_events" in tables
    assert {"idx_ingest_events_ts", "idx_ingest_events_mailbox"} <= indexes
    store.close()

    # migrating again is a no-op, not a second CREATE that errors
    store = Store(db_path)
    store.migrate()
    store.migrate()
    store.close()


def test_upgrade_from_001_adds_ingest_events_and_keeps_rows(tmp_path):
    """The upgrade a real operator's file goes through: only migration 001 has
    ever been applied, the file holds rows, and the current code opens it. The
    test above starts from an empty database, so it never runs 002 as an upgrade
    and never asks whether 001's data survived it — and its user_version check
    tracks len(MIGRATIONS), so nothing there pins the number 3."""
    db_path = tmp_path / "state.db"
    conn = sqlite3.connect(db_path)
    store_module._migration_001_initial(conn)
    conn.execute("PRAGMA user_version = 1")
    conn.execute(
        "INSERT INTO documents (papra_org_id, papra_document_id, name, mime,"
        " ingested_via, first_seen_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("org_1", "doc_1", "rechnung-2026-11.pdf", "application/pdf", "mail",
         "2026-11-16T09:00:00+00:00"),
    )
    conn.execute(
        "INSERT INTO jobs (id, kind, papra_org_id, document_id, content_hash,"
        " payload, status, attempts, next_attempt_at, error, created_at,"
        " updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("job_1", "ocr", "org_1", "doc_1", "a" * 64, '{"pages": 3}', "running",
         1, "2026-11-16T09:00:00+00:00", "upstream 502",
         "2026-11-16T08:00:00+00:00", "2026-11-16T08:30:00+00:00"),
    )
    conn.commit()
    conn.close()

    store = Store(db_path)
    store.migrate()
    try:
        # 003 (index_ledger) is the migration this path now runs, so the expected
        # value moved 2 -> 3 when it landed. Adding a migration 004 must bump it to
        # 4: that is the intended signal, and pinning the literal rather than
        # len(MIGRATIONS) is the whole point — a test that tracked the list length
        # could not notice a migration that never ran.
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 3
        tables = {row[0] for row in store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert "ingest_events" in tables
        document = store._conn.execute(
            "SELECT papra_org_id, papra_document_id, name, mime, ingested_via,"
            " first_seen_at FROM documents").fetchone()
        assert tuple(document) == (
            "org_1", "doc_1", "rechnung-2026-11.pdf", "application/pdf",
            "mail", "2026-11-16T09:00:00+00:00")
        job = store.get_job("job_1")
        assert job is not None
        assert (job.kind, job.papra_org_id, job.document_id) == (
            "ocr", "org_1", "doc_1")
        assert job.content_hash == "a" * 64
        assert job.payload == {"pages": 3}
        assert (job.status, job.attempts, job.error) == ("running", 1, "upstream 502")
        assert job.next_attempt_at == "2026-11-16T09:00:00+00:00"
        assert job.created_at == "2026-11-16T08:00:00+00:00"
        assert job.updated_at == "2026-11-16T08:30:00+00:00"
    finally:
        store.close()


def test_record_and_list_roundtrip_every_kind(store):
    for kind in _event_kinds():
        store.record_ingest_event(
            mailbox_id="mb_1", uid=100, kind=kind, uidvalidity="8813",
            papra_org_id="org_1", filename=f"file-{kind}.pdf",
            sender="billing@example.de", detail="because")
    events = store.list_ingest_events()
    assert {e.kind for e in events} == set(_event_kinds())
    event = events[0]
    assert (event.uid, event.uidvalidity, event.papra_org_id) == (100, "8813", "org_1")
    assert event.sender == "billing@example.de" and event.detail == "because"
    assert event.ts and event.id


def test_unknown_kind_is_refused(store):
    """The closed set is enforced where it is written, so a call-site typo
    fails in the developer's test run instead of inventing a sixth outcome."""
    with pytest.raises(ValueError):
        store.record_ingest_event(mailbox_id="mb_1", uid=1, kind="sender_maybe")


def test_uid_is_required_by_the_schema(store):
    with pytest.raises(sqlite3.IntegrityError):
        store._conn.execute(
            "INSERT INTO ingest_events (ts, mailbox_id, uid, kind)"
            " VALUES ('now', 'mb_1', NULL, 'sender_denied')")


def test_list_orders_newest_first_with_id_tiebreak(store):
    """Rows written inside one clock tick must still have a stable order, or
    'newest first' depends on sqlite's mood."""
    for _ in range(3):
        store.record_ingest_event(mailbox_id="mb_1", uid=1, kind="sender_denied")
    ids = [event.id for event in store.list_ingest_events()]
    assert len(ids) == 3 and ids == sorted(ids, reverse=True)


def test_list_filters(store):
    store.record_ingest_event(mailbox_id="mb_1", uid=1, kind="sender_denied")
    store.record_ingest_event(mailbox_id="mb_2", uid=2, kind="upload_failed")
    assert {e.mailbox_id for e in store.list_ingest_events(mailbox_id="mb_2")} == {"mb_2"}
    assert {e.kind for e in store.list_ingest_events(kinds=["sender_denied"])} == {
        "sender_denied"}
    assert store.list_ingest_events(kinds=[]) == []
    future = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    assert store.list_ingest_events(since=future) == []


def test_list_clamps_limit(store):
    for i in range(5):
        store.record_ingest_event(mailbox_id="mb_1", uid=i, kind="sender_denied")
    assert len(store.list_ingest_events(limit=0)) == 1
    assert len(store.list_ingest_events(limit=-3)) == 1
    assert len(store.list_ingest_events(limit=10_000)) == 5


# -- the untrusted-string boundary ----------------------------------------

def test_sanitise_strips_control_chars_and_collapses_whitespace():
    from papra_suite.store import sanitize_event_text

    assert sanitize_event_text("a\x00b\x1bc\r\nd", limit=100) == "a b c d"
    assert sanitize_event_text("   \x1b  ", limit=100) is None
    assert sanitize_event_text(None, limit=100) is None
    assert sanitize_event_text(42, limit=100) == "42"


def test_sanitise_truncates_within_its_own_budget():
    from papra_suite.store import sanitize_event_text

    out = sanitize_event_text("x" * 500, limit=200)
    assert len(out) <= 200 and out.endswith("…")
    # the limit itself is honoured for a value exactly at the boundary
    assert sanitize_event_text("y" * 200, limit=200) == "y" * 200


def test_sanitise_keeps_quotes_and_does_not_escape():
    """This is SQLite text bound by parameter. HTML-escaping or quote-stripping
    here would destroy the evidence the row exists to preserve."""
    from papra_suite.store import sanitize_event_text

    raw = 'Robert"; DROP TABLE ingest_events; --.pdf'
    assert sanitize_event_text(raw, limit=200) == raw


def test_record_ingest_event_sanitises_every_text_field(store):
    store.record_ingest_event(
        mailbox_id="mb_1", uid=7, kind="attachment_unsupported",
        filename="evil\x00name.pdf", sender="a@b.de\nFAKE ROW",
        detail="PapraError(status=502)")
    event = store.list_ingest_events()[0]
    assert event.filename == "evil name.pdf"
    assert event.sender == "a@b.de FAKE ROW"
    assert "\n" not in event.sender and "\x00" not in event.filename
    assert store._conn.execute(
        "SELECT COUNT(*) FROM ingest_events").fetchone()[0] == 1


def test_prune_keeps_the_window(store):
    old = (datetime.now(UTC) - timedelta(days=91)).isoformat()
    recent = (datetime.now(UTC) - timedelta(days=89)).isoformat()
    for uid, ts in ((1, old), (2, recent)):
        store._conn.execute(
            "INSERT INTO ingest_events (ts, mailbox_id, uid, kind) VALUES (?, 'mb_1', ?,"
            " 'sender_denied')", (ts, uid))
    store._conn.commit()
    store.record_ingest_event(mailbox_id="mb_1", uid=3, kind="sender_denied")
    store.set_setting("ignored", "x")
    assert store.prune_ingest_events(retention_days=90) == 1
    assert sorted(e.uid for e in store.list_ingest_events()) == [2, 3]
    # the recent row survives an idempotent second pass
    assert store.prune_ingest_events(retention_days=90) == 0
    assert store.get_setting("ignored") == "x"


def test_prune_with_a_bounded_window_touches_nothing_fresh(store):
    store.record_ingest_event(mailbox_id="mb_1", uid=1, kind="upload_failed")
    assert store.prune_ingest_events(retention_days=90) == 0


def test_attacker_shaped_values_are_bounded_and_the_table_survives(store):
    """The three shapes that make a "log the raw string" decision embarrassing:
    something enormous, something with control characters, and something that
    looks like an injection. Each is stored, each comes back within its budget and
    with no control characters, and the table is still there to read them from."""
    from papra_suite.store import EVENT_TEXT_LIMITS

    store.record_ingest_event(mailbox_id="mb_1", uid=1,
                              kind="attachment_unsupported", filename="x" * 5000)
    store.record_ingest_event(mailbox_id="mb_1", uid=2,
                              kind="attachment_unsupported",
                              filename="a\x00b\x1bc\r\nd\x7fe")
    store.record_ingest_event(mailbox_id="mb_1", uid=3,
                              kind="attachment_unsupported",
                              filename='Robert"; DROP TABLE ingest_events; --')

    by_uid = {event.uid: event for event in store.list_ingest_events()}
    assert len(by_uid) == 3
    long_name = by_uid[1].filename
    assert len(long_name) == EVENT_TEXT_LIMITS["filename"] and long_name.endswith("…")
    assert by_uid[2].filename == "a b c d e"    # controls became spaces, not holes
    assert by_uid[3].filename == 'Robert"; DROP TABLE ingest_events; --'
    for event in by_uid.values():
        assert not any(ord(ch) < 32 or ord(ch) == 127 for ch in (event.filename or ""))
    assert store._conn.execute(
        "SELECT COUNT(*) FROM ingest_events").fetchone()[0] == 3


def test_detail_budget_is_its_own_and_not_the_filename_one(store):
    """`detail` carries an upstream classification, which can be long. Sharing one
    budget constant across fields is how a filename ends up truncating a reason."""
    from papra_suite.store import EVENT_TEXT_LIMITS

    assert EVENT_TEXT_LIMITS["detail"] != EVENT_TEXT_LIMITS["filename"]
    store.record_ingest_event(mailbox_id="mb_1", uid=1, kind="upload_failed",
                              detail="y" * 4000, filename="z" * 4000)
    event = store.list_ingest_events()[0]
    assert len(event.detail) == EVENT_TEXT_LIMITS["detail"]
    assert len(event.filename) == EVENT_TEXT_LIMITS["filename"]
    assert event.detail.endswith("…")
