import sqlite3
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
    assert fresh._conn.execute("PRAGMA user_version").fetchone()[0] == 1
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
