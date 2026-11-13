import sqlite3
import threading
import time

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


def test_failed_job_is_not_claimable_during_backoff(store):
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1", document_id="doc_1",
                               content_hash="abc", payload={})
    job = store.claim_next_job("ocr")
    store.mark_job_failed(job.id, "boom")  # default backoff_base_s=30
    assert store.get_job(job_id).status == "pending"
    assert store.claim_next_job("ocr") is None
