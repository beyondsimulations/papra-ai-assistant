"""Migration-path tests: what an operator's existing file looks like when the
current code opens it.

`tests/test_store.py` covers the behaviour each migration produces; this module
covers the upgrade edge itself — a database that stopped at an older version,
holds rows, and must arrive at the current one with both its schema and its
data intact. The helper builds those starting states from `MIGRATIONS` itself
rather than from a checked-in `.sql` fixture: a fixture copied out of the schema
drifts the moment someone edits 001, and a drifted fixture tests a database no
operator has.
"""

import sqlite3

import pytest

import papra_suite.store as store_module
from papra_suite.store import Store


def tables(conn) -> set[str]:
    return {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}


def build_store_at_version(path, version: int):
    """Create a database whose schema is exactly `MIGRATIONS[:version]` and
    whose `user_version` is `version`, then close it.

    This is what an operator's file looks like: migration `version` was the last
    one ever applied, so every later migration is pending. Building it by
    *calling* the earlier migrations (instead of executing a frozen dump) keeps
    the past states honest — and it is also the only way a fresh checkout can
    build a "001-era" file, since the schema lives in Python.

    At least one real row is written into a 001-era table, because "the schema
    migrated" and "the rows survived it" are different claims and only the second
    one matters to the operator. `documents` is the table a real deployment has
    rows in, and it is the table `check` reports on.

    Returns the path so a test can read `version` like `Store(path)` would.
    """
    conn = sqlite3.connect(path)
    try:
        for migration in store_module.MIGRATIONS[:version]:
            migration(conn)
        conn.execute(f"PRAGMA user_version = {version}")
        conn.execute(
            "INSERT INTO documents (papra_org_id, papra_document_id, name, mime,"
            " ingested_via, first_seen_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("org_1", "doc_survivor", "rechnung-2026-11.pdf", "application/pdf",
             "mail", "2026-11-16T09:00:00+00:00"),
        )
        conn.commit()
    finally:
        conn.close()
    return path


def test_migration_003_adds_ledger_with_generation_and_cleanup(tmp_path):
    db_path = tmp_path / "db.sqlite"
    build_store_at_version(db_path, 2)
    store = Store(db_path)
    store.migrate()
    try:
        # A 002 file runs 003 *and* everything after it, so this asserts the current
        # version rather than a literal: what belongs to 003 is the ledger below, and
        # the literal that catches a migration that never ran lives in
        # `test_store.py::test_upgrade_from_001_adds_ingest_events_and_keeps_rows`.
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == len(
            store_module.MIGRATIONS)
        cols = {r["name"] for r in store._conn.execute("PRAGMA table_info(index_ledger)")}
        assert {"papra_original_name", "delivered_generation", "cleanup_state",
                "cleanup_kreuzakt_id", "cleanup_generation",
                "cleanup_attempts", "cleanup_next_at", "written_date", "written_notes",
                "date_protected", "notes_protected"} <= cols
        idx = {r["name"] for r in store._conn.execute("PRAGMA index_list(index_ledger)")}
        assert {"idx_index_ledger_kreuzakt", "idx_index_ledger_hash",
                "idx_index_ledger_cleanup"} <= idx
    finally:
        store.close()


def test_failed_migration_003_leaves_nothing_behind_and_can_be_retried(tmp_path, monkeypatch):
    """A migration that dies partway must leave the file exactly as it was.

    `migrate()` wraps every migration in `BEGIN IMMEDIATE` and rolls back on
    failure, which is what makes a half-applied upgrade retryable on the next
    start. `executescript` voids that contract: it commits the pending
    transaction before it runs, so a statement after the first one failing leaves
    `index_ledger` permanently applied while `user_version` stays 2 — and because
    003 has no `IF NOT EXISTS`, the next `migrate()` dies on "table index_ledger
    already exists" and the store can never be opened again. That is not a corner
    case: it is the upgrade path every operator's file takes.

    The failure is injected *after* the real 003 has run, so this says nothing
    about which statement broke. It demands only the two properties that keep an
    operator's database alive: nothing half-applied remains, and the retry works.
    """
    db_path = tmp_path / "db.sqlite"
    build_store_at_version(db_path, 2)
    real = store_module._migration_003_index_ledger

    def exploding(conn):
        real(conn)
        raise RuntimeError("simulated: a statement after the table failed")

    monkeypatch.setattr(store_module, "MIGRATIONS",
                        [*store_module.MIGRATIONS[:2], exploding])
    store = Store(db_path)
    try:
        with pytest.raises(RuntimeError, match="simulated"):
            store.migrate()
        assert "index_ledger" not in tables(store._conn)
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 2
    finally:
        store.close()

    # The next start is the one that matters: an unpatched migrate() has to land
    # on the current version rather than die on a table a rolled-back migration
    # left behind.
    monkeypatch.undo()
    store = Store(db_path)
    try:
        store.migrate()
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == len(
            store_module.MIGRATIONS)
        assert "index_ledger" in tables(store._conn)
    finally:
        store.close()


def test_migration_003_upgrade_keeps_001_rows(tmp_path):
    """003 is additive: the row that was already there is still there.

    `index_ledger` is a new table, so nothing here *should* touch `documents` —
    but a migration that committed implicitly could turn a failed upgrade into a
    half-written one, and the row is the canary for exactly that. (003 used to run
    `executescript`, which does commit implicitly; see
    `test_failed_migration_003_leaves_nothing_behind_and_can_be_retried`.) The row
    is the canary.
    """
    db_path = tmp_path / "db.sqlite"
    build_store_at_version(db_path, 2)
    store = Store(db_path)
    store.migrate()
    try:
        row = store._conn.execute(
            "SELECT papra_org_id, papra_document_id, name FROM documents").fetchone()
        assert tuple(row) == ("org_1", "doc_survivor", "rechnung-2026-11.pdf")
        # and the new table is usable, not merely present
        store.ledger_upsert_reference(papra_org_id="org_1", papra_document_id="doc_1",
                                      file_hash="ab" * 32, papra_original_name="a.pdf")
        assert store.ledger_get("org_1", "doc_1").papra_original_name == "a.pdf"
    finally:
        store.close()


def test_migrate_from_001_lands_on_the_current_version(tmp_path):
    """A file that stopped at 001 runs every later migration, and says so."""
    db_path = tmp_path / "db.sqlite"
    build_store_at_version(db_path, 1)
    store = Store(db_path)
    store.migrate()
    try:
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == len(
            store_module.MIGRATIONS)
        tables = {row[0] for row in store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"ingest_events", "index_ledger"} <= tables
    finally:
        store.close()


def test_partial_index_on_kreuzakt_id_excludes_deleted_rows(tmp_path):
    """`idx_index_ledger_kreuzakt` is partial on purpose.

    The planner must be able to satisfy `ledger_resolve` from the index alone:
    the whole security property of the ledger is that a stale-generation or
    deleted row never resolves, and a query that had to scan to learn that would
    get quietly slower as the corpus grows. Asserting the stored SQL is the only
    way to notice if a later migration drops the predicate.
    """
    db_path = tmp_path / "db.sqlite"
    store = Store(db_path)
    store.migrate()
    try:
        sql = store._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index'"
            " AND name='idx_index_ledger_kreuzakt'").fetchone()[0]
        assert "deleted_at IS NULL" in sql
        assert "kreuzakt_id IS NOT NULL" in sql
    finally:
        store.close()


# --- migration 004: the jobs queue becomes re-schedulable -------------------------

def seed_job_at_003(path, *, job_id, kind, status, document_id="doc_1", content_hash="a" * 64):
    """Put one job row into a database whose schema stops at 003.

    Written as raw SQL rather than through `Store.enqueue_job` because the point of
    these tests is the file an operator already has: a terminal row that the
    full-table unique index let settle onto the idempotency key, before this build
    knew a queue row was supposed to be re-schedulable.
    """
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "INSERT INTO jobs (id, kind, papra_org_id, document_id, content_hash,"
            " payload, status, attempts, next_attempt_at, error, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, '{}', ?, 3, '2026-11-16T09:00:00+00:00',"
            " 'upstream 502', '2026-11-16T09:00:00+00:00', '2026-11-16T09:30:00+00:00')",
            (job_id, kind, "org_1", document_id, content_hash, status),
        )
        conn.commit()
    finally:
        conn.close()
    return path


def idempotency_index(conn):
    """The index guarding the index queue's idempotency key, by stored SQL.

    Looked up by content rather than by name, so one helper answers "what is there
    now" both before and after the upgrade: 004 replaces the index in place, and a
    test that asked only for a name could not tell a replaced index from a second
    one left next to it.
    """
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='jobs'"
        " AND sql LIKE '%content_hash%'").fetchall()
    return [(row["name"], row["sql"]) for row in rows]


def test_migration_004_makes_the_idempotency_index_partial(tmp_path):
    """004 replaces the queue-wide unique index with one over live rows only.

    The stored SQL is asserted because it *is* the contract: with the predicate
    gone the index is a whole-table constraint again, every behaviour built on "a
    terminal row is history, not a lock" quietly stops being true, and nothing else
    in the schema would notice.
    """
    db_path = tmp_path / "db.sqlite"
    build_store_at_version(db_path, 3)
    store = Store(db_path)
    store.migrate()
    try:
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 4
        entries = idempotency_index(store._conn)
        assert len(entries) == 1, f"the old index must be replaced, not doubled: {entries}"
        name, sql = entries[0]
        assert "UNIQUE" in sql.upper()
        assert "WHERE status IN ('pending','running')" in sql
        # The key itself is unchanged: 004 narrows *when* the constraint applies.
        for column in ("kind", "papra_org_id", "document_id", "content_hash"):
            assert column in sql
        assert name == "idx_jobs_idempotency"
    finally:
        store.close()


def test_upgrade_from_003_keeps_terminal_job_rows(tmp_path):
    """The rows an operator's file already holds survive the index swap.

    004 rebuilds an index, not a table, so a completed `index` row and a failed
    `ocr` row have to come out the other side as they went in: they are the audit
    trail of what the bridge already did.
    """
    db_path = tmp_path / "db.sqlite"
    build_store_at_version(db_path, 3)
    seed_job_at_003(db_path, job_id="job_done", kind="index", status="done")
    seed_job_at_003(db_path, job_id="job_failed", kind="ocr", status="failed",
                    document_id="doc_2")

    store = Store(db_path)
    store.migrate()
    try:
        done = store.get_job("job_done")
        failed = store.get_job("job_failed")
        assert (done.status, done.attempts, done.error) == ("done", 3, "upstream 502")
        assert (failed.status, failed.kind) == ("failed", "ocr")
        assert store._conn.execute(
            "SELECT papra_document_id FROM documents").fetchone()[0] == "doc_survivor"
    finally:
        store.close()


def test_upgrade_from_003_frees_a_completed_index_row(tmp_path):
    """The upgrade itself, not merely the new schema: the trapped document is free.

    This is the file the operator has *today* — a completed `index` row sitting on
    the idempotency key of a document whose Kreuzakt copy went away — run through
    `migrate()` rather than into a hand-built 004 database. Before the upgrade the
    second enqueue collapses onto the completed row; after it the same call yields
    a claimable job. That before/after pair is the migration's whole purpose, so it
    is asserted as one story.
    """
    db_path = tmp_path / "db.sqlite"
    build_store_at_version(db_path, 3)
    seed_job_at_003(db_path, job_id="job_done", kind="index", status="done")

    before = Store(db_path)
    try:
        # Pinned pre-upgrade so the "after" half cannot be resting on a different
        # starting state: on a 003 file the enqueue is a no-op that hands back the
        # completed row, and there is nothing to claim.
        assert before.enqueue_job(kind="index", papra_org_id="org_1",
                                  document_id="doc_1", content_hash="a" * 64,
                                  payload={}) == "job_done"
        assert before.claim_next_job("index") is None
    finally:
        before.close()

    after = Store(db_path)
    after.migrate()
    try:
        job_id = after.enqueue_job(kind="index", papra_org_id="org_1",
                                   document_id="doc_1", content_hash="a" * 64,
                                   payload={})
        assert job_id != "job_done"
        claimed = after.claim_next_job("index")
        assert claimed is not None and claimed.id == job_id
        # The history row is still there, still done: the queue grew, it did not
        # rewrite what already happened.
        assert after.get_job("job_done").status == "done"
    finally:
        after.close()


def test_failed_migration_004_leaves_nothing_behind_and_can_be_retried(tmp_path, monkeypatch):
    """004 runs statement by statement inside `migrate()`'s transaction.

    The lesson 003 had to learn once: `executescript` commits the pending
    transaction before it runs, so a `DROP INDEX` that landed followed by a
    `CREATE INDEX` that failed would leave the queue with *no* idempotency
    constraint and a `user_version` still at 3. That is worse than 003's version of
    the bug: the retry would not die on an "index already exists" error, because
    nothing would be there — the queue would simply stop deduplicating, and the
    first sign would be duplicate work rather than an exception.

    The failure is injected after the real 004 has run, so this pins the two
    properties that matter and not which statement broke.
    """
    db_path = tmp_path / "db.sqlite"
    build_store_at_version(db_path, 3)
    real = store_module._migration_004_jobs_live_idempotency

    def exploding(conn):
        real(conn)
        raise RuntimeError("simulated: a statement after the index failed")

    monkeypatch.setattr(store_module, "MIGRATIONS",
                        [*store_module.MIGRATIONS[:3], exploding])
    store = Store(db_path)
    try:
        with pytest.raises(RuntimeError, match="simulated"):
            store.migrate()
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 3
        # The rolled-back file still holds the constraint it started with: a queue
        # with no unique index is the failure this test exists to catch.
        entries = idempotency_index(store._conn)
        assert len(entries) == 1
        assert "WHERE" not in entries[0][1]
    finally:
        store.close()

    monkeypatch.undo()
    store = Store(db_path)
    try:
        store.migrate()
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 4
        assert "WHERE status IN ('pending','running')" in idempotency_index(store._conn)[0][1]
    finally:
        store.close()
