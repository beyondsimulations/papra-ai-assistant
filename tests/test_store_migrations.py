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
    build_store_at_version(tmp_path / "db.sqlite", 2)
    store = Store(tmp_path / "db.sqlite")
    store.migrate()
    assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 3
    cols = {r["name"] for r in store._conn.execute("PRAGMA table_info(index_ledger)")}
    assert {"papra_original_name", "delivered_generation", "cleanup_state",
            "cleanup_kreuzakt_id", "cleanup_generation",
            "cleanup_attempts", "cleanup_next_at", "written_date", "written_notes",
            "date_protected", "notes_protected"} <= cols
    idx = {r["name"] for r in store._conn.execute("PRAGMA index_list(index_ledger)")}
    assert {"idx_index_ledger_kreuzakt", "idx_index_ledger_hash",
            "idx_index_ledger_cleanup"} <= idx
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
    # on 3 rather than die on a table a rolled-back migration left behind.
    monkeypatch.undo()
    store = Store(db_path)
    try:
        store.migrate()
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 3
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
    """A file that stopped at 001 runs 002 *and* 003, and says so."""
    db_path = tmp_path / "db.sqlite"
    build_store_at_version(db_path, 1)
    store = Store(db_path)
    store.migrate()
    try:
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 3
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
