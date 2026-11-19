"""The read-only views the MCP tests build their apps on.

Containment option C (the 2026-11-19 amendment to the 2026-11-16 spec) means `build_mcp_app` refuses
a migration-capable `Store`, so a test that wants an app cannot hand it the fixture's store any
more. This module is the two lines that make that ergonomic — open a `LedgerReader` on the same file
the `Store` fixture migrated, and close the ones a test opened when it finishes.

They are remembered in a module-level list rather than returned, because the app-under-test is built
by a helper three call-depths from the fixture that owns the database, and a test that had to thread
a handle through to `addfinalizer` would be a test that eventually forgets to. Closing them is an
autouse fixture in whichever module uses it; leaking a read-only connection onto a tmp directory is
harmless, but the WAL file it keeps alive is not free.
"""

from __future__ import annotations

from papra_suite.readers import LedgerReader

_open: list[LedgerReader] = []


def remember(view) -> None:
    """Register a view a test opened itself, so the autouse fixture closes it too."""
    _open.append(view)


def view_for(store) -> LedgerReader:
    """A read-only view over the same database `store` was opened on.

    The store stays open: the test seeds it through the mutable handle and the app reads the same
    file through the view, which is exactly the two-connections-on-one-WAL-database arrangement the
    listener has in production.
    """
    reader = LedgerReader(store.db_path)
    _open.append(reader)
    return reader


def close_views() -> None:
    """Close every view this module handed out. Idempotent, and never raises."""
    while _open:
        reader = _open.pop()
        try:
            reader.close()
        except Exception:  # noqa: BLE001 - cleanup must not replace the test's own failure
            pass
