"""Containment option C: one process, objects that cannot write.

The 2026-11-16 spec's amended `## Process and configuration` section decided this differently on
2026-11-19: the MCP listener ships inside `serve`, and the containment that replaces the separate
process is **capability-shaped** — everything reachable from a tool call is handed an object with no
write surface, and a handler that reaches for a write fails to *build* rather than failing review.

This file is that claim, as tests. Four things are pinned here, and each is a different failure:

* **The ledger view cannot write.** Not "does not write": an `INSERT` and an `UPDATE` issued through
  its own connection are refused by SQLite. `PRAGMA query_only=ON` is a property of the connection,
  which is why it works against the live WAL database while `serve` holds it — a `mode=ro` URI open
  would need a usable `-shm` and fail where it would have to create a WAL index.
* **The view never migrates, and says so.** A fresh `DATA_DIR` is a startup error, not a silently
  empty archive: schema ownership stays with `serve` and the CLI.
* **The write names are absent from the objects**, not merely unused by the current handler bodies —
  asserted as an exact method set, so a write cannot be added to a view by forgetting to look.
* **A handler built on a mutable dependency refuses to construct**, named, at construction time.
"""

import sqlite3
import threading

import httpx
import pytest

from papra_suite.readers import LedgerReader
from papra_suite.store import Store

ORG = "org-1"


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


# --- the ledger view: `query_only` is in force, and it is not a migration engine -------------


def test_the_ledger_view_refuses_an_insert_and_an_update(tmp_path, store):
    """The pragma is asserted through the connection itself, because nothing public can write.

    That is the point of the test: there is no method to call, so the only way to ask whether the
    connection is read-only is to try the statement. Deleting the pragma turns both of these into a
    successful write and no other test in the suite notices.
    """
    store.set_setting("seed", "before")
    store.ledger_upsert_reference(papra_org_id=ORG, papra_document_id="d1",
                                  file_hash="ab" * 32, papra_original_name="d1.pdf")
    reader = LedgerReader(store.db_path)
    try:
        with pytest.raises(sqlite3.OperationalError):
            reader._conn.execute("INSERT INTO settings (key, value) VALUES ('x', 'y')")
        with pytest.raises(sqlite3.OperationalError):
            reader._conn.execute("UPDATE settings SET value = 'after' WHERE key = 'seed'")
    finally:
        reader.close()

    assert store.get_setting("seed") == "before", "the refused statement changed the database"


def test_an_unmigrated_database_is_a_startup_error_naming_the_file(tmp_path):
    """A fresh `DATA_DIR` must be fatal, not an empty archive.

    The spec's words: "MCP opens the existing database read-only and fails if the schema is
    unrecognised, so a fresh `DATA_DIR` is a startup error rather than a silently empty archive".
    Both shapes of "fresh" are here: no file at all, and a file that exists because something opened
    it without migrating. Answering `references: 0` from either is a confident wrong answer about a
    corpus the operator believes is being served.
    """
    from papra_suite.readers import LedgerSchemaMissing

    with pytest.raises(LedgerSchemaMissing) as missing_file:
        LedgerReader(tmp_path / "nothing-here" / "state.db")
    assert "state.db" in str(missing_file.value)
    assert not (tmp_path / "nothing-here" / "state.db").exists(), \
        "a refused read created the database it came to report as missing"

    opened_only = Store(tmp_path / "state.db")     # the file exists, the schema does not
    opened_only.close()
    with pytest.raises(LedgerSchemaMissing) as no_schema:
        LedgerReader(tmp_path / "state.db")
    assert "settings" in str(no_schema.value) and "papra-suite check" in str(no_schema.value)

    # And the refusal must not have migrated it on the way out: schema ownership is the whole
    # reason this view exists separately from `Store`.
    after = Store(tmp_path / "state.db")
    try:
        assert after.table_columns("settings") == []
    finally:
        after.close()


def test_the_ledger_view_exposes_exactly_the_reads_and_no_write(tmp_path, store):
    """The view's method set is a list, compared to a list.

    An equality assertion rather than "and it has no `set_setting`": the containment claim is about
    everything the object can do, and a future write added to the view must break something. The
    names the amended spec lists by hand (`update_document`, `delete_document`, `retag_document`,
    `create_organization`) are checked separately, including the two that do not exist anywhere in
    this codebase — the promise is that they never arrive here.
    """
    from papra_suite.readers import FORBIDDEN_WRITE_NAMES

    reader = LedgerReader(store.db_path)
    try:
        public = {name for name in dir(reader) if not name.startswith("_")}
        assert public == {"db_path", "close", "get_setting", "settings_flag", "table_columns",
                          "get_generation", "ledger_org_ids", "documents_org_ids", "ledger_stats"}
        for name in sorted(FORBIDDEN_WRITE_NAMES):
            assert not hasattr(reader, name), f"the ledger view can {name}"
        assert not hasattr(reader, "migrate"), "the listener would migrate on first read"
    finally:
        reader.close()


def test_the_ledger_view_answers_what_the_handlers_ask(tmp_path, store):
    """The whitelist is not empty in principle: every read a tool performs works, off `Store`.

    Same SQL as `papra-suite check`, by construction — these are the same function objects — so a
    number here cannot disagree with the number `ledger --status` prints. That is why the view
    assigns `Store`'s reads instead of re-implementing the correlated scans.
    """
    store.set_setting("mcp_server_enabled", "1")
    store.record_document_seen(papra_org_id=ORG, papra_document_id="orph", name="orph.pdf",
                               mime="application/pdf", ingested_via="test")
    store.ledger_upsert_reference(papra_org_id=ORG, papra_document_id="d1", file_hash="ab" * 32,
                                  papra_original_name="d1.pdf")
    reader = LedgerReader(store.db_path)
    try:
        assert reader.get_setting("mcp_server_enabled") == "1"
        assert reader.settings_flag("mcp_server_enabled") is True
        assert reader.settings_flag("no_such_flag") is False
        assert reader.get_generation() == 1
        assert reader.ledger_org_ids() == [ORG]
        assert reader.documents_org_ids() == [ORG]
        stats = reader.ledger_stats(org_ids=[ORG])
        assert stats["references"] == 1 and stats["unledgered"] == 1
        assert reader.ledger_stats(org_ids=[])["references"] == 0
    finally:
        reader.close()


async def test_the_ledger_view_is_read_from_whichever_worker_thread_asked(tmp_path, store):
    """`asyncio.to_thread`, i.e. the fix I6: the thread that reads is not the one that opened.

    Without `check_same_thread=False` a store read in a tool would raise
    `sqlite3.ProgrammingError` the moment the pool handed the call to a different worker; with it
    and no lock, two concurrent
    reads share one connection from two threads. So this runs the shipped reads on other threads,
    concurrently, and insists nothing complains and every answer is complete.
    """
    import asyncio

    for index in range(12):
        store.ledger_upsert_reference(
            papra_org_id=f"org-{index % 3}", papra_document_id=f"d{index}",
            file_hash=f"{index:02x}" * 32, papra_original_name=f"d{index}.pdf")
    reader = LedgerReader(store.db_path)
    try:
        main_thread = threading.get_ident()

        async def read():
            return await asyncio.to_thread(reader.ledger_stats)

        results = await asyncio.gather(*(read() for _ in range(16)))
    finally:
        reader.close()

    assert all(sum(row.values()) > 0 for row in results)
    assert results[0]["references"] == 12
    assert threading.get_ident() == main_thread


# --- the HTTP views: no POST, PATCH or DELETE, at any depth ----------------------------------


def papra_reader(handler):
    from papra_suite.readers import PapraReader

    return PapraReader("http://papra.test", "ppapi_read_only",
                       transport=httpx.MockTransport(handler))


def kreuzakt_reader(handler):
    from papra_suite.readers import KreuzaktReader

    return KreuzaktReader("http://kreuzakt.test", transport=httpx.MockTransport(handler))


async def test_the_papra_view_has_no_writes_and_cannot_issue_a_write_verb():
    """`update_document`, `delete_document`, `retag_document`, `create_organization`: absent.

    Two doors are shut, and the second is the one that matters. The view has no write method, and it
    also has no way to ask its transport for one: `_request` accepts `GET`, and the transport
    underneath is a wrapper that raises for anything else. So "no PATCH on the request path" is a
    property of the object rather than of the tool bodies that happen to exist today.
    """
    from papra_suite.papra_client import PapraClient
    from papra_suite.readers import FORBIDDEN_WRITE_NAMES, ReadOnlyViolation

    seen = {}

    def handler(request):
        seen["method"] = request.method
        seen["path"] = request.url.path
        return httpx.Response(200, json={"tags": [{"id": "t1", "name": "invoice"}]})

    reader = papra_reader(handler)
    try:
        public = {name for name in dir(reader) if not name.startswith("_")}
        assert public == {"aclose", "get_document", "list_documents", "download_file", "list_tags",
                          "whoami"}
        for name in sorted(FORBIDDEN_WRITE_NAMES):
            assert not hasattr(reader, name), f"the papra view can {name}"
        assert not isinstance(reader, PapraClient), "a read view must not inherit the writer"

        assert await reader.list_tags(ORG) == [{"id": "t1", "name": "invoice"}]
        assert seen == {"method": "GET", "path": f"/api/organizations/{ORG}/tags"}

        with pytest.raises(ReadOnlyViolation):
            await reader._request("PATCH", f"/api/organizations/{ORG}/documents/d1")
        with pytest.raises(ReadOnlyViolation):
            await reader._request("DELETE", "/api/anything")
        with pytest.raises(ReadOnlyViolation):
            await reader._client.request("POST", "/api/anything")
    finally:
        await reader.aclose()


async def test_the_kreuzakt_view_has_no_writes_and_cannot_issue_a_write_verb():
    """The index's own client, minus `upload`, `delete_document` and `retry_queue`.

    `health` and `queue_counts` are what `index_status` asks today; the rest are the reads Task 14's
    search tools will need, and they are here so that task does not have to widen the object under
    pressure. `wait_for_queue_row` is deliberately absent: it is the correlation loop for an upload,
    and an upload cannot happen here.
    """
    from papra_suite.kreuzakt_client import KreuzaktClient
    from papra_suite.readers import FORBIDDEN_WRITE_NAMES, ReadOnlyViolation

    def handler(request):
        if request.url.path == "/api/health":
            return httpx.Response(200, json={"status": "ok", "documents": 3})
        return httpx.Response(200, json={"entries": [], "counts": {"pending": 2, "completed": 9}})

    reader = kreuzakt_reader(handler)
    try:
        public = {name for name in dir(reader) if not name.startswith("_")}
        assert public == {"aclose", "get_document", "health", "list_documents", "queue_counts",
                          "queue_high_water_mark", "queue_page", "search"}
        for name in sorted(FORBIDDEN_WRITE_NAMES):
            assert not hasattr(reader, name), f"the kreuzakt view can {name}"
        assert not isinstance(reader, KreuzaktClient)
        assert not hasattr(reader, "wait_for_queue_row")

        assert (await reader.health())["status"] == "ok"
        assert await reader.queue_counts() == {"pending": 2, "completed": 9}

        with pytest.raises(ReadOnlyViolation):
            await reader._request("DELETE", "/api/documents/1")
        with pytest.raises(ReadOnlyViolation):
            await reader._request("POST", "/api/documents")
        with pytest.raises(ReadOnlyViolation):
            await reader._client.request("PATCH", "/api/anything")
    finally:
        await reader.aclose()


# --- construction: a handler on a mutable dependency does not build --------------------------

PUBLIC_URL = "https://papra-mcp.test"


class OkKreuzakt:
    """The two reads `index_status` performs, and nothing else."""

    async def health(self):
        return {"status": "ok", "documents": 1}

    async def queue_counts(self):
        return {"pending": 0}


def views(tmp_path, store):
    """The three objects the shipped composition root builds."""
    from papra_suite.readers import LedgerReader as Reader

    return Reader(store.db_path), papra_reader(lambda request: httpx.Response(200, json={})), \
        OkKreuzakt()


def build(store_like, papra=None, kz=None, *, url=PUBLIC_URL):
    from papra_suite.mcp_server import build_mcp_app

    return build_mcp_app(store_like, papra if papra is not None else OkPapraLike(),
                         kz if kz is not None else OkKreuzakt(), url=url)


class OkPapraLike:
    """A stand-in for `PapraReader`: reads only, no writes. No tool calls it today."""


def test_the_store_the_listener_builds_on_is_a_view_not_the_writer(tmp_path, store):
    reader, papra, kz = views(tmp_path, store)
    try:
        assert build(reader, papra, kz) is not None
    finally:
        reader.close()


def test_a_handler_handed_the_migration_capable_store_refuses_to_construct(tmp_path, store):
    """The mistake this rule exists for: sharing `serve`'s `Store` because it is right there.

    Named, at construction, before a socket is bound. `Store` can `migrate` and can write, so a
    listener built on it is a listener where the containment is a habit instead of a type — and the
    everyday way that habit breaks is a handler that reaches for one convenient `set_setting`.
    """
    from papra_suite.readers import McpContainmentError

    with pytest.raises(McpContainmentError) as caught:
        build(store)
    message = str(caught.value)
    assert "Store" in message and "store" in message
    assert "migrate" in message
    assert "readers" in message, "the refusal has to name where the read-only object comes from"


def test_a_handler_handed_a_full_api_client_refuses_to_construct(tmp_path, store):
    """Both clients, because both own writes against a system an agent must not change."""
    from papra_suite.kreuzakt_client import KreuzaktClient
    from papra_suite.papra_client import PapraClient
    from papra_suite.readers import LedgerReader, McpContainmentError

    reader = LedgerReader(store.db_path)
    full_papra = PapraClient("http://papra.test", "ppapi_key")
    full_kreuzakt = KreuzaktClient("http://kreuzakt.test")
    try:
        with pytest.raises(McpContainmentError) as papra_refusal:
            build(reader, papra=full_papra)
        assert "PapraClient" in str(papra_refusal.value)
        with pytest.raises(McpContainmentError) as kz_refusal:
            build(reader, kz=full_kreuzakt)
        assert "KreuzaktClient" in str(kz_refusal.value)
    finally:
        reader.close()


def test_a_double_that_grew_a_write_name_is_refused_too(tmp_path, store):
    """The check is structural, so a fake cannot smuggle a write past it.

    `retag_document` does not exist in this codebase; the amended spec names it anyway, and this is
    the assertion that makes naming it mean something: whatever class a `papra` argument turns out
    to be, if it answers to a write name, the listener does not start.
    """
    from papra_suite.readers import LedgerReader, McpContainmentError

    class PapraWithWrites(OkPapraLike):
        async def retag_document(self, org_id, document_id):
            raise AssertionError("unreachable")

        async def create_organization(self, name):
            raise AssertionError("unreachable")

    reader = LedgerReader(store.db_path)
    try:
        with pytest.raises(McpContainmentError) as caught:
            build(reader, papra=PapraWithWrites())
        assert "retag_document" in str(caught.value)
        assert "create_organization" in str(caught.value)
    finally:
        reader.close()


def test_a_store_that_cannot_answer_what_the_handlers_ask_is_refused(tmp_path):
    """The other half of the same refusal: an object too narrow to serve is not a silent 500."""
    from papra_suite.readers import McpContainmentError

    with pytest.raises(McpContainmentError) as caught:
        build(object())
    assert "ledger_stats" in str(caught.value)


def listener_config(tmp_path, port=8765):
    from types import SimpleNamespace

    return SimpleNamespace(mcp_host="127.0.0.1", mcp_port=port, mcp_url=PUBLIC_URL,
                           papra_base_url="http://papra.test", papra_api_key="ppapi_read_only",
                           kreuzakt_base_url="http://kreuzakt.test", data_dir=tmp_path)


async def test_the_listener_builds_the_app_on_views_and_closes_what_it_opened(monkeypatch,
                                                                              tmp_path, store):
    """The composition root, pinned: `serve`'s store stops at `run_mcp_listener`.

    The refusal in `build_mcp_app` is only half of it — somebody has to *make* the views. This is
    the shipped path, so the assertion is on the objects the app was actually handed: a
    `LedgerReader` on the same file, and read-only clients built from the deployment's own
    configuration. Nothing that can write is in the tuple.

    The refusal case is here too, because the leak it prevents is a held database file: a build that
    raises (a bad `PAPRA_MCP_URL`, a refused dependency) must still close the connection it opened.
    """
    from papra_suite.mcp_server import run_mcp_listener
    from papra_suite.readers import KreuzaktReader, LedgerReader, PapraReader

    captured = {}

    def refusing_build(ledger, papra_view, kreuzakt_view, *, url, host, port):
        captured.update(ledger=ledger, papra=papra_view, kreuzakt=kreuzakt_view)
        raise RuntimeError("the app refused to build")

    monkeypatch.setattr("papra_suite.mcp_server.build_mcp_app", refusing_build)
    with pytest.raises(RuntimeError, match="refused to build"):
        await run_mcp_listener(listener_config(tmp_path), store)

    assert isinstance(captured["ledger"], LedgerReader)
    assert not isinstance(captured["ledger"], Store)
    assert captured["ledger"].db_path == store.db_path
    assert isinstance(captured["papra"], PapraReader)
    assert isinstance(captured["kreuzakt"], KreuzaktReader)
    with pytest.raises(sqlite3.ProgrammingError):
        captured["ledger"].get_setting("anything")     # closed on the way out, not leaked


def test_the_auth_gate_refuses_a_write_capable_store_too(store):
    """The verifier writes one settings key, on its own connection, so it is checked too.

    A `Store` here would let the credential stamp grow into "and one more write while we are here".
    """
    from papra_suite.mcp_server import StoreTokenVerifier
    from papra_suite.readers import McpContainmentError

    with pytest.raises(McpContainmentError) as caught:
        StoreTokenVerifier(store)
    assert "store" in str(caught.value)


def test_the_composition_root_borrows_the_clients_serve_hands_it(store):
    """`serve` injects its own mutable clients, so injection must yield views, not a refusal.

    A refusal reads as containment and is really a listener that cannot start in production: the
    unit tests inject read-only fakes, while the process injects the one Papra client and the one
    Kreuzakt client that the pollers, the title worker and the indexer share. Borrowing keeps that
    sharing honest — the views stay out of `owned`, so the listener cannot close a transport the
    pipeline is still using — and the owner's client keeps its writes, because containment is about
    what a tool call can reach, not about crippling the process that owns the credential.
    """
    from types import SimpleNamespace

    from papra_suite.kreuzakt_client import KreuzaktClient
    from papra_suite.mcp_server import _open_read_views
    from papra_suite.papra_client import PapraClient
    from papra_suite.readers import KreuzaktReader, PapraReader, require_read_only

    papra, kz = PapraClient("http://papra.test", "papra-key-1"), KreuzaktClient("http://kz.test")
    config = SimpleNamespace(papra_base_url="http://elsewhere.test", papra_api_key="elsewhere-key",
                             kreuzakt_base_url="http://elsewhere.test")

    ledger, papra_view, kz_view, owned = _open_read_views(config, store, papra, kz)
    try:
        assert isinstance(papra_view, PapraReader) and isinstance(kz_view, KreuzaktReader)
        require_read_only(papra_view, role="papra")
        require_read_only(kz_view, role="kreuzakt")
        assert owned == [], "a borrowed transport must not be closed by the listener"
        assert papra_view._client._client is papra._client, "a second transport was opened"
        assert callable(papra.update_content)  # the owner keeps its writes
    finally:
        ledger.close()
