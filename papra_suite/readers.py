"""Objects that cannot write: the containment the MCP request path is built out of.

The 2026-11-16 spec's `## Process and configuration` section was amended on 2026-11-19. The
listener ships inside `papra-suite serve` rather than in its own process, and what replaces the
process boundary is a capability boundary: everything reachable from a tool call is handed one of
the three views defined here, and each of them has **no write surface at all** — not "does not use
its writes", but "has no writes to use". A handler that reaches for a write therefore fails to
construct, which is the difference between a guarantee and a code-review habit.

Three decisions, each because the obvious version fails quietly:

**The ledger view is a connection under `PRAGMA query_only=ON`, and it never migrates.** Not
`?mode=ro`: the ledger is WAL, and a read-only URI open of a WAL database needs a usable `-shm` and
fails where it would have to create a WAL index — i.e. it breaks against exactly the live database
this listener runs beside. `query_only` is a property of the connection, so it works while `serve`
holds the same file. Schema ownership stays with `serve` and the CLI, so the constructor *refuses*
an unrecognised schema: a fresh `DATA_DIR` is a startup error naming the file, not a silently empty
archive.

**The views are whitelists, not subclasses.** `LedgerReader` does not inherit from `Store`, and
`PapraReader`/`KreuzaktReader` do not inherit from the clients they read through. Inheriting *down*
from a mutable class is how a write reappears the day somebody adds one there. Instead each view
names, in one place, the exact functions it exposes — `get_setting = Store.get_setting` — so the
method set is an explicit list a reviewer can read and a test can compare against. The SQL and the
wire parsing stay single-implementations: `index_status` and `papra-suite check` cannot disagree
about a count because they run the same query object.

**The HTTP views cannot issue a verb that changes anything.** Their `_request` accepts `GET` and
raises for everything else, and the transport underneath is a wrapper that raises too. So a
`PATCH` is unreachable even for code that digs for a private attribute, which is the property the
spec asked for: no POST, PATCH or DELETE on the request path, absent rather than unused.

The threading rule is the one `Store` already follows: handlers run through `asyncio.to_thread`, so
the worker thread that touches a connection is whichever one the pool had free. Each view therefore
opens with `check_same_thread=False` and keeps every access inside a lock.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from papra_suite.kreuzakt_client import KreuzaktClient
from papra_suite.papra_client import PapraClient
from papra_suite.store import Store

#: The tables the ledger view reads. All three must exist before it will open: `settings` for the
#: flags and the generation, `index_ledger` for every count, `documents` for the `unledgered`
#: anti-join. A database missing any of them is an unmigrated one, and this suite's rule is that the
#: reader does not fix that — it refuses.
READER_SCHEMA_TABLES = ("settings", "documents", "index_ledger")

#: Same as `Store`'s: a read that arrives while a delivery write holds the WAL write lock waits for
#: it rather than answering `database is locked` to whoever called the tool.
READER_BUSY_TIMEOUT_MS = 5000


class LedgerSchemaMissing(RuntimeError):
    """The ledger view was pointed at a database that has no schema to read.

    Fatal at construction, per the spec: "MCP opens the existing database read-only and fails if the
    schema is unrecognised, so a fresh `DATA_DIR` is a startup error rather than a silently empty
    archive". Answering `references: 0` from a database that has never been migrated is the
    confident wrong answer this project keeps designing against, and it would also be a lie about
    what this process is: the listener does not migrate, so it cannot know what an empty file was
    meant to hold.
    """


class ReadOnlyViolation(RuntimeError):
    """A verb other than GET was attempted through one of the HTTP views.

    Unreachable from any handler today — there is no method to call and the transport refuses it
    too — and it stays because the containment is about what is *possible* from this object, not
    about what the current tool bodies do.
    """


class McpContainmentError(TypeError):
    """A mutable dependency was handed to something that must not be able to write.

    Raised at construction, so the failure lands on whoever wired it, before a socket is bound. The
    two refusals are the two shapes the mistake comes in: the migration-capable `Store` (or a full
    API client), and anything at all that answers to one of the write names on
    `FORBIDDEN_WRITE_NAMES` — which is how a fake that grew a write, or a read-only view that was
    quietly re-parented onto the mutable class, is caught.
    """


#: Every write this project can perform through the store or the two API clients, plus the four
#: names the amended spec lists by name (`update_document`, `delete_document`, `retag_document`,
#: `create_organization`). The last two do not exist anywhere in this codebase yet; they are listed
#: because the spec's promise is that a view never grows them, and a test asserts they are absent
#: from every view object.
FORBIDDEN_WRITE_NAMES = frozenset({
    # store
    "migrate", "set_setting", "clear_setting", "bump_generation", "append_log_line",
    "record_ingest_event", "ledger_upsert_reference", "ledger_mark_deleted",
    "ledger_mark_restored", "ledger_queue_cleanup", "ledger_record_delivery",
    "ledger_resolve", "ledger_set_gate", "add_document", "add_org",
    # papra
    "create_document", "update_document", "update_content", "add_tag", "delete_document",
    "retag_document", "create_organization",
    # kreuzakt
    "upload", "retry_queue",
})

#: The reads the shipped handlers actually perform, per role. `papra` is empty because no tool
#: registered today reads Papra; the entry exists so the shape of the check — and the day a handler
#: gains a Papra read, which is Task 14 — is a one-line change rather than a new decision.
REQUIRED_READS = {
    "store": frozenset({"get_setting", "settings_flag", "get_generation", "ledger_org_ids",
                        "documents_org_ids", "ledger_stats"}),
    "papra": frozenset(),
    "kreuzakt": frozenset({"health", "queue_counts"}),
}


def _surface(dep: object) -> set[str]:
    """Every name this object answers to, without asking it for one: instance dict plus classes.

    Deliberately not `hasattr` — a double that fabricates attributes in `__getattr__` answers
    `hasattr(x, "update_document")` with True for every name in the language, and a capability check
    that consults the object under test is a check the object can win. Fabricators are refused a few
    lines below, so this stays exact.
    """
    names = set(getattr(dep, "__dict__", {}))
    for klass in type(dep).__mro__:
        if klass is not object:
            names.update(vars(klass))
    return names


def _fabricates_attributes(dep: object) -> str | None:
    """The class that invents attributes on demand, or None.

    A proxy fake is unlimited capability wearing a name: it answers any method, so neither this
    module nor a reader can tell what a tool body could reach through it. Refusing it is the only
    honest answer — the alternative is a check that passes because the fake was generous.
    """
    for klass in type(dep).__mro__:
        if klass is not object and ("__getattr__" in vars(klass)
                                    or "__getattribute__" in vars(klass)):
            return klass.__name__
    return None


def require_read_only(dep: object, *, role: str) -> None:
    """Refuse a dependency that can write, or that cannot answer what the handlers ask.

    Structural rather than `isinstance(dep, SomeReader)`, so a test double that exposes only reads
    still builds — and so the check does not have to be updated every time a view gains a private
    helper. What it refuses is capability: the mutable classes outright, and anything else carrying
    one of the write names.
    """
    if isinstance(dep, Store | PapraClient | KreuzaktClient):
        raise McpContainmentError(
            f"the mcp listener's {role} must be a read-only view, got "
            f"{type(dep).__name__} — that class can write"
            + (" and migrate the schema" if isinstance(dep, Store) else "")
            + "; open a view from papra_suite.readers instead")
    fabricator = _fabricates_attributes(dep)
    if fabricator is not None:
        raise McpContainmentError(
            f"the mcp listener's {role} is a {fabricator} that invents attributes on demand, so "
            "nothing can prove it has no write surface; use a double that names its reads")
    writes = sorted(name for name in FORBIDDEN_WRITE_NAMES if name in _surface(dep))
    if writes:
        raise McpContainmentError(
            f"the mcp listener's {role} exposes write method(s) {', '.join(writes)}; it would "
            f"let a tool call mutate Papra or the ledger. Expose reads on a view in "
            f"papra_suite.readers instead")
    missing = sorted(name for name in REQUIRED_READS[role] if name not in _surface(dep))
    if missing:
        raise McpContainmentError(
            f"the mcp listener's {role} cannot answer what the handlers ask: "
            f"{', '.join(missing)}")


class _GetOnlyClient:
    """An `httpx.AsyncClient` that answers `GET` and refuses every other verb.

    This is the second of the two doors on the HTTP views, and the one that closes the argument
    rather than the loophole: the view's own `_request` already refuses a write, but a caller that
    digs for the transport would otherwise find the ordinary client underneath. With this in front
    of it there is no path — not even `_client.request("PATCH", …)` — that reaches Papra or Kreuzakt
    with a verb that changes anything.
    """

    def __init__(self, client: object) -> None:
        self._client = client

    async def request(self, method: str, *args: object, **kwargs: object):
        if str(method).upper() != "GET":
            raise ReadOnlyViolation(
                f"{method} is not a read: these views issue GET and nothing else")
        return await self._client.request(method, *args, **kwargs)  # type: ignore[attr-defined]

    async def aclose(self) -> None:
        await self._client.aclose()   # type: ignore[attr-defined]


class LedgerReader:
    """The ledger, as the request path is allowed to see it: reads only, on its own connection.

    Not a `Store`. It is built out of `Store`'s own read functions — assigned, not copied — so the
    numbers it returns are the ones `papra-suite check` and `ledger --status` print, and so a write
    cannot arrive here by somebody adding one to `Store` and forgetting this file. The method set
    below is the whole surface; `tests/test_mcp_read_containment.py` compares it against a list, so
    growing it is a deliberate act.

    Threading: handlers reach the store through `asyncio.to_thread`, so the thread that arrives is
    not the one that opened the connection. `check_same_thread=False` with every access inside
    `self._lock` is what `Store` does for the same reason, and it is the only reason this connection
    is safe to hand to a request path.
    """

    # --- the whitelist: `Store`'s read functions, named one by one -------------------------
    get_setting = Store.get_setting
    settings_flag = Store.settings_flag
    table_columns = Store.table_columns
    get_generation = Store.get_generation
    ledger_org_ids = Store.ledger_org_ids
    documents_org_ids = Store.documents_org_ids
    ledger_stats = Store.ledger_stats
    # `get_generation` reads through this pair, and they are reads too; they stay private so the
    # public surface stays the seven names above.
    _read_generation_or_none = Store._read_generation_or_none
    _read_generation = Store._read_generation

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise LedgerSchemaMissing(
                f"no ledger at {self.db_path}: the mcp listener reads the state database, it never "
                "creates it. Run `papra-suite check` (or start `serve`) to migrate this DATA_DIR "
                "first")
        self._lock = threading.Lock()
        # `check_same_thread=False` because the handler runs on a pool thread (`asyncio.to_thread`),
        # and the pool does not promise which one. The lock is the other half of that sentence.
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        try:
            self._conn.row_factory = sqlite3.Row
            # The containment itself. Deliberately not `?mode=ro`: see this module's docstring.
            self._conn.execute("PRAGMA query_only=ON")
            self._conn.execute(f"PRAGMA busy_timeout={READER_BUSY_TIMEOUT_MS}")
            self._require_schema()
        except BaseException:
            self._conn.close()
            raise

    def _require_schema(self) -> None:
        """Refuse a database this view cannot read, before it answers anything from it."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        present = {row["name"] for row in rows}
        missing = [name for name in READER_SCHEMA_TABLES if name not in present]
        if missing:
            raise LedgerSchemaMissing(
                f"the ledger at {self.db_path} has no {', '.join(missing)} table, so this listener "
                "would report an empty archive rather than a missing schema. It never migrates — "
                "schema ownership is `serve`'s and the CLI's — so run `papra-suite check` on this "
                "DATA_DIR first")

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class PapraReader:
    """Papra as the request path is allowed to see it: five GETs and no way to ask for more.

    The five reads are `PapraClient`'s own functions, assigned — the wire parsing stays a single
    implementation, so a document read through this view is the same shape the pipeline reads and a
    field Papra renamed would break both, not just the agent-facing one. The writes
    (`create_document`, `update_document`, `update_content`, `add_tag`) are not inherited, not
    re-implemented, and not reachable: `_request` refuses anything but `GET`, and the transport is
    `_GetOnlyClient`, which refuses it again.

    The `PAPRA_API_KEY` this is built with is the deployment's read-only-scoped key. That scoping is
    Papra's setting and this program cannot verify it from here; what this class can guarantee is
    that *our* side never asks for a write, whatever the key would allow.
    """

    _dispatch = PapraClient._request

    get_document = PapraClient.get_document
    list_documents = PapraClient.list_documents
    download_file = PapraClient.download_file
    list_tags = PapraClient.list_tags
    whoami = PapraClient.whoami

    def __init__(self, base_url: str, api_key: str, *, timeout_s: float = 30.0,
                 backoff_base_s: float = 0.5, transport: object | None = None) -> None:
        PapraClient.__init__(self, base_url, api_key, timeout_s=timeout_s,
                             backoff_base_s=backoff_base_s, transport=transport)
        # Wrapped after construction, so nothing in this process can hand a mutating verb to the
        # real client through this object — not even by reaching for `_client`.
        self._client = _GetOnlyClient(self._client)

    async def _request(self, method: str, path: str, **kwargs) -> object:
        if str(method).upper() != "GET":
            raise ReadOnlyViolation(
                f"{method} {path} is not a read: papra_suite.readers.PapraReader issues GET and "
                "nothing else, because nothing reached from an mcp tool call may modify Papra")
        return await self._dispatch(method, path, **kwargs)

    async def aclose(self) -> None:
        await self._client.aclose()


class KreuzaktReader:
    """The index, read-only: the two calls `index_status` makes, plus what Task 14 will need.

    `upload`, `delete_document` and `retry_queue` are the writes, and they are absent here in the
    same way `update_document` is absent from `PapraReader`. `wait_for_queue_row` is not a write,
    but it is not exposed either: it exists to correlate an upload with its queue row, and this view
    cannot upload — exposing the correlation loop would be a method that can only ever be used by
    the half of the system that writes.

    Why these are `KreuzaktClient`'s functions rather than a second implementation: the envelope
    checks (a `counts` block that must already be integers, a `/api/health` body that must carry
    `status`, a `page` that is 1-based while `offset` is ignored upstream) are each a measured
    behaviour from the pinned container. A copy of them would drift, and the drift would look like
    the index being healthy.
    """

    _dispatch = KreuzaktClient._request
    _get_json = KreuzaktClient._get_json

    health = KreuzaktClient.health
    queue_counts = KreuzaktClient.queue_counts
    queue_page = KreuzaktClient.queue_page
    queue_high_water_mark = KreuzaktClient.queue_high_water_mark
    get_document = KreuzaktClient.get_document
    search = KreuzaktClient.search
    list_documents = KreuzaktClient.list_documents

    def __init__(self, base_url: str, *, timeout_s: float = 30.0,
                 transport: object | None = None) -> None:
        KreuzaktClient.__init__(self, base_url, timeout_s=timeout_s, transport=transport)
        self._client = _GetOnlyClient(self._client)

    async def _request(self, method: str, path: str, **kwargs) -> object:
        if str(method).upper() != "GET":
            raise ReadOnlyViolation(
                f"{method} {path} is not a read: papra_suite.readers.KreuzaktReader issues GET and "
                "nothing else, because nothing reached from an mcp tool call may touch the index")
        return await self._dispatch(method, path, **kwargs)

    async def aclose(self) -> None:
        await self._client.aclose()
