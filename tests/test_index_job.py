"""The `index` job kind: upload, correlate, bind the id, and write nothing to Papra.

Two things make this module's tests unlike the OCR ones.

**The Kreuzakt double is an HTTP double, not a method double.** `FakeKreuzakt`
serves scripted bodies through `httpx.MockTransport` behind the REAL
`KreuzaktClient`, so `wait_for_queue_row`'s correlation — the high-water mark, the
` (N)` collision suffix, the saturated window, the ambiguity, the completed row
with a null id — runs as product code inside every test here. A double that
restated `wait_for_queue_row` would let `run_index_job` pass against rules the
real client does not implement, and the two would diverge in production.

**Every test asserts Papra was not written — while the delivery gate is shut.** An
autouse fixture fails any test whose fake Papra recorded a PATCH with
`index_content_delivery` off: this was the last phase where a bridge bug cannot corrupt
the record, and "we do not write yet" had to be a mechanism rather than a review note.
Task 7 put that gate inside `deliver_content`, so the invariant is gate-scoped now: the
one test here that opens the gate says so in its name, and the matrix of what may be
written lives in `tests/test_content_delivery.py`.
"""

import hashlib
import json
import logging
import posixpath
import re
from dataclasses import asdict
from pathlib import Path

import httpx
import pytest

from papra_suite import indexing
from papra_suite.indexing import IndexFailed, IndexTimeout, index_upload_name, run_index_job
from papra_suite.kreuzakt_client import (
    AmbiguousCorrelation,
    KreuzaktClient,
    KreuzaktError,
    KzDocument,
)
from papra_suite.papra_client import PapraError
from papra_suite.store import Job, Store

ORG = "org_1"
DOC_ID = "doc-1"
# The bytes Papra serves, and the hash of exactly those bytes. `content_hash` is a
# sha256 of Papra's original file in production too — the sweep takes it from Papra's
# own `originalSha256Hash`, the mail path computes it with `compute_file_hash`, and the
# sweep's fallback hashes the download — so the double has to be honest about it or
# `download_original`'s verification check would be tested against a fiction.
PDF_BYTES = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\ntrailer\n%%EOF"
HASH = hashlib.sha256(PDF_BYTES).hexdigest()
NAME = "scan-0001.pdf"
UPLOAD_NAME = f"papra-{DOC_ID}-{HASH[:8]}.pdf"


def make_job(*, store=None, kind="index", org=ORG, document_id=DOC_ID,
             content_hash=HASH, attempts=0) -> Job:
    """A job row — persisted when a store is given, plain otherwise.

    The handler needs a real row: `store.mark_job_failed(job.id, ...)` and the
    job's own `attempts` are part of the retry contract, so a bare dataclass
    cannot stand in for the queued job in those tests.
    """
    if store is not None:
        job_id = store.enqueue_job(
            kind=kind, papra_org_id=org, document_id=document_id,
            content_hash=content_hash,
            payload={"file_hash": content_hash, "name": NAME},
        )
        return store.get_job(job_id)
    return Job(id="job-1", kind=kind, papra_org_id=org, document_id=document_id,
               content_hash=content_hash,
               payload={"file_hash": content_hash, "name": NAME},
               status="pending", attempts=attempts, next_attempt_at="2026-11-17 00:00:00",
               error=None, created_at="2026-11-17 00:00:00",
               updated_at="2026-11-17 00:00:00")


# --- the upload name -------------------------------------------------------------

def test_upload_name_binds_document_and_hash():
    """The name is the only thing that ties a queue row back to a Papra document."""
    assert index_upload_name(make_job(document_id="doc-1", content_hash="a1b2c3d4e5")) \
        == "papra-doc-1-a1b2c3d4.pdf"


@pytest.mark.parametrize("document_id", [
    "papra:doc0005<schräg>.pdf",       # the probe's own name: came back sanitised
    "Papra Doc 0005.pdf",              # spaces and capitals
    "org/../../etc/passwd",            # path separators
    "ré Rechnung №7",                  # non-ASCII
    "a" * 300,                         # a name with nothing legal to keep
    "1.5-2_3",                         # an underscore
])
def test_upload_name_emits_only_the_safe_alphabet(document_id):
    """`[a-z0-9.-]` plus `.pdf`, asserted rather than trusted.

    Measured: a name outside that alphabet comes back sanitised
    (`papra:doc0005<…>.pdf` → `papra_doc0005_…_.pdf`), and `_is_our_row` matches
    by exact equality on purpose, so a sanitised name means our own row is
    unmatchable and the job reports absence for a document it did upload.
    """
    name = index_upload_name(make_job(document_id=document_id))
    assert re.fullmatch(r"[a-z0-9.-]+\.pdf", name), name
    assert not name.startswith("-")           # never reads as a CLI flag or an option
    assert ".." not in name                   # no traversal segment survives the collapse
    assert len(name) <= 128, name             # one filesystem path component


def test_upload_name_of_an_overlong_id_stays_unique():
    """A name that has to shrink must shrink to a digest, not to a prefix.

    Truncation would put two documents that share a long prefix on one upload name,
    and Kreuzakt's dedup would then make the second one index the first one's
    bytes — a wrong document behind a right-looking ledger row.
    """
    shared = "d" * 200
    first = index_upload_name(make_job(document_id=shared + "-1"))
    second = index_upload_name(make_job(document_id=shared + "-2"))
    assert first != second
    assert len(first) <= 128 and len(second) <= 128
    assert first == index_upload_name(make_job(document_id=shared + "-1"))


# --- the doubles ------------------------------------------------------------------

FIXTURES = Path(__file__).parent / "fixtures"
QUEUE_ERROR = ("Parsing error: xberg_native_pdf: failed to load bytes: "
               "Invalid cross-reference table")


class FakePapra:
    """Papra's four calls this phase makes, with every write counted.

    `patched` is the assertion the whole phase rests on: it must stay empty.
    `document` is the live dict (tests move the record's state with it), while
    `get_document` hands out a copy, because a real client cannot hand out the
    server's own object and a handler that mutated it would pass here and corrupt
    nothing only because HTTP is in the way.
    """

    def __init__(self):
        self.documents = {(ORG, DOC_ID): {
            "id": DOC_ID, "organizationId": ORG, "name": NAME,
            "mimeType": "application/pdf", "content": "",
            "originalSha256Hash": HASH, "isDeleted": False, "deletedAt": None,
        }}
        self.patched: list[tuple[str, str, dict]] = []
        self.patch_calls = 0
        self.get_calls = 0
        self.download_calls = 0
        self.raise_on_get: Exception | None = None
        # Set to an exception to make a write fail after it was attempted: Task 7's
        # delivery tests need the failure to land *after* the PATCH left, because the
        # property under test is what the ledger does when Papra says no.
        self.raise_on_update: Exception | None = None
        self.closed = False
        # The bytes the file endpoint serves, separate from the metadata on purpose:
        # `download_original` verifies them against the job's hash, and the race that
        # check exists for is exactly the one where the metadata the job was queued on
        # is still the old one while the body has already been replaced (or the
        # transfer stopped short). `replace_body` moves the bytes and nothing else.
        self.body = PDF_BYTES

    @property
    def document(self) -> dict:
        return self.documents[(ORG, DOC_ID)]

    async def get_document(self, org_id: str, document_id: str) -> dict:
        self.get_calls += 1
        if self.raise_on_get is not None:
            raise self.raise_on_get
        document = self.documents.get((org_id, document_id))
        if document is None:
            raise PapraError(f"GET document failed: 404 {document_id} not found", 404)
        return dict(document)

    async def download_file(self, org_id: str, document_id: str) -> bytes:
        self.download_calls += 1
        if (org_id, document_id) not in self.documents:
            raise PapraError(f"GET file failed: 404 {document_id} not found", 404)
        return self.body

    def replace_body(self, blob: bytes) -> bytes:
        """Change the served bytes, leaving the record's metadata where it is.

        That is the whole point: the job's hash came from the listing, the body moved
        underneath it, and only the download can find out.
        """
        self.body = blob
        return blob

    async def update_document(self, org_id: str, document_id: str, **body) -> dict:
        self.patch_calls += 1
        # Recorded before the failure is raised on purpose: `patched` is "every write
        # this bridge attempted", and a delivery that reached Papra and was refused has
        # to be visible to the test that is checking exactly that.
        self.patched.append((org_id, document_id, body))
        if self.raise_on_update is not None:
            raise self.raise_on_update
        self.documents[(org_id, document_id)].update(body)
        return self.documents[(org_id, document_id)]

    async def update_content(self, org_id: str, document_id: str, content: str) -> None:
        """Mirrors `PapraClient.update_content`, which routes through `update_document`.

        Delegating rather than recording separately keeps one counter set for both
        shapes of write, so a test that asserts "only `content` was sent" asserts it
        about the body that would actually go over the wire.
        """
        await self.update_document(org_id, document_id, content=content)

    def delete(self, org_id: str = ORG, document_id: str = DOC_ID) -> None:
        """The user deleted it: Papra answers 404 for the detail endpoint."""
        self.documents.pop((org_id, document_id), None)

    async def aclose(self) -> None:
        self.closed = True


class FakeKreuzakt:
    """A scripted Kreuzakt behind the REAL client.

    The transport answers HTTP; `KreuzaktClient` does the parsing, the correlation
    and the typed failures. That is deliberate: the rules this handler has to obey
    are enforced by the client, so a test double for the client would be testing a
    client that does not exist. Bodies come from the captured probe fixtures
    (`kreuzakt_queue_page.json`, `kreuzakt_document.json`, `kreuzakt_retry_*.json`)
    rather than from imagination.

    Knobs, all off by default:

    * `queue_error` — uploaded rows come back `failed` with that text, and stay
      failed with the identical text when re-run (measured deterministic failure).
    * `fail_with_document_id` — the failed row also carries an id, so a handler
      that reads `document_id` instead of `status` would bind it.
    * `documents_vanish` — rows complete with `document_id: null`, which is what
      deleting the document leaves behind.
    * `duplicate_our_rows` — one upload produces two terminal rows for our name.
    * `never_completes` — the upload is accepted and no row ever appears.
    * `retry_recovers` — re-running a failed row makes it complete.
    * `window_saturated_once` / `window_saturated_always` — the newest-100 window
      holds only unrelated newer rows, hiding ours.
    * `rename_uploads` — the server stores under another name than we asked.
    * `on_wait` — fired on every `/api/queue` read, for mid-flight races.
    * `on_upload` — fired on every upload, for "the request itself took too long".
    """

    def __init__(self):
        page = json.loads((FIXTURES / "kreuzakt_queue_page.json").read_text())
        self.rows: list[dict] = [dict(row) for row in page["entries"]]
        self.counts: dict = dict(page["counts"])
        self._next_row_id = max(row["id"] for row in self.rows) + 1
        self.documents: dict[int, dict] = {}
        self.next_document_id = 7
        # Ids the read endpoint answers 404 for. Measured, a DELETE nulls the id on
        # every row that carried it, so a completed row naming a document that will
        # not answer is the narrower race — a restart, or a read landing while the
        # index is mid-flight. It is here because the closing `get_document` has to
        # answer a 404 without letting `worker_loop` read that 404 as "a retry cannot
        # help", and a row that names a real document is the honest shape of that.
        self.missing_document_ids: set[int] = set()
        self._blobs: dict[int, bytes] = {}
        self._document_ids: dict[bytes, int] = {}

        self.upload_calls: list[tuple[str, bytes]] = []
        self.retry_calls: list[int] = []
        self.high_water_calls: list[int] = []
        self.deleted: list[int] = []
        self.calls: list[tuple[str, str]] = []
        self.queue_reads = 0

        self.queue_error: str | None = None
        self.fail_with_document_id = False
        self.documents_vanish = False
        self.duplicate_our_rows = False
        self.stuck_row = False
        self.never_completes = False
        self.retry_recovers = False
        self.window_saturated_once = False
        self.window_saturated_always = False
        self.rename_uploads: str | None = None
        self.on_wait = None
        self.on_upload = None

        document = json.loads((FIXTURES / "kreuzakt_document.json").read_text())
        self._document_template = {**document, "original_filename": UPLOAD_NAME}
        self._scripted_id = self.next_document_id
        self.document = {**self._document_template, "id": self.next_document_id}
        self.client = KreuzaktClient("http://kreuzakt.test",
                                     transport=httpx.MockTransport(self._handle))

    # -- the surface the handler uses, delegated to the real client ---------------

    async def queue_high_water_mark(self) -> int:
        mark = await self.client.queue_high_water_mark()
        self.high_water_calls.append(mark)
        return mark

    async def upload(self, filename: str, content: bytes) -> str:
        return await self.client.upload(filename, content)

    async def wait_for_queue_row(self, filename, *, after_id, poll_s, timeout_s):
        return await self.client.wait_for_queue_row(filename, after_id=after_id,
                                                    poll_s=poll_s, timeout_s=timeout_s)

    async def get_document(self, document_id: int):
        return await self.client.get_document(document_id)

    async def retry_queue(self, entry_id: int) -> None:
        await self.client.retry_queue(entry_id)

    async def health(self) -> dict:
        return await self.client.health()

    async def aclose(self) -> None:
        await self.client.aclose()

    # -- the scripted server ------------------------------------------------------

    @property
    def high_water(self) -> int:
        return max((row["id"] for row in self.rows), default=0)

    @property
    def document(self) -> dict:
        """The document a test scripted, or the one the fixture scripted for id 7."""
        return self.documents.get(self._scripted_id, self._document_template)

    @document.setter
    def document(self, value) -> None:
        body = value if isinstance(value, dict) else asdict(value)
        self._scripted_id = body["id"]
        self.documents[body["id"]] = {**body}

    def _handle(self, request: httpx.Request) -> httpx.Response:
        route = request.url.path
        self.calls.append((request.method, route))
        if route == "/api/health":
            return self._json(json.loads((FIXTURES / "kreuzakt_health.json").read_text()))
        if request.method == "GET" and route == "/api/queue":
            return self._queue(request)
        if request.method == "POST" and route == "/api/upload":
            return self._upload(request)
        retry = re.fullmatch(r"/api/queue/(\d+)/retry", route)
        if request.method == "POST" and retry:
            return self._retry(int(retry[1]))
        document = re.fullmatch(r"/api/documents/(\d+)", route)
        if request.method == "GET" and document:
            return self._document(int(document[1]))
        if request.method == "DELETE" and document:
            return self._delete(int(document[1]))
        return self._json({"error": "not_found",
                           "message": f"no fake route: {request.method} {route}"},
                          status=404)

    @staticmethod
    def _json(body: dict, status: int = 200) -> httpx.Response:
        return httpx.Response(status, json=body)

    def _queue(self, request: httpx.Request) -> httpx.Response:
        self.queue_reads += 1
        limit = int(request.url.params.get("limit", "20"))
        if limit >= 100:
            # `on_wait` fires inside the correlation wait, not on the one-row
            # high-water read, so a test's mid-flight race lands where a real race
            # can land: while we are looking for our row.
            if self.on_wait is not None:
                self.on_wait()
        if limit >= 100 and (self.window_saturated_always or self.window_saturated_once):
            self.window_saturated_once = False
            # A `reindex-all` from another writer fills the window: 100 rows, all
            # newer than our mark, none of them ours.
            filler = [{"id": 1000 + n, "filename": f"reindex-all:batch:{n}:x.pdf",
                       "status": "completed", "error": None, "document_id": None,
                       "created_at": "2026-11-17 21:00:00", "completed_at": None,
                       "page_count": None} for n in range(100)]
            return self._json({"entries": filler, "counts": self.counts})
        rows = sorted(self.rows, key=lambda row: row["id"], reverse=True)[:limit]
        return self._json({"entries": [dict(row) for row in rows], "counts": self.counts})

    def _upload(self, request: httpx.Request) -> httpx.Response:
        if self.on_upload is not None:
            self.on_upload()
        name, blob = _parse_multipart(request)
        self.upload_calls.append((name, blob))
        stored = self.rename_uploads or self._stored_name(name)
        if self.never_completes:
            # Accepted, and no row appears at all: the only state in which absence
            # from the window is actually observable.
            return self._json(_upload_body(stored, name))
        self._add_row(stored, blob)
        if self.duplicate_our_rows:
            self._add_row(stored, blob)
        return self._json(_upload_body(stored, name))

    def _stored_name(self, name: str) -> str:
        """Kreuzakt keeps the name and adds ` (N)` when it is already taken."""
        taken = {row["filename"] for row in self.rows}
        if name not in taken:
            return name
        stem, extension = posixpath.splitext(name)
        counter = 1
        while f"{stem} ({counter}){extension}" in taken:
            counter += 1
        return f"{stem} ({counter}){extension}"

    def _add_row(self, filename: str, blob: bytes) -> dict:
        if self.queue_error is not None:
            status, error = "failed", self.queue_error
            document_id = 99 if self.fail_with_document_id else None
        elif self.documents_vanish:
            status, error, document_id = "completed", None, None
        elif self.stuck_row:
            # Accepted and working, forever: the row exists and never answers.
            status, error, document_id = "processing", None, None
        else:
            status, error, document_id = "completed", None, self._document_for(blob)
        row = {"id": self._next_row_id, "filename": filename, "status": status,
               "error": error, "document_id": document_id,
               "created_at": "2026-11-17 21:00:00", "completed_at": "2026-11-17T21:00:02.000Z",
               "page_count": 1 if document_id else None}
        self._next_row_id += 1
        self.rows.append(row)
        self._blobs[row["id"]] = blob
        return row

    def _document_for(self, blob: bytes) -> int:
        """Identical bytes are one document — the fact that makes re-upload cheap.

        A document that has been deleted is not that fact any more: the next upload
        mints a new id, and it is the mint, not the replay of 7 forever, that lets a
        test show a stale binding being replaced rather than confirmed. The scripted
        body rides along so the closing read has something to return.
        """
        if blob not in self._document_ids:
            document_id = self.next_document_id
            self.next_document_id += 1
            self._document_ids[blob] = document_id
            self.documents.setdefault(document_id,
                                      {**self._document_template, "id": document_id})
        return self._document_ids[blob]

    def _retry(self, entry_id: int) -> httpx.Response:
        row = next((row for row in self.rows if row["id"] == entry_id), None)
        if row is None:
            body = json.loads((FIXTURES / "kreuzakt_retry_unknown.json").read_text())
            body["message"] = f"Queue entry {entry_id} not found"
            return self._json(body, status=404)
        if row["status"] != "failed":
            # Verbatim from the capture: re-running anything but a failed row is a
            # 400, so a handler that retries a completed row fails loudly here.
            return self._json({"error": "bad_request",
                               "message": f"Queue entry {entry_id} is not in failed status"},
                              status=400)
        self.retry_calls.append(entry_id)
        if self.retry_recovers:
            row["status"] = "completed"
            row["error"] = None
            row["document_id"] = self._document_for(self._blobs.get(entry_id, b""))
        body = json.loads((FIXTURES / "kreuzakt_retry_requeued.json").read_text())
        return self._json({**body, "id": entry_id, "filename": row["filename"]})

    def _document(self, document_id: int) -> httpx.Response:
        body = None if document_id in self.missing_document_ids \
            else self.documents.get(document_id)
        if body is None:
            return self._json({"error": "not_found",
                               "message": f"Document {document_id} not found"}, status=404)
        return self._json({**body, "id": document_id})

    def _delete(self, document_id: int) -> httpx.Response:
        self.deleted.append(document_id)
        self.documents.pop(document_id, None)
        # The measured side effect: every row that pointed here keeps its status
        # and loses its id. And the bytes stop being deduplicated onto a document
        # that is gone: the next upload of them is a new document with a new id,
        # which is what makes an id we already stored a stale one.
        self._document_ids = {blob: doc for blob, doc in self._document_ids.items()
                              if doc != document_id}
        for row in self.rows:
            if row["document_id"] == document_id:
                row["document_id"] = None
        return httpx.Response(204)


def _upload_body(stored: str, original: str) -> dict:
    body = json.loads((FIXTURES / "kreuzakt_upload_response.json").read_text())
    return {**body, "files": [{**body["files"][0], "originalFilename": original,
                               "relativePath": stored, "storedFilename": stored}]}


def _parse_multipart(request: httpx.Request) -> tuple[str, bytes]:
    """The one file part of the upload: its name and its bytes."""
    body = request.read()
    boundary = request.headers["content-type"].split("boundary=")[1].encode()
    name = re.search(rb'filename="([^"]*)"', body).group(1).decode()
    start = body.index(b"\r\n\r\n") + 4
    end = body.rindex(b"\r\n--" + boundary)
    return name, body[start:end]


class FakeClock:
    """A monotonic clock the test advances on purpose.

    The wait budget is wall-clock arithmetic, and a test that waits in real time to
    prove a timeout is a slow test that still cannot prove the arithmetic: this makes
    "the download ate the budget" a fact the test sets rather than races.
    """

    def __init__(self, now: float = 0.0):
        self.now_value = now
        self.reads = 0

    def now(self) -> float:
        self.reads += 1
        return self.now_value

    def advance(self, seconds: float) -> None:
        self.now_value += seconds


def seed_ledger(store, *, org=ORG, document_id=DOC_ID, file_hash=HASH, name=NAME,
                kreuzakt_id: int | None = None, generation: int = 1) -> None:
    """A ledger row as intake would have left it, optionally bound to an id."""
    store.ledger_upsert_reference(papra_org_id=org, papra_document_id=document_id,
                                  file_hash=file_hash, papra_original_name=name)
    if kreuzakt_id is not None:
        store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=document_id,
                                     kreuzakt_id=kreuzakt_id, generation=generation)


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=ORG, name="Org 1")
    yield store
    store.close()


@pytest.fixture
def fake_papra():
    return FakePapra()


@pytest.fixture
async def fake_kz():
    fake = FakeKreuzakt()
    yield fake
    await fake.aclose()


@pytest.fixture
def job(store):
    return make_job(store=store)


@pytest.fixture(autouse=True)
def papra_is_never_written(fake_papra, store):
    """The phase invariant, scoped to the gate now that a write path exists.

    Task 6 shipped with no write path at all, so any PATCH was a bug whatever the
    settings said. Task 7 moved the gate inside `deliver_content`, which makes the
    honest invariant "nothing reaches Papra while `index_content_delivery` is off" —
    and off is what a fresh store says, so every test in this file that does not open
    the gate is still pinned against a write. A test that opens it is making a delivery
    claim, and the write itself is what `tests/test_content_delivery.py` asserts.
    """
    yield
    if store.settings_flag("index_content_delivery"):
        return
    assert fake_papra.patched == [], (
        f"the gate was shut and Papra was written: {fake_papra.patched}")
    assert fake_papra.patch_calls == 0


# --- the happy path, and what it may not touch ------------------------------------

async def test_index_records_the_binding_and_writes_nothing_to_papra(store, job,
                                                                     fake_papra, fake_kz):
    """The last phase where a bridge bug cannot corrupt the record."""
    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    assert (outcome.kreuzakt_id, outcome.empty_content, outcome.delivered) == (
        7, False, False)
    assert outcome.content_len == len(fake_kz.document["content"])
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.kreuzakt_id == 7 and ref.delivered_generation == store.get_generation()
    assert ref.papra_original_name == NAME and ref.file_hash == HASH

    assert fake_papra.patched == [] and fake_papra.patch_calls == 0
    assert fake_kz.upload_calls == [(index_upload_name(job), PDF_BYTES)]
    # Serial correlation (measured rule 1): mark, upload, correlate, in that order,
    # with one upload outstanding — not a batch that correlates afterwards.
    assert fake_kz.calls[:4] == [
        ("GET", "/api/queue"), ("POST", "/api/upload"),
        ("GET", "/api/queue"), ("GET", "/api/documents/7")]
    assert fake_kz.high_water_calls == [35]


async def test_first_sight_keeps_the_rename_baseline_from_papra(store, job,
                                                                fake_papra, fake_kz):
    """The Papra-side name at first sight is the baseline Task 8 renames against."""
    await run_index_job(fake_kz, store, job, papra=fake_papra, poll_s=0, wait_timeout_s=5)
    assert store.ledger_get(ORG, DOC_ID).papra_original_name == NAME


async def test_a_document_papra_names_nothing_gets_no_baseline(store, job,
                                                               fake_papra, fake_kz):
    """An empty name stays unknown rather than becoming a baseline that matches."""
    fake_papra.document["name"] = ""
    await run_index_job(fake_kz, store, job, papra=fake_papra, poll_s=0, wait_timeout_s=5)
    assert store.ledger_get(ORG, DOC_ID).papra_original_name is None


# --- abandoning a job whose source moved ------------------------------------------

async def test_stale_job_hash_is_abandoned_not_delivered(store, job, fake_papra, fake_kz):
    """Source changed while the job waited: our bytes are not the record's bytes."""
    seed_ledger(store)
    fake_papra.document["originalSha256Hash"] = "different"

    with pytest.raises(IndexFailed, match="source hash changed") as exc:
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None
    assert fake_kz.upload_calls == [] and fake_papra.download_calls == 0
    assert exc.value.permanent is True      # a newer job covers the newer bytes


async def test_document_deleted_in_papra_is_left_to_the_deletion_pass(store, job,
                                                                      fake_papra,
                                                                      fake_kz):
    seed_ledger(store)
    fake_papra.document["isDeleted"] = True

    with pytest.raises(IndexFailed, match="document deleted in papra") as exc:
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert exc.value.permanent is True      # the deletion pass owns it; retry cannot help
    assert fake_kz.upload_calls == []
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None


# --- the four measured queue outcomes ---------------------------------------------

async def test_a_completed_row_without_a_document_id_reuploads_then_says_vanished(
        store, job, fake_papra, fake_kz):
    """Measured: deleting the Kreuzakt document leaves the row `completed`, id null.

    Retrying it is impossible (`retry` accepts only `failed`), so the branch reads
    Papra. Papra still holds the document here, so the recovery is a re-upload —
    bounded, and then the failure names the condition rather than a guess.
    """
    fake_kz.documents_vanish = True

    with pytest.raises(IndexFailed, match="index document vanished") as exc:
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert len(fake_kz.upload_calls) == 3           # bounded by index_max_attempts
    assert fake_kz.retry_calls == []                # a completed row cannot be re-run
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None
    assert "papra still holds" in str(exc.value)    # the half the operator can act on


@pytest.mark.parametrize("mode", ["deleted_flag", "gone", "unreachable"])
async def test_a_vanished_document_is_only_ours_to_forget_when_papra_forgot_it(
        store, job, fake_papra, fake_kz, mode):
    """Papra is the record, so Papra decides whether a vanished id is final.

    The deletion happens between the job's first read and the queue row that says
    "no document": a race the ledger must not paper over by re-uploading a
    document the user has already thrown away.
    """
    fake_kz.documents_vanish = True
    fake_kz.on_wait = lambda: _forget_in_papra(fake_papra, mode)

    if mode == "unreachable":
        with pytest.raises(PapraError, match="503"):
            await run_index_job(fake_kz, store, job, papra=fake_papra,
                                poll_s=0, wait_timeout_s=5)
    else:
        with pytest.raises(IndexFailed, match="index document vanished") as exc:
            await run_index_job(fake_kz, store, job, papra=fake_papra,
                                poll_s=0, wait_timeout_s=5)
        assert exc.value.permanent is True          # Task 9's cleanup path takes it
        assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None
    assert len(fake_kz.upload_calls) == 1           # no re-upload of a deleted document


def _forget_in_papra(fake_papra: FakePapra, mode: str) -> None:
    if mode == "deleted_flag":
        fake_papra.document["isDeleted"] = True
    elif mode == "gone":
        fake_papra.delete()
    else:
        fake_papra.raise_on_get = PapraError("GET document failed: 503 upstream", 503)


async def test_window_saturated_reuploads_then_succeeds(store, job, fake_papra, fake_kz):
    """First attempt sees 100 unrelated rows newer than our mark; the retry re-uploads."""
    fake_kz.window_saturated_once = True

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    assert outcome.kreuzakt_id == 7
    assert len(fake_kz.upload_calls) == 2         # a re-upload, not another read
    assert fake_kz.high_water_calls == [35, 36]   # a fresh mark for the fresh row
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id == 7


async def test_ambiguous_correlation_is_reported_not_guessed(store, job, fake_papra,
                                                             fake_kz):
    """Two terminal rows for one name: picking either attributes a wrong document."""
    fake_kz.duplicate_our_rows = True

    with pytest.raises(KreuzaktError, match="ambiguous") as exc:
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert exc.type is AmbiguousCorrelation       # the client's own reason survives
    assert len(fake_kz.upload_calls) == 3         # retried by re-uploading, then bounded
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None


async def test_queue_timeout_raises_so_the_job_retries_with_backoff(store, job,
                                                                    fake_papra, fake_kz):
    fake_kz.never_completes = True

    with pytest.raises(KreuzaktError, match="no queue row"):
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=0.05)

    assert len(fake_kz.upload_calls) == 1   # a timeout is not a re-upload trigger
    store.mark_job_failed(job.id, "no queue row within 0.05s")   # what the worker does
    claimed = store.get_job(job.id)
    assert claimed.status == "pending" and claimed.attempts == 1  # retriable, not dead
    assert claimed.next_attempt_at > claimed.updated_at


async def test_a_row_still_working_is_not_reported_as_absent(store, job,
                                                             fake_papra, fake_kz):
    """"Not finished yet" and "never appeared" are different answers about different facts."""
    fake_kz.stuck_row = True

    with pytest.raises(KreuzaktError, match="no terminal queue row"):
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=0.05)

    assert len(fake_kz.upload_calls) == 1
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None


# --- failures of the document itself ----------------------------------------------

async def test_deterministic_failure_stops_retrying_after_the_budget(store, job,
                                                                      fake_papra, fake_kz):
    """The probe's corrupt PDF came back with identical text every single time."""
    fake_kz.queue_error = QUEUE_ERROR

    with pytest.raises(IndexFailed, match="xberg_native_pdf") as exc:
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert QUEUE_ERROR in str(exc.value)              # Kreuzakt's own words, kept
    assert len(fake_kz.upload_calls) == 1             # the row is re-run, not re-uploaded
    assert len(fake_kz.retry_calls) == 1              # identical text means stop here
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None


async def test_transient_queue_failure_is_re_run_on_the_failed_row(store, job,
                                                                   fake_papra, fake_kz):
    fake_kz.queue_error = QUEUE_ERROR
    fake_kz.retry_recovers = True

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    assert outcome.kreuzakt_id == 7
    assert len(fake_kz.upload_calls) == 1
    assert len(fake_kz.retry_calls) == 1     # legal only because the row was `failed`
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id == 7


async def test_a_spent_job_budget_does_not_re_run_the_row(store, fake_papra, fake_kz):
    """The job's own attempt count is the ceiling the queue retry answers to."""
    job = make_job(store=store)
    store.mark_job_failed(job.id, "earlier failure")
    store.mark_job_failed(job.id, "earlier failure again")
    job = store.get_job(job.id)
    assert job.attempts == 2

    fake_kz.queue_error = QUEUE_ERROR
    with pytest.raises(IndexFailed, match="xberg_native_pdf"):
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert fake_kz.retry_calls == []         # no more work spent on a dying job
    assert store.get_job(job.id).attempts == 2


async def test_a_row_that_is_not_completed_is_not_completion(store, job, fake_papra,
                                                             fake_kz):
    """Measured rule 3: a non-null `document_id` proves nothing.

    A `reindex-all` row arrives with its id already set, and a failed row here
    carries one too. Only `status == "completed"` may bind an id, or a document
    Kreuzakt never processes gets a ledger row that resolves forever.
    """
    fake_kz.queue_error = QUEUE_ERROR
    fake_kz.fail_with_document_id = True     # id 99 sits there, ready to be grabbed

    with pytest.raises(IndexFailed, match="queue error"):
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None
    assert fake_kz.deleted == []


# --- the bytes we upload are the bytes we hashed -----------------------------------

@pytest.mark.parametrize("mode", ["replaced", "truncated"])
async def test_bytes_that_are_not_the_jobs_are_never_uploaded(store, job, fake_papra,
                                                              fake_kz, mode):
    """The record's hash is read at job start; the body is read later. Verify the body.

    Without the check, a document replaced between those two reads gets its NEW bytes
    indexed while the ledger stamps the OLD `content_hash` — a wrong document behind a
    right-looking row that never heals, because the id is bound under the current
    generation and the document counts as seen. A short transfer is the same defect
    with a different cause, so both land on the same permanent failure: retrying
    downloads the same wrong bytes.
    """
    blob = (b"%PDF-1.4\n%replaced\ntrailer\n%%EOF" if mode == "replaced"
            else PDF_BYTES[:len(PDF_BYTES) // 2])
    fake_papra.replace_body(blob)
    assert fake_papra.document["originalSha256Hash"] == HASH   # the metadata never moved

    with pytest.raises(IndexFailed, match=f"hash to .* not the {HASH} this job") as exc:
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert exc.value.permanent is True
    # `permanent` is the only trigger this error pulls: it carries no HTTP status for
    # `worker_loop`'s 4xx rule, so the reason and the verdict stay in one place.
    assert getattr(exc.value, "status", None) is None
    assert fake_kz.upload_calls == []             # Kreuzakt never saw the wrong bytes
    assert fake_papra.download_calls == 1
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None


# --- a binding that no longer resolves ---------------------------------------------

async def test_a_stale_binding_is_replaced_by_a_fresh_upload(store, job, fake_papra,
                                                            fake_kz):
    """A bound id is a claim about one index, and that index can stop honouring it.

    Measured: deleting the Kreuzakt document leaves our ledger row pointing at an
    integer that answers 404. That is survivable only if the read is not the end of
    the job — `worker_loop` reads a 4xx as "a retry cannot help", and the row would
    stay bound under the current generation, which is the exact state
    `ledger_needs_index` excludes. So the reuse path re-uploads and rebinds.
    """
    seed_ledger(store, kreuzakt_id=42, generation=1)   # document 42 does not exist here

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    assert outcome.kreuzakt_id == 7
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.kreuzakt_id == 7                       # rebound in place, not left at 42
    assert ref.delivered_generation == store.get_generation()
    assert fake_kz.upload_calls == [(UPLOAD_NAME, PDF_BYTES)]
    assert fake_papra.download_calls == 1             # Papra is the byte source
    assert fake_papra.patch_calls == 0


async def test_a_404_on_a_freshly_bound_id_is_retriable_and_self_heals(store, job,
                                                                       fake_papra,
                                                                       fake_kz):
    """The same 404 on the fresh path must not be the job's last one either.

    Here the bind succeeded and the read that follows it 404s, so the binding is kept
    and the job is re-queued — and while an attempt remains, that is enough: the next
    run finds the document answering again and finishes on the binding it already has,
    with no second upload. The two branches are one recovery, which is why neither may
    report permanence, and why the error carries no `status` for `worker_loop`'s "a 4xx
    means a retry cannot help" rule. What happens when no attempt remains is the test
    below this one.
    """
    fake_kz.missing_document_ids = {7}

    with pytest.raises(IndexFailed, match="answers 404") as exc:
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert exc.value.permanent is False
    assert getattr(exc.value, "status", None) is None
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id == 7       # kept, deliberately

    fake_kz.missing_document_ids = set()   # whatever the index was doing has passed

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    assert outcome.kreuzakt_id == 7
    assert len(fake_kz.upload_calls) == 1  # the binding is what the first run bought


async def test_a_fresh_404_that_outlives_the_attempts_leaves_the_row_stuck(store, job,
                                                                          fake_papra,
                                                                          fake_kz):
    """The one case the retry cannot recover, pinned so it cannot be quietly forgotten.

    A 404 that survives every attempt ends with the job `failed` and the row bound
    under the current generation without the document ever having been read — the
    state `ledger_needs_index` excludes, so neither the sweep nor a rebuild looks at it
    again, and the recovery today is `jobs --reset`. That is the queue's limitation as
    much as this handler's: a failed job also blocks every later enqueue for those
    bytes, which is what the strict xfail in the integration suite holds up for Task
    11's migration. Pinned as a fact rather than as approval — unbind the row or make
    the job re-schedulable and this test has to change on purpose.
    """
    fake_kz.missing_document_ids = {7}

    for _ in range(3):                       # the store's default ceiling on attempts
        with pytest.raises(IndexFailed) as exc:
            await run_index_job(fake_kz, store, job, papra=fake_papra,
                                poll_s=0, wait_timeout_s=5)
        # Retriable every time: the worker decides how the budget is spent, and it is
        # not this error that ends the job — the budget is.
        assert exc.value.permanent is False
        store.mark_job_failed(job.id, str(exc.value))
        job = store.get_job(job.id)

    assert job.status == "failed" and job.attempts == 3
    assert len(fake_kz.upload_calls) == 3    # every attempt tried the recovery
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.kreuzakt_id is not None and ref.delivered_generation == 1
    assert store.ledger_needs_index(generation=1) == []   # and nothing looks again


# --- the generation fence ----------------------------------------------------------

async def test_existing_current_id_skips_upload_entirely(store, job, fake_papra, fake_kz):
    seed_ledger(store, kreuzakt_id=7, generation=1)

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    assert outcome.kreuzakt_id == 7
    assert fake_kz.upload_calls == [] and fake_kz.high_water_calls == []
    assert fake_papra.download_calls == 0    # no bytes move for indexed work


async def test_wrong_generation_reuploads_and_rebinds_id(store, job, fake_papra, fake_kz):
    seed_ledger(store, kreuzakt_id=7, generation=1)
    store.bump_generation()                  # Kreuzakt was rebuilt under us

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.delivered_generation == store.get_generation() == 2
    assert outcome.kreuzakt_id == 7          # same bytes, one deduplicated document
    assert fake_kz.upload_calls == [(index_upload_name(job), PDF_BYTES)]
    assert fake_papra.download_calls == 1    # Papra is the byte source, rebuild included


async def test_a_seen_document_without_a_current_id_is_still_indexed(store, job,
                                                                     fake_papra,
                                                                     fake_kz):
    """v1's mistake, pinned: `document_seen` is ingestion history, not eligibility.

    A document we ingested long ago and an id that a rebuild just made meaningless
    are different facts, and only the second one decides whether there is work.
    Read from the seen-table, a rebuild would look already-done to the very sweep
    that exists to redo it.
    """
    seed_ledger(store, kreuzakt_id=7, generation=1)
    store.record_document_seen(papra_org_id=ORG, papra_document_id=DOC_ID, name=NAME,
                               mime="application/pdf", ingested_via="sweep")
    store.bump_generation()

    assert [ref.papra_document_id for ref in store.ledger_needs_index(generation=2)] \
        == [DOC_ID]

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    assert outcome.kreuzakt_id == 7 and len(fake_kz.upload_calls) == 1
    assert store.ledger_needs_index(generation=2) == []


async def test_generation_change_mid_flight_discards_the_result(store, job, fake_papra,
                                                                fake_kz):
    """The race generation exists to catch: a rebuild lands between upload and bind."""
    fake_kz.on_wait = lambda: store.bump_generation()

    with pytest.raises(IndexFailed, match="generation changed mid-flight"):
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id is None
    assert store.get_generation() > 1        # the rebuild happened; the bind did not


# --- reporting, and the phase boundary ---------------------------------------------

async def test_empty_content_is_recorded_as_extraction_empty(store, job, fake_papra,
                                                              fake_kz, caplog):
    caplog.set_level(logging.INFO, logger="papra_suite.indexing")
    fake_kz.document = KzDocument(id=7, title="", description="", content="   \n",
                                  document_date=None, original_filename="f.pdf",
                                  updated_at=None)

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    assert outcome.empty_content is True and outcome.delivered is False
    assert outcome.content_len == 4
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id == 7   # the reference is the record
    assert any("extraction-empty" in record.getMessage() for record in caplog.records)


async def test_content_delivery_enabled_writes_through_the_gate(store, job, fake_papra,
                                                                fake_kz):
    """Task 7's half of the handler: with the flag on, Kreuzakt's text becomes Papra's.

    Only the wiring is tested here — that the handler reaches `deliver_content` and
    reports what it did. What may and may not be written is `test_content_delivery.py`,
    split deliberately: a mutation of the emptiness guard should not be discoverable
    only in the file about the worker.
    """
    store.set_setting("index_content_delivery", "true")

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    text = fake_kz.document["content"]
    assert outcome.delivered is True
    assert fake_papra.document["content"] == text
    assert fake_papra.patched == [(ORG, DOC_ID, {"content": text})]
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.delivered_sha == hashlib.sha256(text.encode()).hexdigest()
    assert ref.delivered_generation == store.get_generation()


async def test_correlation_waits_on_the_name_the_upload_returned(store, job, fake_papra,
                                                                 fake_kz):
    """The response's `relativePath` is the join key, not the name we constructed.

    Measured: the server renames (sanitising, ` (N)`), and matching our own string
    against a row it renamed would report absence for a document we did index.
    """
    fake_kz.rename_uploads = f"b6fa1b2135fe_{UPLOAD_NAME}"

    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)

    assert outcome.kreuzakt_id == 7


async def test_the_vanished_row_error_is_recognised_from_the_real_client(fake_kz, job):
    """The vanished-document branch is coupled to Task 5's wording — so pin the pair.

    `wait_for_queue_row` reports a completed row with a null id as a plain
    `KreuzaktError`, and the client is not this task's file to change. If its
    wording moves, this test fails instead of the branch quietly never firing.
    """
    fake_kz.documents_vanish = True
    name = index_upload_name(job)
    mark = await fake_kz.queue_high_water_mark()
    stored = await fake_kz.upload(name, PDF_BYTES)

    with pytest.raises(KreuzaktError) as exc:
        await fake_kz.wait_for_queue_row(stored, after_id=mark, poll_s=0, timeout_s=0.05)

    assert indexing._is_vanished_row_error(exc.value) is True
    assert indexing._is_vanished_row_error(
        KreuzaktError("no queue row for x.pdf within 5s")) is False


async def test_the_wait_budget_is_a_job_budget_not_a_per_attempt_one(store, job,
                                                                     fake_papra, fake_kz,
                                                                     monkeypatch):
    """Attempts x timeout would hold the serial worker for 45 minutes on one document.

    `wait_timeout_s` covers the whole correlation: when it is gone the job stops with a
    retriable timeout instead of starting an upload it has no time to correlate.
    """
    clock = FakeClock()
    monkeypatch.setattr(indexing, "_clock", clock.now)
    fake_kz.window_saturated_always = True
    fake_kz.on_wait = lambda: clock.advance(3.0)     # every correlation read costs 3s

    with pytest.raises(IndexTimeout, match="wait budget"):
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert len(fake_kz.upload_calls) == 2      # the third upload never started
    assert clock.now_value == 6.0              # and it stopped the moment the budget went


async def test_time_spent_uploading_is_charged_to_the_wait_budget(store, job, fake_papra,
                                                                 fake_kz, monkeypatch):
    """The budget is spent by the whole attempt, not only by the polling loop.

    A 40 MB download plus a slow upload can consume `wait_timeout_s` on their own; a
    wait started with the allowance computed before them would run past the budget it
    was given, which is the difference between a 15-minute job and a 45-minute one.
    """
    clock = FakeClock()
    monkeypatch.setattr(indexing, "_clock", clock.now)
    fake_kz.on_upload = lambda: clock.advance(6.0)    # the upload alone outlasts the budget

    with pytest.raises(IndexTimeout, match="after 1 attempt"):
        await run_index_job(fake_kz, store, job, papra=fake_papra,
                            poll_s=0, wait_timeout_s=5)

    assert len(fake_kz.upload_calls) == 1
    assert fake_kz.queue_reads == 1            # only the high-water read: no wait started
