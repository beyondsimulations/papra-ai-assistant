import asyncio
import logging
import sqlite3

import pytest

from papra_suite import indexing, pipeline
from papra_suite.config import load_config
from papra_suite.pipeline import enqueue_index_job, enqueue_ocr_job, serve, sweep_once
from papra_suite.store import Store
from tests.test_index_job import DOC_ID, HASH, NAME, ORG, FakeKreuzakt, FakePapra, seed_ledger


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


def test_enqueue_ocr_job_enqueue_survives_seen_failure(tmp_path, monkeypatch):
    store = Store(tmp_path / "state.db")
    store.migrate()
    try:
        def _locked(*args, **kwargs):
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(store, "record_document_seen", _locked)

        # the poller treats the raise as a message failure — but enqueue-first
        # ordering means the job already exists and the document is NOT seen:
        # exactly the sweep's self-heal precondition (re-enqueue no-op + record)
        with pytest.raises(sqlite3.OperationalError):
            enqueue_ocr_job(
                store, papra_org_id="org_1", document_id="doc_1", file_hash="abc123",
                name="bill.pdf", mime="application/pdf", ingested_via="mail",
            )
        assert len(store.list_jobs()) == 1
        assert store.document_seen("org_1", "doc_1") is False
    finally:
        store.close()


# --- enqueue_index_job: the intake path for the `index` kind -----------------------
#
# Three orderings have to hold, and a crash between any two of them has to leave a
# state the pipeline can repair: ledger reference, then visible job, then the seen
# record. The first is new (the eligibility rule is derived from the ledger, so a
# job with no row behind it is work nothing can see); the last two are the
# self-heal ordering M3 established, unchanged.

@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=ORG, name="Org 1")
    yield store
    store.close()


def _enqueue(store, **overrides):
    kwargs = dict(papra_org_id=ORG, document_id=DOC_ID, file_hash=HASH, name=NAME,
                  mime="application/pdf", ingested_via="sweep")
    kwargs.update(overrides)
    return enqueue_index_job(store, **kwargs)


def test_enqueue_index_job_puts_the_ledger_row_before_the_visible_job(store):
    """A visible index job always has a ledger row that explains it.

    Written the other way round, a crash between the two leaves a job the sweep
    cannot account for: `ledger_needs_index` reads the ledger, so a document with no
    row is not work — and the job that was already queued is invisible to it.
    """
    observed = []
    real_enqueue = store.enqueue_job

    def spy(**kwargs):
        observed.append((kwargs["kind"], store.ledger_get(ORG, DOC_ID) is not None))
        return real_enqueue(**kwargs)

    store.enqueue_job = spy
    job_id = _enqueue(store)

    # every enqueue, legacy writer included, happens with the row already there
    assert observed == [("index", True), ("ocr", True)]
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.kreuzakt_id is None and ref.file_hash == HASH
    assert ref.papra_original_name == NAME      # the baseline, taken at intake too
    assert store.get_job(job_id).kind == "index"


def test_enqueue_index_job_records_seen_last(store):
    """The M3 ordering, kept: seen-second, so a crash between the writes self-heals."""
    def _locked(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    store.record_document_seen = _locked
    with pytest.raises(sqlite3.OperationalError):
        _enqueue(store)

    assert sorted(job.kind for job in store.list_jobs()) == ["index", "ocr"]
    assert store.document_seen(ORG, DOC_ID) is False   # the sweep re-discovers it
    assert store.ledger_get(ORG, DOC_ID) is not None


def test_enqueue_index_job_keeps_the_old_writers_while_content_delivery_is_off(store):
    """Phase one: Papra keeps the text and the titles it has today.

    The legacy `ocr` enqueue rides along with the index enqueue until the bridge
    owns the text, which is why handing the sweep to `enqueue_index_job` changes
    what is *added* rather than what stops.
    """
    _enqueue(store)

    kinds = sorted(job.kind for job in store.list_jobs())
    assert kinds == ["index", "ocr"]
    assert store.document_seen(ORG, DOC_ID)


def test_enqueue_index_job_stops_the_legacy_writer_once_content_is_ours(store):
    store.set_setting("index_content_delivery", "true")

    _enqueue(store)

    assert [job.kind for job in store.list_jobs()] == ["index"]


def test_enqueue_index_job_stops_the_legacy_enqueue_once_the_writers_are_off(store):
    """The drain half of the handover: `PAPRA_LEGACY_WORKERS=false` stops NEW OCR work.

    Stopping the loops from claiming is only half of an empty queue — a loop that is gone
    still leaves whatever the intake keeps enqueueing, and the sweep runs whether or not
    anyone claims what it produces. Quiescence is proven by the `ocr` rows in `jobs`
    staying at zero twice a minute apart, and that is not reachable while an intake path
    is still filling the queue behind the drain.
    """
    store.set_setting("legacy_workers", "false")

    _enqueue(store)

    assert [job.kind for job in store.list_jobs()] == ["index"]
    assert store.ledger_get(ORG, DOC_ID) is not None    # the index path is unaffected


def test_enqueue_index_job_is_idempotent_and_rehashing_rebinds(store):
    first = _enqueue(store)
    again = _enqueue(store)
    assert first == again
    assert len(store.list_jobs()) == 2          # one index + one ocr, not four

    changed = _enqueue(store, file_hash="fff")
    assert changed != first
    assert store.ledger_get(ORG, DOC_ID).file_hash == "fff"


def test_enqueue_index_job_leaves_a_non_pdf_untouched(store):
    """A JPEG intake belongs to neither queue, and gets no ledger row either."""
    assert _enqueue(store, mime="image/jpeg") == ""

    assert store.list_jobs() == []
    assert store.document_seen(ORG, DOC_ID) is False
    assert store.ledger_get(ORG, DOC_ID) is None


# --- the sweep's enqueue call site --------------------------------------------------

class StubPapraListing:
    """The three Papra reads `sweep_once` makes."""

    def __init__(self, documents):
        self.documents = documents
        self.downloads = 0

    async def list_documents(self, org_id, *, page_index=0, page_size=100,
                             search_query=None, sort_field="createdAt",
                             sort_order="desc"):
        page = self.documents[page_index * page_size:(page_index + 1) * page_size]
        return page, len(self.documents)

    async def download_file(self, org_id, document_id):
        self.downloads += 1
        return b"%PDF-1.4\ntrailer\n%%EOF"

    async def get_document(self, org_id, document_id):
        return {"id": document_id, "name": NAME}


async def test_sweep_enqueues_index_work_for_an_externally_uploaded_pdf(store):
    """Papra-native uploads enter the index through the sweep, with no new machinery.

    The web UI and the intake address create documents we never ingested; the sweep
    already existed to find them, so its enqueue call site is what changes.
    """
    papra = StubPapraListing([{"id": DOC_ID, "name": NAME, "mimeType": "application/pdf",
                               "originalSha256Hash": HASH, "originalSize": 20}])

    result = await sweep_once(store, papra)

    assert (result.documents_checked, result.documents_enqueued) == (1, 1)
    index_job = store.claim_next_job("index")
    assert index_job is not None and index_job.document_id == DOC_ID
    assert store.claim_next_job("ocr") is not None      # the old writer still runs
    ref = store.ledger_get(ORG, DOC_ID)
    assert ref is not None and ref.papra_original_name == NAME
    assert papra.downloads == 0                          # the listing hash was enough


# --- serve(): the index worker loop --------------------------------------------------

SERVE_ENV = {
    "PAPRA_ALLOWED_SENDERS": "sender@x.com",
    "PAPRA_BASE_URL": "http://papra:1221", "PAPRA_API_KEY": "k",
    "LLM_BASE_URL": "http://llm:8000", "LLM_API_KEY": "k",
    "TITLE_MODEL": "t", "OCR_MODEL": "o", "ADMIN_PASSWORD": "p",
    "SWEEP_INTERVAL_S": "0",           # the sweep is tested above; here, the worker
    "PAPRA_KREUZAKT_URL": "http://kreuzakt.test",
}


async def test_serve_runs_the_index_worker(tmp_path, store):
    """`serve` claims `index` jobs the way it claims `ocr` ones, and finishes them."""
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path)})
    seed_ledger(store)
    papra, kreuzakt = FakePapra(), FakeKreuzakt()
    job_id = store.enqueue_job(kind="index", papra_org_id=ORG, document_id=DOC_ID,
                               content_hash=HASH,
                               payload={"file_hash": HASH, "name": NAME})

    stop = asyncio.Event()

    async def run_until_done():
        for _ in range(200):
            if store.get_job(job_id).status == "done":
                break
            await asyncio.sleep(0.01)
        stop.set()

    task = asyncio.get_running_loop().create_task(run_until_done())
    await serve(config, stop=stop, store=store, papra=papra, kreuzakt=kreuzakt)
    await task

    assert store.get_job(job_id).status == "done"
    assert store.ledger_get(ORG, DOC_ID).kreuzakt_id == 7
    await kreuzakt.aclose()


async def test_serve_seeds_the_delivery_flags_from_config(tmp_path, store, caplog):
    """Config is the source of truth at startup, so the CLI and the daemon agree.

    A flag only the scheduler consulted could be bypassed by a CLI call or an inbound
    trigger; a flag only the settings table held would change meaning on every
    restart. So the row is written from `Config` and everyone asks the store.
    """
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path),
                          "PAPRA_INDEX_METADATA_DELIVERY": "1"})
    store.set_setting("index_metadata_delivery", "false")   # stale row from an old start

    stop = asyncio.Event()
    stop.set()                       # start up, seed, shut down: no pass is needed
    await serve(config, stop=stop, store=store, papra=FakePapra(), kreuzakt=FakeKreuzakt())

    assert store.settings_flag("index_metadata_delivery") is True
    assert store.settings_flag("index_content_delivery") is False
    # and the writer switch itself, which the two-writer refusal reads alongside the
    # delivery flag. Seeded the same way, so `check` and a running daemon cannot disagree
    # about who owns Papra's text.
    assert store.settings_flag("legacy_workers") is True


class Tripwire:
    """A client where touching any attribute is a failure.

    The refusal Task 7 adds has to happen BEFORE anything reaches Papra or Kreuzakt: a
    container that half-indexes a corpus and then exits has already done the damage the
    refusal was supposed to prevent. "It did not crash" is not evidence of that, and
    neither is a fake that quietly answers — so this records, and raises, on the
    attribute access itself.
    """

    def __init__(self, name: str, sink: list) -> None:
        self.name = name
        self.sink = sink

    def __getattr__(self, attr: str):
        self.sink.append(f"{self.name}.{attr}")
        raise AssertionError(f"serve reached for {self.name}.{attr} instead of refusing")


async def test_serve_refuses_two_writers_of_the_same_column(tmp_path, store, caplog):
    """Two writers of one column, refused at startup rather than debugged at 03:00.

    The refused pair is this suite's legacy OCR worker and this bridge — the two writers
    whose existence we control, and which are the same shape as the race behind the original
    intermittency (that one was this suite's OCR against Papra's own extraction; Papra's
    extraction is a third hand on the column that no flag here stops, which is what the
    convergence pass is for rather than this check's). `PAPRA_LEGACY_WORKERS` defaults to
    true, so an operator who sets `PAPRA_INDEX_CONTENT_DELIVERY=true` and recreates the
    container gets both of our writers back with no new symptom to look at. The handover
    sequence gets the order right only if a human follows it; this is what stops it being
    followed wrongly.
    """
    from papra_suite.pipeline import ContentWriterConflict

    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path),
                          "PAPRA_INDEX_CONTENT_DELIVERY": "1"})
    seed_ledger(store)
    job_id = store.enqueue_job(kind="index", papra_org_id=ORG, document_id=DOC_ID,
                               content_hash=HASH, payload={"file_hash": HASH, "name": NAME})
    touched: list = []

    stop = asyncio.Event()
    with pytest.raises(ContentWriterConflict) as excinfo:
        await serve(config, stop=stop, store=store, papra=Tripwire("papra", touched),
                    kreuzakt=Tripwire("kreuzakt", touched),
                    ocr_llm=Tripwire("ocr_llm", touched),
                    title_llm=Tripwire("title_llm", touched))

    message = str(excinfo.value)
    assert "PAPRA_INDEX_CONTENT_DELIVERY" in message
    assert "PAPRA_LEGACY_WORKERS" in message
    # The order, not just the two names: the fix is a sequence, and this message is the
    # only documentation an operator reaches for at 23:00 with a container in a restart
    # loop — legacy off, quiescence proven, then the delivery flag.
    assert "quiesc" in message.lower()
    assert message.index("PAPRA_LEGACY_WORKERS") < message.lower().index("quiesc")
    assert message.lower().index("quiesc") < message.lower().index("delivery flag")
    # And the runbook has to be runnable on THIS build. The plan drafted this message
    # around a `papra-suite jobs` command this branch never shipped, so copying it
    # verbatim would tell an operator at 23:00 to type a subcommand that does not exist —
    # which reads as a broken installation rather than as a stale message. The queue is
    # named where it actually is readable, in `state.db`.
    assert "papra-suite jobs" not in message, message
    assert "FROM jobs" in message, message
    assert touched == []                     # neither service was asked for anything
    assert store.get_job(job_id).status == "pending"
    assert any(record.levelno >= logging.ERROR for record in caplog.records)


async def test_the_handover_window_with_no_writer_starts_and_says_so(tmp_path, store, caplog):
    """The opposite combination is legitimate and temporary, so it must not be refused.

    Between `PAPRA_LEGACY_WORKERS=false` and the delivery flags there is a window where
    nobody writes Papra's text: draining the queue takes minutes, and refusing it would
    make the handover unperformable — the legal state could not be reached without
    stopping the service. What it must not be is silent, because a window that lasts a
    week starts to look like a configuration, so it is ERROR, and `check` prints it too.
    """
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path),
                          "PAPRA_LEGACY_WORKERS": "0"})
    stop = asyncio.Event()
    stop.set()
    await serve(config, stop=stop, store=store, papra=FakePapra(),
                kreuzakt=FakeKreuzakt())

    assert store.settings_flag("legacy_workers") is False
    assert store.settings_flag("index_content_delivery") is False
    errors = [record.getMessage() for record in caplog.records
              if record.levelno >= logging.ERROR]
    assert any("handover window" in message and "content" in message
               for message in errors), errors


async def test_serve_starts_no_legacy_worker_loops_once_the_writers_are_off(tmp_path, store,
                                                                           monkeypatch):
    """`PAPRA_LEGACY_WORKERS=false` has to stop the claiming, not only the enqueuing.

    The refusal message tells the operator to switch the legacy writers off and drain, and
    a drain is only reachable if the loops stop taking work. Leaving them running would
    mean the flag renamed the workers rather than stopped them: the queue would never
    empty while the container was up, and the handover could not be completed.
    """
    started: list[str] = []

    async def recording_loop(store, *, kind, handler, idle_sleep_s=1.0, max_attempts=None):
        started.append(kind)

    monkeypatch.setattr(pipeline, "worker_loop", recording_loop)

    stop = asyncio.Event()
    stop.set()
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path),
                          "PAPRA_LEGACY_WORKERS": "0"})
    await serve(config, stop=stop, store=store, papra=FakePapra(), kreuzakt=FakeKreuzakt())
    # kinds, not counts: `OCR_CONCURRENCY` decides how many loops the legacy writer gets,
    # and which loops exist is the property asserted here.
    assert set(started) == {"index"}

    started.clear()
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path)})
    await serve(config, stop=stop, store=store, papra=FakePapra(), kreuzakt=FakeKreuzakt())
    assert set(started) == {"index", "ocr", "title"}


async def test_serve_refuses_a_content_flag_this_build_cannot_honour(tmp_path, store,
                                                                    caplog, monkeypatch):
    """The one flag that must NOT be seeded as given, because honouring it costs text.

    `enqueue_index_job` stops the legacy OCR enqueue while content delivery is on —
    correct now that Task 7 delivers content, and a silent content outage in a build
    without that path. Refusing the flag loudly is the honest startup for such a build,
    and the flag is read as `indexing.CONTENT_DELIVERY_IMPLEMENTED` — looked up on the
    module, not bound by value at import — because binding it is what would make the
    documented way of exercising a build a silent no-op.
    """
    monkeypatch.setattr(indexing, "CONTENT_DELIVERY_IMPLEMENTED", False)
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path),
                          "PAPRA_INDEX_CONTENT_DELIVERY": "1"})

    stop = asyncio.Event()
    stop.set()
    await serve(config, stop=stop, store=store, papra=FakePapra(), kreuzakt=FakeKreuzakt())

    assert store.settings_flag("index_content_delivery") is False
    messages = [record.getMessage() for record in caplog.records]
    assert any("no content delivery path" in m for m in messages), messages
    # and the consequence the warning promises: the old writer is still enqueued
    _enqueue(store)
    assert [job.kind for job in store.list_jobs()] == ["ocr", "index"]


async def test_serve_passes_the_index_attempt_ceiling_to_the_loop_that_spends_it(
        tmp_path, store, monkeypatch):
    """`PAPRA_INDEX_MAX_ATTEMPTS` has to reach the code that ends a job.

    The value went to the handler, which spent it on its own row re-runs, while the
    worker loop marked failures with the store's default of 3 — so the knob moved one
    budget and not the other, and an operator reading `.env.example` would believe a
    configured 5 meant five runs. This pins the wiring at the place that starts the
    worker, and pins the legacy kinds to the store's default so an index-shaped
    ceiling cannot spread by accident.
    """
    ceilings = {}

    async def recording_loop(store, *, kind, handler, idle_sleep_s=1.0, max_attempts=None):
        ceilings[kind] = max_attempts

    monkeypatch.setattr(pipeline, "worker_loop", recording_loop)
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path),
                          "PAPRA_INDEX_MAX_ATTEMPTS": "5"})

    stop = asyncio.Event()
    stop.set()
    await serve(config, stop=stop, store=store, papra=FakePapra(), kreuzakt=FakeKreuzakt())

    assert ceilings["index"] == 5
    assert ceilings["ocr"] is None and ceilings["title"] is None


async def test_serve_honours_the_content_flag_once_the_build_can_deliver(tmp_path, store):
    """The other side of that seam, kept honest by the build actually shipping the path.

    A suite that only ever exercised the refusal would keep passing after the delivery
    path landed, and the setting the writer consults would quietly stay false — which is
    the failure mode where the bridge indexes and nothing ever reaches Papra.
    """
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path),
                          "PAPRA_INDEX_CONTENT_DELIVERY": "1",
                          "PAPRA_LEGACY_WORKERS": "0"})

    stop = asyncio.Event()
    stop.set()
    await serve(config, stop=stop, store=store, papra=FakePapra(), kreuzakt=FakeKreuzakt())

    assert store.settings_flag("index_content_delivery") is True
