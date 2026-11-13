import asyncio
import hashlib
import json
import logging
import sqlite3

import httpx
import pytest

import papra_suite.pipeline as pipeline_mod
from papra_suite.config import load_config
from papra_suite.papra_client import PapraError
from papra_suite.pipeline import (
    MAX_SWEEP_PAGES,
    OCR_MAX_DOCUMENT_BYTES,
    SWEEP_PAGE_SIZE,
    SweepResult,
    on_job_done,
    run_ocr_job,
    run_title_job,
    sweep_once,
    worker_loop,
)
from papra_suite.store import Store

BASE_ENV = {
    "PAPRA_BASE_URL": "http://papra:1221", "PAPRA_API_KEY": "k",
    "LLM_BASE_URL": "http://llm:8000", "LLM_API_KEY": "k",
    "TITLE_MODEL": "t", "OCR_MODEL": "o", "ADMIN_PASSWORD": "p",
}


class StubPapra:
    """Records write-backs; serves a canned PDF for downloads."""

    def __init__(self, pdf: bytes = b"%PDF-1.4 fake-with-pages"):
        self.pdf = pdf
        self.content_updates: list[tuple[str, str | None, str | None]] = []
        self.documents = {  # org -> {doc_id: {"name": ..., "mimeType": ...}}
            "org_1": {
                "doc_1": {"id": "doc_1", "name": "rechnung.pdf", "mimeType": "application/pdf"},
                "doc_2": {"id": "doc_2", "name": "notes.txt", "mimeType": "text/plain"},
            }
        }

    async def download_file(self, org_id, document_id):
        return self.pdf

    async def get_document(self, org_id, document_id):
        return self.documents[org_id][document_id]

    async def update_document(self, org_id, document_id, *, name=None, content=None):
        self.content_updates.append((document_id, name, content))
        doc = self.documents[org_id][document_id]
        if name:
            doc["name"] = name
        if content is not None:
            doc["content"] = content

    async def list_documents(self, org_id, *, page_index=0, page_size=100,
                             search_query=None, sort_field="createdAt",
                             sort_order="desc"):
        docs = list(self.documents[org_id].values())
        page = docs[page_index * page_size:(page_index + 1) * page_size]
        return page, len(docs)


def make_llm(responses: list[str], finish_reasons: list[str] | None = None):
    """finish_reasons optionally sets choice.finish_reason per response index;
    when omitted the fake sends no finish_reason at all (as many servers do)."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        assert body["messages"][0]["role"] == "system"
        index = min(calls["n"], len(responses) - 1)
        choice: dict = {"message": {"content": responses[index]}}
        if finish_reasons is not None:
            choice["finish_reason"] = finish_reasons[min(index, len(finish_reasons) - 1)]
        calls["n"] += 1
        return httpx.Response(200, json={"choices": [choice]})

    from papra_suite.llm_client import LLMClient

    return LLMClient("http://llm.test", "k", "m", backoff_base_s=0.0,
                     transport=httpx.MockTransport(handler)), calls


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    # sweep_once iterates the store's orgs; give it the StubPapra org
    store.create_org(papra_org_id="org_1", name="Org 1")
    yield store
    store.close()


@pytest.fixture
def config():
    return load_config({**BASE_ENV, "DATA_DIR": "/tmp/x"})


def make_job(store, kind="ocr", doc="doc_1", content_hash="h1",
             payload=None):
    job_id = store.enqueue_job(kind=kind, papra_org_id="org_1", document_id=doc,
                               content_hash=content_hash,
                               payload=payload if payload is not None
                               else {"file_hash": content_hash, "name": "rechnung.pdf"})
    return store.get_job(job_id)


async def test_run_ocr_job_writes_back_assembled_markdown(store, config):
    import fitz

    pdf = fitz.open()
    pdf.new_page()
    pdf.new_page()
    pdf_bytes = pdf.tobytes()
    pdf.close()
    papra = StubPapra(pdf_bytes)
    llm, calls = make_llm(["# Seite eins", "# Seite zwei"])

    markdown = await run_ocr_job(papra, llm, make_job(store), config=config)

    assert markdown == "# Seite eins\n\n# Seite zwei"
    assert calls["n"] == 2  # one request per page
    assert papra.content_updates == [("doc_1", None, "# Seite eins\n\n# Seite zwei")]


async def test_run_ocr_job_raises_on_zero_page_pdf(store, config):
    # a zero-page PDF must fail visibly, not write empty content back
    # (handcrafted: this PyMuPDF cannot save a zero-page document)
    data = (
        b"%PDF-1.4\n"
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
        b"2 0 obj\n<< /Type /Pages /Kids [] /Count 0 >>\nendobj\n"
        b"trailer\n<< /Root 1 0 R /Size 3 >>\n"
    )
    papra = StubPapra(data)
    llm, calls = make_llm(["# md"])

    with pytest.raises(ValueError, match="no pages to transcribe"):
        await run_ocr_job(papra, llm, make_job(store), config=config)

    assert calls["n"] == 0  # no LLM calls for a pageless document
    assert papra.content_updates == []  # nothing written back


async def test_run_ocr_job_renders_pages_lazily(store, config, monkeypatch):
    """Pin the lazy path: run_ocr_job must call render_page once per page
    index (off the loop via asyncio.to_thread), not render_pages upfront."""
    import fitz

    pdf = fitz.open()
    for _ in range(3):
        pdf.new_page()
    papra = StubPapra(pdf.tobytes())
    pdf.close()
    llm, calls = make_llm(["# a", "# b", "# c"])

    rendered: list[int] = []

    def spy_render_page(pdf_bytes, index, *, dpi, pixel_budget):
        rendered.append(index)
        return b"fake-png"

    monkeypatch.setattr(pipeline_mod, "render_page", spy_render_page)

    markdown = await run_ocr_job(papra, llm, make_job(store), config=config)

    assert rendered == [0, 1, 2]  # one render call per page index
    assert calls["n"] == 3
    assert markdown == "# a\n\n# b\n\n# c"


async def test_run_ocr_job_rejects_oversized_document(store, config):
    """A document over OCR_MAX_DOCUMENT_BYTES must fail visibly instead of
    being pulled into memory and rendered."""
    papra = StubPapra(b"x" * (OCR_MAX_DOCUMENT_BYTES + 1))
    llm, calls = make_llm(["# md"])

    with pytest.raises(ValueError, match="too large"):
        await run_ocr_job(papra, llm, make_job(store), config=config)

    assert calls["n"] == 0
    assert papra.content_updates == []


def test_on_job_done_enqueues_title_job(store):
    job = make_job(store)
    on_job_done(store, job, ocr_result="# Rechnung\n\nInhalt")
    title_jobs = store.list_jobs()
    assert len(title_jobs) == 2
    title_job = [j for j in title_jobs if j.kind == "title"][0]
    assert title_job.document_id == "doc_1"
    assert title_job.content_hash == hashlib.sha256(b"# Rechnung\n\nInhalt").hexdigest()
    assert title_job.payload["name"] == "rechnung.pdf"
    assert title_job.payload["excerpt"] == "# Rechnung\n\nInhalt"


def test_on_job_done_name_defaults_to_document_pdf(store):
    job = make_job(store, payload={"file_hash": "h1"})  # no name in payload
    on_job_done(store, job, ocr_result="Inhalt")
    title_job = [j for j in store.list_jobs() if j.kind == "title"][0]
    assert title_job.payload["name"] == "document.pdf"  # matches run_title_job's default


def test_on_job_done_ignores_title_kind(store):
    job = make_job(store, kind="title", content_hash="th",
                   payload={"name": "n", "excerpt": "e"})
    on_job_done(store, job, ocr_result="unused")
    assert len(store.list_jobs()) == 1


async def test_run_title_job_writes_generated_name(store):
    papra = StubPapra()
    # the title job's hash must match the document's CURRENT content
    # (the stale-content guard compares them before generating)
    current = "Rechnung Nr. 42 – aktuell archivierter Inhalt"
    papra.documents["org_1"]["doc_1"]["content"] = current
    content_hash = hashlib.sha256(current.encode()).hexdigest()
    llm, _ = make_llm(["Rechnung 42 – Möbel"])
    job = make_job(store, kind="title", content_hash=content_hash,
                   payload={"name": "rechnung.pdf", "excerpt": "Rechnung Nr. 42"})

    await run_title_job(papra, llm, job)

    assert papra.content_updates == [("doc_1", "Rechnung 42 – Möbel", None)]


async def test_stale_title_job_skipped(store):
    """A title job whose content_hash no longer matches the document must
    NOT overwrite a newer title (decisions §2: consumers take the latest)."""
    papra = StubPapra()
    papra.documents["org_1"]["doc_1"]["content"] = "neuer Inhalt v2"
    stale_hash = hashlib.sha256(b"alter Inhalt v1").hexdigest()
    llm, calls = make_llm(["Titel aus v1 – sollte nie geschrieben werden"])
    job = make_job(store, kind="title", content_hash=stale_hash,
                   payload={"name": "rechnung.pdf", "excerpt": "alter Inhalt v1"})

    await run_title_job(papra, llm, job)

    assert papra.content_updates == []  # nothing overwritten
    assert calls["n"] == 0  # no LLM call either


async def test_worker_loop_processes_and_transitions(store, config):
    import fitz

    pdf = fitz.open()
    pdf.new_page()
    papra = StubPapra(pdf.tobytes())
    pdf.close()
    llm, _ = make_llm(["# md"])
    make_job(store)  # enqueue one ocr job (doc_1)

    # run the loop until the queue drains, then stop it via a failing claim
    # patch (BaseException: the claim guard must not swallow the stop seam)
    original_claim = store.claim_next_job
    state = {"claims": 0}

    class _StopLoop(BaseException):
        pass

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise _StopLoop()
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(_StopLoop):
        await worker_loop(store, kind="ocr",
                          handler=lambda job: run_ocr_job(papra, llm, job, config=config))

    jobs = store.list_jobs()
    assert [j.kind for j in jobs if j.status == "done"] == ["ocr"]
    assert [j.kind for j in jobs if j.status == "pending"] == ["title"]


async def test_worker_loop_transitions_before_marking_done(store, config):
    """Order pin: on_job_done runs BEFORE mark_job_done.

    A crash at mark_job_done must leave the title job already enqueued
    (and the OCR job not done) — the crash-safe ordering the startup
    recovery relies on to re-run the OCR job without losing the title.
    """
    import fitz

    pdf = fitz.open()
    pdf.new_page()
    papra = StubPapra(pdf.tobytes())
    pdf.close()
    llm, _ = make_llm(["# md"])
    make_job(store)

    class _Crash(BaseException):
        pass

    def mark_job_done(job_id):
        raise _Crash()  # simulate a crash between transition and done-marking

    store.mark_job_done = mark_job_done
    with pytest.raises(_Crash):
        await worker_loop(store, kind="ocr",
                          handler=lambda job: run_ocr_job(papra, llm, job, config=config))

    jobs = store.list_jobs()
    title_jobs = [j for j in jobs if j.kind == "title"]
    assert [j.status for j in title_jobs] == ["pending"]  # transition ran first
    ocr_job = [j for j in jobs if j.kind == "ocr"][0]
    assert ocr_job.status != "done"  # crash before done-marking


async def test_worker_loop_survives_claim_failure(store, config):
    """A claim that raises is logged and the loop continues — not dies."""
    import fitz

    pdf = fitz.open()
    pdf.new_page()
    papra = StubPapra(pdf.tobytes())
    pdf.close()
    llm, _ = make_llm(["# md"])
    make_job(store)

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] == 1:
            raise RuntimeError("sqlite: database disk image is malformed")
        if state["claims"] > 2:
            raise KeyboardInterrupt()  # stop seam after the job was processed
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", idle_sleep_s=0,
                          handler=lambda job: run_ocr_job(papra, llm, job, config=config))

    jobs = store.list_jobs()
    # the job was processed despite the failed claim
    assert [j.kind for j in jobs if j.status == "done"] == ["ocr"]
    assert [j.kind for j in jobs if j.status == "pending"] == ["title"]


async def test_worker_loop_survives_empty_queue(store):
    """Idle-exit pin: an empty claim (None) must IDLE the loop, not exit it.

    A `if job is None: break` regression silently kills every worker as soon
    as the queue drains once. Claim sequence [job1, None, job2, stop]: BOTH
    jobs must be processed and the stop exception must surface — a
    break-on-None would return before job2 and never raise the stop seam.
    """
    make_job(store, doc="doc_1")
    make_job(store, doc="doc_2")
    processed = []

    async def handler(job):
        processed.append(job.document_id)

    original_claim = store.claim_next_job
    state = {"claims": 0}

    class _StopLoop(BaseException):
        pass

    def claim(kind):
        state["claims"] += 1
        if state["claims"] == 2:
            return None  # empty queue: the loop must idle, not exit
        if state["claims"] > 3:
            raise _StopLoop()  # stop seam AFTER the second job was claimed
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(_StopLoop):
        await worker_loop(store, kind="ocr", idle_sleep_s=0, handler=handler)

    assert processed == ["doc_1", "doc_2"]  # job2 ran AFTER the empty claim
    assert [j.status for j in store.list_jobs() if j.kind == "ocr"] == ["done", "done"]


async def test_worker_loop_marks_failures(store, config):
    llm, _ = make_llm(["# md"])
    make_job(store)

    async def handler(job):
        raise RuntimeError("boom")

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise KeyboardInterrupt()
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", handler=handler)

    job = store.list_jobs()[0]
    assert job.status == "pending" and job.attempts == 1 and "boom" in job.error


async def test_worker_loop_survives_mark_job_failed_error(store, caplog):
    """A sqlite error while RECORDING a failure must not kill the worker
    task (the job would wedge 'running' until restart)."""
    make_job(store)

    async def handler(job):
        raise RuntimeError("boom")

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 2:
            raise KeyboardInterrupt()  # stop seam: the loop SURVIVED claim 2
        return original_claim(kind)

    def mark_job_failed(job_id, error, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    store.claim_next_job = claim
    store.mark_job_failed = mark_job_failed
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", idle_sleep_s=0, handler=handler)

    assert state["claims"] == 3  # loop kept going after the failed mark
    assert any("vanished" in record.getMessage() for record in caplog.records)
    # the job stays 'running' (the failure could not be recorded) — recovery
    # at startup is the backstop, but the WORKER loop itself survived
    assert store.list_jobs()[0].status == "running"


async def test_worker_loop_survives_vanished_job_on_failure_record(store, caplog):
    """The vanished-job guard must also hold for ValueError specifically
    (mark_job_failed raises it when the job row disappeared mid-run): the
    loop survives and logs, instead of dying on the recording failure."""
    make_job(store)

    async def handler(job):
        raise RuntimeError("boom")

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 2:
            raise KeyboardInterrupt()  # stop seam: the loop SURVIVED claim 2
        return original_claim(kind)

    def mark_job_failed(job_id, error, **kwargs):
        raise ValueError(f"unknown job: {job_id}")  # job vanished mid-run

    store.claim_next_job = claim
    store.mark_job_failed = mark_job_failed
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", idle_sleep_s=0, handler=handler)

    assert state["claims"] == 3  # loop kept going after the vanished job
    assert any("vanished" in record.getMessage() for record in caplog.records)


async def test_worker_loop_fails_papra_4xx_immediately(store):
    """Papra 4xx (except 429) never auto-retries (decisions §6): a 404 must
    fail the job permanently on the first attempt, no backoff."""
    make_job(store)

    async def handler(job):
        raise PapraError("GET file failed: 404 not found", 404)

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise KeyboardInterrupt()
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", idle_sleep_s=0, handler=handler)

    job = store.list_jobs()[0]
    assert job.status == "failed"  # permanent — no 3-attempt backoff
    assert job.attempts == 1
    assert "404" in job.error


async def test_worker_loop_backs_off_papra_5xx(store):
    """Papra 5xx keeps the normal retry policy: pending with backoff."""
    make_job(store)

    async def handler(job):
        raise PapraError("GET file failed: 503", 503)

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise KeyboardInterrupt()
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", idle_sleep_s=0, handler=handler)

    job = store.list_jobs()[0]
    assert job.status == "pending" and job.attempts == 1  # normal backoff


async def test_worker_loop_fails_llm_4xx_immediately(store):
    """4xx permanence must apply to ANY client error carrying .status, not
    just PapraError: an LLM 404 (model typo) must fail the job on the first
    attempt instead of re-downloading + re-rendering 3 times per job."""
    from papra_suite.llm_client import LLMError

    make_job(store)

    async def handler(job):
        raise LLMError("model not found", 404)

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise KeyboardInterrupt()
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", idle_sleep_s=0, handler=handler)

    job = store.list_jobs()[0]
    assert job.status == "failed"  # permanent — no 3-attempt backoff
    assert job.attempts == 1
    assert "model not found" in job.error


async def test_worker_loop_backs_off_llm_429(store):
    """429-exclusion pin: rate limiting (429) is a TRANSIENT condition — the
    job stays pending with backoff, NOT permanent, even though 429 sits in
    the 4xx range. Dropping the `!= 429` exclusion from the 4xx permanence
    check fails this test."""
    from papra_suite.llm_client import LLMError

    make_job(store)

    async def handler(job):
        raise LLMError("rate limited", 429)

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise KeyboardInterrupt()
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", idle_sleep_s=0, handler=handler)

    job = store.list_jobs()[0]
    assert job.status == "pending" and job.attempts == 1  # backoff, NOT permanent
    assert "rate limited" in job.error


async def test_worker_loop_fails_status_400_immediately(store):
    """Range-boundary pin (lower edge): status 400 is a permanent client
    error — first-attempt failure, no backoff. Mutating the range to
    `401 <= status` fails this test."""
    from papra_suite.llm_client import LLMError

    make_job(store)

    async def handler(job):
        raise LLMError("bad request", 400)

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise KeyboardInterrupt()
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", idle_sleep_s=0, handler=handler)

    job = store.list_jobs()[0]
    assert job.status == "failed" and job.attempts == 1  # permanent


async def test_worker_loop_backs_off_status_500(store):
    """Range-boundary pin (upper edge): status 500 is a server error — backoff,
    not permanent. Mutating the range to `status <= 500` fails this test."""
    from papra_suite.llm_client import LLMError

    make_job(store)

    async def handler(job):
        raise LLMError("internal error", 500)

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise KeyboardInterrupt()
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", idle_sleep_s=0, handler=handler)

    job = store.list_jobs()[0]
    assert job.status == "pending" and job.attempts == 1  # backoff, NOT permanent


async def test_worker_loop_backs_off_llm_5xx(store):
    """LLM 5xx keeps the normal retry policy: pending with backoff."""
    from papra_suite.llm_client import LLMError

    make_job(store)

    async def handler(job):
        raise LLMError("overloaded", 503)

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise KeyboardInterrupt()
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", idle_sleep_s=0, handler=handler)

    job = store.list_jobs()[0]
    assert job.status == "pending" and job.attempts == 1  # normal backoff


async def test_worker_loop_fails_ocr_when_a_page_is_truncated(store, config, caplog):
    """The real symptom of an output-capped server: page 1 comes back clean,
    page 2 is a half page with finish_reason="length" on an HTTP 200. The job
    must fail PERMANENTLY (attempts 1), write NO partial document back, and
    enqueue NO title job — otherwise a truncated excerpt gets titled and the
    job closes as done with nothing in the logs."""
    import fitz

    pdf = fitz.open()
    pdf.new_page()
    pdf.new_page()
    papra = StubPapra(pdf.tobytes())
    pdf.close()
    llm, calls = make_llm(["# Seite eins", "# Seite zwei (abge"],
                          finish_reasons=["stop", "length"])
    make_job(store)

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise KeyboardInterrupt()
        return original_claim(kind)

    store.claim_next_job = claim
    with caplog.at_level(logging.INFO):
        with pytest.raises(KeyboardInterrupt):
            await worker_loop(store, kind="ocr", idle_sleep_s=0,
                              handler=lambda job: run_ocr_job(papra, llm, job, config=config))

    jobs = store.list_jobs()
    ocr_jobs = [entry for entry in jobs if entry.kind == "ocr"]
    assert len(ocr_jobs) == 1
    assert ocr_jobs[0].status == "failed" and ocr_jobs[0].attempts == 1
    assert "length" in ocr_jobs[0].error
    # no title job — a truncated excerpt must not become the document's title
    assert [entry.kind for entry in jobs if entry.kind == "title"] == []
    assert papra.content_updates == []  # never a partial document
    assert calls["n"] == 2  # page 2 was attempted once, not retried
    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(errors) == 1  # the failure is visible exactly once


async def test_run_ocr_job_renders_off_the_event_loop(store, config, monkeypatch):
    """to_thread(render_page) pin: rendering is CPU-bound and must run OFF the
    event loop. A 5ms ticker runs concurrently with run_ocr_job; a render
    that sleeps 0.3s (simulated CPU work) must not stall the ticker. A
    direct (non-to_thread) call blocks the loop for ~0.3s and fails here."""
    import time

    import fitz

    pdf = fitz.open()
    pdf.new_page()
    papra = StubPapra(pdf.tobytes())
    pdf.close()
    llm, _ = make_llm(["# md"])

    def slow_render_page(pdf_bytes, index, *, dpi, pixel_budget):
        time.sleep(0.3)  # simulated CPU-bound render
        return b"fake-png"

    monkeypatch.setattr(pipeline_mod, "render_page", slow_render_page)

    gaps: list[float] = []
    stop_ticker = asyncio.Event()

    async def ticker():
        last = time.monotonic()
        while not stop_ticker.is_set():
            await asyncio.sleep(0.005)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    ticker_task = asyncio.get_running_loop().create_task(ticker())
    try:
        await run_ocr_job(papra, llm, make_job(store), config=config)
    finally:
        stop_ticker.set()
        await ticker_task

    assert gaps  # the ticker actually ran during the render
    assert max(gaps) < 0.1  # loop stayed responsive (~5ms cadence)


async def test_sweep_once_enqueues_for_unseen_pdfs_only(store):
    papra = StubPapra()
    result = await sweep_once(store, papra)

    assert isinstance(result, SweepResult)
    assert result.documents_checked == 2
    assert result.jobs_enqueued == 1  # doc_1 (pdf), not doc_2 (txt)
    assert result.non_pdf_skipped == 1
    assert store.document_seen("org_1", "doc_1")
    assert store.document_seen("org_1", "doc_2")
    job = store.claim_next_job("ocr")
    assert job.document_id == "doc_1"
    assert job.content_hash == hashlib.sha256(papra.pdf).hexdigest()
    assert job.payload.get("name") == "rechnung.pdf"


async def test_sweep_is_pdf_falls_back_to_pdf_name(store):
    """Extension-fallback pin: a listing entry with NO mimeType at all must
    be classified by its name — scan.pdf enqueued, notes.txt skipped. A
    mime-only _is_pdf mutant fails here."""

    class NoMimePapra(StubPapra):
        def __init__(self):
            super().__init__()
            self.documents = {"org_1": {
                "doc_scan": {"id": "doc_scan", "name": "scan.pdf"},
                "doc_notes": {"id": "doc_notes", "name": "notes.txt"},
            }}

    papra = NoMimePapra()

    result = await sweep_once(store, papra)

    assert result.jobs_enqueued == 1  # scan.pdf made it in via the .pdf name
    assert result.non_pdf_skipped == 1  # notes.txt skipped
    job = store.claim_next_job("ocr")
    assert job is not None and job.document_id == "doc_scan"
    assert store.document_seen("org_1", "doc_notes")  # skip is permanent


async def test_sweep_continues_past_download_failure(store):
    class FlakyDownloadPapra(StubPapra):
        """download_file fails for doc_1, succeeds for doc_2 (both PDFs)."""

        def __init__(self):
            super().__init__()
            self.documents = {
                "org_1": {
                    "doc_1": {"id": "doc_1", "name": "broken.pdf", "mimeType": "application/pdf"},
                    "doc_2": {"id": "doc_2", "name": "fine.pdf", "mimeType": "application/pdf"},
                },
            }

        async def download_file(self, org_id, document_id):
            if document_id == "doc_1":
                raise RuntimeError("404 not found")
            return self.pdf

    papra = FlakyDownloadPapra()

    first = await sweep_once(store, papra)

    assert first.errors == 1
    assert first.jobs_enqueued == 1  # doc_2 enrolled despite doc_1 failing
    assert not store.document_seen("org_1", "doc_1")  # failed doc stays unseen
    assert store.document_seen("org_1", "doc_2")

    # a second sweep retries doc_1 (and fails again) but does not re-enqueue doc_2
    second = await sweep_once(store, papra)
    assert second.errors == 1
    assert second.jobs_enqueued == 0
    assert not store.document_seen("org_1", "doc_1")


async def test_sweep_continues_past_org_listing_failure(store):
    store.create_org(papra_org_id="org_2", name="Org 2")

    class OrgListingFailsPapra(StubPapra):
        """list_documents raises for org_1; org_2 still gets processed."""

        def __init__(self):
            super().__init__()
            self.documents = {
                "org_1": {},
                "org_2": {
                    "doc_3": {"id": "doc_3", "name": "report.pdf", "mimeType": "application/pdf"},
                },
            }

        async def list_documents(self, org_id, *, page_index=0, page_size=100,
                                 search_query=None, sort_field="createdAt",
                                 sort_order="desc"):
            if org_id == "org_1":
                raise RuntimeError("500 internal error")
            docs = list(self.documents[org_id].values())
            page = docs[page_index * page_size:(page_index + 1) * page_size]
            return page, len(docs)

    result = await sweep_once(store, OrgListingFailsPapra())

    assert result.errors == 1
    assert result.jobs_enqueued == 1  # org_2's PDF enrolled despite org_1 failing
    job = store.claim_next_job("ocr")
    assert job.papra_org_id == "org_2" and job.document_id == "doc_3"


class ManyDocsPapra(StubPapra):
    """N PDFs in org_1; records which page indexes the sweep requested."""

    def __init__(self, count: int):
        super().__init__()
        self.documents = {"org_1": {
            f"doc_{i}": {"id": f"doc_{i}", "name": f"doc_{i}.pdf",
                         "mimeType": "application/pdf"}
            for i in range(count)
        }}
        self.pages_requested: list[int] = []

    async def list_documents(self, org_id, *, page_index=0, page_size=100,
                             search_query=None, sort_field="createdAt",
                             sort_order="desc"):
        self.pages_requested.append(page_index)
        return await super().list_documents(
            org_id, page_index=page_index, page_size=page_size,
            sort_field=sort_field, sort_order=sort_order)


async def test_sweep_once_paginates_past_first_page(store):
    papra = ManyDocsPapra(75)  # 50 + 25: needs both pages

    result = await sweep_once(store, papra)

    assert result.documents_checked == 75
    assert result.jobs_enqueued == 75  # every PDF across BOTH pages
    assert papra.pages_requested == [0, 1]  # stopped at the short page


async def test_sweep_once_stops_at_page_boundary(store):
    papra = ManyDocsPapra(SWEEP_PAGE_SIZE)  # exactly one full page

    result = await sweep_once(store, papra)

    assert result.documents_checked == SWEEP_PAGE_SIZE
    assert result.jobs_enqueued == SWEEP_PAGE_SIZE
    # page 0 is full -> one confirming fetch of page 1 (empty) -> stop
    assert papra.pages_requested == [0, 1]


class NeverShortPagePapra(StubPapra):
    """A listing endpoint that ignores page_index: every page comes back full."""

    def __init__(self):
        super().__init__()
        self.pages_requested = 0
        self.documents = {"org_1": {
            f"doc_{i}": {"id": f"doc_{i}", "name": f"doc_{i}.txt",
                         "mimeType": "text/plain"}
            for i in range(SWEEP_PAGE_SIZE)
        }}

    async def list_documents(self, org_id, *, page_index=0, page_size=100,
                             search_query=None, sort_field="createdAt",
                             sort_order="desc"):
        self.pages_requested += 1  # page_index deliberately ignored
        return list(self.documents[org_id].values()), len(self.documents[org_id])


async def test_sweep_once_terminates_on_never_short_listing(store, caplog):
    papra = NeverShortPagePapra()

    with caplog.at_level(logging.WARNING):
        result = await sweep_once(store, papra)

    # the hard cap ends pagination instead of looping forever
    assert papra.pages_requested == MAX_SWEEP_PAGES
    assert result.documents_checked == MAX_SWEEP_PAGES * SWEEP_PAGE_SIZE
    assert any(record.levelno == logging.WARNING and "org_1" in record.getMessage()
               for record in caplog.records)


async def test_sweep_covers_large_orgs_across_passes(store, caplog):
    """Sweep-offset pin: an org larger than MAX_SWEEP_PAGES * SWEEP_PAGE_SIZE
    documents must EVENTUALLY have every document swept. Pass 1 exhausts the
    cap (5000 docs) and persists the page offset; pass 2 resumes and reaches
    the short page (offset reset); pass 3 finds nothing left. A restart-at-
    page-0 mutant never sweeps docs 5000-5999 and fails here."""
    papra = ManyDocsPapra(6000)  # 120 full pages of 50

    with caplog.at_level(logging.WARNING):
        first = await sweep_once(store, papra)

    assert first.jobs_enqueued == MAX_SWEEP_PAGES * SWEEP_PAGE_SIZE  # 5000
    assert store.get_setting("sweep_offset:org_1") == "100"  # resume point saved
    assert any(record.levelno == logging.WARNING and "org_1" in record.getMessage()
               and "1000" in record.getMessage()  # remaining estimate
               for record in caplog.records)

    second = await sweep_once(store, papra)
    assert second.jobs_enqueued == 1000  # docs 5000-5999, resumed at page 100
    assert store.get_setting("sweep_offset:org_1") == "0"  # short page -> reset

    third = await sweep_once(store, papra)
    assert third.jobs_enqueued == 0  # fully caught up; nothing left to enqueue
    # pass 3 re-walked the seen prefix from page 0 and hit the cap again
    # (bounded per-pass work; the seen-table gate skips processed docs)
    assert store.get_setting("sweep_offset:org_1") == "100"

    fourth = await sweep_once(store, papra)
    assert fourth.jobs_enqueued == 0
    assert fourth.documents_checked == 1000  # resumed at 100, short page at 120
    assert store.get_setting("sweep_offset:org_1") == "0"  # reset on the short page
    # every one of the 6000 documents made it into the seen table eventually
    assert all(store.document_seen("org_1", f"doc_{i}") for i in range(6000))


async def test_sweep_uses_listing_hash_without_download(store):
    """Listing-hash pin: a document whose listing entry carries
    originalSha256Hash must be enqueued with THAT hash and NEVER downloaded —
    the download+hash leg doubled the transfer and pulled the full document
    into RAM solely to recompute a value the listing already had. Reverting
    to download-always fails the zero-download spy."""

    class HashListingPapra(StubPapra):
        def __init__(self):
            super().__init__()
            self.download_calls = 0
            self.documents = {"org_1": {
                "doc_h": {"id": "doc_h", "name": "rechnung.pdf",
                          "mimeType": "application/pdf",
                          "originalSha256Hash": "listing-hash-abc123"},
            }}

        async def download_file(self, org_id, document_id):
            self.download_calls += 1
            return self.pdf

    papra = HashListingPapra()

    result = await sweep_once(store, papra)

    assert result.jobs_enqueued == 1
    job = store.claim_next_job("ocr")
    assert job is not None and job.document_id == "doc_h"
    # the LISTING's hash, not a recompute of the downloaded body
    assert job.content_hash == "listing-hash-abc123"
    assert papra.download_calls == 0  # ZERO downloads: hash came from the listing


async def test_sweep_falls_back_to_download_hash_when_listing_hash_absent(store, caplog):
    """Fallback pin: a listing entry WITHOUT originalSha256Hash (older or
    non-compliant server) still gets enqueued via the download+hash path,
    with the fallback logged."""

    class NoHashListingPapra(StubPapra):
        def __init__(self):
            super().__init__()
            self.download_calls = 0
            self.documents = {"org_1": {
                "doc_f": {"id": "doc_f", "name": "rechnung.pdf",
                          "mimeType": "application/pdf"},
            }}

        async def download_file(self, org_id, document_id):
            self.download_calls += 1
            return self.pdf

    papra = NoHashListingPapra()

    with caplog.at_level(logging.INFO):
        result = await sweep_once(store, papra)

    assert result.jobs_enqueued == 1
    assert papra.download_calls == 1  # fallback path did download
    job = store.claim_next_job("ocr")
    assert job.content_hash == hashlib.sha256(papra.pdf).hexdigest()
    assert any("originalSha256Hash" in record.getMessage()
               and "doc_f" in record.getMessage()
               for record in caplog.records)  # the fallback is visible in logs


async def test_sweep_self_heals_corrupt_offset(store):
    """Corrupt-offset pin: a settings row sweep_offset:<org> holding garbage
    (hand-edited or legacy data) must self-heal to offset 0 — the sweep
    completes and enqueues normally. Mutating the guard to raise fails this
    test with the propagated ValueError."""
    store.set_setting("sweep_offset:org_1", "garbage")

    result = await sweep_once(store, StubPapra())

    assert result.errors == 0
    assert result.jobs_enqueued == 1  # swept from healed offset 0
    assert store.document_seen("org_1", "doc_1")
    # healed and then reset to 0 by the short page
    assert store.get_setting("sweep_offset:org_1") == "0"


async def test_sweep_once_is_idempotent(store):
    papra = StubPapra()
    first = await sweep_once(store, papra)
    second = await sweep_once(store, papra)
    assert first.jobs_enqueued == 1 and second.jobs_enqueued == 0
    assert second.documents_checked == 2


async def test_sweep_skips_oversized_document(store, caplog):
    """A listing entry whose originalSize exceeds the cap is skipped via
    metadata — recorded seen, no job, and NEVER downloaded."""

    class OversizedPapra(StubPapra):
        def __init__(self):
            super().__init__()
            self.download_calls = 0
            self.documents = {"org_1": {
                "doc_big": {
                    "id": "doc_big", "name": "huge.pdf",
                    "mimeType": "application/pdf",
                    "originalSize": 200 * 1024 * 1024,
                },
            }}

        async def download_file(self, org_id, document_id):
            self.download_calls += 1
            return self.pdf

    papra = OversizedPapra()

    with caplog.at_level(logging.WARNING):
        result = await sweep_once(store, papra)

    assert result.oversized_skipped == 1
    assert result.jobs_enqueued == 0
    assert store.document_seen("org_1", "doc_big")  # skip is permanent
    assert papra.download_calls == 0  # never downloaded the huge body
    assert any("oversized" in record.getMessage() and "doc_big" in record.getMessage()
               for record in caplog.records)
