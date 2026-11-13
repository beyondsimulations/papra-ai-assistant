import hashlib
import json

import httpx
import pytest

from papra_suite.config import load_config
from papra_suite.pipeline import (
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

    async def update_document(self, org_id, document_id, *, name=None, content=None):
        self.content_updates.append((document_id, name, content))
        if name:
            self.documents[org_id][document_id]["name"] = name

    async def list_documents(self, org_id, *, page_size=100, search_query=None,
                             sort_field="createdAt", sort_order="desc"):
        docs = list(self.documents[org_id].values())
        return docs, len(docs)


def make_llm(responses: list[str]):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        assert body["messages"][0]["role"] == "system"
        content = responses[min(calls["n"], len(responses) - 1)]
        calls["n"] += 1
        return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})

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


def test_on_job_done_ignores_title_kind(store):
    job = make_job(store, kind="title", content_hash="th",
                   payload={"name": "n", "excerpt": "e"})
    on_job_done(store, job, ocr_result="unused")
    assert len(store.list_jobs()) == 1


async def test_run_title_job_writes_generated_name(store):
    papra = StubPapra()
    llm, _ = make_llm(["Rechnung 42 – Möbel"])
    job = make_job(store, kind="title", content_hash="th",
                   payload={"name": "rechnung.pdf", "excerpt": "Rechnung Nr. 42"})

    await run_title_job(papra, llm, job)

    assert papra.content_updates == [("doc_1", "Rechnung 42 – Möbel", None)]


async def test_worker_loop_processes_and_transitions(store, config):
    import fitz

    pdf = fitz.open()
    pdf.new_page()
    papra = StubPapra(pdf.tobytes())
    pdf.close()
    llm, _ = make_llm(["# md"])
    make_job(store)  # enqueue one ocr job (doc_1)

    # run the loop until the queue drains, then stop it via a failing claim patch
    original_claim = store.claim_next_job
    state = {"claims": 0}

    class _StopLoop(Exception):
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

        async def list_documents(self, org_id, *, page_size=100, search_query=None,
                                 sort_field="createdAt", sort_order="desc"):
            if org_id == "org_1":
                raise RuntimeError("500 internal error")
            docs = list(self.documents[org_id].values())
            return docs, len(docs)

    result = await sweep_once(store, OrgListingFailsPapra())

    assert result.errors == 1
    assert result.jobs_enqueued == 1  # org_2's PDF enrolled despite org_1 failing
    job = store.claim_next_job("ocr")
    assert job.papra_org_id == "org_2" and job.document_id == "doc_3"


async def test_sweep_once_is_idempotent(store):
    papra = StubPapra()
    first = await sweep_once(store, papra)
    second = await sweep_once(store, papra)
    assert first.jobs_enqueued == 1 and second.jobs_enqueued == 0
    assert second.documents_checked == 2
