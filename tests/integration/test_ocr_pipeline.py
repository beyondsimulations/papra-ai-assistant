"""End-to-end integration: OCR + title through the FakeLLMServer, and the
sweep picking up externally-uploaded PDFs — against the real scratch Papra."""

import fitz
import pytest

from papra_suite.config import load_config
from papra_suite.llm_client import LLMClient
from papra_suite.papra_client import PapraClient
from papra_suite.pipeline import on_job_done, run_ocr_job, run_title_job, sweep_once
from papra_suite.store import Store
from tests.fakes.llm_server import FakeLLMServer

pytestmark = pytest.mark.integration

SEARCH_PHRASE = "quokka-marmalade-ocr"  # only in OCR content, never in the source PDF
PAGE_ONE_MARKDOWN = "# Rechnung Nr. 42\n\nVielen Dank für Ihre Bestellung."
PAGE_TWO_MARKDOWN = f"## Zahlungsbedingungen\n\nZahlbar bis 15.11.2026.\n\n{SEARCH_PHRASE}"


def make_german_pdf() -> bytes:
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((72, 72), "Rechnung Nr. 42")
    page.insert_text((72, 120), "Vielen Dank für Ihre Bestellung.")
    page = pdf.new_page()
    page.insert_text((72, 72), "Zahlungsbedingungen")
    page.insert_text((72, 120), "Zahlbar bis 15.11.2026.")
    data = pdf.tobytes()
    pdf.close()
    return data


@pytest.fixture
async def stack(tmp_path, papra_stack):
    base_url, org_id, api_key = papra_stack
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=org_id, name="Integration Org")
    config = load_config({
        "PAPRA_BASE_URL": base_url, "PAPRA_API_KEY": api_key,
        "LLM_BASE_URL": "http://fake.invalid", "LLM_API_KEY": "k",
        "TITLE_MODEL": "t", "OCR_MODEL": "o", "ADMIN_PASSWORD": "p",
        "DATA_DIR": str(tmp_path),
    })
    papra = PapraClient(base_url, api_key)
    yield store, papra, org_id, config
    await papra.aclose()
    store.close()


async def test_ocr_and_title_end_to_end(stack):
    store, papra, org_id, config = stack
    pdf = make_german_pdf()
    document = await papra.create_document(org_id, "rechnung-42.pdf", pdf)
    document_id = document["id"]
    job_id = store.enqueue_job(kind="ocr", papra_org_id=org_id,
                               document_id=document_id,
                               content_hash="integration-hash",
                               payload={"file_hash": "integration-hash",
                                        "name": "rechnung-42.pdf"})
    job = store.get_job(job_id)

    server = FakeLLMServer()
    # the fake "vision model": page 1 then page 2 markdown, then the title
    server.responses.extend([
        PAGE_ONE_MARKDOWN, PAGE_TWO_MARKDOWN, "Rechnung 42 – Bestellung",
    ])
    server.start()
    ocr_llm = title_llm = None
    try:
        ocr_llm = LLMClient(server.url, "k", "ocr-model")
        title_llm = LLMClient(server.url, "k", "title-model")

        # OCR worker
        markdown = await run_ocr_job(papra, ocr_llm, job, config=config)
        assert PAGE_ONE_MARKDOWN in markdown and PAGE_TWO_MARKDOWN in markdown
        store.mark_job_done(job.id)
        on_job_done(store, job, ocr_result=markdown)

        # transition enqueued a title job
        title_jobs = [j for j in store.list_jobs() if j.kind == "title"]
        assert len(title_jobs) == 1

        # content is searchable in Papra (spec acceptance 5's mail-path analog);
        # SEARCH_PHRASE exists only in the OCR-written content, never in the source PDF
        documents, count = await papra.list_documents(
            org_id, search_query=SEARCH_PHRASE)
        assert document_id in {d["id"] for d in documents}

        # title worker
        await run_title_job(papra, title_llm, title_jobs[0])
        updated = await papra.get_document(org_id, document_id)
        assert updated["name"] == "Rechnung 42 – Bestellung"

        # the fake saw: 2 page requests (with images) + 1 title request
        assert len(server.requests) == 3
        page_request = server.requests[0]
        content_part = page_request["messages"][1]["content"][1]
        assert content_part["type"] == "image_url"
        # page 2 request also carries an image_url content part
        page_two_request = server.requests[1]
        page_two_part = page_two_request["messages"][1]["content"][1]
        assert page_two_part["type"] == "image_url"
        # the title request's user message is a plain string (no image)
        title_request = server.requests[2]
        assert isinstance(title_request["messages"][1]["content"], str)
    finally:
        server.stop()
        if ocr_llm is not None:
            await ocr_llm.aclose()
        if title_llm is not None:
            await title_llm.aclose()


async def test_sweep_enqueues_externally_uploaded_pdf(stack):
    store, papra, org_id, config = stack
    document = await papra.create_document(org_id, "extern-upload.pdf",
                                           make_german_pdf())

    result = await sweep_once(store, papra)

    assert result.documents_enqueued == 1
    job = store.claim_next_job("ocr")
    assert job.document_id == document["id"]
    assert job.payload["name"] == "extern-upload.pdf"
    # second sweep is a no-op
    assert (await sweep_once(store, papra)).documents_enqueued == 0
