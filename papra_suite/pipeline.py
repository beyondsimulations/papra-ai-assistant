"""Pipeline orchestration: document ingestion and job chaining.

M2 scope: the ingestion path (record document as seen + enqueue OCR job).
M3 adds the OCR/title worker loops and the central transition handler.
"""

import asyncio
import hashlib
import logging
from dataclasses import dataclass

from papra_suite.llm_client import LLMClient
from papra_suite.ocr import OCR_SYSTEM_PROMPT, assemble_markdown, render_pages
from papra_suite.papra_client import PapraClient
from papra_suite.store import Job, Store
from papra_suite.titles import generate_title

logger = logging.getLogger(__name__)

SWEEP_PAGE_SIZE = 50


@dataclass
class SweepResult:
    documents_checked: int = 0
    jobs_enqueued: int = 0
    non_pdf_skipped: int = 0
    errors: int = 0


async def run_ocr_job(papra: PapraClient, ocr_llm: LLMClient, job: Job, *,
                      config) -> str:
    """Download, render, transcribe per page, write back; returns the markdown."""
    pdf_bytes = await papra.download_file(job.papra_org_id, job.document_id)
    pages = render_pages(
        pdf_bytes, dpi=config.ocr_dpi, max_pages=config.ocr_max_pages,
        pixel_budget=config.ocr_page_pixel_budget,
    )
    page_markdowns = []
    for index, png in enumerate(pages):
        logger.info("ocr: document %s page %d/%d", job.document_id, index + 1, len(pages))
        page_markdowns.append(
            await ocr_llm.chat(
                [
                    {"role": "system", "content": OCR_SYSTEM_PROMPT},
                    {"role": "user", "content": "Transcribe this page."},
                ],
                image=png,
            )
        )
    markdown = assemble_markdown(page_markdowns)
    await papra.update_document(job.papra_org_id, job.document_id, content=markdown)
    return markdown


def on_job_done(store: Store, job: Job, *, ocr_result: str) -> None:
    """Central transition handler (decisions §2): ocr done -> enqueue title."""
    if job.kind != "ocr":
        return
    content_hash = hashlib.sha256(ocr_result.encode()).hexdigest()
    store.enqueue_job(
        kind="title",
        papra_org_id=job.papra_org_id,
        document_id=job.document_id,
        content_hash=content_hash,
        payload={"name": job.payload.get("name", ""), "excerpt": ocr_result[:2000]},
    )


async def run_title_job(papra: PapraClient, title_llm: LLMClient, job: Job) -> None:
    title = await generate_title(
        title_llm, filename=job.payload.get("name", "document.pdf"),
        excerpt=job.payload.get("excerpt", ""),
    )
    await papra.update_document(job.papra_org_id, job.document_id, name=title)


async def worker_loop(store: Store, *, kind: str, handler,
                      idle_sleep_s: float = 1.0) -> None:
    """Claim-and-run loop for one job kind. Never raises."""
    while True:
        job = store.claim_next_job(kind)
        if job is None:
            await asyncio.sleep(idle_sleep_s)
            continue
        try:
            result = await handler(job)
            store.mark_job_done(job.id)
            if isinstance(result, str):
                on_job_done(store, job, ocr_result=result)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - the loop must survive
            logger.error("job %s (%s) failed: %s", job.id, kind, error)
            try:
                store.mark_job_failed(job.id, str(error))
            except ValueError:
                logger.error("job %s vanished before failure could be recorded", job.id)


def _is_pdf(document: dict) -> bool:
    mime = document.get("mimeType") or ""
    if mime == "application/pdf":
        return True
    return (document.get("name") or "").lower().endswith(".pdf")


async def sweep_once(store: Store, papra: PapraClient) -> SweepResult:
    """Record and enqueue OCR for externally-ingested documents (decisions §4)."""
    result = SweepResult()
    for org in store.list_orgs():
        try:
            documents, _count = await papra.list_documents(
                org.papra_org_id, page_size=SWEEP_PAGE_SIZE,
                sort_field="createdAt", sort_order="desc",
            )
        except Exception as error:  # noqa: BLE001 - one org must not wedge the sweep
            logger.error("sweep: listing documents for org %s failed: %s",
                         org.papra_org_id, error)
            result.errors += 1
            continue
        for document in documents:
            result.documents_checked += 1
            document_id = document["id"]
            if store.document_seen(org.papra_org_id, document_id):
                continue
            try:
                if _is_pdf(document):
                    file_bytes = await papra.download_file(org.papra_org_id, document_id)
                    file_hash = hashlib.sha256(file_bytes).hexdigest()
                    enqueue_ocr_job(
                        store, papra_org_id=org.papra_org_id, document_id=document_id,
                        file_hash=file_hash, name=document.get("name", "document.pdf"),
                        mime="application/pdf", ingested_via="sweep",
                    )
                    result.jobs_enqueued += 1
                else:
                    store.record_document_seen(
                        papra_org_id=org.papra_org_id, papra_document_id=document_id,
                        name=document.get("name", ""), mime=document.get("mimeType", ""),
                        ingested_via="sweep",
                    )
                    result.non_pdf_skipped += 1
            except Exception as error:  # noqa: BLE001 - one document must not wedge the sweep
                logger.error("sweep: processing document %s in org %s failed: %s",
                             document_id, org.papra_org_id, error)
                result.errors += 1
                continue
    return result


def enqueue_ocr_job(store: Store, *, papra_org_id: str, document_id: str,
                    file_hash: str, name: str, mime: str, ingested_via: str) -> str:
    """Enqueue an OCR job for an ingested document, then record it as seen.

    The job's idempotency key is (kind='ocr', org, document_id, file_hash):
    re-ingesting the same document with the same content is a no-op that
    returns the existing job id; changed content yields a new job.

    Ordering is crash-safety critical: enqueue FIRST, record seen SECOND.
    If the process dies between the two writes, the document is NOT in the
    seen-table, so the M3 sweep re-enqueues it (the idempotent enqueue is a
    no-op) and records it as seen — the pipeline self-heals. The reverse
    order (seen first) strands the document: the sweep skips seen documents,
    so a crash after the seen-write leaves the document with zero jobs
    forever.
    """
    job_id = store.enqueue_job(
        kind="ocr",
        papra_org_id=papra_org_id,
        document_id=document_id,
        content_hash=file_hash,
        payload={"file_hash": file_hash, "name": name},
    )
    store.record_document_seen(
        papra_org_id=papra_org_id,
        papra_document_id=document_id,
        name=name,
        mime=mime,
        ingested_via=ingested_via,
    )
    return job_id
