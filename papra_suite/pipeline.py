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


async def serve(config, *, stop: asyncio.Event | None = None,
                store: Store | None = None, papra: PapraClient | None = None,
                ocr_llm: LLMClient | None = None,
                title_llm: LLMClient | None = None) -> None:
    """The process runtime: pollers, OCR/title workers, sweep.

    Runs until the stop event is set (or the surrounding task is cancelled),
    then cancels its tasks and closes ONLY the clients and store it
    constructed — injected ones belong to the caller (owns_* pattern).
    """
    from papra_suite.logging_setup import register_config_secrets, setup_logging

    stop = stop or asyncio.Event()
    owns_store = store is None
    if store is None:
        store = Store(config.data_dir / "state.db")
    store.migrate()
    recovered = store.recover_running_jobs()
    if recovered:
        logger.info("recovered %d stale running job(s) at startup", recovered)
    setup_logging(store)
    register_config_secrets(config)

    built_clients: list = []
    if papra is None:
        papra = PapraClient(config.papra_base_url, config.papra_api_key)
        built_clients.append(papra)
    if ocr_llm is None:
        ocr_llm = LLMClient(config.llm_base_url, config.llm_api_key,
                            config.ocr_model, timeout_s=300.0)
        built_clients.append(ocr_llm)
    if title_llm is None:
        title_llm = LLMClient(config.llm_base_url, config.llm_api_key,
                              config.title_model, timeout_s=300.0)
        built_clients.append(title_llm)

    tasks = []
    for mailbox in store.list_mailboxes(enabled_only=True):
        tasks.append(asyncio.create_task(_poll_loop(store, papra, mailbox)))
    for _ in range(config.ocr_concurrency):
        tasks.append(asyncio.create_task(worker_loop(
            store, kind="ocr",
            handler=lambda job: run_ocr_job(papra, ocr_llm, job, config=config),
        )))
    tasks.append(asyncio.create_task(worker_loop(
        store, kind="title",
        handler=lambda job: run_title_job(papra, title_llm, job),
    )))
    if config.sweep_interval_s > 0:
        tasks.append(asyncio.create_task(
            _sweep_loop(store, papra, config.sweep_interval_s)))

    stop_waiter = asyncio.create_task(stop.wait())
    try:
        await stop_waiter
    finally:
        stop_waiter.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for client in built_clients:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001 - one failing close must not skip the rest
                logger.exception("error closing client during shutdown")
        if owns_store:
            try:
                store.close()
            except Exception:  # noqa: BLE001
                logger.exception("error closing store during shutdown")


async def _poll_loop(store: Store, papra: PapraClient, mailbox) -> None:
    from papra_suite.mail import poll_mailbox_once

    while True:
        org = store.get_org(mailbox.org_id)
        if org is not None:
            try:
                await poll_mailbox_once(store, papra, mailbox, org)
            except Exception:  # noqa: BLE001 - defense in depth (M2 carry-over)
                logger.exception("poll loop error for mailbox %s", mailbox.id)
        await asyncio.sleep(mailbox.poll_interval_s)


async def _sweep_loop(store: Store, papra: PapraClient, interval_s: int) -> None:
    while True:
        try:
            result = await sweep_once(store, papra)
            if result.jobs_enqueued or result.non_pdf_skipped:
                logger.info("sweep: %d checked, %d enqueued, %d non-pdf",
                            result.documents_checked, result.jobs_enqueued,
                            result.non_pdf_skipped)
        except Exception:  # noqa: BLE001
            logger.exception("sweep loop error")
        await asyncio.sleep(interval_s)


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
