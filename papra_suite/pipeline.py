"""Pipeline orchestration: document ingestion and job chaining.

M2 scope: the ingestion path (record document as seen + enqueue OCR job).
M3 adds the OCR/title worker loops and the central transition handler.
"""

import asyncio
import hashlib
import logging
import sqlite3
from dataclasses import dataclass

from papra_suite.allowlist import AllowList
from papra_suite.indexing import CONTENT_DELIVERY_IMPLEMENTED, run_index_job
from papra_suite.kreuzakt_client import KreuzaktClient
from papra_suite.llm_client import LLMClient
from papra_suite.ocr import OCR_SYSTEM_PROMPT, assemble_markdown, page_count, render_page
from papra_suite.papra_client import PapraClient
from papra_suite.store import Job, Store
from papra_suite.titles import generate_title

logger = logging.getLogger(__name__)

SWEEP_PAGE_SIZE = 50
MAX_SWEEP_PAGES = 100  # hard cap: a listing endpoint that ignores pageIndex must not loop forever
DEFAULT_DOCUMENT_NAME = "document.pdf"
# Upper bound on downloaded document bytes for OCR and the sweep's hash step:
# a hostile/huge document must fail visibly (or be skipped via listing
# metadata) instead of being pulled into memory unbounded.
OCR_MAX_DOCUMENT_BYTES = 100 * 1024 * 1024


@dataclass
class SweepResult:
    documents_checked: int = 0
    jobs_enqueued: int = 0
    non_pdf_skipped: int = 0
    errors: int = 0
    oversized_skipped: int = 0


async def run_ocr_job(papra: PapraClient, ocr_llm: LLMClient, job: Job, *,
                      config) -> str:
    """Download, render, transcribe per page, write back; returns the markdown.

    Pages are rendered LAZILY, one at a time, off the event loop
    (asyncio.to_thread): rendering is CPU-bound (0.6-3s for 50 pages) and
    holding every page PNG across the per-page LLM calls would balloon
    memory (~290MB for a crafted 50-page document).
    """
    pdf_bytes = await papra.download_file(job.papra_org_id, job.document_id)
    if len(pdf_bytes) > OCR_MAX_DOCUMENT_BYTES:
        raise ValueError(f"document too large for OCR: {len(pdf_bytes)} bytes")
    total = page_count(pdf_bytes)
    limit = min(total, config.ocr_max_pages)
    if total > config.ocr_max_pages:
        logger.warning(
            "document has %d pages; skipped %d additional pages beyond max_pages=%d",
            total, total - config.ocr_max_pages, config.ocr_max_pages,
        )
    if limit == 0:
        raise ValueError("document has no pages to transcribe")
    page_markdowns = []
    for index in range(limit):
        png = await asyncio.to_thread(
            render_page, pdf_bytes, index,
            dpi=config.ocr_dpi, pixel_budget=config.ocr_page_pixel_budget,
        )
        logger.info("ocr: document %s page %d/%d", job.document_id, index + 1, limit)
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
        payload={"name": job.payload.get("name", DEFAULT_DOCUMENT_NAME),
                 "excerpt": ocr_result[:2000]},
    )


async def run_title_job(papra: PapraClient, title_llm: LLMClient, job: Job) -> None:
    """Generate and write a title — unless the document moved on (decisions §2).

    A stale title job (content v1, still in backoff) must never overwrite a
    title that a newer job (v2) already wrote from newer content: hash the
    document's CURRENT content and compare to the job's content_hash first;
    on mismatch, skip silently (the worker still marks the job done).
    """
    document = await papra.get_document(job.papra_org_id, job.document_id)
    current_content = document.get("content") or ""
    current_hash = hashlib.sha256(current_content.encode()).hexdigest()
    if current_hash != job.content_hash:
        logger.info(
            "stale title job skipped: document content changed "
            "(job %s for document %s)", job.id, job.document_id,
        )
        return
    title = await generate_title(
        title_llm, filename=job.payload.get("name", DEFAULT_DOCUMENT_NAME),
        excerpt=job.payload.get("excerpt", ""),
    )
    await papra.update_document(job.papra_org_id, job.document_id, name=title)


async def serve(config, *, stop: asyncio.Event | None = None,
                store: Store | None = None, papra: PapraClient | None = None,
                ocr_llm: LLMClient | None = None,
                title_llm: LLMClient | None = None,
                kreuzakt: KreuzaktClient | None = None) -> None:
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
    _sync_settings_from_config(store, config)
    recovered = store.recover_running_jobs()
    if recovered:
        logger.info("recovered %d stale running job(s) at startup", recovered)
    # Ingest events are pruned here at startup and once per sweep pass rather
    # than by a periodic task of their own. The window is 90 days and a container
    # restarts far more often than that, so a dedicated pruner would be a second
    # thing to get right for no gain — and unlike the jobs table, a stale row here
    # costs disk and a longer listing, never correctness.
    _prune_ingest_events_quietly(store)
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
    if kreuzakt is None:
        kreuzakt = KreuzaktClient(config.kreuzakt_base_url)
        built_clients.append(kreuzakt)

    tasks = []
    for mailbox in store.list_mailboxes(enabled_only=True):
        tasks.append(asyncio.create_task(
            _poll_loop(store, papra, mailbox, allowlist=config.allowed_senders),
            name=f"poll:{mailbox.id}"))
    for _ in range(config.ocr_concurrency):
        tasks.append(asyncio.create_task(worker_loop(
            store, kind="ocr",
            handler=lambda job: run_ocr_job(papra, ocr_llm, job, config=config),
        ), name="ocr-worker"))
    tasks.append(asyncio.create_task(worker_loop(
        store, kind="title",
        handler=lambda job: run_title_job(papra, title_llm, job),
    ), name="title-worker"))
    # ONE index worker, on purpose. Correlating an upload needs its queue row inside
    # the newest-100 window, so uploads are serial by construction: a second worker
    # would interleave uploads and make each other's correlation fail.
    tasks.append(asyncio.create_task(worker_loop(
        store, kind="index",
        handler=lambda job: run_index_job(
            kreuzakt, store, job, papra=papra,
            poll_s=config.index_poll_interval_s,
            wait_timeout_s=config.index_wait_timeout_s,
            max_attempts=config.index_max_attempts),
    ), name="index-worker"))
    if config.sweep_interval_s > 0:
        tasks.append(asyncio.create_task(
            _sweep_loop(store, papra, config.sweep_interval_s), name="sweep"))

    def _supervise(task: asyncio.Task, name: str) -> None:
        """Log a long-lived task's exit, loudly only when it is NOT designed.

        A task that RAISED is a real crash: ERROR "exited unexpectedly".
        A task that returned CLEANLY exited by design (a 3-strike-disabled
        or runtime-deleted mailbox stops its poll loop normally): INFO,
        not a false crash alarm in the log stream.
        """
        def _on_done(done: asyncio.Task) -> None:
            if done.cancelled():
                return  # expected shutdown path
            error = done.exception()
            if error is not None:
                logger.error("task %s exited unexpectedly: %s", name, error)
            else:
                logger.info("task %s exited", name)
        task.add_done_callback(_on_done)

    for task in tasks:
        _supervise(task, task.get_name())

    stop_waiter = asyncio.create_task(stop.wait())
    try:
        await stop_waiter
    finally:
        stop_waiter.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(stop_waiter, *tasks, return_exceptions=True)
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


def _sync_settings_from_config(store: Store, config) -> None:
    """Write the delivery flags from `Config` into `settings` at every startup.

    The gate is read out of the store — by the worker loop, by the CLI, and (from
    Task 7) from inside `deliver_content` itself — because a flag only the scheduler
    consults can be bypassed by any other caller. `Config` is what the operator
    actually started the process with, so it is written on startup and the readers
    cannot disagree about which build is running.

    `index_metadata_delivery` is seeded as given although nothing reads it yet:
    writing one flag and not its sibling would make the settings table a half-truth
    about the process that owns it. `index_content_delivery` is the exception, and it
    is a fact about the build rather than about the operator: see the warning below.
    """
    if config.index_content_delivery and not CONTENT_DELIVERY_IMPLEMENTED:
        # Loud, and refused rather than obeyed. Honoured, this flag stops the legacy
        # OCR enqueue — and in a build with no delivery path that is not "the bridge
        # will write the text instead", it is "nothing writes the text".
        logger.warning(
            "PAPRA_INDEX_CONTENT_DELIVERY is set but this build has no content "
            "delivery path yet: leaving the legacy OCR writer in charge of Papra's "
            "text. Task 7's build is the one that can take the flag.")
    store.set_setting("index_content_delivery",
                      "true" if (config.index_content_delivery
                                 and CONTENT_DELIVERY_IMPLEMENTED) else "false")
    store.set_setting("index_metadata_delivery",
                      "true" if config.index_metadata_delivery else "false")


async def _poll_loop(store: Store, papra: PapraClient, mailbox, *,
                 allowlist: AllowList) -> None:
    from papra_suite.mail import poll_mailbox_once

    # The loop owns no shared resource any more: the subject filter's killable
    # regex child is gone with it, so there is nothing here to tear down beyond
    # the task itself. Cancellation stays the exit path for serve shutdown.
    while True:
        # Re-fetch the mailbox row EVERY iteration: the snapshot passed at
        # startup goes stale immediately. Polling it forever would ignore the
        # persisted UID cursor (re-fetching the whole mailbox each poll) and
        # the enabled flag (a mailbox disabled at runtime must stop the loop).
        mailbox_id = mailbox.id
        try:
            mailbox = store.get_mailbox(mailbox_id)
            if mailbox is None or not mailbox.enabled:
                logger.info("poll loop stopping: mailbox %s deleted or disabled",
                            mailbox_id)
                break
            org = store.get_org(mailbox.org_id)
            if org is not None:
                await poll_mailbox_once(store, papra, mailbox, org,
                                    allowlist=allowlist)
            else:
                logger.info("poll loop: org %s for mailbox %s not found; skipping",
                            mailbox.org_id, mailbox.id)
        except Exception:  # noqa: BLE001 - a re-fetch, org lookup, or poll failure must not kill the loop
            logger.exception("poll loop error for mailbox %s", mailbox_id)
        await asyncio.sleep(mailbox.poll_interval_s)


def _prune_ingest_events_quietly(store: Store) -> int:
    """Drop expired ingest events, and never at the cost of ingesting.

    Retention is the least important write in this process, so it cannot be
    allowed to fail a poll, a sweep pass or a startup: a locked or full database
    must cost the operator a stale listing, not a stalled pipeline. A warning is
    logged rather than silence because a prune that never succeeds means the table
    grows without bound and disk is a slow way to learn that.
    """
    try:
        pruned = store.prune_ingest_events()
    except sqlite3.Error as error:
        logger.warning("could not prune ingest events: %s", error)
        return 0
    if pruned:
        logger.info("pruned %d ingest event(s) outside the retention window", pruned)
    return pruned


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
        # Retention rides the sweep's heartbeat instead of a timer of its own: the
        # window is 90 days, so one pass per sweep is orders of magnitude more
        # often than needed, and it adds no concurrency and no new failure
        # surface. It sits OUTSIDE the try above on purpose — a prune that goes
        # wrong must not swallow this pass's sweep summary, which is the line that
        # says whether externally uploaded documents were discovered.
        _prune_ingest_events_quietly(store)
        await asyncio.sleep(interval_s)


async def worker_loop(store: Store, *, kind: str, handler,
                      idle_sleep_s: float = 1.0) -> None:
    """Claim-and-run loop for one job kind. Never raises.

    Transition ordering is crash-safety critical: on_job_done (enqueue the
    title job) runs BEFORE mark_job_done. A crash between the two leaves the
    OCR job 'running' (not done) — startup recovery rewinds it to pending,
    the re-run's idempotent title enqueue is an INSERT OR IGNORE no-op, and
    the job then completes. The reverse order would permanently drop title
    generation: done jobs are not recovered, and the seen-table blocks the
    sweep from re-discovering the document.
    """
    while True:
        try:
            job = store.claim_next_job(kind)
        except Exception as error:  # noqa: BLE001 - a corrupt row or sqlite error must not kill the worker
            logger.error("claiming next %s job failed: %s", kind, error)
            await asyncio.sleep(idle_sleep_s)
            continue
        if job is None:
            await asyncio.sleep(idle_sleep_s)
            continue
        try:
            result = await handler(job)
            if isinstance(result, str):
                on_job_done(store, job, ocr_result=result)
            store.mark_job_done(job.id)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - the loop must survive
            logger.error("job %s (%s) failed: %s", job.id, kind, error)
            try:
                # Two permanence triggers (decisions §6):
                # 1. error.permanent — the client knows a retry cannot help, e.g.
                #    a 200 whose finish_reason says the completion stopped early
                #    (a truncated page retried is just the same half page).
                # 2. a 4xx status (except 429) from ANY client: a deleted
                #    document (PapraError 404) or a bad model name (LLMError 404)
                #    riding the 3-attempt backoff is ~2.5 minutes of futile
                #    retries. Fail such jobs permanently on the first attempt;
                #    5xx/429/other errors keep the backoff.
                status = getattr(error, "status", None)
                permanent = bool(getattr(error, "permanent", False)) or (
                    status is not None and 400 <= status < 500 and status != 429
                )
                if permanent:
                    store.mark_job_failed(job.id, str(error), max_attempts=1)
                else:
                    store.mark_job_failed(job.id, str(error))
            except (ValueError, sqlite3.Error):
                logger.error("job %s vanished before failure could be recorded", job.id)


def _is_pdf(document: dict) -> bool:
    mime = document.get("mimeType") or ""
    if mime == "application/pdf":
        return True
    return (document.get("name") or "").lower().endswith(".pdf")


async def sweep_once(store: Store, papra: PapraClient) -> SweepResult:
    """Record and enqueue indexing for externally-ingested documents (decisions §4).

    Papra-native uploads — a PDF dropped into the web UI — are documents this process
    never ingested, and the sweep is what finds them. Its enqueue call site is
    `enqueue_index_job`, so those documents enter the index pipeline and, while the old
    writers are still running, the OCR queue too.

    One gap worth naming here rather than discovering later: **mail intake does not
    come through this sweep.** `mail.py` enqueues through `enqueue_ocr_job` and records
    the document as seen in the same call, and the sweep skips seen documents, so a
    mail-ingested PDF has no ledger row and no index job until that call site is
    switched over. Switching it is not part of this task's files.

    documents_checked counts every document the sweep LISTED (including
    already-seen and non-PDF ones): it means "listed and examined", not
    "newly enqueued". Documents are paginated SWEEP_PAGE_SIZE at a time;
    a page returning fewer than SWEEP_PAGE_SIZE documents ends the org.
    Enqueuing never creates new Papra documents, so pagination is stable
    while iterating.

    Orgs larger than MAX_SWEEP_PAGES * SWEEP_PAGE_SIZE documents cannot be
    covered in one pass, so each org persists its next page index in the
    settings table (key "sweep_offset:<papra_org_id>"): a pass that exhausts
    the cap resumes where it stopped, and a pass that reaches a SHORT page
    (org fully caught up) resets the offset to 0. This guarantees eventual
    coverage of arbitrarily large orgs with bounded per-pass listing work.
    """
    result = SweepResult()
    for org in store.list_orgs():
        offset_key = f"sweep_offset:{org.papra_org_id}"
        try:
            offset = int(store.get_setting(offset_key) or "0")
        except ValueError:
            offset = 0  # a corrupt offset must not wedge the sweep
        page_index = offset
        short_page = False
        cap_exhausted = False
        count = 0
        while True:
            try:
                documents, count = await papra.list_documents(
                    org.papra_org_id, page_index=page_index,
                    page_size=SWEEP_PAGE_SIZE,
                    sort_field="createdAt", sort_order="desc",
                )
            except Exception as error:  # noqa: BLE001 - one org must not wedge the sweep
                logger.error("sweep: listing documents for org %s failed: %s",
                             org.papra_org_id, error)
                result.errors += 1
                break
            for document in documents:
                result.documents_checked += 1
                document_id = document["id"]
                if store.document_seen(org.papra_org_id, document_id):
                    continue
                try:
                    if _is_pdf(document):
                        original_size = document.get("originalSize")
                        if original_size is not None and original_size > OCR_MAX_DOCUMENT_BYTES:
                            # Skip via listing metadata — never download the
                            # oversize body just to hash it. Recorded as seen:
                            # the skip is permanent, not retried every sweep.
                            store.record_document_seen(
                                papra_org_id=org.papra_org_id, papra_document_id=document_id,
                                name=document.get("name", "document.pdf"),
                                mime=document.get("mimeType", "application/pdf"),
                                ingested_via="sweep",
                            )
                            logger.warning(
                                "sweep: skipping oversized document %s in org %s "
                                "(%d bytes > %d)",
                                document_id, org.papra_org_id,
                                original_size, OCR_MAX_DOCUMENT_BYTES,
                            )
                            result.oversized_skipped += 1
                            continue
                        # The listing already carries the content hash for
                        # compliant servers: use it and never download the
                        # body. The download+hash leg doubled the transfer and
                        # pulled the FULL document into RAM solely to recompute
                        # a value the listing already had.
                        file_hash = document.get("originalSha256Hash")
                        if not file_hash:
                            logger.info(
                                "sweep: document %s in org %s carries no "
                                "originalSha256Hash in the listing; downloading "
                                "to compute the content hash",
                                document_id, org.papra_org_id,
                            )
                            file_bytes = await papra.download_file(org.papra_org_id, document_id)
                            file_hash = hashlib.sha256(file_bytes).hexdigest()
                        enqueue_index_job(
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
            if len(documents) < SWEEP_PAGE_SIZE:
                short_page = True
                break
            page_index += 1
            if page_index >= offset + MAX_SWEEP_PAGES:
                cap_exhausted = True
                break
        if short_page:
            # The org is fully caught up: restart from page 0 next pass so
            # new documents (sorted desc by createdAt) are found quickly.
            store.set_setting(offset_key, "0")
        elif cap_exhausted:
            # The page cap stopped us mid-org: persist the page index reached
            # so the NEXT pass continues where this one stopped. Without the
            # offset, an org larger than MAX_SWEEP_PAGES * SWEEP_PAGE_SIZE
            # documents would NEVER have its tail swept (every pass restarts
            # at page 0). The seen-table gate skips processed docs cheaply.
            store.set_setting(offset_key, str(page_index))
            remaining = max(0, (count or 0) - page_index * SWEEP_PAGE_SIZE)
            logger.warning(
                "sweep: page cap reached for org %s after %d pages; resuming at "
                "page %d next pass (~%d documents remaining)",
                org.papra_org_id, MAX_SWEEP_PAGES, page_index, remaining)
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


def enqueue_index_job(store: Store, *, papra_org_id: str, document_id: str,
                      file_hash: str, name: str, mime: str, ingested_via: str) -> str:
    """Enqueue index work for one ingested document. Returns "" for a non-PDF.

    Three writes, in this order, because each crash has to leave something the
    pipeline can still act on:

    1. the **ledger reference**, before the job is visible. Eligibility is derived
       from the ledger (`ledger_needs_index`), so a queued job with no row behind it
       is work no sweep, backfill or rebuild pass can see — the job would be the
       only record that this document was ever meant to be indexed. The row also
       carries `papra_original_name`, the rename baseline, taken while we hold
       Papra's own name for it.
    2. the **index job**, then the legacy `ocr` job while content delivery is off.
       Enqueue before the seen-record is the M3 ordering, unchanged: a crash between
       the writes leaves the document unseen, so the sweep re-enqueues it (the
       enqueue is idempotent on kind+org+document+hash) and records it. Seen-first
       would strand it forever, because the sweep skips documents it has seen.
    3. the **seen record**, last.

    The legacy `ocr` enqueue is what keeps today's behaviour intact: Papra keeps the
    text and the titles it has now, and the index job costs nothing but Kreuzakt
    work. Task 7 retires it when content delivery is on, Task 8 when metadata is.
    """
    if mime != "application/pdf":
        return ""
    store.ledger_upsert_reference(
        papra_org_id=papra_org_id, papra_document_id=document_id, file_hash=file_hash,
        papra_original_name=name or None,
    )
    job_id = store.enqueue_job(
        kind="index",
        papra_org_id=papra_org_id,
        document_id=document_id,
        content_hash=file_hash,
        payload={"file_hash": file_hash, "name": name},
    )
    if not store.settings_flag("index_content_delivery"):
        store.enqueue_job(
            kind="ocr", papra_org_id=papra_org_id, document_id=document_id,
            content_hash=file_hash, payload={"file_hash": file_hash, "name": name},
        )
    store.record_document_seen(
        papra_org_id=papra_org_id, papra_document_id=document_id, name=name, mime=mime,
        ingested_via=ingested_via,
    )
    return job_id
