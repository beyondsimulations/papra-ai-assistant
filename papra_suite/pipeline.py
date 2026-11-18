"""Pipeline orchestration: document ingestion and job chaining.

M2 scope: the ingestion path (record document as seen + enqueue OCR job).
M3 adds the OCR/title worker loops and the central transition handler.
"""

import asyncio
import hashlib
import logging
import sqlite3
from dataclasses import dataclass

from papra_suite import indexing
from papra_suite.allowlist import AllowList
from papra_suite.config import ConfigError
from papra_suite.indexing import run_index_job, verify_once
from papra_suite.kreuzakt_client import KreuzaktClient
from papra_suite.llm_client import LLMClient
from papra_suite.ocr import OCR_SYSTEM_PROMPT, assemble_markdown, page_count, render_page
from papra_suite.papra_client import PapraClient
from papra_suite.store import TRUTHY_SETTING_VALUES, Job, Store
from papra_suite.titles import generate_title

logger = logging.getLogger(__name__)

SWEEP_PAGE_SIZE = 50
MAX_SWEEP_PAGES = 100  # hard cap: a listing endpoint that ignores pageIndex must not loop forever
DEFAULT_DOCUMENT_NAME = "document.pdf"
# Upper bound on downloaded document bytes for OCR and the sweep's hash step:
# a hostile/huge document must fail visibly (or be skipped via listing
# metadata) instead of being pulled into memory unbounded.
OCR_MAX_DOCUMENT_BYTES = 100 * 1024 * 1024
#: The `settings` row that mirrors `PAPRA_LEGACY_WORKERS`. Like the delivery flags it is
#: written from `Config` at every startup so the sweep, the intake path and the CLI ask
#: the store rather than guessing about a process they cannot see.
LEGACY_WORKERS_SETTING = "legacy_workers"
#: Six hours. The pass is a few hundred document pairs, which is to say seconds, so the
#: interval is a bound on how long Papra's text stays wrong rather than on our cost.
DEFAULT_VERIFICATION_INTERVAL_S = 21600

#: The three states of "who writes Papra's `content` right now", as `check` prints them.
CONTENT_WRITER_LEGACY = "suite ocr worker (legacy)"
CONTENT_WRITER_BRIDGE = "index bridge"
CONTENT_WRITER_NONE = "none (handover window)"
CONTENT_WRITER_BOTH = "two writers (refused at startup)"

#: Who owns Papra's `name`, `documentDate` and `notes`, as `check` prints them. The fourth
#: state is the reason this is a separate line from the content writer's: unlike two writers
#: of `content`, two writers of `name` are not refused at startup, because the state is
#: transient by construction and refusing would make the documented handover order
#: impossible — see `metadata_writer_state`.
METADATA_WRITER_LEGACY = "suite title worker (legacy)"
METADATA_WRITER_BRIDGE = "index bridge"
METADATA_WRITER_NONE = "none (handover window)"
METADATA_WRITER_BOTH = "two writers (title queue draining)"


class ContentWriterConflict(ConfigError):
    """This suite's legacy OCR worker and this bridge were both told to write `content`.

    This is the configuration that produced the original intermittency, and the default one
    to fall into: `PAPRA_LEGACY_WORKERS` is true unless an operator turns it off, so setting
    `PAPRA_INDEX_CONTENT_DELIVERY` alone re-creates the race with nothing new in the log to
    point at it. Refusing is the only mechanism available, because the two writers are in
    different processes and never meet.
    """


def content_writer_state(delivery: bool, legacy: bool) -> str:
    """Who owns Papra's `content`, given the two switches. One definition for `serve`,
    `check` and the refusal message, so the line an operator reads cannot disagree with
    the decision the process made.
    """
    if delivery and legacy:
        return CONTENT_WRITER_BOTH
    if delivery:
        return CONTENT_WRITER_BRIDGE
    if legacy:
        return CONTENT_WRITER_LEGACY
    return CONTENT_WRITER_NONE


def content_writer_conflict(delivery: bool, legacy: bool) -> ContentWriterConflict | None:
    """The refusal for two writers, or None when this configuration can start.

    The message is the runbook: naming both variables is not enough, because the fix is an
    ORDER and the operator reading it at 23:00 is the same person who just set the flag.
    """
    if not (delivery and legacy):
        return None
    return ContentWriterConflict(
        "PAPRA_INDEX_CONTENT_DELIVERY is on while PAPRA_LEGACY_WORKERS is also on: this "
        "suite's legacy OCR worker and this bridge would both write the `content` column, "
        "which is the race this bridge exists to remove. In the required order: set "
        "PAPRA_LEGACY_WORKERS=false and recreate, prove the queue is empty twice a minute "
        "apart (proves quiescence: `sqlite3 $DATA_DIR/state.db \"SELECT count(*) FROM jobs "
        "WHERE kind='ocr' AND status IN ('pending','running')\"` prints 0 both times), and "
        "only then turn on "
        "the delivery flag PAPRA_INDEX_CONTENT_DELIVERY=true. While an ocr row remains, the "
        "convergence pass is fighting a running job for the same document.")


def metadata_writer_state(delivery: bool, legacy: bool) -> str:
    """Who owns Papra's `name`/`documentDate`/`notes`, given the two switches.

    Same shape as `content_writer_state`, and deliberately NOT the same policy: two writers
    of `content` is refused at startup, two writers of `name` is only reported.

    The difference is what each state does to the other writer. The legacy title chain is
    switched off twice while metadata delivery is on: `on_job_done` stops enqueueing `title`
    jobs, for documents from every intake path, sweep and mail alike, and `run_title_job`
    refuses the rows already queued when the flag flipped — it skips them, marks them done and
    writes nothing, the way it skips a job whose content moved on. The second half is what
    makes the state safe: producer-only gating left those rows claimed, writing a `name` the
    ledger had no record of, which the next pass read as a human edit and protected permanently.
    Refusing the combination would still forbid the only order of operations that works:
    `legacy_workers` has to stay on while the queues drain, and the flag that drains them is the
    flag being set. One job already past its gate check when the flag flips can land its write;
    the ledger's not-ours verdict is the containment for that case.

    It is still printed, because the string reports the two flags rather than what they now
    permit: with `legacy_workers` on, the loops are running and the configuration really does
    name two writers, even though the second one refuses its own work. The proof that the state
    has passed is the actionable queue,
    read twice a minute apart while the loops are still running:

        sqlite3 "$DATA_DIR/state.db" "SELECT count(*) FROM jobs WHERE kind='title'
                                      AND status IN ('pending','running');"

    Two zeroes proves the second writer has no work left. It does NOT change this string,
    which is computed from the two flags and not from the queue: while `legacy_workers` is on
    the line keeps saying `two writers`, and that is the honest reading — the loops are still
    running and would claim title work if any existed. The string becomes `index bridge` when
    the legacy loops are switched off, which is content's handover step, not this one.
    """
    if delivery and legacy:
        return METADATA_WRITER_BOTH
    if delivery:
        return METADATA_WRITER_BRIDGE
    if legacy:
        return METADATA_WRITER_LEGACY
    return METADATA_WRITER_NONE


def legacy_writers_enabled(store: Store) -> bool:
    """Are this suite's legacy `ocr`/`title` writers still in charge of their columns?

    Read as ON when the row is absent, which is the one setting in this codebase that
    fails open, and on purpose: `Config.legacy_workers` defaults to true, `serve` writes the
    row on every startup, and what the flag DOES is stop things. A missing row therefore
    means "nobody has taken the legacy writers away yet", which is today's behaviour; the
    fail-closed reading would have stopped Papra's text writer on the store of every test,
    CLI invocation and backfill helper that never ran a seeded startup, and the documents
    in them would quietly have gone unprocessed.
    """
    value = store.get_setting(LEGACY_WORKERS_SETTING)
    if value is None:
        return True
    return value.strip().lower() in TRUTHY_SETTING_VALUES


@dataclass
class SweepResult:
    """What one sweep pass did, counted in DOCUMENTS rather than in queue rows.

    `documents_enqueued` counts the documents this pass enrolled, and one PDF can
    write two job rows while the legacy OCR writer is still switched on for it — so a
    row-counting name here would report two jobs for one document and make the sweep's
    own log line disagree with the queue. `documents_checked` is "listed and
    examined", including documents already seen and documents that are not PDFs.
    """

    documents_checked: int = 0
    documents_enqueued: int = 0
    non_pdf_skipped: int = 0
    errors: int = 0
    oversized_skipped: int = 0
    # Task 9: what delete/restore propagation did. Carried here rather than kept in the pass's
    # own return value because the sweep's log line is the one an operator reads, and two of
    # these (`deletions_aborted`, `deletions_capped`) are alarms that must not be visible only
    # in a log the pass wrote for itself.
    deletions_marked: int = 0
    deletions_pending: int = 0
    deletions_restored: int = 0
    deletions_capped: int = 0
    deletions_aborted: bool = False
    deletions_quarantined: int = 0
    deletions_brake: bool = False
    cleanup_attempted: int = 0
    cleanup_deleted: int = 0
    cleanup_deferred: int = 0
    cleanup_failed: int = 0


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
    """Central transition handler (decisions §2): ocr done -> enqueue title.

    The one transition that is conditional, and the condition is the metadata gate. This is
    where the legacy title chain is switched off, and it is the only place that switch is
    correct: the OCR job above has already written Papra's `content` under its own flag, while
    a `title` job exists only to write the title — so the flag that takes the title away
    should stop the title job, and nothing else.

    Stopping the `ocr` enqueue instead was the alternative, and it is wrong: with metadata
    delivery on, content delivery off and the legacy loops on, that would starve the one
    writer Papra's `content` still has while `check` went on crediting it. Handing the two
    columns over separately is the point of having two flags.
    """
    if job.kind != "ocr":
        return
    if store.settings_flag("index_metadata_delivery"):
        # Once each time is enough: this is the transition, not a retry loop. Wording differs
        # from `run_title_job`'s skip on purpose: this row was never created, that one existed
        # before the flip and was dropped. `run_title_job` refuses those too, so a log line
        # here and one there describe two different things.
        logger.info("title job not enqueued: papra=%s — Papra's title belongs to the index "
                    "bridge while index_metadata_delivery is on", job.document_id)
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


async def run_title_job(papra: PapraClient, title_llm: LLMClient, job: Job, *,
                        store: Store) -> None:
    """Generate and write a title — unless the document moved on (decisions §2).

    A stale title job (content v1, still in backoff) must never overwrite a
    title that a newer job (v2) already wrote from newer content: hash the
    document's CURRENT content and compare to the job's content_hash first;
    on mismatch, skip silently (the worker still marks the job done).

    **The metadata gate is checked before anything else, and it is the consumer half of the
    handover.** `on_job_done` stops producing `title` rows while `index_metadata_delivery`
    is on, but the rows already `pending` or `running` when an operator flips the flag are
    still claimed here, and a title written by this path is recorded nowhere in the ledger.
    The next metadata pass then finds a `name` that is neither `written_name` nor
    `papra_original_name`, answers *not ours*, and sets `name_protected = 1` — permanently,
    and byte-identical to a genuine human rename, because protection is one bit with no
    provenance. The recovery for that is `metadata --force --yes-i-mean-it`, which overwrites
    every genuine rename in scope. So while the metadata gate is on this writer skips, exactly
    as it skips a stale job: nothing asked of the model, nothing sent to Papra, and the row
    ends `done` so the drain the handover asks for can actually reach zero.
    """
    if store.settings_flag("index_metadata_delivery"):
        logger.info("title job skipped: papra=%s job=%s — Papra's title belongs to the index "
                    "bridge while index_metadata_delivery is on", job.document_id, job.id)
        return
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
    """The process runtime: pollers, workers, sweep, and the convergence pass.

    Runs until the stop event is set (or the surrounding task is cancelled),
    then cancels its tasks and closes ONLY the clients and store it
    constructed — injected ones belong to the caller (owns_* pattern).

    Which loops exist depends on the writer switches. `PAPRA_LEGACY_WORKERS=false` takes
    this suite's `ocr`/`title` loops out of the process as well as taking their enqueues
    out; `index_content_delivery` decides whether the index worker writes text, and the
    verification loop is up either way and refuses per pass. The one configuration that is
    not started at all is two writers on `content` — see `ContentWriterConflict`.
    """
    from papra_suite.logging_setup import register_config_secrets, setup_logging

    stop = stop or asyncio.Event()
    owns_store = store is None
    if store is None:
        store = Store(config.data_dir / "state.db")
    store.migrate()
    _sync_settings_from_config(store, config)
    # The two-writer refusal comes before anything else that can reach the network, and
    # before any task is started: a container that half-indexes a corpus and then exits has
    # already done the damage the refusal exists to prevent. Both halves of the pair are
    # read from where their users read them — the gate from the store, because that is what
    # the writer consults, and `legacy_workers` from the config that was just seeded into
    # it, because that is what the operator started the process with.
    conflict = content_writer_conflict(store.settings_flag("index_content_delivery"),
                                       config.legacy_workers)
    if conflict is not None:
        logger.error("refusing to start: %s", conflict)
        raise conflict
    if not config.legacy_workers and not store.settings_flag("index_content_delivery"):
        # Legitimate and temporary — the drain window between the two flags — but never
        # silent. A window that has lasted a week reads as a configuration, and the
        # symptom is only visible as text that never arrives.
        logger.error(
            "no content writer in this process is running: PAPRA_LEGACY_WORKERS is off "
            "and PAPRA_INDEX_CONTENT_DELIVERY is off, so neither our worker nor the bridge "
            "writes Papra's `content` column (handover window; Papra's own extraction still "
            "runs on upload). Turn on the delivery flag once the ocr queue has drained; "
            "`papra-suite check` prints this too.")
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
    if config.legacy_workers:
        # With the legacy writers switched off these loops are what keeps claiming from a
        # queue nobody is filling any more, and a drain is only provable when the loops are
        # gone rather than merely idle. The evidence the handover asks for is the state
        # database reading empty twice a minute apart:
        # `sqlite3 "$DATA_DIR/state.db" "SELECT kind, status, count(*) FROM jobs
        # GROUP BY 1, 2;"` — no `ocr` row in `pending` or `running`, both times.
        for _ in range(config.ocr_concurrency):
            tasks.append(asyncio.create_task(worker_loop(
                store, kind="ocr",
                handler=lambda job: run_ocr_job(papra, ocr_llm, job, config=config),
            ), name="ocr-worker"))
        tasks.append(asyncio.create_task(worker_loop(
            store, kind="title",
            handler=lambda job: run_title_job(papra, title_llm, job, store=store),
        ), name="title-worker"))
    # ONE index worker, on purpose. Correlating an upload needs its queue row inside
    # the newest-100 window, so uploads are serial by construction: a second worker
    # would interleave uploads and make each other's correlation fail.
    # `max_attempts` goes to the LOOP as well as the handler: the loop is what spends
    # an attempt and decides the job is over, and the handler needs the same number to
    # know when a re-run of a Kreuzakt row is the last one anyone will ask for.
    tasks.append(asyncio.create_task(worker_loop(
        store, kind="index",
        handler=lambda job: run_index_job(
            kreuzakt, store, job, papra=papra,
            poll_s=config.index_poll_interval_s,
            wait_timeout_s=config.index_wait_timeout_s,
            max_attempts=config.index_max_attempts),
        max_attempts=config.index_max_attempts,
    ), name="index-worker"))
    if config.sweep_interval_s > 0:
        tasks.append(asyncio.create_task(
            _sweep_loop(store, papra, config.sweep_interval_s, kreuzakt=kreuzakt),
            name="sweep"))
    # The convergence pass runs whether or not the gate is open, and says nothing while it
    # is shut: the gate is read per pass, so the loop being up is what makes a flag flip
    # take effect without a restart, and a loop that had to be started on demand is a loop
    # that quietly never ran on the day the handover happened.
    tasks.append(asyncio.create_task(
        _verification_loop(store, papra, kreuzakt,
                           interval_s=DEFAULT_VERIFICATION_INTERVAL_S, stop=stop),
        name="verification"))

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

    `index_metadata_delivery` is seeded under the same rule as its sibling, and it is read
    now: `deliver_metadata` owns that gate, so a build with no metadata path must not seed
    `true`. That is the `OCR_LANGUAGES` shape — a flag that looks configurable and changes
    nothing, with the documentation as the only thing that lied. Same warning, same refusal,
    same module attribute as the seam.

    `legacy_workers` is seeded too, because it is not only a knob about this process's
    loops: the intake path and `enqueue_index_job` read it to decide whether a new
    document also enters the legacy OCR queue, and a sweep or a CLI call in another
    process has to reach the same answer the running daemon did.
    """
    if config.index_content_delivery and not indexing.CONTENT_DELIVERY_IMPLEMENTED:
        # Loud, and refused rather than obeyed. Honoured, this flag stops the legacy
        # OCR enqueue — and in a build with no delivery path that is not "the bridge
        # will write the text instead", it is "nothing writes the text". The module
        # attribute in `indexing` is the seam: a build that ships delivery sets it True,
        # and a build that does not keeps the legacy writer no matter what the operator
        # asks for.
        logger.warning(
            "PAPRA_INDEX_CONTENT_DELIVERY is set but this build has no content "
            "delivery path: leaving the legacy OCR writer in charge of Papra's "
            "text. The flag is honoured only by a build that implements delivery.")
    store.set_setting("index_content_delivery",
                      "true" if (config.index_content_delivery
                                 and indexing.CONTENT_DELIVERY_IMPLEMENTED) else "false")
    if config.index_metadata_delivery and not indexing.METADATA_DELIVERY_IMPLEMENTED:
        logger.warning(
            "PAPRA_INDEX_METADATA_DELIVERY is set but this build has no metadata delivery "
            "path: leaving the legacy title worker in charge of Papra's name, date and "
            "notes. The flag is honoured only by a build that implements delivery.")
    store.set_setting("index_metadata_delivery",
                      "true" if (config.index_metadata_delivery
                                  and indexing.METADATA_DELIVERY_IMPLEMENTED) else "false")
    store.set_setting(LEGACY_WORKERS_SETTING,
                      "true" if config.legacy_workers else "false")


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


def _log_deletion_counts(result: SweepResult) -> None:
    """The two counts that must never be silent, on the line the operator actually reads.

    A pass that refused to touch anything and a pass that marked the whole of its budget both
    look like a quiet corpus in the ledger, so they are ERROR here and repeated by `check` from
    the store: an alarm that lives only in a debug-level line is an alarm nobody hears. Ordinary
    activity is INFO, and a sweep in which nothing happened says nothing.
    """
    if result.deletions_aborted:
        logger.error(
            "sweep: deletion pass aborted, nothing was marked or queued this pass; the reason "
            "is in `papra-suite check` (a listing that could not be trusted is the usual one)")
    elif result.deletions_brake:
        logger.error(
            "sweep: deletion pass hit its marks brake, so it stopped marking mid-pass or "
            "refused to run at all; `papra-suite check` names the counter and how to clear it")
    elif result.deletions_capped:
        logger.error(
            "sweep: deletion pass capped at %d references, more remain for the next pass",
            result.deletions_capped)
    elif result.deletions_quarantined:
        logger.error(
            "sweep: %d bucket(s) quarantined by the deletion pass — an operator must check that "
            "they really are empty, because while quarantined nothing about their contents is "
            "propagated", result.deletions_quarantined)
    elif (result.deletions_marked or result.deletions_restored or result.cleanup_attempted):
        logger.info(
            "sweep: deletions %d marked, %d queued upstream, %d restored; cleanup %d attempted, "
            "%d deleted, %d deferred, %d failed", result.deletions_marked,
            result.deletions_pending, result.deletions_restored, result.cleanup_attempted,
            result.cleanup_deleted, result.cleanup_deferred, result.cleanup_failed)


async def _sweep_loop(store: Store, papra: PapraClient, interval_s: int, *,
                      kreuzakt: KreuzaktClient | None = None) -> None:
    while True:
        try:
            result = await sweep_once(store, papra, kreuzakt=kreuzakt)
            if result.documents_enqueued or result.non_pdf_skipped:
                logger.info("sweep: %d checked, %d documents enqueued, %d non-pdf",
                            result.documents_checked, result.documents_enqueued,
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


async def _verification_loop(store: Store, papra: PapraClient, kz, *,
                             interval_s: int = DEFAULT_VERIFICATION_INTERVAL_S,
                             stop: asyncio.Event | None = None) -> None:
    """Papra's current text against the index's, on a timer. Never raises.

    The phase is the first half of the design, and it is worth saying plainly: the pass
    runs IMMEDIATELY when this loop starts, and only then waits `interval_s`. Startup is
    the moment the two columns are furthest apart — the container was down while Papra kept
    writing into documents we had delivered — so a loop that slept first would make every
    deploy and every restart wait six hours to notice. `interval_s` is the wait BETWEEN
    passes.

    It survives a failing pass because nothing else runs this one: an exception escaping an
    iteration would end convergence for the rest of the container's life, and the only
    evidence would be an absence of repairs. The `stop` event is checked before each pass
    and after it, so a shutdown during a six-hour sleep is the caller's cancellation and a
    shutdown between passes is a clean return.
    """
    while True:
        if stop is not None and stop.is_set():
            return
        try:
            result = await verify_once(store, papra, kz)
            if result.diverged or result.suppressed or result.lookup_errors:
                # Silence means converged, which is the normal state and not worth a line
                # every six hours. Anything else is the record of the column drifting.
                logger.info(
                    "verification: %d scanned, %d diverged, %d repaired, %d suppressed, "
                    "%d extraction-empty, %d read/repair errors",
                    result.scanned, result.diverged, result.repaired, result.suppressed,
                    result.empty, result.lookup_errors)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the loop outlives a bad pass
            logger.exception("verification pass failed")
        if stop is not None and stop.is_set():
            return
        await asyncio.sleep(interval_s)


async def worker_loop(store: Store, *, kind: str, handler,
                      idle_sleep_s: float = 1.0,
                      max_attempts: int | None = None) -> None:
    """Claim-and-run loop for one job kind. Never raises.

    `max_attempts` is this kind's ceiling on attempts at the JOB across runs — the
    number the operator configured (`PAPRA_INDEX_MAX_ATTEMPTS` for the index worker),
    and it is honoured here because this is the loop that spends an attempt. Left as
    None it keeps `Store.mark_job_failed`'s default, which is what the kinds written
    before that knob existed still get. It is not the handler's bound on recovery
    actions inside one run (`indexing.DEFAULT_INDEX_RECOVERY_ACTIONS`), and the two
    must not be passed for each other: one is "how many times may this job be tried",
    the other is "how much repair may one try attempt".

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
                    if kind == "index":
                        # Stated because nothing else says it. `check`'s counters are all
                        # ledger-derived, so this job flipping `done` -> `failed` moves no
                        # number an operator reads, and `enqueue_job` is `INSERT OR IGNORE`
                        # against the unique idempotency index: the failed row now absorbs
                        # every later request to index these same bytes, which is what a
                        # restore or a backfill does to bring a document back.
                        logger.error(
                            "index job %s for papra document %s failed permanently at "
                            "content %s: the queue row is the idempotency key, so every later "
                            "enqueue for these same bytes is ignored until that row is reset — "
                            "by `jobs --reset` once Task 15 ships that command, by a hand edit "
                            "of the `jobs` table until then. A restore or backfill that reports "
                            "success and enqueues nothing has met this row.",
                            job.id, job.document_id, job.content_hash[:12])
                elif max_attempts is None:
                    store.mark_job_failed(job.id, str(error))
                else:
                    store.mark_job_failed(job.id, str(error), max_attempts=max_attempts)
            except (ValueError, sqlite3.Error):
                logger.error("job %s vanished before failure could be recorded", job.id)


def _is_pdf(document: dict) -> bool:
    mime = document.get("mimeType") or ""
    if mime == "application/pdf":
        return True
    return (document.get("name") or "").lower().endswith(".pdf")


async def sweep_once(store: Store, papra: PapraClient,
                     *, kreuzakt: KreuzaktClient | None = None) -> SweepResult:
    """Record and enqueue indexing for externally-ingested documents (decisions §4).

    Papra-native uploads — a PDF dropped into the web UI — are documents this process
    never ingested, and the sweep is what finds them. Its enqueue call site is
    `enqueue_index_job`, so those documents enter the index pipeline and, while the old
    writers are still running, the OCR queue too.

    With a `kreuzakt` client it also propagates deletions, at the end: Papra's trash into the
    index, the queued upstream deletes executed, restores taken back. That stage needs both
    services, so without a client it does not run — and says so, because a sweep that skipped
    the look would otherwise report exactly what a sweep that found nothing reports. Its
    failures are the pass's own to report (`aborted`, `capped`); see `reconcile_deletions`.

    One gap worth naming here rather than discovering later: **mail intake does not
    come through this sweep**, and since Task 7 it does not need to. `mail.py` picks its
    queue from the writer switch: while the legacy writers are on it enqueues OCR and
    records the document as seen in the same call, and the sweep skips seen documents, so
    such a document has no ledger row until the flag flips; with the legacy writers off it
    goes through `enqueue_index_job` and reaches the index on arrival. `check`'s
    `content writer:` line names which of the two this process is running.

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
                        result.documents_enqueued += 1
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
    if kreuzakt is None:
        # Said out loud, because the alternative is indistinguishable from a corpus in which
        # nothing was deleted: this sweep did not look for deletions at all. Propagation needs
        # both services — Papra's listing for the verdict and Kreuzakt for the delete.
        logger.warning("sweep: no kreuzakt client, so no delete propagation ran this pass")
        return result
    deletions = await indexing.reconcile_deletions(store, papra, kreuzakt)
    result.deletions_marked = deletions.refs_marked
    result.deletions_pending = deletions.upstream_pending
    result.deletions_restored = deletions.restored
    result.deletions_capped = deletions.capped
    result.deletions_aborted = deletions.aborted
    result.deletions_quarantined = deletions.quarantined
    result.deletions_brake = deletions.brake_tripped
    cleanup = await indexing.run_cleanup(store, kreuzakt)
    result.cleanup_attempted = cleanup.attempted
    result.cleanup_deleted = cleanup.deleted
    result.cleanup_deferred = cleanup.deferred
    result.cleanup_failed = cleanup.failed
    # Reported here rather than by the loop that calls this: `aborted` and `capped` are facts
    # about the pass, and a caller that runs one pass from a command line deserves the same
    # alarm the daemon gets. Silence is the failure mode — see `_log_deletion_counts`.
    _log_deletion_counts(result)
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
    work. Content delivery retires it, because the bridge then writes the very column this
    worker writes; the metadata flag deliberately does NOT, because with content still the
    legacy worker's, suppressing the `ocr` row would leave Papra's `content` with no writer
    while `check` went on crediting one. The title chain is switched off one step later, in
    `on_job_done`, where stopping it stops a writer of `name` and nothing else — which is
    what makes the two handovers genuinely independent. `legacy_workers` retires the `ocr`
    enqueue earlier still, because a queue the drained workers are no longer claiming still
    has to stop being filled for a drain to be provable.
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
    if not store.settings_flag("index_content_delivery") and legacy_writers_enabled(store):
        store.enqueue_job(
            kind="ocr", papra_org_id=papra_org_id, document_id=document_id,
            content_hash=file_hash, payload={"file_hash": file_hash, "name": name},
        )
    store.record_document_seen(
        papra_org_id=papra_org_id, papra_document_id=document_id, name=name, mime=mime,
        ingested_via=ingested_via,
    )
    return job_id
