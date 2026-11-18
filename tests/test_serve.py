"""Unit tests for `serve`: the process runtime (start, recover, stop cleanly).

The CONTRACT: serve starts, survives poll failures against an unreachable
IMAP host, recovers stale running jobs ONCE at startup, returns cleanly on
stop, and closes only what it constructed (an injected store must NOT be
closed — the tests keep using their store handle after serve returns).
"""

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from papra_suite.allowlist import parse_allowlist
from papra_suite.config import load_config
from papra_suite.pipeline import serve
from papra_suite.store import Store

BASE_ENV = {
    "PAPRA_ALLOWED_SENDERS": "sender@x.com",
    "PAPRA_BASE_URL": "http://papra:1221", "PAPRA_API_KEY": "k",
    "LLM_BASE_URL": "http://llm:8000", "LLM_API_KEY": "k",
    "TITLE_MODEL": "t", "OCR_MODEL": "o", "ADMIN_PASSWORD": "p",
    "SWEEP_INTERVAL_S": "0",  # disable the sweep for the test
}

# coroutine qualnames of the long-lived tasks serve spawns; after serve
# returns, none of them may linger (clean shutdown, no dangling tasks)
_SERVE_TASK_NAMES = {"_poll_loop", "worker_loop", "_sweep_loop", "_verification_loop"}

# The poll loop now requires an allow-list; these tests stub the poll itself,
# so the list only has to exist and not deny the loop its row re-fetch.
ALLOW = parse_allowlist("x.com")


def _dangling_serve_tasks() -> list:
    return [
        task for task in asyncio.all_tasks()
        if (coro := task.get_coro()) is not None
        and getattr(coro, "__qualname__", "").split(".")[-1] in _SERVE_TASK_NAMES
    ]


async def test_serve_runs_and_stops_cleanly(tmp_path):
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_1", name="Org")
    # 127.0.0.1:1 is a closed port: the connection is refused fast and
    # deterministically (a DNS-based host like imap.invalid can stall in
    # resolution, making the stop fire mid-connect and flake the assertion)
    store.create_mailbox(org_id=org_id, host="127.0.0.1", port=1,
                         username="u", password="p", poll_interval_s=3600)

    stop = asyncio.Event()

    async def set_stop_later():
        await asyncio.sleep(0.5)
        stop.set()

    # poll attempts fail (connection refused) — the loop must survive them
    asyncio.get_running_loop().create_task(set_stop_later())
    await serve(config, stop=stop, store=store)  # returns when stop is set

    # the mailbox recorded its failure; the process stayed alive until stop
    # (the injected store is still usable: serve must NOT have closed it)
    mailbox = store.get_mailbox(store.list_mailboxes()[0].id)
    assert mailbox is not None  # state machinery ran
    assert mailbox.last_error is not None  # the failed poll was recorded
    assert not _dangling_serve_tasks()  # clean shutdown, no dangling tasks
    store.close()


async def test_serve_recovers_stale_jobs_at_startup(tmp_path):
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    db_path = tmp_path / "state.db"
    store = Store(db_path)
    store.migrate()
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1",
                                document_id="doc_1", content_hash="h", payload={})
    store.claim_next_job("ocr")  # now running (simulated crash leftover)

    stop = asyncio.Event()

    async def wait_until_recovered_then_stop():
        # Watch via a second Store handle. The job starts 'running' (crash
        # leftover); the ONLY way it can ever read 'pending' is if serve's
        # startup recovery reset it (a worker claim requires a pending job,
        # and a failed attempt rewinds to pending — both presuppose the
        # recovery). So observing 'pending' proves recover_running_jobs ran.
        watcher = Store(db_path)
        deadline = asyncio.get_running_loop().time() + 10.0
        try:
            while True:
                job = watcher.get_job(job_id)
                if job is not None and job.status == "pending":
                    break
                if asyncio.get_running_loop().time() > deadline:
                    pytest.fail("serve never recovered the stale running job")
                await asyncio.sleep(0.01)
        finally:
            watcher.close()
        stop.set()

    # serve with an EMPTY mailbox set: create the org but no mailbox
    asyncio.get_running_loop().create_task(wait_until_recovered_then_stop())
    await serve(config, stop=stop, store=store)
    assert not _dangling_serve_tasks()  # clean shutdown, no dangling tasks
    store.close()


async def test_serve_logs_unexpected_task_exit(tmp_path, monkeypatch, caplog):
    """Supervision pin: a long-lived task dying mid-run must be logged
    loudly ("exited unexpectedly") — deleting the done-callback supervision
    fails this test."""
    import logging

    import papra_suite.pipeline as pipeline_mod
    from papra_suite import logging_setup

    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_1", name="Org")
    store.create_mailbox(org_id=org_id, host="127.0.0.1", port=1,
                         username="u", password="p", poll_interval_s=3600)

    async def dying_poll_loop(store, papra, mailbox, allowlist):
        raise RuntimeError("died fast")  # the long-lived task dies immediately

    monkeypatch.setattr(pipeline_mod, "_poll_loop", dying_poll_loop)
    # serve's setup_logging clears root handlers (including pytest's capture
    # handler) and register_config_secrets feeds the redaction filter left on
    # the root logger by earlier serve tests (it mutates the SHARED record,
    # so caplog would see redacted text). Neutralize both so caplog observes
    # the supervision log verbatim.
    monkeypatch.setattr(logging_setup, "setup_logging", lambda store=None: None)
    monkeypatch.setattr(logging_setup, "register_config_secrets", lambda config: None)

    stop = asyncio.Event()

    async def set_stop_later():
        await asyncio.sleep(0.3)  # the task dies long before this
        stop.set()

    asyncio.get_running_loop().create_task(set_stop_later())
    with caplog.at_level(logging.ERROR):
        await serve(config, stop=stop, store=store)

    assert any("exited unexpectedly" in record.getMessage()
               and "died fast" in record.getMessage()
               for record in caplog.records)
    assert not _dangling_serve_tasks()
    store.close()


async def test_serve_logs_designed_task_exit_as_info(tmp_path, monkeypatch, caplog):
    """Clean-exit pin: a poll loop that stops BY DESIGN (the mailbox is
    disabled at runtime by the 3-strike failure accounting) exits normally,
    and the supervisor must log INFO "task X exited" — NOT a false ERROR
    "exited unexpectedly" crash alarm. Reverting the supervisor to
    ERROR-always fails this test."""
    import logging

    import papra_suite.mail as mail_mod
    from papra_suite import logging_setup

    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_1", name="Org")
    mailbox_id = store.create_mailbox(org_id=org_id, host="127.0.0.1", port=1,
                                       username="u", password="p", poll_interval_s=0)

    async def failing_poll(store_arg, papra, mailbox, org, allowlist=None):
        # one poll failure with disable_after=1 disables the mailbox row;
        # the REAL _poll_loop then re-fetches the row and stops by design
        store_arg.record_poll_failure(mailbox.id, "boom", disable_after=1)

    monkeypatch.setattr(mail_mod, "poll_mailbox_once", failing_poll)
    # same neutralization as the ERROR-arm pin: keep pytest's caplog handler
    # and bypass the redaction filter so the supervision log is observed
    # verbatim
    monkeypatch.setattr(logging_setup, "setup_logging", lambda store=None: None)
    monkeypatch.setattr(logging_setup, "register_config_secrets", lambda config: None)

    stop = asyncio.Event()

    async def set_stop_later():
        await asyncio.sleep(0.3)  # the designed exit happens long before this
        stop.set()

    asyncio.get_running_loop().create_task(set_stop_later())
    with caplog.at_level(logging.INFO):
        await serve(config, stop=stop, store=store)

    task_name = f"poll:{mailbox_id}"
    # the designed exit is INFO "task X exited" (no "unexpectedly")
    assert any(record.levelno == logging.INFO
               and record.getMessage() == f"task {task_name} exited"
               for record in caplog.records)
    # no false crash alarm anywhere in the stream
    assert not any("exited unexpectedly" in record.getMessage()
                   for record in caplog.records)
    assert not any(record.levelno == logging.ERROR for record in caplog.records)
    assert not _dangling_serve_tasks()
    store.close()


async def test_serve_spawns_sweep_loop(tmp_path, monkeypatch):
    """Sweep-spawn pin: serve must actually spawn the sweep loop. With
    SWEEP_INTERVAL_S=1 and a sweep_once that records and stops serve, the
    recording proves the spawn — and that exactly one sweep ran."""
    import papra_suite.pipeline as pipeline_mod
    from papra_suite.pipeline import SweepResult

    config = load_config({**BASE_ENV, "SWEEP_INTERVAL_S": "1",
                          "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id="org_1", name="Org")  # no mailboxes

    stop = asyncio.Event()
    sweeps = []

    async def sweep_once_and_stop(store_arg, papra, *, kreuzakt=None):
        # `kreuzakt` is Task 9's: the sweep propagates deletions, so the double takes the
        # keyword the loop passes, and the assertion below pins serve handing it over.
        sweeps.append((store_arg, kreuzakt))
        stop.set()  # end serve from within the FIRST sweep
        return SweepResult()

    monkeypatch.setattr(pipeline_mod, "sweep_once", sweep_once_and_stop)

    async def watchdog_stop():
        # only ever reached if the sweep never spawned (mutant guard: the
        # test then FAILS on the assertion instead of hanging forever)
        await asyncio.sleep(5.0)
        stop.set()

    watchdog = asyncio.get_running_loop().create_task(watchdog_stop())
    await serve(config, stop=stop, store=store)
    watchdog.cancel()  # untriggered in the passing case — do not linger

    assert len(sweeps) == 1  # the sweep ran once, then serve stopped
    assert sweeps[0][1] is not None, \
        "serve gave the sweep no kreuzakt client, so delete propagation would never run"
    assert not _dangling_serve_tasks()
    store.close()


async def test_serve_spawns_title_worker_and_one_ocr_worker_per_concurrency(tmp_path):
    """Worker-spawn wiring pin: kills deleting the `title-worker` task (OCR
    content still gets written back, but no document ever gets a generated
    title — only filenames, forever) and kills
    `for _ in range(config.ocr_concurrency)` -> `range(1)` (the operator's
    OCR_CONCURRENCY=3 silently degrades to serial OCR). serve's sweep spawn and
    poller are pinned elsewhere; nothing inspected worker task names or counts."""
    config = load_config({**BASE_ENV, "OCR_CONCURRENCY": "3",
                          "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id="org_1", name="Org")  # no mailboxes, no jobs

    stop = asyncio.Event()
    names: list[str] = []

    async def snapshot_then_stop():
        # serve builds its whole task list before its first await, so these
        # tasks already exist here — the snapshot cannot lose a race.
        await asyncio.sleep(0)
        names.extend(task.get_name() for task in asyncio.all_tasks())
        stop.set()

    asyncio.get_running_loop().create_task(snapshot_then_stop())
    await serve(config, stop=stop, store=store)

    assert names.count("title-worker") == 1  # exactly one title worker
    assert names.count("ocr-worker") == 3  # one per OCR_CONCURRENCY, not one
    assert not _dangling_serve_tasks()
    store.close()


async def test_sweep_loop_survives_an_escaping_sweep_failure(tmp_path, caplog):
    """Sweep-guard pin: kills removing the try/except around `sweep_once` in
    `_sweep_loop`. Failures really do escape the shipped sweep_once — a listing
    entry without `id` raises KeyError outside its inner guards — and `serve`
    only logs `task sweep exited unexpectedly` without restarting, so the
    removed guard means externally uploaded PDFs are never discovered or OCR'd
    until the process restarts."""
    import logging

    class EscapingSweepPapra:
        """Iteration 1: a listing entry with no `id` (API drift) -> KeyError
        escapes sweep_once. Iteration 2: the stop seam (BaseException passes
        the guard), so reaching it proves the loop survived iteration 1."""

        def __init__(self):
            self.listings = 0

        async def list_documents(self, org_id, **kwargs):
            self.listings += 1
            if self.listings == 1:
                return [{"name": "no-id.pdf", "mimeType": "application/pdf"}], 1
            raise KeyboardInterrupt()

    from papra_suite.pipeline import _sweep_loop

    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id="org_1", name="Org")
    papra = EscapingSweepPapra()

    with caplog.at_level(logging.ERROR):
        with pytest.raises(KeyboardInterrupt):
            await _sweep_loop(store, papra, interval_s=0)

    assert papra.listings == 2  # the loop SURVIVED the escaping KeyError
    assert any("sweep loop error" in record.getMessage() for record in caplog.records)
    store.close()


async def test_serve_spawns_the_verification_loop(tmp_path, monkeypatch):
    """Convergence-spawn pin: `serve` must actually start the pass, at six hours.

    The verification loop is the only thing that notices Papra overwrote a document we
    delivered — nothing in the pipeline pushes it — so a loop that is written, tested and
    never started looks identical to a corpus that has no divergence. The interval is
    asserted too: six hours is the design's cost model (five hundred documents is
    seconds, so a pass can be frequent), and a mutated constant would be invisible in a
    test that only checked the spawn.
    """
    import papra_suite.pipeline as pipeline_mod

    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id="org_1", name="Org")

    passes: list[int] = []
    seen: dict = {}

    async def recording_loop(store_arg, papra, kz, *, interval_s=21600, stop=None):
        seen.update(store=store_arg, papra=papra, kz=kz, interval_s=interval_s, stop=stop)
        while stop is None or not stop.is_set():
            passes.append(interval_s)
            if stop is not None:
                stop.set()                # the fake ends its own loop, as the real one would
            await asyncio.sleep(0)

    monkeypatch.setattr(pipeline_mod, "_verification_loop", recording_loop)

    stop = asyncio.Event()

    async def stop_after_first_pass():
        for _ in range(200):              # watchdog-shaped, like the sweep spawn pin above
            if passes:
                break
            await asyncio.sleep(0.01)
        stop.set()

    waiter = asyncio.get_running_loop().create_task(stop_after_first_pass())
    await serve(config, stop=stop, store=store)
    await waiter

    assert len(passes) == 1
    assert seen["interval_s"] == 21600
    assert seen["store"] is store and seen["stop"] is stop
    assert not _dangling_serve_tasks()
    store.close()


async def test_verification_loop_takes_its_first_pass_before_it_sleeps(tmp_path, monkeypatch):
    """Startup, then every six hours — in that order.

    The pass at startup is what makes a restart useful: the container is most likely to
    have been down while Papra kept writing, so the moment it comes back is exactly when
    the columns are most out of step. Sleeping first would make that drift wait six hours
    on every deploy. Asserted by the absence of a sleep, so a reordered loop fails here
    rather than by making the suite wait an interval.
    """
    from types import SimpleNamespace

    import papra_suite.pipeline as pipeline_mod
    from papra_suite.indexing import VerifyResult

    store = Store(tmp_path / "state.db")
    store.migrate()
    stop = asyncio.Event()
    calls: list[int] = []
    slept: list[float] = []

    async def spy_sleep(seconds, *args, **kwargs):
        slept.append(seconds)
        await asyncio.sleep(0)            # yields for real, so a loop that spins is visible

    async def fake_verify(store_arg, papra, kz):
        calls.append(1)
        stop.set()
        return VerifyResult(scanned=1)

    monkeypatch.setattr(pipeline_mod, "verify_once", fake_verify)
    # Patched on the pipeline module only: the test's own `asyncio` stays real, so the
    # spy cannot leak into the loop teardown of anything else running in this event loop.
    monkeypatch.setattr(pipeline_mod, "asyncio",
                        SimpleNamespace(sleep=spy_sleep,
                                        CancelledError=asyncio.CancelledError))

    await pipeline_mod._verification_loop(store, papra=None, kz=None,
                                         interval_s=21600, stop=stop)

    assert calls == [1]
    assert slept == []                    # the pass came first; the stop ended the loop
    store.close()


async def test_verification_loop_waits_the_default_interval_between_passes(tmp_path,
                                                                          monkeypatch):
    """Two passes, one sleep of six hours, and the loop exits on `stop`.

    The 21600 is pinned at the loop as well as at `serve`: the loop is what sleeps, and a
    default that drifted to 0 would turn a cheap six-hourly sweep into a hot PATCH loop
    against the record of record.
    """
    from types import SimpleNamespace

    import papra_suite.pipeline as pipeline_mod
    from papra_suite.indexing import VerifyResult

    store = Store(tmp_path / "state.db")
    store.migrate()
    stop = asyncio.Event()
    calls: list[int] = []
    slept: list[float] = []

    async def spy_sleep(seconds, *args, **kwargs):
        slept.append(seconds)
        await asyncio.sleep(0)

    async def fake_verify(store_arg, papra, kz):
        calls.append(1)
        if len(calls) >= 2:
            stop.set()
        return VerifyResult(scanned=1)

    monkeypatch.setattr(pipeline_mod, "verify_once", fake_verify)
    monkeypatch.setattr(pipeline_mod, "asyncio",
                        SimpleNamespace(sleep=spy_sleep,
                                        CancelledError=asyncio.CancelledError))

    await pipeline_mod._verification_loop(store, papra=None, kz=None, stop=stop)

    assert calls == [1, 1]
    assert slept == [21600]
    store.close()


async def test_verification_loop_survives_a_pass_that_raises(tmp_path, monkeypatch, caplog):
    """A six-hourly loop that dies once is a corpus that quietly stops converging.

    Twin of the sweep-loop pin: nothing else ever runs this pass, so an exception escaping
    one pass would end convergence for the rest of the container's life and leave no
    evidence but an absence of repairs.
    """
    import logging
    from types import SimpleNamespace

    import papra_suite.pipeline as pipeline_mod
    from papra_suite.indexing import VerifyResult

    caplog.set_level(logging.ERROR, logger="papra_suite.pipeline")
    store = Store(tmp_path / "state.db")
    store.migrate()
    stop = asyncio.Event()
    calls: list[int] = []

    async def spy_sleep(seconds, *args, **kwargs):
        await asyncio.sleep(0)

    async def failing_verify(store_arg, papra, kz):
        calls.append(1)
        if len(calls) == 1:
            raise sqlite3.OperationalError("database is locked")
        stop.set()
        return VerifyResult()

    monkeypatch.setattr(pipeline_mod, "verify_once", failing_verify)
    monkeypatch.setattr(pipeline_mod, "asyncio",
                        SimpleNamespace(sleep=spy_sleep,
                                        CancelledError=asyncio.CancelledError))

    await pipeline_mod._verification_loop(store, papra=None, kz=None, interval_s=0, stop=stop)

    assert len(calls) == 2                       # the second pass happened anyway
    assert any("verification" in record.getMessage().lower()
               for record in caplog.records)
    store.close()


async def test_poll_loop_sleeps_for_the_mailbox_poll_interval(tmp_path, monkeypatch):
    """Poll-cadence pin: kills `await asyncio.sleep(mailbox.poll_interval_s)` ->
    `asyncio.sleep(0)`, which polls every mailbox back-to-back (each poll
    re-selects and re-fetches up to MAX_POLL_BYTES) and hammers the operator's
    IMAP server instead of the configured cadence. Every existing poll-loop test
    uses interval 0 or an exception stop seam, so the sleep argument was never
    observed."""
    from types import SimpleNamespace

    import papra_suite.mail as mail_mod
    from papra_suite.pipeline import _poll_loop

    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_1", name="Org")
    mailbox_id = store.create_mailbox(org_id=org_id, host="127.0.0.1", port=1,
                                      username="u", password="p", poll_interval_s=45)
    polls = []

    class _StopLoop(BaseException):
        pass

    async def fake_poll(store_arg, papra, mailbox, org, allowlist=None):
        polls.append(mailbox.id)
        if len(polls) >= 3:
            raise _StopLoop()

    monkeypatch.setattr(mail_mod, "poll_mailbox_once", fake_poll)
    # The stub replaces the pipeline MODULE's `asyncio` name only, so the shared
    # asyncio module (the poll loop's regex-child teardown included) keeps its
    # real sleep and cannot pollute the observed cadence.
    slept: list[float] = []

    async def spy_sleep(seconds, *args, **kwargs):
        slept.append(seconds)

    from papra_suite import pipeline as pipeline_mod
    monkeypatch.setattr(pipeline_mod, "asyncio", SimpleNamespace(sleep=spy_sleep))
    with pytest.raises(_StopLoop):
        await _poll_loop(store, papra=None,
                     mailbox=store.get_mailbox(mailbox_id), allowlist=ALLOW)

    assert slept == [45, 45]  # the configured cadence, not a hot spin
    assert polls == [mailbox_id, mailbox_id, mailbox_id]  # 3 polls, 2 sleeps in between
    store.close()


async def test_serve_survives_store_close_failure(tmp_path, monkeypatch):
    """Shutdown store-guard pin: kills removing the try/except around
    `store.close()` in serve's shutdown. Its twin for the CLIENT closes is
    pinned only with a non-raising recorder, so the store guard is never
    executed: a busy sqlite handle or a disk error during the close of an
    otherwise clean shutdown would propagate out of serve -> an asyncio.run
    traceback, and main() would never return 0."""
    original_close = Store.close
    close_attempts = []

    def failing_close(self):
        close_attempts.append(True)
        try:
            original_close(self)  # release the sqlite handle anyway
        finally:
            raise RuntimeError("close failed")

    monkeypatch.setattr(Store, "close", failing_close)

    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    stop = asyncio.Event()

    async def set_stop_later():
        await asyncio.sleep(0.2)
        stop.set()

    asyncio.get_running_loop().create_task(set_stop_later())
    await serve(config, stop=stop)  # NO injected store: serve owns the close

    assert close_attempts == [True]  # the close was attempted and absorbed
    assert not _dangling_serve_tasks()


async def test_serve_closes_remaining_clients_when_one_close_fails(
        tmp_path, monkeypatch):
    """Close-guard pin: one built client's aclose raising must not skip the
    other closes — both LLM clients still close and serve's own store still
    closes. Reverting the per-close try wrappers fails this test."""
    import papra_suite.pipeline as pipeline_mod

    class FailingClosePapra:
        def __init__(self, base_url, api_key):
            pass

        async def aclose(self):
            raise RuntimeError("close blew up")

    llm_closes = []

    class RecordingLLM:
        def __init__(self, base_url, api_key, model, timeout_s=300.0):
            pass

        async def aclose(self):
            llm_closes.append(True)

    store_closes = []

    def recording_close(self):
        store_closes.append(True)

    monkeypatch.setattr(pipeline_mod, "PapraClient", FailingClosePapra)
    monkeypatch.setattr(pipeline_mod, "LLMClient", RecordingLLM)
    monkeypatch.setattr(Store, "close", recording_close)

    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    stop = asyncio.Event()

    async def set_stop_later():
        await asyncio.sleep(0.2)
        stop.set()

    asyncio.get_running_loop().create_task(set_stop_later())
    await serve(config, stop=stop)  # NO injected store: serve owns it

    assert len(llm_closes) == 2  # ocr + title clients closed despite the raise
    assert store_closes == [True]  # the owned store closed too
    assert not _dangling_serve_tasks()


async def test_serve_closes_own_store_on_shutdown(tmp_path, monkeypatch):
    """owns_store pin: serve WITHOUT an injected store constructs its own
    store and closes it on shutdown."""
    store_closes = []
    original_close = Store.close

    def recording_close(self):
        store_closes.append(True)
        original_close(self)

    monkeypatch.setattr(Store, "close", recording_close)

    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    stop = asyncio.Event()

    async def set_stop_later():
        await asyncio.sleep(0.2)
        stop.set()

    asyncio.get_running_loop().create_task(set_stop_later())
    await serve(config, stop=stop)  # serve builds AND owns the store

    assert store_closes == [True]  # constructed and closed its own
    assert not _dangling_serve_tasks()


async def test_poll_loop_survives_get_org_failure():
    """A get_org failure (inside the defensive try) must not kill the loop."""
    from types import SimpleNamespace

    from papra_suite.pipeline import _poll_loop

    class FailingOrgStore:
        def __init__(self, mailbox):
            self.mailbox = mailbox
            self.calls = 0

        def get_mailbox(self, mailbox_id):
            return self.mailbox  # the fixed loop re-fetches the row each iteration

        def get_org(self, org_id):
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("sqlite: database is locked")
            raise KeyboardInterrupt()  # stop seam (BaseException passes the guard)

    mailbox = SimpleNamespace(id="mb_1", org_id="org_1", poll_interval_s=0,
                              enabled=True)
    store = FailingOrgStore(mailbox)

    with pytest.raises(KeyboardInterrupt):
        await _poll_loop(store, papra=None, mailbox=mailbox, allowlist=ALLOW)

    assert store.calls == 3  # the loop survived BOTH get_org failures


async def test_poll_loop_survives_get_mailbox_failure():
    """A get_mailbox failure (inside the defensive try) must not kill the loop."""
    from types import SimpleNamespace

    from papra_suite.pipeline import _poll_loop

    class FailingMailboxStore:
        def __init__(self, mailbox):
            self.mailbox = mailbox
            self.calls = 0

        def get_mailbox(self, mailbox_id):
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("sqlite: database is locked")
            raise KeyboardInterrupt()  # stop seam (BaseException passes the guard)

        def get_org(self, org_id):
            raise AssertionError("must not be reached while get_mailbox fails")

    mailbox = SimpleNamespace(id="mb_1", org_id="org_1", poll_interval_s=0,
                              enabled=True)
    store = FailingMailboxStore(mailbox)

    with pytest.raises(KeyboardInterrupt):
        await _poll_loop(store, papra=None, mailbox=mailbox, allowlist=ALLOW)

    assert store.calls == 3  # the loop survived BOTH get_mailbox failures


async def test_poll_loop_polls_with_fresh_mailbox_row(tmp_path, monkeypatch):
    """Fresh-row pin: each iteration must re-fetch the mailbox from the
    store. Polling the startup snapshot forever would ignore the persisted
    UID cursor (every poll re-fetches the whole mailbox; once the prefix
    exceeds MAX_POLL_BYTES, newer mail is NEVER reached) and the disable
    flag. Advancing the row's last_seen_uid between iterations must be
    observed by the NEXT poll.
    """
    import papra_suite.mail as mail_mod
    from papra_suite.pipeline import _poll_loop

    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_1", name="Org")
    mailbox_id = store.create_mailbox(org_id=org_id, host="127.0.0.1", port=1,
                                      username="u", password="p", poll_interval_s=0)
    captured = []

    class _StopLoop(BaseException):
        pass

    async def fake_poll(store_arg, papra, mailbox, org, allowlist=None):
        captured.append(mailbox.last_seen_uid)
        # simulate a successful poll persisting a new UID cursor
        store_arg.record_poll_success(
            mailbox.id, last_seen_uid=(mailbox.last_seen_uid or 0) + 10,
            uidvalidity="v1",
        )
        if len(captured) >= 2:
            raise _StopLoop()

    monkeypatch.setattr(mail_mod, "poll_mailbox_once", fake_poll)
    with pytest.raises(_StopLoop):
        await _poll_loop(store, papra=None,
                     mailbox=store.get_mailbox(mailbox_id), allowlist=ALLOW)

    # iteration 1 saw the initial (None) cursor; iteration 2 must have seen
    # the PERSISTED cursor (10) — a stale-snapshot mutant sees None twice
    assert captured == [None, 10]
    store.close()


async def test_poll_loop_stops_when_mailbox_disabled_mid_loop(tmp_path, monkeypatch, caplog):
    """A mailbox disabled at runtime (consecutive failures) must stop its
    poll loop — the disable flag must not be inert."""
    import logging

    import papra_suite.mail as mail_mod
    from papra_suite.pipeline import _poll_loop

    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_1", name="Org")
    mailbox_id = store.create_mailbox(org_id=org_id, host="127.0.0.1", port=1,
                                      username="u", password="p", poll_interval_s=0)
    polls = []

    async def failing_poll(store_arg, papra, mailbox, org, allowlist=None):
        polls.append(mailbox.id)
        store_arg.record_poll_failure(mailbox.id, "boom", disable_after=1)

    monkeypatch.setattr(mail_mod, "poll_mailbox_once", failing_poll)
    with caplog.at_level(logging.INFO):
        await _poll_loop(store, papra=None,
                     mailbox=store.get_mailbox(mailbox_id), allowlist=ALLOW)

    assert polls == [mailbox_id]  # one poll, then the disabled row stopped the loop
    assert any("deleted or disabled" in record.getMessage() for record in caplog.records)
    store.close()


async def test_poll_loop_stops_when_mailbox_deleted_mid_loop(tmp_path, monkeypatch, caplog):
    """A mailbox deleted at runtime must stop its poll loop (get_mailbox
    returns None), not keep polling a ghost row."""
    import logging

    import papra_suite.mail as mail_mod
    from papra_suite.pipeline import _poll_loop

    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_1", name="Org")
    mailbox_id = store.create_mailbox(org_id=org_id, host="127.0.0.1", port=1,
                                      username="u", password="p", poll_interval_s=0)
    polls = []

    async def deleting_poll(store_arg, papra, mailbox, org, allowlist=None):
        polls.append(mailbox.id)
        store_arg.delete_mailbox(mailbox.id)

    monkeypatch.setattr(mail_mod, "poll_mailbox_once", deleting_poll)
    with caplog.at_level(logging.INFO):
        await _poll_loop(store, papra=None,
                     mailbox=store.get_mailbox(mailbox_id), allowlist=ALLOW)

    assert polls == [mailbox_id]
    assert any("deleted or disabled" in record.getMessage() for record in caplog.records)
    store.close()


async def test_poll_loop_survives_missing_org_without_disable_storm(
        tmp_path, monkeypatch, caplog):
    """Org-None guard pin: a mailbox whose org_id points at a deleted org
    must be skipped (logged, no poll) — the loop survives and the mailbox is
    NOT dragged into the 3-failure disable storm by polls against a ghost
    org. Dropping the guard fails this test (poll runs, failures recorded).
    """
    import logging

    import papra_suite.mail as mail_mod
    from papra_suite.pipeline import _poll_loop

    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_1", name="Org")
    mailbox_id = store.create_mailbox(org_id=org_id, host="127.0.0.1", port=1,
                                      username="u", password="p", poll_interval_s=0)
    store._conn.execute("PRAGMA foreign_keys=OFF")  # simulate the org being
    with store._conn:  # deleted out from under the mailbox (FK bypassed)
        store._conn.execute("DELETE FROM orgs WHERE id = ?", (org_id,))
    store._conn.execute("PRAGMA foreign_keys=ON")

    poll_calls = []

    async def ghost_org_poll(store_arg, papra, mailbox, org, allowlist=None):
        poll_calls.append(org)  # must NEVER run: org is None

    monkeypatch.setattr(mail_mod, "poll_mailbox_once", ghost_org_poll)

    state = {"n": 0}

    def get_org(org_id):
        state["n"] += 1
        if state["n"] > 2:  # stop seam after the loop survived 2 iterations
            raise KeyboardInterrupt()
        return None  # the org is gone

    store.get_org = get_org
    with caplog.at_level(logging.INFO):
        with pytest.raises(KeyboardInterrupt):
            await _poll_loop(store, papra=None,
                     mailbox=store.get_mailbox(mailbox_id), allowlist=ALLOW)

    assert poll_calls == []  # the guard held: no poll against a missing org
    mailbox = store.get_mailbox(mailbox_id)
    assert mailbox.enabled  # no disable storm
    assert mailbox.consecutive_failures == 0
    assert mailbox.last_error is None
    assert any("not found" in record.getMessage() for record in caplog.records)
    store.close()
async def test_serve_with_sweep_disabled_spawns_no_sweep_task(tmp_path, monkeypatch):
    """Sweep-disable guard pin: serve with SWEEP_INTERVAL_S=0 must NOT
    spawn the sweep task. A monkeypatched sweep_once records invocations;
    dropping the `> 0` guard fails this test (the sweep runs)."""
    import papra_suite.pipeline as pipeline_mod
    from papra_suite.pipeline import SweepResult

    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})  # SWEEP_INTERVAL_S=0
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id="org_1", name="Org")  # no mailboxes

    sweeps = []

    async def recording_sweep(store_arg, papra):
        sweeps.append(store_arg)
        return SweepResult()

    monkeypatch.setattr(pipeline_mod, "sweep_once", recording_sweep)

    stop = asyncio.Event()

    async def set_stop_later():
        await asyncio.sleep(0.2)
        stop.set()

    asyncio.get_running_loop().create_task(set_stop_later())
    await serve(config, stop=stop, store=store)

    assert sweeps == []  # the sweep task never spawned
    assert not _dangling_serve_tasks()
    store.close()


async def test_serve_prunes_old_ingest_events_at_startup(tmp_path):
    """Retention is enforced where the store is opened, not by an operator
    remembering to run a vacuum. The assertion is that serve itself removed the
    old row and kept the fresh one — not that the method exists."""
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    _seed_aged_event(store, days=91)
    store.record_ingest_event(mailbox_id="mb1", uid=2, kind="sender_denied")

    stop = asyncio.Event()
    stop.set()   # startup work runs before serve waits, so this still exercises it
    await serve(config, stop=stop, store=store)

    assert [event.uid for event in store.list_ingest_events()] == [2]
    store.close()


class _StubSweepPapra:
    """One empty listing, then the stop seam. `KeyboardInterrupt` is a
    BaseException, so it passes the loop's guard and ends the test — reaching it
    proves the previous pass completed."""

    def __init__(self):
        self.listings = 0

    async def list_documents(self, org_id, **kwargs):
        self.listings += 1
        if self.listings >= 2:
            raise KeyboardInterrupt()
        return [], 0


def _seed_aged_event(store, days):
    store._conn.execute(
        "INSERT INTO ingest_events (ts, mailbox_id, uid, kind) VALUES (?, 'mb1', 1,"
        " 'sender_denied')", ((datetime.now(UTC) - timedelta(days=days)).isoformat(),))
    store._conn.commit()


async def test_a_sweep_pass_prunes_expired_events(tmp_path):
    """Retention runs on the sweep's heartbeat, not only at startup, so a long
    uptime cannot grow the table past its window."""
    from papra_suite.pipeline import _sweep_loop

    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id="org_1", name="Org")
    _seed_aged_event(store, days=91)
    _seed_aged_event(store, days=89)
    papra = _StubSweepPapra()

    with pytest.raises(KeyboardInterrupt):
        await _sweep_loop(store, papra, interval_s=0)

    assert papra.listings == 2
    assert len(store.list_ingest_events()) == 1
    store.close()


async def test_a_prune_that_fails_does_not_stop_the_sweep(tmp_path, caplog):
    """Retention is the least important write here; a locked database must cost a
    stale listing, never a pipeline that stops discovering documents."""
    import logging

    from papra_suite.pipeline import _sweep_loop

    class PruneHostile:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def prune_ingest_events(self, retention_days=90):
            raise sqlite3.OperationalError("database is locked")

    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id="org_1", name="Org")
    papra = _StubSweepPapra()

    with caplog.at_level(logging.WARNING), pytest.raises(KeyboardInterrupt):
        await _sweep_loop(PruneHostile(store), papra, interval_s=0)

    assert papra.listings == 2       # the sweep kept going
    assert any("could not prune ingest events" in record.getMessage()
               for record in caplog.records)
    store.close()
