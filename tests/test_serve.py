"""Unit tests for `serve`: the process runtime (start, recover, stop cleanly).

The CONTRACT: serve starts, survives poll failures against an unreachable
IMAP host, recovers stale running jobs ONCE at startup, returns cleanly on
stop, and closes only what it constructed (an injected store must NOT be
closed — the tests keep using their store handle after serve returns).
"""

import asyncio

import pytest

from papra_suite.config import load_config
from papra_suite.pipeline import serve
from papra_suite.store import Store

BASE_ENV = {
    "PAPRA_BASE_URL": "http://papra:1221", "PAPRA_API_KEY": "k",
    "LLM_BASE_URL": "http://llm:8000", "LLM_API_KEY": "k",
    "TITLE_MODEL": "t", "OCR_MODEL": "o", "ADMIN_PASSWORD": "p",
    "SWEEP_INTERVAL_S": "0",  # disable the sweep for the test
}

# coroutine qualnames of the long-lived tasks serve spawns; after serve
# returns, none of them may linger (clean shutdown, no dangling tasks)
_SERVE_TASK_NAMES = {"_poll_loop", "worker_loop", "_sweep_loop"}


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

    async def dying_poll_loop(store, papra, mailbox):
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

    async def sweep_once_and_stop(store_arg, papra):
        sweeps.append(store_arg)
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
    assert not _dangling_serve_tasks()
    store.close()


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
        await _poll_loop(store, papra=None, mailbox=mailbox)

    assert store.calls == 3  # the loop survived BOTH get_org failures


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

    async def fake_poll(store_arg, papra, mailbox, org):
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
        await _poll_loop(store, papra=None, mailbox=store.get_mailbox(mailbox_id))

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

    async def failing_poll(store_arg, papra, mailbox, org):
        polls.append(mailbox.id)
        store_arg.record_poll_failure(mailbox.id, "boom", disable_after=1)

    monkeypatch.setattr(mail_mod, "poll_mailbox_once", failing_poll)
    with caplog.at_level(logging.INFO):
        await _poll_loop(store, papra=None, mailbox=store.get_mailbox(mailbox_id))

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

    async def deleting_poll(store_arg, papra, mailbox, org):
        polls.append(mailbox.id)
        store_arg.delete_mailbox(mailbox.id)

    monkeypatch.setattr(mail_mod, "poll_mailbox_once", deleting_poll)
    with caplog.at_level(logging.INFO):
        await _poll_loop(store, papra=None, mailbox=store.get_mailbox(mailbox_id))

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

    async def ghost_org_poll(store_arg, papra, mailbox, org):
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
            await _poll_loop(store, papra=None, mailbox=store.get_mailbox(mailbox_id))

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
