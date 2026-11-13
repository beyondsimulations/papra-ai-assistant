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


async def test_poll_loop_survives_get_org_failure():
    """A get_org failure (inside the defensive try) must not kill the loop."""
    from types import SimpleNamespace

    from papra_suite.pipeline import _poll_loop

    class FailingOrgStore:
        def __init__(self):
            self.calls = 0

        def get_org(self, org_id):
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("sqlite: database is locked")
            raise KeyboardInterrupt()  # stop seam (BaseException passes the guard)

    store = FailingOrgStore()
    mailbox = SimpleNamespace(id="mb_1", org_id="org_1", poll_interval_s=0)

    with pytest.raises(KeyboardInterrupt):
        await _poll_loop(store, papra=None, mailbox=mailbox)

    assert store.calls == 3  # the loop survived BOTH get_org failures
