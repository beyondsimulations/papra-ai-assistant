"""Tests pinning the `papra-suite check` command contract.

These call main() directly so mutations of the exit codes or of the
store.migrate() call fail a test.
"""

import asyncio
import sqlite3
import sys

import pytest

from papra_suite.__main__ import main
from tests.test_config import BASE_ENV


def test_version_flag_prints_version_and_exits_zero(monkeypatch, capsys):
    from papra_suite import __version__

    monkeypatch.setattr(sys, "argv", ["papra-suite", "--version"])
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_check_success_migrates_and_exits_zero(monkeypatch, tmp_path):
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 0

    db_path = tmp_path / "state.db"
    assert db_path.exists()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        user_version = conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()
    assert user_version == 1


def test_check_config_error_exits_two(monkeypatch, capsys, tmp_path):
    env = dict(BASE_ENV)
    del env["PAPRA_API_KEY"]
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 2
    stderr = capsys.readouterr().err
    assert "configuration error" in stderr
    assert "PAPRA_API_KEY" in stderr


def test_serve_subcommand_runs_serve(monkeypatch, tmp_path):
    """Serve-wiring pin: `papra-suite serve` must actually reach
    asyncio.run(serve(config)). A recording coroutine stands in for serve;
    dropping the asyncio.run call (or the subcommand branch) fails here."""
    import papra_suite.pipeline as pipeline_mod

    calls = []

    async def recording_serve(config, **kwargs):
        # serve is now handed the stop event that the signal handlers set
        # (`run_serve`), so the stub takes it and the assertion below pins that
        # wiring too: serve reached with the loaded config AND a stop event.
        calls.append((config, kwargs))

    monkeypatch.setattr(pipeline_mod, "serve", recording_serve)
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "serve"])

    assert main() == 0
    assert len(calls) == 1  # serve was reached with the loaded config
    config, kwargs = calls[0]
    assert config.data_dir == tmp_path
    assert isinstance(kwargs["stop"], asyncio.Event)


async def test_sigterm_stops_serve_and_kills_the_regex_child(tmp_path, monkeypatch):
    """`docker stop` pin: SIGTERM must reach serve's stop event, and the
    regex child a poll loop owns must die with it.

    Without a handler SIGTERM's default disposition kills the process where it
    stands, so serve's `finally` never runs — no client/store teardown, and
    every poll loop leaves its child behind (a busy one keeps burning CPU
    after its parent is gone, and only its own self-deadline ends it). The
    same wiring keeps Ctrl-C working: SIGINT now runs the identical clean
    stop instead of a KeyboardInterrupt into the loop.
    """
    import asyncio
    import os
    import signal

    import papra_suite.safematch as safematch_mod
    from papra_suite.__main__ import run_serve
    from papra_suite.config import load_config
    from papra_suite.store import Store
    from tests.test_serve import _dangling_serve_tasks

    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_1", name="Org")
    # a subject filter makes the FIRST poll spawn the child; the IMAP connect
    # then fails fast, but the matcher (and its child) live on with the loop
    store.create_mailbox(org_id=org_id, host="127.0.0.1", port=1, username="u",
                         password="p", subject_filter=".*x.*", poll_interval_s=3600)

    created: list = []

    class SpyMatcher(safematch_mod.KillableMatcher):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    monkeypatch.setattr(safematch_mod, "KillableMatcher", SpyMatcher)

    loop = asyncio.get_running_loop()
    # fallback FIRST: if the wiring under test is missing, the signal lands
    # here instead of killing the pytest process, and the assertion below
    # names the actual defect
    canary = asyncio.Event()
    loop.add_signal_handler(signal.SIGTERM, canary.set)
    task = asyncio.create_task(run_serve(config, store=store))
    try:
        child_pid = None
        for _ in range(3000):
            if created and created[0].pid is not None:
                child_pid = created[0].pid
                break
            await asyncio.sleep(0.01)
        assert child_pid is not None, "the poll loop never spawned a regex child"

        os.kill(os.getpid(), signal.SIGTERM)      # what docker stop sends
        await asyncio.wait_for(task, timeout=30)

        assert not canary.is_set(), (
            "SIGTERM was caught by the test's fallback handler: nothing wired "
            "it to serve's stop event, so serve's finally blocks never ran"
        )
        assert not _dangling_serve_tasks()        # the runtime stopped cleanly
        assert created[0].child_returncode is not None   # child killed AND reaped
        import pytest

        with pytest.raises(ProcessLookupError):   # and gone from the table
            os.kill(child_pid, 0)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        store.close()


async def test_sigint_stops_serve_too(tmp_path, monkeypatch):
    """Ctrl-C keeps working — and now it is the same graceful path rather
    than a KeyboardInterrupt thrown into the running loop."""
    import asyncio
    import os
    import signal

    from papra_suite.__main__ import run_serve
    from papra_suite.config import load_config
    from papra_suite.store import Store

    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id="org_1", name="Org")

    loop = asyncio.get_running_loop()
    previous = signal.getsignal(signal.SIGINT)
    canary = asyncio.Event()
    loop.add_signal_handler(signal.SIGINT, canary.set)
    task = asyncio.create_task(run_serve(config, store=store))
    try:
        for _ in range(200):                      # let serve reach its stop wait
            await asyncio.sleep(0.01)
        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.wait_for(task, timeout=30)
        assert not canary.is_set(), "SIGINT was not wired to the stop event"
    finally:
        loop.remove_signal_handler(signal.SIGINT)
        signal.signal(signal.SIGINT, previous)
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        store.close()
