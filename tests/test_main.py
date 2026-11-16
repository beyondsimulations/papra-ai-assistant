"""Tests pinning the `papra-suite check` command contract.

These call main() directly so mutations of the exit codes or of the
store.migrate() call fail a test.
"""

import asyncio
import sqlite3
import sys

import pytest

import papra_suite.store as store_module
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
    assert user_version == len(store_module.MIGRATIONS)


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


def _replay_env(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("PAPRA_BASE_URL", "http://papra:1221")
    monkeypatch.setenv("PAPRA_API_KEY", "k")
    monkeypatch.setenv("LLM_BASE_URL", "http://llm:8000")
    monkeypatch.setenv("LLM_API_KEY", "k")
    monkeypatch.setenv("TITLE_MODEL", "t")
    monkeypatch.setenv("OCR_MODEL", "o")
    monkeypatch.setenv("ADMIN_PASSWORD", "p")
    monkeypatch.setenv("PAPRA_ALLOWED_SENDERS", "sender@x.com")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))


def _seed_store(tmp_path, *, last_seen_uid=42):
    from papra_suite.store import Store

    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p"
    )
    store.record_poll_failure(mailbox_id, "boom")
    store.record_poll_failure(mailbox_id, "boom")
    with store._lock, store._conn:  # set a cursor without a successful poll
        store._conn.execute("UPDATE mailboxes SET last_seen_uid = ? WHERE id = ?",
                            (last_seen_uid, mailbox_id))
        store._conn.commit()
    mailbox = store.get_mailbox(mailbox_id)
    store.close()
    return mailbox_id, mailbox


def test_replay_dry_run_changes_nothing(monkeypatch, tmp_path, capsys):
    from papra_suite.__main__ import main
    from papra_suite.store import Store

    _replay_env(monkeypatch, tmp_path, capsys)
    mailbox_id, before = _seed_store(tmp_path)
    monkeypatch.setattr("sys.argv", ["papra-suite", "replay", "--mailbox-id",
                                     mailbox_id, "--dry-run"])

    assert main() == 0
    assert "dry run" in capsys.readouterr().out

    store = Store(tmp_path / "state.db")
    mailbox = store.get_mailbox(mailbox_id)
    store.close()
    assert mailbox.last_seen_uid == before.last_seen_uid
    assert mailbox.consecutive_failures == 2  # untouched


def test_replay_rewinds_the_cursor_and_clears_failure_state(monkeypatch, tmp_path,
                                                            capsys):
    from papra_suite.__main__ import main
    from papra_suite.store import Store

    _replay_env(monkeypatch, tmp_path, capsys)
    mailbox_id, _ = _seed_store(tmp_path)
    monkeypatch.setattr("sys.argv", ["papra-suite", "replay", "--mailbox-id",
                                     mailbox_id])

    assert main() == 0
    assert "rewound from 42" in capsys.readouterr().out

    store = Store(tmp_path / "state.db")
    mailbox = store.get_mailbox(mailbox_id)
    store.close()
    assert mailbox.last_seen_uid is None
    assert mailbox.consecutive_failures == 0 and mailbox.last_error is None


def test_replay_can_resume_after_a_given_uid(monkeypatch, tmp_path, capsys):
    from papra_suite.__main__ import main
    from papra_suite.store import Store

    _replay_env(monkeypatch, tmp_path, capsys)
    mailbox_id, _ = _seed_store(tmp_path, last_seen_uid=99)
    monkeypatch.setattr("sys.argv", ["papra-suite", "replay", "--mailbox-id",
                                     mailbox_id, "--from-uid", "10"])

    assert main() == 0
    store = Store(tmp_path / "state.db")
    assert store.get_mailbox(mailbox_id).last_seen_uid == 10
    store.close()


def test_replay_names_the_known_mailboxes_for_a_typo(monkeypatch, tmp_path, capsys):
    from papra_suite.__main__ import main

    _replay_env(monkeypatch, tmp_path, capsys)
    _seed_store(tmp_path)
    monkeypatch.setattr("sys.argv", ["papra-suite", "replay", "--mailbox-id", "nope"])

    assert main() == 2
    err = capsys.readouterr().err
    assert "unknown mailbox nope" in err and "known mailboxes:" in err
