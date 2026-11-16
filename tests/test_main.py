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


# -- papra-suite events ------------------------------------------------------

def _events_env(monkeypatch, tmp_path):
    _replay_env(monkeypatch, tmp_path, None)


def _event_columns():
    from papra_suite.__main__ import EVENT_COLUMNS

    return EVENT_COLUMNS


def _seed_events(tmp_path, rows):
    """Rows written straight through the store, so the CLI is tested against
    realistic content rather than against a hand-written fixture."""
    from papra_suite.store import Store

    store = Store(tmp_path / "state.db")
    store.migrate()
    for row in rows:
        store.record_ingest_event(**row)
    store.close()


def _data_lines(output):
    lines = [line for line in output.strip().splitlines() if line]
    return lines[1:] if lines else []


def test_events_prints_the_recorded_disposition(monkeypatch, tmp_path, capsys):
    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(
        mailbox_id="mb_7", uid=512, kind="sender_denied", uidvalidity="8813",
        papra_org_id="org_abc", sender="Stranger <spam@other.net>")])
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events"])

    assert main() == 0
    out = capsys.readouterr().out
    assert "kind" in out.splitlines()[0]      # a header, so columns are decidable
    line = _data_lines(out)[0]
    for expected in ("mb_7", "512", "8813", "sender_denied", "org_abc",
                     "spam@other.net"):
        assert expected in line


def test_events_prints_a_dash_for_an_absent_value(monkeypatch, tmp_path, capsys):
    """Absent must not print as nothing: an empty field in a tab-separated row
    shifts every column after it, and "no sender" would look like a parse error."""
    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(mailbox_id="mb_7", uid=9, kind="message_oversized")])
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events"])

    assert main() == 0
    fields = _data_lines(capsys.readouterr().out)[0].split("\t")
    assert len(fields) == len(_event_columns())
    assert fields.count("-") == 5   # uidvalidity, org, sender, filename, detail
    assert '""' not in fields


def test_events_quotes_a_value_that_could_forge_a_row(monkeypatch, tmp_path, capsys):
    """Defence in depth: the store sanitises on write, but rows written before
    that rule existed are still in old databases, and a filename with a newline
    in it must not become a second row on the way to a terminal."""
    import sqlite3

    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(mailbox_id="mb_7", uid=1, kind="sender_denied")])
    conn = sqlite3.connect(tmp_path / "state.db")
    conn.execute("UPDATE ingest_events SET filename = ?", ("R\u00f6ntgen\nname.pdf",))
    conn.commit()
    conn.close()
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events"])

    assert main() == 0
    out = capsys.readouterr().out
    assert len(_data_lines(out)) == 1
    assert "\\n" in out            # escaped, so the byte is visible and inert
    assert "R\u00f6ntgen" in out   # readable, not escaped into \u00f6: reading is
                                    # the only job this line has


def test_events_clamps_the_limit(monkeypatch, tmp_path, capsys):
    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(mailbox_id="mb_7", uid=uid, kind="sender_denied")
                            for uid in range(3)])
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events", "--limit", "0"])

    assert main() == 0
    assert len(_data_lines(capsys.readouterr().out)) == 1


def test_events_filters_by_kind_and_mailbox(monkeypatch, tmp_path, capsys):
    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [
        dict(mailbox_id="mb_7", uid=1, kind="sender_denied"),
        dict(mailbox_id="mb_8", uid=2, kind="upload_failed"),
    ])
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events", "--kind",
                                      "upload_failed", "--mailbox-id", "mb_8"])

    assert main() == 0
    lines = _data_lines(capsys.readouterr().out)
    assert len(lines) == 1 and "upload_failed" in lines[0] and "mb_8" in lines[0]


def test_events_empty_answer_says_what_was_asked(monkeypatch, tmp_path, capsys):
    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(mailbox_id="mb_7", uid=1, kind="sender_denied")])
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events", "--kind",
                                      "upload_failed", "--since", "3"])

    assert main() == 0
    out = capsys.readouterr().out
    assert "no matching ingest events" in out
    assert "upload_failed" in out and "3 day" in out
    assert "90 days" in out       # says what the window is, so empty is readable


def test_events_rejects_an_unknown_kind(monkeypatch, tmp_path, capsys):
    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(mailbox_id="mb_7", uid=1, kind="sender_denied")])
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events", "--kind", "denied"])

    assert main() == 2
    err = capsys.readouterr().err
    assert "unknown kind" in err and "sender_denied" in err


def test_events_on_an_unmigrated_store_says_so_instead_of_tracing_back(
        monkeypatch, tmp_path, capsys):
    """`events` never migrates — a curiosity query must not perform a schema
    change — so an old store is a stated condition with the fix in the message."""
    _events_env(monkeypatch, tmp_path)
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events"])

    assert main() == 2
    err = capsys.readouterr().err
    assert "no ingest_events table" in err and "papra-suite check" in err
    assert not (tmp_path / "state.db").exists() or _table_is_absent(tmp_path)


def _table_is_absent(tmp_path):
    import sqlite3

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        return conn.execute("SELECT name FROM sqlite_master WHERE name ="
                            " 'ingest_events'").fetchone() is None
    finally:
        conn.close()
