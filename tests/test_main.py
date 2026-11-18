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


def _seed_index_corpus(tmp_path):
    """A ledger with one row of each shape `check` has to distinguish.

    Written through the Store API rather than by hand, so the line under test is rendered
    from counts the store actually produces:

    * `doc-1` — indexed and delivered, with an extraction that came back empty, and two
      repairs inside the window.
    * `doc-2` — indexed, never delivered, and owed an upstream delete.
    * `doc-3` — never indexed, and a delete the retries have given up on.

    Three fields are also protected, one per field and with different counts, because
    `protected_*` is the part of this line that tells an operator whether a corpus-wide lock
    exists: the numbers already come out of `ledger_stats`, and today the metadata CLI is the
    only other reader of them.
    """
    from papra_suite.store import EMPTY_EXTRACTION_SHA, Store

    store = Store(tmp_path / "state.db")
    store.migrate()
    generation = store.get_generation()
    for document_id in ("doc-1", "doc-2", "doc-3"):
        store.ledger_upsert_reference(papra_org_id="org_1", papra_document_id=document_id,
                                      file_hash="ab" * 32,
                                      papra_original_name=f"{document_id}.pdf")
    for document_id in ("doc-1", "doc-2"):
        store.ledger_set_kreuzakt_id(papra_org_id="org_1", papra_document_id=document_id,
                                     kreuzakt_id=7, generation=generation)
    store.ledger_record_delivery(papra_org_id="org_1", papra_document_id="doc-1",
                                 delivered_at="2026-11-17 10:00:00",
                                 delivered_sha=EMPTY_EXTRACTION_SHA,
                                 generation=generation)
    store.ledger_bump_repair(papra_org_id="org_1", papra_document_id="doc-1")
    store.ledger_bump_repair(papra_org_id="org_1", papra_document_id="doc-1")
    store.ledger_queue_cleanup(papra_org_id="org_1", papra_document_id="doc-2",
                               due_at="2026-11-17T10:00:00+00:00", kreuzakt_id=7,
                               generation=generation)
    with store._conn:      # the gave-up-on state a cleanup pass reaches after its cap
        store._conn.execute("UPDATE index_ledger SET cleanup_state = 'failed'"
                            " WHERE papra_document_id = 'doc-3'")
    for document_id in ("doc-1", "doc-2"):
        store.ledger_protect(papra_org_id="org_1", papra_document_id=document_id,
                             field="name")
    store.ledger_protect(papra_org_id="org_1", papra_document_id="doc-1", field="date")
    store.ledger_protect(papra_org_id="org_1", papra_document_id="doc-3", field="notes")
    store.close()


def test_check_prints_the_index_counts(monkeypatch, tmp_path, capsys):
    """The corpus line, in the order an operator reads it: what exists, what is indexed,
    what Papra holds, and the two numbers that say the extractor or the cleanup is hurt.

    Format is part of the contract: this line is what gets pasted into a bug report, and
    `extraction-empty` is the one number that separates "a few odd pages" from "the
    extractor is wrong for this corpus" — a silently missing field would remove the
    warning without removing the confidence.
    """
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    _seed_index_corpus(tmp_path)
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 0

    out = capsys.readouterr().out
    assert ("index: 3 refs, 2 current, 1 delivered, 1 extraction-empty, "
            "protected_name=2 protected_date=1 protected_notes=1, "
            "2 repairs(24h), 2 cleanup-pending (failed 1)") in out


def test_check_counts_an_empty_corpus_without_hiding_it(monkeypatch, tmp_path, capsys):
    """A fresh install says so in the same shape, rather than omitting the line."""
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 0

    assert ("index: 0 refs, 0 current, 0 delivered, 0 extraction-empty, "
            "protected_name=0 protected_date=0 protected_notes=0, "
            "0 repairs(24h), 0 cleanup-pending (failed 0)") in capsys.readouterr().out


def test_check_reports_an_unreadable_index_instead_of_a_traceback(monkeypatch, tmp_path,
                                                                  capsys):
    """A corrupt generation degrades the line, and says which half is unusable.

    `ledger_stats` fails closed on a generation it cannot read, and `check` is the command
    an operator runs BECAUSE something is wrong — so the failure has to arrive as a
    reason, not as a traceback that hides the config and sender lines above it.
    """
    from papra_suite.store import Store

    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    store = Store(tmp_path / "state.db")
    store.migrate()
    # The same corruption `test_store_ledger` pictures: a settings row that became a
    # note instead of a number, so no generation is current.
    store.set_setting("index_generation", "rebuild-2026-11-17")
    store.close()
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 0

    out = capsys.readouterr().out
    assert "index: " in out and "generation" in out
    assert "Traceback" not in out


def test_check_names_the_content_writer(monkeypatch, tmp_path, capsys):
    """One line answers "who writes Papra's text right now", including when nobody does.

    The three states are the handover: the legacy writer, the bridge, and the window in
    between where nobody owns the column. The last one is legitimate for minutes and
    wrong for days, and there is no other place an operator can see it.
    """
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 0
    assert "content writer: suite ocr worker (legacy)" in capsys.readouterr().out

    monkeypatch.setenv("PAPRA_INDEX_CONTENT_DELIVERY", "1")
    monkeypatch.setenv("PAPRA_LEGACY_WORKERS", "0")
    assert main() == 0
    assert "content writer: index bridge" in capsys.readouterr().out

    monkeypatch.setenv("PAPRA_INDEX_CONTENT_DELIVERY", "0")
    assert main() == 0
    assert "content writer: none (handover window)" in capsys.readouterr().out


def test_check_refuses_a_config_with_two_content_writers(monkeypatch, tmp_path, capsys):
    """`check` proves the deployment can start, so it refuses what `serve` would refuse.

    Catching it here rather than in a restart loop is the difference between an operator
    reading the order once and reading a container log twice a day.
    """
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PAPRA_INDEX_CONTENT_DELIVERY", "1")
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 2

    err = capsys.readouterr().err
    assert "PAPRA_INDEX_CONTENT_DELIVERY" in err
    assert "PAPRA_LEGACY_WORKERS" in err


def test_serve_exits_two_on_a_conflicting_writer_config(monkeypatch, tmp_path, capsys):
    """The same refusal reaches the process exit code, not just a traceback.

    A container that dies with a stack trace gets filed as a crash; one that prints the
    order to fix gets fixed.
    """
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PAPRA_INDEX_CONTENT_DELIVERY", "1")
    monkeypatch.setattr(sys, "argv", ["papra-suite", "serve"])

    assert main() == 2

    err = capsys.readouterr().err
    assert "configuration error" in err and "quiesc" in err.lower()
    # Deliberately NOT asserted: that stderr holds no traceback. `serve` logs the refusal
    # before `setup_logging` attaches this process's store handler, and in a shared test
    # process a handler left by an earlier test writes into a closed store, which Python
    # prints as "--- Logging error ---" with its own traceback. That is harness noise, not
    # a crash; the contract is the exit code and the message.
    assert "PAPRA_LEGACY_WORKERS" in err and "PAPRA_INDEX_CONTENT_DELIVERY" in err


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
    # The cursor IS the last uid seen, so uid 10 itself will not be read again.
    # `papra-suite events` prints uids, and its help and README both tell the reader
    # to pass one LESS than the uid they want — an off-by-one here means the message
    # an operator came for is the one message that stays behind. Pinned end-to-end
    # against a real IMAP server in tests/integration/test_mail_ingestion.py.
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
                                      "upload_failed", "--since", "2099-01-01"])

    assert main() == 0
    out = capsys.readouterr().out
    assert "no matching ingest events" in out
    assert "upload_failed" in out and "since 2099-01-01" in out
    assert "90 days" in out       # says what the window is, so empty is readable


def test_events_rejects_a_malformed_since_date(monkeypatch, tmp_path, capsys):
    """`--since` is a date, not a number of days: "7" would silently mean the year
    7 AD to a lexical timestamp comparison, which reads as an empty inbox."""
    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(mailbox_id="mb_7", uid=1, kind="sender_denied")])
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events", "--since", "7"])

    assert main() == 2
    assert "2026-11-09" in capsys.readouterr().err   # names the expected shape


def test_events_since_is_a_utc_date_boundary(monkeypatch, tmp_path, capsys):
    """The cutoff is midnight UTC of the given day, and a row from that day is
    inside the window: an off-by-one here silently hides a day of refusals."""
    import sqlite3
    from datetime import UTC, datetime, timedelta

    _events_env(monkeypatch, tmp_path)
    yesterday = datetime.now(UTC) - timedelta(days=1)
    _seed_events(tmp_path, [
        dict(mailbox_id="mb_7", uid=1, kind="sender_denied"),
        dict(mailbox_id="mb_7", uid=2, kind="upload_failed"),
    ])
    # age one row to just before midnight UTC of yesterday, leave the other now
    midnight = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    boundary = midnight - timedelta(seconds=1)
    conn = sqlite3.connect(tmp_path / "state.db")
    conn.execute("UPDATE ingest_events SET ts = ? WHERE uid = 2",
                 (boundary.isoformat(),))
    conn.commit()
    conn.close()
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events", "--since",
                                      yesterday.date().isoformat()])

    assert main() == 0
    lines = _data_lines(capsys.readouterr().out)
    assert len(lines) == 1 and "\t2\t" not in lines[0]   # only uid 1 survives


def test_events_rejects_an_unknown_kind(monkeypatch, tmp_path, capsys):
    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(mailbox_id="mb_7", uid=1, kind="sender_denied")])
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events", "--kind", "denied"])

    assert main() == 2
    err = capsys.readouterr().err
    assert "unknown kind" in err and "sender_denied" in err


def test_events_refuses_a_missing_database_without_creating_one(
        monkeypatch, tmp_path, capsys):
    """Store.__init__ creates the parent directory AND the file, so checking for a
    missing database after opening one is impossible: the symptom the operator came
    to read would have been replaced by an empty store that answers "no events".
    The file must not exist afterwards, in any size."""
    _events_env(monkeypatch, tmp_path)
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events"])

    assert main() == 2
    err = capsys.readouterr().err
    assert "no database at" in err and "papra-suite check" in err
    assert not (tmp_path / "state.db").exists()


def test_events_on_an_unmigrated_store_says_so_instead_of_tracing_back(
        monkeypatch, tmp_path, capsys):
    """`events` never migrates — a curiosity query must not perform a schema
    change — so a store that predates the table is a stated condition with the fix
    in the message, and the distinction from "no database at all" is preserved."""
    _events_env(monkeypatch, tmp_path)
    (tmp_path / "state.db").touch()      # a database file with no tables
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events"])

    assert main() == 2
    err = capsys.readouterr().err
    assert "no ingest_events table" in err and "papra-suite check" in err


def test_events_json_emits_the_record_fields(monkeypatch, tmp_path, capsys):
    """`--json` is the record, not the display: `-` for absent is a rendering
    convention, and a script reading null as "-" is a bug waiting for a quiet
    day."""
    import dataclasses
    import json

    from papra_suite.store import IngestEvent

    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(mailbox_id="mb_7", uid=512, kind="sender_denied",
                                 sender="Stranger <spam@other.net>")])
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events", "--json"])

    assert main() == 0
    rows = json.loads(capsys.readouterr().out)
    assert [row["uid"] for row in rows] == [512]
    assert rows[0]["uidvalidity"] is None
    assert set(rows[0]) == {field.name for field in dataclasses.fields(IngestEvent)}


def test_check_counts_yesterday_and_today_apart(monkeypatch, tmp_path, capsys):
    """`check` is the command an operator already runs, so the counts live there:
    "polling fine, nothing arrives" should be one command, not two."""
    from datetime import UTC, datetime, timedelta

    from papra_suite.store import Store

    _events_env(monkeypatch, tmp_path)
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])
    assert main() == 0
    out = capsys.readouterr().out
    assert "config ok" in out and "events (last 24h): none" in out

    store = Store(tmp_path / "state.db")
    store.record_ingest_event(mailbox_id="mb_7", uid=1, kind="sender_denied")
    store.record_ingest_event(mailbox_id="mb_7", uid=2, kind="sender_denied")
    old = (datetime.now(UTC) - timedelta(days=3)).isoformat()
    store._conn.execute(
        "INSERT INTO ingest_events (ts, mailbox_id, uid, kind) VALUES (?, 'mb_7', 3,"
        " 'upload_failed')", (old,))
    store._conn.commit()
    store.close()

    assert main() == 0
    out = capsys.readouterr().out
    assert "events (last 24h): sender_denied=2" in out
    assert "upload_failed" not in out.split("events (last 24h)")[1]


def test_events_escapes_bytes_a_terminal_would_act_on(monkeypatch, tmp_path, capsys):
    """The threat is not a newline, it is an escape sequence.

    These rows are written straight into the table, bypassing the store's writer,
    because that is exactly the case the renderer exists for: a database written by
    an older build, or by hand while debugging, still contains what the messages
    contained. `\\x1b[2J` clears a screen and C1 CSI (`\\x9b`) starts a control
    sequence; neither contains whitespace, so a quoting rule keyed on whitespace
    alone prints them raw and lets the filename format the terminal showing it.
    """
    import sqlite3

    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(mailbox_id="mb_7", uid=1, kind="sender_denied")])
    hostile = ["\x1b[2Jevil.pdf", "a\x9bQ", "line\u2028break", "zero\u200bwidth"]
    conn = sqlite3.connect(tmp_path / "state.db")
    for index, value in enumerate(hostile):
        conn.execute("INSERT INTO ingest_events (ts, mailbox_id, uid, kind, filename)"
                     " VALUES ('2026-11-16T00:00:00+00:00', 'mb_7', ?, 'sender_denied',"
                     " ?)", (index + 2, value))
    conn.commit()
    conn.close()
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events"])

    assert main() == 0
    out = capsys.readouterr().out
    assert len(_data_lines(out)) == len(hostile) + 1
    for raw in ("\x1b", "\x9b", "\u2028", "\u200b"):
        assert raw not in out                 # never raw
    for escaped in ("\\u001b", "\\u009b", "\\u2028", "\\u200b"):
        assert escaped in out                 # and visible as what it was


def test_events_json_leaves_no_terminal_control_bytes(monkeypatch, tmp_path, capsys):
    """`--json` is machine output, but machine output is piped into `less` and
    pasted into terminals. json.dumps escapes C0 and leaves C1 and U+2028 raw; the
    escaping stays inside the JSON string, so the document is still valid."""
    import json
    import sqlite3

    _events_env(monkeypatch, tmp_path)
    _seed_events(tmp_path, [dict(mailbox_id="mb_7", uid=1, kind="sender_denied",
                                 filename="R\u00f6ntgen.pdf")])
    conn = sqlite3.connect(tmp_path / "state.db")
    conn.execute("INSERT INTO ingest_events (ts, mailbox_id, uid, kind, filename)"
                 " VALUES ('2026-11-16T00:00:00+00:00', 'mb_7', 2, 'sender_denied',"
                 " 'a\x9bb\u2028')")
    conn.commit()
    conn.close()
    monkeypatch.setattr(sys, "argv", ["papra-suite", "events", "--json"])

    assert main() == 0
    out = capsys.readouterr().out
    assert "\x9b" not in out and "\u2028" not in out
    rows = json.loads(out)                     # still parseable
    assert rows[0]["filename"] == "R\u00f6ntgen.pdf"   # readable, not \\u00f6
    assert any(row["filename"] == "a\x9bb\u2028" for row in rows)  # value preserved
