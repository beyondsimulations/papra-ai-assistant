"""`papra-suite mcp` — the command that sets the two rows the listener is gated on.

Why this file exists at all: `tests/test_shipped_citations.py` pins the runbook to one raw
`sqlite3` invocation, and the MCP review round predicted the way that pin would be broken — *"two
settings rows gate the listener, and no command sets either… the operator will reach for
`PAPRA_MCP_HOST` and get the refusal."* The prediction arrived as documentation: the README ended up
handing an operator two `INSERT OR REPLACE INTO settings` statements, which is the exact shape the
guard exists to refuse. The fix is the missing command, not a looser guard.

So the properties here are the ones a raw `INSERT` could not provide:

**The transition is named.** `mcp_server_enabled: unset → 1` is a readable audit line; a statement
that silently overwrote a row an operator had set to `off` on purpose was neither of those.

**A rehearsal rehearses.** `--dry-run` writes no settings row and still says that it opened and
migrated the store, because a command that writes must migrate, and a promise of "nothing changed"
would be a lie about the one thing it did do (Task 11's rule).

**Widening exposure needs a second flag.** `--allow-nonloopback` moves a bearer token from a private
tailnet to whatever the chosen interface reaches, so it prints the consequence and asks for
`--yes-i-mean-it` the way `--reindex-all` does. `--dry-run` skips the ask, never the warning.

**The command will not create a listener that cannot start.** Enabling is refused when the state it
would produce is one `serve` refuses to start under — a routable `PAPRA_MCP_HOST` without the
non-loopback permission, or a `PAPRA_MCP_URL` it cannot use. Task 12's availability round was about
`check` asserting a listener that could not exist; writing the row that produces one is the same
defect with a signature.

**No credential material.** `--status` reports token *counts*. A token and a token digest are the
same secret seen from two sides, and this command's output is meant to be pasted into an issue.
"""

import argparse
import hashlib
import logging
import sqlite3
import sys
from types import SimpleNamespace

import pytest

from papra_suite.mcp_cli import (
    EXIT_INCOMPLETE,
    EXIT_OK,
    EXIT_REFUSED,
    add_subparser,
    run_mcp,
)
from papra_suite.mcp_server import MCP_ENABLED_SETTING, MCP_NONLOOPBACK_SETTING
from papra_suite.mcp_tokens import TOKEN_SETTING_PREFIX, mint_token, resolve_token
from papra_suite.store import Store
from tests.test_config import BASE_ENV

PUBLIC_URL = "https://papra-mcp.ts.net"
ROUTABLE_HOST = "10.0.0.5"


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.db_path = tmp_path / "state.db"
    yield store
    store.close()


@pytest.fixture
def config(tmp_path):
    """A config of the shape `load_config` produces, pointed at the fixture's data dir."""
    return SimpleNamespace(data_dir=tmp_path, mcp_host="127.0.0.1", mcp_port=8765,
                           mcp_url=PUBLIC_URL)


def parse(argv):
    parser = argparse.ArgumentParser(prog="papra-suite")
    sub = parser.add_subparsers(dest="command", required=True)
    add_subparser(sub)
    return parser.parse_args(["mcp", *argv])


def run(argv, store=None, config=None):
    return run_mcp(config, parse(argv), store=store)


def gated_rows(store):
    """The two rows this command owns, raw, as one dict — the haystack the "wrote nothing"
    assertions are made against."""
    return {key: store.get_setting(key) for key in (MCP_ENABLED_SETTING, MCP_NONLOOPBACK_SETTING)}


# --- the surface ---------------------------------------------------------------

def test_the_parser_takes_the_documented_surface():
    bare = parse([])
    assert not any(value for key, value in vars(bare).items() if key != "command")
    assert parse(["--status"]).status is True
    assert parse(["--enable"]).enable is True
    assert parse(["--disable"]).disable is True
    assert parse(["--allow-nonloopback"]).allow_nonloopback is True
    assert parse(["--deny-nonloopback"]).deny_nonloopback is True
    combined = parse(["--enable", "--allow-nonloopback", "--dry-run", "--yes-i-mean-it"])
    assert (combined.enable, combined.allow_nonloopback, combined.dry_run,
            combined.yes_i_mean_it) == (True, True, True, True)


def test_the_two_directions_cannot_be_asked_for_at_once():
    for argv in (["--enable", "--disable"], ["--status", "--enable"],
                 ["--allow-nonloopback", "--deny-nonloopback"]):
        with pytest.raises(SystemExit) as caught:
            parse(argv)
        assert caught.value.code == 2


# --- reading ---------------------------------------------------------------------

def test_status_names_both_rows_the_bind_and_the_token_counts(store, config, capsys):
    assert run(["--status"], store=store, config=config) == EXIT_OK

    out = capsys.readouterr().out
    assert MCP_ENABLED_SETTING in out and MCP_NONLOOPBACK_SETTING in out
    assert "127.0.0.1:8765" in out
    assert "0 active, 0 revoked" in out
    assert "mcp-token --list" in out
    assert gated_rows(store) == {MCP_ENABLED_SETTING: None, MCP_NONLOOPBACK_SETTING: None}


def test_bare_form_is_status(store, config, capsys):
    assert run([], store=store, config=config) == EXIT_OK
    bare = capsys.readouterr().out

    assert run(["--status"], store=store, config=config) == EXIT_OK

    # One answer, printed one way: a reader who types the bare form must not get a shorter, and
    # therefore differently-worded, report of the same two rows.
    assert capsys.readouterr().out == bare
    assert MCP_ENABLED_SETTING in bare and MCP_NONLOOPBACK_SETTING in bare


def test_status_of_an_enabled_listener_asks_for_the_restart(store, config, capsys):
    """These rows are read when `serve` starts, so the operator has to be told to restart."""
    store.set_setting(MCP_ENABLED_SETTING, "1")

    assert run(["--status"], store=store, config=config) == EXIT_OK

    out = capsys.readouterr().out
    assert "would start" in out and "127.0.0.1:8765" in out
    assert "restart" in out


def test_status_reports_a_refused_bind_rather_than_inventing_one(store, config, capsys):
    store.set_setting(MCP_ENABLED_SETTING, "1")
    config.mcp_host = ROUTABLE_HOST      # routable, and the permission row is absent

    assert run(["--status"], store=store, config=config) == EXIT_OK

    out = capsys.readouterr().out
    assert ROUTABLE_HOST in out and "refus" in out
    assert MCP_NONLOOPBACK_SETTING in out


def test_status_never_migrates_an_unmigrated_store(tmp_path, capsys):
    """A read of two settings rows is not a reason to perform a schema change (as with `events`)."""
    unprepared = Store(tmp_path / "state.db")
    try:
        code = run_mcp(SimpleNamespace(data_dir=tmp_path, mcp_host="127.0.0.1", mcp_port=8765,
                                       mcp_url=PUBLIC_URL),
                       parse(["--status"]), store=unprepared)

        assert code == EXIT_REFUSED
        assert "check" in capsys.readouterr().err
        assert unprepared.table_columns("settings") == []
    finally:
        unprepared.close()


def test_status_refuses_a_missing_database_before_creating_it(tmp_path, capsys):
    """`Store()` creates the file, so the missing-database answer has to come first."""
    missing = tmp_path / "data"

    assert run(["--status"], config=SimpleNamespace(data_dir=missing, mcp_host="127.0.0.1",
                                                    mcp_port=8765, mcp_url=PUBLIC_URL)) == \
        EXIT_REFUSED

    assert not (missing / "state.db").exists()
    assert "check" in capsys.readouterr().err


# --- turning the listener on -------------------------------------------------------

def test_enable_writes_the_row_and_names_the_transition(store, config, capsys):
    assert run(["--enable"], store=store, config=config) == EXIT_OK

    assert store.settings_flag(MCP_ENABLED_SETTING) is True
    out = capsys.readouterr().out
    assert f"{MCP_ENABLED_SETTING}: unset → 1" in out
    assert "restart" in out


def test_enable_names_the_value_it_replaced_when_the_row_said_off(store, config, capsys):
    """`off` was put there by someone, and `0 → 1` is the line that says so."""
    store.set_setting(MCP_ENABLED_SETTING, "off")

    assert run(["--enable"], store=store, config=config) == EXIT_OK

    assert f"{MCP_ENABLED_SETTING}: off → 1" in capsys.readouterr().out


def test_enable_is_a_no_op_that_says_so_when_the_row_is_already_on(store, config, capsys):
    store.set_setting(MCP_ENABLED_SETTING, "on")

    assert run(["--enable"], store=store, config=config) == EXIT_OK

    out = capsys.readouterr().out
    assert "already" in out and "nothing" in out
    # `on` was the operator's spelling; a command that rewrote it to `1` would be editing a row it
    # was asked to leave alone.
    assert store.get_setting(MCP_ENABLED_SETTING) == "on"


def test_dry_run_writes_nothing_and_qualifies_the_migration(store, config, capsys):
    assert run(["--enable", "--dry-run"], store=store, config=config) == EXIT_OK

    assert gated_rows(store) == {MCP_ENABLED_SETTING: None, MCP_NONLOOPBACK_SETTING: None}
    out = capsys.readouterr().out
    assert f"{MCP_ENABLED_SETTING}: unset → 1" in out
    assert "dry run" in out and "migrat" in out


def test_dry_run_still_shows_the_consequence_it_is_rehearsing(store, config, capsys):
    assert run(["--allow-nonloopback", "--dry-run"], store=store, config=config) == EXIT_OK

    out = capsys.readouterr().out
    assert "loopback" in out and "token" in out
    assert gated_rows(store) == {MCP_ENABLED_SETTING: None, MCP_NONLOOPBACK_SETTING: None}


# --- opening the interface ---------------------------------------------------------

def test_allow_nonloopback_without_the_confirmation_writes_nothing(store, config, capsys):
    assert run(["--allow-nonloopback"], store=store, config=config) == EXIT_INCOMPLETE

    assert gated_rows(store) == {MCP_ENABLED_SETTING: None, MCP_NONLOOPBACK_SETTING: None}
    out = capsys.readouterr().out
    assert "--yes-i-mean-it" in out and "loopback" in out


def test_allow_nonloopback_names_the_consequence_and_the_host_it_applies_to(store, config,
                                                                            capsys):
    assert run(["--allow-nonloopback", "--yes-i-mean-it"], store=store,
               config=config) == EXIT_OK

    assert store.settings_flag(MCP_NONLOOPBACK_SETTING) is True
    out = capsys.readouterr().out
    assert f"{MCP_NONLOOPBACK_SETTING}: unset → 1" in out
    # The row grants permission; `PAPRA_MCP_HOST` still decides where the listener sits, and a
    # reader who is told only "you are now exposed" will go and set the wrong thing.
    assert "PAPRA_MCP_HOST" in out and "127.0.0.1" in out


def test_enable_is_refused_when_the_bind_would_be_refused(store, config, capsys):
    config.mcp_host = ROUTABLE_HOST

    assert run(["--enable"], store=store, config=config) == EXIT_REFUSED

    assert gated_rows(store) == {MCP_ENABLED_SETTING: None, MCP_NONLOOPBACK_SETTING: None}
    err = capsys.readouterr().err
    assert ROUTABLE_HOST in err and MCP_NONLOOPBACK_SETTING in err


def test_enable_is_refused_when_the_public_url_would_not_start_the_listener(store, config,
                                                                           capsys):
    config.mcp_url = ""

    assert run(["--enable"], store=store, config=config) == EXIT_REFUSED

    assert gated_rows(store) == {MCP_ENABLED_SETTING: None, MCP_NONLOOPBACK_SETTING: None}
    assert "PAPRA_MCP_URL" in capsys.readouterr().err


def test_permission_and_enable_in_one_command_is_allowed(store, config, capsys):
    """The order the operator means: open the interface, then start the listener on it."""
    config.mcp_host = ROUTABLE_HOST

    assert run(["--allow-nonloopback", "--enable", "--yes-i-mean-it"], store=store,
               config=config) == EXIT_OK

    assert store.settings_flag(MCP_NONLOOPBACK_SETTING) is True
    assert store.settings_flag(MCP_ENABLED_SETTING) is True
    out = capsys.readouterr().out
    assert f"{MCP_NONLOOPBACK_SETTING}: unset → 1" in out
    assert f"{MCP_ENABLED_SETTING}: unset → 1" in out


def test_enable_with_a_deny_in_the_same_command_is_refused_before_anything_is_written(
        store, config, capsys):
    """The end state is what is validated: enabled + routable host + no permission cannot start."""
    store.set_setting(MCP_NONLOOPBACK_SETTING, "1")
    config.mcp_host = ROUTABLE_HOST

    assert run(["--enable", "--deny-nonloopback", "--yes-i-mean-it"], store=store,
               config=config) == EXIT_REFUSED

    assert gated_rows(store) == {MCP_ENABLED_SETTING: None, MCP_NONLOOPBACK_SETTING: "1"}


def test_denying_permission_is_never_refused_but_says_serve_will_now_refuse(store, config,
                                                                            capsys):
    """Tightening is safe even when it strands the deployment, and it must not be gated."""
    store.set_setting(MCP_ENABLED_SETTING, "1")
    store.set_setting(MCP_NONLOOPBACK_SETTING, "1")
    config.mcp_host = ROUTABLE_HOST

    assert run(["--deny-nonloopback"], store=store, config=config) == EXIT_OK

    assert store.get_setting(MCP_NONLOOPBACK_SETTING) is None
    out = capsys.readouterr().out
    assert f"{MCP_NONLOOPBACK_SETTING}: 1 → unset (effective 0)" in out
    assert ROUTABLE_HOST in out and "refus" in out


# --- turning things off, and the fence on it ----------------------------------------

def test_disable_clears_the_row_and_says_unset_effective_zero(store, config, capsys):
    store.set_setting(MCP_ENABLED_SETTING, "1")

    assert run(["--disable"], store=store, config=config) == EXIT_OK

    assert store.get_setting(MCP_ENABLED_SETTING) is None
    assert f"{MCP_ENABLED_SETTING}: 1 → unset (effective 0)" in capsys.readouterr().out


def test_disable_of_an_absent_row_says_there_is_nothing_to_disable(store, config, capsys):
    assert run(["--disable"], store=store, config=config) == EXIT_OK

    assert "nothing" in capsys.readouterr().out
    assert gated_rows(store) == {MCP_ENABLED_SETTING: None, MCP_NONLOOPBACK_SETTING: None}


def test_disable_reports_a_row_that_moved_under_it(store, config, capsys):
    """The clear is a compare-and-swap: a row that changed since it was printed is not this row."""
    store.set_setting(MCP_ENABLED_SETTING, "1")
    monkeypatched = False

    def refusing_clear(key, *, expected):
        nonlocal monkeypatched
        monkeypatched = True
        return False

    store.clear_setting = refusing_clear

    assert run(["--disable"], store=store, config=config) == EXIT_INCOMPLETE

    assert monkeypatched and "again" in capsys.readouterr().err


def test_a_failed_clear_stops_the_rest_of_the_plan(store, config, capsys):
    """Half a plan applied and reported as a whole one is the reading nobody can recover from."""
    store.set_setting(MCP_ENABLED_SETTING, "1")
    store.set_setting(MCP_NONLOOPBACK_SETTING, "1")
    store.clear_setting = lambda key, *, expected: False

    assert run(["--disable", "--deny-nonloopback"], store=store, config=config) == EXIT_INCOMPLETE

    assert store.get_setting(MCP_ENABLED_SETTING) == "1"
    assert store.get_setting(MCP_NONLOOPBACK_SETTING) == "1"


# --- the shape of the failure paths ---------------------------------------------------

def test_the_command_prints_no_token_and_no_digest(store, config, capsys, caplog):
    token = mint_token(store, label="hermes", org_ids=["org-1"])
    digest = hashlib.sha256(token.encode()).hexdigest()
    assert resolve_token(store, token) is not None            # the row is really there
    assert f"mcp_token:{digest}" in store.list_setting_keys(prefix="mcp_token:")

    with caplog.at_level(logging.DEBUG):
        assert run(["--status"], store=store, config=config) == EXIT_OK

    printed = capsys.readouterr().out + capsys.readouterr().err
    assert token not in printed + caplog.text and digest not in printed + caplog.text
    # The settings key is that digest behind a prefix, so a line that echoed a key would echo a
    # third of a credential's input space. Checked on what the command printed: the logger's own
    # name contains `mcp_token`, and a guard that fired on that trains people to delete it.
    assert TOKEN_SETTING_PREFIX not in printed
    assert "1 active" in printed


def test_an_interruption_is_a_sentence_and_a_nonzero_exit(store, config, capsys):
    def interrupted():
        raise KeyboardInterrupt

    store.migrate = interrupted

    assert run(["--enable"], store=store, config=config) == EXIT_INCOMPLETE

    err = capsys.readouterr().err
    assert "interrupted" in err and "Traceback" not in err


def test_a_store_that_will_not_answer_is_a_sentence(store, config, capsys):
    def locked(key, value):
        raise sqlite3.OperationalError("database is locked")

    store.set_setting = locked

    assert run(["--enable"], store=store, config=config) == EXIT_REFUSED

    err = capsys.readouterr().err
    assert "locked" in err and "Traceback" not in err


def test_a_close_that_fails_cannot_replace_the_verdict(store, config, capsys, caplog, monkeypatch):
    """The write is committed and printed before the store is shut; a stuck close is a log line.

    A `finally` that raises discards the value the `try` was returning, which is how a command that
    did its job reports failure — the one reading an operator cannot recover from.
    """

    class StubbornStore(Store):
        def close(self):
            raise sqlite3.OperationalError("cannot close, checkpoint busy")

    monkeypatch.setattr("papra_suite.mcp_cli.Store", StubbornStore)

    with caplog.at_level(logging.WARNING):
        assert run_mcp(config, parse(["--enable"])) == EXIT_OK

    assert "unset → 1" in capsys.readouterr().out
    assert store.get_setting(MCP_ENABLED_SETTING) == "1"
    assert "close" in caplog.text


# --- the entry point ---------------------------------------------------------------

def test_the_cli_is_wired_into_the_entry_point(tmp_path, monkeypatch):
    """`main()` reaches the module attribute, so the dispatch is testable rather than assumed."""
    from papra_suite import mcp_cli
    from papra_suite.__main__ import main

    monkeypatch.setattr(mcp_cli, "run_mcp", lambda config, args: 7)
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "mcp"])

    assert main() == 7


def test_the_entry_point_turns_the_listener_on_end_to_end(tmp_path, monkeypatch, capsys):
    """Parser, config, store from `DATA_DIR`, one printed transition, one committed row."""
    from papra_suite.__main__ import main

    seed = Store(tmp_path / "state.db")
    seed.migrate()
    seed.close()
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PAPRA_MCP_URL", PUBLIC_URL)
    monkeypatch.setattr(sys, "argv", ["papra-suite", "mcp", "--enable"])

    assert main() == EXIT_OK

    assert f"{MCP_ENABLED_SETTING}: unset → 1" in capsys.readouterr().out
    reading = Store(tmp_path / "state.db")
    try:
        assert reading.get_setting(MCP_ENABLED_SETTING) == "1"
    finally:
        reading.close()


def test_the_entry_point_refuses_to_enable_without_a_public_url(tmp_path, monkeypatch, capsys):
    """The environment that produces a listener which cannot start is refused by the command."""
    from papra_suite.__main__ import main

    seed = Store(tmp_path / "state.db")
    seed.migrate()
    seed.close()
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "mcp", "--enable"])

    assert main() == EXIT_REFUSED

    assert "PAPRA_MCP_URL" in capsys.readouterr().err
    reading = Store(tmp_path / "state.db")
    try:
        assert reading.get_setting(MCP_ENABLED_SETTING) is None
    finally:
        reading.close()


#: Every invocation the README prints for this command.
CITED_INVOCATIONS = [
    ["mcp"], ["mcp", "--status"], ["mcp", "--enable"], ["mcp", "--disable"],
    ["mcp", "--enable", "--dry-run"],
    ["mcp", "--allow-nonloopback", "--yes-i-mean-it"],
    ["mcp", "--deny-nonloopback"],
]


@pytest.mark.parametrize("argv", CITED_INVOCATIONS, ids=" ".join)
def test_the_shipped_entry_point_accepts_every_invocation_the_runbook_prints(argv):
    """A runbook line that stops with `unrecognized arguments` is the defect Task 7 pinned."""
    import subprocess
    from pathlib import Path

    import papra_suite as package

    root = Path(package.__file__).resolve().parents[1]
    proc = subprocess.run([sys.executable, "-m", "papra_suite", *argv, "--help"],
                          cwd=root, capture_output=True, text=True, timeout=120)

    assert proc.returncode == 0, f"{' '.join(argv)}: {proc.stderr}"


def test_the_runbook_names_the_command_and_hands_over_no_settings_row():
    """The guard on the runbook lives in `test_shipped_citations.py`; this one lives here.

    Deliberately a second site: the mutation that makes the citation guard accept any number of raw
    invocations edits that file, and an invariant restated somewhere it cannot reach still fails. It
    is the same rule with the same evidence, not a new one — the MCP section must name the command
    and must not hand over a `settings` row to type by hand.
    """
    from tests.test_shipped_citations import (
        ACCEPTED_RAW_WRITES,
        README,
        mcp_section,
        raw_invocations,
    )

    text = README.read_text(encoding="utf-8")
    offenders = raw_invocations(text)
    assert len(offenders) == 1, f"raw SQL handed to an operator: {offenders}"
    number, _line = offenders[0]
    body = " ".join(text.splitlines()[number - 1:number + 2])
    assert any(write in body for write in ACCEPTED_RAW_WRITES), body

    section = mcp_section(text)
    assert "papra-suite mcp --enable" in section
    assert "papra-suite mcp --allow-nonloopback" in section
    assert not raw_invocations(section)
