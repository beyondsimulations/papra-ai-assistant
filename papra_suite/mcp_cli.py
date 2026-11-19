"""`papra-suite mcp` — the two settings rows the listener is gated on, set by command.

This module exists because of a sentence in the runbook. The MCP review round flagged the gap as a
Minor with a prediction — *"two settings rows gate the listener, and no command sets either… the
operator will reach for `PAPRA_MCP_HOST` and get the refusal"* — and the prediction arrived as
documentation: the README ended up printing two `INSERT OR REPLACE INTO settings` statements for an
operator to type, beside a paragraph conceding that raw SQL "is not a thing to hand an operator at
2am". `tests/test_shipped_citations.py` pins the runbook to exactly one raw invocation, and it was
right: the guard was not the thing in the way.

What a command can do that a typed `INSERT` cannot:

**Name the transition.** `mcp_server_enabled: off → 1` says what changed, against the row that was
actually there. A statement that overwrote a row somebody had set to `off` on purpose reported
nothing at all.

**Refuse a listener that cannot start.** Enabling is refused when the state it would produce is one
`serve` will not start under — a routable `PAPRA_MCP_HOST` without the non-loopback permission
(`resolve_bind`), or no usable `PAPRA_MCP_URL`. Task 12's availability round was about `check`
asserting a listener that could not exist; writing the row that produces one is that defect with a
signature on it.

**Make exposure a deliberate act.** `--allow-nonloopback` moves a bearer token off a private tailnet
behind TLS to whatever the chosen interface can reach, so it states the consequence and asks for
`--yes-i-mean-it`, the discipline `ledger --reindex-all` uses.

**Read it back.** `--status` prints the two rows, the bind they imply, the public URL's verdict and
the token *counts* — never a token and never a token digest, because this is the output an operator
pastes into an issue.

Two things this command is not. It does not talk to a running `serve`: both rows are read when the
listener starts, so a change lands on the next start and the output says so. It owns no credential
— minting, listing and revoking stay in `mcp-token`, which is where the rule that a plaintext is
printed exactly once lives.
"""

from __future__ import annotations

import logging
import sqlite3
import sys

from papra_suite.mcp_server import (
    LOOPBACK_HOSTS,
    MCP_ENABLED_SETTING,
    MCP_NONLOOPBACK_SETTING,
    McpConfigError,
    _public_url,
    resolve_bind,
)
from papra_suite.mcp_tokens import token_counts
from papra_suite.store import Store

logger = logging.getLogger("papra_suite.mcp_cli")

#: Exit codes, with the meaning `check`, `ledger` and `mcp-token` already give them: `0` the command
#: did what was asked, `1` it stopped partway or refused an unconfirmed act, `2` it would not start
#: at all — which is also exactly what a refused enable means.
EXIT_OK = 0
EXIT_INCOMPLETE = 1
EXIT_REFUSED = 2

#: The spelling written, because it is the one `TRUTHY_SETTING_VALUES` and the runbook agree on.
SETTING_ON = "1"

#: What a cleared row reads as afterwards. The row is *gone* and the zero is `settings_flag`'s
#: reading of an absent key, so the line says both: claiming a `0` had been written would describe a
#: row this command did not create.
EFFECTIVE_OFF = "unset (effective 0)"

# The rows this command owns, with what each one being off means. Ordered the way the reader asks:
# does it listen at all, then does it leave loopback.
GATED_SETTINGS = ((MCP_ENABLED_SETTING, "serve starts no listener"),
                  (MCP_NONLOOPBACK_SETTING, "only loopback may be bound"))


def add_subparser(subparsers) -> None:
    """Register `mcp` on the entry point's subparser table."""
    parser = subparsers.add_parser(
        "mcp",
        help="read or set the two settings rows that gate the MCP listener "
             "(with no flag, prints what is configured)",
    )
    listener = parser.add_mutually_exclusive_group()
    listener.add_argument("--status", action="store_true",
                          help="print both rows, the bind they imply, PAPRA_MCP_URL's verdict and "
                               "the token counts; changes no setting and migrates nothing")
    listener.add_argument("--enable", action="store_true",
                          help=f"set {MCP_ENABLED_SETTING}, so `serve` starts the listener. "
                               "Refused if the resulting bind or public URL would not start")
    listener.add_argument("--disable", action="store_true",
                          help=f"clear {MCP_ENABLED_SETTING}, so nothing answers the network. "
                               "Never refused: switching a listener off is the safe direction")
    interface = parser.add_mutually_exclusive_group()
    interface.add_argument("--allow-nonloopback", dest="allow_nonloopback", action="store_true",
                           help=f"set {MCP_NONLOOPBACK_SETTING}, which permits a non-loopback "
                                "bind. Needs --yes-i-mean-it: a bearer token on a reachable "
                                "interface is a different security story")
    interface.add_argument("--deny-nonloopback", dest="deny_nonloopback", action="store_true",
                           help=f"clear {MCP_NONLOOPBACK_SETTING}, so only loopback may be bound "
                                "again")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would change and write no settings row")
    parser.add_argument("--yes-i-mean-it", dest="yes_i_mean_it", action="store_true",
                        help="confirm a change that lets the listener leave loopback")


def run_mcp(config, args, *, store: Store | None = None) -> int:
    """Do what was asked of the two rows. Returns the process exit code.

    `store` is injectable for the same reason `run_ledger` and `run_mcp_token` take theirs: the
    behaviour that matters here is what a settings command does against a store that has moved, is
    unmigrated, or will not answer, and that is not something to stage by hand.

    Reading (`--status`, and the bare form) neither migrates nor changes a setting, following
    `events` and `mcp-token --list`: a command an operator runs to look must not perform a schema
    change. It does open the database, so it is not a promise of no writes at the filesystem level —
    SQLite's own pragmas are its own business — which is why the refusal of an unmigrated store
    names `check` rather than reporting two absent rows. Writing forms migrate first, including
    under `--dry-run`: what a rehearsal promises is that no settings row changes, not that no
    statement is ever executed (Task 11's rule, and the reason the dry-run line says which of the
    two it did).
    """
    # A name of its own, so the refusals that happen before a store is opened cannot leave the
    # `finally` closing something that was never opened.
    opened: Store | None = None
    try:
        ops = _plan(args)
        reading = not ops
        if store is None:
            db_path = config.data_dir / "state.db"
            if reading and not db_path.exists():
                # Before `Store()`: opening the file would replace the missing database this
                # command came to report with an empty one, and the next run would say "unset".
                print(f"no database at {db_path}", file=sys.stderr)
                print("run `papra-suite check` first; there is nothing to report",
                      file=sys.stderr)
                return EXIT_REFUSED
            config.data_dir.mkdir(parents=True, exist_ok=True)
            opened = Store(db_path)
            store = opened
        if reading:
            if not store.table_columns("settings"):
                print("this store has no settings table yet", file=sys.stderr)
                print("run `papra-suite check` to migrate it; nothing to report",
                      file=sys.stderr)
                return EXIT_REFUSED
            return _status(store, config)
        store.migrate()
        return _apply(store, config, args, ops)
    except (ValueError, sqlite3.Error) as error:
        # `McpConfigError` is a `ValueError`, so a config this listener will not start under lands
        # here too when it escapes the checks below — and it must land as a sentence. A container
        # that dies with a stack trace gets filed as a crash; one that prints the refusal gets
        # acted on.
        print(f"mcp refused: {error}", file=sys.stderr)
        return EXIT_REFUSED
    except KeyboardInterrupt:
        print("mcp stopped: interrupted. Each row is one statement, so either the change printed "
              "above happened or it did not — `papra-suite mcp --status` reads them back",
              file=sys.stderr)
        return EXIT_INCOMPLETE
    finally:
        if opened is not None:
            _close_quietly(opened)


def _close_quietly(store: Store) -> None:
    """Close the store this command opened, without letting the close decide the verdict.

    An exception raised from a `finally` discards the value the `try` was returning, so a WAL
    checkpoint that would not complete could turn a settings row that really was written into a
    traceback. `ledger_cli` learned this the hard way with its clients; the same rule applies here
    in its milder form: the write is committed and printed before this runs, so a stuck close is a
    WARNING about a socket the process is leaving, not a verdict on work that already happened.
    """
    try:
        store.close()
    except sqlite3.Error as error:
        logger.warning("mcp: the store would not close cleanly (%s: %s); the settings rows are "
                       "committed either way", type(error).__name__, error)


def _plan(args) -> list[tuple[str, str]]:
    """The requested writes, in the order they have to happen. Empty means "just read".

    The order is not cosmetic. Opening the interface comes **before** enabling, so the enable's bind
    check judges the state the operator asked for rather than the one that happened to be there; and
    disabling comes **before** taking the permission away, so a command that stops halfway can never
    leave a listener enabled against a bind it cannot hold.

    These are two settings operations, not one transaction: the confirmation gate and the
    pre-flight refusal both guarantee that nothing was written, but an interruption or a
    compare-and-swap failure between the two can leave the first one applied. That is what the
    printed transitions are for, and a failed clear stops the plan where it stands.
    """
    ops: list[tuple[str, str]] = []
    if getattr(args, "allow_nonloopback", False):
        ops.append(("set", MCP_NONLOOPBACK_SETTING))
    if getattr(args, "enable", False):
        ops.append(("set", MCP_ENABLED_SETTING))
    if getattr(args, "disable", False):
        ops.append(("clear", MCP_ENABLED_SETTING))
    if getattr(args, "deny_nonloopback", False):
        ops.append(("clear", MCP_NONLOOPBACK_SETTING))
    return ops


class _ProspectiveStore:
    """The store as it will be once the requested change lands.

    `resolve_bind` asks a store exactly one question — is `mcp_bind_nonloopback` truthy — so
    rehearsing that answer needs a stand-in that answers one question, not a copy of the database.
    Everything else delegates, which is the part worth having: if `resolve_bind` grows a second
    question, this reads it from the rows that are actually there instead of from a guess.
    """

    def __init__(self, store: Store, *, nonloopback: bool):
        self._store = store
        self._nonloopback = nonloopback

    def settings_flag(self, name: str) -> bool:
        if name == MCP_NONLOOPBACK_SETTING:
            return self._nonloopback
        return self._store.settings_flag(name)

    def get_setting(self, name: str) -> str | None:
        return self._store.get_setting(name)


def _startup_refusals(config, store) -> list[str]:
    """Every reason `serve` would refuse to start this listener, in the order it meets them.

    Both checks are the shipped ones — `resolve_bind` and the URL parser behind `Host`/`Origin` — so
    this command cannot hold a listener to a different standard than the process that runs it.
    Asking both at once, rather than the first one, is deliberate: an operator who has to run the
    command twice to discover the second problem is an operator who has written one bad row.
    """
    refusals = []
    try:
        resolve_bind(config, store=store)
    except McpConfigError as error:
        refusals.append(str(error))
    try:
        _public_url(getattr(config, "mcp_url", "") or "")
    except McpConfigError as error:
        refusals.append(str(error))
    return refusals


def _render(value: str | None) -> str:
    """A settings row as the operator reads it: `unset` and the empty string are the same answer."""
    return "unset" if value in (None, "") else value


def _intended(store, args, ops) -> tuple[bool, bool]:
    """`(listener, permission)` as they will read once the plan has been applied."""
    def after(key: str, setter: str, clearer: str, current: bool) -> bool:
        if (setter, key) in ops:
            return True
        if (clearer, key) in ops:
            return False
        return current

    return (after(MCP_ENABLED_SETTING, "set", "clear", store.settings_flag(MCP_ENABLED_SETTING)),
            after(MCP_NONLOOPBACK_SETTING, "set", "clear",
                  store.settings_flag(MCP_NONLOOPBACK_SETTING)))


def _nonloopback_consequence(config) -> str:
    """What is being accepted, said once, before anything asks for confirmation.

    Two things the wording has to get right. The row grants *permission*; `PAPRA_MCP_HOST` still
    decides where the listener sits, and an operator who reads only "you are about to be reachable"
    will go and set the wrong thing — so the current host is printed beside it. And the exposure is
    the bearer token's, not the process's: on loopback it is presented to something already on this
    machine, and off it it is presented to whatever can route to the port.
    """
    host = getattr(config, "mcp_host", "") or "127.0.0.1"
    return (f"{MCP_NONLOOPBACK_SETTING} permits a non-loopback bind: the bearer token then travels "
            f"to whatever can reach the bound interface, not only to something already on this "
            f"machine, and a token copied off a reachable port is a copy of every bucket "
            f"that token can read. This command does not change PAPRA_MCP_HOST, which is "
            f"{host!r} now — the row opens the door, that variable says where the listener "
            f"sits.")


def _apply(store: Store, config, args, ops) -> int:
    """Confirm, refuse, write. In that order, because each step is dearer than the one before it.

    Nothing is written before the whole requested end state has been checked: a run that would leave
    `mcp_server_enabled` next to a bind `resolve_bind` refuses is refused as a run, not applied as
    two settings operations. The refusal is also why `--enable` on an already-enabled but
    unstartable pair of rows exits `2` rather than reporting a tidy no-op — the operator asked for a
    listener that works, and this configuration does not have one.
    """
    grants = ("set", MCP_NONLOOPBACK_SETTING) in ops
    if grants:
        print(_nonloopback_consequence(config))

    enabled, nonloopback = _intended(store, args, ops)
    if enabled and any(action == "set" for action, _key in ops):
        refusals = _startup_refusals(config, _ProspectiveStore(store, nonloopback=nonloopback))
        if refusals:
            print("mcp refused: the listener would be enabled, and this is not a configuration "
                  "`serve` starts", file=sys.stderr)
            for line in refusals:
                print(f"  {line}", file=sys.stderr)
            print("nothing was written", file=sys.stderr)
            return EXIT_REFUSED

    if args.dry_run:
        for action, key in ops:
            previous = store.get_setting(key)
            if action == "set":
                print(f"dry run: {key}: {_render(previous)} → {SETTING_ON}")
            elif previous is None:
                print(f"dry run: {key}: unset, so there is nothing to clear")
            else:
                print(f"dry run: {key}: {previous} → {EFFECTIVE_OFF}")
        print("dry run: no settings row was written. This command did open the store and run its "
              "migrations — a command that writes has to migrate, and a rehearsal that promised it "
              "had touched nothing at all would be a rehearsal of a different command")
        return EXIT_OK

    if grants and not args.yes_i_mean_it:
        print("not applied: this is the row that lets a bearer token leave loopback. Run the same "
              "command with --yes-i-mean-it to say you mean the interface as well as the flag")
        return EXIT_INCOMPLETE

    changed, incomplete = _execute(store, ops)
    if changed:
        _print_footer(store)
        _print_stranded_note(store, config)
    return EXIT_INCOMPLETE if incomplete else EXIT_OK


def _execute(store: Store, ops) -> tuple[bool, bool]:
    """Apply the plan, naming every transition. Returns `(changed, incomplete)`.

    The `set` direction reads the row again rather than trusting the value printed a moment earlier:
    a row that `serve` or another terminal changed in between is not the row this command described,
    and overwriting it silently is how `off` becomes `1` with no witness. There is no fenced write
    for this: `Store.set_setting` means "make it so" and has no condition — so the worst that can
    happen is that the printed `from` is one glance stale, and the re-read is what keeps even that
    rare.

    The `clear` direction is the opposite act, and the store already offers the fence for it:
    `clear_setting(key, expected=...)` deletes the row only if it still holds what was read. A False
    there means the value moved, and the command stops rather than carrying on to a second write on
    the strength of a stale glance.
    """
    changed = False
    for action, key in ops:
        previous = store.get_setting(key)
        if action == "set":
            if store.settings_flag(key):
                print(f"{key}: already {_render(previous)}, so nothing was written")
                continue
            store.set_setting(key, SETTING_ON)
            print(f"{key}: {_render(previous)} → {SETTING_ON}")
            changed = True
            continue
        if previous is None:
            print(f"{key}: unset already, so there is nothing to clear")
            continue
        if not store.clear_setting(key, expected=previous):
            print(f"nothing further changed: {key} is no longer {_render(previous)}, so the value "
                  "printed above is not the one that was there. Read it again with "
                  "`papra-suite mcp --status` before deciding", file=sys.stderr)
            return changed, True
        print(f"{key}: {previous} → {EFFECTIVE_OFF}")
        changed = True
    if not changed:
        print("nothing changed: the rows already say what this command was asked to say")
    return changed, False


def _print_stranded_note(store: Store, config) -> None:
    """Say so, out loud, when the rows now name a listener `serve` will not start.

    Reached by tightening, and only by it: a plan that removes a permission is never refused,
    because refusing it would leave an exposed configuration in place on the strength of the outage
    that follows. The outage is the guard working — `resolve_bind` will not put a bearer token on a
    reachable interface — so the note names the refused address and the row that ends it, rather
    than handing the operator a `serve` that stops at startup with no explanation.
    """
    if not store.settings_flag(MCP_ENABLED_SETTING):
        return
    refusals = _startup_refusals(config, store)
    if not refusals:
        return
    print("note: the listener is still enabled, and this is now a configuration `serve` refuses to "
          "start: " + " | ".join(refusals))
    print("that refusal is the guard rather than a fault. Set PAPRA_MCP_HOST back to loopback, or "
          "run `papra-suite mcp --disable` if the listener should not run at all")


def _print_footer(store: Store) -> None:
    """What happens next, because a settings row is not a running listener.

    Both rows are read when `serve` starts the listener, so the change lands on the next start and
    nothing else; and a listener enabled with no token is a port that answers 401 to everything,
    which reads as a broken server rather than as an empty credential table.
    """
    print("These rows are read when `serve` starts: a running `serve` keeps the listener it "
          "already has, so restart it to apply this. `papra-suite check` prints the `mcp:` line, "
          "and `papra-suite mcp --status` reads the rows back.")
    if store.settings_flag(MCP_ENABLED_SETTING):
        active, _revoked = token_counts(store)
        if not active:
            print("note: no active token exists, so the listener will answer 401 to every request "
                  "until `papra-suite mcp-token --create --label NAME --org-id ID` mints one "
                  "(the plaintext is printed once, and this command never prints one)")


def _status(store: Store, config) -> int:
    """What is configured, and what `serve` would do with it. Changes no setting, migrates nothing.

    The two rows are printed with the store's own reading beside them, because "unset" and "set to
    `off`" and "set to `maybe`" are three states that all mean 0 and only the operator can tell
    whether one of them was a mistake. The bind and the URL are printed as the listener's own
    functions judge them — including the refusal, verbatim — so this cannot report a listener that
    would not start. Token *counts* only: a token and its digest are the same secret from two sides,
    and the names are one command away in `mcp-token --list`, which is run at the operator's own
    terminal.
    """
    for key, meaning in GATED_SETTINGS:
        value = store.get_setting(key)
        line = f"{key}: {_render(value)}"
        if not store.settings_flag(key):
            line += f"  (effective 0 — {meaning})"
        print(line)

    try:
        bind = resolve_bind(config, store=store)
        print(f"bind: {bind} ({'loopback' if bind.host in LOOPBACK_HOSTS else 'NOT loopback'})")
    except McpConfigError as error:
        bind = None
        print(f"bind: refused — {error}")

    try:
        scheme, host, port = _public_url(getattr(config, "mcp_url", "") or "")
        shown = f"{scheme}://{host}" + (f":{port}" if port else "")
        print(f"public url: {shown} (PAPRA_MCP_URL)")
    except McpConfigError as error:
        print(f"public url: refused — {error}")

    active, revoked = token_counts(store)
    print(f"tokens: {active} active, {revoked} revoked — names: `papra-suite mcp-token --list`")

    if not store.settings_flag(MCP_ENABLED_SETTING):
        print("listener: off. `papra-suite mcp --enable` turns it on for the next `serve`")
        return EXIT_OK
    refusals = _startup_refusals(config, store)
    if refusals:
        print("listener: enabled, and `serve` would not start it: " + " | ".join(refusals))
        return EXIT_OK
    print(f"listener: enabled — `serve` would start it on {bind}. A running `serve` keeps the "
          "listener it already bound, so a change to these rows needs a restart")
    return EXIT_OK
