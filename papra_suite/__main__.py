"""The `papra-suite` entry point.

Commands are deliberately boring: `check` proves the deployment can start,
`serve` runs it, `replay` rewinds a cursor, `events` answers "why did nothing
arrive?" from the durable record. Nothing here decides policy; the store and the
pipeline do, and this file turns arguments into calls and rows into text.
"""

import argparse
import contextlib
import json
import signal
import sys

from papra_suite import __version__
from papra_suite.config import ConfigError, load_config
from papra_suite.store import INGEST_EVENT_KINDS

# Signals that must stop the service cleanly. SIGTERM is what `docker stop`
# sends; SIGINT is Ctrl-C.
STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def _run_events(config, args) -> int:
    """`papra-suite events`: read the durable answer to "why did nothing arrive?"

    Read-only: it never migrates. A command that migrated on read would let a
    curiosity query perform a schema change, and the schema belongs to `check`
    and `serve`. An unmigrated store is therefore a stated condition, not a
    traceback and not a silent empty table.
    """
    import sqlite3
    from datetime import UTC, datetime, timedelta

    from papra_suite.store import INGEST_EVENT_RETENTION_DAYS, Store

    unknown = [kind for kind in (args.kind or []) if kind not in INGEST_EVENT_KINDS]
    if unknown:
        print(f"unknown kind: {', '.join(unknown)}",
              file=sys.stderr)
        print(f"known kinds: {', '.join(INGEST_EVENT_KINDS)}", file=sys.stderr)
        return 2
    if args.since is not None and args.since < 1:
        print("--since must be at least 1 day", file=sys.stderr)
        return 2

    store = Store(config.data_dir / "state.db")
    try:
        known = store.table_columns("ingest_events")
        if not known:
            print("no ingest_events table in this store yet",
                  file=sys.stderr)
            print("run `papra-suite check` to migrate it; nothing to list",
                  file=sys.stderr)
            return 2
        if args.since is not None:
            since = (datetime.now(UTC) - timedelta(days=args.since)).isoformat()
        else:
            since = None
        rows = store.list_ingest_events(
            mailbox_id=args.mailbox_id, kinds=args.kind, since=since,
            limit=args.limit,
        )
        columns = [name for name in EVENT_COLUMNS if name in known]
    except sqlite3.Error as error:
        print(f"could not read ingest events: {error}", file=sys.stderr)
        return 2
    finally:
        store.close()

    if not rows:
        # An empty answer still says what was asked, because "no rows" and "no
        # rows for mailbox mb_7 in the last 3 days" are different answers.
        print(f"no matching ingest events{_describe_event_scope(args)}")
        print(f"note: this table holds the last {INGEST_EVENT_RETENTION_DAYS} days, "
              "and a message that ingested successfully never appears in it")
        return 0

    print("\t".join(columns))
    for row in rows:
        print("\t".join(_render_event_value(getattr(row, name)) for name in columns))
    return 0


# Printed in this order; anything the store does not have is skipped rather than
# printed empty, so an older database read by a newer CLI still says what it
# actually knows instead of inventing a column of dashes.
EVENT_COLUMNS = (
    "ts", "mailbox_id", "uid", "uidvalidity", "kind", "papra_org_id",
    "sender", "filename", "detail",
)


def _render_event_value(value) -> str:
    """One field, one line, whatever it contains.

    Absent renders as `-`, never as an empty field: "no sender was recorded" and
    "a sender was recorded that renders as nothing" have to be distinguishable,
    and a zero-width field in a tab-separated row shifts every column after it.

    Text is quoted with JSON escaping when it contains whitespace, a quote or a
    backslash, and left bare otherwise. The reason to quote is that these values
    are bytes someone else sent — a MIME filename, a From: field — and a value
    carrying a tab or a newline would otherwise forge a column or a whole row.
    The store already turns control characters into spaces; this is the second
    half of the answer, and it is deliberately not the store's job, because rows
    written before that rule existed are still in old databases. Common values
    stay unquoted so `cut -f` and `grep` work on ordinary days.
    """
    if value is None:
        return "-"
    if not isinstance(value, str):
        return str(value)
    if value == "":
        return '""'
    if any(char.isspace() for char in value) or '"' in value or "\\" in value:
        # ensure_ascii=False: the escape-everything default would write a German
        # filename as "R\u00f6ntgen", which is inert but unreadable, and reading
        # it is the only job this line has. Non-ASCII stays as typed; the
        # characters that could forge a field or a row are still escaped.
        return json.dumps(value, ensure_ascii=False)
    return value


def _describe_event_scope(args) -> str:
    parts = []
    if args.mailbox_id:
        parts.append(f"for mailbox {args.mailbox_id}")
    if args.kind:
        parts.append("of kind " + ", ".join(args.kind))
    if args.since is not None:
        parts.append(f"in the last {args.since} day(s)")
    return (" " + " ".join(parts)) if parts else ""


async def run_serve(config, **kwargs) -> None:
    """Run serve until SIGTERM/SIGINT (or its own stop conditions) end it.

    WHY HANDLERS EXIST. serve already stops on an asyncio.Event, but with no
    handler SIGTERM's DEFAULT disposition is to terminate the process where it
    stands: serve's `finally` (cancel the poll loops and workers, close the
    clients, close the store, and above all the task teardowns for every
    poll loop's regex child) never runs. So a `docker stop` used to leave a
    busy child orphaned at ppid=1 — only the child's own self-deadline ended
    it, after it had burned a core for its whole budget. add_signal_handler is
    the only mechanism that can run loop code from a signal, so the handler
    does the one thing that matters: it sets serve's stop event, and shutdown
    is the same clean path the tests drive. The handlers are removed on the
    way out so a library use of this module does not keep the process's
    signals pointed at a dead loop.
    """
    import asyncio

    from papra_suite.pipeline import serve

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous: dict[int, object] = {}
    for sig in STOP_SIGNALS:
        try:
            previous[sig] = signal.getsignal(sig)
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            previous.pop(sig, None)   # no signal support here: stop still works
    try:
        await serve(config, stop=stop, **kwargs)
    finally:
        for sig, handler in previous.items():
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(sig)
                signal.signal(sig, handler)   # restore exactly what we replaced


def main() -> int:
    parser = argparse.ArgumentParser(prog="papra-suite")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="validate config, open and migrate the store, exit")
    sub.add_parser("serve", help="run the pipeline: pollers, OCR/title workers, sweep")
    replay = sub.add_parser(
        "replay",
        help="rewind a mailbox's poll cursor so refused or missed mail is read "
             "again (e.g. after correcting PAPRA_ALLOWED_SENDERS)",
    )
    replay.add_argument("--mailbox-id", required=True)
    replay.add_argument(
        "--from-uid", type=int, default=None,
        help="resume after this UID; the default re-reads the whole mailbox",
    )
    replay.add_argument("--dry-run", action="store_true",
                        help="report what would change and change nothing")

    events = sub.add_parser(
        "events",
        help="list recorded reasons why a message or attachment was NOT ingested",
    )
    events.add_argument("--limit", type=int, default=200,
                        help="most recent rows to show (clamped to 1..1000)")
    events.add_argument("--mailbox-id", default=None, help="only this mailbox")
    events.add_argument(
        "--kind", action="append", default=None,
        help="only this outcome; repeatable (" + ", ".join(INGEST_EVENT_KINDS) + ")",
    )
    events.add_argument("--since", type=int, default=None, metavar="DAYS",
                        help="only rows from the last DAYS days")

    args = parser.parse_args()

    try:
        config = load_config()
    except ConfigError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2

    if args.command == "check":
        from papra_suite.store import Store

        config.data_dir.mkdir(parents=True, exist_ok=True)
        store = Store(config.data_dir / "state.db")
        try:
            store.migrate()
        finally:
            store.close()
        print(f"config ok; store migrated; data dir: {config.data_dir}")
        # Printed last because it is the line that answers "why is nothing
        # arriving?": an empty allow-list denies every sender by design.
        print(f"senders: {config.allowed_senders.describe()}")
        return 0

    if args.command == "replay":
        from papra_suite.store import Store

        store = Store(config.data_dir / "state.db")
        try:
            mailbox = store.get_mailbox(args.mailbox_id)
            if mailbox is None:
                known = ", ".join(m.id for m in store.list_mailboxes()) or "none"
                print(f"unknown mailbox {args.mailbox_id}; known mailboxes: {known}",
                      file=sys.stderr)
                return 2
            scope = (f"mail after uid {args.from_uid}" if args.from_uid is not None
                     else "the whole mailbox")
            print(f"mailbox {mailbox.id} ({mailbox.username}@{mailbox.host}): "
                  f"cursor is {mailbox.last_seen_uid}; replay would re-read "
                  f"{scope}")
            if args.dry_run:
                print("dry run: nothing changed")
                return 0
            try:
                previous = store.reset_mailbox_cursor(args.mailbox_id,
                                                      from_uid=args.from_uid)
            except ValueError as error:
                print(f"replay refused: {error}", file=sys.stderr)
                return 2
            print(f"cursor rewound from {previous} to {args.from_uid}; "
                  "already-ingested documents are protected by content dedup")
            if not mailbox.enabled:
                print("mailbox was disabled by repeated poll failures and has been "
                      "re-enabled; the poll loop restarts on its next interval")
            if mailbox.last_seen_uid is not None:
                print("note: run this while `serve` is stopped, or expect the poll "
                      "in flight to write its own cursor when it finishes")
        finally:
            store.close()
        return 0

    if args.command == "events":
        return _run_events(config, args)

    if args.command == "serve":
        import asyncio

        asyncio.run(run_serve(config))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
