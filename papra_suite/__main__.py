"""The `papra-suite` entry point.

Commands are deliberately boring: `check` proves the deployment can start,
`serve` runs it, `replay` rewinds a cursor, `events` answers "why did nothing
arrive?" from the durable record. Nothing here decides policy; the store and the
pipeline do, and this file turns arguments into calls and rows into text.
"""

import argparse
import contextlib
import dataclasses
import json
import signal
import sqlite3
import sys
import unicodedata
from datetime import UTC, datetime, timedelta

from papra_suite import __version__
from papra_suite.config import ConfigError, load_config
from papra_suite.store import INGEST_EVENT_KINDS
from papra_suite.untrusted_text import UNSAFE_CATEGORIES as _UNSAFE_CATEGORIES

# Signals that must stop the service cleanly. SIGTERM is what `docker stop`
# sends; SIGINT is Ctrl-C.
STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def _recent_event_counts(events: dict) -> str:
    """One line for `check`: what the poller decided against in the last day.

    This is the line that turns "the mailbox is polling but nothing arrives" into
    a one-command answer, because `check` is the command an operator already runs
    when something is wrong — and the counts say "three senders refused today"
    without anyone having to open a second command.

    It never fails `check`. A fresh or empty store has no events, which is not a
    configuration error, and a check that exited non-zero for an empty table would
    train people to stop running check; so an unreadable table degrades to a
    stated reason on the same line rather than a new exit code.

    Rendered from the `events` group of `_check_facts`, which is also what `check --json`
    prints: an empty table is `none` here and `{}` there, and both statements come from one
    read of one store.
    """
    if events.get("unavailable"):
        return f"events (last 24h): unavailable ({events['unavailable']})"
    counts = events["by_kind"]
    if not counts:
        return "events (last 24h): none"
    summary = ", ".join(f"{kind}={counts[kind]}" for kind in sorted(counts))
    return f"events (last 24h): {summary}"


def _run_events(config, args) -> int:
    """`papra-suite events`: read the durable answer to "why did nothing arrive?"

    Read-only: it never migrates. A command that migrated on read would let a
    curiosity query perform a schema change, and the schema belongs to `check`
    and `serve`. An unmigrated store is therefore a stated condition, not a
    traceback and not a silent empty table.
    """
    from papra_suite.store import INGEST_EVENT_RETENTION_DAYS, Store
    unknown = [kind for kind in (args.kind or []) if kind not in INGEST_EVENT_KINDS]
    if unknown:
        print(f"unknown kind: {', '.join(unknown)}",
              file=sys.stderr)
        print(f"known kinds: {', '.join(INGEST_EVENT_KINDS)}", file=sys.stderr)
        return 2
    since = None
    if args.since is not None:
        try:
            since = datetime.strptime(args.since, "%Y-%m-%d").replace(
                tzinfo=UTC).isoformat()
        except ValueError:
            print(f"--since wants a date like 2026-11-09, got {args.since!r}",
                  file=sys.stderr)
            return 2

    # A missing database is refused BEFORE the store is opened: Store.__init__
    # creates the parent directory and the file, so by the time a Store exists
    # there is never a "missing database" left to report — the symptom an
    # operator came to read would have been replaced by an empty one.
    db_path = config.data_dir / "state.db"
    if not db_path.exists():
        print(f"no database at {db_path}", file=sys.stderr)
        print("run `papra-suite check` first; nothing to list", file=sys.stderr)
        return 2

    store = Store(db_path)
    try:
        known = store.table_columns("ingest_events")
        if not known:
            print("no ingest_events table in this store yet",
                  file=sys.stderr)
            print("run `papra-suite check` to migrate it; nothing to list",
                  file=sys.stderr)
            return 2
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

    if args.json:
        # Exactly the dataclass fields, no formatting decisions smuggled in: a
        # script should not have to re-derive what a record is from column order,
        # and `-` for absent is a display convention that would be a lie as JSON.
        print(_dump_json([dataclasses.asdict(row) for row in rows]))
        return 0

    if not rows:
        # An empty answer still says what was asked, because "no rows" and "no
        # rows for mailbox mb_7 since 2026-11-09" are different answers.
        print(f"no matching ingest events{_describe_event_scope(args)}")
        print(f"note: this table holds the last {INGEST_EVENT_RETENTION_DAYS} days. "
              "A successful ingestion is never recorded, but a message can still "
              "appear here for the attachment it skipped while another one "
              "uploaded, so no rows means no skipped or failed part — not that "
              "nothing arrived")
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


# Categories that must never reach a terminal raw: C0 and C1 controls (including
# ESC 0x1b and C1 CSI 0x9b, which are active escape sequences), format characters
# (zero-width space, which splices two values into one visually), surrogates and
# unassigned code points, and the Unicode line separators U+2028 / U+2029, which
# some renderers treat as a line break and so can still forge a row.
#
# The set lives in `papra_suite.untrusted_text` since Task 14, because the MCP document tools apply
# the same judgement to extracted text before handing it to an agent. Two copies would drift, and
# the drift would look like the same bytes being safe in `papra-suite events` and unsafe in a tool
# result — or the other way round, which is the direction that matters.

# Short escapes for the controls an operator actually meets, so a quoted value
# reads as "line\nbreak" rather than as a wall of \u000a.
_SHORT_ESCAPES = {"\n": "\\n", "\r": "\\r", "\t": "\\t",
                  "\b": "\\b", "\f": "\\f"}

# The same characters json.dumps leaves alone (it escapes only <0x20 plus " and \),
# listed explicitly because escaping them inside a JSON string literal is always
# legal and they can appear nowhere in JSON's structural syntax.
_JSON_UNSAFE = {chr(cp): f"\\u{cp:04x}"
                for cp in [*range(0x80, 0xA0), 0x2028, 0x2029]}


def _quote_untrusted(value: str) -> str:
    """Wrap a value in quotes and escape everything a terminal could act on."""
    chars: list[str] = []
    for char in value:
        if char in ('"', "\\"):
            chars.append("\\" + char)
        elif char in _SHORT_ESCAPES:
            chars.append(_SHORT_ESCAPES[char])
        elif unicodedata.category(char) in _UNSAFE_CATEGORIES:
            chars.append(f"\\u{ord(char):04x}")
        else:
            chars.append(char)
    return '"' + "".join(chars) + '"'


def _render_event_value(value) -> str:
    """One field, one line, whatever it contains.

    Absent renders as `-`, never as an empty field: "no sender was recorded" and
    "a sender was recorded that renders as nothing" have to be distinguishable, and
    a zero-width field in a tab-separated row shifts every column after it.

    Quoting rule: quote when the value is not printable, contains whitespace, or
    contains a quote or a backslash; leave everything else bare so `cut -f` and
    `grep` work on ordinary days. The trigger is `isprintable()` plus the character
    categories above rather than "contains whitespace", because the thing to fear in
    a terminal is not a newline but an **escape sequence**: a stored
    `\x1b[2Jevil.pdf` contains no whitespace at all, and printed raw its bytes drive
    the terminal that is supposed to be displaying them.

    This is deliberately the renderer's job and not the store's. The store turns
    control characters into spaces on write, but rows written before that rule
    existed are still sitting in old databases, and a reader must not assume its own
    write path produced the bytes it is about to print. Printable non-ASCII stays as
    typed: escaping `ä` as `\u00e4` would be no safer here, and reading the line is
    the only job this output has.
    """
    if value is None:
        return "-"
    if not isinstance(value, str):
        return str(value)
    if value == "":
        return '""'
    needs_quoting = (not value.isprintable()
                     or any(char.isspace() for char in value)
                     or '"' in value or "\\" in value)
    return _quote_untrusted(value) if needs_quoting else value


def _dump_json(payload) -> str:
    """JSON for scripts, with the characters a terminal could act on escaped.

    `--json` is machine output, but machine output gets piped into `less` and
    pasted into terminals, and json.dumps leaves C1 controls and U+2028/U+2029 as
    raw bytes. Escaping them after serialising keeps non-ASCII readable (which
    `ensure_ascii=True` would destroy) and the document valid.
    """
    text = json.dumps(payload, ensure_ascii=False)
    for char, escaped in _JSON_UNSAFE.items():
        text = text.replace(char, escaped)
    return text


def _describe_event_scope(args) -> str:
    parts = []
    if args.mailbox_id:
        parts.append(f"for mailbox {args.mailbox_id}")
    if args.kind:
        parts.append("of kind " + ", ".join(args.kind))
    if args.since is not None:
        parts.append(f"since {args.since}")
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


def _index_line(index, index_unavailable) -> str:
    """One line for `check`: what the index holds, what Papra holds, and what hurts.

    The fields are ordered the way the question is asked — how many references exist, how
    many are indexed in the generation that is current, how many of those Papra has the
    text for — and they end with the two numbers that say a subsystem is hurt rather than
    merely behind:

    * `extraction-empty` is the cheap signal during a backfill. A vision model that fails
      on a page tends to fail by returning nothing, so this one count is the difference
      between "a few odd pages" and "the extractor is wrong for this corpus". It only
      means anything because `deliver_content` writes `EMPTY_EXTRACTION_SHA`.
    * `cleanup-pending (failed N)` is the debt of upstream deletes still owed, with the
      part the retries have stopped on called out: a counter that watched only `pending`
      would fall to zero exactly when automatic retry stops.
    * `protected_name/date/notes` are the fields the bridge has stopped writing because Papra
      holds a value it cannot attribute — a human rename, or a write the ledger never
      recorded. Protection is permanent and carries no provenance, so a corpus-wide lock and
      one legitimate rename look identical from the outside, and the only other reader of
      these three numbers is `metadata --dry-run`, which an operator has to already suspect
      the answer to run. That is why they are on the line an operator runs when something is
      wrong: `check` is the command the README names for "why did nothing change".

    It never fails `check`, for the same reason the events line does: an empty store is not
    a configuration error, and a check that exits non-zero on one trains people to stop
    running check. `index` is None when the store refused to count at all; the reason is
    `index_unavailable`, chosen by `_check_facts`, and `--json` reports the same pair as
    `index: null` beside the reason rather than as a table of zeros.
    """
    if index is None:
        return f"index: unavailable ({index_unavailable})"
    stats = index
    return (f"index: {stats['references']} refs, {stats['indexed_current']} current, "
            f"{stats['stale']} stale, {stats['unledgered']} unledgered, "
            f"{stats['delivered']} delivered, {stats['extraction_empty']} extraction-empty, "
            f"{stats['delivery_blocked']} delivery-blocked, "
            f"protected_name={stats['protected_name']} "
            f"protected_date={stats['protected_date']} "
            f"protected_notes={stats['protected_notes']}, "
            f"{stats['repairs_24h']} repairs(24h), {stats['cleanup_pending']} "
            f"cleanup-pending (failed {stats['cleanup_failed']})")


def _deletion_pass_line(deletion: dict) -> str:
    """`deletions: ...` — what the last delete-propagation pass did, or why it refused.

    `check` opens no Papra or Kreuzakt client — its whole purpose is answering "is this
    healthy?" without the services in front of it — so it cannot run a pass to find out. The
    pass therefore stamps its own outcome into `settings` and this prints it, which is the only
    way `aborted` and `capped` reach the command the README names for "why did nothing change".
    They need to: a corpus whose listing stopped being trustworthy propagates no deletions at
    all, and from the ledger that is indistinguishable from a corpus where nobody deleted
    anything.

    The four extras are read once, by `_check_facts`, and `--json` emits the same group as data, so
    a quarantine cannot be visible on one surface and invisible on the other.
    """
    from papra_suite import indexing

    stamp = deletion["stamp"]
    # Four things a pass can be quietly wrong about, printed beside its own verdict: buckets it set
    # aside, the marks counter that can hold marking for a day, the walk cadence that can skip the
    # pass entirely, and the ids the liveness probe still cannot read. All four are stated in the
    # pass's own log, but `check` is where an operator looks when something is not happening, and a
    # quarantine is exactly that.
    extra = ""
    if deletion["set_aside"]:
        # The list is rewritten only by a pass that walked every bucket to its end, so after an
        # abort it is the PREVIOUS pass's verdict. Printed unqualified it reads as current, which
        # is the staleness the row was rewritten from a lock in order to avoid.
        extra += f" | set aside: {deletion['set_aside']}"
        if deletion["set_aside_stale"]:
            extra += " (not re-read by the last pass)"
    if deletion["marks_counted"] is not None:
        # The counter alone is not a fact: 34 means nothing until it is beside the 30 it is being
        # judged against, and the row to clear has to be named or this becomes an sqlite lesson.
        if deletion["marks_allowance"] is not None:
            # Both names belong here: the settings row is what holds the state, and the
            # subcommand is what clears it without an sqlite lesson at 23:00.
            extra += (f" | marks this window: {deletion['marks_counted']} of "
                      f"{deletion['marks_allowance']} allowed (clear "
                      f"{indexing.MARKS_ROLLING_SETTING} with `papra-suite ledger --clear-marks` "
                      "to resume)")
        else:
            # The allowance could not be read, but the count is still closer to useful than the
            # raw settings text.
            extra += f" | marks this window: {deletion['marks_counted']}"
    if deletion["gated_until"]:
        # Without this, a pass that is sitting out its cadence prints the last walk's `ok` line and
        # looks like a pass that found nothing, beside a document somebody trashed five minutes ago.
        extra += f" | gated until {deletion['gated_until']}"
    if deletion["probe_rereads"]:
        extra += f" | probe re-reads: {deletion['probe_rereads']}"
    if not stamp:
        return ("deletions: never ran (the sweep does this once per pass, with kreuzakt up)"
                + extra)
    return f"deletions: {stamp}" + extra


def _mcp_line(mcp: dict) -> str:
    """`mcp: ...` — whether this build would answer the network, and with how many credentials.

    The state a reader needs before they look for a token problem: a server that is disabled answers
    "connection refused", which looks identical from a client to a server that is firewalled,
    down, or
    has no tokens. One line distinguishes all four, and prints no label (see `mcp_state_line`).
    Rendered from `mcp_server.mcp_state`, which is also the group `check --json` emits.
    """
    from papra_suite import mcp_server

    if mcp.get("state") == "unavailable":
        return f"mcp: unavailable ({mcp['reason']})"
    return mcp_server.render_mcp_state(mcp)


def _oversized_line(oversized: dict) -> str:
    """`oversized: ...` — the documents the ceiling refused, and which ceiling that was.

    One line, three numbers: the count, the largest size seen, the limit in force. It is here
    because a refused document has **no ledger row** — it is skipped before enrollment — so
    `_index_line` above cannot see this population at all, and before the record existed nothing
    printed it.

    The wording is load-bearing: "N seen since <pass>" is what the record holds, namely what the
    passes walked. "Your corpus has N" would claim a census of documents nobody listed, and this
    suite has no catalogue of the corpus except the walks it performed.

    It never fails `check`, for the reason the lines above it do not: a scan over 100 MB is not a
    configuration error, and a check that exits non-zero on one trains people to stop running check.

    **There is no dashboard yet.** This line, `check --json`'s `oversized` group, and the MCP
    `index_status` tool are the surfaces that show the reason; when a dashboard exists it reads this
    same record.
    """
    from papra_suite.oversized import OVERSIZED_RECORD_LIMIT

    if oversized.get("unavailable"):
        # Same degradation as `_index_line`: `check` is run *because* something looks wrong, often
        # while the daemon is mid-write, and a traceback here would cost the operator every line of
        # this output rather than the one they came for.
        return f"oversized: unavailable ({oversized['unavailable']})"
    limit = oversized["limit_in_force"]
    if oversized["count"] == 0:
        return (f"oversized: none seen (limit in force {limit} bytes) — over "
                "it a document is neither enrolled nor indexed: compress it and re-upload to papra")
    moved = (f" (refused at {oversized['limit']}; they fit the limit now, and each drops out of "
             "this count when it is enrolled)"
             if oversized["limit"] is not None and oversized["limit"] != limit else "")
    evicted = (f", {oversized['evicted']} not named (the record lists the newest "
               f"{OVERSIZED_RECORD_LIMIT})" if oversized["evicted"] else "")
    return (f"oversized: {oversized['count']} seen since {oversized['since']}, largest "
            f"{oversized['largest']} bytes, limit in force {limit}{moved}{evicted} — not enrolled, "
            "not indexed: compress and re-upload to papra, or raise PAPRA_MAX_DOCUMENT_BYTES and "
            "run `papra-suite backfill` again")


def _content_writer_facts(config) -> dict:
    """The content-writer state as data: the state string, plus the two settings behind it.

    The booleans are included because a script watching a handover wants the cause rather than the
    prose, and because `content_writer_state` maps a pair of flags onto a word — the word alone
    cannot be inverted back into the configuration that produced it.
    """
    from papra_suite import indexing
    from papra_suite.pipeline import content_writer_state

    delivery = bool(config.index_content_delivery
                    and indexing.CONTENT_DELIVERY_IMPLEMENTED)
    return {"state": content_writer_state(delivery, config.legacy_workers),
            "delivery": delivery, "legacy_workers": bool(config.legacy_workers)}


def _content_writer_line(writer: dict) -> str:
    """`content writer: ...` — who writes Papra's text under this configuration.

    Computed from the config rather than from the settings rows, because `check` is asking
    "will this start, and what will it do", and `serve` seeds those rows from this same
    config on every startup. The state an operator cannot see is the dangerous one: the
    handover window, where nobody owns the column, is legitimate for an afternoon and wrong
    for a fortnight, and the two look identical from the outside.
    """
    return f"content writer: {writer['state']}"


def _metadata_writer_facts(config) -> dict:
    """The metadata-writer state as data, shaped exactly like the content writer's group."""
    from papra_suite import indexing
    from papra_suite.pipeline import metadata_writer_state

    delivery = bool(config.index_metadata_delivery
                    and indexing.METADATA_DELIVERY_IMPLEMENTED)
    return {"state": metadata_writer_state(delivery, config.legacy_workers),
            "delivery": delivery, "legacy_workers": bool(config.legacy_workers)}


def _metadata_writer_line(writer: dict) -> str:
    """`metadata writer: ...` — who writes Papra's name, date and notes under this config.

    Its own line rather than a clause on the content writer's, because the two columns
    change hands on different flags, in different orders, and one can be handed over while
    the other is not. The arrangement an operator has to be able to see is the one they set
    by accident: delivery on with the legacy title worker still running, which prints
    `two writers` rather than refusing, and stays true for exactly as long as the `ocr` and
    `title` queues take to starve — see `metadata_writer_state`.
    """
    return f"metadata writer: {writer['state']}"


def _check_facts(store, config) -> dict:
    """Every read `check` performs, taken once, in one place.

    This function exists so the two renderings of the readout cannot disagree: the lines an operator
    pastes into a bug report, and the JSON a script watches a multi-day backfill with. Every
    renderer above takes one group this produces, and nothing reads the store on the way to the
    terminal. A second read would risk more than a different number under concurrent writes — it
    would risk a different *meaning*, which is the failure a machine surface is supposed to remove
    rather than add.

    It reads while the store is open and never fails: every group degrades to a stated reason
    (`unavailable`, or a named `index_unavailable`), because `check` is what an operator runs *when*
    something is wrong, often with the daemon mid-write. The one case worth naming is a corrupt
    `index_generation`: the store refuses to count against a generation that does not exist, and
    naming the key is the useful part, because the repair is `ledger --invalidate` and a bare
    "unavailable" would send the operator to the wrong service.

    No credential material is read here. That stays a rule because `--json` is the output that gets
    redirected, collected, and pasted into an agent's context, and it is tested as one.
    """
    from papra_suite import indexing, mcp_server
    from papra_suite.oversized import oversized_seen

    facts: dict = {
        "data_dir": str(config.data_dir),
        "max_document_bytes": config.max_document_bytes,
        "senders": {"describe": config.allowed_senders.describe()},
        "index": None,
        "index_unavailable": None,
    }

    try:
        counts = store.count_ingest_events_by_kind(
            since=(datetime.now(UTC) - timedelta(days=1)).isoformat()
        )
        facts["events"] = {"window_hours": 24, "by_kind": dict(counts)}
    except sqlite3.Error as error:
        facts["events"] = {"window_hours": 24, "by_kind": None,
                           "unavailable": type(error).__name__}

    try:
        facts["index"] = dict(store.ledger_stats())
    except ValueError:
        facts["index_unavailable"] = "index_generation names no current generation"
    except sqlite3.Error as error:
        facts["index_unavailable"] = type(error).__name__

    deletion: dict = {"stamp": store.get_setting(indexing.LAST_DELETION_PASS_SETTING),
                      "set_aside": None, "set_aside_stale": False,
                      "marks_counted": None, "marks_allowance": None,
                      "gated_until": None, "probe_rereads": None}
    quarantine = store.get_setting(indexing.DELETION_QUARANTINE_SETTING)
    deletion["set_aside"] = quarantine
    deletion["set_aside_stale"] = (bool(quarantine)
                                   and not (deletion["stamp"] or "").startswith("ok "))
    marks = store.get_setting(indexing.MARKS_ROLLING_SETTING)
    if marks and "|" in marks:
        try:
            counted, allowance = indexing.marks_window_state(store)
            deletion["marks_counted"], deletion["marks_allowance"] = counted, allowance
        except ValueError:
            # The allowance is unreadable; the count is still the number an operator wants, and the
            # renderer says so without inventing a limit.
            deletion["marks_counted"] = int(marks.split("|", 1)[1])
    deletion["gated_until"] = indexing.listing_walk_gate(store)
    deletion["probe_rereads"] = indexing.pending_probe_rereads(store)
    facts["deletions"] = deletion

    try:
        record = oversized_seen(store)
        facts["oversized"] = {"count": record.count, "since": record.since,
                              "largest": record.largest, "evicted": record.evicted,
                              "limit": record.limit,
                              "limit_in_force": config.max_document_bytes}
    except sqlite3.Error as error:
        facts["oversized"] = {"unavailable": type(error).__name__}

    facts["content_writer"] = _content_writer_facts(config)
    facts["metadata_writer"] = _metadata_writer_facts(config)

    try:
        facts["mcp"] = mcp_server.mcp_state(store, config)
    except sqlite3.Error as error:
        facts["mcp"] = {"state": "unavailable", "reason": type(error).__name__}
    return facts


def main() -> int:
    parser = argparse.ArgumentParser(prog="papra-suite")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check",
                           help="validate config, open and migrate the store, exit")
    check.add_argument("--json", action="store_true",
                       help="print the readout as one JSON document, to watch a long backfill from "
                            "a script; every number is the one the text lines print")
    sub.add_parser("serve", help="run the pipeline: pollers, OCR/title workers, sweep")
    replay = sub.add_parser(
        "replay",
        help="rewind a mailbox's poll cursor so refused or missed mail is read "
             "again (e.g. after correcting PAPRA_ALLOWED_SENDERS)",
    )
    replay.add_argument("--mailbox-id", required=True)
    replay.add_argument(
        "--from-uid", type=int, default=None,
        help="rewind so that mail strictly AFTER this UID is re-read; to re-read a "
             "UID shown by `events`, pass one LESS than that UID. The default "
             "re-reads the whole mailbox",
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
    events.add_argument("--since", default=None, metavar="YYYY-MM-DD",
                        help="only rows at or after that UTC date")
    events.add_argument("--json", action="store_true",
                        help="emit the records as JSON instead of one line each")

    from papra_suite import metadata_cli

    metadata_cli.add_subparser(sub)
    # Same registration shape as `metadata`, for the same reason: the ledger commands are operator
    # commands with their own exit-code meaning, and the entry point stays a table of where to go.
    from papra_suite import ledger_cli

    ledger_cli.add_subparser(sub)
    # And again for the MCP credentials: `mcp-token` mints, lists and revokes, and the rule that a
    # plaintext is printed once lives in the module, not in this dispatch.
    from papra_suite import mcp_tokens

    mcp_tokens.add_subparser(sub)
    # Registered the same way, for the same reason: `jobs` is an operator command with its own
    # exit-code meaning, and the entry point stays a table of where to go.
    from papra_suite import jobs_cli

    jobs_cli.add_subparser(sub)
    # Same registration shape again, same reason: `backfill` is an operator command with its own
    # exit-code meaning and its own Papra walk, and the entry point stays a table of where to go.
    from papra_suite import backfill_cli

    backfill_cli.add_subparser(sub)
    # Same registration shape again, for the same reason: `mcp` sets the two rows the listener is
    # gated on; the entry point stays a table of where to go, not a second implementation.
    from papra_suite import mcp_cli

    mcp_cli.add_subparser(sub)

    args = parser.parse_args()

    try:
        config = load_config()
    except ConfigError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2

    if args.command == "check":
        from papra_suite import indexing
        from papra_suite.pipeline import content_writer_conflict
        from papra_suite.store import Store

        # Asked before the store is opened: this is the one configuration that must not
        # start, and `check` is how an operator learns it before a container enters a
        # restart loop rather than after.
        conflict = content_writer_conflict(
            bool(config.index_content_delivery and indexing.CONTENT_DELIVERY_IMPLEMENTED),
            config.legacy_workers)
        if conflict is not None:
            print(f"configuration error: {conflict}", file=sys.stderr)
            return 2

        config.data_dir.mkdir(parents=True, exist_ok=True)
        store = Store(config.data_dir / "state.db")
        try:
            store.migrate()
            # Read while the store is still open: `check` closes it in a finally,
            # and querying a closed store raises — which the helper would report
            # as "unavailable" instead of the counts that are the point of the
            # line. One read feeds both output modes, so the text lines and
            # `--json` cannot disagree about the store they just opened.
            facts = _check_facts(store, config)
        finally:
            store.close()
        if args.json:
            # The machine surface exists for the corpus backfill: extraction costs minutes a page,
            # so "how far along" is a question worth asking on a timer. Everything that could have
            # made this command print a warning instead of a document already returned non-zero
            # above, so stdout is one JSON value and nothing else.
            print(_dump_json(facts))
            return 0
        print(f"config ok; store migrated; data dir: {facts['data_dir']}")
        # Printed last because it is the line that answers "why is nothing
        # arriving?": an empty allow-list denies every sender by design.
        print(f"senders: {facts['senders']['describe']}")
        print(_recent_event_counts(facts["events"]))
        print(_index_line(facts["index"], facts["index_unavailable"]))
        print(_deletion_pass_line(facts["deletions"]))
        print(_oversized_line(facts["oversized"]))
        print(_content_writer_line(facts["content_writer"]))
        print(_metadata_writer_line(facts["metadata_writer"]))
        print(_mcp_line(facts["mcp"]))
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

    if args.command == "metadata":
        # Reached through the module, not by value: `tests/test_metadata_delivery.py` swaps
        # `metadata_cli.run_metadata` to prove this dispatch exists, and a `from ... import
        # run_metadata` here would bind the original at import time and make that test
        # assert about a name nothing calls.
        return metadata_cli.run_metadata(config, args)

    if args.command == "ledger":
        # Through the module for the same reason as `metadata` above, and with the same stakes:
        # this dispatch is the difference between a documented command and a module that imports
        # cleanly but cannot be reached from a shell.
        from papra_suite import ledger_cli

        return ledger_cli.run_ledger(config, args)

    if args.command == "mcp-token":
        # Through the module, as with `metadata` and `ledger`: the dispatch is what makes the
        # documented command reachable, and a `from ... import` here would make a test that swaps
        # the
        # module attribute assert about a name nothing calls.
        from papra_suite import mcp_tokens

        return mcp_tokens.run_mcp_token(config, args)

    if args.command == "jobs":
        # Through the module, as above: `tests/test_jobs_cli.py` swaps `jobs_cli.run_jobs` to pin
        # that this branch exists, and binding the function by name here would make that test
        # assert about a copy nothing calls.
        from papra_suite import jobs_cli

        return jobs_cli.run_jobs(config, args)

    if args.command == "backfill":
        # Through the module, as with `jobs`: the dispatch is what a README citation depends on,
        # and binding the function by name would let this branch drift from the module the tests
        # swap out.
        from papra_suite import backfill_cli

        return backfill_cli.run_backfill(config, args)

    if args.command == "mcp":
        # Through the module, as with `jobs` and `backfill`: the dispatch is what makes the
        # documented command reachable, and `tests/test_mcp_cli.py` swaps `mcp_cli.run_mcp` to pin
        # that this branch exists.
        from papra_suite import mcp_cli

        return mcp_cli.run_mcp(config, args)

    if args.command == "serve":
        import asyncio

        from papra_suite.mcp_server import McpListenerDied

        try:
            asyncio.run(run_serve(config))
        except ConfigError as error:
            # The startup refusals — two writers of `content`, above all — arrive here as
            # this process's answer rather than as a traceback. A container that dies with
            # a stack trace gets filed as a crash; one that prints the order to fix gets
            # fixed, and `2` is the exit `check` gives for the same configuration.
            print(f"configuration error: {error}", file=sys.stderr)
            return 2
        except McpListenerDied as error:
            # Option C, availability: the port stopped answering, so the process stops with it, and
            # non-zero — `restart: on-failure` never fires on a clean exit. One sentence, no stack.
            print(f"fatal: {error}", file=sys.stderr)
            return 1
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
