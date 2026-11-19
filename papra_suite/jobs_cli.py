"""`papra-suite jobs` — read the queue, reset one row, prove a drain.

Three commands wear one subcommand, and the reason they are commands rather than SQLite
incantations is that each one has a wrong reading a person at a terminal can reach.

**The listing answers "what is happening to this document?"** Since migration 004 made
`idx_jobs_idempotency` partial (`WHERE status IN ('pending','running')`), one
`(kind, org, document, content_hash)` can hold a `failed` row *and* the fresh `pending` row the
sweep queued for the same bytes. That is the design working, not a queue that has lost track of
itself — but printed as a list of rows it looks like the latter, so each row says whether it is
live and the footer counts the documents that hold more than one. A row count is never a document
count here.

**`--reset` names ONE row.** `Store.reset_job` already refuses to revive a terminal row whose key
has a live row, because that would collide with the partial index; what the CLI adds on top is
the refusal to revive a row a worker is *holding*. A `running` row belongs to a loop between its
gate check and its `mark_job_done`/`mark_job_failed` transition: putting it back to `pending`
lets a second worker claim the same document and lands the first worker's write on work it no
longer owns. The store primitive does not look at the named row's own status — it has no reason
to, and `recover_running_jobs` owns the restart case — so the command a human types has to.

**`--drain` is a gate, not a summary.** It is step two of the `content` handover (legacy off →
prove quiescence → delivery on) and it refuses while anything is `pending` or `running`, naming
both counts. It reads the queue with `count_jobs`, never with the listing's filters, and it
refuses `--status`/`--limit`/`--json` outright: `--drain --status pending` would answer "no
pending rows" and print a quiescence sentence above a `running` job.

Read-only apart from `--reset`, and it never migrates: like `events`, it refuses a missing
database *before* constructing `Store` — which creates the file it is handed, and would replace
"you pointed at the wrong directory" with "the queue is empty", the one wrong answer that looks
like a right one. It speaks to neither Papra nor Kreuzakt, so it works in the middle of the
outage it is being run during.
"""

from __future__ import annotations

import sqlite3
import sys
from dataclasses import asdict

from papra_suite.store import Store

#: Exit codes, with the meaning `check`, `replay`, `metadata` and `ledger` already give them:
#: `0` the command did what was asked, `1` it stopped or declined to change anything, `2` it
#: would not start at all (no database, no table, a row it refuses to touch, a store that will
#: not answer).
EXIT_OK = 0
EXIT_INCOMPLETE = 1
EXIT_REFUSED = 2

#: The two statuses migration 004's partial index constrains, i.e. the rows that are *work*.
#: `--drain` counts these and only these; `tests/test_jobs_cli.py` pins the pair against
#: `store.LIVE_JOB_STATUS_SQL` so the two cannot drift apart in silence.
LIVE_STATUSES = ("pending", "running")
TERMINAL_STATUSES = ("done", "failed")
JOB_STATUSES = LIVE_STATUSES + TERMINAL_STATUSES

#: The listing default from the plan, and the ceiling `events` already holds itself to.
DEFAULT_LIMIT = 50
MAX_LIMIT = 1000


def add_subparser(subparsers) -> None:
    """Register `jobs` on the entry point's subparser table."""
    parser = subparsers.add_parser(
        "jobs",
        help="read the job queue, reset one settled row (--reset), or prove a queue has "
             "drained before a writer handover (--drain)",
    )
    act = parser.add_mutually_exclusive_group()
    act.add_argument("--reset", metavar="JOB_ID", default=None,
                     help="put ONE row back to pending: attempts cleared, stored error cleared, "
                          "claimable now. Names a row, never a whole document — the superseded "
                          "rows of the same content stay as they are. Refuses a row a worker is "
                          "running right now")
    act.add_argument("--drain", action="store_true",
                     help="refuse (exit 1) while anything in scope is pending or running, and "
                          "say how many of each. This is the handover gate: run it twice a "
                          "minute apart and only switch the delivery flag on when both readings "
                          "are 0. Takes no listing filters")
    parser.add_argument("--kind", default=None,
                        help="only this job kind (ocr, title, index). With --drain, the queue to "
                             "drain; without it, the whole queue is judged")
    parser.add_argument("--status", choices=JOB_STATUSES, default=None,
                        help="only rows in this status (listing only)")
    parser.add_argument("--document-id", default=None,
                        help="only rows about this Papra document — all of them, including the "
                             "superseded history, so 'what is happening to this document?' has "
                             "one answer")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"most recent rows to show (default {DEFAULT_LIMIT}, clamped to "
                             f"1..{MAX_LIMIT}); says so when rows were left out")
    parser.add_argument("--json", action="store_true",
                        help="emit the job records as JSON instead of one line each, with the "
                             "full content hash")


def _display():
    """The entry point's own renderers for untrusted text, imported where they are used.

    One implementation of "somebody else's bytes on a terminal" is the point: `events`
    established the rules (escape what a terminal can act on, keep non-ASCII readable, `-` for
    absent) and a second, slightly different copy here would drift the first time one of them is
    fixed. `__main__` imports this module lazily inside `main()`, so importing it back here —
    inside a function, at call time, never at module import — cannot form a cycle.
    """
    from papra_suite.__main__ import _dump_json, _render_event_value

    return _dump_json, _render_event_value


def run_jobs(config, args, *, store: Store | None = None) -> int:
    """Do what was asked of the queue. Returns the process exit code.

    `store` is injectable for the same reason the passes take their clients: the behaviour that
    matters here is what a command does against a store it cannot read, and that is not something
    to stage by locking a database by hand.
    """
    if args.drain and (args.status or args.json
                       or (args.limit or DEFAULT_LIMIT) != DEFAULT_LIMIT):
        print("drain reads the whole queue in scope: --status, --limit and --json narrow a "
              "listing, and a narrowed reading is not a drain", file=sys.stderr)
        print("run `papra-suite jobs --drain` on its own (add --kind to scope it)",
              file=sys.stderr)
        return EXIT_REFUSED
    if args.reset and (args.status or args.document_id or args.json
                       or (args.limit or DEFAULT_LIMIT) != DEFAULT_LIMIT):
        # Reads like a batch command and must not become one. "Every failed index job" is a
        # rebuild's job (`ledger --requeue`) or a fresh enqueue's; this act names a row, and
        # quietly ignoring the filter that made the request a set would be the worse error.
        print("jobs refused: --reset names ONE row by id; --status, --document-id, --limit and "
              "--json belong to the listing", file=sys.stderr)
        print("read the queue and copy one id: `papra-suite jobs --status failed`", file=sys.stderr)
        return EXIT_REFUSED

    owns_store = store is None
    if owns_store:
        # Asked before the store is opened, for the reason in the module docstring: `Store()`
        # creates the file, and an empty database is a symptom destroyed.
        db_path = config.data_dir / "state.db"
        if not db_path.exists():
            print(f"no database at {db_path}", file=sys.stderr)
            print("run `papra-suite check` first; nothing to list", file=sys.stderr)
            return EXIT_REFUSED
        store = Store(db_path)
    try:
        if not store.table_columns("jobs"):
            print("no jobs table in this store yet, so `jobs` has nothing to read",
                  file=sys.stderr)
            print("run `papra-suite check` to migrate it; this command never migrates a store "
                  "it only reads", file=sys.stderr)
            return EXIT_REFUSED
        if args.reset:
            return _reset(store, args)
        if args.drain:
            return _drain(store, args)
        return _listing(store, args)
    except (ValueError, sqlite3.Error) as error:
        # A store that will not answer — locked mid-write by a worker, corrupt `settings`,
        # generation state that makes no sense — is a different report from a queue that is
        # quiet, and it must never arrive as a traceback: this is the command reached for during
        # the incident.
        print(f"jobs refused: {error}", file=sys.stderr)
        return EXIT_REFUSED
    except KeyboardInterrupt:
        print("jobs stopped: interrupted. Nothing was changed by an interrupted listing",
              file=sys.stderr)
        return EXIT_INCOMPLETE
    finally:
        if owns_store:
            store.close()


#: Printed in this order. `live` first, because with migration 004 the first question about any
#: printed row is whether it is work or history.
COLUMNS = ("live", "id", "kind", "org", "document", "content", "status", "attempts",
           "next attempt", "error")


def _scope(args) -> str:
    parts = []
    if args.kind:
        parts.append(f"kind {args.kind}")
    if args.status:
        parts.append(f"status {args.status}")
    if args.document_id:
        parts.append(f"document {args.document_id}")
    return " (" + ", ".join(parts) + ")" if parts else ""


def _job_line(job, render) -> str:
    """One row, one line, whatever the stored error contains.

    `document_id` and `error` both go through the entry point's renderer: the error text arrives
    from Kreuzakt, Papra or a model, and the document id arrives from Papra. `content_hash` is
    truncated to what a human can compare by eye, and `--json` carries the whole value for
    scripts that compare it programmatically.
    """
    return "\t".join((
        "yes" if job.status in LIVE_STATUSES else "no",
        job.id,
        job.kind,
        job.papra_org_id,
        render(job.document_id),
        job.content_hash[:12],
        job.status,
        str(job.attempts),
        render(job.next_attempt_at),
        render(job.error),
    ))


def _tuple_note(rows) -> str:
    """One line, only when the rows on screen are not one row per document.

    Suppressed when there is nothing in it for the same reason `ledger`'s remainder buckets are:
    a note that prints on every healthy reading is a note nobody reads on the reading where the
    queue really has two rows for one document, which is the reading it exists for.
    """
    counts: dict[tuple[str, str, str, str], int] = {}
    for row in rows:
        key = (row.kind, row.papra_org_id, row.document_id, row.content_hash)
        counts[key] = counts.get(key, 0) + 1
    multi = {key: n for key, n in counts.items() if n > 1}
    if not multi:
        return ""
    named = ", ".join(f"{key[2]} ({key[0]}, {n} rows)"
                      for key, n in sorted(multi.items())[:3])
    more = f" and {len(multi) - 3} more" if len(multi) > 3 else ""
    return (f"note: {len(multi)} work key(s) hold more than one row — {named}{more}. Since "
            "migration 004 a settled row does not block a retry, so superseded history and live "
            "work coexist by design: the row marked live=yes is the one that will run, and "
            "`--reset JOB_ID` takes ONE row id, never the whole key")


def _listing(store: Store, args) -> int:
    dump_json, render = _display()
    limit = max(1, min(int(args.limit or DEFAULT_LIMIT), MAX_LIMIT))
    rows = store.list_jobs(kind=args.kind, status=args.status, document_id=args.document_id,
                           limit=limit)
    # Counted separately from the rows printed: "50 rows" and "50 of 312 rows" are different
    # answers, and the second is the one that stops a capped listing reading as a whole queue.
    matched = store.count_jobs(kind=args.kind, status=args.status,
                               document_id=args.document_id)
    if args.json:
        print(dump_json([asdict(row) for row in rows]))
        return EXIT_OK
    if not rows:
        print(f"no jobs{_scope(args)}")
        print("`papra-suite jobs` with no filter lists every kind and every status, so an empty "
              "answer here means the filter matched, not that the table is empty")
        return EXIT_OK

    print("\t".join(COLUMNS))
    for row in rows:
        print(_job_line(row, render))
    live = sum(1 for row in rows if row.status in LIVE_STATUSES)
    print(f"shown {len(rows)} of {matched} row(s) matching{_scope(args)}: {live} live "
          f"({'/'.join(LIVE_STATUSES)}), {len(rows) - live} settled history")
    if matched > len(rows):
        print(f"{matched - len(rows)} further row(s) sit outside --limit {limit} — the oldest "
              "ones, raise the limit before concluding anything about the whole queue")
    note = _tuple_note(rows)
    if note:
        print(note)
    print("`papra-suite jobs --reset JOB_ID` revives one settled row; `--drain` proves a queue is "
          "empty before a writer handover")
    return EXIT_OK


def _print_reset(job, siblings) -> None:
    """Say which row moved, and what the command did NOT do to the rest of the tuple."""
    was = "was waiting out a backoff" if job.status == "pending" else f"was {job.status}"
    print(f"reset {job.kind} job {job.id}")
    print(f"  bucket {job.papra_org_id}  document {job.document_id}  "
          f"content {job.content_hash[:12]}")
    print(f"  {was}; now pending, attempts 0, stored error cleared, claimable by the next poll")
    if siblings:
        live = [row for row in siblings if row.status in LIVE_STATUSES]
        settled = [row for row in siblings if row.status in TERMINAL_STATUSES]
        print(f"  {len(siblings)} other row(s) cover the same work ({len(live)} live, "
              f"{len(settled)} settled history) and were left exactly as they were")
    print("  nothing was sent to Papra or Kreuzakt: this queues work only. The worker re-reads "
          "Papra's bytes and verifies them against this content hash before it uploads")


def _reset(store: Store, args) -> int:
    job = store.get_job(args.reset)
    if job is None:
        print(f"unknown job {args.reset}", file=sys.stderr)
        print("copy the id from the listing: `papra-suite jobs --status failed` "
              "(--kind and --document-id narrow it)", file=sys.stderr)
        return EXIT_REFUSED
    if job.status == "running":
        print(f"refusing to reset {job.id}: it is running, so a worker is holding it right now",
              file=sys.stderr)
        print("resetting a running row lets a second worker claim the same document while the "
              "first still expects to settle this one. Let it finish, then read it again: "
              f"`papra-suite jobs --document-id {job.document_id}`", file=sys.stderr)
        return EXIT_REFUSED

    # Read before the write, so the line printed afterwards can say what was NOT touched. The
    # store refuses a reset that would collide with a live row, and the live row's id is the
    # answer the operator needs — which is why the siblings are read even on the refusal path.
    siblings = [row for row in store.list_jobs(kind=job.kind, document_id=job.document_id,
                                               limit=MAX_LIMIT)
                if row.content_hash == job.content_hash and row.id != job.id]
    try:
        store.reset_job(job.id)
    except ValueError as error:
        if "running" in str(error):
            # The row was claimed after the read above said `pending`. The guard next to the write
            # is the one that holds, and the operator gets the same refusal either way — a
            # traceback here would bury the one fact that matters, which row is now a worker's.
            print(f"refusing to reset {job.id}: it is running, a worker claimed it just now",
                  file=sys.stderr)
            print("let it settle, then read the document: "
                  f"`papra-suite jobs --document-id {job.document_id}`", file=sys.stderr)
            return EXIT_REFUSED
        if "already queued" in str(error):
            # Nothing to fix and nothing changed: the work IS queued, under another row. Exit 1
            # rather than 0 because the row that was named is still settled, and 0 would read as
            # "this row runs now".
            print(error)
            print("nothing was changed — that other row is the same work and it will be claimed. "
                  f"Read it with `papra-suite jobs --document-id {job.document_id}`")
            return EXIT_INCOMPLETE
        raise
    _print_reset(job, siblings)
    return EXIT_OK


def _kinds_holding_work(store: Store, kind: str | None) -> list[str]:
    """Which kinds the refusal is about, named.

    Listing rows only to read their `kind` is deliberate: `count_jobs` gives the exact totals the
    gate is judged on, and this gives the names. The listing cap can therefore only ever shorten
    a name list, never inflate the count that decides the answer.
    """
    kinds: set[str] = set()
    for status in LIVE_STATUSES:
        kinds.update(row.kind for row in
                     store.list_jobs(kind=kind, status=status, limit=MAX_LIMIT))
    return sorted(kinds)


def _drain(store: Store, args) -> int:
    """The handover gate: 0 pending and 0 running in scope, or a refusal that counts both.

    Two numbers, not one total, because they say different things about why the queue is not
    quiet: `pending` means something is still filling it, `running` means a worker is inside a
    document. Both have to reach zero, and both have to read zero twice a minute apart — a single
    empty reading is what a loop that claims the next row one second later looks like.
    """
    scope = f"kind {args.kind}" if args.kind else "every kind"
    # One statement for both live counts: two queries could be answered from two instants, and a
    # worker restart moves `running` back to `pending` in between, which is how both numbers come
    # home 0 above a queue that has work.
    counts = store.count_jobs_by_status(args.kind)
    pending = counts.get("pending", 0)
    running = counts.get("running", 0)
    if pending or running:
        print(f"cannot drain {scope}: {pending} pending, {running} running")
        holders = _kinds_holding_work(store, args.kind)
        if holders:
            print(f"still holding work: {', '.join(holders)}")
        print("keep the delivery flag off — the legacy loops are still feeding or working this "
              "queue, and two writers of the same column is the race this bridge exists to remove")
        print("read it again a minute from now; the handover wants 0 pending and 0 running twice "
              "in a row")
        return EXIT_INCOMPLETE
    settled = counts.get("done", 0) + counts.get("failed", 0)
    print(f"{scope}: 0 pending, 0 running — quiet ({settled} settled row(s) are history, not "
          "work)")
    print("safe to switch the writer over once a second reading a minute from now is also 0: run "
          "`papra-suite jobs --drain` again, then turn the delivery flag on")
    return EXIT_OK
