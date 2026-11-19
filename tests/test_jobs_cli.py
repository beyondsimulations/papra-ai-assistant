"""Task 15: `papra-suite jobs` — the queue an operator can read, and nudge one row of.

Three shapes govern this file, and all three come from the state the queue is actually in
after migration 004.

**A work tuple can hold several rows.** `(kind, org, document, content_hash)` now constrains
*live* rows only, so a `failed` row and the fresh `pending` row the sweep queued afterwards
coexist on purpose. Every assertion about listing therefore counts rows, never "the job for a
document", and every assertion about `--reset` names ONE row id — a reset that swept the tuple
would resurrect superseded history and hand a worker bytes the ledger has already moved past.

**`--reset` may not reach into a worker's hands.** A `running` row belongs to a loop that is
between its gate check and its `mark_job_done`/`mark_job_failed` transition; putting it back to
`pending` mid-flight lets a second worker claim the same document and makes the first worker's
transition land on work somebody else now owns. So the command refuses a row a worker is
holding, and only such a row — a `pending` row in backoff is exactly what "retry it now, do not
wait eight minutes" is for.

**`--drain` is a gate, not a convenience.** It is the middle step of the `content` handover,
where the cost of a wrong reading is two writers on one column. It reads the queue with its own
count (never the listing's filters), it refuses while anything is `pending` or `running`, and
the refusal names how many of each, because "0 twice a minute apart" is the proof the runbook
asks for and a bare "not empty" is not.

The store surface this command needs is pinned here too (`list_jobs(document_id=…)`,
`count_jobs`), because it was added for this command and a filter that silently does nothing is
the failure that hid a re-scheduling gap behind a passing `xfailed` in Task 6.
"""

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import papra_suite.store as store_module
from papra_suite.jobs_cli import (
    EXIT_INCOMPLETE,
    EXIT_REFUSED,
    JOB_STATUSES,
    LIVE_STATUSES,
    MAX_LIMIT,
    add_subparser,
    run_jobs,
)
from papra_suite.store import Store
from tests.test_config import BASE_ENV
from tests.test_index_job import HASH as H

ORG = "org-a"


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=ORG, name="Org A")
    yield store
    store.close()


def cli_args(**over):
    base = dict(command="jobs", kind=None, status=None, document_id=None, limit=50,
                reset=None, drain=False, json=False)
    return SimpleNamespace(**{**base, **over})


def parse_cli(argv):
    """Parse through the real parser, so the flag surface itself is under test."""
    parser = argparse.ArgumentParser(prog="papra-suite")
    sub = parser.add_subparsers(dest="command", required=True)
    add_subparser(sub)
    return sub.choices["jobs"].parse_args(argv)


def _run_one(store, kind, job_id):
    """Claim THIS row, not just the next one.

    `claim_next_job` takes the oldest *claimable* row of a kind, so a second row of the same kind
    would be claimed instead of the one this test is putting into `running`/`done`/`failed`. The
    others are pushed past their next attempt — they stay `pending`, only later — because parking
    them as `done` would change the very statuses the drain and count assertions read.
    """
    for other in store.list_jobs(kind=kind, status="pending", limit=500):
        if other.id != job_id:
            store.mark_job_failed(other.id, "deferred by the test", max_attempts=10 ** 6)
    for _ in range(10):
        claimed = store.claim_next_job(kind)
        if claimed is None:
            break
        if claimed.id == job_id:
            return
        store.mark_job_done(claimed.id)
    raise AssertionError(f"could not get {kind} row {job_id} into running")


def seed_job(store, kind="ocr", status="pending", *, org=ORG, document_id=None,
             content_hash=H, error=None):
    """Put one job row into `status`, through the store's own transitions.

    Never an UPDATE: what `pending`/`running`/`done`/`failed` mean is defined by the calls that
    produce them, and a row written by hand is not the row the workers leave behind.
    """
    doc = document_id if document_id is not None else f"doc-{kind}-1"
    job_id = store.enqueue_job(kind=kind, papra_org_id=org, document_id=doc,
                               content_hash=content_hash, payload={"name": f"{doc}.pdf"})
    if status == "pending":
        return job_id
    if status == "running":
        _run_one(store, kind, job_id)
        return job_id
    if status == "done":
        _run_one(store, kind, job_id)
        store.mark_job_done(job_id)
        return job_id
    _run_one(store, kind, job_id)
    store.mark_job_failed(job_id, error or "kreuzakt queue error", max_attempts=1)
    return job_id


def failing_index_job(store, attempts=3, *, org=ORG, document_id="doc-1", content_hash=H,
                      error="kreuzakt queue error: extraction died"):
    """An `index` row that has spent its allowance, so the ceiling is what stopped it."""
    job_id = store.enqueue_job(kind="index", papra_org_id=org, document_id=document_id,
                               content_hash=content_hash,
                               payload={"file_hash": content_hash, "name": "scan.pdf"})
    _run_one(store, "index", job_id)
    # One claim, then the failures the worker would have written: `mark_job_failed` is what spends
    # an attempt, and a second claim per attempt would have to wait out the real backoff.
    for _ in range(attempts):
        store.mark_job_failed(job_id, error, max_attempts=attempts)
    assert store.get_job(job_id).status == "failed"
    return job_id


# --- the store surface this command exists to expose ---------------------------------

def test_list_jobs_document_filter_returns_only_that_document(store):
    """The filter Task 6 had to xfail around: "what is happening to this document?".

    With the filter ignored, a typo in a document id answers the question with the whole
    queue and the answer looks right. So the assertion is about the ids that come back, in
    both directions: the named document's rows, and nothing else's.
    """
    mine = store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-mine",
                             content_hash=H, payload={})
    store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-other",
                      content_hash=H, payload={})

    rows = store.list_jobs(document_id="doc-mine")

    assert [row.id for row in rows] == [mine]
    assert store.list_jobs(document_id="doc-none") == []
    assert len(store.list_jobs()) == 2                # the unfiltered read is unchanged


def test_list_jobs_kind_filter_and_the_combination_that_makes_a_question_specific(store):
    """`--kind` belongs in SQL, not in the CLI: a filter applied after LIMIT pages wrongly.

    Fifty `index` rows ahead of one `ocr` row would leave `jobs --kind ocr` printing nothing
    while the queue holds work — the exact reading that makes an operator believe a drain.
    """
    ocr = seed_job(store, kind="ocr", document_id="doc-1")
    index = store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-1",
                              content_hash=H, payload={})

    assert [row.id for row in store.list_jobs(kind="ocr")] == [ocr]
    assert [row.id for row in store.list_jobs(kind="ocr", document_id="doc-1")] == [ocr]
    assert store.list_jobs(kind="ocr", document_id="doc-2") == []
    # Two filters, one row: the pair is what answers "is the index worker busy with doc-1?".
    assert [row.id for row in store.list_jobs(status="pending", kind="index")] == [index]
    assert store.list_jobs(status="failed", kind="index") == []


def test_count_jobs_answers_the_question_drain_needs_exactly(store):
    """The count comes from SQL, so a listing cap cannot turn a bad queue into a calm one.

    `count_jobs(kind, status)` is the plan's shape and it has to hold its own: per kind, per
    status, per document, and the unfiltered total.
    """
    seed_job(store, kind="ocr", status="pending", document_id="doc-1")
    seed_job(store, kind="ocr", status="running", document_id="doc-2")
    seed_job(store, kind="ocr", status="failed", document_id="doc-3")
    seed_job(store, kind="index", status="pending", document_id="doc-1")

    assert store.count_jobs("ocr", "pending") == 1
    assert store.count_jobs("ocr", "running") == 1
    assert store.count_jobs("ocr") == 3
    assert store.count_jobs() == 4
    assert store.count_jobs(kind="index", status="pending") == 1
    assert store.count_jobs(document_id="doc-1") == 2
    assert store.count_jobs("ocr", "done") == 0


def test_the_live_statuses_the_cli_prints_are_the_ones_the_index_constrains():
    """`LIVE_STATUSES` must stay the pair migration 004 indexes on, or `--drain` lies.

    The drain gate counts `pending` and `running` because those are the rows the partial unique
    index calls live. If one list moves and the other does not, the runbook's proof of
    quiescence stops meaning anything — so this pins the pair against the store's own SQL.
    """
    for status in LIVE_STATUSES:
        assert f"'{status}'" in store_module.LIVE_JOB_STATUS_SQL
    for status in ("done", "failed"):
        assert f"'{status}'" not in store_module.LIVE_JOB_STATUS_SQL
    assert JOB_STATUSES == ("pending", "running", "done", "failed")


# --- the listing -------------------------------------------------------------------

def test_jobs_lists_kind_org_document_attempts_error(store, capsys):
    """The row an operator needs in order to act: which document, how often, and why not."""
    job_id = store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-1",
                               content_hash=H, payload={})
    store.mark_job_failed(job_id, "kreuzakt queue error: extraction died", max_attempts=1)

    assert run_jobs(None, cli_args(kind="index", status="failed"), store=store) == 0

    out = capsys.readouterr().out
    for needle in ("index", ORG, "doc-1", "extraction died", job_id, "1"):
        assert needle in out


def test_an_empty_listing_says_what_was_asked(store, capsys):
    """`no jobs` and `no failed index jobs` are different answers.

    The second is the one that follows a `--reset` that seemed to do nothing, and the operator
    has to be able to tell "the queue is empty" from "your filter matched nothing".
    """
    assert run_jobs(None, cli_args(kind="index", status="failed"), store=store) == 0

    out = capsys.readouterr().out
    assert "no jobs" in out and "index" in out and "failed" in out


def test_several_rows_of_one_work_tuple_are_all_listed_and_the_live_one_is_named(store,
                                                                                capsys):
    """Migration 004's visible consequence, asserted on the display rather than the table.

    A spent `failed` row and the `pending` row the sweep queued for the same bytes are both
    true, and only one of them will run. Printed as two indistinguishable rows, the pair reads
    as a queue that has lost track of itself; printed with the live row named and the history
    counted, it reads as what it is.
    """
    spent = failing_index_job(store, attempts=1, document_id="doc-1")
    live = store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-1",
                             content_hash=H, payload={})

    assert run_jobs(None, cli_args(kind="index", document_id="doc-1"), store=store) == 0

    out = capsys.readouterr().out
    body = [line for line in out.splitlines() if line.startswith(("yes\t", "no\t"))]
    assert len(body) == 2, out
    live_rows = [line for line in body if line.startswith("yes\t")]
    history_rows = [line for line in body if line.startswith("no\t")]
    assert len(live_rows) == 1 and live in live_rows[0]
    assert len(history_rows) == 1 and spent in history_rows[0]
    assert "more than one row" in out and "--reset" in out


def test_listing_says_when_the_cap_left_rows_behind_it(store, capsys):
    """A capped listing that prints no more rows than it was told is not an honest one.

    The cap is a display limit, not a fact about the queue, and the queue is the thing the
    operator came to read about — so the number left out is printed beside the cap.
    """
    for index in range(6):
        seed_job(store, kind="index", document_id=f"doc-{index}")

    assert run_jobs(None, cli_args(kind="index", limit=4), store=store) == 0

    out = capsys.readouterr().out
    assert "6" in out and "--limit 4" in out


def test_a_stored_error_cannot_drive_the_terminal_it_is_printed_in(store, capsys):
    """`error` is somebody else's bytes, and the listing prints it.

    Kreuzakt, Papra and a model can all put text in that column. Printed raw, an escape
    sequence clears the screen the operator is reading and a newline forges a row that never
    existed — so the discipline `events` established is used here too, not a second, weaker copy
    of it.
    """
    hostile = "died\x1b[2J\nforged row"
    store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-x",
                      content_hash=H, payload={})
    failing = store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-y",
                                content_hash="ab" * 32, payload={})
    store.mark_job_failed(failing, hostile, max_attempts=1)

    assert run_jobs(None, cli_args(status="failed"), store=store) == 0

    out = capsys.readouterr().out
    assert len([line for line in out.splitlines() if line.startswith(("yes\t", "no\t"))]) == 1
    assert "\\u001b[2J" in out and "\\n" in out


def test_json_output_is_the_record_and_not_the_display(store, capsys):
    """`--json` is for scripts, so it gives the fields, the full hash, and no `-`.

    The table truncates the content hash to something a human can compare by eye; a script
    comparing hashes needs the whole thing, and a dash standing in for `None` is a display
    convention that would be a lie inside JSON.
    """
    failing_index_job(store, attempts=1, document_id="doc-1")

    assert run_jobs(None, cli_args(json=True, kind="index"), store=store) == 0

    rows = json.loads(capsys.readouterr().out)
    assert len(rows) == 1
    assert rows[0]["document_id"] == "doc-1"
    assert len(rows[0]["content_hash"]) == 64
    assert rows[0]["status"] == "failed" and "extraction died" in rows[0]["error"]


# --- `--reset` ---------------------------------------------------------------------

def test_reset_clears_error_and_makes_the_job_claimable(store, capsys):
    """The escape hatch `reset_job` was written for and never had a caller for."""
    job_id = failing_index_job(store, attempts=3)
    assert store.claim_next_job("index") is None          # attempts exhausted

    assert run_jobs(None, cli_args(reset=job_id), store=store) == 0

    row = store.get_job(job_id)
    assert row.status == "pending" and row.attempts == 0 and row.error is None
    assert store.claim_next_job("index").id == job_id
    out = capsys.readouterr().out
    assert job_id in out and "pending" in out


def test_reset_names_the_row_it_reset_and_the_work_it_belongs_to(store, capsys):
    """"reset 4f2a…" is not an answer if the operator cannot see which document that was.

    The id is opaque by design; the kind, bucket, document and content are what the person at
    the terminal matches against the failure in their log.
    """
    job_id = failing_index_job(store, attempts=1, document_id="doc-42")

    assert run_jobs(None, cli_args(reset=job_id), store=store) == 0

    out = capsys.readouterr().out
    for needle in ("index", ORG, "doc-42", H[:12]):
        assert needle in out


def test_reset_unknown_id_exits_2_and_writes_nothing(store, capsys):
    """A typo must not become "done", and it must not become the nearest row either."""
    assert run_jobs(None, cli_args(reset="nope"), store=store) == EXIT_REFUSED

    assert "unknown job" in capsys.readouterr().err
    assert store.list_jobs() == []


def test_reset_refuses_a_running_row_a_worker_is_holding(store, capsys):
    """Resetting a row out from under its worker corrupts that worker's transition.

    The loop is between its gate check and `mark_job_done`; a second claimer would run the same
    document twice and the first worker's write would land on work it no longer owns. The store
    primitive does not look at the named row's own status, so the command a human types has to.
    """
    job_id = seed_job(store, kind="index", status="running", document_id="doc-1")
    before = store.get_job(job_id)

    assert run_jobs(None, cli_args(reset=job_id), store=store) == EXIT_REFUSED

    after = store.get_job(job_id)
    assert after.status == "running" and after.attempts == before.attempts
    assert "running" in capsys.readouterr().err


def test_reset_requeues_a_pending_row_in_backoff_without_duplicating_it(store, capsys):
    """The other useful case: work that will not be claimed for another eight minutes.

    A `pending` row is not in anyone's hands, so clearing its backoff is a scheduling change,
    not a race — and it must stay one row, because the point is to hurry the existing work.
    """
    job_id = seed_job(store, kind="index", document_id="doc-1")
    store.claim_next_job("index")
    store.mark_job_failed(job_id, "kreuzakt 503", max_attempts=5)   # backoff, still pending
    assert store.claim_next_job("index") is None

    assert run_jobs(None, cli_args(reset=job_id), store=store) == 0

    assert store.claim_next_job("index").id == job_id
    assert len(store.list_jobs(kind="index")) == 1
    assert "backoff" in capsys.readouterr().out


def test_reset_touches_the_named_row_and_not_the_whole_work_tuple(store):
    """Two spent rows for one tuple, one reset, and the other stays spent.

    Migration 004 makes this state normal: the first failure held the key, the sweep's retry
    took a row of its own and failed too. Reviving both would run the same bytes twice and
    rewrite history the operator never asked to revisit — one id in, one row out.
    """
    first = failing_index_job(store, attempts=1, document_id="doc-1")
    second = store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-1",
                               content_hash=H, payload={})
    store.claim_next_job("index")
    store.mark_job_failed(second, "still broken", max_attempts=1)
    assert store.get_job(first).status == "failed"

    assert run_jobs(None, cli_args(reset=first), store=store) == 0

    assert store.get_job(first).status == "pending"
    assert store.get_job(second).status == "failed"
    assert len(store.list_jobs(kind="index")) == 2       # no third row invented


def test_reset_defers_to_the_live_row_of_the_tuple_and_names_it(store, capsys):
    """The collision the store refuses, translated into the reading an operator needs.

    The work is already queued under another id: saying "unknown" or exiting 0 would be wrong
    in opposite directions, so the live row's id is printed and the exit says nothing changed.
    """
    spent = failing_index_job(store, attempts=1, document_id="doc-1")
    live = store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-1",
                             content_hash=H, payload={})

    assert run_jobs(None, cli_args(reset=spent), store=store) == EXIT_INCOMPLETE

    out = capsys.readouterr().out
    assert live in out and "already queued" in out
    assert store.get_job(spent).status == "failed"
    assert store.get_job(live).status == "pending"


def test_reset_refuses_the_filters_that_would_have_to_choose_the_row(store, capsys):
    """`--reset --status failed` reads like a batch command, and this task has no batch act.

    Reviving every failed row at once is a rebuild or a fresh enqueue (`ledger --requeue`), not a
    repair of one document. Silently ignoring the filter would be the worse error, because the
    operator would get one row back and believe they had asked for the set.
    """
    job_id = failing_index_job(store, attempts=1)

    for over in ({"status": "failed"}, {"document_id": "doc-1"}, {"json": True},
                 {"limit": 5}):
        assert run_jobs(None, cli_args(reset=job_id, **over), store=store) == EXIT_REFUSED, over

    assert "--reset" in capsys.readouterr().err
    assert store.get_job(job_id).status == "failed"


# --- `--drain` ---------------------------------------------------------------------

def test_drain_refuses_while_any_job_is_pending_or_running(store, capsys):
    """The handover gate, and the count that makes the refusal actionable.

    "Not empty" sends an operator to look at the table by hand; `1 pending, 1 running` says both
    loops are still being fed and the flag must stay off.
    """
    seed_job(store, kind="ocr", status="pending", document_id="doc-1")
    seed_job(store, kind="ocr", status="running", document_id="doc-2")

    assert run_jobs(None, cli_args(kind="ocr", drain=True), store=store) == EXIT_INCOMPLETE

    out = capsys.readouterr().out
    assert "cannot drain" in out and "1 pending" in out and "1 running" in out


def test_drain_succeeds_when_queue_is_quiet(store, capsys):
    """Zero is only a proof when the command says which queue it read.

    So the success line names the kind it drained, and the interval the runbook wants (two
    readings a minute apart) is repeated here rather than left in a document nobody has open at
    23:00.
    """
    assert run_jobs(None, cli_args(kind="ocr", drain=True), store=store) == 0

    out = capsys.readouterr().out
    assert "safe to switch" in out and "ocr" in out


def test_drain_counts_every_kind_when_none_is_named(store, capsys):
    """Forgetting `--kind` must not produce a false all-clear.

    Draining `ocr` is one handover step; `--drain` with no kind is a question about the whole
    queue, and answering it with the `ocr` numbers would tell an operator to turn on a delivery
    flag while the index worker is mid-document.
    """
    seed_job(store, kind="index", status="running")

    assert run_jobs(None, cli_args(drain=True), store=store) == EXIT_INCOMPLETE

    out = capsys.readouterr().out
    assert "cannot drain" in out and "index" in out


def test_drain_ignores_the_spent_rows_that_are_history_not_work(store, capsys):
    """`done` and `failed` rows are why migration 004 exists, and must not block a drain.

    If they counted, the handover could never be proven on any deployment that had ever failed
    one job, and the runbook would be back to a hand-written query with `status IN
    ('pending','running')` in it.
    """
    seed_job(store, kind="ocr", status="failed")
    seed_job(store, kind="ocr", status="done", document_id="doc-2")

    assert run_jobs(None, cli_args(kind="ocr", drain=True), store=store) == 0

    out = capsys.readouterr().out
    assert "safe to switch" in out and "2" in out


def test_drain_refuses_a_document_scope_because_it_is_not_a_gate(store, capsys):
    """`--drain --document-id DOC` reads as "is this document quiet?" and is not the handover gate.

    The gate is judged over everything in scope, so answering it from one document would print the
    quiescence sentence above every row that is still work. It is also a flag `_drain` never passes
    anywhere: typed beside `--drain` it silently asked for something the command does not do — the
    reading this CLI refuses instead of reinterpreting, and the refusal has to name the flag or the
    operator cannot see what to drop.
    """
    seed_job(store, kind="index", document_id="doc-1")

    assert run_jobs(None, cli_args(drain=True, document_id="doc-1"), store=store) == EXIT_REFUSED

    err = capsys.readouterr().err
    assert "drain" in err and "--document-id" in err
    assert store.count_jobs() == 1                 # nothing drained, nothing changed


def test_the_drain_refusal_names_a_kind_the_listing_cap_would_hide(store, capsys):
    """1001 `ocr` rows must not silence the one `index` row that is also holding work.

    The counts and the exit code have been exact all along — they come from SQL — but the *names* in
    the refusal came from a capped listing, so past roughly a thousand live rows the sentence read
    `still holding work: ocr` while an `index` job sat waiting, and the operator went to watch the
    loop that was already quiet. The listing is newest-first, which is what hides the oldest kind:
    the `index` row is seeded first here so a capped read cannot see it.
    """
    seed_job(store, kind="index", document_id="doc-index")        # the oldest live row
    for index in range(MAX_LIMIT + 1):
        store.enqueue_job(kind="ocr", papra_org_id=ORG, document_id=f"doc-{index}",
                          content_hash=H, payload={})
    capped = store.list_jobs(status="pending", limit=MAX_LIMIT)
    assert len(capped) == MAX_LIMIT and "index" not in {row.kind for row in capped}

    assert run_jobs(None, cli_args(drain=True), store=store) == EXIT_INCOMPLETE

    out = capsys.readouterr().out
    assert f"{MAX_LIMIT + 2} pending" in out, out
    assert "still holding work: index, ocr" in out, out


def test_drain_refuses_filters_that_would_narrow_the_reading(store, capsys):
    """A drain that reads one status is not a drain.

    `--drain --status pending` would answer "no pending rows" and print the quiescence sentence
    above a `running` job — so the combination is refused instead of quietly reinterpreted.
    """
    for over in ({"status": "pending"}, {"limit": 5}, {"json": True}):
        args = cli_args(kind="ocr", drain=True, **over)
        assert run_jobs(None, args, store=store) == EXIT_REFUSED, over
    assert "drain" in capsys.readouterr().err


def test_the_listing_prints_newest_first_after_the_rowid_ordering(store, capsys):
    """The queue view's contract, at the terminal rather than in the store.

    `ORDER BY rowid DESC` replaced `ORDER BY created_at DESC, rowid DESC` to stop the bare listing
    sorting the whole table, and the claim is that the operator sees the same page, in the same
    order. `enqueue_job` stamps `created_at` as it inserts, so newest-last-inserted is both
    orderings; this pins it where it is read, because that order is how an operator picks a row to
    `--reset`.
    """
    first = seed_job(store, kind="index", document_id="doc-first")
    second = seed_job(store, kind="index", document_id="doc-second")
    third = seed_job(store, kind="index", document_id="doc-third")

    assert run_jobs(None, cli_args(kind="index"), store=store) == 0

    body = [line for line in capsys.readouterr().out.splitlines()
            if line.startswith(("yes\t", "no\t"))]
    assert [line.split("\t")[1] for line in body] == [third, second, first]


@pytest.mark.parametrize("flag,over", [("--document-id", {"document_id": ""}),
                                      ("--kind", {"kind": ""}),
                                      ("--document-id", {"document_id": "   "})],
                         ids=["empty-document", "empty-kind", "blank-document"])
def test_an_empty_scope_value_is_refused_rather_than_reading_the_whole_queue(store, capsys, flag,
                                                                            over):
    """`jobs --document-id "$DOC"` with `DOC` unset is the whole queue, printed as an answer.

    An empty value is dropped by every filter it would have gone into, so a shell variable that
    never got set means "no filter" and the operator gets rows about other people's documents with
    no scope echoed above them — the failure class Task 6's xfail was written about, and the
    plausible-looking wrong answer of the ones this store can give. So an explicitly-passed empty
    scope is refused, on the listing and on the gate alike.
    """
    seed_job(store, kind="index", document_id="doc-1")

    assert run_jobs(None, cli_args(**over), store=store) == EXIT_REFUSED, over
    assert run_jobs(None, cli_args(drain=True, **over), store=store) == EXIT_REFUSED, over

    err = capsys.readouterr().err
    assert "empty" in err and flag in err, err
    assert capsys.readouterr().out == ""            # not even an empty listing


def test_a_quiet_drain_names_the_writer_it_proves_quiet(store, capsys):
    """The operator is holding two flags, and one kind only speaks for its own loop.

    `--kind ocr --drain` is step two of the `content` handover: it says the legacy OCR loop has no
    work left, which is what makes `PAPRA_INDEX_CONTENT_DELIVERY` safe to switch on. It says nothing
    about the title loop, and `--kind title --drain` says nothing about the delivery flag — so the
    sentence has to name the loop it read, the flag that reading covers, and the flag it does not,
    or a person who drained one queue switches both.
    """
    assert run_jobs(None, cli_args(kind="ocr", drain=True), store=store) == 0

    out = capsys.readouterr().out
    assert "safe to switch" in out and "PAPRA_INDEX_CONTENT_DELIVERY" in out
    assert "`ocr` loop" in out and "`title`" in out and "separate reading" in out

    capsys.readouterr()
    assert run_jobs(None, cli_args(kind="title", drain=True), store=store) == 0
    out = capsys.readouterr().out
    assert "PAPRA_LEGACY_WORKERS" in out and "`title` loop" in out
    assert "says nothing about" in out                 # the delivery flag is not this reading

    capsys.readouterr()
    assert run_jobs(None, cli_args(drain=True), store=store) == 0
    out = capsys.readouterr().out
    assert "PAPRA_INDEX_CONTENT_DELIVERY" in out and "PAPRA_LEGACY_WORKERS" in out


# --- the store, the database, and the entry point ---------------------------------

def test_jobs_refuses_a_missing_database_before_opening_the_store(tmp_path, capsys,
                                                                  monkeypatch):
    """`Store()` creates the file it is handed, so the symptom would be erased by reading.

    An operator who points `DATA_DIR` at the wrong directory came here to find out why nothing
    is running. Creating an empty database on the way past would turn "wrong path" into "empty
    queue" — the one wrong answer that looks like a right one — so this refuses before the
    store is constructed, exactly as `events` does.
    """
    from papra_suite.__main__ import main

    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "wrong-dir"))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "jobs"])

    assert main() == EXIT_REFUSED
    assert "database" in capsys.readouterr().err
    assert not (tmp_path / "wrong-dir").exists()


def test_jobs_never_migrates_the_store_it_only_reads(tmp_path, capsys, monkeypatch):
    """Curiosity must not be allowed to perform a schema change.

    `check` and `serve` own the schema, so a store from an older build is a stated condition
    with the command that fixes it — and the pragma is the proof this one did not migrate on its
    way to saying so.
    """
    from papra_suite.__main__ import main

    db = tmp_path / "state.db"
    Store(db).close()
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "jobs"])

    assert main() == EXIT_REFUSED
    err = capsys.readouterr().err
    assert "check" in err and "jobs" in err
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    finally:
        conn.close()


def test_a_locked_store_is_a_sentence_and_not_a_traceback(store, capsys, monkeypatch):
    """The queue is read *during* an incident, often while a worker is mid-write.

    A `database is locked` there costs the operator the reading they came for if it surfaces as
    a traceback, and buries the reason. "This will not answer" is a different report from "the
    queue is quiet", and it must not be counted as one of those.
    """
    def locked(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    # Both counting reads: the listing takes its total from `count_jobs`, the handover gate from
    # `count_jobs_by_status`, and neither may answer "quiet" when the table would not answer.
    monkeypatch.setattr(Store, "count_jobs", locked)
    monkeypatch.setattr(Store, "count_jobs_by_status", locked)

    assert run_jobs(None, cli_args(kind="ocr", drain=True), store=store) == EXIT_REFUSED
    assert "refused" in capsys.readouterr().err


def test_an_interrupted_command_is_a_sentence_with_a_non_zero_exit(store, capsys,
                                                                   monkeypatch):
    """Ctrl-C at a terminal is normal, and the README promises it will not be a traceback."""
    def interrupting(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(Store, "list_jobs", interrupting)

    assert run_jobs(None, cli_args(), store=store) == EXIT_INCOMPLETE
    assert "interrupted" in capsys.readouterr().err


def test_the_cli_is_wired_into_the_entry_point(tmp_path, monkeypatch):
    """`papra-suite jobs ...` has to reach the module, not merely import cleanly.

    Parser registration, the `args.command` branch and the command body are three places that
    can each be wrong on their own, and a mutation that deletes one leaves the module
    importable and the documented command unreachable.
    """
    import papra_suite.jobs_cli as jobs_cli_module
    from papra_suite.__main__ import main

    seen = []
    monkeypatch.setattr(jobs_cli_module, "run_jobs",
                        lambda config, args, **kw: (seen.append(args), 0)[1])
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "jobs", "--kind", "ocr", "--drain"])

    assert main() == 0
    assert seen and seen[0].drain is True and seen[0].kind == "ocr"


def test_the_entry_point_lists_a_real_store_end_to_end(tmp_path, monkeypatch, capsys):
    """The whole chain unmocked, because the README tells people to type this.

    Parser, config, store opened from `DATA_DIR`, the row `--reset` then names: every citation
    this task makes is only true if the command parses and works in the build that ships it.
    """
    from papra_suite.__main__ import main

    db = tmp_path / "state.db"
    seeded = Store(db)
    seeded.migrate()
    job_id = seeded.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-1",
                                content_hash=H, payload={})
    seeded.mark_job_failed(job_id, "kreuzakt queue error: extraction died", max_attempts=1)
    seeded.close()
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "jobs", "--status", "failed"])

    assert main() == 0
    out = capsys.readouterr().out
    assert "doc-1" in out and "extraction died" in out


def test_the_entry_point_resets_one_row_end_to_end(tmp_path, monkeypatch, capsys):
    """`papra-suite jobs --reset JOB_ID`, typed, against a real database.

    This is the command the worker's ERROR line, the store docstrings and the runbook all point
    at. Asserted through `main()` because a citation that merely parses is the failure mode this
    project has a standing rule about.
    """
    from papra_suite.__main__ import main

    db = tmp_path / "state.db"
    seeded = Store(db)
    seeded.migrate()
    job_id = seeded.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-1",
                                content_hash=H, payload={})
    seeded.claim_next_job("index")
    seeded.mark_job_failed(job_id, "gone", max_attempts=1)
    seeded.close()
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "jobs", "--reset", job_id])

    assert main() == 0
    assert "pending" in capsys.readouterr().out
    reopened = Store(db)
    try:
        assert reopened.get_job(job_id).status == "pending"
    finally:
        reopened.close()


def test_the_parses_surface_is_what_the_readme_prints():
    """Flags and their spellings, checked on the parser rather than in prose.

    `--kind`, `--status`, `--document-id` and `--limit` are listings; `--reset` and `--drain`
    are the two acts; and an act combined with a listing filter is exactly the ambiguity
    argparse is there to refuse.
    """
    args = parse_cli(["--kind", "ocr", "--status", "failed", "--limit", "5"])
    assert (args.kind, args.status, args.limit) == ("ocr", "failed", 5)
    assert parse_cli(["--document-id", "doc-1", "--json"]).document_id == "doc-1"
    assert parse_cli(["--reset", "abc"]).reset == "abc"
    assert parse_cli(["--kind", "ocr", "--drain"]).drain is True
    assert parse_cli([]).limit == 50 and parse_cli([]).json is False

    for argv in (["--reset", "abc", "--drain"], ["--status", "bogus"],
                 ["--limit", "five"]):
        with pytest.raises(SystemExit):
            parse_cli(argv)


def test_every_papra_suite_command_the_shipped_code_names_is_one_that_parses():
    """A message that names a subcommand is a promise about this binary.

    Task 7 pinned this failure from the other side: the plan's handover message cited a
    `papra-suite jobs` command this branch had not shipped, which reads to an operator at 23:00 as a
    broken installation rather than as a stale message, so the test asserted the raw-SQL reading
    instead. Any log line or docstring in any module can reintroduce it, and the standing rule is
    "never cite a subcommand you did not implement".

    Scope, stated plainly: a *name* check against the `add_subparser` helpers `main()` calls,
    so it catches a command that no module registers. It says nothing about the flags printed beside
    a name — that is `test_the_shipped_entry_point_accepts_every_invocation_the_runbook_prints`,
    which runs the real binary.
    """
    import argparse
    import importlib
    import pkgutil
    import re
    from pathlib import Path

    import papra_suite

    parser = argparse.ArgumentParser(prog="papra-suite")
    sub = parser.add_subparsers(dest="command", required=True)
    modules = []
    for info in pkgutil.iter_modules(papra_suite.__path__):
        module = importlib.import_module(f"papra_suite.{info.name}")
        modules.append(module)
        add = getattr(module, "add_subparser", None)
        if add is not None:
            add(sub)

    named: set[str] = set()
    for module in modules:
        text = Path(module.__file__).read_text(encoding="utf-8")
        named.update(re.findall(r"papra-suite ([a-z][a-z-]*) +--?[a-z]", text))

    assert named, "the scan matched nothing, which is not the same as nothing being wrong"
    unregistered = sorted(named - set(sub.choices))
    assert not unregistered, f"cited but not registered: {unregistered}"


def test_drain_counts_a_queue_bigger_than_any_listing_cap(store, capsys):
    """The claim "no cap can make a busy queue look quiet" is only true in SQL.

    A count taken from `list_jobs` would answer a 120-row `ocr` backlog with "0 pending" once the
    listing stopped at 100 — and that reading is the one an operator turns into
    `PAPRA_INDEX_CONTENT_DELIVERY=true`.
    """
    for index in range(120):
        store.enqueue_job(kind="ocr", papra_org_id=ORG, document_id=f"doc-{index}",
                          content_hash=H, payload={})

    assert run_jobs(None, cli_args(kind="ocr", drain=True), store=store) == EXIT_INCOMPLETE

    out = capsys.readouterr().out
    assert "120 pending" in out and "cannot drain" in out
    assert store.count_jobs("ocr", "pending") == 120


def test_reset_job_refuses_a_running_row_at_the_write(store):
    """The guard that protects a worker has to be the one next to the UPDATE.

    `reset_job` refuses a reset that would collide with a live sibling; it says nothing about the
    named row itself being `running`, so a caller that checks first and writes second can still put
    a row back mid-flight. The command's refusal is for the operator; this is the one that holds
    when the two happen in the other order.
    """
    job_id = store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-1",
                               content_hash=H, payload={})
    store.claim_next_job("index")

    with pytest.raises(ValueError, match="running"):
        store.reset_job(job_id)

    unchanged = store.get_job(job_id)
    assert unchanged.status == "running" and unchanged.error is None


def test_reset_refuses_a_row_a_second_connection_claimed_after_the_listing(tmp_path, capsys):
    """The race the CLI's own check cannot see, run on purpose with two connections to one file.

    One `Store` is the operator's command, the other a worker in another process: the listing said
    `pending`, the worker claimed the row, enter was already pressed. A reset landing here gives the
    document a second claimer while the first is still uploading, and the first worker's
    `mark_job_done` then settles work it no longer owns — so the refusal is the answer, and the row
    stays `running`.
    """
    path = tmp_path / "state.db"
    operator = Store(path)
    operator.migrate()
    operator.create_org(papra_org_id=ORG, name="Org A")
    worker = Store(path)
    try:
        job_id = operator.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-race",
                                      content_hash=H, payload={})
        assert operator.get_job(job_id).status == "pending"
        assert worker.claim_next_job("index").id == job_id

        assert run_jobs(None, cli_args(reset=job_id), store=operator) == EXIT_REFUSED

        assert "running" in capsys.readouterr().err
        assert operator.get_job(job_id).status == "running"
    finally:
        worker.close()
        operator.close()


def test_a_row_claimed_between_the_read_and_the_write_is_refused_in_words(store, capsys):
    """The window between "it is pending" and the UPDATE, opened on purpose.

    The proxy is a worker that claims the row after the command has read it: the check in the
    command passes on a stale read, so only a guard next to the write can stop it — and when that
    guard raises, the operator gets a sentence and exit 2, not a traceback about `ValueError`.
    """
    job_id = store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-window",
                               content_hash=H, payload={})

    class ClaimedUnderneath:
        def __init__(self, inner):
            self._inner = inner
            self._claimed = False

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def get_job(self, job_id):
            job = self._inner.get_job(job_id)
            if job is not None and not self._claimed:
                self._claimed = True
                self._inner.claim_next_job(job.kind)      # a worker wins the race
            return job

    assert run_jobs(None, cli_args(reset=job_id), store=ClaimedUnderneath(store)) == EXIT_REFUSED

    err = capsys.readouterr().err
    assert "running" in err and "Traceback" not in err
    assert store.get_job(job_id).status == "running"


def test_the_reset_refusals_travel_on_types_and_not_on_wording(store, capsys, monkeypatch):
    """A copy edit in the store must not move an exit code an operator acts on.

    `--reset` answers a row a worker is holding with `2` and a row whose work is queued elsewhere
    with `1`: "leave it alone" and "the work is queued, nothing changed" are different instructions,
    and the exit code is what a script reads. Matching the message for the words `running` and
    `already queued` to tell them apart makes rewording a store sentence a wrong exit code here, so
    the store raises one typed error each — and this test raises them with messages that match
    neither phrase.
    """
    from papra_suite.store import JobAlreadyQueuedError, JobIsRunningError

    job_id = seed_job(store, kind="index", document_id="doc-1")

    def refused(error):
        def raiser(_job_id):
            raise error

        return raiser

    monkeypatch.setattr(store, "reset_job",
                        refused(JobIsRunningError("nothing here matches the old phrase")))
    assert run_jobs(None, cli_args(reset=job_id), store=store) == EXIT_REFUSED
    assert "running" in capsys.readouterr().err

    monkeypatch.setattr(store, "reset_job",
                        refused(JobAlreadyQueuedError("nothing here matches either")))
    assert run_jobs(None, cli_args(reset=job_id), store=store) == EXIT_INCOMPLETE
    assert "Traceback" not in capsys.readouterr().err


def test_count_jobs_by_status_answers_every_status_in_one_read(store):
    """One GROUP BY over the scope, so a caller can compare statuses without reading twice.

    `--drain` needs `pending` and `running` together; asked for them separately, SQLite can answer
    from two different instants and a row that moves between them (a worker restart puts `running`
    back to `pending`) can make both come back 0 above a queue that has work.
    """
    for index in range(2):
        store.enqueue_job(kind="index", papra_org_id=ORG, document_id=f"doc-p{index}",
                          content_hash=H, payload={})
    store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-f",
                      content_hash=H, payload={})
    store.enqueue_job(kind="ocr", papra_org_id=ORG, document_id="doc-o",
                      content_hash=H, payload={})
    store.claim_next_job("index")
    store.mark_job_failed(store.list_jobs(kind="ocr")[0].id, "gone", max_attempts=1)

    assert store.count_jobs_by_status() == {"pending": 2, "running": 1, "failed": 1}
    assert store.count_jobs_by_status("index") == {"pending": 2, "running": 1}
    assert store.count_jobs_by_status("ocr") == {"failed": 1}
    assert store.count_jobs_by_status("title") == {}
    # `claim_next_job` takes the oldest claimable row, which is `doc-p0`, so the untouched
    # document is the one that still answers `pending`.
    assert store.count_jobs_by_status(document_id="doc-p0") == {"running": 1}
    assert store.count_jobs_by_status(document_id="doc-f") == {"pending": 1}


def test_drain_reads_its_two_numbers_from_one_snapshot(store, capsys):
    """The proxy forbids per-status counting, which makes one read the contract, not a habit.

    `--drain` is the step whose wrong answer is two writers on Papra's `content` column, so it may
    not assemble its verdict out of two readings of the queue.
    """
    store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-live",
                      content_hash=H, payload={})
    store.enqueue_job(kind="index", papra_org_id=ORG, document_id="doc-run",
                      content_hash=H, payload={})
    store.claim_next_job("index")

    class OneRead:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def count_jobs(self, *args, **kwargs):
            raise AssertionError("drain counted the queue status by status")

    assert run_jobs(None, cli_args(drain=True), store=OneRead(store)) == EXIT_INCOMPLETE

    out = capsys.readouterr().out
    assert "1 pending, 1 running" in out


#: Every invocation the README prints or a log line names for the two commands in this task.
CITED_INVOCATIONS = [
    ["jobs"], ["jobs", "--reset", "JOB_ID"], ["jobs", "--drain"],
    ["jobs", "--kind", "ocr", "--drain"], ["jobs", "--kind", "title", "--drain"],
    ["jobs", "--kind", "index", "--drain"],
    ["jobs", "--kind", "index", "--status", "failed"],
    ["jobs", "--document-id", "DOC", "--json"],
    ["backfill"], ["backfill", "--dry-run"], ["backfill", "--limit", "20"],
    ["backfill", "--org-id", "ORG", "--limit", "5"], ["backfill", "--all-stale"],
]


@pytest.mark.parametrize("argv", CITED_INVOCATIONS, ids=" ".join)
def test_the_shipped_entry_point_accepts_every_invocation_the_runbook_prints(argv):
    """The README's code blocks are what an operator types, so parse them with the real binary.

    The scan test above proves a cited command name is registered by some module of this package;
    only `main()` proves the flags printed beside it exist. What that catches is a runbook line that
    stops with `unrecognized arguments` in front of somebody in the middle of an incident, which is
    the same class of stale message Task 7 pinned from the plan.
    """
    root = Path(store_module.__file__).resolve().parents[1]
    proc = subprocess.run([sys.executable, "-m", "papra_suite", *argv, "--help"],
                          cwd=root, capture_output=True, text=True, timeout=120)

    assert proc.returncode == 0, f"{' '.join(argv)}: {proc.stderr}"

