"""Task 11: rebuilding the index from Papra's bytes, and the operator commands around it.

Two things shape this file, and both are about what a rebuild is *for*.

**A rebuild re-derives; it never repairs in place.** The Kreuzakt ids an operator is
rebuilding away from are integers in an index that has been thrown away, and a rebuilt
Kreuzakt restarts them at 1 — so the previous ids are not stale facts worth consulting,
they are facts about a different corpus. Every path here therefore goes through the
intake queue (`enqueue_index_job`) with Papra's own bytes behind it, and nothing in here
copies an id forward, writes to Papra, or trusts `document_seen` to say what is indexed.

**The work has to be schedulable, and it has to finish.** Migration 004 is what makes the
first half true — a terminal job row no longer absorbs the request — and the second half
is the reason these walks page by keyset and stop loudly instead of half-finishing: the
generation is bumped *before* the work is queued, so a walk that dies halfway leaves the
rest of the corpus stale, seen, and invisible to the sweep. A rebuild that cannot be
resumed is a rebuild that cannot be interrupted.

The doubles come from `tests/test_index_job.py`: `FakePapra` counts every write, which is
how "this task does not touch Papra" is a fact about the run rather than a claim, and
`FakeKreuzakt` is the real client behind a scripted transport.
"""

import argparse
import hashlib
import logging
import sys
from types import SimpleNamespace

import pytest

import papra_suite.store as store_module
from papra_suite.config import load_config
from papra_suite.indexing import (
    CLEANUP_MAX_ATTEMPTS,
    MARKS_ROLLING_SETTING,
    IndexFailed,
    run_index_job,
)
from papra_suite.kreuzakt_client import KreuzaktError
from papra_suite.ledger_cli import RebuildResult, add_subparser, reconcile_generation, run_ledger
from papra_suite.papra_client import PapraError
from papra_suite.pipeline import OCR_MAX_DOCUMENT_BYTES, enqueue_index_job
from papra_suite.store import DELIVERY_MAX_FAILURES, Store
from tests.test_config import BASE_ENV
from tests.test_index_job import DOC_ID, HASH, NAME, ORG, FakeKreuzakt, FakePapra

PDF = "application/pdf"


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=ORG, name="Org 1")
    yield store
    store.close()


@pytest.fixture
async def fake_kz():
    fake = FakeKreuzakt()
    yield fake
    await fake.aclose()


def indexable(store, *, org=ORG, doc=DOC_ID, kreuzakt_id=7, file_hash=HASH,
              name=NAME):
    """A reference the index holds, bound under the generation current when it was made."""
    store.ledger_upsert_reference(papra_org_id=org, papra_document_id=doc,
                                  file_hash=file_hash, papra_original_name=name)
    store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=doc,
                                 kreuzakt_id=kreuzakt_id,
                                 generation=store.get_generation())
    return store.ledger_get(org, doc)


def seen_only(store, doc, *, org=ORG, mime=PDF, name=None, hash_=HASH):
    """A document Papra holds and this suite has listed, with no reference behind it.

    The pre-handover mail path leaves exactly this: `documents` says we ingested it,
    `index_ledger` has never heard of it, and every eligibility rule derives from the
    ledger, so no count, sweep or rebuild can see the gap.
    """
    store.record_document_seen(papra_org_id=org, papra_document_id=doc,
                               name=name or f"{doc}.pdf", mime=mime, ingested_via="mail")
    papra_record = {"id": doc, "organizationId": org, "name": name or f"{doc}.pdf",
                    "mimeType": mime, "content": "", "originalSha256Hash": hash_,
                    "isDeleted": False, "deletedAt": None}
    return papra_record


def with_papra(records):
    """A Papra double holding exactly `records`, keyed by (org, id)."""
    papra = FakePapra()
    papra.documents = {(r["organizationId"], r["id"]): dict(r) for r in records}
    return papra


def jobs_for(store, kind="index"):
    return [job for job in store.list_jobs(limit=500) if job.kind == kind]


# --- re-queueing what the ledger already knows ----------------------------------------

async def test_a_generation_bump_makes_the_corpus_queueable_again(store):
    """The premise of the whole task, asserted as scheduling rather than as eligibility.

    `ledger_needs_index` said these rows were work all along; what the operator could not
    do was ask for it. Before migration 004 the enqueue returned the spent row and the
    rebuild queued nothing while reporting success.
    """
    for doc, kreuzakt_id in (("doc-1", 1), ("doc-2", 2)):
        indexable(store, doc=doc, kreuzakt_id=kreuzakt_id)
        # Drained to `done` the way a worker drains one: this is the state that used to be
        # terminal for the queue, and the enqueue that follows is the request it used to swallow.
        store.mark_job_done(enqueue_index_job(store, papra_org_id=ORG, document_id=doc,
                                             file_hash=HASH, name=NAME, mime=PDF,
                                             ingested_via="sweep"))
    store.bump_generation()

    result = await reconcile_generation(store)

    assert isinstance(result, RebuildResult)
    assert (result.generation, result.stale, result.requeued) == (2, 2, 2)
    pending = [job for job in jobs_for(store) if job.status == "pending"]
    assert len(pending) == 2 and len(jobs_for(store)) == 4    # the spent rows are history, not gone


async def test_reconcile_carries_the_ledgers_own_hash_and_never_reads_papra(store):
    """The stale population is served from the ledger, because the ledger has the hash.

    Reading Papra for every reference would put a few hundred requests on the record on a
    rebuild that does not need them: `run_index_job` downloads and verifies the bytes
    against this hash anyway, so a hash read here would be a guess the job then checks. It
    also keeps the walk working while Papra is unhappy, which is often the reason for the
    rebuild in the first place.
    """
    papra = with_papra([seen_only(store, DOC_ID)])
    indexable(store, kreuzakt_id=7)
    store.bump_generation()

    result = await reconcile_generation(store, papra=papra)

    # The work has to be there, or "nothing was written" is true of a pass that did nothing.
    assert result.requeued == 1 and result.enrolled == 0
    assert papra.get_calls == 0 and papra.download_calls == 0
    assert papra.patched == [] and papra.patch_calls == 0
    job = jobs_for(store)[0]
    assert job.content_hash == HASH and job.papra_org_id == ORG


async def test_reconcile_writes_nothing_to_papra(store):
    """Z8, asserted on the fake that records every attempted write."""
    papra = with_papra([seen_only(store, DOC_ID)])
    indexable(store, kreuzakt_id=7)
    store.bump_generation()

    await reconcile_generation(store, papra=papra)

    assert papra.patched == [] and papra.patch_calls == 0


async def test_re_enrolment_keeps_the_rename_baseline_and_the_protection(store):
    """The plan's own assertion: asking for the work again is not a new first sight.

    `papra_original_name` is the baseline a rename is measured against and the protection
    flag is a person's decision. Both live on the reference, and the intake path is
    idempotent about them — which is only worth pinning because the rebuild goes through
    that path rather than around it.
    """
    indexable(store, kreuzakt_id=7, name="scan-0001.pdf")
    store.ledger_protect(papra_org_id=ORG, papra_document_id=DOC_ID, field="name")
    store.bump_generation()

    await reconcile_generation(store)

    ref = store.ledger_get(ORG, DOC_ID)
    assert ref.papra_original_name == "scan-0001.pdf"
    assert ref.name_protected is True
    # ...and the binding it is protecting is still the destroyed one, not quietly cleared:
    # the job that runs next decides what replaces it, under the generation fence.
    assert ref.kreuzakt_id == 7 and ref.delivered_generation == 1


async def test_reconcile_does_not_refill_the_legacy_ocr_queue(store, monkeypatch):
    """A rebuild asks the index to re-extract; it does not order a VLM pass per document.

    Reusing the intake path is the plan's rule, and with the delivery gate shut that path
    also fills the legacy `ocr` queue — which before migration 004 was harmless because the
    spent rows absorbed the insert. Now it would be one paid extraction per document in the
    corpus, for text Papra already holds. So the rebuild asks for the index job only, and
    this test pins that the rest of the intake path (reference, job, seen record, in that
    order) is untouched.
    """
    store.set_setting("legacy_workers", "1")
    store.set_setting("index_content_delivery", "0")
    indexable(store, kreuzakt_id=7)
    store.bump_generation()

    await reconcile_generation(store)

    assert len(jobs_for(store, "index")) == 1
    assert jobs_for(store, "ocr") == []


async def test_reconcile_walks_every_page_of_a_corpus_larger_than_a_page(store,
                                                                         monkeypatch):
    """A rebuild that stops at the page size would report the corpus rebuilt.

    Enqueuing does not make a reference leave `ledger_needs_index` — only a bound id does
    that — so this walk cannot re-ask for the first page and call it progress.
    """
    for index in range(7):
        indexable(store, doc=f"doc-{index}", kreuzakt_id=index + 1)
    store.bump_generation()
    calls = {"n": 0}
    real = store.ledger_needs_index

    def smaller_page(**kwargs):
        kwargs["limit"] = 2
        calls["n"] += 1
        return real(**kwargs)

    monkeypatch.setattr(store, "ledger_needs_index", smaller_page)

    result = await reconcile_generation(store)

    assert result.stale == 7 and result.requeued == 7
    assert calls["n"] >= 4                        # 2 + 2 + 2 + the page that ends the walk
    assert len(jobs_for(store)) == 7


async def test_reconcile_refuses_to_finish_a_walk_the_generation_moved_under(
        store, monkeypatch):
    """Two rebuilds at once must not both claim the corpus.

    A bump mid-walk means someone else started a newer index. The rows left unqueued are
    stale under both generations, so nothing is lost by stopping — but a pass that walked on
    and returned `complete` would have told the operator the older rebuild covered a corpus
    that the newer one has already re-opened.
    """
    for index in range(4):
        indexable(store, doc=f"doc-{index}", kreuzakt_id=index + 1)
    store.bump_generation()
    real = store.ledger_needs_index
    state = {"n": 0}

    def bumping_page(**kwargs):
        state["n"] += 1
        if state["n"] == 2:
            store.bump_generation()               # a second operator, mid-walk
        kwargs["limit"] = 2
        return real(**kwargs)

    monkeypatch.setattr(store, "ledger_needs_index", bumping_page)

    result = await reconcile_generation(store)

    assert result.aborted is not None and "generation" in result.aborted
    assert result.complete is False
    assert result.requeued == 2                   # stopped at the page it noticed on
    assert result.generation == 2                 # the generation it scheduled against


# --- resuming a rebuild that stopped --------------------------------------------------

async def test_a_stopped_rebuild_resumes_on_the_same_generation(store, monkeypatch):
    """The property the bump-before-queueing costs, and pays for.

    Bumping first is what makes every stale id worthless at once, and the price is that the
    corpus is stale-and-seen until the queueing finishes: the sweep will not rescue those
    rows, because it skips documents it has seen. So the queueing has to be re-runnable
    without another bump, or an interrupted rebuild is a half-destroyed index.
    """
    for index in range(4):
        indexable(store, doc=f"doc-{index}", kreuzakt_id=index + 1)
    store.bump_generation()
    real_enqueue = store.enqueue_job
    state = {"n": 0}

    def enqueue_then_die(**kwargs):
        state["n"] += 1
        if state["n"] == 3:
            raise KeyboardInterrupt("the operator pressed ctrl-c")
        return real_enqueue(**kwargs)

    monkeypatch.setattr(store, "enqueue_job", enqueue_then_die)
    with pytest.raises(KeyboardInterrupt):
        await reconcile_generation(store)
    monkeypatch.undo()

    interrupted = len(jobs_for(store))
    assert interrupted == 2

    resumed = await reconcile_generation(store)

    assert resumed.generation == 2 == store.get_generation()   # no second bump
    # Every stale reference is asked for again, including the two already queued: the live rows
    # absorb those requests rather than duplicating them, which is the other half of what makes
    # a resume safe — the count says "four documents are queued", the table says "four jobs".
    assert (resumed.stale, resumed.requeued) == (4, 4)
    assert len(jobs_for(store)) == 4
    assert resumed.complete is True


# --- the population that has no ledger row (Z6) ---------------------------------------

async def test_a_seen_document_with_no_reference_is_enrolled_and_becomes_resolvable(
        store):
    """The count that used to be impossible, made reachable rather than merely reported.

    A document marked seen with no reference is invisible to every eligibility rule, every
    count and the sweep, all of which read the ledger. Enrolling it through the intake path
    has to end with the document resolvable through the authorization boundary — a row in a
    table is not the fix, a binding that survives `ledger_resolve` is.
    """
    record = seen_only(store, "doc-email-1", hash_="cd" * 32)
    papra = with_papra([record])
    assert store.ledger_stats()["unledgered"] == 1

    result = await reconcile_generation(store, papra=papra)

    assert (result.enrolled, result.unledgered) == (1, 0)
    ref = store.ledger_get(ORG, "doc-email-1")
    assert ref is not None and ref.file_hash == "cd" * 32
    assert ref.papra_original_name == "doc-email-1.pdf"   # Papra's name, first sight
    job = jobs_for(store)[0]
    assert job.document_id == "doc-email-1" and job.content_hash == "cd" * 32
    assert store.ledger_stats()["unledgered"] == 0


async def test_enrolment_uses_papras_hash_because_the_seen_table_stores_none(store):
    """The hash on the job must be Papra's, or the job fails permanently on download.

    `run_index_job` verifies the bytes it downloads against the job's content hash. A hash
    invented here — or a stale one read out of `documents`, which never stored one — becomes
    a permanent failure with a message about the document moving. When Papra's record
    carries no hash, the fallback is the sweep's: download, measure, hash, and refuse the
    document rather than guess at it.
    """
    no_hash = seen_only(store, "doc-no-hash")
    no_hash.pop("originalSha256Hash")
    papra = with_papra([no_hash])
    papra.body = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\ntrailer\n%%EOF"
    expected = hashlib.sha256(papra.body).hexdigest()

    result = await reconcile_generation(store, papra=papra)

    assert result.enrolled == 1
    assert papra.download_calls == 1
    assert jobs_for(store)[0].content_hash == expected


async def test_reconcile_counts_what_it_cannot_enrol_when_papra_is_absent(
        store, caplog):
    """No Papra client means no enrolment, and that has to be shouted, not defaulted.

    The rebuild could have taken the plan's two-argument signature and left the unledgered
    population quietly outside the index forever — which is the exact failure this task was
    written to close. Without a client it still counts them, still queues the stale
    references, and refuses to call itself complete.
    """
    seen_only(store, "doc-email-1")
    indexable(store, doc="doc-stale", kreuzakt_id=9)
    store.bump_generation()
    caplog.set_level(logging.ERROR, logger="papra_suite.ledger_cli")

    result = await reconcile_generation(store)

    assert result.requeued == 1 and result.enrolled == 0
    assert result.unledgered == 1
    assert result.complete is False
    assert "unledgered" in caplog.text and "papra" in caplog.text.lower()


async def test_enrolment_leaves_what_the_index_cannot_hold_out_and_names_it(store):
    """Z6's other half: zero is not the only acceptable remainder, but a remainder needs a name.

    A PNG is outside the index by design and a 400 MB scan is outside the resource limit
    that made it invisible in the first place; neither is a bug, and neither may be counted
    as enrolled to make the number tidy.
    """
    png = seen_only(store, "doc-photo", mime="image/png", name="holiday.png")
    huge = seen_only(store, "doc-huge")
    huge["originalSize"] = OCR_MAX_DOCUMENT_BYTES + 1
    papra = with_papra([png, huge])

    result = await reconcile_generation(store, papra=papra)

    assert (result.enrolled, result.skipped_non_pdf, result.skipped_oversized) \
        == (0, 1, 1)
    # The oversized document was rejected on the size Papra declared, not by downloading it.
    assert papra.download_calls == 0
    assert store.ledger_get(ORG, "doc-photo") is None
    assert store.ledger_get(ORG, "doc-huge") is None


async def test_enrolment_leaves_a_deleted_document_to_the_deletion_pass(store):
    """Papra's trash is not index work, and enrolling it would be resurrecting it."""
    gone = seen_only(store, "doc-trashed")
    gone["isDeleted"] = True
    gone["deletedAt"] = "2026-11-19T09:00:00+00:00"
    papra = with_papra([gone])

    result = await reconcile_generation(store, papra=papra)

    assert (result.enrolled, result.skipped_deleted) == (0, 1)
    assert store.ledger_get(ORG, "doc-trashed") is None


async def test_a_papra_that_will_not_answer_leaves_the_walk_incomplete(store):
    """A candidate not read is not a candidate processed.

    Transient failures keep the document in the unledgered population and make the walk
    report itself unfinished, so the operator re-runs it instead of recording a rebuild that
    skipped the documents Papra was temporarily quiet about.
    """
    seen_only(store, "doc-flaky")
    papra = with_papra([seen_only(store, "doc-flaky")])
    papra.raise_on_get = PapraError("GET document failed: 503", 503)

    result = await reconcile_generation(store, papra=papra)

    assert (result.enrolled, result.read_errors, result.complete) == (0, 1, False)
    assert result.unledgered == 1

    papra.raise_on_get = None
    again = await reconcile_generation(store, papra=papra)
    assert (again.enrolled, again.complete) == (1, True)


async def test_enrolment_survives_a_corpus_bigger_than_one_page(store):
    """Enrolling takes rows out of the walk that found them.

    Keyset pagination is the only shape that works here: enrol three documents from a page
    of three and an offset walk starts past the ones that moved up, skipping documents that
    then stay outside the index with a green result printed above them.
    """
    records = [seen_only(store, f"doc-{index}") for index in range(5)]
    papra = with_papra(records)
    real = store.documents_without_reference
    pages = {"n": 0}

    def smaller(**kwargs):
        pages["n"] += 1
        kwargs["limit"] = 2
        return real(**kwargs)

    original = store.documents_without_reference
    store.documents_without_reference = smaller
    try:
        result = await reconcile_generation(store, papra=papra)
    finally:
        store.documents_without_reference = original

    assert result.enrolled == 5 and result.unledgered == 0
    assert pages["n"] >= 4


# --- the generation fence on what the rebuild sets up ---------------------------------

async def test_a_generation_bump_mid_rebuild_rejects_the_binding_from_the_old_index(
        store, fake_kz):
    """The fence that makes a rebuild's re-derived id safe, seen from the rebuild's side.

    The requeued job binds under the generation the queueing pass saw. If a newer rebuild lands
    while that job is uploading, the id it is holding came from an index that has since been
    thrown away — and binding it under the new generation would point a live reference at an
    integer that now names something else. That is the one failure mode this whole design exists
    to prevent, and it is why the bind is a compare-and-swap on the generation rather than an
    assignment: the job refuses, and the row goes back to being work.
    """
    indexable(store, kreuzakt_id=7)
    store.bump_generation()
    await reconcile_generation(store)
    job = store.claim_next_job("index")
    assert job is not None and job.kind == "index"

    fake_kz.on_wait = lambda: store.bump_generation()      # a second rebuild lands mid-flight
    papra = with_papra([seen_only(store, DOC_ID)])

    with pytest.raises(IndexFailed, match="generation changed mid-flight"):
        await run_index_job(fake_kz, store, job, papra=papra, poll_s=0, wait_timeout_s=5)

    after = store.ledger_get(ORG, DOC_ID)
    # Untouched: still the pre-rebuild epoch and the id that belonged to it. The job did not
    # "fix" anything, which is the correct outcome — an id from a destroyed index is not a fact
    # about the new one, and the row staying stale is what makes the newer rebuild pick it up.
    assert after.delivered_generation == 1 and after.kreuzakt_id == 7
    assert after.delivered_generation != store.get_generation()
    # The refusal is not a verdict of "done": the document is work again for the newer index.
    assert [ref.papra_document_id
            for ref in store.ledger_needs_index(generation=store.get_generation())] == [DOC_ID]


# --- the operator commands -----------------------------------------------------------

def cli_args(**over):
    base = dict(command="ledger", status=False, reindex_all=False, requeue=False,
                cleanup=False, clear_marks=False, dry_run=False, yes_i_mean_it=False)
    return SimpleNamespace(**{**base, **over})


def parse_cli(argv):
    """Parse through the real parser, so the flag surface itself is under test."""
    parser = argparse.ArgumentParser(prog="papra-suite")
    sub = parser.add_subparsers(dest="command", required=True)
    add_subparser(sub)
    return sub.choices["ledger"].parse_args(argv)


def configuration(tmp_path, **env):
    return load_config({**BASE_ENV, "DATA_DIR": str(tmp_path), **env})


class ScriptedHealth:
    """A Kreuzakt whose only endpoint is the one the rebuild's gate reads, answering as told.

    Three refusals and one permission are modelled here rather than by breaking `FakeKreuzakt`'s
    transport, because they are different events with different causes: a transport failure, a
    service that replies "I am not ok", and a healthy index. The middle one is what a
    `status`-shaped check exists for — a client that raises only on HTTP errors would let a
    degraded body through the gate as though it were a healthy one — and `health_calls` is how the
    test says the gate ran *after* the confirmation rather than before it.
    """

    def __init__(self, *, raise_error=False, status="unreachable"):
        self.raise_error = raise_error
        self.status = status
        self.health_calls = 0

    async def health(self):
        self.health_calls += 1
        if self.raise_error:
            raise KreuzaktError("GET /api/health failed: connection refused")
        return {"status": self.status}


class ScriptedDeleter(ScriptedHealth):
    """A Kreuzakt that records deletes instead of performing them.

    `--cleanup` is the only command in this file that destroys something, so the assertion that
    matters most is the one about which ids reached the index — and `FakeKreuzakt`'s transport is
    the wrong tool for reading that off: it needs an event loop of its own to close, and the
    command owns the loop it runs in.
    """

    def __init__(self, *, fail=False, **over):
        super().__init__(**over)
        self.deleted: list[int] = []
        # A 500 rather than a transport failure on purpose: `run_cleanup` treats "the index is not
        # answering" as a reason to stop without spending anyone's retries, and the property here
        # is about the retry allowance, which only an actual refusal charges.
        self.fail = KreuzaktError("DELETE /api/documents failed: 500 upstream", 500) \
            if fail else None

    async def delete_document(self, document_id: int) -> None:
        if self.fail is not None:
            raise self.fail
        self.deleted.append(document_id)


def test_ledger_cli_parses_the_documented_surface():
    assert parse_cli(["--status"]).status is True
    # `--invalidate` is the name the runbook and the client's own docs use, `--reindex-all` is the
    # name that says what happens; both are one command, so they must not be two behaviours.
    assert parse_cli(["--invalidate"]).reindex_all is True
    assert parse_cli(["--reindex-all", "--yes-i-mean-it"]).reindex_all is True
    assert parse_cli(["--requeue"]).requeue is True
    assert parse_cli(["--cleanup"]).cleanup is True
    assert parse_cli(["--clear-marks", "--yes-i-mean-it"]).clear_marks is True
    with pytest.raises(SystemExit):
        parse_cli([])
    with pytest.raises(SystemExit):
        parse_cli(["--status", "--cleanup"])


def test_reindex_all_is_refused_while_kreuzakt_is_down(store, tmp_path, capsys):
    """The bump is a one-row UPDATE that reads as an outage while the index is not answering.

    Nothing here may change state: an operator who runs this against a dead index has to be able
    to run it again afterwards and get the rebuild they meant, rather than a corpus that is
    stale, marked seen, and queued into an index that was down when the queue was filled.
    """
    indexable(store, kreuzakt_id=7)

    code = run_ledger(configuration(tmp_path), cli_args(reindex_all=True, yes_i_mean_it=True),
                      store=store, papra=with_papra([]), kreuzakt=ScriptedHealth(raise_error=True))

    captured = capsys.readouterr()
    assert code == 2
    assert store.get_generation() == 1
    assert jobs_for(store) == []
    assert "unreachable" in captured.err


def test_a_health_body_that_is_not_ok_refuses_the_bump(store, tmp_path, capsys):
    indexable(store, kreuzakt_id=7)

    code = run_ledger(configuration(tmp_path), cli_args(reindex_all=True, yes_i_mean_it=True),
                      store=store, papra=with_papra([]),
                      kreuzakt=ScriptedHealth(status="degraded"))

    assert code == 2 and store.get_generation() == 1
    assert "degraded" in capsys.readouterr().err


def test_reindex_all_needs_a_second_thought(store, tmp_path, capsys):
    indexable(store, kreuzakt_id=7)
    healthy = ScriptedHealth(status="ok")

    code = run_ledger(configuration(tmp_path), cli_args(reindex_all=True),
                      store=store, papra=with_papra([]), kreuzakt=healthy)

    out = capsys.readouterr().out
    assert code == 1 and store.get_generation() == 1 and jobs_for(store) == []
    assert "--yes-i-mean-it" in out and "1 reference(s)" in out
    # the health gate runs after the confirmation, not before it: the refusal costs a request
    # and the confirmation costs nothing, so an unanswered index must not be what a person
    # discovers when they were only being asked to think about it
    assert healthy.health_calls == 0


def test_reindex_all_bumps_once_and_queues_the_corpus(store, tmp_path, capsys):
    indexable(store, kreuzakt_id=7)

    code = run_ledger(configuration(tmp_path), cli_args(reindex_all=True, yes_i_mean_it=True),
                      store=store, papra=with_papra([]), kreuzakt=FakeKreuzakt())

    out = capsys.readouterr().out
    assert code == 0 and store.get_generation() == 2
    assert len(jobs_for(store)) == 1
    assert "generation 2: 1 of 1 stale reference(s) queued" in out
    assert "left out of the index on purpose" in out


def test_reindex_all_reports_a_walk_it_could_not_finish(store, tmp_path, capsys):
    """The bump has already happened, so the only honest exit is "come back".

    Rolling the generation back would be worse: the ids really are worthless, because the operator
    asked for them to be. What the command owes instead is a non-zero exit and the instruction
    that actually works, which is to run it again.
    """
    seen_only(store, "doc-email-1")
    papra = with_papra([seen_only(store, "doc-email-1")])
    papra.raise_on_get = PapraError("GET document failed: 503", 503)

    code = run_ledger(configuration(tmp_path), cli_args(reindex_all=True, yes_i_mean_it=True),
                      store=store, papra=papra, kreuzakt=FakeKreuzakt())

    captured = capsys.readouterr()
    assert code == 1
    assert store.get_generation() == 2
    assert "run the command again" in captured.err
    assert "still unledgered: 1" in captured.out


def test_requeue_finishes_an_interrupted_rebuild_without_a_second_bump(store, tmp_path, capsys,
                                                                      monkeypatch):
    """The README's resume promise, tested through the command that has to keep it.

    `--reindex-all` bumps the generation and then queues, so an interruption leaves the corpus
    stale with only part of the work in the queue. Finishing it must not be another invalidation:
    that would throw away the bindings the first pass had already re-derived. `--requeue` asks the
    queue for what is still stale under the generation that is already current, and the live rows
    absorb the documents that were queued before the interruption.
    """
    for index in range(4):
        indexable(store, doc=f"doc-{index}", kreuzakt_id=index + 1)
    config = configuration(tmp_path)
    real_enqueue = store.enqueue_job
    state = {"n": 0}

    def enqueue_then_die(**kwargs):
        state["n"] += 1
        if state["n"] == 3:
            raise KeyboardInterrupt("the operator pressed ctrl-c")
        return real_enqueue(**kwargs)

    monkeypatch.setattr(store, "enqueue_job", enqueue_then_die)
    first = cli_args(reindex_all=True, yes_i_mean_it=True)
    with pytest.raises(KeyboardInterrupt):
        run_ledger(config, first, store=store, papra=with_papra([]),
                   kreuzakt=ScriptedHealth(status="ok"))
    monkeypatch.undo()
    assert store.get_generation() == 2 and len(jobs_for(store)) == 2

    code = run_ledger(config, cli_args(requeue=True), store=store, papra=with_papra([]))

    assert code == 0
    assert store.get_generation() == 2, "--requeue bumped the generation"
    assert len(jobs_for(store)) == 4, "the resume duplicated or lost work"
    assert "generation 2: 4 of 4 stale reference(s) queued" in capsys.readouterr().out


def test_requeue_says_so_when_nothing_is_stale(store, tmp_path, capsys):
    """An empty answer has to be an answer.

    This is the command an operator reaches for when work they asked for never ran, so "0 jobs
    queued" and "there was no work to queue" are different replies and only one of them means the
    queue is fine.
    """
    indexable(store, kreuzakt_id=7)

    code = run_ledger(configuration(tmp_path), cli_args(requeue=True), store=store,
                      papra=with_papra([]))

    out = capsys.readouterr().out
    assert code == 0 and jobs_for(store) == []
    assert "nothing is stale, so nothing was queued" in out and "1 of 1" in out


def test_requeue_dry_run_asks_for_nothing(store, tmp_path, capsys):
    indexable(store, kreuzakt_id=7)
    store.bump_generation()

    code = run_ledger(configuration(tmp_path), cli_args(requeue=True, dry_run=True), store=store,
                      papra=with_papra([]))

    out = capsys.readouterr().out
    assert code == 0 and jobs_for(store) == []
    assert "dry run: generation 2 would be left alone" in out and "1 stale reference(s)" in out


def test_status_names_the_debt_and_the_command_that_settles_it(store, tmp_path, capsys):
    """Every number a person can act on is printed beside the command that acts on it.

    The four populations are different debts with different cures, and a bare count invites the
    wrong fix: `unledgered` is one command, `cleanup failed` is another, `delivery blocked` is a
    conversation with Papra, and `stale` after a bump is not a debt at all but a queue.
    """
    indexable(store, doc="doc-stale", kreuzakt_id=1)
    seen_only(store, "doc-never-enrolled")
    blocked = indexable(store, doc="doc-blocked", kreuzakt_id=2)
    for _ in range(DELIVERY_MAX_FAILURES):
        store.ledger_record_delivery_failure(papra_org_id=ORG, papra_document_id="doc-blocked",
                                            expected_kreuzakt_id=blocked.kreuzakt_id,
                                            generation=blocked.delivered_generation,
                                            reason="papra said no")
    store.ledger_upsert_reference(papra_org_id=ORG, papra_document_id="doc-owed",
                                  file_hash="cd" * 32, papra_original_name="c.pdf")
    store.ledger_mark_deleted(papra_org_id=ORG, papra_document_id="doc-owed")
    for _ in range(CLEANUP_MAX_ATTEMPTS):
        store.ledger_cleanup_result(papra_org_id=ORG, papra_document_id="doc-owed", ok=False,
                                    reason="kreuzakt is not answering")
    store.bump_generation()          # the rebuild that made the bound rows backlog

    code = run_ledger(configuration(tmp_path), cli_args(status=True), store=store)

    out = capsys.readouterr().out
    assert code == 0
    assert "generation: 2" in out and "references: 3" in out and "indexed current: 0" in out
    # Three rows, three states, three different sentences: the bound pair is backlog, the
    # refused one is a document that will never converge on its own, the deleted one is unpaid
    # debt upstream — and the fourth number is the document no ledger row ever held.
    assert "stale: 2" in out
    assert "unledgered: 1" in out and "--reindex-all" in out
    assert "delivery blocked: 1" in out
    assert "cleanup failed: 1" in out and "--cleanup" in out


def test_clear_marks_reports_and_requires_the_confirmation(store, tmp_path, capsys):
    """The brake protects against one bad listing, so releasing it cannot be a reflex.

    Three exits, all stated in the same words: nothing set (nothing to do), set without
    confirmation (here is what you would be releasing), then confirmed (the value it removed).
    """
    assert run_ledger(configuration(tmp_path), cli_args(clear_marks=True), store=store) == 0
    assert "not set" in capsys.readouterr().out

    store.set_setting(MARKS_ROLLING_SETTING, "2026-11-19T09:00:00+00:00|34")
    config = configuration(tmp_path)

    assert run_ledger(config, cli_args(clear_marks=True), store=store) == 1
    out = capsys.readouterr().out
    assert "2026-11-19T09:00:00+00:00|34" in out and "--yes-i-mean-it" in out
    assert store.get_setting(MARKS_ROLLING_SETTING) == "2026-11-19T09:00:00+00:00|34"

    assert run_ledger(config, cli_args(clear_marks=True, yes_i_mean_it=True), store=store) == 0
    out = capsys.readouterr().out
    assert "cleared index_marks_rolling" in out and "34" in out
    assert store.get_setting(MARKS_ROLLING_SETTING) is None


def test_clear_marks_refuses_to_clear_a_counter_that_moved(store, tmp_path, capsys, monkeypatch):
    """The value shown is the value cleared, or nothing is cleared.

    A deletion pass can rewrite the window while this command runs. Read-then-delete would wipe
    the marks counted since the read — resetting the brake on the strength of a glance that is
    already out of date — so the compare-and-swap answers no and leaves the new value standing.
    """
    store.set_setting(MARKS_ROLLING_SETTING, "2026-11-19T09:00:00+00:00|34")
    real = store.get_setting

    def stale_read(name):
        return ("2026-11-19T09:00:00+00:00|34" if name == MARKS_ROLLING_SETTING
                else real(name))

    monkeypatch.setattr(store, "get_setting", stale_read)
    store.set_setting(MARKS_ROLLING_SETTING, "2026-11-19T10:00:00+00:00|41")

    code = run_ledger(configuration(tmp_path),
                      cli_args(clear_marks=True, yes_i_mean_it=True), store=store)

    assert code == 1
    assert "changed under this command" in capsys.readouterr().err
    monkeypatch.undo()          # the read below must be the real one, not the stale one
    assert store.get_setting(MARKS_ROLLING_SETTING) == "2026-11-19T10:00:00+00:00|41"


def failed_delete(store, doc, *, kreuzakt_id=7, generation=None):
    """Put one upstream delete into `cleanup_state='failed'`, by spending its allowance.

    Driven through the store's own state machine rather than an UPDATE, because the state this
    command reads is defined by what got it there: five spent attempts, an id and a generation
    stamped at queue time.
    """
    ref = indexable(store, doc=doc, kreuzakt_id=kreuzakt_id)
    store.ledger_mark_deleted(papra_org_id=ORG, papra_document_id=doc)
    store.ledger_queue_cleanup(papra_org_id=ORG, papra_document_id=doc,
                               due_at=store_module._now(), kreuzakt_id=kreuzakt_id,
                               generation=generation or ref.delivered_generation)
    for _ in range(CLEANUP_MAX_ATTEMPTS):
        store.ledger_cleanup_result(papra_org_id=ORG, papra_document_id=doc, ok=False,
                                    reason="kreuzakt is not answering")
    assert store.ledger_get(ORG, doc).cleanup_state == "failed"


def test_cleanup_settles_a_failed_delete_and_names_the_state(store, tmp_path, capsys):
    """The debt was visible and unpayable; now it is payable, and the row says so.

    No automatic pass will look at a `failed` row again — it is not due — so without this command
    the operator's only lever is an UPDATE by hand, on a table whose state machine four other
    writers depend on. The output has to name the state it left, because "deleted 1" alone does
    not tell anyone that a delete had stopped being attempted days ago.
    """
    failed_delete(store, "doc-owed")
    index = ScriptedDeleter()

    code = run_ledger(configuration(tmp_path), cli_args(cleanup=True), store=store,
                      papra=with_papra([]), kreuzakt=index)

    out = capsys.readouterr().out
    assert code == 0
    assert "re-queued 1 delete(s) from cleanup_state 'failed'" in out
    assert index.deleted == [7]
    ref = store.ledger_get(ORG, "doc-owed")
    assert ref.cleanup_state == "done"
    # It was 5 when this command read it. Back to 0 means the allowance really was refilled —
    # without that, the first failure after a week of outage would have written `failed` again
    # with Kreuzakt healthy, and the command would look like it did nothing.
    assert ref.cleanup_attempts == 0


def test_cleanup_refills_the_allowance_rather_than_the_failures(store, tmp_path, capsys):
    """The reset has to be a fresh allowance, which only a failing delete can show.

    A successful settle proves nothing about it: success clears the counter either way. So this
    delete fails once. With the reset the row goes back to `pending` on attempt one of five and the
    daemon's own pass keeps trying; without it the row was already at the ceiling, so the first
    failure after the outage would write `failed` again with Kreuzakt perfectly healthy, and the
    command would look like it had done nothing.
    """
    failed_delete(store, "doc-owed")
    index = ScriptedDeleter(fail=True)

    code = run_ledger(configuration(tmp_path), cli_args(cleanup=True), store=store,
                      papra=with_papra([]), kreuzakt=index)

    out = capsys.readouterr().out
    assert code == 1, "a delete that failed again must not exit 0"
    assert "1 failed again" in out
    ref = store.ledger_get(ORG, "doc-owed")
    assert ref.cleanup_state == "pending" and ref.cleanup_attempts == 1


def test_cleanup_dry_run_requeues_nothing(store, tmp_path, capsys):
    failed_delete(store, "doc-owed")
    index = ScriptedDeleter()

    code = run_ledger(configuration(tmp_path), cli_args(cleanup=True, dry_run=True), store=store,
                      papra=with_papra([]), kreuzakt=index)

    assert code == 0 and "dry run: 1 delete(s)" in capsys.readouterr().out
    assert store.ledger_get(ORG, "doc-owed").cleanup_state == "failed"
    assert index.deleted == []


def test_cleanup_does_not_turn_an_old_id_into_permission_to_delete(store, tmp_path, capsys):
    """Re-queueing an intent keeps the generation it was recorded under, always.

    After a rebuild the same integer can name different content, so stamping the current
    generation onto an old intent would convert an id from a destroyed index into authorisation to
    delete whatever holds that number now. The reset refills the attempts and nothing else; the
    pass's own revalidation sees the mismatch and settles the row as a recorded skip, which is
    reported rather than hidden inside a `skipped 1`.
    """
    failed_delete(store, "doc-owed", generation=1)
    store.bump_generation()
    index = ScriptedDeleter()

    code = run_ledger(configuration(tmp_path), cli_args(cleanup=True), store=store,
                      papra=with_papra([]), kreuzakt=index)

    out = capsys.readouterr().out
    assert code == 0
    assert "belong to an older generation" in out
    assert index.deleted == []
    ref = store.ledger_get(ORG, "doc-owed")
    assert ref.cleanup_state == "skipped" and ref.cleanup_generation == 1


def test_the_cli_is_wired_into_the_entry_point(tmp_path, monkeypatch):
    """`papra-suite ledger ...` has to reach the module, not merely import cleanly.

    The dispatch is the difference between a documented command and a file nothing calls, and it
    is the part a mutation to `__main__.py` deletes without anything in this file noticing: the
    parser registration, the `args.command` branch and the command body are three places that can
    each be wrong on their own.
    """
    import papra_suite.ledger_cli as ledger_cli_module
    from papra_suite.__main__ import main

    seen = []
    monkeypatch.setattr(ledger_cli_module, "run_ledger",
                        lambda config, args, **kw: (seen.append(args), 0)[1])
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "ledger", "--invalidate"])

    assert main() == 0
    # `--invalidate` arrived as an alias and must land on the same branch as `--reindex-all`.
    assert seen and seen[0].reindex_all is True


def test_the_entry_point_runs_a_real_status_command(tmp_path, monkeypatch, capsys):
    """The whole chain unmocked: parser, config, store creation, output, exit code.

    `--status` is what an operator runs first, and being the one command here that needs neither
    service it is also the one that can be proven end to end against nothing but a directory.
    """
    from papra_suite.__main__ import main

    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "ledger", "--status"])

    assert main() == 0
    out = capsys.readouterr().out
    assert "generation: 1" in out and "references: 0" in out
    assert "unledgered: 0" in out and "cleanup failed: 0" in out
