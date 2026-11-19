"""Task 15: `papra-suite backfill` — the staged corpus enqueue, with the ledger as the referee.

What makes this command different from `ledger --requeue`, and therefore worth its own subcommand:
`--requeue` finishes an interrupted rebuild and deliberately does not touch the legacy `ocr` queue,
while `backfill` is the operator slicing a corpus into the index on purpose — `--limit 20`, watch
`check`, then more — and it goes through the ordinary intake path, so while content delivery is off
it also asks for the text to be re-extracted. That is the point of the exercise on a corpus whose
Papra text was produced with the wrong OCR languages, and it is why the cost is printed rather than
left in a help string.

**Eligibility is the ledger's, never `document_seen`.** The seen flag records *ingestion*, so after
a rebuild every document in the corpus would look already-done and the command would report a
finished backfill over an empty index. A document is work when the current generation has no usable
reference for it — the same predicate `ledger_needs_index` implements — and the population this
exists for is the one the sweep will never visit: `document_seen` knows it, the ledger never held
it. Those are the PDFs mailed before the handover and the documents a restore left with a sighting
and no reference; the oversized skips are named only to say they are **counted and refused**, not
admitted, and they stay outside the index. Enrolling one document writes the ledger reference
**before** the job becomes visible, the intake ordering, because a queued job with
no row behind it is work no later pass can see.

**A remainder is printed, never implied.** `--limit 5` over 12 candidates prints `7 remaining`, and
the walk keeps listing after the cap so that number is measured rather than guessed: a command that
stopped at the cap could not tell "the rest is eligible" from "the rest is already indexed", and the
difference is whether you run it again.

Nothing here writes to Papra and nothing calls Kreuzakt: enqueuing work is the whole extent of the
command. The doubles assert that, and `patched` staying empty is the assertion.
"""

import argparse
import hashlib
import sqlite3
import sys
from types import SimpleNamespace

import pytest

from papra_suite.backfill_cli import (
    EXIT_INCOMPLETE,
    EXIT_OK,
    EXIT_REFUSED,
    LISTING_PAGE_SIZE,
    add_subparser,
    backfill_corpus,
    run_backfill,
)
from papra_suite.papra_client import PapraError
from papra_suite.pipeline import OCR_MAX_DOCUMENT_BYTES
from papra_suite.store import Store
from tests.test_config import BASE_ENV
from tests.test_index_job import PDF_BYTES

ORG = "org-a"
PDF = "application/pdf"
HASH = "cd" * 32


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=ORG, name="Org A")
    yield store
    store.close()


class FakeListing:
    """Papra's document listing, paged the way the real client pages it.

    `list_calls` records every page request so a test can prove the walk paged instead of trusting
    a document count. The write endpoints are present and recorded for the opposite reason: this
    command's contract is that it enqueues work and nothing else, and a double without those
    methods would let a stray `PATCH` pass silently.
    """

    def __init__(self, records=None):
        self.records = {}
        self.list_calls = []
        self.get_calls = 0
        self.download_calls = 0
        self.patched = []
        self.raise_on_list = None          # an Exception, or {org: Exception}
        self.raise_on_download = None      # an Exception, or {document_id: Exception}
        self.body = PDF_BYTES
        self.closed = False
        for record in records or []:
            self.records[(record["organizationId"], record["id"])] = record

    def add(self, document_id, *, org=ORG, mime=PDF, name=None, content_hash=HASH,
            size=None, deleted=False):
        # The name follows the mime on purpose: `_is_pdf` accepts a `.pdf` name as well as the
        # mime, so a PNG named `doc-3.pdf` is a PDF as far as the intake rules are concerned, and a
        # double that always said `.pdf` could not express "not a pdf" at all.
        extension = ".pdf" if mime == PDF else ".png" if mime == "image/png" else ".bin"
        record = {"id": document_id, "organizationId": org,
                  "name": name or f"{document_id}{extension}", "mimeType": mime,
                  "originalSha256Hash": content_hash, "isDeleted": deleted,
                  "deletedAt": None}
        if deleted:
            record["deletedAt"] = "2026-11-19T09:00:00+00:00"
        if size is not None:
            record["originalSize"] = size
        self.records[(org, document_id)] = record
        return record

    def _raise_for(self, org_id):
        error = (self.raise_on_list.get(org_id) if isinstance(self.raise_on_list, dict)
                 else self.raise_on_list)
        if error is not None:
            raise error

    async def list_documents(self, org_id, *, page_index=0, page_size=100,
                             search_query=None, sort_field="createdAt",
                             sort_order="desc"):
        self.list_calls.append((org_id, page_index, page_size))
        self._raise_for(org_id)
        rows = [row for (org, _), row in self.records.items() if org == org_id]
        start = page_index * page_size
        return rows[start:start + page_size], len(rows)

    async def get_document(self, org_id, document_id):
        self.get_calls += 1
        raise PapraError(f"GET document failed: 404 {document_id} not found", 404)

    def _raise_for_download(self, document_id):
        error = (self.raise_on_download.get(document_id)
                 if isinstance(self.raise_on_download, dict) else self.raise_on_download)
        if error is not None:
            raise error

    async def download_file(self, org_id, document_id):
        self.download_calls += 1
        self._raise_for_download(document_id)
        return self.body

    async def update_document(self, org_id, document_id, **body):
        self.patched.append((org_id, document_id, body))
        raise AssertionError("backfill must not write to papra")

    async def update_content(self, org_id, document_id, content):
        self.patched.append((org_id, document_id, {"content": content}))
        raise AssertionError("backfill must not write to papra")

    async def aclose(self):
        self.closed = True


def seed_listing(papra, count=5, *, org=ORG, mime=PDF, start=0, **over):
    """`count` documents in one bucket, ids `doc-0`, `doc-1`, …"""
    for index in range(start, start + count):
        papra.add(f"doc-{index}", org=org, mime=mime, **over)
    return papra


def seed_ledger(store, *, org=ORG, document_id, kreuzakt_id, generation, file_hash=HASH,
                name=None):
    """A reference bound under `generation` — the state that makes a document not-work."""
    store.ledger_upsert_reference(papra_org_id=org, papra_document_id=document_id,
                                  file_hash=file_hash,
                                  papra_original_name=name or f"{document_id}.pdf")
    store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=document_id,
                                 kreuzakt_id=kreuzakt_id, generation=generation)


def cli_args(**over):
    base = dict(command="backfill", org_id=None, limit=None, dry_run=False, all_stale=False)
    return SimpleNamespace(**{**base, **over})


def index_jobs(store):
    return [job for job in store.list_jobs(limit=500) if job.kind == "index"]


def ocr_jobs(store):
    return [job for job in store.list_jobs(limit=500) if job.kind == "ocr"]


def run(store, papra, *, config=None, max_document_bytes=None, **over):
    """One CLI invocation, with a real `Config` because the command reads a value out of it.

    `PAPRA_MAX_DOCUMENT_BYTES` is that value, so `max_document_bytes=` here is the operator's knob
    and `max_document_bytes=` on `backfill_corpus` is the same ceiling one level down.
    """
    if config is None:
        from papra_suite.config import load_config

        env = dict(BASE_ENV)
        if max_document_bytes is not None:
            env["PAPRA_MAX_DOCUMENT_BYTES"] = str(max_document_bytes)
        config = load_config({**env, "DATA_DIR": "/tmp/x"})
    return run_backfill(config, cli_args(**over), store=store, papra=papra)


# --- the rehearsal, and the eligibility rule it rehearses -------------------------

async def test_dry_run_counts_the_work_and_changes_nothing(store):
    """The rehearsal is what makes `--limit` safe to guess with.

    Three populations have to still be absent afterwards — the queue, the ledger and the seen
    table — because the reference write is the one part of intake that a rehearsal could perform
    by accident and leave a document looking enrolled while nothing was queued.
    """
    papra = seed_listing(FakeListing(), count=12)

    result = await backfill_corpus(store, papra, orgs=[ORG], dry_run=True)

    assert (result.candidates, result.asked_for, result.remaining) == (12, 12, 0)
    assert store.list_jobs() == [] and store.ledger_org_ids() == []
    assert store.document_seen(ORG, "doc-0") is False
    assert papra.patched == [] and papra.download_calls == 0
    assert papra.list_calls[0] == (ORG, 0, LISTING_PAGE_SIZE)


def test_backfill_dry_run_counts_without_enqueuing(store, capsys):
    papra = seed_listing(FakeListing(), count=12)

    assert run(store, papra, dry_run=True) == 0

    assert papra.list_calls and store.list_jobs() == []
    assert "would queue 12" in capsys.readouterr().out


async def test_documents_the_ledger_calls_current_are_walked_past(store):
    """The skip is per document, and it is the ledger's verdict, not the seen flag's.

    Ten documents with a reference bound under the current generation are finished work; the run
    has to say so in the same sentence it uses to say it queued nothing, or "0" alone cannot be
    told apart from a walk that found no documents at all.
    """
    papra = seed_listing(FakeListing(), count=10)
    for index in range(10):
        seed_ledger(store, document_id=f"doc-{index}", kreuzakt_id=index + 1,
                    generation=store.get_generation())

    result = await backfill_corpus(store, papra, orgs=[ORG])

    assert (result.asked_for, result.already_current, result.remaining) == (0, 10, 0)
    assert store.list_jobs() == []


def test_backfill_reports_zero_new_rows_beside_the_ten_it_examined(store, capsys):
    papra = seed_listing(FakeListing(), count=10)
    for index in range(10):
        seed_ledger(store, document_id=f"doc-{index}", kreuzakt_id=index + 1,
                    generation=store.get_generation())

    assert run(store, papra) == 0

    out = capsys.readouterr().out
    assert "queued 0 new row(s)" in out and "0 of 10" in out and "remaining" in out


async def test_seen_is_not_eligibility_a_mailed_pdf_with_no_reference_is_enrolled(store):
    """The population the sweep will never visit, and the reason this command exists.

    `document_seen` says this suite has ingested the document; the ledger has never held it, and
    every count, cadence and eligibility rule reads the ledger. So the sweep skips it as already
    seen and `ledger_needs_index` cannot see it either — it is invisible from every other angle in
    the system, which is exactly the state the pre-handover mailings left the corpus in.
    """
    papra = seed_listing(FakeListing(), count=3)
    for index in range(3):
        store.record_document_seen(papra_org_id=ORG, papra_document_id=f"doc-{index}",
                                   name=f"doc-{index}.pdf", mime=PDF, ingested_via="mail")

    result = await backfill_corpus(store, papra, orgs=[ORG])

    assert (result.asked_for, result.enrolled) == (3, 3)
    assert len(index_jobs(store)) == 3
    assert [ref.papra_document_id for ref in store.ledger_needs_index(
        generation=store.get_generation())] == ["doc-0", "doc-1", "doc-2"]


async def test_the_reference_exists_before_the_job_becomes_visible(store):
    """The intake ordering, asserted on the enqueue call rather than on the final state.

    A job queued with no ledger row behind it is work no sweep, rebuild or later backfill can see:
    eligibility is derived from the ledger, so the document would be enqueued once and be invisible
    forever after. The final state cannot show that — both rows exist either way — so the spy
    records what the ledger held at the moment each job row was inserted.
    """
    papra = seed_listing(FakeListing(), count=2)
    observed = []
    real = store.enqueue_job

    def spy(**kwargs):
        observed.append((kwargs["kind"], kwargs["document_id"],
                         store.ledger_get(ORG, kwargs["document_id"]) is not None))
        return real(**kwargs)

    store.enqueue_job = spy
    await backfill_corpus(store, papra, orgs=[ORG])

    assert observed and all(preceded_by_reference for _kind, _doc,
                            preceded_by_reference in observed), observed
    assert {doc for _kind, doc, _ok in observed} == {"doc-0", "doc-1"}


async def test_a_stale_reference_is_work_without_any_extra_flag(store):
    """Eligibility is "no current-generation reference", which includes a stale binding.

    After a generation bump the whole corpus is stale, and a backfill that only looked at
    documents the ledger never held would report a successful run over an index holding nothing —
    the mistake the plan calls out as the one v1 would have made silently.
    """
    papra = seed_listing(FakeListing(), count=4)
    for index in range(4):
        seed_ledger(store, document_id=f"doc-{index}", kreuzakt_id=index + 1, generation=1)
    store.bump_generation()

    result = await backfill_corpus(store, papra, orgs=[ORG])

    assert (result.asked_for, result.stale_queued, result.enrolled) == (4, 4, 0)
    assert len(index_jobs(store)) == 4


async def test_all_stale_reaches_references_the_listing_never_showed(store):
    """What `--all-stale` is for: the ledger's own population, walked from the ledger.

    A reference can be stale for a document Papra's listing does not hand this walk — a bucket
    that moved on, a page that changed under the paging, a document enrolled by a restore. Without
    the flag it stays queued by nobody and printed by nothing; with it, the ledger itself is the
    worklist, read by keyset and hashed from the reference, exactly as a rebuild reads it.
    """
    papra = seed_listing(FakeListing(), count=1)          # only doc-0 is listed
    seed_ledger(store, document_id="doc-0", kreuzakt_id=1, generation=1)
    seed_ledger(store, document_id="doc-invisible", kreuzakt_id=2, generation=1)
    store.bump_generation()

    without = await backfill_corpus(store, papra, orgs=[ORG])
    assert (without.asked_for, without.remaining) == (1, 0)
    # Spend that row so the second run has to ask again for the same bytes: since migration 004 a
    # terminal row does not block it, so the second run takes a row of its own — which is why the
    # assertion below counts live rows, the only sense in which a document is queued once.
    store.mark_job_failed(index_jobs(store)[0].id, "test", max_attempts=1)

    with_flag = await backfill_corpus(store, papra, orgs=[ORG], all_stale=True)

    assert with_flag.asked_for >= 1
    live = [job.document_id for job in index_jobs(store) if job.status == "pending"]
    assert "doc-invisible" in live
    assert live.count("doc-0") == 1, "the second walk queued the listing's document twice"


# --- the cap, and the remainder it leaves -----------------------------------------

async def test_the_limit_caps_the_run_and_the_remainder_is_measured(store):
    """`--limit` is the whole point of the command, so the rest has to be counted.

    The walk keeps listing after the cap for exactly this: stopping at the cap would leave
    `remaining` unknown, and an unknown remainder printed as silence is how a half-enrolled corpus
    reads as a finished one.
    """
    papra = seed_listing(FakeListing(), count=12)

    result = await backfill_corpus(store, papra, orgs=[ORG], limit=5)

    assert (result.asked_for, result.remaining, result.candidates) == (5, 7, 12)
    assert len(index_jobs(store)) == 5


def test_backfill_respects_limit_and_reports_remaining(store, capsys):
    papra = seed_listing(FakeListing(), count=12)

    assert run(store, papra, limit=5) == 0

    out = capsys.readouterr().out
    assert len(index_jobs(store)) == 5
    assert "7 remaining" in out and "`papra-suite backfill` again" in out


def test_a_limit_is_a_deliberate_slice_and_not_a_failure(store, capsys):
    """Exit 0 with the work still owed, printed.

    The command did what was asked. Reserve exit 1 for a walk that could not be finished, or a
    "done" here would stop meaning anything while the operator still had nine runs to make.
    """
    papra = seed_listing(FakeListing(), count=10)

    assert run(store, papra, limit=1) == 0

    assert "9 remaining" in capsys.readouterr().out


def test_the_dry_run_respects_the_limit_too(store, capsys):
    """A rehearsal that ignores the cap rehearses a different command.

    The operator uses it to choose the slice, so `--limit 5 --dry-run` has to say it would enqueue
    5 of 12 and leave 7 — or the number it prints is not the number the real run would produce.
    """
    papra = seed_listing(FakeListing(), count=12)

    assert run(store, papra, dry_run=True, limit=5) == 0

    out = capsys.readouterr().out
    assert "would queue 5" in out and "7 remaining" in out


# --- what is left out, and why it is said out loud --------------------------------

async def test_non_pdf_documents_are_outside_a_pdf_only_index(store):
    papra = seed_listing(FakeListing(), count=4, mime="image/png")

    result = await backfill_corpus(store, papra, orgs=[ORG])

    assert (result.asked_for, result.skipped_non_pdf, result.candidates) == (0, 4, 0)
    assert store.list_jobs() == []


def test_backfill_ignores_non_pdf_documents(store, capsys):
    papra = seed_listing(FakeListing(), count=4, mime="image/png")

    assert run(store, papra) == 0

    assert store.list_jobs() == []
    assert "4 not a pdf" in capsys.readouterr().out


async def test_a_document_in_papra_s_trash_is_the_deletion_pass_s_work(store):
    """Enrolling a trashed document would queue work that resurrects it.

    The index job refuses to write to a deleted record, so the queue row would only fail; and
    `enqueue_index_job` writes a fresh reference, which is the ledger deciding a live document
    exists after Papra said it does not.
    """
    papra = FakeListing()
    papra.add("doc-trashed", deleted=True)
    papra.add("doc-live")

    result = await backfill_corpus(store, papra, orgs=[ORG])

    assert (result.asked_for, result.skipped_deleted) == (1, 1)
    assert store.ledger_get(ORG, "doc-trashed") is None


async def test_an_oversized_document_is_refused_on_the_size_papra_declared(store):
    """The skip that made this population invisible in the first place, without paying for it.

    Downloading a 400 MB scan to discover it is 400 MB is how a backfill becomes the incident; the
    listing carries the declared size, and that is what the limit is judged against.
    """
    papra = FakeListing()
    papra.add("doc-huge", size=OCR_MAX_DOCUMENT_BYTES + 1)

    result = await backfill_corpus(store, papra, orgs=[ORG])

    assert (result.asked_for, result.skipped_oversized) == (0, 1)
    assert papra.download_calls == 0
    assert store.ledger_get(ORG, "doc-huge") is None


async def test_a_listing_without_a_hash_is_hashed_from_the_body(store):
    """The job's content hash has to be Papra's fact about its bytes.

    A hash invented here fails permanently the moment the job downloads and verifies, with a
    message about the document having moved — so the fallback is the sweep's: download, measure,
    hash. The size limit is re-checked on the bytes actually held, because a declared size is a
    claim and the download is the measurement.
    """
    papra = FakeListing()
    papra.add("doc-no-hash", content_hash=None)

    result = await backfill_corpus(store, papra, orgs=[ORG])

    assert result.asked_for == 1 and papra.download_calls == 1
    assert index_jobs(store)[0].content_hash == hashlib.sha256(PDF_BYTES).hexdigest()


async def test_a_document_too_big_to_hash_is_refused_after_the_download(store):
    """The re-check on measured bytes, at a size a test can afford.

    The ceiling is passed in at eight bytes rather than the body inflated: the branch under test is
    "the download turned out to be over the limit", and allocating 100 MB per test would make the
    suite slower without testing anything the small version does not. Passing it, rather than
    monkeypatching a module constant, is also what makes this a test of the wiring and not of a name
    some other module happens to hold.
    """
    papra = FakeListing()
    papra.add("doc-quiet-huge", content_hash=None)
    papra.body = b"x" * 9

    result = await backfill_corpus(store, papra, orgs=[ORG], max_document_bytes=8)

    assert (result.asked_for, result.skipped_oversized) == (0, 1)
    assert store.list_jobs() == []


async def test_a_refused_document_is_written_to_the_record_at_the_moment_of_the_skip(store):
    """`check` counts these; it can only do that if the walk wrote them down.

    An oversized document has no ledger row — it is refused before enrollment — so there is nothing
    for a count to read afterwards. The skip is the last moment anyone knows about it.
    """
    from papra_suite.oversized import oversized_seen

    papra = FakeListing()
    papra.add("doc-huge", size=OCR_MAX_DOCUMENT_BYTES + 1)
    papra.add("doc-quiet-huge", content_hash=None)
    papra.body = b"x" * 9      # only reachable by the download leg, under the ceiling below

    await backfill_corpus(store, papra, orgs=[ORG])

    record = oversized_seen(store)
    assert record.count == 1, "the declared-size refusal is the one this walk made"
    assert record.documents[0].document_id == "doc-huge"
    assert record.documents[0].size == OCR_MAX_DOCUMENT_BYTES + 1
    assert record.limit == OCR_MAX_DOCUMENT_BYTES


async def test_a_refusal_after_the_download_is_recorded_too(store):
    """Two gates, one record. The measured body is the truer of the two sizes."""
    from papra_suite.oversized import oversized_seen

    papra = FakeListing()
    papra.add("doc-quiet-huge", content_hash=None)
    papra.body = b"x" * 9

    await backfill_corpus(store, papra, orgs=[ORG], max_document_bytes=8)

    record = oversized_seen(store)
    assert [(d.document_id, d.size) for d in record.documents] == [("doc-quiet-huge", 9)]
    assert record.limit == 8


def test_a_dry_run_names_every_refused_document_with_its_size(store, capsys):
    """The rehearsal has to say what it would refuse, or the operator learns it from the real run.

    This population is the one the pre-handover corpus left behind, and until now the only place it
    appeared was a WARNING per document in a log line nobody reads. No dashboard ships yet, so
    `backfill --dry-run` and `check` are the surfaces.
    """
    from papra_suite.oversized import oversized_seen

    papra = FakeListing()
    papra.add("doc-big", size=400)
    papra.add("doc-bigger", size=500)

    assert run(store, papra, dry_run=True, max_document_bytes=100) == EXIT_OK

    out = capsys.readouterr().out
    assert "org-a/doc-big (400 bytes)" in out
    assert "org-a/doc-bigger (500 bytes)" in out
    assert "2" in out and "100" in out
    assert oversized_seen(store).count == 0, "a dry run records nothing, reasons included"


def test_the_named_refusals_are_bounded_and_the_total_is_printed(store, capsys):
    """A backlog of 400 scans must not become 400 lines in a terminal.

    The cap is stated on the line that carries it, and the total comes from the counter rather than
    from the listing — a number read out of a capped list can only ever shorten.
    """
    from papra_suite.backfill_cli import OVERSIZED_NAMES_SHOWN

    papra = FakeListing()
    for index in range(OVERSIZED_NAMES_SHOWN + 5):
        papra.add(f"doc-{index}", size=200 + index)

    assert run(store, papra, max_document_bytes=100) == EXIT_OK

    out = capsys.readouterr().out
    assert f"{OVERSIZED_NAMES_SHOWN} of 15" in out
    assert "doc-0 (200 bytes)" in out
    assert f"doc-{OVERSIZED_NAMES_SHOWN} (210 bytes)" not in out, "the cap has to be a cap"
    assert "15" in out and "largest 214" in out


# --- the walk itself ---------------------------------------------------------------

async def test_a_corpus_bigger_than_a_page_is_walked_to_its_end(store):
    """A single page is 100 documents, and no real corpus stops there.

    A backfill that read one page would report the first hundred and call the bucket done — so the
    page requests themselves are asserted, not only the count.
    """
    papra = seed_listing(FakeListing(), count=LISTING_PAGE_SIZE + 20)

    result = await backfill_corpus(store, papra, orgs=[ORG])

    assert (result.asked_for, result.listed) == (120, 120)
    assert [call[1] for call in papra.list_calls if call[0] == ORG] == [0, 1]


async def test_every_known_bucket_is_walked_when_none_is_named(store):
    """The org list comes from the store, and the ledger's buckets count too.

    A bucket can leave the `orgs` table while its references remain; a scope built only from
    configured mailboxes would leave its documents un-backed-filled and silent.
    """
    papra = FakeListing()
    papra.add("doc-a", org=ORG)
    papra.add("doc-b", org="org-b")
    store.create_org(papra_org_id="org-b", name="Org B")
    seed_ledger(store, org="org-b", document_id="doc-b", kreuzakt_id=9, generation=1)
    store.bump_generation()

    result = await backfill_corpus(store, papra, orgs=[ORG, "org-b"])

    assert sorted(result.orgs_queued) == [ORG, "org-b"]
    assert sorted(job.papra_org_id for job in index_jobs(store)) == [ORG, "org-b"]


async def test_one_bucket_that_will_not_answer_does_not_end_the_others(store):
    """Papra refusing a listing is a fact about that bucket, not a reason to stop.

    The walk still has to report itself unfinished though: "queued 4" with one bucket unread
    is not a corpus that has been backfilled, and the exit code is what the runbook checks.
    """
    papra = FakeListing()
    papra.add("doc-a", org=ORG)
    papra.add("doc-b", org="org-b")
    papra.raise_on_list = {"org-b": PapraError("GET documents failed: 503", 503)}

    result = await backfill_corpus(store, papra, orgs=[ORG, "org-b"])

    assert result.asked_for == 1 and result.unreadable == ["org-b"]
    assert result.complete is False


@pytest.mark.parametrize("error", [
    PapraError("GET file failed: 500 upstream died", 500),
    PapraError("GET file failed after 3 attempts: connection reset by peer", None)],
    ids=["papra-500", "transport-exhausted"])
async def test_a_body_that_will_not_arrive_is_an_unfinished_walk(store, error):
    """A 500 mid-download is a bucket that was not read, not a stack trace.

    The listing is guarded and the download was not: this is the second network call the walk
    makes, and it happens for every document one of Papra's servers does not publish a content
    hash for — which is the atlas deployment. Both shapes come from the same client (an HTTP
    failure and a transport failure it exhausted its retries on), and the handler is the wide
    tuple `ledger` carries, so a store refusal or a locked database takes the same road rather
    than a traceback. The verdict is the listing's, for the listing's reason: documents already
    queued stay queued, the run is incomplete, and the instruction is to run it again once Papra
    answers. A traceback is the wrong shape twice over — an operator cannot re-run what looks like
    a crash, and one unread document would read as a whole unread bucket.
    """
    papra = FakeListing()
    papra.add("doc-1")                                 # carries a hash: queued without a download
    papra.add("doc-2", content_hash=None)              # needs the body, and Papra refuses it
    papra.raise_on_download = {"doc-2": error}

    result = await backfill_corpus(store, papra, orgs=[ORG])

    assert result.new_rows == 1 and len(index_jobs(store)) == 1
    assert result.unreadable == [ORG] and result.complete is False
    assert "doc-2" in result.read_errors[0]
    assert type(error).__name__ in result.read_errors[0]


def test_a_download_that_dies_mid_corpus_exits_1_and_prints_no_traceback(store, capsys):
    """The same fact at the exit code and on the terminal.

    `backfill` is the command an operator reaches for during an unhappy-service window, which is
    exactly when a body fetch fails, so this is the failure the file's own promise is about: an
    unreadable population is reported as unfinished, in words, with exit `1`.
    """
    papra = FakeListing()
    papra.add("doc-1")
    papra.add("doc-2", content_hash=None)
    papra.raise_on_download = {"doc-2": PapraError("GET file failed: 500 upstream died", 500)}

    assert run(store, papra) == EXIT_INCOMPLETE

    err = capsys.readouterr().err
    assert "Traceback" not in err
    assert "could not read" in err and "doc-2" in err
    assert len(index_jobs(store)) == 1                 # what was queued stays queued


def test_a_client_that_refuses_to_be_built_is_a_refusal_and_not_a_traceback(tmp_path, capsys,
                                                                           monkeypatch):
    """Nothing in this command may reach a terminal as a traceback.

    The listing's failures are handled where they happen; this is the catch-all beside them, and it
    is the tuple `ledger` already carries — a client that cannot be built, a transport error, a
    locked store. Narrowing it to `sqlite3.Error` is what let a `PapraError` out of a download.
    """
    from papra_suite.config import load_config

    seeded = Store(tmp_path / "state.db")
    seeded.migrate()
    seeded.create_org(papra_org_id=ORG, name="Org A")
    seeded.close()

    def no_client(*args, **kwargs):
        raise PapraError("could not build a client: no api key", None)

    monkeypatch.setattr("papra_suite.backfill_cli.PapraClient", no_client)
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})

    assert run_backfill(config, cli_args()) == EXIT_REFUSED

    err = capsys.readouterr().err
    assert "refused" in err and "Traceback" not in err


async def test_another_buckets_live_row_does_not_absorb_this_documents_request(store):
    """"Already queued" is a statement about this bucket, not about a document id.

    The work key is `(kind, papra_org_id, document_id, content_hash)`, and Papra's ids are scoped
    to their organization, so a live row in `org-b` says nothing about whether `org-a`'s copy is
    queued. Read without the bucket, the run reports "already queued, unchanged" and enqueues
    nothing — and since the eligibility rule is the ledger's, nothing else will queue it either.
    """
    store.create_org(papra_org_id="org-b", name="Org B")
    store.enqueue_job(kind="index", papra_org_id="org-b", document_id="doc-1",
                      content_hash=HASH, payload={})
    papra = FakeListing()
    papra.add("doc-1", org=ORG)

    result = await backfill_corpus(store, papra, orgs=[ORG])

    assert (result.new_rows, result.already_queued) == (1, 0)
    assert sorted(job.papra_org_id for job in index_jobs(store)) == [ORG, "org-b"]


async def test_the_corpus_walk_never_reads_the_queue_through_the_listing(store):
    """The per-document queue check goes to the store's work-key lookup, not to a listing.

    `--limit` and `--all-stale` make this call once per PDF over a corpus of thousands, and the
    listing read was correct — it was just a per-kind filter plus a sort, per document, on the
    command that is supposed to be the bounded way in. So the shape is forbidden rather than
    discouraged: a run whose listing read is asked for anything fails here, the same way
    `test_drain_reads_its_two_numbers_from_one_snapshot` forbids per-status counting.
    """
    papra = seed_listing(FakeListing(), count=3)

    class NoListing:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def list_jobs(self, *args, **kwargs):
            raise AssertionError("the per-document queue check read the queue through the listing")

    result = await backfill_corpus(NoListing(store), papra, orgs=[ORG])

    assert result.new_rows == 3 and len(index_jobs(store)) == 3


async def test_the_walk_touches_papra_only_through_the_listing(store):
    """No detail reads for documents the listing already described, and no writes at all.

    The listing carries the hash, the name and the size; asking for each document's detail as well
    would be one request per document for nothing, and this is the command an operator runs over a
    whole corpus while a service is unhappy.
    """
    papra = seed_listing(FakeListing(), count=6)

    await backfill_corpus(store, papra, orgs=[ORG])

    assert papra.get_calls == 0 and papra.patched == [] and papra.download_calls == 0


# --- what the queue ends up holding ------------------------------------------------

async def test_backfill_fills_the_legacy_ocr_queue_while_the_bridge_is_not_the_writer(store):
    """This is the re-extraction path, and that is the difference from `ledger --requeue`.

    The corpus on atlas holds Papra text extracted with the wrong OCR languages, and re-indexing
    alone does not change a column Papra owns. So backfill goes through the ordinary intake path
    and takes the legacy `ocr` enqueue with it — the paid VLM extraction per document is the point,
    and it is what `--dry-run` has to say out loud before the operator commits to it.
    """
    store.set_setting("legacy_workers", "1")
    store.set_setting("index_content_delivery", "0")
    papra = seed_listing(FakeListing(), count=3)

    await backfill_corpus(store, papra, orgs=[ORG])

    assert len(index_jobs(store)) == 3 and len(ocr_jobs(store)) == 3


async def test_no_vlm_extraction_is_ordered_once_the_bridge_owns_the_text(store):
    """With delivery on, the same command queues index work and nothing else.

    The flag decides, not the caller: ordering an extraction the bridge is already doing would be
    a paid duplicate of a write the index job performs itself.
    """
    store.set_setting("index_content_delivery", "1")
    papra = seed_listing(FakeListing(), count=3)

    await backfill_corpus(store, papra, orgs=[ORG])

    assert len(index_jobs(store)) == 3 and ocr_jobs(store) == []


def test_the_cost_of_a_slice_is_printed_before_it_is_paid(store, capsys):
    papra = seed_listing(FakeListing(), count=2)
    store.set_setting("legacy_workers", "1")
    store.set_setting("index_content_delivery", "0")

    assert run(store, papra, dry_run=True) == 0

    out = capsys.readouterr().out.lower()
    assert "vlm" in out and "ocr" in out and "kreuzakt" in out


def test_the_cost_line_stops_promising_an_extraction_that_will_not_happen(store, capsys):
    store.set_setting("index_content_delivery", "1")
    papra = seed_listing(FakeListing(), count=2)

    assert run(store, papra, dry_run=True) == 0

    out = capsys.readouterr().out.lower()
    assert "vlm" in out and "not" in out


def test_backfill_requeues_everything_after_a_generation_bump(store, capsys):
    """The rebuild case v1 would have silently skipped: seen is not indexed.

    Twelve documents, twelve seen rows, twelve references bound under the generation that has just
    been retired. A rule that asked `document_seen` would find nothing to do; the ledger finds the
    whole corpus, and `--all-stale` walks it from the ledger as well as from the listing.
    """
    papra = seed_listing(FakeListing(), count=12)
    for index in range(12):
        seed_ledger(store, document_id=f"doc-{index}", kreuzakt_id=index + 1, generation=1)
        store.record_document_seen(papra_org_id=ORG, papra_document_id=f"doc-{index}",
                                   name="x", mime=PDF, ingested_via="mail")
    store.bump_generation()

    assert run(store, papra, all_stale=True) == 0

    assert len(index_jobs(store)) == 12
    assert "12 of 12" in capsys.readouterr().out


# --- refusals, and the shapes of not-finishing ------------------------------------

def test_backfill_refuses_a_missing_database_before_opening_the_store(tmp_path, capsys,
                                                                     monkeypatch):
    """`Store()` would create the file, and the operator's question would be erased.

    "Why has nothing been indexed?" answered against the wrong `DATA_DIR` must stay answerable: an
    empty database created on the way in turns a wrong path into a plausible empty corpus.
    """
    from papra_suite.__main__ import main

    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "wrong-dir"))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "backfill"])

    assert main() == EXIT_REFUSED
    assert "database" in capsys.readouterr().err
    assert not (tmp_path / "wrong-dir").exists()


def test_an_empty_scope_is_refused_rather_than_reported_as_finished(tmp_path, capsys):
    """A store with no buckets knows nothing to walk, and "0 documents" would be a lie.

    The org list is this store's knowledge, not Papra's: the command cannot discover buckets it has
    never seen, so it refuses and names the flag that supplies one.
    """
    blank = Store(tmp_path / "blank.db")
    blank.migrate()
    try:
        code = run(blank, FakeListing())
    finally:
        blank.close()

    assert code == EXIT_REFUSED
    assert "--org-id" in capsys.readouterr().err


def test_a_locked_store_is_a_refusal_and_not_a_traceback(store, capsys, monkeypatch):
    def locked(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(Store, "ledger_get", locked)
    papra = seed_listing(FakeListing(), count=3)

    assert run(store, papra) == EXIT_REFUSED
    assert "refused" in capsys.readouterr().err
    assert store.list_jobs() == []


def test_an_interrupted_walk_keeps_what_it_queued_and_says_it_stopped(store, capsys,
                                                                     monkeypatch):
    """Ctrl-C in the middle of a corpus is expected, and the work already queued stays queued.

    That is what makes a bounded enqueue safe to run at all: the answer to an interruption is to
    run the command again, so the interruption must not look like a crash (a traceback an operator
    will not re-run) and must not look like a success (a corpus they will not finish).
    """
    real = store.enqueue_job
    state = {"n": 0}

    def enqueue_then_stop(**kwargs):
        state["n"] += 1
        if state["n"] == 3:
            raise KeyboardInterrupt("the operator pressed ctrl-c")
        return real(**kwargs)

    monkeypatch.setattr(store, "enqueue_job", enqueue_then_stop)
    papra = seed_listing(FakeListing(), count=6)
    # Legacy workers off so one document costs one enqueue: with them on, the intake path queues the
    # `ocr` row too and the counter above would stop on the second row of the first document.
    store.set_setting("legacy_workers", "0")

    assert run(store, papra) == EXIT_INCOMPLETE

    err = capsys.readouterr().err
    assert "interrupted" in err and "again" in err
    assert len(index_jobs(store)) == 2


# --- the client, and the entry point ------------------------------------------------

def test_the_client_this_command_builds_is_closed_by_this_command(tmp_path, monkeypatch):
    """`run_backfill` owns the client it constructs, and closes it on the loop that used it.

    The lifecycle is the one `ledger` learned the hard way: closing an `httpx` transport after
    `asyncio.run` has closed its loop either raises or leaks, and a `finally` that raises on the way
    out replaces a successful result with an exception. So the close happens inside the coroutine.
    """
    from papra_suite.config import load_config

    db = tmp_path / "state.db"
    seeded = Store(db)
    seeded.migrate()
    seeded.create_org(papra_org_id=ORG, name="Org A")
    seeded.close()
    fake = seed_listing(FakeListing(), count=2)
    monkeypatch.setattr("papra_suite.backfill_cli.PapraClient",
                        lambda *a, **k: fake)
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})

    assert run_backfill(config, cli_args(dry_run=True), store=None) == 0
    assert fake.closed is True


def test_the_cli_is_wired_into_the_entry_point(tmp_path, monkeypatch):
    """`papra-suite backfill` has to reach the module, not merely import cleanly."""
    import papra_suite.backfill_cli as backfill_cli_module
    from papra_suite.__main__ import main

    seen = []
    monkeypatch.setattr(backfill_cli_module, "run_backfill",
                        lambda config, args, **kw: (seen.append(args), 0)[1])
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "backfill", "--org-id", ORG, "--limit",
                                      "5", "--dry-run"])

    assert main() == 0
    assert seen and seen[0].org_id == ORG and seen[0].limit == 5 and seen[0].dry_run is True


def test_the_entry_point_walks_a_real_store_end_to_end(tmp_path, monkeypatch, capsys):
    """Parser, config, store from `DATA_DIR`, client built from config, one printed verdict.

    The README tells people to type this against a live deployment; a citation that only parses is
    the failure this project has a standing rule about, so the whole chain runs here with only
    Papra replaced.
    """
    from papra_suite.__main__ import main

    db = tmp_path / "state.db"
    seeded = Store(db)
    seeded.migrate()
    seeded.create_org(papra_org_id=ORG, name="Org A")
    seeded.close()
    fake = seed_listing(FakeListing(), count=7)
    monkeypatch.setattr("papra_suite.backfill_cli.PapraClient", lambda *a, **k: fake)
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "backfill", "--dry-run"])

    assert main() == 0
    out = capsys.readouterr().out
    assert "would queue 7" in out and "backfill" in out


def test_the_summary_names_the_buckets_it_queued_across(store, capsys):
    """The headline's shape: `queued N new row(s) across K bucket(s); asked for N+A of C
    candidate(s); L remaining`.

    Bucket count is in it because an operator deciding whether to run again wants to know whether
    the run covered one organisation or four — and it hangs off the new rows, because a run that
    absorbed everything touched no bucket at all.
    """
    papra = FakeListing()
    papra.add("doc-a", org=ORG)
    papra.add("doc-b", org="org-b")
    store.create_org(papra_org_id="org-b", name="Org B")

    assert run(store, papra) == 0

    out = capsys.readouterr().out
    assert "queued 2 new row(s) across 2 bucket(s)" in out
    assert "asked for 2 of 2 candidate(s)" in out and "0 remaining" in out


def test_the_parses_surface_is_what_the_readme_prints():
    """Flags and their spellings, checked on the parser rather than in prose."""
    parser = argparse.ArgumentParser(prog="papra-suite")
    sub = parser.add_subparsers(dest="command", required=True)
    add_subparser(sub)
    parse = sub.choices["backfill"].parse_args

    args = parse(["--org-id", ORG, "--limit", "20", "--dry-run", "--all-stale"])
    assert (args.org_id, args.limit, args.dry_run, args.all_stale) == (ORG, 20, True, True)
    assert parse([]) and parse([]).limit is None and parse([]).all_stale is False
    with pytest.raises(SystemExit):
        parse(["--limit", "many"])


def test_a_limit_of_zero_is_refused_rather_than_run_as_nothing(store, capsys):
    """`--limit 0` is a typo for "do not run", and there is no run that enqueues zero.

    Saying so costs one line and saves the operator from reading "asked for 0 of 843 candidate(s)"
    as a backfill that finished.
    """
    assert run(store, FakeListing(), limit=0) == EXIT_REFUSED
    err = capsys.readouterr().err
    assert "--limit" in err and "row" in err and "document" not in err
    assert store.list_jobs() == []


async def test_a_stuck_head_does_not_eat_the_next_slice(store, capsys):
    """`--limit 2` twice has to reach documents 3 and 4 while 1 and 2 are still queued.

    An absorbed request changes nothing — a live row already carries that work — so it must not
    spend the cap. If it did, a stalled worker would make every limited run re-report the same two
    documents and the rest of the corpus would never be reached, while the line still read
    `queued 2 new row(s)`.

    All three of `asked_for`, `new_rows` and `already_queued` are pinned here because the second run
    is the shape that separates them: the absorbed documents were still asked for, so they count in
    `asked_for` and in `already_queued` while `new_rows` — the number `--limit` caps — stays at 2.
    Pinning two of the three is how the asked-for count was able to sit equal to `new_rows` through
    a unit suite of over a thousand tests, under a field name that promised the other thing.
    """
    papra = FakeListing()
    for index in range(1, 5):
        papra.add(f"doc-{index}", content_hash=f"{index:02}" * 32)

    first = await backfill_corpus(store, papra, orgs=[ORG], limit=2)
    assert (first.asked_for, first.new_rows, first.already_queued) == (2, 2, 0)
    assert first.remaining == 2

    second = await backfill_corpus(store, papra, orgs=[ORG], limit=2)

    assert (second.asked_for, second.new_rows, second.already_queued) == (4, 2, 2)
    assert second.remaining == 0
    assert store.count_jobs("index") == 4


def test_the_report_names_the_rows_it_only_found_already_queued(store, capsys):
    """The second run over one corpus prints 0 new rows, 4 asked for, 4 already queued separately.

    The leading number is what changed, because that is what an operator can act on: a corpus whose
    workers are stalled produces a run that asked for everything and changed nothing, and a headline
    that led with the asked-for count would read as progress. The asked-for count is printed too,
    since it is what `asked_for` counts, and the parenthesis is what makes the totals add up.
    """
    papra = FakeListing()
    for index in range(1, 5):
        papra.add(f"doc-{index}", content_hash=f"{index:02}" * 32)
    assert run(store, papra) == EXIT_OK        # the first pass queues all four
    capsys.readouterr()

    assert run(store, papra) == EXIT_OK

    out = capsys.readouterr().out
    assert "queued 0 new row(s) across 0 bucket(s)" in out
    assert "asked for 4 of 4 candidate(s) (4 already queued, unchanged)" in out


async def test_the_machine_payload_carries_two_different_counts(store):
    """The serialized result has to keep `asked_for` and `new_rows` apart, not clone them.

    `backfill` prints prose and has no `--json` flag, so the assertion runs the result through the
    project's own machine serializer (`_dump_json`, the one `jobs --json` uses) over
    `dataclasses.asdict` — the payload any machine output of this command would be built from, and
    the surface where two fields that can never differ get read as two populations. The whole run is
    done with the cap off so the second pass absorbs every document.
    """
    import dataclasses
    import json

    from papra_suite.__main__ import _dump_json

    papra = FakeListing()
    for index in range(1, 5):
        papra.add(f"doc-{index}", content_hash=f"{index:02}" * 32)
    await backfill_corpus(store, papra, orgs=[ORG])

    again = await backfill_corpus(store, papra, orgs=[ORG])

    payload = dataclasses.asdict(again)
    payload["orgs_queued"] = sorted(payload["orgs_queued"])
    machine = json.loads(_dump_json(payload))

    assert (machine["asked_for"], machine["new_rows"], machine["already_queued"]) == (4, 0, 4)
    assert machine["asked_for"] != machine["new_rows"]
    assert machine["asked_for"] == machine["new_rows"] + machine["already_queued"]
