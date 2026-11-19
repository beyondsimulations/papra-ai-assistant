"""Task 15 against the real Papra: `backfill` enrols from a live listing, `jobs` reads and repairs.

Why these need the stack at all, when the unit suite covers the logic: the two things that cannot be
proven against a double are the listing's own shape and SQLite's own constraints.

* **The listing.** `backfill` trusts `originalSha256Hash` from Papra's page and only downloads when
  it is absent. A fake that always supplies one, or never does, cannot show which field a real Papra
  answers under, at what page size, or that a real org's documents are the ones that come back.
  Getting this wrong is not a crash — it is every document in the corpus queued under a hash the job
  then fails to verify.
* **The multi-row tuple.** `jobs --reset` refusing to sweep a `(kind, org, document, content_hash)`
  tuple, and `enqueue_job` taking a fresh row behind a spent one, are behaviour of the *partial*
  unique index migration 004 built. That is asserted here against the schema the migration actually
  produced, not against a re-creation of it.

Neither command talks to Kreuzakt, so neither test needs it up: enqueuing work is the whole extent
of `backfill`, and `jobs` reads one table. `patches` staying empty is the assertion that neither
wrote a byte to Papra, which matters because Papra fills a PDF's text layer by itself and a changed
record would not otherwise be evidence of anything.

Run by the controller after the branch merges (`make test-integration`); not runnable from a lane
that must not touch the operator's stack.
"""

import hashlib

import fitz
import pytest

from papra_suite.backfill_cli import EXIT_OK, backfill_corpus
from papra_suite.jobs_cli import EXIT_INCOMPLETE, EXIT_REFUSED, run_jobs
from papra_suite.papra_client import PapraClient
from papra_suite.store import Store
from tests.test_jobs_cli import cli_args as jobs_args

pytestmark = pytest.mark.integration

PDF_BYTES = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\ntrailer\n%%EOF"


class CountingPapra(PapraClient):
    """Papra, with every read and write counted.

    `downloads` is the one that decides a test: a backfill that downloaded a body per document when
    the listing carried the hash would be correct and slow, and nothing but the count tells you.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.patches = []
        self.downloads = 0
        self.lists = 0

    async def list_documents(self, org_id, **kwargs):
        self.lists += 1
        return await super().list_documents(org_id, **kwargs)

    async def download_file(self, org_id, document_id):
        self.downloads += 1
        return await super().download_file(org_id, document_id)

    async def update_document(self, org_id, document_id, **body):
        self.patches.append((document_id, body))
        return await super().update_document(org_id, document_id, **body)


def make_pdf(marker: str) -> bytes:
    """A one-page PDF with a real text layer, so Papra's own extraction has something to find."""
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((72, 72), f"papra-suite task 15 marker {marker}")
    data = pdf.tobytes()
    pdf.close()
    return data


@pytest.fixture
async def live(tmp_path, papra_stack):
    """A store that has never seen this bucket, and a live Papra client pointed at it."""
    base_url, org_id, api_key = papra_stack
    store = Store(tmp_path / "state.db")
    store.migrate()
    papra = CountingPapra(base_url, api_key)
    yield type("Live", (), {"store": store, "papra": papra, "org_id": org_id})()
    await papra.aclose()
    store.close()


async def upload(live, name: str, marker: str) -> dict:
    document = await live.papra.create_document(live.org_id, name, make_pdf(marker))
    return document


# --- backfill against a live listing ------------------------------------------------

async def test_backfill_enrols_a_live_bucket_from_the_listing_without_downloading(live):
    """The documents the sweep has not reached yet, queued with Papra's own hashes.

    Asserted on the hash on the queued row: `run_index_job` verifies the bytes it downloads against
    it, so a hash that is not Papra's turns into a permanent failure with a message about the
    document having moved. And on `downloads == 0`, because the listing carries the hash and paying
    the transfer to recompute it is the doubling the sweep already stopped doing.
    """
    first = await upload(live, "rechnung-1.pdf", "a1")
    second = await upload(live, "rechnung-2.pdf", "b2")

    result = await backfill_corpus(live.store, live.papra, orgs=[live.org_id])

    assert (result.enqueued, result.enrolled, result.remaining) == (2, 2, 0)
    assert live.papra.downloads == 0 and live.papra.patches == []
    jobs = {job.document_id: job for job in live.store.list_jobs(limit=50)
            if job.kind == "index"}
    assert set(jobs) == {first["id"], second["id"]}
    assert jobs[first["id"]].content_hash == first["originalSha256Hash"]
    # The reference exists beside the job, not after it: eligibility is derived from the ledger, so
    # a queued job with no row behind it is work no later pass can see.
    for document_id in jobs:
        ref = live.store.ledger_get(live.org_id, document_id)
        assert ref is not None and ref.file_hash == jobs[document_id].content_hash
    assert live.store.ledger_stats()["unledgered"] == 0


async def test_backfill_is_idempotent_against_a_live_bucket(live):
    """Running it twice queues the work once, against the real partial unique index.

    The unit double cannot show this: the second enqueue has to be absorbed by the *live* row that
    migration 004's index admits, and a mistake here is a corpus queued twice — one Kreuzakt upload
    and one VLM extraction per document, twice.
    """
    await upload(live, "rechnung-1.pdf", "c3")

    again = await backfill_corpus(live.store, live.papra, orgs=[live.org_id])

    assert again.enqueued == 1
    assert live.store.count_jobs("index") == 1
    # A terminal row does not block a retry — that is the point of migration 004 — but it does not
    # make the second run queue a document that is already queued either.
    job = live.store.list_jobs(kind="index")[0]
    live.store.claim_next_job("index")
    live.store.mark_job_failed(job.id, "kreuzakt refused", max_attempts=1)
    third = await backfill_corpus(live.store, live.papra, orgs=[live.org_id])
    assert third.enqueued == 1 and live.store.count_jobs("index") == 2


async def test_backfill_limit_leaves_a_counted_remainder_against_live_data(live):
    """`--limit 1` over three real documents prints `2 remaining`, measured not guessed."""
    for index in range(3):
        await upload(live, f"rechnung-{index}.pdf", f"d{index}")

    result = await backfill_corpus(live.store, live.papra, orgs=[live.org_id], limit=1)

    assert (result.enqueued, result.remaining, result.candidates) == (1, 2, 3)
    assert live.store.count_jobs("index") == 1


async def test_backfill_dry_run_leaves_a_live_bucket_untouched(live):
    """The rehearsal against real documents: no job, no reference, no seen row, no write."""
    document = await upload(live, "rechnung-1.pdf", "e5")

    result = await backfill_corpus(live.store, live.papra, orgs=[live.org_id], dry_run=True)

    assert result.enqueued == 1 and result.remaining == 0
    assert live.store.list_jobs() == []
    assert live.store.ledger_get(live.org_id, document["id"]) is None
    assert live.store.document_seen(live.org_id, document["id"]) is False
    assert live.papra.patches == []


# --- the queue, against the schema the migrations produced -------------------------

def seed_failed_index_job(store, org, document_id, *, error="kreuzakt queue error: dead"):
    content_hash = hashlib.sha256(PDF_BYTES).hexdigest()
    job_id = store.enqueue_job(kind="index", papra_org_id=org, document_id=document_id,
                               content_hash=content_hash, payload={"name": "x.pdf"})
    store.claim_next_job("index")
    store.mark_job_failed(job_id, error, max_attempts=1)
    return job_id, content_hash


def test_jobs_lists_resets_and_drains_against_a_real_store(tmp_path):
    """The whole operator surface on a migrated `state.db`, in the order it is used.

    Read, decide, act, prove: `--status failed` to find the row, `--reset` to revive exactly that
    row, `--drain` to prove the queue is quiet. `get_job` afterwards is the assertion that matters —
    the printed sentence is not the state.
    """
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id="org-live", name="Live")
    try:
        job_id, content_hash = seed_failed_index_job(store, "org-live", "doc-live-1")
        assert store.claim_next_job("index") is None

        assert run_jobs(None, jobs_args(status="failed"), store=store) == EXIT_OK
        assert run_jobs(None, jobs_args(reset=job_id), store=store) == EXIT_OK
        assert store.get_job(job_id).status == "pending"
        assert store.get_job(job_id).error is None
        assert store.claim_next_job("index").id == job_id

        # Running: `--drain` must refuse, and `--reset` must not reach into the worker's hands.
        assert run_jobs(None, jobs_args(drain=True), store=store) == EXIT_INCOMPLETE
        assert run_jobs(None, jobs_args(reset=job_id), store=store) == EXIT_REFUSED
        assert store.get_job(job_id).status == "running"

        store.mark_job_done(job_id)
        assert run_jobs(None, jobs_args(drain=True), store=store) == EXIT_OK
    finally:
        store.close()


def test_several_rows_of_one_live_tuple_are_all_visible_and_only_one_is_reset(tmp_path):
    """The R1 case, on the index migration 004 actually built.

    A spent row and the live row the next request took are both listed, `--document-id` answers with
    both, and reviving the older one defers to the live row by name instead of reviving it.
    """
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id="org-live", name="Live")
    try:
        spent, content_hash = seed_failed_index_job(store, "org-live", "doc-tuple")
        live_id = store.enqueue_job(kind="index", papra_org_id="org-live",
                                    document_id="doc-tuple", content_hash=content_hash,
                                    payload={})
        assert live_id != spent

        rows = store.list_jobs(document_id="doc-tuple")
        assert {row.id for row in rows} == {spent, live_id}
        assert store.count_jobs("index") == 2

        assert run_jobs(None, jobs_args(reset=spent), store=store) == EXIT_INCOMPLETE
        assert store.get_job(spent).status == "failed"
        assert store.get_job(live_id).status == "pending"
    finally:
        store.close()


async def test_backfill_of_a_live_bucket_the_store_knows_from_the_ledger_only(live):
    """A bucket that is in the ledger but not in `orgs` is still walked by the default scope.

    The delete pass already treats the ledger as the authority on which buckets exist, for the
    reason stated there: references outlive the `orgs` row. A backfill scoped to configured
    mailboxes alone would leave those documents unqueued and silent, so the scope here is whatever
    the store knows,
    and the document still comes back queued and enrolled.
    """
    document = await upload(live, "rechnung-orphan.pdf", "f7")
    live.store.ledger_upsert_reference(papra_org_id=live.org_id,
                                       papra_document_id=document["id"],
                                       file_hash=document["originalSha256Hash"],
                                       papra_original_name="rechnung-orphan.pdf")

    result = await backfill_corpus(live.store, live.papra,
                                   orgs=live.store.ledger_org_ids())

    assert result.enqueued == 1
    assert [job.document_id for job in live.store.list_jobs(limit=50)
            if job.kind == "index"] == [document["id"]]
