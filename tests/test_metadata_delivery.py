"""Task 8: metadata delivery — name, date, notes — on Papra-side baselines.

Three things give this file its shape, and all three are about who owns a field a human
can see and edit.

**The baseline is Papra's own name at first sight, never Kreuzakt's
`original_filename`.** We upload under a synthetic `papra-<docId>-<hash8>.pdf`, so
Kreuzakt's filename describes our upload and proves nothing about ownership. Every
rename test below therefore sets what *Papra* holds and expects the verdict to follow
it, which is what kills the mutant that reads the other column.

**A synthetic name is refused on both sides of the comparison.** On a keyless index the
model title is derived from our upload filename, so a naive pass renames the corpus to
our own garbage and then records it as authoritative in `written_name`. Measured on the
pinned container on 2026-11-18, that title is not even the raw name:
`papra-doc-1-a1b2c3d4.pdf` comes back as `Papra Doc 1 A1b2c3d4`, and
`papra-doc-1-a1b2c3d4 (1).pdf` as `Papra Doc 1 A1b2c3d4 (1)`. So the guard matches the
name, its collision suffix and its prettified form, before the ownership question.

**Protection is durable and logs once.** The durable flag, not the caller's snapshot, is
the memory: three protected calls produce no PATCH and one line.

Also measured against the pinned Papra and encoded rather than guessed:
`PATCH documentDate="2025-10-01"` is accepted and reads back as
`"2025-10-01T00:00:00.000Z"`; `notes` at 2048 chars is accepted while 2049 is a 400; an
empty `name` is a 400. The wire names are `name`, `documentDate`, `notes`.
"""

import hashlib
import json
import logging
import sqlite3
import sys
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest

from papra_suite import indexing
from papra_suite.indexing import (
    OwnershipVerdict,
    deliver_metadata,
    is_synthetic_upload_name,
    looks_like_synthetic_upload_name,
    normalize_date,
    ownership,
    synthetic_upload_name,
)
from papra_suite.kreuzakt_client import KreuzaktError, KzDocument
from papra_suite.papra_client import PapraClient, PapraError
from papra_suite.store import Store
from tests.test_config import BASE_ENV
from tests.test_index_job import DOC_ID, HASH, NAME, ORG, FakeKreuzakt, FakePapra, make_job

#: Our upload name for the standing reference: `papra-<documentId>-<hash8>.pdf`.
UPLOAD_NAME = synthetic_upload_name(DOC_ID, HASH)
#: What the keyless container answers as the title for exactly that upload (measured).
MEASURED_PRETTIFIED_TITLE = "Papra Doc 1 " + HASH[:8].capitalize()
NOTE = "Rechnung ueber 1.234,56 Euro"


def kz_doc(*, title="Rechnung 4711", description=NOTE, document_date="2025-10-01",
           content="Rechnung Nr. 4711", doc_id=7) -> KzDocument:
    """A Kreuzakt document, with the three delivered fields set to ordinary values.

    `original_filename` is our synthetic upload name on purpose: that is what the real
    server reports for our uploads, so a test that started measuring ownership against
    it would at least be honest about the shape of the input it got wrong.
    """
    return KzDocument(id=doc_id, title=title, description=description, content=content,
                      document_date=document_date, original_filename=UPLOAD_NAME,
                      updated_at="2026-11-18 09:00:00")


def seed_written(store, **values) -> None:
    """Say to the store what this bridge last sent: the row is what ownership is measured on."""
    store.ledger_record_metadata(papra_org_id=ORG, papra_document_id=DOC_ID, **values)


def ref_seeded(store, **over):
    """`ref_with`, made durable: write the claimed state into the store, return the row.

    `deliver_metadata` answers the ownership question from the ledger rather than from the
    snapshot its caller hands it — deliberately, and the comment in `deliver_metadata` says
    why — so a delivery test that means "we sent this last time" has to say so to the store.
    Handing back the row is also what makes the test look like the real caller: the worker and
    the CLI both hold a row read from this same table.
    """
    ref = ref_with(**over)
    for field in ("name", "date", "notes"):
        value = getattr(ref, f"written_{field}")
        if value is not None:
            seed_written(store, **{field: value})
        if getattr(ref, f"{field}_protected"):
            store.ledger_protect(papra_org_id=ORG, papra_document_id=DOC_ID, field=field)
    return store.ledger_get(ORG, DOC_ID)


def kz_only(**over) -> KzDocument:
    """A Kreuzakt document with nothing to deliver except the fields named here.

    A one-field test that leaves the other two full would have to assert about writes it
    never meant to make: `patch_calls == 0` is the assertion that says "this verdict cost
    Papra nothing", and it is only honest when the other candidates were empty too.
    """
    base = dict(title="", description="", document_date=None)
    return kz_doc(**{**base, **over})


def ref_with(**over):
    """A `LedgerRef` with the ownership columns defaulted and the rest filled in."""
    base = dict(papra_org_id=ORG, papra_document_id=DOC_ID, kreuzakt_id=7, file_hash=HASH,
                papra_original_name=NAME, delivered_at=None, delivered_sha=None,
                delivered_generation=1, written_name=None, name_protected=False,
                written_date=None, date_protected=False, written_notes=None,
                notes_protected=False, repair_count=0, last_repair_at=None,
                deleted_at=None, cleanup_state="none", cleanup_attempts=0,
                cleanup_next_at=None)
    return SimpleNamespace(**{**base, **over})


class ScriptedIndex:
    """The index, serving one document for every id — the only read the CLI performs.

    A purpose-built double rather than `FakeKreuzakt` because the CLI's Kreuzakt surface
    is a single `get_document`, and the fixture-served title in `FakeKreuzakt` is a
    *different* synthetic name, which would make these tests read as guard tests.
    """

    def __init__(self, document=None, missing=()) -> None:
        self.document = document or kz_doc()
        self.missing = set(missing)
        self.reads: list[int] = []

    async def get_document(self, document_id: int) -> KzDocument:
        self.reads.append(document_id)
        if document_id in self.missing:
            raise KreuzaktError(f"Document {document_id} not found", 404)
        return replace(self.document, id=document_id)


class MetadataPapra(FakePapra):
    """Task 6's fake, writing back under Papra's own field names.

    `PapraClient.update_document` maps `document_date` onto `documentDate` before the
    wire, so a fake that stored the Python kwarg would put a column Papra has never had
    into the record — and the next ownership comparison, which reads Papra's record,
    would be measuring against a fiction. Two measured behaviours are mirrored for the
    same reason: a bare ISO date comes back as a UTC timestamp, and the last write is
    kept as the body that went out (`last_patch`), which is what "only the fields that
    changed" is asserted against.
    """

    TO_WIRE = {"document_date": "documentDate"}

    def __init__(self):
        super().__init__()
        self.documents[(ORG, DOC_ID)].update({"documentDate": None, "notes": None})

    @property
    def last_patch(self) -> dict:
        return self.patched[-1][2] if self.patched else {}

    async def update_document(self, org_id, document_id, **body) -> dict:
        wire = {self.TO_WIRE.get(key, key): value for key, value in body.items()}
        fields = set(wire)
        assert fields <= {"name", "documentDate", "notes"} or fields == {"content"}, \
            f"a write mixed the metadata fields with a column this task does not own: " \
            f"{sorted(fields)}"
        self.patch_calls += 1
        self.patched.append((org_id, document_id, wire))
        if self.raise_on_update is not None:
            raise self.raise_on_update
        stored = dict(wire)
        if stored.get("documentDate"):          # measured: Papra stores a UTC timestamp
            stored["documentDate"] = f"{stored['documentDate'][:10]}T00:00:00.000Z"
        self.documents[(org_id, document_id)].update(stored)
        return self.documents[(org_id, document_id)]


class GateClosingIndex(ScriptedIndex):
    """The delivery flag flips off in another process while the CLI walks the scope.

    Flips between the first and the second reference, so the walk has already written one
    document by the time it meets the closed gate — the case whose report must survive.
    """

    def __init__(self, store, document=None) -> None:
        super().__init__(document)
        self.store = store

    async def get_document(self, document_id: int) -> KzDocument:
        if len(self.reads) == 1:
            self.store.set_setting("index_metadata_delivery", "false")
        return await super().get_document(document_id)


class WirePapra:
    """The real `PapraClient`, with only the reads this module performs scripted.

    A thin proxy rather than the fake, because that test's claim is about the body the
    real client puts on the wire — which is the thing Papra acts on, and which a fake
    that accepts `**body` cannot show.
    """

    def __init__(self, client: PapraClient) -> None:
        self._client = client
        self.patch_calls = 0
        self.documents = {(ORG, DOC_ID): {"id": DOC_ID, "organizationId": ORG,
                                          "name": NAME, "mimeType": "application/pdf",
                                          "content": "", "documentDate": None,
                                          "notes": None, "originalSha256Hash": HASH,
                                          "isDeleted": False, "deletedAt": None}}

    async def get_document(self, org_id, document_id):
        return dict(self.documents[(org_id, document_id)])

    async def update_document(self, org_id, document_id, **body):
        self.patch_calls += 1
        return await self._client.update_document(org_id, document_id, **body)


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=ORG, name="Org 1")
    seed_reference(store)
    yield store
    store.close()


def seed_reference(store, *, org=ORG, document_id=DOC_ID, name=NAME,
                   kreuzakt_id=7) -> None:
    """The ledger row the index job leaves behind: bound, with a Papra-side baseline."""
    store.ledger_upsert_reference(papra_org_id=org, papra_document_id=document_id,
                                  file_hash=HASH, papra_original_name=name)
    store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=document_id,
                                 kreuzakt_id=kreuzakt_id, generation=1)


def seed_papra_document(fake_papra, document_id: str, *, org=ORG, name=None,
                        notes=None, document_date=None) -> None:
    fake_papra.documents[(org, document_id)] = {
        "id": document_id, "organizationId": org, "name": name or f"{document_id}.pdf",
        "mimeType": "application/pdf", "content": "", "documentDate": document_date,
        "notes": notes, "originalSha256Hash": HASH, "isDeleted": False, "deletedAt": None}


@pytest.fixture
def fake_papra():
    return MetadataPapra()


@pytest.fixture
async def fake_kz():
    fake = FakeKreuzakt()
    yield fake
    await fake.aclose()


def open_gate(store):
    store.set_setting("index_metadata_delivery", "true")
    return store


# --------------------------------------------------------------------------------
# Step 1a: the ownership question, asked of one readable function
# --------------------------------------------------------------------------------

def test_ownership_refuses_a_field_the_ledger_marks_protected():
    assert ownership(ref_with(name_protected=True), field="name",
                     current=NAME) == OwnershipVerdict(False, "protected")


def test_ownership_of_the_name_baseline_is_papras_first_sight_name():
    """`papra_original_name` is the baseline; Kreuzakt's `original_filename` is not.

    Both columns hold a filename, and only one of them was ever typed by a human. The
    other is our own `papra-<docId>-<hash8>.pdf`, which every document in the corpus
    shares the shape of; treating it as the baseline is how a rename pass stops ever
    asking the question it exists to ask.
    """
    assert ownership(ref_with(), field="name", current=NAME).owned is True
    assert ownership(ref_with(), field="name", current=UPLOAD_NAME).owned is False


def test_ownership_of_a_written_field_is_papra_still_holding_our_string():
    assert ownership(ref_with(written_name="Rechnung 4711"), field="name",
                     current="Rechnung 4711").owned is True
    assert ownership(ref_with(written_name="Rechnung 4711"), field="name",
                     current="Meine Steuer 2025") == OwnershipVerdict(False, "not-ours")


def test_first_write_of_date_and_notes_requires_an_empty_papra_value():
    """No baseline exists for these two, so a value we did not write is not ours.

    Papra runs its own date detection, so a non-empty `documentDate` on a document we
    never touched may be Papra's; nothing here can tell that from a human's, and the
    conservative answer is to leave it alone.
    """
    assert ownership(ref_with(), field="date", current=None) == OwnershipVerdict(True, "")
    assert ownership(ref_with(), field="date",
                     current="2024-05-05T00:00:00.000Z") == OwnershipVerdict(
        False, "empty-required")
    assert ownership(ref_with(), field="notes", current="") == OwnershipVerdict(True, "")
    assert ownership(ref_with(), field="notes",
                     current="Von Hand notiert").owned is False


def test_a_synthetic_name_in_papra_is_never_treated_as_ours():
    """Even against a baseline that matches: a synthetic name proves nothing.

    If `papra_original_name` itself came in looking like our upload name — a document
    re-uploaded from one of our own exports, say — a bare baseline comparison would
    read "Papra still holds what we first saw" and take that as licence to rename.
    """
    ref = ref_with(papra_original_name=UPLOAD_NAME)
    assert ownership(ref, field="name", current=UPLOAD_NAME) == OwnershipVerdict(
        False, "synthetic-name")


def test_the_synthetic_name_helpers_know_our_own_shapes():
    assert synthetic_upload_name("doc-1", HASH) == f"papra-doc-1-{HASH[:8]}.pdf"
    assert is_synthetic_upload_name(UPLOAD_NAME, document_id=DOC_ID, file_hash=HASH)
    assert is_synthetic_upload_name(f"papra-doc-1-{HASH[:8]} (2).pdf",
                                    document_id=DOC_ID, file_hash=HASH)
    assert is_synthetic_upload_name(UPLOAD_NAME[:-4], document_id=DOC_ID, file_hash=HASH)
    # the container's prettified title for the same upload (measured)
    assert is_synthetic_upload_name("Papra Doc 1 " + HASH[:8].capitalize(),
                                    document_id=DOC_ID, file_hash=HASH)
    assert not is_synthetic_upload_name("Rechnung 4711", document_id=DOC_ID,
                                        file_hash=HASH)
    # another document's upload name is not this one's
    assert not is_synthetic_upload_name(synthetic_upload_name("doc-2", HASH),
                                        document_id=DOC_ID, file_hash=HASH)
    assert looks_like_synthetic_upload_name("papra-doc-9-deadbeef.pdf")
    assert not looks_like_synthetic_upload_name("papra-rechnung-4711.pdf")


# --------------------------------------------------------------------------------
# Step 1b: `normalize_date` — the model's wording in, an ISO date or nothing out
# --------------------------------------------------------------------------------

@pytest.mark.parametrize("value, expected", [
    ("2025-10-01", "2025-10-01"),
    ("  2025-10-01 ", "2025-10-01"),
    (None, None),
    ("", None),
    ("   ", None),
    ("01.10.2025", None),
    ("Oktober 2025", None),
    ("2025-10-1", None),
    ("2025-13-01", None),      # a real-looking string is not a real date
    ("2025-02-30", None),
    ("2025-10-01T00:00:00.000Z", None),   # Papra's form, not the model's
])
def test_normalize_date_accepts_an_iso_calendar_date_and_nothing_else(value, expected):
    assert normalize_date(value) == expected


# --------------------------------------------------------------------------------
# Step 1c: the gate, the deleted document, and the shared write boundary
# --------------------------------------------------------------------------------

async def test_the_metadata_gate_refuses_every_caller_while_the_flag_is_off(store,
                                                                            fake_papra):
    """The gate lives in the writer, and it refuses before it reads.

    A gate that queries first and refuses second still puts a GET on the record it is
    not allowed to touch; a gate that lives only in the scheduler is bypassed by the
    CLI and by whatever path is added next.
    """
    store.set_setting("index_metadata_delivery", "false")
    reads_before = fake_papra.get_calls

    with pytest.raises(indexing.DeliveryDisabled):
        await deliver_metadata(fake_papra, store, ref_with(), kz_doc())

    assert fake_papra.patch_calls == 0
    assert fake_papra.get_calls == reads_before


async def test_the_gate_is_read_from_the_store_on_every_call(store, fake_papra):
    """Flipping the setting mid-process moves the writer: the handover has no restart."""
    with pytest.raises(indexing.DeliveryDisabled):
        await deliver_metadata(fake_papra, store, ref_with(), kz_doc())

    open_gate(store)
    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_doc())).name_action == "written"


async def test_metadata_is_never_written_into_a_document_papra_says_is_deleted(store,
                                                                               fake_papra):
    """The same containment as content, from the same single definition of "deleted".

    Papra resurrects a trashed document when a column changes, so writing metadata into
    one un-deletes it on purpose. The ledger still calls the reference live until
    Task 9's pass owns it, so this is the only thing standing in the way.
    """
    open_gate(store)
    fake_papra.document["isDeleted"] = True

    with pytest.raises(indexing.IndexFailed):
        await deliver_metadata(fake_papra, store, ref_with(), kz_doc())

    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).written_name is None


async def test_a_refused_patch_records_nothing_and_leaves_the_field_retriable(store,
                                                                             fake_papra):
    """Write, then record — so a refusal claims nothing and the next pass retries.

    A delivery that recorded before Papra answered would leave the ledger certain of a
    name Papra never got, and every later pass would call that ours.
    """
    open_gate(store)
    fake_papra.raise_on_update = PapraError("PATCH failed: 400 no", 400)

    with pytest.raises(PapraError):
        await deliver_metadata(fake_papra, store, ref_with(), kz_doc())

    assert store.ledger_get(ORG, DOC_ID).written_name is None

    fake_papra.raise_on_update = None
    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_doc())).name_action == "written"
    assert store.ledger_get(ORG, DOC_ID).written_name == "Rechnung 4711"


async def test_a_lost_record_after_a_landed_patch_names_the_values_it_lost(store, fake_papra,
                                                                          caplog):
    """The one ordering hole left: Papra has the values, the ledger never heard about them.

    If `ledger_record_metadata` fails after the PATCH landed, the next pass reads our own
    write as somebody else's and protects the field for good. This test pins the recovery
    rather than the damage: the ERROR line has to name the values, because the only way back
    is writing that history by hand, and a log line that says "recording failed" cannot be
    acted on.

    The exception still travels. Nothing here pretends to have made the write durable, and no
    provisional "maybe delivered" state is invented: a column that claimed a value might be
    ours is a licence to overwrite a human's, which is the one unrecoverable act on this path.
    """
    open_gate(store)

    def lose(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    store.ledger_record_metadata = lose
    caplog.set_level(logging.ERROR, logger="papra_suite.indexing")

    with pytest.raises(sqlite3.OperationalError):
        await deliver_metadata(fake_papra, store, ref_with(), kz_doc())

    assert fake_papra.patch_calls == 1          # Papra really was written
    assert store.ledger_get(ORG, DOC_ID).written_name is None
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "Rechnung 4711" in text and NOTE in text and "2025-10-01" in text, text


# --------------------------------------------------------------------------------
# Step 1d: `name` — the one field a human definitely edits
# --------------------------------------------------------------------------------

async def test_rename_allowed_when_papra_holds_our_last_name(store, fake_papra):
    open_gate(store)
    fake_papra.document["name"] = "Altes Auto-Title.pdf"
    ref = ref_seeded(store, written_name="Altes Auto-Title.pdf")

    assert (await deliver_metadata(fake_papra, store, ref,
                                  kz_doc())).name_action == "written"
    assert store.ledger_get(ORG, DOC_ID).written_name == "Rechnung 4711"


async def test_first_rename_allowed_when_papra_still_holds_its_own_original_name(store,
                                                                                fake_papra):
    """Baseline is Papra's name at first sight — NOT Kreuzakt's `original_filename`,
    which is our synthetic papra-<id>-<hash>.pdf and proves nothing.
    """
    open_gate(store)
    fake_papra.document["name"] = NAME

    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_doc())).name_action == "written"


async def test_human_rename_protects_permanently_and_logs_once(store, fake_papra, caplog):
    open_gate(store)
    caplog.set_level(logging.DEBUG, logger="papra_suite.indexing")
    fake_papra.document["name"] = "Meine Steuer 2025"

    for _ in range(3):
        assert (await deliver_metadata(fake_papra, store, ref_with(),
                                      kz_only(title="Rechnung 4711"))).name_action == \
            "protected"

    assert fake_papra.patch_calls == 0
    assert sum("protecting name" in record.getMessage()
               for record in caplog.records) == 1
    assert store.ledger_get(ORG, DOC_ID).name_protected is True


async def test_protection_blocks_the_rename_even_for_a_caller_holding_a_stale_ref(store,
                                                                                 fake_papra):
    """The caller's snapshot is not the authority on protection.

    The worker's `ref` was read at job start; a CLI run in another process can have
    protected the field since. Taking the more protective of the two views costs one
    ledger read and closes the window in which a stale read overwrites a human's work.
    """
    open_gate(store)
    store.ledger_protect(papra_org_id=ORG, papra_document_id=DOC_ID, field="name")
    fake_papra.document["name"] = "Von Hand umbenannt"

    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_only(title="Rechnung 4711"))).name_action == \
            "protected"
    assert fake_papra.patch_calls == 0


async def test_a_stale_written_value_does_not_make_a_human_value_ours(store, fake_papra):
    """The ledger row is the authority in both directions, not only for protection.

    The caller's snapshot remembers writing `scan-0001.pdf`. Since it was read, a later
    delivery put `Rechnung 4711` there, and now Papra holds `scan-0001.pdf` again. Believing
    the stale column would read that as "still ours" and rename over whoever put the old name
    back — which is why a conservative OR of the two views is not enough: the older view can
    claim a field the newer one knows we gave up.
    """
    open_gate(store)
    seed_written(store, name=NAME)
    seed_written(store, name="Rechnung 4711")           # a later delivery, another process
    stale = ref_with(written_name=NAME)                 # the caller's older view
    assert stale.written_name == NAME
    fake_papra.document["name"] = NAME

    assert (await deliver_metadata(fake_papra, store, stale,
                                  kz_only(title="Neuer Titel"))).name_action == "protected"
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).name_protected is True


async def test_the_ledger_row_is_required_rather_than_the_callers_memory(store, fake_papra):
    """No row, no write — even though the caller is holding a reference.

    Every caller reads its reference out of this table, so a row that has gone away means
    someone deleted the reference and the history that says which fields are ours went with
    it. Falling back to the caller's snapshot there would deliver metadata on the strength of
    history that was deliberately removed, and a permanent failure is the only answer that
    says so to the retry loop as well as to the operator.
    """
    open_gate(store)
    orphan = ref_with(papra_document_id="doc-gone")
    seed_papra_document(fake_papra, "doc-gone", name=NAME)

    with pytest.raises(indexing.IndexFailed):
        await deliver_metadata(fake_papra, store, orphan, kz_only(title="Rechnung 4711"))

    assert fake_papra.patch_calls == 0


def test_a_permanently_failed_index_row_no_longer_blocks_the_next_enqueue(store):
    """The trap this build used to have, pinned so its inversion is on the record.

    The verdict that puts a row in `failed` is permanent — `worker_loop` writes it with
    `mark_job_failed(..., max_attempts=1)` — and for the whole of this file's history
    `enqueue_job` was `INSERT OR IGNORE` against a *whole-table* unique
    `idx_jobs_idempotency (kind, org, document, content_hash)`. From that moment every
    later enqueue for the same bytes was a silent no-op returning the failed row's id,
    and every re-admission move works by enqueuing exactly that tuple: the restore and
    unbind passes, the rebuild, and Task 15's backfill. A restore could report success
    and enqueue nothing.

    Migration 004 inverts this on purpose: the index is unique over live rows only, so
    the spent row stops being a lock, the next enqueue is a row that can be claimed,
    and the failure keeps its own row as the record it is. The escape hatch this test
    used to point at (`jobs --reset` on that row) is now a convenience rather than the
    only way out.
    """
    job_id = store.enqueue_job(kind="index", papra_org_id=ORG, document_id=DOC_ID,
                               content_hash=HASH, payload={"file_hash": HASH})
    store.mark_job_failed(job_id, "no ledger reference", max_attempts=1)
    assert store.get_job(job_id).status == "failed"

    again = store.enqueue_job(kind="index", papra_org_id=ORG, document_id=DOC_ID,
                              content_hash=HASH, payload={"file_hash": HASH,
                                                          "ingested_via": "restore"})

    assert again != job_id, "the spent row absorbed the enqueue: 004's predicate is gone"
    claimed = store.claim_next_job("index")
    assert claimed is not None and claimed.id == again
    rows = [job for job in store.list_jobs() if job.kind == "index"]
    assert len(rows) == 2
    assert store.get_job(job_id).status == "failed"


async def test_a_permanently_failed_index_job_names_what_it_leaves_behind(store, caplog):
    """The consequence has to be in the log, not only in a plan document.

    `check`'s counters are all ledger-derived, so a `done` job flipping to `failed` moves no
    number an operator reads; the ERROR line is the only place this failure is written down.
    It must therefore say what the failure costs — that nothing will ask about these bytes
    again by itself — otherwise the operator reads "failed" as "it will try again". What it
    must not do is blame the queue: since migration 004 a spent row does not block a fresh
    request, and a message that sent an operator to untangle a lock that does not exist
    would cost them the actual cause, which is a ledger row that reads as finished.
    """
    from papra_suite.pipeline import worker_loop

    job_id = store.enqueue_job(kind="index", papra_org_id=ORG, document_id=DOC_ID,
                               content_hash=HASH, payload={"file_hash": HASH})

    async def handler(job):
        raise indexing.IndexFailed("no ledger reference: history is gone", permanent=True)

    original_claim = store.claim_next_job
    claims = {"n": 0}

    def claim(kind):
        claims["n"] += 1
        if claims["n"] > 1:
            raise _StopWorkerLoop()
        return original_claim(kind)

    store.claim_next_job = claim
    caplog.set_level(logging.ERROR, logger="papra_suite.pipeline")
    try:
        with pytest.raises(_StopWorkerLoop):
            await worker_loop(store, kind="index", handler=handler)
    finally:
        store.claim_next_job = original_claim

    assert store.get_job(job_id).status == "failed"
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "jobs --reset" in text, text
    assert DOC_ID in text


async def test_a_stale_snapshot_does_not_block_a_field_the_ledger_still_owns(store,
                                                                             fake_papra):
    """The other direction: a caller remembering an older value must not cost us the field.

    The caller believes we last wrote `scan-0001.pdf`; a later delivery put `Rechnung 4711`
    there and Papra still holds it. Reading the row gives the honest answer — ours — while
    reading the snapshot would call this a human edit and protect a field we are holding. The
    pair with the test above is what makes "the row wins" more than a slogan: it wins in the
    direction that stops a write and in the direction that permits one.
    """
    open_gate(store)
    seed_written(store, name=NAME)
    seed_written(store, name="Rechnung 4711")
    stale = ref_with(written_name=NAME)
    fake_papra.document["name"] = "Rechnung 4711"

    assert (await deliver_metadata(fake_papra, store, stale,
                                   kz_only(title="Rechnung 4711 Steuer"))).name_action == \
        "written"
    assert fake_papra.last_patch["name"] == "Rechnung 4711 Steuer"


def test_the_recovery_order_for_a_field_a_human_really_edited(store, fake_papra, tmp_path,
                                                             capsys):
    """The CLI sequence that actually returns an edited field to automatic delivery.

    Not one command but two, in this order, and pinned as a sequence because each command's
    own contract is defensible and the combination is what an operator needs: `--force` writes
    the index's value over the human's and leaves the flag set, `--restore-auto` then drops the
    flag, and only a following ordinary delivery shows `unchanged` with protection off. Doing
    it the other way round — clearing first — is the test above: the field is re-protected on
    the next pass, and the CLI now prints that order rather than leaving it to be discovered.
    """
    from papra_suite.metadata_cli import run_metadata

    open_gate(store)
    seed_written(store, name=NAME)
    store.ledger_protect(papra_org_id=ORG, papra_document_id=DOC_ID, field="name")
    fake_papra.document["name"] = "Von Hand umbenannt"

    assert run_metadata(configuration(tmp_path),
                        cli_args(force=True, yes_i_mean_it=True, field="name"),
                        store=store, papra=fake_papra, kreuzakt=ScriptedIndex()) == 0
    assert fake_papra.document["name"] == "Rechnung 4711"
    assert store.ledger_get(ORG, DOC_ID).name_protected is True      # force does not clear

    assert run_metadata(configuration(tmp_path),
                        cli_args(restore_auto=True, field="name"), store=store) == 0
    assert store.ledger_get(ORG, DOC_ID).name_protected is False

    fake_papra.patched.clear()
    assert run_metadata(configuration(tmp_path), cli_args(field="name"), store=store,
                        papra=fake_papra, kreuzakt=ScriptedIndex()) == 0
    out = capsys.readouterr().out
    assert "name=unchanged" in out
    assert fake_papra.patched == []                                  # Papra already holds it
    assert store.ledger_get(ORG, DOC_ID).name_protected is False     # automation restored


async def test_clearing_protection_reopens_the_question_without_answering_it(store,
                                                                            fake_papra):
    """`--restore-auto` un-protects; it does not make a human's value ours.

    Pinned because the command's name promises more than it does, and an operator who reads
    "restored automatic delivery" and walks away finds the field protected again on the next
    pass. Clearing the flag re-admits the field to the ownership question; the answer is still
    "Papra holds something we never wrote", so the verdict is still `protected` and the flag is
    set again. Putting the index's value back is `--force --yes-i-mean-it`, which is the
    command that says out loud a human's text is about to be overwritten — and the CLI prints
    that order for a reason.
    """
    open_gate(store)
    seed_written(store, name=NAME)
    store.ledger_protect(papra_org_id=ORG, papra_document_id=DOC_ID, field="name")
    fake_papra.document["name"] = "Von Hand umbenannt"

    assert store.ledger_clear_protection(field="name", org_id=ORG,
                                        document_id=DOC_ID) == 1
    assert store.ledger_get(ORG, DOC_ID).name_protected is False

    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_only(title="Rechnung 4711"))).name_action == "protected"
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).name_protected is True


async def test_synthetic_name_in_papra_is_never_treated_as_ours(store, fake_papra):
    open_gate(store)
    fake_papra.document["name"] = UPLOAD_NAME

    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_only(title="Rechnung 4711"))).name_action == \
            "protected"
    assert fake_papra.patch_calls == 0


async def test_a_rename_that_changes_nothing_asks_for_no_patch(store, fake_papra):
    open_gate(store)
    fake_papra.document["name"] = NAME

    result = await deliver_metadata(fake_papra, store, ref_with(), kz_only(title=NAME))

    assert result.name_action == "unchanged"
    assert fake_papra.patch_calls == 0
    # Recorded even though nothing was sent: `written_name` answers "is Papra still holding
    # what this pass endorses?", and the answer is yes whether or not a PATCH was needed.
    # Leaving it null would make the next human rename indistinguishable from a first write.
    assert store.ledger_get(ORG, DOC_ID).written_name == NAME


async def test_a_blank_title_is_skipped_and_protects_nothing(store, fake_papra):
    """Papra answers 400 for an empty `name` (measured), so it must never be sent.

    And a field the index has nothing to say about is not evidence of a human edit, so
    skipping it must not leave protection behind either.
    """
    open_gate(store)

    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_only(title="  "))).name_action == "skipped"
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).name_protected is False


# --------------------------------------------------------------------------------
# Step 1e: never deliver our own upload name as a title, on either side
# --------------------------------------------------------------------------------

@pytest.mark.parametrize("title", [
    UPLOAD_NAME,                                   # the raw name
    UPLOAD_NAME[:-4],                              # without the extension
    f"papra-doc-1-{HASH[:8]} (1)",                 # collision suffix, no extension
    f"papra-doc-1-{HASH[:8]} (3).pdf",             # collision suffix, with it
    MEASURED_PRETTIFIED_TITLE,                     # what the keyless container reports
    "Papra Doc 1 " + HASH[:8].capitalize() + " (1)",
])
async def test_our_upload_name_is_never_delivered_as_a_title(store, fake_papra, title):
    """The guard fires before the ownership question, and leaves nothing recorded.

    On a keyless deployment the model title is derived from our own upload filename, so
    the "improvement" is a corpus-wide rename to `Papra Doc 1 a1b2c3d4` — and
    `written_name` would then record it as ours, making every later pass treat the
    garbage as the authoritative title. `skipped` is the only verdict that both refuses
    and explains: it is not a human edit, so it must not protect, and it is not a write,
    so it must not record.
    """
    open_gate(store)
    fake_papra.document["name"] = NAME          # ours to rename: Papra holds the baseline

    result = await deliver_metadata(fake_papra, store, ref_with(), kz_only(title=title))

    assert result.name_action == "skipped"
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).written_name is None
    assert store.ledger_get(ORG, DOC_ID).name_protected is False


async def test_a_title_that_only_shares_a_prefix_with_our_name_is_still_delivered(store,
                                                                                 fake_papra):
    """The guard refuses our name, not everything that starts like it."""
    open_gate(store)
    fake_papra.document["name"] = NAME

    assert (await deliver_metadata(
        fake_papra, store, ref_with(),
        kz_doc(title=f"Papra Doc 1 {HASH[:8]} Rechnung 4711"))).name_action == "written"


async def test_the_synthetic_title_refusal_says_which_document_it_blocked(store, fake_papra,
                                                                          caplog):
    open_gate(store)
    caplog.set_level(logging.INFO, logger="papra_suite.indexing")

    await deliver_metadata(fake_papra, store, ref_with(), kz_doc(title=UPLOAD_NAME))

    assert any(DOC_ID in record.getMessage() and "upload name" in record.getMessage()
               for record in caplog.records), caplog.text


async def test_the_refused_title_does_not_stop_the_other_two_fields(store, fake_papra):
    open_gate(store)
    fake_papra.document["name"] = NAME

    result = await deliver_metadata(fake_papra, store, ref_with(),
                                    kz_doc(title=MEASURED_PRETTIFIED_TITLE))

    assert (result.name_action, result.date_action, result.notes_action) == \
        ("skipped", "written", "written")
    assert set(fake_papra.last_patch) == {"documentDate", "notes"}


# --------------------------------------------------------------------------------
# Step 1f: `date`
# --------------------------------------------------------------------------------

async def test_date_first_write_only_when_papra_date_is_empty(store, fake_papra):
    open_gate(store)
    fake_papra.document["documentDate"] = "2024-05-05T00:00:00.000Z"   # Papra inferred it

    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_doc())).date_action == "protected"
    assert store.ledger_get(ORG, DOC_ID).date_protected is True

    # Emptying Papra's column underneath us does not undo it, and that is the rule working
    # rather than a bug: what was protected was the fact that a value we cannot attribute sat
    # there, and a column that went empty is not evidence that the value was ours. The way
    # back is the operator's command, which is exactly what the ledger count below proves.
    fake_papra.document["documentDate"] = None
    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_doc())).date_action == "protected"

    assert store.ledger_clear_protection(field="date", org_id=ORG,
                                        document_id=DOC_ID) == 1
    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_doc())).date_action == "written"


async def test_blank_model_date_is_skipped_not_protected(store, fake_papra):
    open_gate(store)

    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_only(document_date=""))).date_action == "skipped"
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).date_protected is False


async def test_dates_are_normalised_before_comparison(store, fake_papra):
    open_gate(store)
    fake_papra.document["documentDate"] = "2025-10-01"

    result = await deliver_metadata(
        fake_papra, store, ref_seeded(store, written_date="2025-10-01"),
        kz_only(document_date="2025-10-01"))

    assert result.date_action == "unchanged"
    assert fake_papra.patch_calls == 0


async def test_a_date_we_wrote_that_papra_stores_as_a_timestamp_is_still_ours(store,
                                                                             fake_papra):
    """Measured: `documentDate="2025-10-01"` reads back as `...T00:00:00.000Z`.

    Comparing raw strings here would call our own write a human edit and protect the
    field on every document, on every pass, forever.
    """
    open_gate(store)
    fake_papra.document["documentDate"] = "2025-10-01T00:00:00.000Z"

    result = await deliver_metadata(
        fake_papra, store, ref_seeded(store, written_date="2025-10-01"),
        kz_only(document_date="2025-10-01"))

    assert result.date_action == "unchanged"
    assert fake_papra.patch_calls == 0


async def test_a_date_the_human_changed_after_ours_is_protected(store, fake_papra):
    open_gate(store)
    fake_papra.document["documentDate"] = "2024-01-01"

    assert (await deliver_metadata(
        fake_papra, store, ref_seeded(store, written_date="2025-10-01"),
        kz_only(document_date="2025-10-01"))).date_action == "protected"
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).date_protected is True


async def test_an_unreadable_papra_date_is_protected_and_left_alone(store, fake_papra):
    """A value that is not a date is still a value: do not overwrite it, do not parse it."""
    open_gate(store)
    fake_papra.document["documentDate"] = "irgendwann im Herbst"

    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_only(document_date="2025-10-01"))).date_action == \
            "protected"
    assert fake_papra.patch_calls == 0


# --------------------------------------------------------------------------------
# Step 1g: `notes`
# --------------------------------------------------------------------------------

async def test_notes_recorded_as_truncated_value_we_actually_wrote(store, fake_papra):
    long = ("wort " * 900).strip()

    open_gate(store)
    await deliver_metadata(fake_papra, store, ref_with(),
                           kz_only(description=long))

    written = store.ledger_get(ORG, DOC_ID).written_notes
    assert written == fake_papra.last_patch["notes"]
    assert len(written) <= 2001
    assert written != long
    assert written.endswith("\u2026")


async def test_a_note_we_wrote_that_papra_still_holds_needs_no_patch(store, fake_papra):
    open_gate(store)
    fake_papra.document["notes"] = NOTE

    result = await deliver_metadata(
        fake_papra, store, ref_seeded(store, written_notes=NOTE), kz_only(description=NOTE))

    assert result.notes_action == "unchanged"
    assert fake_papra.patch_calls == 0


async def test_a_note_we_did_not_write_is_protected_on_sight(store, fake_papra):
    """Papra has no writer for `notes`, so a note we did not write is a human's."""
    open_gate(store)
    fake_papra.document["notes"] = "An die Steuermappe"

    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_only(description=NOTE))).notes_action == "protected"
    assert fake_papra.patch_calls == 0
    assert store.ledger_get(ORG, DOC_ID).notes_protected is True


async def test_an_empty_description_is_skipped(store, fake_papra):
    open_gate(store)

    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_only(description="   "))).notes_action == "skipped"
    assert fake_papra.patch_calls == 0


# --------------------------------------------------------------------------------
# Step 1h: one PATCH, metadata only, and no repair budget spent
# --------------------------------------------------------------------------------

async def test_only_the_fields_that_changed_go_in_the_patch(store, fake_papra):
    open_gate(store)
    fake_papra.document["notes"] = NOTE

    await deliver_metadata(fake_papra, store, ref_seeded(store, written_notes=NOTE),
                           kz_doc())

    assert fake_papra.patch_calls == 1
    assert set(fake_papra.last_patch) == {"name", "documentDate"}


async def test_nothing_is_sent_when_no_field_changed(store, fake_papra):
    open_gate(store)
    fake_papra.document.update({"name": "Rechnung 4711", "documentDate": "2025-10-01",
                                "notes": NOTE})
    ref = ref_seeded(store, written_name="Rechnung 4711", written_date="2025-10-01",
                     written_notes=NOTE)

    result = await deliver_metadata(fake_papra, store, ref, kz_doc(description=NOTE))

    assert (result.name_action, result.date_action, result.notes_action) == \
        ("unchanged", "unchanged", "unchanged")
    assert fake_papra.patch_calls == 0


async def test_the_body_on_the_wire_carries_papras_names_and_nothing_else(store, tmp_path):
    """X9 against the real client, not the fake: Papra's field names, nothing extra.

    The fake records Python kwargs, which is the wrong thing to assert for a claim about
    Papra's request body, so a `PapraClient` over a recording transport sends one real
    PATCH here. `content` in that body would mean this path reached for a column Task 7
    owns and gates separately.
    """
    sent = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append((request.method, json.loads(request.content)))
        return httpx.Response(200, json={"document": {"id": DOC_ID}})

    client = PapraClient("http://papra.test", "ppapi_x",
                         transport=httpx.MockTransport(handler))
    open_gate(store)
    try:
        await deliver_metadata(WirePapra(client), store, ref_with(), kz_doc())
    finally:
        await client.aclose()

    assert [method for method, _body in sent] == ["PATCH"]
    assert sent[0][1] == {"name": "Rechnung 4711", "documentDate": "2025-10-01",
                          "notes": NOTE}


async def test_a_metadata_write_spends_no_content_repair_and_no_delivery_receipt(store,
                                                                                 fake_papra):
    """`repairs(24h)` counts losses of the text column; this path never touches it.

    Charging a rename to that budget would walk documents into suppression for a fight
    that never happened, and the pass that reads the budget would stop repairing text.
    """
    open_gate(store)

    await deliver_metadata(fake_papra, store, ref_with(), kz_only(title="Rechnung 4711"))

    ref = store.ledger_get(ORG, DOC_ID)
    assert (ref.repair_count, ref.delivered_sha) == (0, None)


# --------------------------------------------------------------------------------
# Step 2's wiring: the index worker, so the flag actually does something
# --------------------------------------------------------------------------------

async def test_the_index_worker_delivers_metadata_when_the_gate_is_open(store, fake_papra,
                                                                        fake_kz):
    """A flag that gates a writer nobody calls is the `OCR_LANGUAGES` incident again.

    The index worker is the path that runs unattended, so it is the one that has to
    honour the setting; the CLI is the operator's path, not the pipeline's.
    """
    open_gate(store)
    store.set_setting("legacy_workers", "false")
    job = make_job(store=store)
    fake_kz.document = kz_doc(description="Notiz")

    outcome = await indexing.run_index_job(fake_kz, store, job, papra=fake_papra,
                                           poll_s=0, wait_timeout_s=5)

    assert outcome.metadata.name_action == "written"
    assert fake_papra.last_patch["name"] == "Rechnung 4711"


async def test_the_index_worker_leaves_metadata_alone_when_the_gate_is_shut(store,
                                                                           fake_papra,
                                                                           fake_kz):
    """One flag, one set of columns: content delivery does not imply rename delivery."""
    store.set_setting("legacy_workers", "false")
    store.set_setting("index_content_delivery", "true")
    job = make_job(store=store)
    fake_kz.document = kz_doc()

    outcome = await indexing.run_index_job(fake_kz, store, job, papra=fake_papra,
                                           poll_s=0, wait_timeout_s=5)

    assert outcome.delivered is True
    assert outcome.metadata is None
    assert all(set(body) == {"content"} for _o, _d, body in fake_papra.patched), \
        fake_papra.patched
    assert store.ledger_get(ORG, DOC_ID).written_name is None


# --------------------------------------------------------------------------------
# X1: the flag is honoured, seeded honestly, and visible in `check`
# --------------------------------------------------------------------------------

def test_the_settings_seed_refuses_to_claim_a_metadata_writer_this_build_lacks(tmp_path,
                                                                              monkeypatch,
                                                                              caplog):
    """`METADATA_DELIVERY_IMPLEMENTED` is read as an attribute OF the module.

    Bound by value at import, the documented way to exercise a build without the path
    would stop working, and the seed would say `true` for a build that renames nothing —
    which is exactly the inert-flag shape this task exists to remove.
    """
    from papra_suite.config import load_config
    from papra_suite.pipeline import _sync_settings_from_config

    monkeypatch.setattr(indexing, "METADATA_DELIVERY_IMPLEMENTED", False)
    caplog.set_level(logging.WARNING, logger="papra_suite.pipeline")
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path),
                          "PAPRA_INDEX_METADATA_DELIVERY": "true"})
    fresh = Store(tmp_path / "seed.db")
    fresh.migrate()
    try:
        _sync_settings_from_config(fresh, config)
        assert fresh.settings_flag("index_metadata_delivery") is False
    finally:
        fresh.close()
    assert "no metadata delivery path" in caplog.text


def test_check_reports_the_metadata_writer_next_to_the_content_writer(monkeypatch, tmp_path,
                                                                     capsys):
    from papra_suite.__main__ import main

    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 0
    out = capsys.readouterr().out
    assert "metadata writer: suite title worker (legacy)" in out
    assert "content writer:" in out            # its neighbour, on the line above or below

    monkeypatch.setenv("PAPRA_INDEX_METADATA_DELIVERY", "1")
    assert main() == 0
    # Legacy worker still on, so this is the state the handover passes through. Reported, not
    # refused — `metadata_writer_state` carries the argument for why.
    assert "metadata writer: two writers (title queue draining)" in capsys.readouterr().out

    monkeypatch.setenv("PAPRA_LEGACY_WORKERS", "0")
    assert main() == 0
    assert "metadata writer: index bridge" in capsys.readouterr().out

    monkeypatch.setenv("PAPRA_INDEX_METADATA_DELIVERY", "0")
    assert main() == 0
    assert "metadata writer: none (handover window)" in capsys.readouterr().out


def test_check_names_both_metadata_writers_while_the_title_queue_drains(monkeypatch,
                                                                        tmp_path, capsys):
    """Delivery on while the legacy title worker still runs is a transient, not a bug.

    It cannot be refused the way two content writers are: `legacy_workers` has to stay on
    while the queues drain, and the flag that drains them is the one being set. The second
    writer stops being fed — `on_job_done` refuses the ocr-done-to-title transition while
    metadata is ours, for every intake path — and `run_title_job` refuses the rows already in
    the queue instead of writing a `name` the ledger would read as a human edit. "Stops being
    fed" is not "empty", though, so the state is legible rather than refused, and
    `metadata_writer_state` carries the whole argument.
    """
    from papra_suite.__main__ import main

    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PAPRA_INDEX_METADATA_DELIVERY", "1")
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 0
    out = capsys.readouterr().out
    assert "metadata writer: two writers" in out
    assert "title queue" in out


# --------------------------------------------------------------------------------
# Step 4: the title chain stops being fed
# --------------------------------------------------------------------------------

def _queued_kinds(store, document_id: str) -> list[str]:
    return sorted(job.kind for job in store.list_jobs() if job.document_id == document_id)


def test_metadata_delivery_stops_the_title_chain_without_starving_the_text_writer(store):
    """Two flags, two columns: taking the title must not take Papra's text away.

    The obvious wiring — "metadata is ours, so stop feeding the legacy OCR queue, since the
    title chain hangs off it" — is wrong in a way `check` cannot see: with content delivery off
    and `legacy_workers` on, that `ocr` row is the only thing that will ever write this
    document's `content`, while `check` goes on crediting the suite's OCR worker for the
    column. So the switch sits one step later, at `on_job_done`, where stopping the transition
    stops a writer of `name` and nothing else.

    The pair of assertions is what makes that visible: the `ocr` row is still enqueued, and no
    `title` row appears when that OCR job finishes.
    """
    from papra_suite.pipeline import enqueue_index_job, on_job_done

    open_gate(store)
    store.set_setting("index_content_delivery", "false")
    enqueue_index_job(store, papra_org_id=ORG, document_id="doc-new", file_hash=HASH,
                      name="new.pdf", mime="application/pdf", ingested_via="sweep")
    assert _queued_kinds(store, "doc-new") == ["index", "ocr"]

    ocr_job = next(job for job in store.list_jobs() if job.kind == "ocr")
    on_job_done(store, ocr_job, ocr_result="Rechnung ueber 1.234,56 Euro")

    assert _queued_kinds(store, "doc-new") == ["index", "ocr"]      # no title job


def test_the_title_chain_is_still_fed_while_metadata_is_off(store):
    """The default configuration, unchanged: OCR done means a title job, which writes the name.

    The pair matters. A state string that changes without changing an enqueue is the
    inert-flag complaint all over again, and this is the half that shows the flag moves
    something on the path that runs unattended.
    """
    from papra_suite.pipeline import enqueue_index_job, on_job_done

    enqueue_index_job(store, papra_org_id=ORG, document_id="doc-new", file_hash=HASH,
                      name="new.pdf", mime="application/pdf", ingested_via="sweep")
    ocr_job = next(job for job in store.list_jobs() if job.kind == "ocr")
    on_job_done(store, ocr_job, ocr_result="Rechnung ueber 1.234,56 Euro")

    assert _queued_kinds(store, "doc-new") == ["index", "ocr", "title"]


class _StopWorkerLoop(BaseException):
    """Stop seam for `worker_loop`. BaseException because the loop swallows Exception."""


_TITLE_CONTENT = "Rechnung Nr. 42 – aktuell archivierter Inhalt"


def _queued_title_job(store, papra, *, document_id: str = "doc_1"):
    """Queue a title job whose `content_hash` really matches what Papra holds.

    The hash has to match, or the job is skipped for staleness and the test proves nothing
    about the metadata gate: `run_title_job`'s first guard already returns quietly.
    """
    papra.documents[ORG][document_id] = {"id": document_id, "name": "rechnung.pdf",
                                         "mimeType": "application/pdf",
                                         "content": _TITLE_CONTENT}
    job_id = store.enqueue_job(kind="title", papra_org_id=ORG, document_id=document_id,
                               content_hash=hashlib.sha256(
                                   _TITLE_CONTENT.encode()).hexdigest(),
                               payload={"name": "rechnung.pdf",
                                        "excerpt": "Rechnung ueber 1.234,56 Euro"})
    return store.get_job(job_id)


async def _run_title_worker_once(store, papra, llm) -> None:
    """Let the legacy title worker claim and finish one job, then stop it.

    Driven through `worker_loop` rather than by calling the handler, because half of what
    the skip has to achieve is that the row ends `done`: a skip that leaves the job
    `pending` never lets the handover's drain proof reach zero.
    """
    from papra_suite.pipeline import run_title_job, worker_loop

    original_claim = store.claim_next_job
    claims = {"n": 0}

    def claim(kind):
        claims["n"] += 1
        if claims["n"] > 1:
            raise _StopWorkerLoop()
        return original_claim(kind)

    store.claim_next_job = claim
    try:
        with pytest.raises(_StopWorkerLoop):
            await worker_loop(store, kind="title",
                              handler=lambda job: run_title_job(papra, llm, job, store=store))
    finally:
        # Restore, or a second call re-wraps this wrapper and stops on its very first claim.
        store.claim_next_job = original_claim


async def test_a_queued_title_job_stops_writing_names_once_the_gate_is_open(store, caplog):
    """Critical #1's consumer half: stopping the producer leaves the queue's leftovers live.

    `on_job_done` refuses to *create* a title job while metadata delivery is ours, but the
    rows already `pending` when an operator flips `PAPRA_INDEX_METADATA_DELIVERY` are still
    claimed by the legacy title worker, and every one of them PATCHes Papra's `name` while
    recording nothing in the ledger. The next metadata pass reads back a title it never
    wrote, answers *not ours*, and sets `name_protected = 1` — permanently, and
    byte-identical to a genuine human rename because protection is one bit with no
    provenance. Bulk recovery is then `metadata --force --yes-i-mean-it`, which overwrites
    every real rename in scope, and `--restore-auto` is a no-op for exactly these rows.

    So the worker skips the job the way it already skips a stale one: nothing asked of the
    LLM, nothing sent to Papra, and the row marked done so the drain proof can reach zero.
    The second half flips the gate shut and shows the same worker still writing, which is
    what kills a mutant that skips unconditionally.
    """
    from tests.test_workers import StubPapra, make_llm

    open_gate(store)
    papra = StubPapra()
    llm, calls = make_llm(["Rechnung 42 – Möbel"])
    job = _queued_title_job(store, papra)
    caplog.set_level(logging.INFO, logger="papra_suite.pipeline")

    await _run_title_worker_once(store, papra, llm)

    assert papra.content_updates == [], "the legacy writer renamed a document we own"
    assert papra.documents[ORG]["doc_1"]["name"] == "rechnung.pdf"
    assert calls["n"] == 0, "a skipped title job still asked the model for a title"
    assert store.get_job(job.id).status == "done"
    assert sum("title job skipped" in r.getMessage() for r in caplog.records) == 1

    # The pair: with the gate shut the same worker still titles documents, unchanged.
    store.set_setting("index_metadata_delivery", "false")
    still_ours = _queued_title_job(store, papra, document_id="doc_2")

    await _run_title_worker_once(store, papra, llm)

    assert papra.content_updates == [("doc_2", "Rechnung 42 – Möbel", None)]
    assert store.get_job(still_ours.id).status == "done"


def cli_args(**over):
    base = dict(command="metadata", restore_auto=False, force=False, org_id=None,
                document_id=None, field=None, dry_run=False, yes_i_mean_it=False)
    return SimpleNamespace(**{**base, **over})


def parse_cli(argv):
    """Parse through the real parser, so the flag surface itself is under test."""
    import argparse

    from papra_suite.metadata_cli import add_subparser

    parser = argparse.ArgumentParser(prog="papra-suite")
    sub = parser.add_subparsers(dest="command", required=True)
    add_subparser(sub)
    return sub.choices["metadata"].parse_args(argv)


def configuration(tmp_path, **env):
    from papra_suite.config import load_config

    return load_config({**BASE_ENV, "DATA_DIR": str(tmp_path), **env})


def test_metadata_cli_parses_the_documented_surface(tmp_path):
    args = parse_cli(["--restore-auto", "--org-id", "org_a", "--document-id", "doc_1",
                      "--field", "notes", "--dry-run"])
    assert (args.restore_auto, args.org_id, args.document_id, args.field,
            args.dry_run) == (True, "org_a", "doc_1", "notes", True)

    assert parse_cli(["--force", "--yes-i-mean-it"]).force is True
    with pytest.raises(SystemExit):
        parse_cli(["--restore-auto", "--force"])
    with pytest.raises(SystemExit):
        parse_cli(["--field", "content"])


def test_restore_auto_clears_protection_in_scope_and_prints_the_count(store, capsys,
                                                                     tmp_path):
    from papra_suite.metadata_cli import run_metadata

    store.ledger_protect(papra_org_id=ORG, papra_document_id=DOC_ID, field="name")
    other = "doc-other"
    seed_reference(store, document_id=other)
    store.ledger_protect(papra_org_id=ORG, papra_document_id=other, field="notes")

    code = run_metadata(configuration(tmp_path), cli_args(restore_auto=True, field="name"),
                        store=store)

    assert code == 0
    out = capsys.readouterr().out
    assert "1" in out and "name" in out
    assert store.ledger_get(ORG, DOC_ID).name_protected is False
    assert store.ledger_get(ORG, other).notes_protected is True     # a different field


def test_restore_auto_scoped_to_one_document_leaves_the_others_alone(store, tmp_path):
    from papra_suite.metadata_cli import run_metadata

    other = "doc-other"
    seed_reference(store, document_id=other)
    for document_id in (DOC_ID, other):
        store.ledger_protect(papra_org_id=ORG, papra_document_id=document_id, field="name")

    assert run_metadata(configuration(tmp_path),
                        cli_args(restore_auto=True, document_id=DOC_ID), store=store) == 0
    assert store.ledger_get(ORG, DOC_ID).name_protected is False
    assert store.ledger_get(ORG, other).name_protected is True


def test_restore_auto_dry_run_changes_nothing(store, capsys, tmp_path):
    from papra_suite.metadata_cli import run_metadata

    store.ledger_protect(papra_org_id=ORG, papra_document_id=DOC_ID, field="name")

    assert run_metadata(configuration(tmp_path),
                        cli_args(restore_auto=True, dry_run=True), store=store) == 0
    assert "dry run" in capsys.readouterr().out
    assert store.ledger_get(ORG, DOC_ID).name_protected is True


def test_force_without_confirmation_prints_the_plan_and_exits_one(store, fake_papra,
                                                                 tmp_path, capsys):
    from papra_suite.metadata_cli import run_metadata

    store.ledger_protect(papra_org_id=ORG, papra_document_id=DOC_ID, field="name")
    open_gate(store)

    code = run_metadata(configuration(tmp_path), cli_args(force=True), store=store,
                        papra=fake_papra, kreuzakt=ScriptedIndex())

    assert code == 1
    assert "not run" in capsys.readouterr().out
    assert fake_papra.patch_calls == 0


def test_force_overrides_protection_once_and_still_records_what_it_wrote(store, fake_papra,
                                                                        tmp_path, capsys):
    from papra_suite.metadata_cli import run_metadata

    store.ledger_protect(papra_org_id=ORG, papra_document_id=DOC_ID, field="name")
    open_gate(store)

    code = run_metadata(configuration(tmp_path),
                        cli_args(force=True, yes_i_mean_it=True), store=store,
                        papra=fake_papra, kreuzakt=ScriptedIndex())

    assert code == 0
    out = capsys.readouterr().out
    assert "overwrote" in out and "1" in out
    assert fake_papra.document["name"] == "Rechnung 4711"
    assert store.ledger_get(ORG, DOC_ID).written_name == "Rechnung 4711"
    # once, not forever: `--force` overrides this run, `--restore-auto` is the durable one
    assert store.ledger_get(ORG, DOC_ID).name_protected is True


def test_dry_run_prints_per_field_verdicts_and_patches_nothing(store, fake_papra, tmp_path,
                                                              capsys):
    """All four verdicts, one line each per reference, and `patch_calls` empty.

    The quiet half of the CLI's contract: `--dry-run` is what an operator reads after the
    gate is open and before letting the pass touch anything, so a dry run that writes is
    worse than a dry run that lies — and the write that matters here is not the PATCH.
    A `protected` verdict sets a durable flag, permanently, and protection is the one thing
    in this feature no later pass can undo. So the ledger row is asserted too: a rehearsal
    that quietly protected the field it was rehearsing would be the worst mutation this path
    can carry, and `patch_calls == 0` alone does not see it.

    `date` cannot be covered by this fixture and does not need to be: the scripted index
    carries no date, so its verdict is `skipped` and the protection branch is never reached.
    Reaching a second `protected` verdict here would cost the `skipped` half of the
    four-verdict assertion below.
    """
    from papra_suite.metadata_cli import run_metadata

    open_gate(store)
    quiet = "doc-quiet"
    seed_reference(store, document_id=quiet, kreuzakt_id=8)
    store.ledger_record_metadata(papra_org_id=ORG, papra_document_id=quiet,
                                 name="Rechnung 4711", date="2025-10-01", notes=NOTE)
    seed_papra_document(fake_papra, quiet, name="Rechnung 4711",
                        document_date="2025-10-01", notes=NOTE)
    fake_papra.document["notes"] = "Von Hand notiert"      # doc-1: a human's note
    index = ScriptedIndex(kz_doc(document_date=None))      # and no date either side

    code = run_metadata(configuration(tmp_path), cli_args(dry_run=True), store=store,
                        papra=fake_papra, kreuzakt=index)

    assert code == 0
    out = capsys.readouterr().out
    assert fake_papra.patch_calls == 0
    row = store.ledger_get(ORG, DOC_ID)
    assert row.written_name is None
    flags = (row.name_protected, row.date_protected, row.notes_protected)
    assert flags == (False, False, False), f"a dry run wrote a protection flag: {flags}"
    assert DOC_ID in out and quiet in out
    for verdict in ("written", "protected", "skipped", "unchanged"):
        assert verdict in out, f"{verdict} missing from:\n{out}"


def test_a_real_cli_run_refuses_while_the_flag_is_off(store, fake_papra, tmp_path, capsys):
    """X1: the gate is the boundary, and `--force` overrides protection, not the flag.

    The message names the variable, because the operator about to type this command is
    the same one who just set it.
    """
    from papra_suite.metadata_cli import run_metadata

    store.set_setting("index_metadata_delivery", "false")

    code = run_metadata(configuration(tmp_path), cli_args(yes_i_mean_it=True), store=store,
                        papra=fake_papra, kreuzakt=ScriptedIndex())

    assert code == 2
    assert "PAPRA_INDEX_METADATA_DELIVERY" in capsys.readouterr().err
    assert fake_papra.patch_calls == 0


def test_field_scope_writes_only_that_field(store, fake_papra, tmp_path):
    from papra_suite.metadata_cli import run_metadata

    open_gate(store)

    code = run_metadata(configuration(tmp_path), cli_args(field="notes"), store=store,
                        papra=fake_papra, kreuzakt=ScriptedIndex())

    assert code == 0
    assert set(fake_papra.last_patch) == {"notes"}
    assert store.ledger_get(ORG, DOC_ID).written_notes == NOTE
    assert store.ledger_get(ORG, DOC_ID).written_name is None


def test_the_cli_walks_the_scope_and_reports_per_reference_lines(store, fake_papra, tmp_path,
                                                                capsys):
    from papra_suite.metadata_cli import run_metadata

    open_gate(store)
    second = "doc-second"
    seed_reference(store, document_id=second, kreuzakt_id=8)
    seed_papra_document(fake_papra, second, name="zweit.pdf")

    assert run_metadata(configuration(tmp_path), cli_args(), store=store,
                        papra=fake_papra, kreuzakt=ScriptedIndex()) == 0
    out = capsys.readouterr().out
    assert DOC_ID in out and second in out
    assert "2 references" in out


def test_an_unreachable_index_row_is_counted_and_does_not_end_the_run(store, fake_papra,
                                                                    tmp_path, capsys):
    """One document whose index row will not answer is a count, not the end of the walk.

    The same containment the convergence pass learned the hard way: a reference that
    cannot be read must not cost the operator the rest of the corpus in one command.
    """
    from papra_suite.metadata_cli import run_metadata

    open_gate(store)
    second = "doc-second"
    seed_reference(store, document_id=second, kreuzakt_id=8, name=NAME)
    seed_papra_document(fake_papra, second, name=NAME)

    code = run_metadata(configuration(tmp_path), cli_args(), store=store,
                        papra=fake_papra, kreuzakt=ScriptedIndex(missing={7}))

    assert code == 1
    out = capsys.readouterr().out
    assert "unreadable" in out
    # the walk went on: the second reference really was renamed
    assert fake_papra.documents[(ORG, second)]["name"] == "Rechnung 4711"


def test_the_gate_closing_mid_walk_stops_the_run_and_keeps_its_report(store, fake_papra,
                                                                     tmp_path, capsys):
    """The flag can close under a run that already started, and that is not a crash.

    `deliver_metadata` re-reads the gate for every reference — that is the point of it — so a
    flip in another process raises `DeliveryDisabled` from the middle of the walk. It was not
    in the tuple that catches Papra's refusals, so it escaped `_walk`: the operator lost the
    summary of what had already been written, which after a partial `--force` is the one thing
    they need. Counting it as one more refusal per reference would be no better: every
    remaining document answers identically, so the run grinds out a line each and stops
    nowhere. It stops here, and says where.
    """
    from papra_suite.metadata_cli import run_metadata

    open_gate(store)
    for document_id, kreuzakt_id in (("doc-2", 8), ("doc-3", 9)):
        seed_reference(store, document_id=document_id, kreuzakt_id=kreuzakt_id)
        seed_papra_document(fake_papra, document_id, name=NAME)

    index = GateClosingIndex(store)
    code = run_metadata(configuration(tmp_path), cli_args(), store=store, papra=fake_papra,
                        kreuzakt=index)

    assert code == 1
    out = capsys.readouterr().out
    assert "gate closed" in out, out
    assert "1 of 3" in out, out
    assert index.reads == [7, 8], "the walk kept going after the gate shut"
    assert fake_papra.documents[(ORG, DOC_ID)]["name"] == "Rechnung 4711"   # already done
    assert fake_papra.documents[(ORG, "doc-3")]["name"] == NAME             # never touched


def test_the_cli_is_wired_into_the_entry_point(tmp_path, monkeypatch):
    import papra_suite.metadata_cli as metadata_cli
    from papra_suite.__main__ import main

    seen = []
    monkeypatch.setattr(metadata_cli, "run_metadata",
                        lambda config, args, **kw: (seen.append(args), 0)[1])
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "metadata", "--dry-run"])

    assert main() == 0
    assert seen and seen[0].dry_run is True


def test_restore_auto_needs_no_papra_and_no_index(tmp_path, capsys):
    """`--restore-auto` is a ledger command: it runs while both services are down.

    It is the command an operator reaches for during the incident, when the last thing
    they need is a network dependency in the escape hatch.
    """
    from papra_suite.metadata_cli import run_metadata

    fresh = Store(tmp_path / "state.db")
    fresh.migrate()
    try:
        assert run_metadata(configuration(tmp_path), cli_args(restore_auto=True),
                            store=fresh) == 0
    finally:
        fresh.close()
    assert "0" in capsys.readouterr().out
