"""Task 8's metadata delivery against the live services, asserted structurally.

Two things can only be learned here, and both are the reason this file exists.

**The synthetic-title guard is reachable in the real deployment, and the pinned Kreuzakt is
the deployment that makes it reachable.** A Kreuzakt with no vision model configured derives
a document's title from its filename, and every filename it is ever given here is ours:
`papra-<documentId>-<hash8>.pdf`. So the index's title for such a server is our own upload
name, and a pass that "improved" Papra's `name` with it would rename the corpus to garbage
and record the garbage as authoritative. That is not a double's invention — it is what this
container answers, so the guard is measured against it rather than only simulated.

**Papra's field names and limits are the ones the client has to satisfy.** Measured against
this image and encoded below rather than in a comment: the date column is `documentDate`, a
bare ISO date is accepted and read back as `...T00:00:00.000Z`, `notes` accepts 2048
characters and rejects 2049, and an empty `name` is a 400. What a delivery puts on the wire
is asserted as the body, because the body is what Papra acts on.

As in `test_index_pipeline.py`, extraction *wording* is never asserted: this container is
keyless, so its "title" is a filename's shape, and a test that pinned a model's phrasing
would blame our code for a prompt change. When `PAPRA_KREUZAKT_URL` is absent the tests skip,
unless `PAPRA_REQUIRE_STACK=1` says the stack is meant to be up — then a missing index is a
failure rather than a green run that tested nothing.
"""

import os
from dataclasses import dataclass, replace

import fitz
import httpx
import pytest

from papra_suite.indexing import (
    deliver_metadata,
    is_synthetic_upload_name,
    looks_like_synthetic_upload_name,
    normalize_date,
    run_index_job,
    synthetic_upload_name,
)
from papra_suite.kreuzakt_client import KreuzaktClient
from papra_suite.papra_client import PapraClient
from papra_suite.pipeline import sweep_once
from papra_suite.store import Store
from tests.integration.conftest import _skip_or_fail

pytestmark = pytest.mark.integration

KREUZAKT_URL = os.environ.get("PAPRA_KREUZAKT_URL", "http://127.0.0.1:12400")
TEXT_LAYER_MARKER = "papra-metadata-marker-3d7b19"


def _kreuzakt_is_up() -> bool:
    try:
        return httpx.get(f"{KREUZAKT_URL}/api/health", timeout=2.0).status_code == 200
    except httpx.TransportError:
        return False


class MetadataPapra(PapraClient):
    """Papra, with the body of every PATCH recorded as it was sent.

    The kwargs rather than the wire dict, mapped through the client's own table: the claim
    this supports is "a metadata delivery never asks for `content`", and the only way to
    check that without re-implementing the client is to record what the client was handed and
    let the client itself be the thing that builds the body.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.patches: list[dict] = []

    async def update_document(self, org_id, document_id, *, name=None, content=None,
                              document_date=None, notes=None):
        given = {"name": name, "content": content, "document_date": document_date,
                 "notes": notes}
        self.patches.append({key: value for key, value in given.items()
                             if value is not None})
        return await super().update_document(org_id, document_id, name=name, content=content,
                                             document_date=document_date, notes=notes)


@dataclass
class Stack:
    store: Store
    papra: MetadataPapra
    kreuzakt: KreuzaktClient
    org_id: str


def make_pdf(marker: str = TEXT_LAYER_MARKER) -> bytes:
    """A one-page PDF with a real text layer, so the index has something to work with."""
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((72, 72), "Rechnung Nr. 4711")
    page.insert_text((72, 110), f"Referenz: {marker}")
    data = pdf.tobytes()
    pdf.close()
    return data


@pytest.fixture
async def stack(tmp_path, papra_stack):
    base_url, org_id, api_key = papra_stack
    if not _kreuzakt_is_up():
        _skip_or_fail(f"Kreuzakt not reachable at {KREUZAKT_URL}; bring the bridge stack up")
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=org_id, name="Metadata Org")
    papra = MetadataPapra(base_url, api_key)
    kreuzakt = KreuzaktClient(KREUZAKT_URL)
    yield Stack(store=store, papra=papra, kreuzakt=kreuzakt, org_id=org_id)
    await papra.aclose()
    await kreuzakt.aclose()
    store.close()


async def _index_one_document(stack: Stack, filename: str = "rechnung-4711.pdf"):
    """Upload to Papra, discover by sweep, index, and hand back the bound reference.

    The real sequence rather than a hand-made ledger row, because the guard being tested has
    to be shown firing on a document whose Papra name, Kreuzakt filename and ledger baseline
    all came from the services instead of from a fixture.
    """
    document = await stack.papra.create_document(stack.org_id, filename, make_pdf())
    document_id = document["id"]
    await sweep_once(stack.store, stack.papra)
    job = stack.store.claim_next_job("index")
    assert job is not None and job.document_id == document_id
    outcome = await run_index_job(stack.kreuzakt, stack.store, job, papra=stack.papra,
                                  poll_s=0.5, wait_timeout_s=90)
    stack.store.mark_job_done(job.id)
    ref = stack.store.ledger_get(stack.org_id, document_id)
    assert ref is not None and ref.kreuzakt_id == outcome.kreuzakt_id
    return ref


async def test_the_keyless_index_title_is_our_upload_name_and_is_never_delivered(stack):
    """X5 on live services: the guard fires on what the real index really answers.

    Three claims, all of them about the container rather than about a double:

    1. Kreuzakt's `original_filename` for our upload is exactly
       `papra-<documentId>-<hash8>.pdf` — the name we chose, not one the user typed;
    2. on this keyless server the derived `title` is that same name (prettified or not, both
       forms are the guard's business), so the "improvement" this pass could make to Papra's
       `name` is a rename to our own upload garbage;
    3. delivering anyway leaves Papra's `name` exactly as it was, records nothing, and — the
       part a `protected` verdict would get wrong — protects nothing, because our own garbage
       is not evidence that a human touched the field.
    """
    ref = await _index_one_document(stack)
    index_document = await stack.kreuzakt.get_document(ref.kreuzakt_id)
    upload_name = synthetic_upload_name(ref.papra_document_id, ref.file_hash)

    assert index_document.original_filename == upload_name
    assert is_synthetic_upload_name(index_document.title,
                                    document_id=ref.papra_document_id,
                                    file_hash=ref.file_hash), (
        f"this container answered {index_document.title!r} for an upload named "
        f"{upload_name!r}; if it has started inventing titles, the guard below is no longer "
        "the live risk this test claims — so the test has to change, not pass quietly")
    assert looks_like_synthetic_upload_name(index_document.title)

    stack.store.set_setting("index_metadata_delivery", "true")
    name_before = (await stack.papra.get_document(stack.org_id,
                                                  ref.papra_document_id))["name"]

    result = await deliver_metadata(stack.papra, stack.store, ref, index_document)

    assert result.name_action == "skipped"
    assert all("name" not in body for body in stack.papra.patches), stack.papra.patches
    after = await stack.papra.get_document(stack.org_id, ref.papra_document_id)
    # The name was OURS to change - Papra still holds the value the ledger took as its
    # baseline - and it was still left exactly where it was. Refusing a field that was not
    # ours would be a weaker claim than the one this test is making.
    assert after["name"] == name_before == ref.papra_original_name
    fresh = stack.store.ledger_get(stack.org_id, ref.papra_document_id)
    assert (fresh.written_name, fresh.name_protected) == (None, False)


async def test_the_collision_form_of_our_name_is_refused_too(stack):
    """`papra-doc-1-a1b2c3d4 (1)` is the same name, and the guard treats it as such.

    The suffix is constructed rather than coaxed out of the services, and said out loud
    because it is the unusual part of this file: Kreuzakt dedups identical bytes, so our own
    naming never produces a colliding filename — a ` (N)` appears only when something else
    shares the name, which is exactly how a deployment would meet it. What is live here is
    everything the guard decides *against*: the real Papra record, the real ledger row, and
    the real PATCH that does not happen.
    """
    ref = await _index_one_document(stack)
    index_document = await stack.kreuzakt.get_document(ref.kreuzakt_id)
    upload_name = synthetic_upload_name(ref.papra_document_id, ref.file_hash)
    colliding = replace(index_document, title=f"{upload_name[:-4]} (1)")
    assert is_synthetic_upload_name(colliding.title, document_id=ref.papra_document_id,
                                    file_hash=ref.file_hash)

    stack.store.set_setting("index_metadata_delivery", "true")
    result = await deliver_metadata(stack.papra, stack.store, ref, colliding)

    assert result.name_action == "skipped"
    assert all("name" not in body for body in stack.papra.patches), stack.papra.patches
    fresh = stack.store.ledger_get(stack.org_id, ref.papra_document_id)
    assert (fresh.written_name, fresh.name_protected) == (None, False)


async def test_a_real_title_date_and_note_reach_papra_in_one_metadata_only_patch(stack):
    """The other half of the wire: what a delivery that SHOULD write actually sends.

    A title that is a title rather than a filename, so the pass has something to deliver; the
    date as a bare ISO day, because that is the form this Papra accepts and the ledger keeps;
    and a description longer than the suite's own note limit, because the field's real bound
    is Papra's 2048 and truncation is the only reason this does not 400.

    Asserted on the live record afterwards, not only on the request: `documentDate` must read
    back as the day we sent in whatever form Papra stores it, `notes` must be exactly the
    string the ledger recorded, and no PATCH may carry `content` — that column belongs to
    Task 7's flag, and a metadata write that touched it would rewrite a document's text on
    the strength of a title change.
    """
    ref = await _index_one_document(stack)
    index_document = await stack.kreuzakt.get_document(ref.kreuzakt_id)
    long_note = ("rechnung ueber 1234,56 euro " * 200).strip()
    human = replace(index_document, title="Rechnung 4711 Steuerbuero",
                    document_date="2025-10-01", description=long_note)
    assert not is_synthetic_upload_name(human.title, document_id=ref.papra_document_id,
                                       file_hash=ref.file_hash)

    stack.store.set_setting("index_metadata_delivery", "true")
    result = await deliver_metadata(stack.papra, stack.store, ref, human)

    assert (result.name_action, result.date_action, result.notes_action) == \
        ("written", "written", "written")
    assert all("content" not in body for body in stack.papra.patches), stack.papra.patches
    body = stack.papra.patches[-1]
    assert set(body) == {"name", "document_date", "notes"}

    document = await stack.papra.get_document(stack.org_id, ref.papra_document_id)
    assert document["name"] == "Rechnung 4711 Steuerbuero"
    assert normalize_date(str(document["documentDate"])[:10]) == "2025-10-01", document
    fresh = stack.store.ledger_get(stack.org_id, ref.papra_document_id)
    assert document["notes"] == fresh.written_notes        # X7: what was sent, not the model's
    assert fresh.written_notes != long_note
    assert len(document["notes"]) <= 2048

    # Second pass: Papra holds exactly what we sent, so nothing is written again. Without
    # this the pass would PATCH every document on every run and `written_*` would be the only
    # thing keeping a human's edit safe.
    before = list(stack.papra.patches)
    again = await deliver_metadata(stack.papra, stack.store, fresh, human)
    assert (again.name_action, again.date_action, again.notes_action) == \
        ("unchanged", "unchanged", "unchanged")
    assert stack.papra.patches == before
