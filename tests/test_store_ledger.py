"""The `index_ledger` API.

The ledger is the authorization boundary: Kreuzakt has no tenancy, so every MCP
handler resolves a Kreuzakt id through these rows and returns Papra identities
only for what comes back. That makes two properties load-bearing rather than
cosmetic, and the tests below are organised around them:

* **Resolution fails closed.** A row whose `delivered_generation` is not the
  current one, or whose `deleted_at` is set, must not resolve — a Kreuzakt id is
  only meaningful inside one index generation, and after a rebuild the same
  integer names a different document.
* **Deletes are reference-counted.** Kreuzakt dedupes by file hash, so identical
  bytes in two Papra buckets produce one Kreuzakt document and two ledger rows.
  Deleting one reference must never be enough to destroy the shared document.
"""

import dataclasses
import sqlite3

import pytest

import papra_suite.store as store_module
from papra_suite.store import EMPTY_EXTRACTION_SHA, LedgerRef, Store

H = "ab" * 32


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


def two_refs(store):
    """Two buckets, same bytes, therefore one Kreuzakt document (id 7)."""
    for org, doc in (("org-a", "doc-1"), ("org-b", "doc-2")):
        store.ledger_upsert_reference(papra_org_id=org, papra_document_id=doc,
                                      file_hash=H, papra_original_name=f"{doc}.pdf")
        store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=doc,
                                     kreuzakt_id=7, generation=store.get_generation())
    return store


# -- resolution: the security property -----------------------------------------

def test_two_references_share_one_kreuzakt_id(store):        # no unique index
    assert len(two_refs(store).ledger_resolve([7], generation=1)) == 2


def test_resolve_returns_each_reference_own_bucket(store):
    """Resolution returns *references*, not Kreuzakt ids.

    The MCP layer filters these rows on the token's buckets, so a row must carry
    the org it belongs to after a round-trip through a shared kreuzakt_id — if
    resolution collapsed the two rows into one "document 7" answer, a one-bucket
    token would see the other bucket's document.
    """
    refs = two_refs(store).ledger_resolve([7], generation=1)
    assert sorted((r.papra_org_id, r.papra_document_id) for r in refs) == [
        ("org-a", "doc-1"), ("org-b", "doc-2")]
    assert all(r.file_hash == H and r.kreuzakt_id == 7 for r in refs)


def test_stale_generation_resolves_to_nothing(store):        # the rebuild rule
    store = two_refs(store)
    store.bump_generation()
    assert store.ledger_resolve([7], generation=store.get_generation()) == []
    assert [r.papra_document_id for r in store.ledger_needs_index(generation=2)] == [
        "doc-1", "doc-2"]


def test_resolve_excludes_deleted_and_restore_brings_the_row_back(store):
    """Soft delete is the fail-closed state; clearing it is the whole restore.

    Three states, all asserted on the same kreuzakt_id so the only variable is
    `deleted_at`: active resolves, soft-deleted does not, restored does.
    """
    store = two_refs(store)
    generation = store.get_generation()
    store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="doc-1")
    assert [r.papra_document_id for r in
            store.ledger_resolve([7], generation=generation)] == ["doc-2"]
    assert store.ledger_get("org-a", "doc-1").deleted_at is not None
    # the untouched reference is still fully usable
    other = store.ledger_get("org-b", "doc-2")
    assert other.deleted_at is None and other.kreuzakt_id == 7
    store.ledger_mark_restored(papra_org_id="org-a", papra_document_id="doc-1")
    restored = store.ledger_get("org-a", "doc-1")
    assert restored.deleted_at is None
    assert [r.papra_document_id for r in
            store.ledger_resolve([7], generation=generation)] == ["doc-1", "doc-2"]


def test_resolve_of_unknown_ids_is_empty(store):
    two_refs(store)
    assert store.ledger_resolve([999], generation=1) == []
    assert store.ledger_resolve([], generation=1) == []       # never "no filter"


def test_never_resolves_across_generations_even_when_ids_collide(store):
    """After a rebuild, id 7 means a different document — the old row must stay dark."""
    store = two_refs(store)
    store.bump_generation()
    store.ledger_upsert_reference(papra_org_id="org-c", papra_document_id="doc-3",
                                  file_hash=H, papra_original_name="doc-3.pdf")
    store.ledger_set_kreuzakt_id(papra_org_id="org-c", papra_document_id="doc-3",
                                 kreuzakt_id=7, generation=store.get_generation())
    refs = store.ledger_resolve([7], generation=store.get_generation())
    assert [r.papra_document_id for r in refs] == ["doc-3"]


# -- binding a kreuzakt id, with the fence -------------------------------------

def test_set_kreuzakt_id_without_fence_writes_and_returns_true(store):
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="doc-1.pdf")
    generation = store.get_generation()
    assert store.ledger_set_kreuzakt_id(papra_org_id="org-a", papra_document_id="doc-1",
                                        kreuzakt_id=7, generation=generation) is True
    ref = store.ledger_get("org-a", "doc-1")
    assert (ref.kreuzakt_id, ref.delivered_generation) == (7, generation)


def test_set_kreuzakt_id_fenced_write_succeeds_while_generation_holds(store):
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="doc-1.pdf")
    generation = store.get_generation()
    assert store.ledger_set_kreuzakt_id(
        papra_org_id="org-a", papra_document_id="doc-1", kreuzakt_id=7,
        generation=generation, only_if_generation=generation) is True
    assert store.ledger_get("org-a", "doc-1").kreuzakt_id == 7


def test_set_kreuzakt_id_fenced_refusal_leaves_the_row_untouched(store):
    """Task 6's mid-rebuild race: a stale bind must be a refusal, not a `None`.

    The job read generation 1, Kreuzakt was wiped while it waited, and binding
    the id it got back would point a live reference at a meaningless integer.
    `ledger_set_kreuzakt_id` returning False is what makes the job discard its
    result; a `None`-returning version would make the refusal look like success.
    """
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="doc-1.pdf")
    stale = store.get_generation()
    store.ledger_set_kreuzakt_id(papra_org_id="org-a", papra_document_id="doc-1",
                                 kreuzakt_id=7, generation=stale)
    store.bump_generation()
    assert store.ledger_set_kreuzakt_id(
        papra_org_id="org-a", papra_document_id="doc-1", kreuzakt_id=42,
        generation=stale, only_if_generation=stale) is False
    ref = store.ledger_get("org-a", "doc-1")
    assert ref.kreuzakt_id == 7                      # the old binding survived intact
    assert ref.delivered_generation == stale


def test_set_kreuzakt_id_on_unknown_reference_is_false(store):
    assert store.ledger_set_kreuzakt_id(papra_org_id="nope", papra_document_id="nope",
                                        kreuzakt_id=7, generation=1) is False


def test_set_kreuzakt_id_refuses_a_stale_stamp_with_a_fresh_fence(store):
    """`generation` and `only_if_generation` describe one moment or the write lies.

    The fence said "the index is still generation 2" while the stamp said "record
    this binding as generation 1". That combination returned True and left a row
    holding a `delivered_generation` no backfill will ever match: the reference
    resolved to nothing, never appeared as stale in a way the sweep could read as
    "rebind me under 2", and reported success the whole time. A mismatch is a
    caller bug, so it raises before anything is written rather than picking one of
    the two numbers.
    """
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="doc-1.pdf")
    store.bump_generation()
    with pytest.raises(ValueError, match="generation"):
        store.ledger_set_kreuzakt_id(papra_org_id="org-a", papra_document_id="doc-1",
                                     kreuzakt_id=7, generation=1,
                                     only_if_generation=store.get_generation())
    ref = store.ledger_get("org-a", "doc-1")
    assert (ref.kreuzakt_id, ref.delivered_generation) == (None, None)


# -- deletion, reference counted ----------------------------------------------

def test_delete_is_reference_counted(store):
    store = two_refs(store)
    assert store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="doc-1") == (7, 1)
    assert [r.papra_document_id for r in store.ledger_resolve([7], generation=1)] == ["doc-2"]
    assert store.ledger_mark_deleted(papra_org_id="org-b", papra_document_id="doc-2") == (7, 0)
    assert store.ledger_resolve([7], generation=1) == []


def test_delete_counts_only_active_rows_of_the_current_generation(store):
    """`remaining` answers "may we delete upstream?", so it must count what would
    still resolve — a stale-generation sibling protects nothing and must not be
    counted as a reason to keep the Kreuzakt document alive.

    The sibling is made stale the way a rebuild leaves it: the generation moves,
    then the backfill rebinds one reference and not the other. Both rows going
    stale would test the returned id instead (see
    `test_delete_of_a_stale_binding_authorises_nothing`), which is a different rule.
    """
    store = two_refs(store)
    store.bump_generation()
    store.ledger_set_kreuzakt_id(papra_org_id="org-a", papra_document_id="doc-1",
                                 kreuzakt_id=7, generation=store.get_generation())
    assert store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="doc-1") == (7, 0)


def test_delete_of_a_stale_binding_authorises_nothing(store):
    """A stale id is not an authorization, so returning it must not look like one.

    `remaining` was already counted over the current generation, which made a
    stale row report `(7, 0)`: "this was the last live reference, go delete 7".
    Task 9's revalidation cannot catch that, because the queued row carries the
    generation it was queued under — current, after a bump — so the generation
    check passes, and `ledger_resolve([7], current)` is empty only because the
    backfill has not rebound anything yet. The delete then fires at a reused
    integer that a volume restore has since given to unrelated content. When the
    row's own `delivered_generation` is not current, the answer is `(-1, -1)`:
    nothing upstream may be deleted on this row's word.
    """
    store = two_refs(store)
    current = store.bump_generation()
    assert store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="doc-1") == (-1, -1)
    ref = store.ledger_get("org-a", "doc-1")
    assert ref.deleted_at is not None                       # still a soft delete
    assert (ref.kreuzakt_id, ref.delivered_generation) == (7, current - 1)   # still stale


def test_delete_without_a_kreuzakt_id_reports_minus_one(store):
    """(-1, -1) means "nothing upstream to delete", distinct from (7, 0)."""
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="doc-1.pdf")
    assert store.ledger_mark_deleted(papra_org_id="org-a",
                                     papra_document_id="doc-1") == (-1, -1)
    assert store.ledger_get("org-a", "doc-1").deleted_at is not None   # still marked
    assert store.ledger_mark_deleted(papra_org_id="org-a",
                                     papra_document_id="missing") == (-1, -1)


def test_delete_is_idempotent_and_does_not_double_count(store):
    store = two_refs(store)
    assert store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="doc-1") == (7, 1)
    again = store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="doc-1")
    assert again == (7, 1)             # the same answer, not (7, 0) twice


def test_mark_deleted_holds_the_write_lock_while_it_counts(store, tmp_path, monkeypatch):
    """The remaining count is an authorization to delete upstream, so nothing may
    move the index between reading the generation and writing `deleted_at`.

    Proven by attempting an independent write from a second connection while
    `mark_deleted` is taking its reading: with the work inside a `BEGIN IMMEDIATE`
    transaction that write is refused. If it were allowed, the count we just
    computed would describe a generation that had already been superseded.
    """
    store = two_refs(store)
    other = Store(tmp_path / "state.db")
    other._conn.execute("PRAGMA busy_timeout=50")
    real_read = Store._read_generation
    observed = []

    def spy(self):
        value = real_read(self)
        try:
            other._conn.execute("BEGIN IMMEDIATE")
            other._conn.execute(
                "UPDATE settings SET value = '99' WHERE key = ?",
                (store_module.INDEX_GENERATION_KEY,),
            )
            other._conn.commit()
            observed.append("second connection wrote during the count")
        except sqlite3.OperationalError:
            observed.append("refused")        # the write lock is held: correct
            other._conn.rollback()
        return value

    monkeypatch.setattr(Store, "_read_generation", spy)
    try:
        assert store.ledger_mark_deleted(papra_org_id="org-a",
                                         papra_document_id="doc-1") == (7, 1)
        assert observed == ["refused"]
    finally:
        other.close()


# -- upsert keeps what it did not come to write --------------------------------

def test_upsert_does_not_clobber_protection(store):
    store = two_refs(store)
    store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field="name")
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="scan.pdf")
    assert store.ledger_get("org-a", "doc-1").name_protected is True
    assert store.ledger_get("org-a", "doc-1").kreuzakt_id == 7


def test_upsert_does_not_clobber_delivery_state(store):
    """Re-ingesting a document must not erase the fact that we already wrote
    Kreuzakt's text into Papra: `delivered_sha` is what the convergence pass
    compares, and losing it would make every redelivery look like a first write."""
    store = two_refs(store)
    generation = store.get_generation()
    store.ledger_record_delivery(papra_org_id="org-a", papra_document_id="doc-1",
                                 delivered_at="2026-11-17T09:00:00+00:00",
                                 delivered_sha="ca" * 32, generation=generation)
    store.ledger_record_metadata(papra_org_id="org-a", papra_document_id="doc-1",
                                 name="Rechnung 4711")
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="scan.pdf")
    ref = store.ledger_get("org-a", "doc-1")
    assert ref.delivered_at == "2026-11-17T09:00:00+00:00"
    assert ref.delivered_sha == "ca" * 32
    assert ref.delivered_generation == generation
    assert ref.written_name == "Rechnung 4711"


def test_upsert_keeps_the_first_seen_papra_name_as_the_baseline(store):
    """`papra_original_name` is the ownership baseline Task 8 compares against.

    It is the name Papra had the first time we saw the document. Overwriting it
    with a later name would move the baseline under the comparison — and a moved
    baseline either protects a field forever or authorises a rename we did not
    make.
    """
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="first.pdf")
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="later.pdf")
    assert store.ledger_get("org-a", "doc-1").papra_original_name == "first.pdf"


def test_upsert_follows_the_hash_when_the_bytes_change(store):
    """The hash describes today's bytes; keeping yesterday's would make
    `ledger_resolve` prove a claim about content it never saw."""
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="doc-1.pdf")
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash="cd" * 32, papra_original_name="doc-1.pdf")
    assert store.ledger_get("org-a", "doc-1").file_hash == "cd" * 32


def test_changed_bytes_drop_the_binding_and_the_row_becomes_reindexable(store):
    """Kreuzakt indexed those bytes, not these.

    Keeping the id would leave the row resolving to text extracted from a document
    that no longer exists, AND keep it out of `ledger_needs_index` — so the sweep
    would never correct it and search would keep answering with the old file's
    text. Dropping the binding is the only state that is both honest and
    self-healing. A human's protection survives: it is a decision about the
    document, not about which bytes were indexed.
    """
    store = two_refs(store)
    store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field="name")
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash="cd" * 32, papra_original_name="doc-1.pdf")
    ref = store.ledger_get("org-a", "doc-1")
    assert (ref.file_hash, ref.kreuzakt_id, ref.delivered_generation) == (
        "cd" * 32, None, None)
    assert ref.name_protected is True
    assert [r.papra_document_id for r in
            store.ledger_needs_index(generation=1)] == ["doc-1"]
    # the other bucket's reference to the same Kreuzakt document is untouched
    assert [r.papra_document_id for r in store.ledger_resolve([7], generation=1)] == ["doc-2"]


def test_unchanged_bytes_keep_the_binding(store):
    """Re-ingesting the same file must not schedule a pointless re-index."""
    store = two_refs(store)
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="doc-1.pdf")
    assert store.ledger_get("org-a", "doc-1").kreuzakt_id == 7
    assert store.ledger_needs_index(generation=1) == []


def test_ledger_get_unknown_returns_none(store):
    assert store.ledger_get("org-a", "doc-1") is None


def test_record_delivery_refuses_a_row_it_cannot_vouch_for(store, caplog):
    """Recording a delivery also stamps `delivered_generation`, which is what makes
    a binding resolve — so it is an authorization write, not a receipt.

    Writing the current generation onto a row still bound to a destroyed index
    would make that stale integer resolvable again; writing an old generation onto
    a freshly rebound row would retire a live one. Both are refusals.

    The refusal is visible in the row and in the log, not in the return value:
    this method's contract is `-> None`, and a caller that cannot see the refusal
    in either of those two places would be the bug.
    """
    store = two_refs(store)
    stale = store.get_generation()
    store.bump_generation()
    with caplog.at_level("INFO"):
        assert store.ledger_record_delivery(
            papra_org_id="org-a", papra_document_id="doc-1", delivered_at="x",
            delivered_sha="ca" * 32, generation=store.get_generation()) is None
    assert "delivery refused" in caplog.text
    ref = store.ledger_get("org-a", "doc-1")
    assert ref.delivered_sha is None and ref.delivered_generation == stale
    # recording for the generation the row is actually bound to still works
    assert store.ledger_record_delivery(papra_org_id="org-a", papra_document_id="doc-1",
                                        delivered_at="x", delivered_sha="ca" * 32,
                                        generation=stale) is None
    ref = store.ledger_get("org-a", "doc-1")
    assert ref.delivered_sha == "ca" * 32
    # an unknown reference is a no-op, not an error for a document nobody tracked
    assert store.ledger_record_delivery(papra_org_id="org-z", papra_document_id="doc-z",
                                        delivered_at="x", delivered_sha="ca" * 32,
                                        generation=stale) is None


def test_a_late_delivery_cannot_restamp_a_rebound_row(store, caplog):
    store = two_refs(store)
    old = store.get_generation()
    store.bump_generation()
    current = store.get_generation()
    store.ledger_set_kreuzakt_id(papra_org_id="org-a", papra_document_id="doc-1",
                                 kreuzakt_id=42, generation=current)
    with caplog.at_level("INFO"):
        store.ledger_record_delivery(papra_org_id="org-a", papra_document_id="doc-1",
                                     delivered_at="x", delivered_sha="ca" * 32,
                                     generation=old)
    ref = store.ledger_get("org-a", "doc-1")
    assert (ref.kreuzakt_id, ref.delivered_generation) == (42, current)
    assert ref.delivered_sha is None
    assert "delivery refused" in caplog.text


# -- protection ---------------------------------------------------------------

def test_clear_protection_counts_and_scopes(store):
    store = two_refs(store)
    for field in ("name", "date", "notes"):
        store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field=field)
    assert store.ledger_clear_protection(field="name", org_id="org-a") == 1
    ref = store.ledger_get("org-a", "doc-1")
    assert not ref.name_protected and ref.date_protected and ref.notes_protected


def test_clear_protection_without_field_clears_all_three(store):
    """`field=None` means every field; the count is references, not flags.

    One reference with all three flags cleared is 1, because the number the CLI
    prints has to answer "how many documents did this change?") — counting flags
    would multiply the blast radius by three on the same set of documents.
    """
    store = two_refs(store)
    for field in ("name", "date", "notes"):
        store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field=field)
    assert store.ledger_clear_protection(org_id="org-a") == 1
    ref = store.ledger_get("org-a", "doc-1")
    assert (ref.name_protected, ref.date_protected, ref.notes_protected) == (
        False, False, False)
    assert store.ledger_get("org-b", "doc-2").name_protected is False


def test_clear_protection_is_scoped_to_one_document(store):
    store = two_refs(store)
    store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field="name")
    store.ledger_protect(papra_org_id="org-b", papra_document_id="doc-2", field="name")
    assert store.ledger_clear_protection(field="name", org_id="org-a",
                                         document_id="doc-1") == 1
    assert store.ledger_get("org-b", "doc-2").name_protected is True


def test_clear_protection_without_scope_clears_every_bucket(store):
    store = two_refs(store)
    for org, doc in (("org-a", "doc-1"), ("org-b", "doc-2")):
        store.ledger_protect(papra_org_id=org, papra_document_id=doc, field="notes")
    assert store.ledger_clear_protection(field="notes") == 2


def test_protection_field_is_a_closed_set(store):
    """A typo here would protect nothing while the operator believed otherwise."""
    store = two_refs(store)
    with pytest.raises(ValueError, match="field"):
        store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field="tile")
    with pytest.raises(ValueError, match="field"):
        store.ledger_clear_protection(field="titel")


def test_clear_protection_counts_only_references_that_were_protected(store):
    """The number the CLI prints is "how many documents did this change?".

    SQLite reports every row the WHERE matched, protection or not, so without a
    flag predicate a second run of the same command would report the same count
    again and an operator could not tell a real un-protect from a no-op.
    """
    store = two_refs(store)
    store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field="date")
    assert store.ledger_clear_protection(field="name") == 0     # nothing was name-protected
    assert store.ledger_clear_protection(field="date") == 1
    assert store.ledger_clear_protection(field="date") == 0
    assert store.ledger_clear_protection() == 0
    assert store.ledger_get("org-a", "doc-1").date_protected is False


# -- ownership baselines ------------------------------------------------------

def test_record_metadata_records_only_the_fields_given(store):
    store = two_refs(store)
    store.ledger_record_metadata(papra_org_id="org-a", papra_document_id="doc-1",
                                 name="Rechnung 4711")
    ref = store.ledger_get("org-a", "doc-1")
    assert ref.written_name == "Rechnung 4711"
    assert ref.written_date is None and ref.written_notes is None
    store.ledger_record_metadata(papra_org_id="org-a", papra_document_id="doc-1",
                                 date="2026-11-17", notes="aus Kreuzakt")
    ref = store.ledger_get("org-a", "doc-1")
    assert (ref.written_name, ref.written_date, ref.written_notes) == (
        "Rechnung 4711", "2026-11-17", "aus Kreuzakt")


def test_record_metadata_of_nothing_changes_nothing(store):
    store = two_refs(store)
    store.ledger_record_metadata(papra_org_id="org-a", papra_document_id="doc-1",
                                 name="Rechnung 4711")
    store.ledger_record_metadata(papra_org_id="org-a", papra_document_id="doc-1")
    assert store.ledger_get("org-a", "doc-1").written_name == "Rechnung 4711"


def test_record_metadata_does_not_touch_protection(store):
    """Recording what we wrote and lifting a protection are different decisions;
    conflating them would let a delivery silently un-protect a human's rename."""
    store = two_refs(store)
    store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field="name")
    store.ledger_record_metadata(papra_org_id="org-a", papra_document_id="doc-1",
                                 name="unsere Version")
    assert store.ledger_get("org-a", "doc-1").name_protected is True


# -- repair window ------------------------------------------------------------

def test_repair_window_rolls_over(store):
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 1
    assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 2
    store._conn.execute("UPDATE index_ledger SET last_repair_at = '2000-01-01T00:00:00+00:00'")
    store._conn.commit()
    assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 1
    assert store.ledger_get("o", "d").last_repair_at > "2026-01-01"


def test_repair_counter_is_per_reference(store):
    store = two_refs(store)
    assert store.ledger_bump_repair(papra_org_id="org-a", papra_document_id="doc-1") == 1
    assert store.ledger_bump_repair(papra_org_id="org-a", papra_document_id="doc-1") == 2
    assert store.ledger_bump_repair(papra_org_id="org-b", papra_document_id="doc-2") == 1
    assert store.ledger_get("org-b", "doc-2").repair_count == 1


def test_repairs_in_window_reads_the_window_without_spending_it(store):
    """The read-only twin of `ledger_bump_repair`, for a ceiling checked before writing.

    Task 7's review found that bumping before the PATCH let a write Papra *refused* spend
    the suppression budget: a document whose upload Papra persistently rejects reached
    suppression without ever having been repaired once, and the pass then blamed a second
    writer. So the pass asks this question first and spends the unit only when the text
    landed. It must therefore answer one thing exactly: what `ledger_bump_repair` would
    return, minus the unit the caller has not spent yet — 0 in the state where the bump
    returns 1.
    """
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    assert store.ledger_repairs_in_window(papra_org_id="o", papra_document_id="d") == 0

    assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 1
    assert store.ledger_repairs_in_window(papra_org_id="o", papra_document_id="d") == 1
    assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 2
    assert store.ledger_repairs_in_window(papra_org_id="o", papra_document_id="d") == 2

    # Reading is not spending: the bump's own arithmetic is untouched.
    assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 3


def test_repairs_in_window_forgets_a_window_that_rolled_over(store):
    """Same rolling rule as the bump, so a bad week cannot suppress a document forever."""
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_bump_repair(papra_org_id="o", papra_document_id="d")
    store.ledger_bump_repair(papra_org_id="o", papra_document_id="d")
    assert store.ledger_repairs_in_window(papra_org_id="o", papra_document_id="d") == 2

    store._conn.execute("UPDATE index_ledger SET last_repair_at = '2000-01-01T00:00:00+00:00'")
    store._conn.commit()
    assert store.ledger_repairs_in_window(papra_org_id="o", papra_document_id="d") == 0


def test_repairs_in_window_on_unknown_reference_raises(store):
    """The same fail-closed rule as `ledger_bump_repair`: a row we never saw is a caller bug."""
    with pytest.raises(ValueError, match="unknown ledger reference"):
        store.ledger_repairs_in_window(papra_org_id="o", papra_document_id="missing")


def test_bump_repair_on_unknown_reference_raises(store):
    with pytest.raises(ValueError, match="unknown ledger reference"):
        store.ledger_bump_repair(papra_org_id="o", papra_document_id="missing")


def test_bump_repair_holds_the_write_lock_while_it_counts(store, tmp_path, monkeypatch):
    """The repair counter decides a suppression, so a lost increment is a missed one.

    Same hazard the previous two commits fixed in `ledger_mark_deleted` and
    `ledger_cleanup_result`: reading `repair_count`/`last_repair_at` before
    pysqlite opens the transaction lets the daemon's verification pass and an
    operator's forced repair both read 1 and both write 2. Task 7 suppresses on
    "two repairs in 24 h", so the lost increment is a document that keeps
    diverging while the counter says it is new. Proven the same way: the read
    itself attempts an independent write from a second connection, which must be
    refused.
    """
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 1
    other = Store(tmp_path / "state.db")
    other._conn.execute("PRAGMA busy_timeout=50")
    real_read = Store._read_repair_row
    observed = []

    def spy(self, papra_org_id, papra_document_id):
        row = real_read(self, papra_org_id, papra_document_id)
        try:
            other._conn.execute("BEGIN IMMEDIATE")
            other._conn.execute(
                "UPDATE index_ledger SET repair_count = 99 WHERE papra_document_id = ?",
                (papra_document_id,),
            )
            other._conn.commit()
            observed.append("second connection wrote during the read")
        except sqlite3.OperationalError:
            observed.append("refused")        # the write lock is held: correct
            other._conn.rollback()
        return row

    monkeypatch.setattr(Store, "_read_repair_row", spy)
    try:
        assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 2
        assert observed == ["refused"]
        assert store.ledger_get("o", "d").repair_count == 2
    finally:
        other.close()


# -- cleanup queue ------------------------------------------------------------

def test_cleanup_due_ignores_done_and_future_rows(store):
    for doc in ("due", "done", "future"):
        store.ledger_upsert_reference(papra_org_id="o", papra_document_id=doc,
                                      file_hash=H, papra_original_name=f"{doc}.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="due",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=store.get_generation())

    def repatch(doc, state, nxt):
        store._conn.execute("UPDATE index_ledger SET cleanup_state=?, cleanup_next_at=? "
                            "WHERE papra_document_id=?", (state, nxt, doc))

    repatch("done", "done", "2026-11-17T00:00:00+00:00")
    repatch("future", "pending", "2099-01-01T00:00:00+00:00")
    store._conn.commit()
    due = store.ledger_cleanup_due(now="2026-11-17T12:00:00+00:00")
    assert [r.papra_document_id for r in due] == ["due"]


def test_queue_cleanup_records_the_intent_on_the_ref(store):
    """`run_cleanup` revalidates from these two columns after a restart: which
    Kreuzakt id it is allowed to delete, and which generation gave it meaning.
    If either were absent, a rebuild would turn the queued delete into a hit on a
    reused integer."""
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    generation = store.get_generation()
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=generation)
    ref = store.ledger_get("o", "d")
    assert ref.cleanup_state == "pending"
    assert ref.cleanup_next_at == "2026-11-17T00:00:00+00:00"
    assert ref.cleanup_kreuzakt_id == 7 and ref.cleanup_generation == generation
    assert ref.cleanup_attempts == 0


def test_cleanup_result_success_marks_done(store):
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d", ok=True)
    ref = store.ledger_get("o", "d")
    assert ref.cleanup_state == "done" and ref.cleanup_next_at is None
    assert store.ledger_cleanup_due(now="2026-11-18T00:00:00+00:00") == []


def test_cleanup_result_failure_backs_off_then_stops_persistently(store, caplog):
    """Five failures is the end of automatic retry, and one loud line about it.

    The count and the final state are what `papra-suite ledger --cleanup` needs
    to force a retry; the single ERROR is what stops a polling loop from writing
    the same line every pass forever.
    """
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    for attempt in range(1, store_module.CLEANUP_MAX_ATTEMPTS):
        store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d", ok=False,
                                    next_at=f"2026-11-17T0{attempt}:00:00+00:00",
                                    reason="kreuzakt 503")
        ref = store.ledger_get("o", "d")
        assert ref.cleanup_state == "pending"
        assert ref.cleanup_attempts == attempt
        assert ref.cleanup_next_at == f"2026-11-17T0{attempt}:00:00+00:00"
    with caplog.at_level("INFO"):
        store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d", ok=False,
                                    reason="kreuzakt 503")
    ref = store.ledger_get("o", "d")
    assert ref.cleanup_state == "failed"
    assert ref.cleanup_attempts == store_module.CLEANUP_MAX_ATTEMPTS
    assert sum("cleanup failed permanently" in r.message for r in caplog.records) == 1
    # a permanently failed row is no longer due: the queue stops grinding on it
    assert store.ledger_cleanup_due(now="2099-01-01T00:00:00+00:00") == []


def test_cleanup_result_without_next_at_is_due_immediately(store):
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d", ok=False)
    assert store.ledger_cleanup_due(now="2999-01-01T00:00:00+00:00") != []


def test_cleanup_due_is_ordered_by_due_time_and_capped(store):
    for index in range(5):
        store.ledger_upsert_reference(papra_org_id="o", papra_document_id=f"doc-{index}",
                                      file_hash=H, papra_original_name=f"{index}.pdf")
        store.ledger_queue_cleanup(papra_org_id="o", papra_document_id=f"doc-{index}",
                                   due_at=f"2026-11-17T00:00:0{index}+00:00",
                                   kreuzakt_id=7, generation=1)
    due = store.ledger_cleanup_due(now="2026-11-17T12:00:00+00:00", limit=2)
    assert [r.papra_document_id for r in due] == ["doc-0", "doc-1"]


def test_cleanup_due_includes_a_pending_row_with_no_next_time(store):
    """A `pending` row whose `cleanup_next_at` is NULL is owed, not scheduled away.

    Not reachable through today's API — `ledger_queue_cleanup` always writes a
    time and so does a recorded failure — but the comparison is `cleanup_next_at
    <= ?`, and `NULL <= now` is NULL: a later edit, or an operator repairing the
    queue with a plain `UPDATE`, would leave a queued delete permanently due
    without it ever being reported. The guard costs nothing and keeps "pending"
    meaning "someone still has to do this".
    """
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    store._conn.execute("UPDATE index_ledger SET cleanup_next_at = NULL"
                        " WHERE papra_document_id = 'd'")
    store._conn.commit()
    due = store.ledger_cleanup_due(now="2026-11-17T00:00:00+00:00")
    assert [r.papra_document_id for r in due] == ["d"]


def test_skip_cleanup_parks_a_row_without_retrying_or_failing_it(store, caplog):
    """The state machine needs Task 9's third answer: owed, but not ours to do.

    A queued delete can turn out to be unnecessary rather than failed — the Papra
    document was restored, the generation moved so the id means something else
    now, or a live sibling still resolves to it. Without a `skipped` outcome in
    the machine that pass would have to hand-write SQL against the table the MCP
    server authorizes from, so the machine gained the fourth state. Two properties
    make it the right state rather than a fifth spelling of `done`: it is not due
    again (nothing is waiting for a retry), and it is not counted as a failure
    (nothing is failing).
    """
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    with caplog.at_level("INFO"):
        store.ledger_skip_cleanup(papra_org_id="o", papra_document_id="d",
                                  reason="generation changed")
    ref = store.ledger_get("o", "d")
    assert ref.cleanup_state == "skipped"
    assert ref.cleanup_next_at is None
    assert ref.cleanup_attempts == 0            # skipping is not an attempt
    assert store.ledger_cleanup_due(now="2999-01-01T00:00:00+00:00") == []
    stats = store.ledger_stats()
    assert stats["cleanup_pending"] == 0 and stats["cleanup_failed"] == 0
    assert [r.message for r in caplog.records if "cleanup failed" in r.message] == []
    assert any("cleanup skipped" in r.message for r in caplog.records)


def test_skip_cleanup_on_unknown_reference_raises(store):
    with pytest.raises(ValueError, match="unknown ledger reference"):
        store.ledger_skip_cleanup(papra_org_id="o", papra_document_id="missing")


def test_a_permanently_failed_delete_is_still_counted(store):
    """`check` must not go quiet at the moment a delete stops happening by itself.

    On the fifth failure the row leaves `pending`, so a counter that watches only
    `pending` reports a clean queue exactly when automatic retry ends — the number
    fell to 0 while the debt became permanent. `cleanup_pending` is now the total
    that still owes an upstream delete, and `cleanup_failed` names the subset the
    retries have stopped on, so an operator can tell "still trying" from "this one
    is on you" without either one hiding the other.
    """
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    for _ in range(store_module.CLEANUP_MAX_ATTEMPTS):
        store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d",
                                    ok=False, reason="kreuzakt 503")
    assert store.ledger_get("o", "d").cleanup_state == "failed"
    stats = store.ledger_stats()
    assert stats["cleanup_pending"] == 1
    assert stats["cleanup_failed"] == 1


def test_forced_cleanup_pass_can_give_a_failed_row_a_working_allowance(store):
    """`papra-suite ledger --cleanup` re-queues on purpose, and says so in code.

    With no way to reset, the operator's deliberate command got exactly one try:
    the row was already at `CLEANUP_MAX_ATTEMPTS`, so the first failure after the
    command wrote `failed` again even when the outage had ended. `reset_attempts`
    exists so that pass can be explicit about refilling the allowance. The default
    is False — and has to stay False — because a re-queue that refilled for free
    would let an ingest-driven re-mark top the counter up on every delete and the
    five-attempt cap would never bite.
    """
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    for _ in range(store_module.CLEANUP_MAX_ATTEMPTS):
        store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d",
                                    ok=False, reason="kreuzakt 503")
    # the default: re-queueing changes the schedule, not the allowance
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-18T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    assert store.ledger_get("o", "d").cleanup_attempts == store_module.CLEANUP_MAX_ATTEMPTS
    # the deliberate reset, then one full recovery pass
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-18T00:00:00+00:00",
                               kreuzakt_id=7, generation=1, reset_attempts=True)
    ref = store.ledger_get("o", "d")
    assert (ref.cleanup_state, ref.cleanup_attempts) == ("pending", 0)
    assert [r.papra_document_id for r in
            store.ledger_cleanup_due(now="2026-11-18T12:00:00+00:00")] == ["d"]
    store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d", ok=False,
                                next_at="2026-11-18T13:00:00+00:00", reason="kreuzakt 503")
    assert store.ledger_get("o", "d").cleanup_state == "pending"   # not failed again
    store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d", ok=True)
    assert store.ledger_get("o", "d").cleanup_state == "done"


@pytest.mark.parametrize("due_at", ["2026-11-17T00:00:00Z", "2026-11-17T00:00:00",
                                    "2026-11-17T00:00:00+01:00", "2026-11-17 00:00:00+00:00",
                                    "tomorrow"])
def test_queue_cleanup_refuses_a_due_at_that_is_not_the_stores_utc_form(store, due_at):
    """Backoffs are compared as text, so the write boundary is the only honest guard.

    `cleanup_next_at <= now` is chronological only while every value carries the
    same `+00:00` offset: a `Z` suffix sorts after every offset, a naive or a
    `+01:00` value is silently hours early or late, and the result is a backoff
    that mis-times with no error anywhere. Refusing the write is cheap; discovering
    it from a delete that fired too early is not.
    """
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    with pytest.raises(ValueError, match="due_at"):
        store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                                   due_at=due_at, kreuzakt_id=7, generation=1)
    ref = store.ledger_get("o", "d")
    assert (ref.cleanup_state, ref.cleanup_next_at) == ("none", None)   # nothing written


def test_cleanup_result_refuses_a_next_at_that_is_not_the_stores_utc_form(store):
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    with pytest.raises(ValueError, match="next_at"):
        store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d", ok=False,
                                    next_at="2026-11-17T01:00:00Z", reason="kreuzakt 503")
    ref = store.ledger_get("o", "d")
    assert (ref.cleanup_state, ref.cleanup_attempts) == ("pending", 0)  # attempt not burned


# -- generation ---------------------------------------------------------------

def test_generation_defaults_to_one_and_bumps_monotonically(store):
    assert store.get_generation() == 1
    assert store.bump_generation() == 2
    assert store.bump_generation() == 3
    assert store.get_generation() == 3
    assert store.get_setting("index_generation") == "3"


def test_generation_survives_reopening_the_store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.bump_generation()
    store.close()
    reopened = Store(tmp_path / "state.db")
    reopened.migrate()
    try:
        assert reopened.get_generation() == 2
    finally:
        reopened.close()


def test_a_corrupt_generation_fails_closed_rather_than_reauthorising_epoch_one(store):
    """Defaulting a corrupt value to 1 would be a fail-open.

    Generation 1 is the namespace the very first index bound into, so a settings
    row that became unreadable mid-life (an operator's note typed over the value,
    a half-applied migration) would silently re-authorise every row that a rebuild
    was supposed to retire. Refusing is the only reading that authorises nothing,
    and the message names the key so the operator knows what to set.
    """
    two_refs(store)
    store.set_setting("index_generation", "rebuild-2026-11-17")
    with pytest.raises(ValueError, match="index_generation"):
        store.get_generation()
    with pytest.raises(ValueError, match="index_generation"):
        store.ledger_stats()
    with pytest.raises(ValueError, match="index_generation"):
        store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="doc-1")
    # nothing was written on the way to the refusal
    assert store.ledger_get("org-a", "doc-1").deleted_at is None


def test_bump_generation_repairs_a_corrupt_value_above_every_existing_epoch(store):
    """The rebuild command has to work on a corrupt store, and its answer must not
    be an epoch some existing row already carries — that is the same reauthorising
    mistake in a different place. Deriving the next value from the ledger's highest
    bound generation makes the repair strictly new."""
    store.set_setting("index_generation", "5")
    two_refs(store)                      # bound at generation 5
    store.set_setting("index_generation", "rebuild-2026-11-17")
    assert store.bump_generation() == 6
    assert store.ledger_resolve([7], generation=store.get_generation()) == []
    assert len(store.ledger_needs_index(generation=store.get_generation())) == 2


# -- settings_flag ------------------------------------------------------------

def test_settings_flag_missing_key_is_false(store):
    assert store.settings_flag("index_content_delivery") is False


def test_settings_flag_reads_the_spellings_an_operator_types(store):
    for value in ("1", "true", "True", "TRUE", "yes", "on"):
        store.set_setting("index_content_delivery", value)
        assert store.settings_flag("index_content_delivery") is True, value
    for value in ("0", "false", "False", "no", "off", ""):
        store.set_setting("index_content_delivery", value)
        assert store.settings_flag("index_content_delivery") is False, value


# -- needs_index --------------------------------------------------------------

def test_needs_index_covers_unbound_stale_and_skips_deleted(store):
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="unbound",
                                  file_hash=H, papra_original_name="unbound.pdf")
    generation = store.get_generation()
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="bound",
                                  file_hash=H, papra_original_name="bound.pdf")
    store.ledger_set_kreuzakt_id(papra_org_id="org-a", papra_document_id="bound",
                                 kreuzakt_id=7, generation=generation)
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="gone",
                                  file_hash=H, papra_original_name="gone.pdf")
    store.ledger_set_kreuzakt_id(papra_org_id="org-a", papra_document_id="gone",
                                 kreuzakt_id=8, generation=generation)
    store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="gone")
    assert [r.papra_document_id for r in
            store.ledger_needs_index(generation=generation)] == ["unbound"]
    store.bump_generation()
    assert [r.papra_document_id for r in store.ledger_needs_index(generation=2)] == [
        "bound", "unbound"]


def test_needs_index_scopes_to_one_bucket_and_honours_the_limit(store):
    store = two_refs(store)
    store.bump_generation()
    assert [r.papra_document_id for r in
            store.ledger_needs_index(generation=2, org_id="org-b")] == ["doc-2"]
    assert len(store.ledger_needs_index(generation=2, limit=1)) == 1


# -- delivered_refs: Task 7's verification walk ------------------------------------

def many_bound(store, *, count: int, org: str = "org-a", generation: int = 1) -> None:
    for index in range(count):
        document_id = f"doc-{index:03d}"
        store.ledger_upsert_reference(papra_org_id=org, papra_document_id=document_id,
                                      file_hash=H, papra_original_name=f"{document_id}.pdf")
        store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=document_id,
                                     kreuzakt_id=7, generation=generation)


def test_delivered_refs_walks_rows_bound_to_the_generation(store):
    """The filter is "indexed in this generation", not "we have written this one".

    The name says *delivered*, and the rows that matter most after the handover are the
    ones that were never delivered: five hundred documents indexed while content
    delivery was off, holding Papra's own extraction. A walk that filtered on
    `delivered_at` would converge nothing on the first pass after the flag flip and
    report a healthy corpus the whole time.
    """
    many_bound(store, count=2)
    store.ledger_record_delivery(papra_org_id="org-a", papra_document_id="doc-000",
                                 delivered_at="2026-11-17 10:00:00",
                                 delivered_sha=H, generation=1)

    refs = store.ledger_delivered_refs(generation=1)

    assert [ref.papra_document_id for ref in refs] == ["doc-000", "doc-001"]
    assert refs[1].delivered_at is None and refs[1].delivered_sha is None


def test_delivered_refs_skips_stale_unbound_and_deleted(store):
    """Three rows that must not be written into, for three different reasons.

    A stale generation names an index that no longer exists, an unbound row has no text
    to deliver, and a soft-deleted row is a document the user took away. All three are
    exclusion by the same filter the MCP resolver uses, because the reason a stale
    integer must not resolve is the reason it must not be PATCHed over.
    """
    many_bound(store, count=1)                                        # doc-000, gen 1
    many_bound(store, count=1, org="org-stale", generation=2)         # doc-000, gen 2
    store.ledger_upsert_reference(papra_org_id="org-b", papra_document_id="doc-new",
                                  file_hash=H, papra_original_name="doc-new.pdf")
    store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="doc-000")

    refs = store.ledger_delivered_refs(generation=1)

    assert [(ref.papra_org_id, ref.papra_document_id) for ref in refs] == []
    # and with the deletion undone the row is back, so the exclusion is `deleted_at`
    # rather than the row having been touched by anything else.
    store.ledger_mark_restored(papra_org_id="org-a", papra_document_id="doc-000")
    assert [ref.papra_document_id for ref in store.ledger_delivered_refs(generation=1)] == [
        "doc-000"]
    assert [ref.papra_document_id
            for ref in store.ledger_delivered_refs(generation=2,
                                                  org_ids=["org-stale"])] == ["doc-000"]


def test_delivered_refs_pages_by_keyset_without_repeating(store):
    many_bound(store, count=5)

    first = store.ledger_delivered_refs(generation=1, limit=2)
    second = store.ledger_delivered_refs(generation=1, limit=2,
                                         after=(first[-1].papra_org_id,
                                                first[-1].papra_document_id))
    third = store.ledger_delivered_refs(generation=1, limit=2,
                                        after=(second[-1].papra_org_id,
                                               second[-1].papra_document_id))

    seen = [ref.papra_document_id for page in (first, second, third) for ref in page]
    assert seen == ["doc-000", "doc-001", "doc-002", "doc-003", "doc-004"]
    assert len(third) == 1                       # a short page says "that is the end"


def test_delivered_refs_scopes_to_buckets_and_reads_an_empty_list_as_nothing(store):
    """`org_ids=[]` is "no buckets", matching `ledger_stats` and `ledger_resolve`.

    A token with nothing granted must not receive a corpus-wide walk it can write to.
    """
    many_bound(store, count=1)
    many_bound(store, count=1, org="org-b")

    assert store.ledger_delivered_refs(generation=1, org_ids=[]) == []
    assert [ref.papra_org_id
            for ref in store.ledger_delivered_refs(generation=1, org_ids=["org-b"])] == ["org-b"]


def test_delivered_refs_limit_is_bounded_and_never_zero(store):
    """A limit of 0 would end the walk on its first page, silently and successfully."""
    many_bound(store, count=3)

    assert len(store.ledger_delivered_refs(generation=1, limit=0)) == 1
    assert len(store.ledger_delivered_refs(generation=1, limit=10 ** 6)) == 3


# -- stats --------------------------------------------------------------------

def test_stats_keys_and_counts(store):
    generation = store.get_generation()
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="doc-1.pdf")
    store.ledger_upsert_reference(papra_org_id="org-b", papra_document_id="doc-2",
                                  file_hash=H, papra_original_name="doc-2.pdf")
    store.ledger_set_kreuzakt_id(papra_org_id="org-a", papra_document_id="doc-1",
                                 kreuzakt_id=7, generation=generation)
    store.ledger_record_delivery(papra_org_id="org-a", papra_document_id="doc-1",
                                 delivered_at="2026-11-17T09:00:00+00:00",
                                 delivered_sha="ca" * 32, generation=generation)
    store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field="name")
    store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field="date")
    store.ledger_bump_repair(papra_org_id="org-a", papra_document_id="doc-1")
    store.ledger_queue_cleanup(papra_org_id="org-b", papra_document_id="doc-2",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=generation)
    stats = store.ledger_stats()
    assert set(stats) == {"references", "indexed_current", "delivered", "extraction_empty",
                          "protected_name", "protected_date", "protected_notes",
                          "repairs_24h", "cleanup_pending", "cleanup_failed"}
    assert stats["references"] == 2
    assert stats["indexed_current"] == 1
    assert stats["delivered"] == 1
    assert stats["protected_name"] == 1
    assert stats["protected_date"] == 1
    assert stats["protected_notes"] == 0
    assert stats["repairs_24h"] == 1
    assert stats["cleanup_pending"] == 1
    assert stats["cleanup_failed"] == 0        # queued, but still being retried


def test_stats_scopes_to_the_requested_buckets(store):
    store = two_refs(store)
    assert store.ledger_stats(org_ids=["org-a"])["references"] == 1
    assert store.ledger_stats(org_ids=["org-a", "org-b"])["references"] == 2
    # an empty scope is "no buckets", never "no filter": a token with nothing
    # granted must not receive the corpus-wide counts
    empty = store.ledger_stats(org_ids=[])
    assert all(value == 0 for value in empty.values())
    assert empty["references"] == 0


def test_stats_counts_only_the_current_generation_as_indexed(store):
    store = two_refs(store)
    store.ledger_record_delivery(papra_org_id="org-a", papra_document_id="doc-1",
                                 delivered_at="2026-11-17T09:00:00+00:00",
                                 delivered_sha="ca" * 32,
                                 generation=store.get_generation())
    store.bump_generation()
    stats = store.ledger_stats()
    assert stats["references"] == 2            # the rows are still there
    assert stats["indexed_current"] == 0       # but they index nothing any more
    assert stats["delivered"] == 0


def test_extraction_empty_counts_rows_whose_delivered_text_was_empty(store):
    store = two_refs(store)
    generation = store.get_generation()
    store.ledger_record_delivery(papra_org_id="org-a", papra_document_id="doc-1",
                                 delivered_at="2026-11-17T09:00:00+00:00",
                                 delivered_sha=EMPTY_EXTRACTION_SHA, generation=generation)
    assert store.ledger_stats()["extraction_empty"] == 1
    assert store.ledger_stats()["delivered"] == 1      # the other row was never delivered


def test_stats_repairs_window_ignores_old_repairs(store):
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_bump_repair(papra_org_id="o", papra_document_id="d")
    store._conn.execute("UPDATE index_ledger SET last_repair_at = '2000-01-01T00:00:00+00:00'")
    store._conn.commit()
    assert store.ledger_stats()["repairs_24h"] == 0


# -- the contract itself ------------------------------------------------------

def test_cleanup_success_is_not_reported_as_a_failure(store, caplog):
    """A completed delete must not write the failure line.

    The one place an operator learns that an orphan is gone would otherwise be
    indistinguishable from the place he learns the retry is still failing — the
    state machine said `done` while the log said "will retry".
    """
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    with caplog.at_level("INFO"):
        store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d", ok=True)
    assert store.ledger_get("o", "d").cleanup_state == "done"
    assert [r.message for r in caplog.records if "cleanup failed" in r.message] == []


def test_permanent_cleanup_failure_is_announced_once(store, caplog):
    """The transition into `failed` is the news; repeating it every forced retry is
    noise, and "exactly one ERROR" is what a log-scraping operator counts on."""
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    with caplog.at_level("INFO"):
        for _ in range(store_module.CLEANUP_MAX_ATTEMPTS + 2):
            store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d",
                                        ok=False, reason="kreuzakt 503")
    ref = store.ledger_get("o", "d")
    assert ref.cleanup_state == "failed"
    assert ref.cleanup_attempts > store_module.CLEANUP_MAX_ATTEMPTS
    assert sum("cleanup failed permanently" in r.message for r in caplog.records) == 1


def test_cleanup_result_holds_the_write_lock_while_it_counts(store, tmp_path, monkeypatch):
    """Same hazard as the delete count, one layer down.

    The attempt counter and the "did this row just become permanently failed?"
    judgement are a read-modify-write. Read outside the write lock, two migrators
    — the daemon's cleanup pass and an operator's forced `ledger --cleanup` — both
    see attempt 4, both write 5, and the operator hears "we have stopped trying"
    twice about one delete while one increment is lost. Proven by having the read
    itself attempt an independent write from a second connection.
    """
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="d",
                               due_at="2026-11-17T00:00:00+00:00",
                               kreuzakt_id=7, generation=1)
    other = Store(tmp_path / "state.db")
    other._conn.execute("PRAGMA busy_timeout=50")
    real_read = Store._read_cleanup_row
    observed = []

    def spy(self, papra_org_id, papra_document_id):
        row = real_read(self, papra_org_id, papra_document_id)
        try:
            other._conn.execute("BEGIN IMMEDIATE")
            other._conn.execute(
                "UPDATE index_ledger SET cleanup_attempts = 99 WHERE papra_document_id = ?",
                (papra_document_id,),
            )
            other._conn.commit()
            observed.append("second connection wrote during the read")
        except sqlite3.OperationalError:
            observed.append("refused")        # the write lock is held: correct
            other._conn.rollback()
        return row

    monkeypatch.setattr(Store, "_read_cleanup_row", spy)
    try:
        store.ledger_cleanup_result(papra_org_id="o", papra_document_id="d", ok=False,
                                    reason="kreuzakt 503")
        assert observed == ["refused"]
        assert store.ledger_get("o", "d").cleanup_attempts == 1
    finally:
        other.close()


def test_ledger_ref_exposes_the_contract_fields_in_order():
    """Task 8 builds a `LedgerRef` from a dict of defaults, so the field set is an
    interface other modules construct against — pin it here rather than let a
    rename in a migration drift it."""
    names = [field.name for field in dataclasses.fields(LedgerRef)]
    assert names == [
        "papra_org_id", "papra_document_id", "kreuzakt_id", "file_hash",
        "papra_original_name", "delivered_at", "delivered_sha", "delivered_generation",
        "written_name", "name_protected", "written_date", "date_protected",
        "written_notes", "notes_protected", "repair_count", "last_repair_at",
        "deleted_at", "cleanup_state", "cleanup_attempts", "cleanup_next_at",
        # the two cleanup columns the delete-propagation pass revalidates from:
        "cleanup_kreuzakt_id", "cleanup_generation",
    ]


def test_protected_flags_are_bools_not_ints(store):
    """`is True` is what the delivery code compares against; a raw 1 would make
    every protection check in the next two tasks silently false."""
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    assert store.ledger_get("o", "d").name_protected is False
    store.ledger_protect(papra_org_id="o", papra_document_id="d", field="date")
    assert store.ledger_get("o", "d").date_protected is True


def test_ledger_rows_survive_reopening_the_store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="doc-1.pdf")
    store.close()
    reopened = Store(tmp_path / "state.db")
    reopened.migrate()
    try:
        ref = reopened.ledger_get("org-a", "doc-1")
        assert ref is not None and ref.papra_original_name == "doc-1.pdf"
        assert ref.cleanup_state == "none"
    finally:
        reopened.close()
