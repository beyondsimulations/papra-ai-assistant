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
    counted as a reason to keep the Kreuzakt document alive."""
    store = two_refs(store)
    store.bump_generation()          # both rows are stale now
    assert store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="doc-1") == (7, 0)


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


def test_ledger_get_unknown_returns_none(store):
    assert store.ledger_get("org-a", "doc-1") is None


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


def test_bump_repair_on_unknown_reference_raises(store):
    with pytest.raises(ValueError, match="unknown ledger reference"):
        store.ledger_bump_repair(papra_org_id="o", papra_document_id="missing")


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


def test_a_corrupt_generation_value_fails_to_the_default(store):
    """Reading garbage as a generation would make every row stale forever, and
    reading it as a crash would make `check` unrunnable — the operator's way out
    is to set the value again, so default it and stay readable."""
    store.set_setting("index_generation", "not-a-number")
    assert store.get_generation() == 1
    assert store.bump_generation() == 2


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
                          "repairs_24h", "cleanup_pending"}
    assert stats["references"] == 2
    assert stats["indexed_current"] == 1
    assert stats["delivered"] == 1
    assert stats["protected_name"] == 1
    assert stats["protected_date"] == 1
    assert stats["protected_notes"] == 0
    assert stats["repairs_24h"] == 1
    assert stats["cleanup_pending"] == 1


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
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    generation = store.get_generation()
    store.ledger_record_delivery(papra_org_id="o", papra_document_id="d",
                                 delivered_at="2026-11-17T09:00:00+00:00",
                                 delivered_sha=EMPTY_EXTRACTION_SHA, generation=generation)
    assert store.ledger_stats()["extraction_empty"] == 1


def test_stats_repairs_window_ignores_old_repairs(store):
    store.ledger_upsert_reference(papra_org_id="o", papra_document_id="d",
                                  file_hash=H, papra_original_name="d.pdf")
    store.ledger_bump_repair(papra_org_id="o", papra_document_id="d")
    store._conn.execute("UPDATE index_ledger SET last_repair_at = '2000-01-01T00:00:00+00:00'")
    store._conn.commit()
    assert store.ledger_stats()["repairs_24h"] == 0


# -- the contract itself ------------------------------------------------------

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
