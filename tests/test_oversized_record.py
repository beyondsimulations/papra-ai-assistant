"""The record of documents refused for size, and the three bounds it lives inside.

An oversized document has **no ledger row**: it is refused before enrollment, so there is no
reference to hang the reason on and nothing for `check` to count. The record in this file is that
missing row — a settings document, written where the skip happens.

Three failures it exists to prevent, one per test group below:

* **The unbounded row.** A corpus with 400 scans in it must not grow one settings value for as long
  as the process lives, so the listing is capped and the oldest entries leave first. The count is
  kept beside the listing, not derived from it, because a number read out of a capped list can only
  ever shorten — which is the confident wrong answer this project keeps designing against.
* **The stale reason.** A document that comes back under the limit (compressed and re-uploaded, the
  operator's remedy) is enrolled, and enrollment drops it out of the record. If it did not, the
  reason would outlive the problem: `check` would go on saying "3 oversized" over a corpus with
  nothing oversized in it, which is worse than saying nothing.
* **The label.** "N oversized seen since <pass>", never "your corpus has N": the record holds what
  the passes happened to walk. It is not a census and not a lower bound either — `count` tallies
  refusal *observations*, and the tests below pin exactly how far that claim reaches.
"""

import pytest

from papra_suite.oversized import (
    OVERSIZED_RECORD_LIMIT,
    OVERSIZED_SETTING,
    clear_oversized_skip,
    oversized_seen,
    record_oversized_skip,
)
from papra_suite.pipeline import enqueue_index_job
from papra_suite.store import Store

ORG = "org_1"
LIMIT = 100


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


def skip(store, document_id, *, size=150, org=ORG, limit=LIMIT, now="2026-11-19T10:00:00+00:00"):
    record_oversized_skip(store, papra_org_id=org, document_id=document_id, size=size,
                          limit=limit, now=now)


# --- what the record says -----------------------------------------------------------


def test_an_empty_record_says_so_without_inventing_a_limit(store):
    record = oversized_seen(store)

    assert record.count == 0
    assert record.documents == ()
    assert record.since is None
    # No skip has ever been recorded, so no ceiling has been applied to anything this store saw.
    # Zero here would read as "a limit of zero bytes refused them all".
    assert record.limit is None
    assert store.get_setting(OVERSIZED_SETTING) is None


def test_a_skip_is_recorded_with_its_size_and_the_limit_that_refused_it(store):
    skip(store, "doc_big", size=157286400, limit=104857600)

    record = oversized_seen(store)
    assert record.count == 1
    assert record.largest == 157286400
    assert record.limit == 104857600
    assert record.since == "2026-11-19T10:00:00+00:00"
    assert [(d.org_id, d.document_id, d.size) for d in record.documents] == [
        (ORG, "doc_big", 157286400)]


def test_a_repeat_skip_of_the_same_document_is_not_counted_twice(store):
    """The same scan walked by a sweep, a backfill and a reconcile is one oversized document.

    Counting it three times would inflate the only number `check` prints about it, and the sizes
    would be three copies of the same claim about one body.
    """
    skip(store, "doc_big", size=150)
    skip(store, "doc_big", size=160, now="2026-11-19T11:00:00+00:00")

    record = oversized_seen(store)
    assert record.count == 1
    assert len(record.documents) == 1
    assert record.documents[0].size == 160
    assert record.largest == 160


def test_the_largest_size_seen_survives_a_smaller_skip_after_it(store):
    skip(store, "doc_big", size=400)
    skip(store, "doc_smallish", size=200)

    assert oversized_seen(store).largest == 400


def test_a_different_limit_starts_the_record_again(store):
    """Every entry in the record was refused by the limit the record names.

    Entries refused under 100 MiB printed beside a limit of 500 MiB read as a contradiction —
    "150 MB is oversized at 500 MB?" — and it is a real contradiction the moment an operator
    raises the knob. So raising it starts a new record: `since`, `count` and `largest` describe
    the ceiling
    that is in force, and the old ones go with the ceiling that produced them.
    """
    skip(store, "doc_big", size=400, limit=100)
    skip(store, "doc_bigger", size=700, limit=500, now="2026-11-20T09:00:00+00:00")

    record = oversized_seen(store)
    assert record.limit == 500
    assert record.count == 1
    assert record.largest == 700
    assert record.since == "2026-11-20T09:00:00+00:00"
    assert [d.document_id for d in record.documents] == ["doc_bigger"]


# --- the bounds ---------------------------------------------------------------------


def test_the_listing_is_capped_and_evicts_the_oldest_first(store):
    """400 refusals must not become 400 rows in one settings value.

    The cap bounds the row; the count keeps telling the truth about the 400. Which entries are
    listed is the least important fact here, so the oldest give way first: the ones a pass recorded
    minutes ago are the ones an operator is reading `check` about now.
    """
    total = OVERSIZED_RECORD_LIMIT + 10
    for index in range(total):
        skip(store, f"doc-{index:03d}", size=200 + index)

    record = oversized_seen(store)
    assert len(record.documents) == OVERSIZED_RECORD_LIMIT
    assert record.count == total
    assert [d.document_id for d in record.documents] == [
        f"doc-{index:03d}" for index in range(10, total)]
    assert record.evicted == 10
    # The row itself stays bounded, not merely the list this function rebuilt.
    assert len(store.get_setting(OVERSIZED_SETTING)) < 20_000


def test_a_second_walk_over_evicted_documents_counts_observations_not_documents(store):
    """What the cap really costs, stated as behaviour rather than as a promise.

    A document that has left the listing cannot be recognised when a later pass walks it again, so
    it is counted again — and when it is finally enrolled, there is no entry to remove, so the
    number does not fall. Both halves are deliberate: the alternative is a counter that quietly
    under-reports, or a second unbounded set of ids kept to remember what the capped one forgot.
    This is why every surface says "N oversized **seen** since <pass>" and never "there are N":
    `count` is a tally of refusal observations, and a reader who takes it for a distinct-document
    count is being lied to by the label,
    not by the number.
    """
    total = OVERSIZED_RECORD_LIMIT + 10
    for index in range(total):
        skip(store, f"doc-{index:03d}", size=200)
    evicted = "doc-000"  # pushed out of the listing by the 10 records after the cap
    assert evicted not in [d.document_id for d in oversized_seen(store).documents]

    # A second pass walks the head of the corpus again: the same document, refused again.
    skip(store, evicted, size=200)
    record = oversized_seen(store)
    assert record.count == total + 1, "a re-walked refusal is a new observation, and is counted"
    assert len(record.documents) == OVERSIZED_RECORD_LIMIT

    # Enrolling a document that is still listed removes it; the number comes down by one.
    listed = oversized_seen(store).documents[-1].document_id
    assert clear_oversized_skip(store, papra_org_id=ORG, document_id=listed) is True
    assert oversized_seen(store).count == total

    # Enrolling the evicted one changes nothing, and says so: no entry, no decrement, False.
    assert clear_oversized_skip(store, papra_org_id=ORG, document_id=evicted) is False
    assert oversized_seen(store).count == total


def test_a_row_that_moved_while_clearing_is_reported_as_not_cleared(store):
    """The fence is only worth having if losing it is reported as losing it.

    The last entry leaving deletes the row with `expected=`, because a skip recorded between the
    read and the delete is a document that is *still* oversized. `clear_setting` answers False
    when the row moved. Returning True there would tell the caller its enrollment left nothing
    behind while a live
    refusal sat in the row — so the False is propagated, and the row survives intact.
    """
    skip(store, "doc-scan", size=157286400)
    raw = store.get_setting(OVERSIZED_SETTING)

    class ContendedStore:
        """The store as seen by a writer that lost the row to somebody else mid-call."""

        def get_setting(self, key):
            return store.get_setting(key)

        def set_setting(self, key, value):
            store.set_setting(key, value)

        def clear_setting(self, key, *, expected):
            return False  # the row moved: another pass recorded a refusal

    assert clear_oversized_skip(ContendedStore(), papra_org_id=ORG, document_id="doc-scan") is False
    assert store.get_setting(OVERSIZED_SETTING) == raw, "the live refusal is still recorded"


def test_the_count_outlives_the_entries_it_evicted(store):
    skip(store, "doc-big-1", size=157286400)
    for index in range(OVERSIZED_RECORD_LIMIT):
        skip(store, f"doc-{index}", size=200)

    record = oversized_seen(store)
    assert record.count == OVERSIZED_RECORD_LIMIT + 1
    assert "doc-big-1" not in [d.document_id for d in record.documents]
    # Largest is a running figure over every skip, so the evicted giant is still the biggest thing
    # this store refused, and `check`'s "largest size seen" would be a lie without it.
    assert record.largest == 157286400


def test_a_hostile_document_id_cannot_grow_the_row(store):
    """A listing is Papra's text. It is bounded before it is stored, like every other stored field.

    The sanitizer is the house one (`store.sanitize_event_text`): control characters become spaces
    so one entry stays one field, and the length bound is what makes "capped at N entries" a bound
    on bytes rather than a bound on a server's imagination.
    """
    skip(store, "doc\nwith|separators," + "x" * 500, size=200)

    record = oversized_seen(store)
    stored = store.get_setting(OVERSIZED_SETTING)
    assert len(record.documents) == 1
    assert len(record.documents[0].document_id) <= 128
    assert "\n" not in record.documents[0].document_id
    assert "\n" not in stored
    # And the row still parses: a sanitizer that broke the encoding would read as a corrupt record.
    assert oversized_seen(store).count == 1


def test_a_corrupt_row_is_no_record_and_a_warning_not_a_crash(store, caplog):
    """`check` and `index_status` read this on a path that must not raise over a bad row.

    It is a report, not a schedule or a gate: nothing downstream decides anything with it, so the
    honest reading of a row this program cannot parse is "I have no record", printed as one.
    """
    store.set_setting(OVERSIZED_SETTING, "not json at all")

    with caplog.at_level("WARNING"):
        record = oversized_seen(store)

    assert record.count == 0 and record.limit is None and record.documents == ()
    assert OVERSIZED_SETTING in caplog.text


@pytest.mark.parametrize("value", [
    '{"since": 5, "limit": 100, "largest": 1, "count": 1, "docs": []}',
    '{"since": "x", "limit": "y", "largest": 1, "count": 1, "docs": []}',
    '{"since": "x", "limit": 100, "largest": 1, "count": 1, "docs": {"a": 1}}',
    '{"since": "x", "limit": 100, "largest": 1, "count": 1, "docs": [["o", 7, "big"]]}',
    '{"count": 1}',
])
def test_a_row_that_is_shaped_wrong_reads_as_no_record(store, value):
    store.set_setting(OVERSIZED_SETTING, value)

    assert oversized_seen(store).count == 0


# --- the stale reason: enrollment drops the entry out -------------------------------


def test_enrolling_a_document_drops_it_out_of_the_record(store):
    """The failure mode this pins is the reason outliving the problem.

    The operator's remedy for a 400 MB scan is to compress it and re-upload to Papra. The next pass
    enrols it, and from that moment the document is indexed: a record that kept naming it would
    leave `check` reporting an oversized document that no longer exists at that size, forever, in
    the one place the operator looks to find out whether the remedy worked.
    """
    skip(store, "doc_big", size=400)
    skip(store, "doc_other", size=500)

    assert clear_oversized_skip(store, papra_org_id=ORG, document_id="doc_big") is True

    record = oversized_seen(store)
    assert record.count == 1
    assert [d.document_id for d in record.documents] == ["doc_other"]


def test_the_record_disappears_when_the_last_document_is_enrolled(store):
    skip(store, "doc_big", size=400)

    assert clear_oversized_skip(store, papra_org_id=ORG, document_id="doc_big") is True

    assert oversized_seen(store).count == 0
    assert store.get_setting(OVERSIZED_SETTING) is None, \
        "an emptied record must not leave a stale `since` behind"


def test_clearing_a_document_that_is_not_recorded_writes_nothing(store):
    skip(store, "doc_other", size=400)
    before = store.get_setting(OVERSIZED_SETTING)

    assert clear_oversized_skip(store, papra_org_id=ORG, document_id="doc_never_seen") is False
    assert store.get_setting(OVERSIZED_SETTING) == before


def test_the_wrong_bucket_does_not_clear_another_bucket_s_document(store):
    skip(store, "doc_big", size=400, org="org_1")

    assert clear_oversized_skip(store, papra_org_id="org_2", document_id="doc_big") is False
    assert oversized_seen(store).count == 1


def test_the_intake_path_drops_the_entry_when_it_enrols_the_document(store):
    """`enqueue_index_job` is the enrollment every path shares, so it is where the entry must go.

    Sweep, backfill, reconcile, restore and mail all enrol through it. Clearing from the reporting
    side instead — filtering the record against the ledger at read time — would work for the paths
    that have a ledger row and be silently wrong for none of them... except it would not: the
    record's whole point is documents with no ledger row, so a read-time join needs state the
    record is the only holder of. Write-time removal is the version that can see the enrollment.
    """
    skip(store, "doc_big", size=400)

    enqueue_index_job(store, papra_org_id=ORG, document_id="doc_big", file_hash="a" * 64,
                      name="scan.pdf", mime="application/pdf", ingested_via="backfill")

    assert oversized_seen(store).count == 0
    assert store.get_setting(OVERSIZED_SETTING) is None


def test_a_non_pdf_enrollment_leaves_the_record_alone(store):
    """The gate that skipped the document is the pdf gate and the size gate; only the second
    writes this record, and a non-pdf enqueue has never been in it."""
    skip(store, "doc_big", size=400)

    enqueue_index_job(store, papra_org_id=ORG, document_id="doc_big", file_hash="a" * 64,
                      name="scan.txt", mime="text/plain", ingested_via="backfill")

    assert oversized_seen(store).count == 1


def test_the_record_is_written_with_the_same_now_the_store_writes_everywhere_else(store):
    """`since` is an ISO-8601 UTC string like every other stored timestamp, not a float."""
    skip(store, "doc_big", size=400, now=None)

    record = oversized_seen(store)
    assert record.since is not None and record.since.endswith("+00:00")
    from datetime import datetime
    assert datetime.fromisoformat(record.since).year >= 2026


def test_the_record_can_be_read_off_a_read_only_view(tmp_path, store):
    """`index_status` reads this through `LedgerReader`, which exposes `get_setting` and nothing
    else.

    That is the containment rule for the request path: new ledger data goes onto the reader's
    method set, and a settings row read with the read it already has needs no new door at all.
    """
    from papra_suite.readers import LedgerReader

    skip(store, "doc_big", size=400)
    view = LedgerReader(store.db_path)
    try:
        assert oversized_seen(view).count == 1
    finally:
        view.close()
