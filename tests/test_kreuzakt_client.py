"""`KreuzaktClient` against captured payloads — and against bodies no server sent.

Three kinds of body appear here, and which kind a test uses decides what its verdict is
evidence of:

1. **Captured.** Every *well-formed* body is a file under `tests/fixtures/` that a probe
   wrote from a live container
   (`docs/superpowers/probes/2026-11-17-kreuzakt-contracts/`):
   `kreuzakt_queue_page.json`, `kreuzakt_document.json` from Task 3's `probe_queue.py`;
   `kreuzakt_search_page.json`, `kreuzakt_search_empty.json`,
   `kreuzakt_documents_page.json`, `kreuzakt_upload_response.json` from Task 5's
   `probe_search.py`. A test that passes on one of these says something about Kreuzakt.
2. **Derived.** The fail-loud tests take a captured payload and remove or null one key
   (`without(...)`, `patched(...)`). The surrounding payload is real; the defect is
   invented, because no probe could make the server drop a column. Their verdict is about
   this client's behaviour under a surprise we have not observed.
3. **Hand-built.** `queue_row(...)` rows, the `failedCount: 1` upload rejection, and the
   non-object/HTML bodies are assembled from captured *field names* only. They pin policy,
   not server behaviour, and each says so where it matters.

The parameters the client refuses to send are asserted as deliberately as the ones it
sends: `/api/queue` ignores `page`/`offset` entirely (walking it re-reads one page
forever), and `/api/search` ignores `offset` while honouring `page`. A parameter the
server ignores must not appear in the interface, because a caller would believe it
narrowed something.
"""

import json
from pathlib import Path

import httpx
import pytest

from papra_suite.kreuzakt_client import (
    AmbiguousCorrelation,
    CorrelationWindowFull,
    KreuzaktClient,
    KreuzaktError,
    QueueEntry,
)

FIXTURES = Path(__file__).parent / "fixtures"
UPLOAD_NAME = "papra-search0001-11111111.pdf"


def load(name):  # tests/fixtures written by the probes — no hand-guessed payloads
    return json.loads((FIXTURES / name).read_text())


def make_client(responses):
    """responses: list of (path_fragment, (status, json_body)) served by first match.

    Each call is recorded as `(method, url, request_body)`, so a test can assert on the
    query string the client actually built and on the multipart field name it used.
    """
    calls = []

    def handler(request):
        calls.append((request.method, str(request.url), request.content))
        for fragment, (status, body) in responses:
            if fragment in str(request.url):
                return httpx.Response(status, json=body)
        return httpx.Response(404, json={"error": "unhandled", "url": str(request.url)})

    return KreuzaktClient("http://kreuzakt:3000",
                          transport=httpx.MockTransport(handler)), calls


def ok(body):
    return 200, body


def queue_body(rows, counts=None):
    """The envelope `/api/queue` really returns: entries plus a counts block."""
    return {
        "entries": rows,
        "counts": counts or {"pending": 0, "processing": 0, "completed": len(rows),
                             "failed": 0},
    }


def queue_row(id, filename=UPLOAD_NAME, status="completed", document_id=1, error=None,
              page_count=1):
    return {
        "id": id,
        "filename": filename,
        "status": status,
        "error": error,
        "document_id": document_id,
        "created_at": "2026-11-17 22:28:04",
        "completed_at": "2026-11-17T22:28:06.067Z",
        "page_count": page_count,
    }


# ------------------------------------------------------------------ upload (Q1, S3)


async def test_upload_returns_relative_path_and_sends_files_field():
    client, calls = make_client([("/api/upload", ok(load("kreuzakt_upload_response.json")))])
    assert await client.upload(UPLOAD_NAME, b"%PDF") == UPLOAD_NAME
    assert calls[0][1].endswith("/api/upload")
    upload_bodies = [body for _, _, body in calls]
    assert b'name="files"' in upload_bodies[0]        # the handler records request bodies
    assert b'filename="' + UPLOAD_NAME.encode() + b'"' in upload_bodies[0]
    await client.aclose()


async def test_upload_returns_the_stored_name_not_the_name_we_sent():
    """Correlation matches on the response's `relativePath`; the name we constructed
    can be sanitised away upstream, so the client must hand back the server's value."""
    body = load("kreuzakt_upload_response.json")
    client, _ = make_client([("/api/upload", ok(body))])
    assert await client.upload("papra:d1<hash>.pdf", b"%PDF") == UPLOAD_NAME
    await client.aclose()


async def test_upload_reports_the_upstream_refusal_verbatim_with_its_status():
    """Measured 400 body: 'Upload requests must include at least one file'."""
    client, _ = make_client([("/api/upload", (400, load("kreuzakt_upload_rejected.json")))])
    with pytest.raises(KreuzaktError) as excinfo:
        await client.upload(UPLOAD_NAME, b"%PDF")
    assert excinfo.value.status == 400
    assert "Upload requests must include at least one file" in str(excinfo.value)
    await client.aclose()


async def test_upload_fails_closed_when_no_file_was_accepted():
    """`failedCount` set / `files` empty is a rejection, not an empty success: a silent
    `None` here would leave the job to correlate against a row that will never exist.

    NOT CAPTURED EVIDENCE: the body below is hand-built from the field names of
    `kreuzakt_upload_response.json` (`files`, `uploadedCount`, `failedCount`, `errors`).
    The probe captured a *400* rejection (`kreuzakt_upload_rejected.json`) but never a
    200 with `failedCount: 1`, so this pins the client's policy for an inferred shape —
    it is not an observation of what Kreuzakt answers. The field names are the only
    measured part; do not cite this test as server behaviour.
    """
    client, _ = make_client([("/api/upload", ok({
        "files": [], "uploadedCount": 0, "failedCount": 1,
        "errors": ["file is not a PDF"],
    }))])
    with pytest.raises(KreuzaktError, match="upload rejected"):
        await client.upload(UPLOAD_NAME, b"%PDF")
    await client.aclose()


# ------------------------------------------------------- queue pages (Q4, Q6)


async def test_queue_page_parses_the_captured_envelope():
    page = load("kreuzakt_queue_page.json")
    client, calls = make_client([("/api/queue", ok(page))])
    rows = await client.queue_page()
    assert len(rows) == len(page["entries"])
    assert isinstance(rows[0], QueueEntry)
    assert rows[0].id == 35 and rows[0].status == "failed"
    assert rows[0].document_id is None
    assert "Invalid cross-reference table" in rows[0].error
    assert rows[0].page_count is None
    assert rows[-1].id == 1 and rows[-1].document_id == 1
    # `limit` is the only paging knob the endpoint has, and it must be sent explicitly:
    # the server default is 20, and correlation reads a 100-row window.
    assert "limit=100" in calls[0][1]
    # Measured: `page`, `offset`, `perPage` and `cursor` are silently ignored.
    assert "offset" not in calls[0][1] and "page=" not in calls[0][1]
    await client.aclose()


async def test_queue_page_sends_status_only_when_asked():
    client, calls = make_client([("/api/queue", ok(queue_body([])))])
    await client.queue_page(status="failed")
    await client.queue_page()
    assert "status=failed" in calls[0][1]
    assert "status=" not in calls[1][1]
    await client.aclose()


async def test_queue_counts_come_from_the_same_captured_envelope():
    page = load("kreuzakt_queue_page.json")
    client, _ = make_client([("/api/queue", ok(page))])
    assert await client.queue_counts() == page["counts"]
    await client.aclose()


async def test_queue_page_refuses_to_read_an_unexpected_body_as_an_empty_queue():
    """A proxy error page or a changed envelope must not look like 'nothing in the
    queue': that would make the high-water mark 0 and every upload uncorrelatable."""
    client, _ = make_client([("/api/queue", ok({"detail": "upstream unavailable"}))])
    with pytest.raises(KreuzaktError, match="unexpected /api/queue"):
        await client.queue_page()
    await client.aclose()


# ----------------------------------------------- high-water mark (Q4, Q10)


async def test_high_water_mark_is_the_first_row_of_a_one_row_request():
    """Measured: ordering is descending, ids ascend, and there is no pagination to walk.
    One request is the whole algorithm; asserting the query string here is what keeps a
    future 'optimisation' from re-introducing a page walk that cannot terminate."""
    client, calls = make_client(
        [("/api/queue", ok(queue_body([queue_row(155, "x.pdf", "pending", None)])))])
    assert await client.queue_high_water_mark() == 155
    # `limit=1` exactly, not merely a URL that contains it: `limit=100` would satisfy a
    # substring check while re-reading the whole window.
    assert calls[0][1].endswith("/api/queue?limit=1")
    assert len(calls) == 1
    await client.aclose()


async def test_high_water_mark_on_an_empty_queue_is_zero():
    client, _ = make_client([("/api/queue", ok(queue_body([])))])
    assert await client.queue_high_water_mark() == 0
    await client.aclose()


async def test_high_water_mark_ignores_the_rest_of_the_page():
    """`limit=1` asks for one row; anything the server sends beyond it is not consulted,
    because only `entries[0]` is the newest row (measured ordering, id DESC)."""
    client, _ = make_client([("/api/queue", ok(queue_body(
        [queue_row(9, "a.pdf"), queue_row(400, "b.pdf")])))])
    assert await client.queue_high_water_mark() == 9
    await client.aclose()


# ------------------------------------------------ correlation (Q2, Q3, Q4, Q6)


async def test_a_full_window_is_reported_as_saturated_not_as_absent():
    """100 rows newer than hwm, none of them ours: the window proves nothing, so this must
    be retryable. Returning None here would make the job 'succeed' by silently skipping."""
    rows = [queue_row(1000 + i, f"other{i}.pdf") for i in range(100)]
    client, _ = make_client([("/api/queue", ok(queue_body(rows)))])
    with pytest.raises(CorrelationWindowFull):
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=900,
                                        poll_s=0, timeout_s=0.1)
    await client.aclose()


async def test_a_full_window_of_rows_below_the_mark_is_not_saturation():
    """Rows the mark already excludes cannot crowd us out; that case is 'not yet'."""
    rows = [queue_row(i, f"other{i}.pdf") for i in range(1, 101)]
    client, _ = make_client([("/api/queue", ok(queue_body(rows)))])
    with pytest.raises(KreuzaktError, match="no queue row"):
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=500,
                                        poll_s=0, timeout_s=0.05)
    await client.aclose()


async def test_collision_suffix_matches_and_two_candidates_are_ambiguous():
    ok_client = make_client([("/api/queue", ok(queue_body(
        [queue_row(12, "papra-d1-a1b2 (1).pdf", document_id=9)])))])[0]
    assert (await ok_client.wait_for_queue_row("papra-d1-a1b2.pdf", after_id=11,
                                               poll_s=0, timeout_s=0.1)).document_id == 9
    await ok_client.aclose()
    dup = make_client([("/api/queue", ok(queue_body([
        queue_row(13, "papra-d1-a1b2.pdf", document_id=9),
        queue_row(12, "papra-d1-a1b2.pdf", document_id=9)])))])[0]
    with pytest.raises(AmbiguousCorrelation):
        await dup.wait_for_queue_row("papra-d1-a1b2.pdf", after_id=11, poll_s=0, timeout_s=0.1)
    await dup.aclose()


async def test_requires_exact_filename_and_greater_id():
    """Suffix matching and 'newest row' are both guesses; neither is used. The decoy is
    the captured `reindex-all:...:papra-doc0004-4d5e6f70.pdf` row, which ends with the
    name we uploaded but is not ours."""
    page = load("kreuzakt_queue_page.json")
    client, _ = make_client([("/api/queue", ok(page))])
    entry = await client.wait_for_queue_row("papra-doc0004-4d5e6f70.pdf", after_id=0,
                                            poll_s=0, timeout_s=1)
    assert entry.id == 4 and entry.document_id == 4
    await client.aclose()


async def test_a_reindex_row_is_never_a_match_even_at_the_top_of_the_window():
    """The reindex rows carry our filename as a suffix; matching them would attribute a
    re-extraction to an upload that never happened."""
    page = load("kreuzakt_queue_page.json")
    client, _ = make_client([("/api/queue", ok(page))])
    with pytest.raises(KreuzaktError, match="no queue row"):
        await client.wait_for_queue_row("papra-doc0004-4d5e6f70.pdf", after_id=18,
                                        poll_s=0, timeout_s=0.05)
    await client.aclose()


async def test_wait_for_queue_row_ignores_a_stale_row_below_the_mark():
    client, _ = make_client([("/api/queue", ok(queue_body([
        queue_row(50, document_id=3),                       # an earlier attempt
    ])))])
    with pytest.raises(KreuzaktError, match="no queue row"):
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=101,
                                        poll_s=0, timeout_s=0.05)
    await client.aclose()


async def test_wait_for_queue_row_times_out_loudly_not_as_none():
    client, _ = make_client([("/api/queue", ok(queue_body([])))])
    with pytest.raises(KreuzaktError, match="no queue row"):
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=1,
                                        poll_s=0, timeout_s=0.05)
    await client.aclose()


async def test_a_pending_row_is_waited_for_rather_than_returned():
    """`status == "completed"` is the only completion signal: `reindex-all` rows are
    created with `document_id` already set, so a non-null id proves nothing. The timeout
    text says `no terminal queue row`, not `no queue row`: the row exists, it is the
    answer that does not exist yet (see
    `test_the_timeout_diagnostic_says_which_row_is_stuck_instead_of_denying_it_exists`).
    """
    client, calls = make_client([("/api/queue", ok(queue_body(
        [queue_row(102, status="pending", document_id=None)])))])
    with pytest.raises(KreuzaktError, match="no terminal queue row"):
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=101,
                                        poll_s=0, timeout_s=0.05)
    assert len(calls) > 1                       # it kept polling, it did not give up early
    await client.aclose()


async def test_the_row_is_found_on_a_later_poll():
    """The poll loop must re-read the window, not cache the first answer."""
    empty = queue_body([])
    later = queue_body([queue_row(102, document_id=7)])
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(200, json=later if len(seen) > 1 else empty)

    client = KreuzaktClient("http://kreuzakt:3000", transport=httpx.MockTransport(handler))
    entry = await client.wait_for_queue_row(UPLOAD_NAME, after_id=101,
                                            poll_s=0.01, timeout_s=1)
    assert entry.id == 102 and entry.document_id == 7
    assert len(seen) == 2
    await client.aclose()


async def test_two_terminal_rows_for_one_upload_are_ambiguous_not_resolved():
    client, _ = make_client([("/api/queue", ok(queue_body([
        queue_row(102, document_id=7),
        queue_row(103, status="failed", document_id=9, error="boom"),
    ])))])
    with pytest.raises(KreuzaktError, match="ambiguous"):
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=101,
                                        poll_s=0, timeout_s=5)
    await client.aclose()


async def test_two_candidates_one_of_them_terminal_is_that_one_row():
    """A pending twin is not a second terminal row: the terminal row is the answer."""
    client, _ = make_client([("/api/queue", ok(queue_body([
        queue_row(103, status="pending", document_id=None),
        queue_row(102, document_id=7),
    ])))])
    entry = await client.wait_for_queue_row(UPLOAD_NAME, after_id=101,
                                            poll_s=0, timeout_s=5)
    assert entry.id == 102 and entry.document_id == 7
    await client.aclose()


async def test_completed_row_with_null_document_id_is_an_error_not_a_silent_none():
    """Task 3 question 9 decides whether this can happen; the client refuses anyway."""
    client, _ = make_client([("/api/queue", ok(queue_body([
        queue_row(102, document_id=None),
    ])))])
    with pytest.raises(KreuzaktError, match="without document_id"):
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=101,
                                        poll_s=0, timeout_s=5)
    await client.aclose()


async def test_a_failed_row_is_returned_so_the_job_can_read_its_error():
    """Failure is an answer, not a lookup miss: the job needs Kreuzakt's own error text
    (measured: a corrupt PDF re-fails with identical text every time)."""
    client, _ = make_client([("/api/queue", ok(queue_body([
        queue_row(102, status="failed", document_id=None,
                  error=load("kreuzakt_queue_page.json")["entries"][0]["error"]),
    ])))])
    entry = await client.wait_for_queue_row(UPLOAD_NAME, after_id=101,
                                            poll_s=0, timeout_s=5)
    assert entry.status == "failed" and entry.document_id is None
    assert "Parsing error" in entry.error
    await client.aclose()


# ---------------------------------------------------------------- search (S1-S7)


async def test_search_returns_hits_and_the_total_from_the_captured_page():
    client, calls = make_client([("/api/search", ok(load("kreuzakt_search_page.json")))])
    hits, total = await client.search("Ziphoid")
    assert total == 1 and len(hits) == 1
    assert hits[0].id == 1 and hits[0].original_filename == UPLOAD_NAME
    assert "/api/search" in calls[0][1] and "q=Ziphoid" in calls[0][1]
    await client.aclose()


async def test_search_clamps_limit_to_100_and_never_exposes_content_or_score():
    client, calls = make_client([("/api/search", ok(load("kreuzakt_search_page.json")))])
    hits, total = await client.search("rechnung", limit=500)
    assert "limit=100" in calls[-1][1]
    assert total == 1 and len(hits) == 1
    assert {f for f in vars(hits[0])} == {"id", "title", "document_date", "added_at",
                                          "mime_type", "original_filename"}
    await client.aclose()


async def test_search_never_sends_offset_because_the_server_ignores_it():
    """Measured: `offset=1` and `offset=2` each returned the newest page unchanged, while
    `page=2` returned the second row. Exposing `offset` would lie about narrowing."""
    client, calls = make_client([("/api/search", ok(load("kreuzakt_search_page.json")))])
    await client.search("Rechnung", page=2)
    await client.search("Rechnung")
    assert "page=2" in calls[0][1]
    assert all("offset" not in url for _, url, _ in calls)
    assert "page=1" in calls[1][1]
    await client.aclose()


async def test_search_empty_page_is_zero_hits_not_an_error():
    client, _ = make_client([("/api/search", ok(load("kreuzakt_search_empty.json")))])
    hits, total = await client.search("zzqxvbnmt-not-a-word-anywhere")
    assert hits == [] and total == 0
    await client.aclose()


async def test_search_refuses_to_read_an_unexpected_body_as_no_hits():
    client, _ = make_client([("/api/search", ok({"detail": "gateway timeout"}))])
    with pytest.raises(KreuzaktError, match="unexpected /api/search"):
        await client.search("Rechnung")
    await client.aclose()


async def test_search_clamps_a_non_positive_limit_rather_than_being_rejected():
    """Measured: `limit=0` is a 400 'Pagination parameters must be positive integers'."""
    client, calls = make_client([("/api/search", ok(load("kreuzakt_search_empty.json")))])
    await client.search("Rechnung", limit=0)
    # Parsed, not substring-matched: `"limit=1" in url` is satisfied by `limit=100` too,
    # which is the same flaw this file criticises in the plan's own sketch.
    assert httpx.URL(calls[0][1]).params.get("limit") == "1"
    await client.aclose()


# --------------------------------------------------------- documents (D1-D4)


async def test_get_document_has_no_field_holding_a_kreuzakt_path():
    client, _ = make_client([("/api/documents/15", ok(load("kreuzakt_document.json")))])
    doc = await client.get_document(15)
    assert not any("/documents/" in str(v) for v in vars(doc).values())
    await client.aclose()


async def test_get_document_keeps_only_the_fields_the_bridge_may_write_back():
    document = load("kreuzakt_document.json")
    client, _ = make_client([("/api/documents/15", ok(document))])
    doc = await client.get_document(15)
    assert {f for f in vars(doc)} == {"id", "title", "description", "content",
                                      "document_date", "original_filename", "updated_at"}
    assert doc.id == 15 and doc.title == "Papra Seq0004 9e9e9e9e"
    assert doc.updated_at == "2026-11-17 20:02:42"
    assert "Rechnung Nr. 4711" in doc.content
    await client.aclose()


async def test_list_documents_returns_the_captured_rows_in_the_servers_order():
    page = load("kreuzakt_documents_page.json")
    client, calls = make_client([("/api/documents", ok(page))])
    rows = await client.list_documents()
    assert [row["id"] for row in rows] == [4, 3, 2, 1]
    assert "content" not in rows[0]              # the list row never carried content
    assert "limit=100" in calls[0][1]
    assert "offset" not in calls[0][1]           # measured ignored, like search's
    await client.aclose()


async def test_list_documents_sends_since_in_the_form_the_server_filters_on():
    """Measured: `since=2026-11-17 22:28:05` filtered, and `since=2030-01-01T00:00:00Z`
    returned nothing at all — the ISO form silently matches nothing, so it must not be
    sent as if it worked."""
    page = load("kreuzakt_documents_page.json")
    client, calls = make_client([("/api/documents", ok(page))])
    rows = await client.list_documents(since="2026-11-17 20:00:00")
    assert len(rows) == 4
    assert "since=" in calls[0][1] and "2026-11-17" in calls[0][1]
    await client.aclose()


async def test_list_documents_refuses_an_iso_8601_since_that_silently_matches_nothing():
    page = load("kreuzakt_documents_page.json")
    client, _ = make_client([("/api/documents", ok(page))])
    with pytest.raises(KreuzaktError, match="since"):
        await client.list_documents(since="2026-11-17T22:28:05Z")
    await client.aclose()


async def test_list_documents_refuses_to_read_an_unexpected_body_as_no_documents():
    client, _ = make_client([("/api/documents", ok({"detail": "bad gateway"}))])
    with pytest.raises(KreuzaktError, match="unexpected /api/documents"):
        await client.list_documents()
    await client.aclose()


# ------------------------------------------------ delete / retry / health / errors


async def test_delete_document_accepts_204_and_treats_404_as_already_gone():
    gone, _ = make_client([("/api/documents/9", (404, load("kreuzakt_retry_unknown.json")))])
    assert await gone.delete_document(9) is None
    await gone.aclose()

    calls = []

    def handler(request):
        calls.append((request.method, str(request.url)))
        return httpx.Response(204)

    client = KreuzaktClient("http://kreuzakt:3000", transport=httpx.MockTransport(handler))
    assert await client.delete_document(9) is None
    assert calls == [("DELETE", "http://kreuzakt:3000/api/documents/9")]
    await client.aclose()


async def test_delete_document_raises_on_any_other_status():
    client, _ = make_client([("/api/documents/9", (500, {"error": "boom"}))])
    with pytest.raises(KreuzaktError) as excinfo:
        await client.delete_document(9)
    assert excinfo.value.status == 500
    await client.aclose()


async def test_retry_queue_keeps_the_upstream_400_verbatim():
    """Measured: retrying a `completed` row is 400,
    'Queue entry 31 is not in failed status' — not a 404, and not success."""
    client, calls = make_client(
        [("/retry", (400, load("kreuzakt_retry_not_failed.json")))])
    with pytest.raises(KreuzaktError) as excinfo:
        await client.retry_queue(31)
    assert excinfo.value.status == 400
    assert "Queue entry 31 is not in failed status" in str(excinfo.value)
    assert calls[0][0] == "POST" and calls[0][1].endswith("/api/queue/31/retry")
    await client.aclose()


async def test_retry_queue_keeps_the_upstream_404_distinct_from_the_400():
    client, _ = make_client([("/retry", (404, load("kreuzakt_retry_unknown.json")))])
    with pytest.raises(KreuzaktError) as excinfo:
        await client.retry_queue(999999)
    assert excinfo.value.status == 404
    assert "Queue entry 999999 not found" in str(excinfo.value)
    await client.aclose()


async def test_retry_queue_accepts_the_requeue_response():
    client, _ = make_client([("/retry", ok(load("kreuzakt_retry_requeued.json")))])
    assert await client.retry_queue(35) is None
    await client.aclose()


async def test_health_returns_the_captured_body():
    health = load("kreuzakt_health.json")
    client, calls = make_client([("/api/health", ok(health))])
    assert await client.health() == health
    assert calls[0][1].endswith("/api/health")
    await client.aclose()


async def test_non_2xx_raises_with_status_and_truncated_text():
    """PapraError truncates response text to 200 chars; copy that discipline."""
    long_text = "x" * 500
    client, _ = make_client([("/api/documents/1", (500, {"message": long_text}))])
    with pytest.raises(KreuzaktError) as excinfo:
        await client.get_document(1)
    message = str(excinfo.value)
    assert excinfo.value.status == 500
    assert "500" in message
    assert long_text not in message            # the full body never travels
    await client.aclose()


async def test_a_5xx_message_keeps_only_the_first_200_characters_of_the_body():
    marker = "MARKER"
    body_text = marker + "y" * 400
    client, _ = make_client([("/api/documents/1", (503, {"echo": body_text}))])
    with pytest.raises(KreuzaktError) as excinfo:
        await client.get_document(1)
    message = str(excinfo.value)
    assert marker in message
    assert "y" * 150 in message              # a long prefix is kept, diagnostically
    assert "y" * 400 not in message          # but the body is cut, at 200 chars
    await client.aclose()


async def test_a_transport_failure_raises_with_no_status():
    def handler(request):
        raise httpx.ConnectError("connection refused", request=request)

    client = KreuzaktClient("http://kreuzakt:3000", transport=httpx.MockTransport(handler))
    with pytest.raises(KreuzaktError) as excinfo:
        await client.health()
    assert excinfo.value.status is None
    assert "connection refused" in str(excinfo.value)
    await client.aclose()


async def test_the_client_exposes_neither_rescan_nor_reindex_all():
    """Both are unusable from here and one is actively misleading: `rescan` creates no
    queue row so it cannot be awaited, and `reindex-all` rows are created *with*
    `document_id` set, which would make every completion test pass for the wrong
    reason."""
    client, _ = make_client([])
    assert not hasattr(client, "rescan")
    assert not hasattr(client, "reindex_all")
    assert not hasattr(client, "rescan_document")
    await client.aclose()


async def test_list_documents_refuses_a_since_that_only_accidentally_matches():
    """`since=1700000000` returned every row (the string sorts before a
    `YYYY-MM-DD HH:MM:SS` stamp), which is 'no filter' wearing a filter. A value that
    only accidentally works is refused, like the ISO form."""
    page = load("kreuzakt_documents_page.json")
    client, calls = make_client([("/api/documents", ok(page))])
    for bogus in ("1700000000", "not-a-date", "2026-11-17 22:28", "2026-11-17 22:28:05.500"):
        with pytest.raises(KreuzaktError, match="since"):
            await client.list_documents(since=bogus)
    assert calls == []                     # nothing was sent, so nothing can look filtered
    await client.aclose()


async def test_list_documents_accepts_both_measured_since_forms():
    page = load("kreuzakt_documents_page.json")
    client, calls = make_client([("/api/documents", ok(page))])
    await client.list_documents(since="2026-11-17")
    await client.list_documents(since="2026-11-17 22:28:05")
    assert len(calls) == 2
    assert all("since=" in url for _, url, _ in calls)
    await client.aclose()


# --------------------------------------------------- fail-loud typing (review pass)
#
# This client is the only code that speaks Kreuzakt HTTP, and Task 6 is its first
# consumer. Every test in this section asks the same question of one call site: when the
# server surprises us, does the caller get an error it can classify, or a plausible
# value that will be written into an index? The second answer is always worse than a
# crash, because it is indistinguishable from success afterwards.


def without(body, *keys):
    """A copy of a captured body with `keys` removed — a server that stopped sending them."""
    return {key: value for key, value in body.items() if key not in keys}


def patched(body, **values):
    """A copy of a captured body with `values` overwritten (including set-to-null)."""
    return {**body, **values}


def text_client(body_text):
    """A client whose every 200 answers `body_text` — what an HTML proxy page looks like."""
    def handler(request):
        return httpx.Response(200, text=body_text,
                              headers={"content-type": "text/html"})

    return KreuzaktClient("http://kreuzakt:3000", transport=httpx.MockTransport(handler))


def json_client(payload):
    """A client that answers 200 with arbitrary JSON, for the bodies `make_client` cannot
    express because they are not objects."""
    def handler(request):
        return httpx.Response(200, json=payload)

    return KreuzaktClient("http://kreuzakt:3000", transport=httpx.MockTransport(handler))


# ---- 1. the fields the bridge writes back must not be guessed


async def test_get_document_refuses_a_body_without_content_rather_than_blanking_the_text():
    """`content` and `title` are written back into Papra. `body.get("content") or ""`
    turned a dropped key into an empty document, so the next write erases the user's
    text and nothing anywhere records that the server stopped answering in full."""
    body = without(load("kreuzakt_document.json"), "content")
    client, _ = make_client([("/api/documents/15", ok(body))])
    with pytest.raises(KreuzaktError) as excinfo:
        await client.get_document(15)
    assert "content" in str(excinfo.value)
    assert "title" in str(excinfo.value)          # the keys actually present are named
    await client.aclose()


async def test_get_document_refuses_an_absent_title_rather_than_writing_back_an_empty_one():
    body = without(load("kreuzakt_document.json"), "title")
    client, _ = make_client([("/api/documents/15", ok(body))])
    with pytest.raises(KreuzaktError, match="missing title"):
        await client.get_document(15)
    await client.aclose()


async def test_get_document_refuses_a_null_title_rather_than_writing_back_an_empty_one():
    """A null is the same blanking risk as an absent key, so it is refused the same way."""
    client, _ = make_client(
        [("/api/documents/15", ok(patched(load("kreuzakt_document.json"), title=None)))])
    with pytest.raises(KreuzaktError, match="title"):
        await client.get_document(15)
    await client.aclose()


async def test_get_document_refuses_a_null_content_even_though_the_key_is_present():
    """The `or ""` form could not tell `content` absent from `content` null from `content`
    empty; only the third one is something Kreuzakt actually said about the document."""
    client, _ = make_client(
        [("/api/documents/15", ok(patched(load("kreuzakt_document.json"), content=None)))])
    with pytest.raises(KreuzaktError, match="missing content"):
        await client.get_document(15)
    await client.aclose()


async def test_get_document_keeps_a_value_the_server_actually_sent_as_empty():
    """The requirement is about keys that are *gone*, not about empty strings: a document
    Kreuzakt itself reports as empty is empty, and refusing it would be its own lie."""
    client, _ = make_client([("/api/documents/15", ok(
        patched(load("kreuzakt_document.json"), content="", title="")))]
    )
    doc = await client.get_document(15)
    assert doc.content == "" and doc.title == ""
    await client.aclose()


async def test_search_refuses_a_hit_missing_a_field_the_row_type_declares():
    """`KzHit` declares these as `str`; `or ""` handed out a value the type says cannot
    happen. Task 13 sorts by `added_at`, so `""` reads as 'the oldest document there is'."""
    page = load("kreuzakt_search_page.json")
    for key in ("id", "title", "added_at", "mime_type", "original_filename"):
        broken = dict(page, results=[without(page["results"][0], key)])
        client, _ = make_client([("/api/search", ok(broken))])
        with pytest.raises(KreuzaktError, match=f"missing {key}"):
            await client.search("Rechnung")
        await client.aclose()


async def test_search_refuses_a_null_added_at_instead_of_typing_it_as_a_str():
    page = load("kreuzakt_search_page.json")
    broken = dict(page, results=[patched(page["results"][0], added_at=None)])
    client, _ = make_client([("/api/search", ok(broken))])
    with pytest.raises(KreuzaktError, match="added_at"):
        await client.search("Rechnung")
    await client.aclose()


# ---- 2. an unknown queue status is a fact we do not have


async def test_an_unknown_queue_status_fails_immediately_rather_than_being_polled():
    """A future `cancelled` row is not 'still pending'. Read as pending it would be
    polled for `index_wait_timeout_s` (900 s by default) and then reported as *absence* —
    the one outcome this design forbids. Measured set: pending, processing, completed,
    failed, and nothing else appeared in any capture."""
    client, calls = make_client([("/api/queue", ok(queue_body(
        [queue_row(102, status="cancelled", document_id=None)])))])
    with pytest.raises(KreuzaktError, match="unknown queue status") as excinfo:
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=101,
                                        poll_s=0, timeout_s=30)
    assert "cancelled" in str(excinfo.value)
    assert len(calls) == 1                    # refused on the first read, not polled out
    await client.aclose()


async def test_an_unknown_status_is_rejected_by_queue_page_itself():
    """The validation belongs in the row parser, so no caller — `queue_page` for an
    operator view, `queue_high_water_mark`, or correlation — can meet it first."""
    client, _ = make_client([("/api/queue", ok(queue_body(
        [queue_row(102, status="queued", document_id=None)])))])
    with pytest.raises(KreuzaktError, match="unknown queue status"):
        await client.queue_page()
    await client.aclose()


async def test_the_four_measured_queue_statuses_still_parse():
    rows = [queue_row(4, status="completed"), queue_row(3, status="failed"),
            queue_row(2, status="processing"), queue_row(1, status="pending")]
    client, _ = make_client([("/api/queue", ok(queue_body(rows)))])
    assert [row.status for row in await client.queue_page()] == [
        "completed", "failed", "processing", "pending"]
    await client.aclose()


# ---- 3. saturation is what the window shows, not what our constant says


async def test_a_saturated_window_is_judged_by_the_window_itself_not_by_our_constant():
    """If the server's cap ever drops to 5, five rows whose oldest is still above our
    mark prove exactly as little as a hundred. `len(newer) >= 100` called that 'no queue
    row' — an absence claim the window cannot support, and the failure this branch exists
    to prevent."""
    rows = [queue_row(1000 + i, f"other{i}.pdf") for i in range(5)]
    client, calls = make_client([("/api/queue", ok(queue_body(rows)))])
    with pytest.raises(CorrelationWindowFull):
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=900,
                                        poll_s=0, timeout_s=30)
    assert len(calls) == 1                    # decided on the first read, not the timeout
    await client.aclose()


async def test_a_window_that_reaches_below_the_mark_is_still_an_absence_answer():
    """The conservative rule must not become 'always retryable': once a row at or below
    the mark is visible, every row newer than it was inside the window, so absence is
    provable and the caller waits instead of re-uploading forever.

    The window here is mixed — two rows above the mark, one sitting on it — because a
    window of only-older rows would pass without ever testing the boundary.
    """
    rows = [queue_row(104, "other1.pdf"), queue_row(103, "other2.pdf"),
            queue_row(100, "older.pdf")]
    client, _ = make_client([("/api/queue", ok(queue_body(rows)))])
    with pytest.raises(KreuzaktError, match="no queue row"):
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=100,
                                        poll_s=0, timeout_s=0.05)
    await client.aclose()


# ---- 4. every call site decodes, or nothing does


async def test_upload_rejects_an_html_proxy_page_as_a_typed_error():
    """`upload` was the one call site with no decode guard: a 200 that is not JSON raised
    `json.JSONDecodeError`, which Task 6 cannot classify as a Kreuzakt failure."""
    client = text_client("<html><body>502 Bad Gateway</body></html>")
    with pytest.raises(KreuzaktError, match="not JSON"):
        await client.upload(UPLOAD_NAME, b"%PDF")
    await client.aclose()


async def test_upload_rejects_a_json_array_where_an_object_is_expected():
    client = json_client([])
    with pytest.raises(KreuzaktError, match="expected a JSON object"):
        await client.upload(UPLOAD_NAME, b"%PDF")
    await client.aclose()


async def test_upload_rejects_a_file_entry_it_cannot_read_rather_than_raising_typeerror():
    """`payload["files"][0]["relativePath"]` is the stored name correlation depends on;
    `["papra.pdf"]` or `[{}]` used to be a `TypeError`/`KeyError`."""
    for files in (["papra.pdf"], [{}], [{"relativePath": None}]):
        client = json_client({"files": files, "uploadedCount": 1,
                              "failedCount": 0, "errors": []})
        with pytest.raises(KreuzaktError, match="relativePath"):
            await client.upload(UPLOAD_NAME, b"%PDF")
        await client.aclose()


async def test_queue_page_refuses_a_row_that_is_not_an_object():
    client, _ = make_client([("/api/queue", ok(queue_body(["35"])))])
    with pytest.raises(KreuzaktError, match="expected a JSON object"):
        await client.queue_page()
    await client.aclose()


async def test_queue_page_refuses_a_row_without_the_keys_the_row_type_requires():
    for key in ("id", "filename", "status"):
        row = without(queue_row(7), key)
        client, _ = make_client([("/api/queue", ok(queue_body([row])))])
        with pytest.raises(KreuzaktError, match=f"missing {key}"):
            await client.queue_page()
        await client.aclose()


async def test_queue_page_refuses_an_id_that_is_not_an_integer():
    """`id` is the high-water mark and the correlation comparison; `"35"` would compare
    against an int with a `TypeError` deep inside `wait_for_queue_row`."""
    client, _ = make_client([("/api/queue", ok(queue_body(
        [patched(queue_row(35), id="35")])))])
    with pytest.raises(KreuzaktError, match="not an integer"):
        await client.queue_page()
    await client.aclose()


async def test_queue_counts_refuses_a_value_that_is_not_an_integer_count():
    """`int(value)` raised a bare `ValueError` for `"many"` and quietly accepted `"3"`;
    a caller reading queue depth should not have to guess which non-int slipped through."""
    for bad in ("many", "3", None, 1.5):
        client, _ = make_client([("/api/queue", ok(queue_body([], {"pending": bad})))])
        with pytest.raises(KreuzaktError, match="counts.pending"):
            await client.queue_counts()
        await client.aclose()


async def test_health_refuses_a_body_that_is_not_an_object():
    client = json_client(["ok"])
    with pytest.raises(KreuzaktError, match="expected a JSON object"):
        await client.health()
    await client.aclose()


async def test_health_requires_the_status_the_bridge_checks():
    """Task 11's `ledger --invalidate` reads `health()["status"] == "ok"` before it will
    invalidate an index; an object without `status` used to arrive as a bare `KeyError`
    at the worst possible moment."""
    client, _ = make_client([("/api/health", ok({"database": True}))])
    with pytest.raises(KreuzaktError, match="missing status"):
        await client.health()
    await client.aclose()


# ---- 5. the timeout diagnostic has to survive being read


async def test_the_timeout_diagnostic_says_which_row_is_stuck_instead_of_denying_it_exists():
    """The old text was 'no queue row' for a window that contained our row in
    `processing`. Whoever pages on this message goes looking for a missing upload while
    the upload is sitting in front of them."""
    client, _ = make_client([("/api/queue", ok(queue_body(
        [queue_row(102, status="processing", document_id=None)])))])
    with pytest.raises(KreuzaktError) as excinfo:
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=101,
                                        poll_s=0, timeout_s=0.05)
    message = str(excinfo.value)
    assert "102" in message and "processing" in message
    assert "no queue row for" not in message
    assert "no terminal queue row" in message


async def test_the_timeout_diagnostic_stops_calling_the_window_edge_the_newest_row_above_the_mark():
    """For a window of ids 100/50/20 with the mark at 1000, the old parenthetical reported
    `newest id above the mark: 100` while nothing in the window was above the mark."""
    rows = [queue_row(100, "a.pdf"), queue_row(50, "b.pdf"), queue_row(20, "c.pdf")]
    client, _ = make_client([("/api/queue", ok(queue_body(rows)))])
    with pytest.raises(KreuzaktError, match="no queue row") as excinfo:
        await client.wait_for_queue_row(UPLOAD_NAME, after_id=1000,
                                        poll_s=0, timeout_s=0.05)
    message = str(excinfo.value)
    assert "above the mark" not in message
    assert "0 of them newer than id 1000" in message
    assert "100..20" in message                    # the window's own edges, honestly


# ---- 6. the collision counter is ASCII digits or it is a different file


async def test_a_non_ascii_collision_suffix_is_not_one_of_ours():
    """`str.isdigit()` is True for `٣` and `²`, so in an index another writer also uses,
    `file (٣).pdf` could be matched as our `file.pdf` and its `document_id` attributed to
    our job — a wrong document indexed under the wrong id, with no error anywhere.
    Kreuzakt's own counter is an ASCII integer, so anything else is not ours.

    The window also carries one row sitting at the mark, so its lower edge is not above
    the mark: that makes absence observable, and 'no queue row' then really means 'the
    non-ASCII name was not matched as ours' rather than 'the window proved nothing'.
    """
    for counter in ("\u0663", "\u00b2", "1\u0663"):
        name = f"papra-d1-a1b2 ({counter}).pdf"
        client, _ = make_client([("/api/queue", ok(queue_body(
            [queue_row(12, name, document_id=9), queue_row(11, "older.pdf")])))])
        with pytest.raises(KreuzaktError, match="no queue row"):
            await client.wait_for_queue_row("papra-d1-a1b2.pdf", after_id=11,
                                            poll_s=0, timeout_s=0.05)
        await client.aclose()


# ---- 8. `list_documents` must not be the one leak in the narrowing


async def test_list_documents_drops_the_links_that_would_escape_the_narrowing():
    """`KzDocument`/`KzHit` exist so no Kreuzakt-side path reaches a caller, and the plan
    forbids `/thumbnail` leaving the proxy — but `list_documents` returned the server's
    dicts verbatim, `thumbnail_url` included."""
    page = load("kreuzakt_documents_page.json")
    client, _ = make_client([("/api/documents", ok(page))])
    rows = await client.list_documents()
    assert "thumbnail_url" in page["documents"][0]      # the captured row really had it
    assert not any(key in row for row in rows for key in
                   ("thumbnail_url", "download_url", "link_for_user", "stored_filename"))
    assert "/documents/" not in json.dumps(rows)
    assert set(rows[0]) == set(page["documents"][0]) - {"thumbnail_url"}
    assert rows[0]["id"] == 4 and rows[0]["title"]
    await client.aclose()


async def test_list_documents_refuses_a_row_that_is_not_an_object():
    client, _ = make_client([("/api/documents", ok({"documents": ["4"]}))])
    with pytest.raises(KreuzaktError, match="expected a JSON object"):
        await client.list_documents()
    await client.aclose()


async def test_list_documents_drops_the_link_keys_a_row_does_not_yet_carry():
    """The captured list row only ever had `thumbnail_url`, so the three other link keys
    are asserted here against synthetic rows: the filter is a rule about the field names,
    not a special case for the one key today's build happens to send. If a future Kreuzakt
    serves `download_url` on this endpoint it must not become the leak.

    SYNTHETIC: `download_url`, `link_for_user` and `stored_filename` were added to a
    captured row by this test; the document endpoint (not the list) is where the probe
    observed them, so their exact spelling here is inference from
    `kreuzakt_document.json`, not a captured list row.
    """
    page = load("kreuzakt_documents_page.json")
    page["documents"] = [patched(row,
                                 download_url="/api/documents/4/original",
                                 link_for_user="http://kreuzakt:3000/documents/4",
                                 stored_filename="originals/4/papra-search0004-44444444.pdf")
                         for row in page["documents"]]
    client, _ = make_client([("/api/documents", ok(page))])
    rows = await client.list_documents()
    # Spelled out, not imported from `_LINK_KEYS`: a test that subtracts the client's own
    # constant would keep passing if an entry were dropped from it.
    assert set(rows[0]) == set(page["documents"][0]) - {
        "thumbnail_url", "download_url", "link_for_user", "stored_filename"}
    assert "/documents/" not in json.dumps(rows)
    assert "originals/" not in json.dumps(rows)
    assert "kreuzakt" not in json.dumps(rows)
    assert rows[0]["original_filename"] == "papra-search0004-44444444.pdf"   # real fields survive
    await client.aclose()
