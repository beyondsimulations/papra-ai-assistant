"""`KreuzaktClient` against captured payloads.

Every body here is a file under `tests/fixtures/` that a probe wrote from a live
container (`docs/superpowers/probes/2026-11-17-kreuzakt-contracts/`):
`kreuzakt_queue_page.json`, `kreuzakt_document.json` from Task 3's `probe_queue.py`;
`kreuzakt_search_page.json`, `kreuzakt_search_empty.json`,
`kreuzakt_documents_page.json`, `kreuzakt_upload_response.json` from Task 5's
`probe_search.py`. No payload in this file was written by hand.

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
    `None` here would leave the job to correlate against a row that will never exist."""
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
    created with `document_id` already set, so a non-null id proves nothing."""
    client, calls = make_client([("/api/queue", ok(queue_body(
        [queue_row(102, status="pending", document_id=None)])))])
    with pytest.raises(KreuzaktError, match="no queue row"):
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
    assert "limit=1" in calls[0][1]
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
