"""Kreuzakt's HTTP API, narrowed to the calls this bridge may actually make.

Every shape here comes from a captured response, not from the API's appearance:
`docs/superpowers/probes/2026-11-17-kreuzakt-contracts/` (`capture-queue.json` for
upload/queue/retry/delete, `capture-search.json` for search/documents). Two
consequences of that evidence govern this whole module.

**There is no pagination on `/api/queue`.** `limit` (default 20, clamped to 100) and
`status` are the only accepted parameters; `page`, `offset`, `perPage`, `count`,
`after` and `cursor` are silently ignored, and the endpoint serves only the newest
100 rows. A page walk therefore re-reads one page forever, so a high-water mark
produced by walking is either wrong or a claim of exhaustion that cannot be made.
`queue_high_water_mark` is one `?limit=1` request instead: ordering is
`created_at DESC, id DESC` while ids ascend with insertion, so the first row of a
one-row request is the newest row.

**Absence from that window is not absence from the queue.** The window can saturate
(a `reindex-all`, another writer, our own retries), so `wait_for_queue_row` has three
outcomes — a matched row, `AmbiguousCorrelation`, and `CorrelationWindowFull`. The
last two are retryable (Task 6 re-uploads; identical bytes deduplicate onto one
document) and none of them is `None`, because a job that reads `None` as "nothing to
do" reports success while having indexed nothing.

`/api/search` and `/api/documents` are the opposite case: both *do* honour `page` and
`limit` (measured 2026-11-17: `page=2&limit=1` returned the second row, `limit=1000`
came back with `limit: 100`), so those two are exposed, and `offset` is not, because
it demonstrably did nothing on either endpoint.
"""

from __future__ import annotations

import asyncio
import posixpath
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx

# `/api/queue` clamps `limit` to 100 and serves only the newest 100 rows; that is both
# the biggest window there is and the whole window, which is why it is a constant
# rather than a parameter a caller can raise.
_QUEUE_WINDOW = 100
_SEARCH_LIMIT_MAX = 100
_LIMIT_MAX = 100
_TERMINAL_STATUSES = ("completed", "failed")


class KreuzaktError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class CorrelationWindowFull(KreuzaktError):
    """The newest-100 window was full of other rows newer than our mark.

    Our row may well exist one row further down the table, where this API cannot show
    it. Retryable by re-uploading; never report it as "no such row".
    """


class AmbiguousCorrelation(KreuzaktError):
    """More than one terminal row carries our filename above our mark.

    Measured: one upload of bytes that were already present produced a second row, and
    a re-upload of the same *name* produced a third, all with the same `document_id`.
    Picking one would attribute a document to the wrong attempt, so this is reported
    and retried by re-uploading instead.
    """


@dataclass(frozen=True)
class QueueEntry:
    """One row of `/api/queue`.

    `page_count` is joined from the document upstream, so it is null whenever there is
    no live document — it is carried for reporting, never used as a completion signal.
    """

    id: int
    filename: str
    status: str
    error: str | None
    document_id: int | None
    page_count: int | None


@dataclass(frozen=True)
class KzDocument:
    """The fields `GET /api/documents/{id}` returns that this bridge may read or write.

    Deliberately absent: `stored_filename`, `thumbnail_url`, `download_url`,
    `link_for_user`. Kreuzakt has no tenancy, so a path into its storage is not
    something a caller of this client should ever be handed.
    """

    id: int
    title: str
    description: str
    content: str
    document_date: str | None
    original_filename: str
    updated_at: str | None


@dataclass(frozen=True)
class KzHit:
    """One row of `/api/search`. No `content` (the caller fetches the document when it
    needs the text, so a page of hits cannot blow up the response) and no `score`,
    which the measured response never carried."""

    id: int
    title: str
    document_date: str | None
    added_at: str
    mime_type: str
    original_filename: str


def _to_queue_entry(row: Mapping[str, Any]) -> QueueEntry:
    return QueueEntry(
        id=row["id"],
        filename=row["filename"],
        status=row["status"],
        error=row.get("error"),
        document_id=row.get("document_id"),
        page_count=row.get("page_count"),
    )


def _is_our_row(stored: str, uploaded: str) -> bool:
    """Exact equality, or the ` (N)` collision suffix. Nothing else.

    Measured: `filename` is the stored name, byte-identical to the upload response's
    `relativePath`, and a repeat of a name comes back as `papra-doc0006-6f708192
    (1).pdf`. Kreuzakt also *sanitises* unsafe characters (`papra:doc0005<…>.pdf` →
    `papra_doc0005_…_.pdf`), and that is not reimplemented here: Task 6 constrains the
    upload name to `[a-z0-9.-]` plus `.pdf`, so a sanitised name can only mean the
    server changed something it should not have, and matching it would be a guess.

    Suffix matching in particular would be wrong: `reindex-all` rows end with the
    original filename (`reindex-all:<batch>:15:papra-seq0004-9e9e9e9e.pdf`), so any
    suffix rule would read a re-extraction as one of our uploads.
    """
    if stored == uploaded:
        return True
    stem, extension = posixpath.splitext(uploaded)
    prefix = f"{stem} ("
    suffix = f"){extension}"
    if not (stored.startswith(prefix) and stored.endswith(suffix)):
        return False
    counter = stored[len(prefix):len(stored) - len(suffix)]
    return counter.isdigit()


class KreuzaktClient:
    def __init__(self, base_url: str, *, timeout_s: float = 30.0,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_s,
            transport=transport,
        )

    async def __aenter__(self) -> KreuzaktClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        try:
            response = await self._client.request(method, path, **kwargs)
        except httpx.HTTPError as error:
            # No status: there was no response. Retry policy is the caller's (Task 6
            # bounds it with `index_max_attempts`), because a deterministic failure
            # here re-fails identically and an unbounded retry would spin on it.
            raise KreuzaktError(
                f"{method} {path} failed: {type(error).__name__}: {error}"
            ) from error
        if response.is_success:
            return response
        raise KreuzaktError(
            f"{method} {path} failed: {response.status_code} {response.text[:200]}",
            response.status_code,
        )

    async def _get_json(self, path: str, **params: Any) -> Any:
        response = await self._request("GET", path, params=params or None)
        try:
            return response.json()
        except ValueError as error:
            raise KreuzaktError(f"{path} returned a body that is not JSON") from error

    async def health(self) -> dict:
        return await self._get_json("/api/health")

    async def queue_counts(self) -> dict[str, int]:
        """Queue depth by status. `counts` is a global aggregate of the envelope, so
        the one-row request that also carries it costs nothing extra."""
        body = await self._get_json("/api/queue", limit=1)
        counts = body.get("counts") if isinstance(body, dict) else None
        if not isinstance(counts, dict):
            raise KreuzaktError("unexpected /api/queue envelope: no counts block")
        return {str(key): int(value) for key, value in counts.items()}

    async def queue_page(self, *, limit: int = _QUEUE_WINDOW,
                         status: str | None = None) -> list[QueueEntry]:
        """The newest `limit` rows (server caps at 100), optionally one status.

        No `page`/`offset`: they are ignored upstream, so exposing them here would let
        a caller believe it had asked for something else.
        """
        params: dict[str, Any] = {"limit": max(1, min(limit, _QUEUE_WINDOW))}
        if status is not None:
            params["status"] = status
        body = await self._get_json("/api/queue", **params)
        rows = body.get("entries") if isinstance(body, dict) else None
        if not isinstance(rows, list):
            raise KreuzaktError(
                "unexpected /api/queue envelope: no entries list "
                f"(body keys: {sorted(body) if isinstance(body, dict) else type(body).__name__})"
            )
        return [_to_queue_entry(row) for row in rows]

    async def queue_high_water_mark(self) -> int:
        """Newest queue id right now, or 0 for an empty queue. Recorded BEFORE upload.

        Measured 2026-11-17: `/api/queue` has no pagination (only `limit`, clamped to
        100, and `status`; `page`/`offset`/`cursor` are ignored), it serves the newest
        100 rows, and it orders them `created_at DESC, id DESC` while ids ascend with
        insertion. So the newest row is the first row of a one-row request: one round
        trip, and no walk to get wrong.

        What this can no longer claim is *exhaustion* — a newest-100 window cannot be
        proven complete — which is why `wait_for_queue_row` reports a saturated window
        as `CorrelationWindowFull` rather than as "never uploaded".
        """
        rows = await self.queue_page(limit=1)
        return rows[0].id if rows else 0

    async def wait_for_queue_row(self, filename: str, *, after_id: int, poll_s: float,
                                 timeout_s: float) -> QueueEntry:
        """The one queue row our upload produced, or a typed failure.

        One match, three failures:

        * a `QueueEntry` in `completed` or `failed` — the matched row;
        * `AmbiguousCorrelation` — more than one terminal row matches, so no row can be
          picked honestly;
        * `CorrelationWindowFull` — the window held 100 rows newer than `after_id` and
          none of them was ours, which says nothing about whether our row exists;
        * `KreuzaktError("no queue row …")` once `timeout_s` has passed with the window
          readable and no candidate.

        `filename` must be the `relativePath` the upload response returned, not the name
        we constructed. Matching is exact plus the ` (N)` collision suffix, and the id
        comparison is strictly greater than the mark recorded before the upload: rows at
        or below it are earlier attempts at the same name.
        """
        deadline = time.monotonic() + timeout_s
        while True:
            rows = await self.queue_page(limit=_QUEUE_WINDOW)
            candidates = [row for row in rows
                          if row.id > after_id and _is_our_row(row.filename, filename)]
            terminal = [row for row in candidates if row.status in _TERMINAL_STATUSES]
            if len(terminal) > 1:
                raise AmbiguousCorrelation(
                    f"ambiguous correlation: {len(terminal)} terminal queue rows above id "
                    f"{after_id} for {filename} (ids {[row.id for row in terminal]})"
                )
            if len(terminal) == 1:
                row = terminal[0]
                if row.status == "completed" and row.document_id is None:
                    # Measured: deleting a document nulls `document_id` on every row
                    # that pointed at it while the row stays `completed`. So this is a
                    # vanished document, and re-running the queue row is not even
                    # possible (`retry` accepts only `failed`). The caller decides by
                    # reading Papra's own state, so hand it the error, not a row.
                    raise KreuzaktError(
                        f"queue row {row.id} for {filename} is completed without document_id"
                    )
                return row
            newer = [row for row in rows if row.id > after_id]
            if not candidates and len(newer) >= _QUEUE_WINDOW:
                raise CorrelationWindowFull(
                    f"correlation window full: {len(newer)} other queue rows newer than "
                    f"id {after_id} and none named {filename}; the newest-{_QUEUE_WINDOW} "
                    "window cannot show older rows, so this is not evidence of absence"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise KreuzaktError(
                    f"no queue row for {filename} within {timeout_s}s (newest id above "
                    f"the mark: {max((row.id for row in rows), default=after_id)})"
                )
            await asyncio.sleep(min(poll_s, remaining) if poll_s > 0 else min(0.01, remaining))

    async def upload(self, filename: str, content: bytes) -> str:
        """Upload one file and return the name Kreuzakt stored it under.

        The multipart field is `files` (measured: a wrong field name is a 400 whose
        verbatim message is `Upload requests must include at least one file`), and one
        request carries exactly one file: the correlation window is 100 rows, so a
        caller that fires several uploads before correlating is gambling that nothing
        else wrote 90 rows in between.

        The return value is the server's `relativePath`, not `filename`: sanitising and
        the collision suffix both rename the row under us, and the stored name is the
        only value guaranteed to be what the queue will report.
        """
        response = await self._request(
            "POST", "/api/upload",
            files={"files": (filename, content, "application/pdf")},
        )
        payload = response.json()
        if payload.get("failedCount") or not payload.get("files"):
            raise KreuzaktError(f"upload rejected: {payload.get('errors')!r}")
        return payload["files"][0]["relativePath"]

    async def get_document(self, document_id: int) -> KzDocument:
        body = await self._get_json(f"/api/documents/{document_id}")
        if not isinstance(body, dict) or "id" not in body:
            raise KreuzaktError(f"unexpected /api/documents/{document_id} body")
        return KzDocument(
            id=body["id"],
            title=body.get("title") or "",
            description=body.get("description") or "",
            content=body.get("content") or "",
            document_date=body.get("document_date"),
            original_filename=body.get("original_filename") or "",
            updated_at=body.get("updated_at"),
        )

    async def delete_document(self, document_id: int) -> None:
        """Delete a document. 404 counts as done: already gone is gone.

        Measured side effect worth knowing when the caller reads a queue row: the delete
        nulls `document_id` on every queue row that referenced this document and leaves
        the rows themselves in place, `completed`.
        """
        try:
            await self._request("DELETE", f"/api/documents/{document_id}")
        except KreuzaktError as error:
            if error.status == 404:
                return None
            raise
        return None

    async def search(self, query: str, *, page: int = 1,
                     limit: int = _SEARCH_LIMIT_MAX) -> tuple[list[KzHit], int]:
        """One page of hits plus the server's `total`.

        Measured 2026-11-17: `q`, `page` (1-based) and `limit` (default 20, clamped to
        100) all do something — `page=2&limit=1` returned the second row and `limit=1000`
        came back with `limit: 100`. `offset`, `pageIndex` and `pageSize` did nothing at
        all, so they are not parameters here. A page beyond the end is a 200 with no
        results, not an error, and `total` still reports the full match count.

        A hit carries `content`, `snippet` and `thumbnail_url`; none of them reach the
        caller — see `KzHit`.
        """
        params = {
            "q": query,
            "page": max(1, page),
            "limit": max(1, min(limit, _SEARCH_LIMIT_MAX)),
        }
        body = await self._get_json("/api/search", **params)
        rows = body.get("results") if isinstance(body, dict) else None
        total = body.get("total") if isinstance(body, dict) else None
        if not isinstance(rows, list) or not isinstance(total, int):
            raise KreuzaktError(
                "unexpected /api/search envelope: need `results` and `total` "
                f"(body keys: {sorted(body) if isinstance(body, dict) else type(body).__name__})"
            )
        hits = [KzHit(id=row["id"], title=row.get("title") or "",
                      document_date=row.get("document_date"),
                      added_at=row.get("added_at") or "",
                      mime_type=row.get("mime_type") or "",
                      original_filename=row.get("original_filename") or "")
                for row in rows]
        return hits, total

    async def list_documents(self, *, page: int = 1, limit: int = _LIMIT_MAX,
                             since: str | None = None) -> list[dict]:
        """Rows of `GET /api/documents` in the server's own order (newest first).

        Measured: the envelope is `{documents, total, page, limit}` and a row carries
        `id, title, description, document_date, added_at, original_filename,
        mime_type, thumbnail_url` — no `content`. `limit` is honoured and clamped to
        100; `page` is honoured; `offset` is ignored and so is not offered.

        `since` is honoured but has one trap: Kreuzakt compares it against a
        `YYYY-MM-DD HH:MM:SS` column, so `"2026-11-17 22:28:05"` filters while the same
        instant as `"2026-11-17T22:28:05Z"` matched *nothing* (200, `total: 0`) — a
        silent empty result, which for an incremental scan means "nothing changed,
        forever". A value Kreuzakt would silently ignore is refused here instead; which
        column it filters was not determined, so `since` is a coarse gate and the
        caller must not read it as a watermark.
        """
        params: dict[str, Any] = {"page": max(1, page),
                                  "limit": max(1, min(limit, _LIMIT_MAX))}
        if since is not None:
            params["since"] = _require_documents_since(since)
        body = await self._get_json("/api/documents", **params)
        rows = body.get("documents") if isinstance(body, dict) else None
        if not isinstance(rows, list):
            raise KreuzaktError(
                "unexpected /api/documents envelope: no documents list "
                f"(body keys: {sorted(body) if isinstance(body, dict) else type(body).__name__})"
            )
        return list(rows)

    async def retry_queue(self, entry_id: int) -> None:
        """Re-queue a `failed` row.

        The upstream 400/404 distinction survives verbatim, because the two mean
        different things to the caller: 400 `Queue entry 31 is not in failed status`
        means the row already finished (so the answer is to read the row, not retry it),
        404 `Queue entry 999999 not found` means the row is gone.

        Measured: a retry **reuses** the row rather than appending one, and a
        deterministic failure re-fails with identical text, so the attempt counter that
        bounds this lives in the job, not here.
        """
        await self._request("POST", f"/api/queue/{entry_id}/retry")


_DOCUMENTS_SINCE = re.compile(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$")


def _require_documents_since(since: str) -> str:
    """Accept only the two timestamp shapes `/api/documents` demonstrably filters on.

    Measured: `"2026-11-17 22:28:05"` and `"2026-11-17"` filter, while
    `"2026-11-17T22:28:05Z"` — the same instant — came back 200 with **no** rows, and
    `"1700000000"` came back with **all** of them. Both wrong answers look like a
    working request: one means an incremental scan concludes nothing ever changed, the
    other means it re-reads everything. The comparison upstream is a lexical string
    comparison against a `YYYY-MM-DD HH:MM:SS` column, so only that shape (or its date
    prefix) is a filter at all.
    """
    if not _DOCUMENTS_SINCE.match(since):
        raise KreuzaktError(
            f"since={since!r} is not a Kreuzakt document watermark: `since` must be "
            "'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' (measured: an ISO-8601 value with "
            "'T'/'Z' returns 200 with no rows, and an epoch integer returns every row, "
            "so neither can be sent as if it filtered)"
        )
    return since
